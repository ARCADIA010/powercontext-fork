# Copyright (c) 2026 OceanBase.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Deterministic, self-validating Context Pack and prefix delta wire format."""
# ruff: noqa: TRY003

from __future__ import annotations

import base64
import binascii
import hashlib
import json
from dataclasses import dataclass

_PACK_SCHEMA = "powercontext.context-pack.v1"
_DELTA_SCHEMA = "powercontext.context-pack-delta.v1"


class ContextPackError(ValueError):
    """Invalid Context Pack content, wire data, or delta application."""


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _strict_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ContextPackError("duplicate JSON object field")
        value[key] = item
    return value


def _invalid_constant(value: str) -> None:
    raise ContextPackError(f"invalid JSON numeric constant: {value}")


def _decode_object(data: bytes) -> dict[str, object]:
    if type(data) is not bytes:
        raise ContextPackError("wire data must be bytes")
    try:
        value = json.loads(data.decode("utf-8"), object_pairs_hook=_strict_object, parse_constant=_invalid_constant)
    except ContextPackError:
        raise
    except (UnicodeError, json.JSONDecodeError, TypeError, ValueError) as error:
        raise ContextPackError("invalid Context Pack JSON") from error
    if not isinstance(value, dict):
        raise ContextPackError("wire payload must be a JSON object")
    return value


def _check_fields(value: dict[str, object], expected: set[str]) -> None:
    if set(value) != expected:
        raise ContextPackError("wire payload fields do not match the schema")


def _check_digest(value: object) -> str:
    if not isinstance(value, str) or len(value) != 71 or not value.startswith("sha256:"):
        raise ContextPackError("digest must be a sha256 digest")
    hexadecimal = value[7:]
    if any(character not in "0123456789abcdef" for character in hexadecimal):
        raise ContextPackError("digest must be a lowercase hexadecimal sha256 digest")
    return value


def _decode_items(value: object) -> tuple[bytes, ...]:
    if not isinstance(value, list):
        raise ContextPackError("items must be a Base64 string array")
    items: list[bytes] = []
    for encoded in value:
        if not isinstance(encoded, str):
            raise ContextPackError("items must be a Base64 string array")
        try:
            item = base64.b64decode(encoded, validate=True)
            if base64.b64encode(item).decode("ascii") != encoded:
                raise ContextPackError("items must use canonical Base64 encoding")
            item.decode("utf-8")
        except (binascii.Error, UnicodeError, ValueError) as error:
            raise ContextPackError("items must contain valid Base64-encoded UTF-8") from error
        items.append(item)
    return tuple(items)


def _validate_items(items: tuple[bytes, ...]) -> tuple[bytes, ...]:
    if not isinstance(items, tuple):
        raise ContextPackError("items must be a tuple of bytes")
    for item in items:
        if type(item) is not bytes:
            raise ContextPackError("each item must be bytes")
        try:
            item.decode("utf-8")
        except UnicodeError as error:
            raise ContextPackError("each item must contain valid UTF-8") from error
    return items


def _pack_unsigned(items: tuple[bytes, ...]) -> dict[str, object]:
    return {"schema": _PACK_SCHEMA, "items": [base64.b64encode(item).decode("ascii") for item in items]}


@dataclass(frozen=True, init=False)
class ContextPack:
    """Immutable UTF-8 chunks with a canonical digest and exact serialized size."""

    items: tuple[bytes, ...]
    digest: str
    byte_count: int

    def __init__(self, items: tuple[bytes, ...]) -> None:
        validated = _validate_items(items)
        unsigned = _pack_unsigned(validated)
        digest = f"sha256:{hashlib.sha256(_canonical_json(unsigned)).hexdigest()}"
        byte_count = 0
        while True:
            payload = {**unsigned, "digest": digest, "byte_count": byte_count}
            measured = len(_canonical_json(payload))
            if measured == byte_count:
                break
            byte_count = measured
        object.__setattr__(self, "items", validated)
        object.__setattr__(self, "digest", digest)
        object.__setattr__(self, "byte_count", byte_count)

    @classmethod
    def build(cls, items: tuple[bytes, ...]) -> ContextPack:
        """Construct a pack from already separated UTF-8 byte chunks."""

        return cls(items)

    @classmethod
    def from_content(cls, content: str, *, chunk_bytes: int = 512) -> ContextPack:
        """Split text deterministically on character boundaries within the byte limit."""

        if not isinstance(content, str):
            raise ContextPackError("content must be a string")
        if type(chunk_bytes) is not int or chunk_bytes <= 0:
            raise ContextPackError("chunk_bytes must be a positive integer")
        try:
            chunks: list[bytes] = []
            current: list[str] = []
            current_bytes = 0
            for character in content:
                encoded = character.encode("utf-8")
                if len(encoded) > chunk_bytes:
                    raise ContextPackError("chunk_bytes cannot fit one UTF-8 character")
                if current and current_bytes + len(encoded) > chunk_bytes:
                    chunks.append("".join(current).encode("utf-8"))
                    current = []
                    current_bytes = 0
                current.append(character)
                current_bytes += len(encoded)
            if current:
                chunks.append("".join(current).encode("utf-8"))
        except UnicodeError as error:
            raise ContextPackError("content must be valid UTF-8 text") from error
        return cls.build(tuple(chunks))

    def to_bytes(self) -> bytes:
        """Return the canonical complete Pack JSON wire representation."""

        return _canonical_json({**_pack_unsigned(self.items), "digest": self.digest, "byte_count": self.byte_count})

    @classmethod
    def from_bytes(cls, data: bytes) -> ContextPack:
        """Decode and verify a complete Pack payload without trusting its metadata."""

        value = _decode_object(data)
        _check_fields(value, {"schema", "items", "digest", "byte_count"})
        if value["schema"] != _PACK_SCHEMA:
            raise ContextPackError("unsupported Context Pack schema")
        digest = _check_digest(value["digest"])
        byte_count = value["byte_count"]
        if type(byte_count) is not int or byte_count < 0:
            raise ContextPackError("byte_count must be a non-negative integer")
        pack = cls.build(_decode_items(value["items"]))
        if pack.digest != digest:
            raise ContextPackError("Context Pack digest does not match its items")
        if pack.byte_count != byte_count:
            raise ContextPackError("Context Pack byte_count does not match its canonical wire representation")
        return pack


@dataclass(frozen=True)
class ContextPackDelta:
    """Prefix reuse and appended chunks that reconstruct one target Pack."""

    base_digest: str
    target_digest: str
    prefix_items: int
    appended: tuple[bytes, ...]

    def __post_init__(self) -> None:
        _check_digest(self.base_digest)
        _check_digest(self.target_digest)
        if type(self.prefix_items) is not int or self.prefix_items < 0:
            raise ContextPackError("prefix_items must be a non-negative integer")
        _validate_items(self.appended)

    @classmethod
    def between(cls, base: ContextPack, target: ContextPack) -> ContextPackDelta:
        """Reuse the longest equal chunk prefix and append the complete target suffix."""

        if not isinstance(base, ContextPack) or not isinstance(target, ContextPack):
            raise ContextPackError("between requires ContextPack values")
        prefix_items = 0
        maximum = min(len(base.items), len(target.items))
        while prefix_items < maximum and base.items[prefix_items] == target.items[prefix_items]:
            prefix_items += 1
        return cls(base.digest, target.digest, prefix_items, target.items[prefix_items:])

    @property
    def byte_count(self) -> int:
        return len(self.to_bytes())

    def to_bytes(self) -> bytes:
        """Return the canonical delta JSON wire representation."""

        return _canonical_json({
            "schema": _DELTA_SCHEMA,
            "base_digest": self.base_digest,
            "target_digest": self.target_digest,
            "prefix_items": self.prefix_items,
            "appended": [base64.b64encode(item).decode("ascii") for item in self.appended],
        })

    @classmethod
    def from_bytes(cls, data: bytes) -> ContextPackDelta:
        """Decode a delta while rejecting malformed fields and Base64 chunks."""

        value = _decode_object(data)
        _check_fields(value, {"schema", "base_digest", "target_digest", "prefix_items", "appended"})
        if value["schema"] != _DELTA_SCHEMA:
            raise ContextPackError("unsupported Context Pack Delta schema")
        prefix_items = value["prefix_items"]
        if type(prefix_items) is not int or prefix_items < 0:
            raise ContextPackError("prefix_items must be a non-negative integer")
        delta = cls(
            base_digest=_check_digest(value["base_digest"]),
            target_digest=_check_digest(value["target_digest"]),
            prefix_items=prefix_items,
            appended=_decode_items(value["appended"]),
        )
        return delta


def apply_context_pack_delta(base: ContextPack, delta: ContextPackDelta) -> ContextPack:
    """Rebuild and verify the target Pack using only a base and decoded delta."""

    if not isinstance(base, ContextPack) or not isinstance(delta, ContextPackDelta):
        raise ContextPackError("delta application requires a ContextPack and ContextPackDelta")
    if base.digest != delta.base_digest:
        raise ContextPackError("delta base digest does not match the supplied Pack")
    if type(delta.prefix_items) is not int or delta.prefix_items < 0 or delta.prefix_items > len(base.items):
        raise ContextPackError("delta prefix_items is outside the supplied Pack")
    target = ContextPack.build(base.items[: delta.prefix_items] + delta.appended)
    if target.digest != delta.target_digest:
        raise ContextPackError("delta target digest does not match reconstructed items")
    return target
