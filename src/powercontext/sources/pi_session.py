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

"""Capture complete Pi branch evidence with durable incremental checkpoints."""

from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
from typing import Any
from urllib.parse import quote

from pydantic import BaseModel, JsonValue

from powercontext.limits import MAX_SOURCE_ID_LENGTH
from powercontext.sources.connectors import (
    ConnectorRunCompletion,
    ConnectorRunSession,
    ConnectorRunStatus,
    ConnectorSubmissionStatus,
)
from powercontext.sources.definitions import AdapterSourceDefinition, SourceDefinition
from powercontext.sources.models import Source, SourceMaterialization
from powercontext.sources.projections import TEXT_EVIDENCE_PROJECTION_KEY, TextEvidence

PI_EVENT_SOURCE_NAME = "pi-event"


class PiSessionInput(BaseModel):
    """An offline Pi v3 archive and optional selected branch endpoint."""

    content: bytes
    leaf_id: str | None = None


class PiEventInput(BaseModel):
    """One visible event, including its complete original JSON record."""

    session_id: str
    event_id: str
    parent_id: str | None
    event_type: str
    role: str
    content: str
    record: dict[str, Any]


class PiEventSource(Source):
    """Captured original evidence independent of a session's selected branch."""

    source_id: str
    session_id: str
    event_id: str
    parent_id: str | None
    event_type: str
    role: str
    content: str
    record: dict[str, Any]


def _source_id(session_id: str, event_id: str) -> str:
    if not session_id.strip() or not event_id.strip():
        raise ValueError("Pi session and event identities must not be blank")  # noqa: TRY003
    identity = f"pi:{quote(session_id, safe='')}:{quote(event_id, safe='')}"
    if len(identity) > MAX_SOURCE_ID_LENGTH:
        raise ValueError("Pi event identity exceeds the Source length limit")  # noqa: TRY003
    return identity


def _reject_constant(value: str) -> None:
    raise ValueError(f"Invalid JSON numeric constant: {value}")  # noqa: TRY003


def _read_archive(content: bytes) -> tuple[str, dict[str, dict[str, Any]]]:
    records: list[dict[str, Any]] = []
    for line in io.StringIO(content.decode("utf-8-sig")):
        if not line.strip():
            continue
        record = json.loads(line, parse_constant=_reject_constant)
        if not isinstance(record, dict):
            raise ValueError("Pi records must be JSON objects")  # noqa: TRY003, TRY004
        records.append(record)
    if not records:
        raise ValueError("Pi archive requires a session header")  # noqa: TRY003
    header, *nodes = records
    session_id = _session_id(header)
    by_id: dict[str, dict[str, Any]] = {}
    for record in nodes:
        event_id = record.get("id")
        if not isinstance(event_id, str) or not event_id.strip() or event_id in by_id:
            raise ValueError("Pi event ids must be non-empty and unique")  # noqa: TRY003
        if "parentId" not in record:
            raise ValueError("Pi event requires parentId")  # noqa: TRY003
        parent_id = record["parentId"]
        # Requiring an earlier parent also rules out self-loops and longer cycles.
        if parent_id is not None and (not isinstance(parent_id, str) or parent_id not in by_id):
            raise ValueError("Pi parentId must reference an earlier event or be null")  # noqa: TRY003
        if not isinstance(record.get("type"), str) or not record["type"]:
            raise ValueError("Pi event type must be a non-empty string")  # noqa: TRY003
        by_id[event_id] = record
    return session_id, by_id


def _session_id(header: dict[str, Any]) -> str:
    session_id = header.get("id")
    if (
        header.get("type") != "session"
        or type(header.get("version")) is not int
        or header["version"] != 3
        or not isinstance(session_id, str)
        or not session_id.strip()
    ):
        raise ValueError("Pi archive requires a v3 session header with a non-empty id")  # noqa: TRY003
    return session_id


def _visible_content(content: Any, *, tool_calls: bool = False) -> str:
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for block in content:
        if not isinstance(block, dict):
            continue
        if block.get("type") == "text" and isinstance(block.get("text"), str):
            parts.append(block["text"])
        elif tool_calls and block.get("type") == "toolCall":
            arguments = json.dumps(
                block.get("arguments", {}), ensure_ascii=False, sort_keys=True, separators=(",", ":")
            )
            parts.append(f"[tool_call {block.get('name', '')} {block.get('id', '')}] {arguments}")
    return "\n".join(parts)


def _message_body(record: dict[str, Any]) -> tuple[str, str]:
    message = record.get("message")
    if not isinstance(message, dict):
        raise ValueError("Pi message must be an object")  # noqa: TRY003, TRY004
    role = message.get("role")
    if not isinstance(role, str) or not role:
        raise ValueError("Pi message role must be a non-empty string")  # noqa: TRY003
    if role == "system":
        return role, ""
    body = _visible_content(message.get("content"), tool_calls=True)
    if role == "toolResult":
        if not body:
            return role, ""
        body = f"[tool_result {message.get('toolName', '')} {message.get('toolCallId', '')}] {body}"
    elif role == "bashExecution":
        command, output = message.get("command", ""), message.get("output", "")
        if not isinstance(command, str) or not isinstance(output, str):
            raise ValueError("Pi bash command and output must be strings")  # noqa: TRY003
        if not command and not output:
            return role, ""
        body = f"$ {command}\n{output}"
    return role, body


def _event_input(session_id: str, record: dict[str, Any]) -> PiEventInput | None:
    event_type = record["type"]
    if event_type == "message":
        role, body = _message_body(record)
    elif event_type in {"compaction", "branch_summary"}:
        role, body = event_type, record.get("summary", "")
        if not isinstance(body, str):
            raise ValueError("Pi summary must be a string")  # noqa: TRY003
    elif event_type == "custom_message":
        role, body = "custom", _visible_content(record.get("content"))
    else:
        return None
    if not body:
        return None
    _source_id(session_id, record["id"])
    return PiEventInput(
        session_id=session_id,
        event_id=record["id"],
        parent_id=record["parentId"],
        event_type=event_type,
        role=role,
        content=f"{role}: {body}",
        record=record,
    )


def _parse_archive(value: PiSessionInput) -> tuple[str, dict[str, dict[str, Any]], tuple[PiEventInput, ...]]:
    session_id, records = _read_archive(value.content)
    visible: dict[str, PiEventInput] = {}
    for event_id, record in records.items():
        event = _event_input(session_id, record)
        if event is not None:
            visible[event_id] = event
    leaf_id = value.leaf_id
    if leaf_id is None:
        leaf_id = next(reversed(records), None)
    elif leaf_id not in records:
        raise ValueError("Pi leaf_id must identify an event in the archive")  # noqa: TRY003
    chain: list[PiEventInput] = []
    while leaf_id is not None:
        if leaf_id in visible:
            chain.append(visible[leaf_id])
        leaf_id = records[leaf_id]["parentId"]
    return session_id, records, tuple(reversed(chain))


def parse_pi_session(value: PiSessionInput) -> tuple[PiEventInput, ...]:
    """Validate the entire archive, then return visible root-to-leaf evidence."""

    return _parse_archive(value)[2]


class PiEventSourceAdapter:
    """Resolve and read complete captured Pi events through native contracts."""

    input_class = PiEventInput
    name = PI_EVENT_SOURCE_NAME
    source_class = PiEventSource

    async def resolve(self, value: PiEventInput, /) -> PiEventSource:
        identity = _source_id(value.session_id, value.event_id)
        return PiEventSource(
            name=identity,
            source_id=identity,
            materialization=SourceMaterialization.CAPTURED,
            **value.model_dump(),
        )

    async def read(self, source: PiEventSource, /) -> PiEventInput:
        return PiEventInput.model_validate(source.model_dump())


class PiEventTextEvidenceProjection:
    """Expose full event text and identity without changing original evidence."""

    name = TEXT_EVIDENCE_PROJECTION_KEY.name
    version = TEXT_EVIDENCE_PROJECTION_KEY.version
    source_class = PiEventSource
    output_class: type[BaseModel] = TextEvidence

    def project(self, source: PiEventSource, /) -> TextEvidence:
        return TextEvidence(
            source_type=PI_EVENT_SOURCE_NAME,
            source_id=source.name,
            content=source.content,
            metadata={
                "session_id": source.session_id,
                "event_id": source.event_id,
                "parent_id": source.parent_id,
                "event_type": source.event_type,
                "role": source.role,
                "record": source.record,
            },
        )


PI_EVENT_SOURCE_DEFINITION: SourceDefinition[PiEventInput, PiEventSource, PiEventInput] = AdapterSourceDefinition(
    PiEventSourceAdapter(), projections=(PiEventTextEvidenceProjection(),)
)


def _record_digest(record: dict[str, Any]) -> str:
    canonical = json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _checkpoint_events(checkpoint: JsonValue | None, session_id: str) -> dict[str, str]:
    if checkpoint is None:
        return {}
    if not isinstance(checkpoint, dict) or checkpoint.get("session_id") != session_id:
        raise ValueError("Pi checkpoint belongs to a different session or has invalid format")  # noqa: TRY003
    events = checkpoint.get("events")
    if not isinstance(events, dict) or not all(isinstance(digest, str) for digest in events.values()):
        raise ValueError("Pi checkpoint events must map event ids to record digests")  # noqa: TRY003
    return {event_id: str(digest) for event_id, digest in events.items()}


class PiSessionConnector:
    """Import new selected-branch events while rejecting rewritten history."""

    name = "pi-session-history"
    version = "1"
    source_definitions = frozenset({PI_EVENT_SOURCE_NAME})

    def __init__(self, path: Path, leaf_id: str | None = None) -> None:
        self.path = path
        self.leaf_id = leaf_id

    async def run(self, session: ConnectorRunSession, /) -> ConnectorRunCompletion:
        session_id, records, events = _parse_archive(
            PiSessionInput(content=self.path.read_bytes(), leaf_id=self.leaf_id)
        )
        imported = _checkpoint_events(session.checkpoint, session_id)
        # Check all previously imported branches before submitting any new item.
        for event_id, digest in imported.items():
            if event_id not in records or _record_digest(records[event_id]) != digest:
                raise ValueError("Previously imported Pi event was removed or rewritten")  # noqa: TRY003
        for event in events:
            if event.event_id in imported:
                continue
            outcome = await session.submit(event.event_id, PI_EVENT_SOURCE_NAME, event)
            if outcome.status is not ConnectorSubmissionStatus.ACCEPTED:
                return ConnectorRunCompletion(status=ConnectorRunStatus.INCOMPLETE, checkpoint=session.checkpoint)
            imported[event.event_id] = _record_digest(event.record)
        checkpoint: JsonValue = {"session_id": session_id, "events": dict(imported)}
        return ConnectorRunCompletion(status=ConnectorRunStatus.COMPLETE, checkpoint=checkpoint)
