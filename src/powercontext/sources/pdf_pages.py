"""Capture complete physical PDF pages and retrieve exact scoped Memory evidence offline."""

from __future__ import annotations

import hashlib
import io
import json
import math
import re
import zlib
from collections import Counter
from dataclasses import dataclass
from itertools import pairwise

from pydantic import BaseModel, Field, field_validator
from pypdf import PageObject, PdfReader, get_configuration
from pypdf.filters import ASCII85Decode, ASCIIHexDecode, FlateDecode
from pypdf.generic import ArrayObject, DictionaryObject, EncodedStreamObject, IndirectObject

from powercontext.artifacts import MemoryCitation
from powercontext.builtin.runtime import GetMemoryEntryRequest, SearchMemoryRequest
from powercontext.sources.definitions import AdapterSourceDefinition, SourceDefinition
from powercontext.sources.models import Source, SourceMaterialization
from powercontext.sources.projections import TEXT_EVIDENCE_PROJECTION_KEY, TextEvidence


class PDFDocumentInput(BaseModel):
    """Original PDF bytes and a caller-owned, nonblank document identity."""

    document_id: str
    content: bytes

    @field_validator("document_id")
    @classmethod
    def validate_document_id(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("document_id must not be empty or blank")  # noqa: TRY003
        return value


class PDFPageInput(BaseModel):
    """All extractable text from one one-based physical page of an exact revision."""

    document_id: str
    document_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    page_number: int = Field(ge=1, strict=True)
    text: str

    _validate_document_id = field_validator("document_id")(PDFDocumentInput.validate_document_id.__func__)


def parse_pdf_document(value: PDFDocumentInput) -> tuple[PDFPageInput, ...]:
    """Parse every physical page atomically; never execute embedded PDF content."""

    try:
        reader = PdfReader(io.BytesIO(value.content), strict=True)
        if reader.is_encrypted and not reader.decrypt(""):
            raise ValueError("PDF requires a nonempty password")  # noqa: TRY003, TRY301
        digest = hashlib.sha256(value.content).hexdigest()
        pages = []
        for number, page in enumerate(reader.pages, 1):
            _validate_page_flate(page, number)
            pages.append(
                PDFPageInput(
                    document_id=value.document_id,
                    document_sha256=digest,
                    page_number=number,
                    text=page.extract_text() or "",
                )
            )
        return tuple(pages)
    except Exception as error:
        # pypdf exposes malformed structures through several exception families,
        # including failures encountered only while decoding a later page stream.
        raise ValueError("Invalid or incompletely readable PDF document") from error  # noqa: TRY003


def _validate_flate(data: bytes, output_limit: int) -> None:
    _decode_flate(data, output_limit, keep=False)


def _decode_flate(data: bytes, output_limit: int, *, keep: bool) -> bytes:
    # Validate framing and checksum; with keep=True the bounded output is retained
    # so a following chain stage is replayed against real decoded data.
    decoder = zlib.decompressobj()
    chunk_size = 64 * 1024
    produced = 0
    kept = bytearray()
    for offset in range(0, len(data), chunk_size):
        pending = data[offset : offset + chunk_size]
        while pending:
            capacity = min(chunk_size, output_limit - produced + 1) if output_limit else chunk_size
            decoded = decoder.decompress(pending, capacity)
            produced += len(decoded)
            if output_limit and produced > output_limit:
                raise ValueError("PDF content exceeds the configured Flate output limit")  # noqa: TRY003
            if keep:
                kept.extend(decoded)
            if decoder.eof:
                # pypdf accepts padding after a complete zlib stream.
                return bytes(kept)
            tail = decoder.unconsumed_tail
            if len(tail) == len(pending) and not decoded:
                raise ValueError("PDF Flate decoder made no progress")  # noqa: TRY003
            pending = tail
    raise ValueError("Incomplete PDF Flate stream")  # noqa: TRY003


# Chain stages whose decoded output is deterministic enough to replay the next
# stage's input; anything else stops the replay and keeps pypdf behavior.
_CHAIN_PASSTHROUGH = {
    "/ASCIIHexDecode": ASCIIHexDecode,
    "/AHx": ASCIIHexDecode,
    "/ASCII85Decode": ASCII85Decode,
    "/A85": ASCII85Decode,
}
_XOBJECT_VISIT_LIMIT = 256


def _validate_encoded_stream(stream: EncodedStreamObject, number: int, label: str) -> None:
    """Replay decodable filter stages strictly, before pypdf's lossy recovery."""

    filters = stream.get("/Filter")
    if filters is None:
        return
    filters = filters.get_object()
    stages = list(filters) if isinstance(filters, ArrayObject) else [filters]
    decode_parms = stream.get("/DecodeParms", (DictionaryObject(),) * len(stages))
    if not isinstance(decode_parms, (list, tuple)):
        decode_parms = (decode_parms,)
    data = stream._data
    try:
        # pypdf's decode_stream_data pairs stages with plain zip truncation;
        # keep that exact semantics instead of rejecting short DecodeParms.
        for index, (stage, params) in enumerate(zip(stages, decode_parms, strict=False)):
            name = stage.get_object()
            final = index == len(stages) - 1
            if name in ("/FlateDecode", "/Fl"):
                _decode_flate(data, get_configuration().zlib_maximum_output_length, keep=not final)
                if final:
                    return
                if isinstance(params, IndirectObject):
                    params = params.get_object()
                # Strict validation already passed, so this cannot hit the lossy path.
                data = FlateDecode.decode(data, params if isinstance(params, DictionaryObject) else None)
            elif not final and name in _CHAIN_PASSTHROUGH:
                data = _CHAIN_PASSTHROUGH[name].decode(data)
            else:
                # Filters we cannot replay deterministically keep pypdf behavior.
                return
    except (ValueError, zlib.error) as error:
        raise ValueError(f"Invalid Flate {label} on physical page {number}") from error  # noqa: TRY003


def _validate_form_xobjects(resources: object, number: int, seen: set[int]) -> None:
    # Text-bearing Forms: pypdf tolerates damaged Flate data there with only a
    # logged warning, so integrity is checked wherever extraction will read.
    if isinstance(resources, IndirectObject):
        resources = resources.get_object()
    if not isinstance(resources, DictionaryObject) or len(seen) >= _XOBJECT_VISIT_LIMIT:
        return
    xobjects = resources.get("/XObject")
    if xobjects is None:
        return
    xobjects = xobjects.get_object()
    if not isinstance(xobjects, DictionaryObject):
        return
    for reference in xobjects.values():
        if len(seen) >= _XOBJECT_VISIT_LIMIT:
            return
        stream = reference.get_object()
        marker = id(stream)
        if marker in seen:
            continue
        seen.add(marker)
        if not isinstance(stream, DictionaryObject) or stream.get("/Subtype") == "/Image":
            continue
        if isinstance(stream, EncodedStreamObject):
            _validate_encoded_stream(stream, number, "XObject stream")
        _validate_form_xobjects(stream.get("/Resources"), number, seen)


def _validate_page_flate(page: PageObject, number: int) -> None:
    contents = page.get("/Contents")
    if contents is not None:
        contents = contents.get_object()
        streams = contents if isinstance(contents, ArrayObject) else (contents,)
        for reference in streams:
            stream = reference.get_object()
            if isinstance(stream, EncodedStreamObject):
                _validate_encoded_stream(stream, number, "Contents")
    _validate_form_xobjects(page.get("/Resources"), number, set())


class PDFPageSource(Source):
    """Captured original page, independent of later changes to the input file."""

    document_id: str
    document_sha256: str
    page_number: int
    text: str


class PDFPageSourceAdapter:
    input_class = PDFPageInput
    source_class = PDFPageSource
    name = "pdf-page"

    async def resolve(self, value: PDFPageInput, /) -> PDFPageSource:
        identity = json.dumps(
            (value.document_id, value.document_sha256, value.page_number),
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        return PDFPageSource(
            name=f"pdf:{hashlib.sha256(identity).hexdigest()}",
            materialization=SourceMaterialization.CAPTURED,
            **value.model_dump(),
        )

    async def read(self, source: PDFPageSource, /) -> PDFPageInput:
        return PDFPageInput(**{key: getattr(source, key) for key in PDFPageInput.model_fields})


class PDFPageTextEvidenceProjection:
    name = TEXT_EVIDENCE_PROJECTION_KEY.name
    version = TEXT_EVIDENCE_PROJECTION_KEY.version
    source_class = PDFPageSource
    output_class: type[BaseModel] = TextEvidence

    def project(self, source: PDFPageSource, /) -> TextEvidence:
        return TextEvidence(
            source_type="pdf-page",
            source_id=source.name,
            content=f"{source.document_id}\n{source.text}",
            metadata={
                "document_id": source.document_id,
                "document_sha256": source.document_sha256,
                "page_number": source.page_number,
            },
        )


PDF_PAGE_SOURCE_DEFINITION: SourceDefinition[PDFPageInput, PDFPageSource, PDFPageInput] = AdapterSourceDefinition(
    PDFPageSourceAdapter(), projections=(PDFPageTextEvidenceProjection(),)
)


_STOP_WORDS = frozenset(
    "a an the and or of to in on at by for from with as is are was were be been being "  # noqa: SIM905
    "do does did has have had what which who when where why how much many "
    "can could would should will please tell me about according report annual "
    "company fiscal year years during between compared calculate its it this that these those "
    "s d analyst find figure alongside show shows shown using use details assuming perspective "
    "respond response answer based value number amount state explicitly outlined quantity "
    "give provide reported reporting versus vs".split()
)


def _terms(text: str) -> list[str]:
    # CJK bigrams allow natural questions without requiring a segmenter dependency.
    result = re.findall(r"[a-z]+|\d+", text.casefold())
    for run in re.findall(r"[\u3400-\u9fff]+", text):
        result.extend(run[index : index + 2] for index in range(max(1, len(run) - 1)))
    return result


# Query aliases are financial vocabulary, independent of document identity or page.
# They only affect retrieval; captured Source text and exact citations stay original.
_CONCEPT_ALIASES = (
    ("capex", "capital expenditure", "capital expenditures", "capital spending"),
    ("d&a", "depreciation and amortization", "depreciation and amortisation"),
    ("operating income", "operating profit"),
    ("income statement", "statement of income", "earnings statement", "p&l", "profit and loss"),
    ("cash flow statement", "statement of cash flows"),
    ("segment", "segments", "division", "divisions"),
    ("geography", "geographies", "geographic", "footprint", "regions", "countries", "territories"),
    ("lawsuit", "lawsuits", "litigation", "legal proceedings"),
    ("rental", "rentals", "lease", "leases"),
    ("buyback", "buybacks", "repurchase", "repurchases"),
    ("eps", "earnings per share"),
)


def _lexeme(term: str) -> str:
    term = term.replace("isation", "ization")
    if len(term) > 4 and term.endswith("ies"):
        return term[:-3] + "y"
    if len(term) > 4 and term.endswith("s") and not term.endswith(("ss", "us")):
        return term[:-1]
    return term


def _contains_phrase(text: str, phrase: str) -> bool:
    return re.search(r"(?<!\w)" + re.escape(phrase) + r"(?!\w)", text) is not None


def _query_plan(question: str) -> tuple[list[str], dict[str, float], tuple[tuple[str, ...], ...]]:
    normalized = re.sub(r"\bfy(?=\d{4}\b)", "", question.casefold())
    terms = list(dict.fromkeys(term for term in _terms(normalized) if term not in _STOP_WORDS))
    weights = {_lexeme(term): 1.0 for term in terms}
    aliases = tuple(group for group in _CONCEPT_ALIASES if any(_contains_phrase(normalized, alias) for alias in group))
    queries = [question]
    for term in terms:
        queries.append(term)
        stem = _lexeme(term)
        if stem != term:
            queries.append(stem)
    for group in aliases:
        queries.extend(group)
        for alias in group:
            for term in _terms(alias):
                if term not in _STOP_WORDS:
                    weights.setdefault(_lexeme(term), 0.65)
    return list(dict.fromkeys(queries)), weights, aliases


@dataclass(frozen=True)
class _PageCandidate:
    citation: MemoryCitation
    counts: Counter[str]
    text: str


async def _retrieve_pages(memory, queries: list[str], limit: int) -> list[_PageCandidate]:
    candidates = {}
    for query in queries:
        page = await memory.search(SearchMemoryRequest(query=query, mode="fts", limit=max(80, limit * 5)))
        for hit in page.hits:
            key = (hit.memory_ref.model_dump_json(), hit.entry_id, hit.entry_version_id)
            if key in candidates:
                continue
            citation = MemoryCitation(
                memory_ref=hit.memory_ref, entry_id=hit.entry_id, entry_version_id=hit.entry_version_id
            )
            body = hit.text.partition("\n")[2]
            candidates[key] = _PageCandidate(
                citation, Counter(_lexeme(term) for term in _terms(hit.text)), " ".join(body.casefold().split())
            )
    return list(candidates.values())


def _rank_pages(
    values: list[_PageCandidate],
    weights: dict[str, float],
    aliases: tuple[tuple[str, ...], ...],
    question: str,
) -> list[_PageCandidate]:
    average_length = sum(sum(item.counts.values()) for item in values) / len(values) or 1
    frequencies = Counter(term for item in values for term in weights if term in item.counts)
    original_terms = [term for term in _terms(question) if term not in _STOP_WORDS]

    def score(item: _PageCandidate) -> float:
        length = sum(item.counts.values())
        total = 0.0
        for term, weight in weights.items():
            frequency = item.counts[term]
            if not frequency:
                continue
            inverse_frequency = math.log(1 + (len(values) - frequencies[term] + 0.5) / (frequencies[term] + 0.5))
            if term.isdigit():
                weight *= 0.25
            total += (
                weight
                * inverse_frequency
                * frequency
                * 2.2
                / (frequency + 1.2 * (0.25 + 0.75 * length / average_length))
            )
        total += sum(0.35 for left, right in pairwise(original_terms) if f"{left} {right}" in item.text)
        for group in aliases:
            total += max(
                (1.0 + 0.4 * (" " in alias) for alias in group if _contains_phrase(item.text, alias)), default=0
            )
            # A statement title near the start is stronger evidence than a passing
            # reference to that statement in a long note or policy discussion.
            total += max((5.0 for alias in group if _contains_phrase(item.text[:256], alias)), default=0)
        return total

    return sorted(values, key=lambda item: (-score(item), item.citation.entry_id))


async def _select_pages(
    memory, ranked: list[_PageCandidate], weights: dict[str, float], limit: int
) -> tuple[MemoryCitation, ...]:
    selected, seen = [], set()
    for item in ranked:
        if not weights.keys() & item.counts.keys():
            continue
        record = await memory.get(GetMemoryEntryRequest(citation=item.citation))
        if len(record.entry.sources) != 1 or record.entry.sources[0].source_type != "pdf-page":
            continue
        source_id = record.entry.sources[0].source_id
        if source_id in seen:
            continue
        seen.add(source_id)
        selected.append(item.citation)
        if len(selected) == limit:
            break
    return tuple(selected)


async def search_pdf_pages(memory, question: str, limit: int = 10) -> tuple[MemoryCitation, ...]:
    """Retrieve scoped Memory evidence and rank pages with financial query aliases.

    Single-term routes retain partial evidence for multi-page questions. Candidate
    text comes from public search hits; selected pages undergo exact get and Source
    validation. No Source text is rewritten or external PDF opened during search.
    """

    if limit <= 0 or not question.strip():
        return ()
    queries, weights, aliases = _query_plan(question)
    if not weights:
        return ()
    candidates = await _retrieve_pages(memory, queries, limit)
    if not candidates:
        return ()
    ranked = _rank_pages(candidates, weights, aliases, question)
    return await _select_pages(memory, ranked, weights, limit)
