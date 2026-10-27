"""Capture complete physical PDF pages and retrieve exact scoped Memory evidence offline."""

from __future__ import annotations

import hashlib
import io
import json
import math
import re
from collections import Counter

from pydantic import BaseModel, Field, field_validator
from pypdf import PdfReader

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
            raise ValueError("document_id must not be empty or blank")
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
            raise ValueError("PDF requires a nonempty password")
        digest = hashlib.sha256(value.content).hexdigest()
        return tuple(
            PDFPageInput(
                document_id=value.document_id,
                document_sha256=digest,
                page_number=number,
                text=page.extract_text() or "",
            )
            for number, page in enumerate(reader.pages, 1)
        )
    except Exception as error:
        # pypdf exposes malformed structures through several exception families,
        # including failures encountered only while decoding a later page stream.
        raise ValueError("Invalid or incompletely readable PDF document") from error


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
    "a an the and or of to in on at by for from with as is are was were be been being "
    "do does did has have had what which who when where why how much many "
    "can could would should will please tell me about according report annual "
    "company fiscal year years during between compared calculate its it this that these those".split()
)


def _terms(text: str) -> list[str]:
    # CJK bigrams allow natural questions without requiring a segmenter dependency.
    result = re.findall(r"[a-z]+|\d+", text.casefold())
    for run in re.findall(r"[\u3400-\u9fff]+", text):
        result.extend(run[index : index + 2] for index in range(max(1, len(run) - 1)))
    return result


def _lexeme(term: str) -> str:
    if len(term) > 4 and term.endswith("ies"):
        return term[:-3] + "y"
    if len(term) > 4 and term.endswith("s") and not term.endswith(("ss", "us")):
        return term[:-1]
    return term


async def search_pdf_pages(memory, question: str, limit: int = 10) -> tuple[MemoryCitation, ...]:
    """Retrieve through scoped Memory FTS, then rank full pages with lexical BM25.

    Single-term searches avoid the shared long-query admission floor discarding
    pages that provide just one part of a multi-page financial question. No global
    corpus cache is retained, so revisions and independent scopes cannot leak.
    """

    if limit <= 0 or not question.strip():
        return ()
    terms = list(dict.fromkeys(term for term in _terms(question) if term not in _STOP_WORDS))
    if not terms:
        return ()
    queries = [question]
    for term in terms:
        queries.append(term)
        stem = _lexeme(term)
        if stem != term:
            queries.append(stem)
    candidates = {}
    for query in dict.fromkeys(queries):
        page = await memory.search(SearchMemoryRequest(query=query, mode="fts", limit=max(80, limit * 5)))
        for hit in page.hits:
            # ArtifactRef is a mutable Pydantic value; serialize the address for keys.
            key = (hit.memory_ref.model_dump_json(), hit.entry_id, hit.entry_version_id)
            if key in candidates:
                continue
            citation = MemoryCitation(
                memory_ref=hit.memory_ref, entry_id=hit.entry_id, entry_version_id=hit.entry_version_id
            )
            record = await memory.get(GetMemoryEntryRequest(citation=citation))
            references = tuple(ref for ref in record.entry.sources if ref.source_type == "pdf-page")
            if len(references) != 1 or len(record.entry.sources) != 1:
                continue
            body = record.entry.text.partition("\n")[2]
            counts = Counter(_lexeme(term) for term in _terms(body))
            candidates[key] = (citation, references[0].source_id, counts, body.casefold())
    if not candidates:
        return ()
    query_terms = set(_lexeme(term) for term in terms)
    values = list(candidates.values())
    average_length = sum(sum(item[2].values()) for item in values) / len(values) or 1
    frequencies = Counter(term for item in values for term in query_terms if term in item[2])

    def score(item):
        counts = item[2]
        length = sum(counts.values())
        total = 0.0
        for term in query_terms:
            frequency = counts[term]
            if not frequency:
                continue
            inverse_frequency = math.log(1 + (len(values) - frequencies[term] + 0.5) / (frequencies[term] + 0.5))
            weight = 0.25 if term.isdigit() else 1.0
            total += weight * inverse_frequency * frequency * 2.2 / (
                frequency + 1.2 * (0.25 + 0.75 * length / average_length)
            )
        # Adjacent content terms reward financial concepts over scattered mentions.
        total += sum(0.35 for left, right in zip(terms, terms[1:]) if f"{left} {right}" in item[3])
        return total

    ranked = sorted(values, key=lambda item: (-score(item), item[1]))
    selected, seen = [], set()
    for citation, source_id, counts, _ in ranked:
        if source_id in seen or not query_terms.intersection(counts):
            continue
        seen.add(source_id)
        selected.append(citation)
        if len(selected) == limit:
            break
    return tuple(selected)
