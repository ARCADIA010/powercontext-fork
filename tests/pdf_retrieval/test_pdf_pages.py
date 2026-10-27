"""Independent contract and non-vacuous retrieval tests using the real scoped Runtime."""

import hashlib
import io
from pathlib import Path

import pytest
from pypdf import PdfWriter
from pypdf.generic import DecodedStreamObject, DictionaryObject, NameObject
from selftest_support.source_acceptance import persist

from powercontext.builtin.runtime import GetMemoryEntryRequest, open_builtin_runtime
from powercontext.builtin.scope import ScopeDraft
from powercontext.sources import SourceDefinitionRegistry, project_source_for_transport
from powercontext.sources.models import SourceMaterialization
from powercontext.sources.pdf_pages import (
    PDF_PAGE_SOURCE_DEFINITION,
    PDFDocumentInput,
    PDFPageInput,
    parse_pdf_document,
    search_pdf_pages,
)
from powercontext.sources.projections import TEXT_EVIDENCE_PROJECTION_KEY


def pdf_bytes(texts, password=None):
    writer = PdfWriter()
    font = DictionaryObject({
        NameObject("/Type"): NameObject("/Font"),
        NameObject("/Subtype"): NameObject("/Type1"),
        NameObject("/BaseFont"): NameObject("/Helvetica"),
    })
    for text in texts:
        page = writer.add_blank_page(width=600, height=800)
        if text is None:
            continue
        page[NameObject("/Resources")] = DictionaryObject({
            NameObject("/Font"): DictionaryObject({NameObject("/F1"): writer._add_object(font)})
        })
        stream = DecodedStreamObject()
        stream.set_data(b"BT /F1 12 Tf 30 700 Td (" + text.encode("ascii") + b") Tj ET")
        page[NameObject("/Contents")] = writer._add_object(stream)
    if password is not None:
        writer.encrypt(user_password=password, owner_password="owner")
    buffer = io.BytesIO()
    writer.write(buffer)
    return buffer.getvalue()


async def test_complete_page_contract_and_long_identity():
    raw = pdf_bytes(["Revenue 123.45 table 2025", None, "Final page 987.65"])
    document_id = " 公司:/" * 500
    pages = parse_pdf_document(PDFDocumentInput(document_id=document_id, content=raw))
    assert [page.page_number for page in pages] == [1, 2, 3]
    assert [page.text for page in pages] == ["Revenue 123.45 table 2025", "", "Final page 987.65"]
    assert all(page.document_sha256 == hashlib.sha256(raw).hexdigest() for page in pages)
    definition = PDF_PAGE_SOURCE_DEFINITION
    assert (definition.name, definition.version, definition.input_class) == ("pdf-page", "1", PDFPageInput)
    registry = SourceDefinitionRegistry((definition,))
    names = set()
    for page in pages:
        source = await registry.resolve(page)
        assert source.materialization == SourceMaterialization.CAPTURED
        assert len(source.name) <= 256
        assert await registry.read(source) == page
        assert (await registry.resolve(page)).name == source.name
        projection = project_source_for_transport(registry, source).projection(TEXT_EVIDENCE_PROJECTION_KEY)
        assert projection == {
            "source_type": "pdf-page", "source_id": source.name,
            "content": f"{document_id}\n{page.text}",
            "metadata": {"document_id": document_id, "document_sha256": page.document_sha256,
                         "page_number": page.page_number},
        }
        names.add(source.name)
    assert len(names) == 3
    revised = parse_pdf_document(PDFDocumentInput(document_id=document_id, content=raw + b"\n"))
    assert (await registry.resolve(revised[0])).name not in names
    other = pages[0].model_copy(update={"document_id": document_id + "x"})
    assert (await registry.resolve(other)).name not in names


@pytest.mark.parametrize("document_id", ["", " ", "\n\t"])
def test_blank_document_identity_rejected(document_id):
    with pytest.raises(ValueError):
        PDFDocumentInput(document_id=document_id, content=b"")
    with pytest.raises(ValueError):
        PDFPageInput(document_id=document_id, document_sha256="a" * 64, page_number=1, text="")


@pytest.mark.parametrize("raw", [b"", b"nonsense", b"%PDF-1.7\ntruncated"])
def test_corruption_rejected(raw):
    with pytest.raises(ValueError):
        parse_pdf_document(PDFDocumentInput(document_id="bad", content=raw))


def test_password_and_late_page_failure_are_atomic():
    assert len(parse_pdf_document(PDFDocumentInput(document_id="ok", content=pdf_bytes([None], "")))) == 1
    with pytest.raises(ValueError):
        parse_pdf_document(PDFDocumentInput(document_id="locked", content=pdf_bytes([None], "secret")))
    raw = pdf_bytes(["First page valid", "Second page invalid"])
    # Keep the xref offsets intact but leave the later literal string unclosed.
    start = raw.index(b"(Second page invalid)")
    raw = raw[:start] + raw[start:].replace(b") Tj ET", b"  Tj ET", 1)
    with pytest.raises(ValueError):
        parse_pdf_document(PDFDocumentInput(document_id="broken-later", content=raw))


async def test_full_page_text_survives_the_durable_entry_bound(tmp_path: Path):
    """A legitimate dense page whose projection exceeds the old 8192-byte bound
    must persist verbatim and read back exactly; nothing is truncated."""

    dense = "quarterly revenue " * 530
    pages = parse_pdf_document(PDFDocumentInput(document_id="dense", content=pdf_bytes([dense])))
    assert pages[0].text == dense
    projection_bytes = len(f"dense\n{pages[0].text}".encode())
    assert projection_bytes > 8192
    registry = SourceDefinitionRegistry((PDF_PAGE_SOURCE_DEFINITION,))
    observations = [project_source_for_transport(registry, await registry.resolve(page)) for page in pages]
    config, scope_id, stored = await persist(tmp_path, PDF_PAGE_SOURCE_DEFINITION, observations)
    async with open_builtin_runtime(config) as runtime:
        memory = runtime.memory.for_scope(scope_id)
        results = await search_pdf_pages(memory, "quarterly revenue", limit=5)
        assert len(results) == 1
        entry = (await memory.get(GetMemoryEntryRequest(citation=results[0]))).entry
        projection = stored[entry.sources[0].source_id].projection(TEXT_EVIDENCE_PROJECTION_KEY)
        assert entry.text == projection["content"].strip()
        assert projection["content"].encode("utf-8") == f"dense\n{dense}".encode()


async def test_restarted_memory_retrieves_ranks_deduplicates_and_isolates(tmp_path: Path):
    registry = SourceDefinitionRegistry((PDF_PAGE_SOURCE_DEFINITION,))
    pages = list(parse_pdf_document(PDFDocumentInput(document_id="synthetic", content=pdf_bytes([
        "Orbital revenue net sales increased to 991 million in 2025.",
        "Orbital revenue net sales regional breakdown North 600 South 391.",
        "Employee pension obligations and retirement benefits total 42.",
        "Weather rainfall temperature humidity forecast.",
        None,
    ]))))
    pages.append(PDFPageInput(document_id="中文财报", document_sha256="b" * 64,
                              page_number=1, text="火星公司营业收入增长，净利润为三百万元。"))
    revision = pages[0].model_copy(update={"document_sha256": "c" * 64, "text": "Revised orbital revenue 1001."})
    pages.append(revision)
    observations = [project_source_for_transport(registry, await registry.resolve(page)) for page in pages]
    config, scope_id, stored = await persist(tmp_path, PDF_PAGE_SOURCE_DEFINITION, observations)
    async with open_builtin_runtime(config) as runtime:
        memory = runtime.memory.for_scope(scope_id)
        results = await search_pdf_pages(memory, "What is the orbital revenue net sales in 2025?", limit=2)
        assert len(results) == 2
        locations = []
        for citation in results:
            entry = (await memory.get(GetMemoryEntryRequest(citation=citation))).entry
            assert len(entry.sources) == 1
            projection = stored[entry.sources[0].source_id].projection(TEXT_EVIDENCE_PROJECTION_KEY)
            assert entry.text == projection["content"].strip()
            locations.append(projection["metadata"]["page_number"])
        assert locations == [1, 2]
        pension = await search_pdf_pages(memory, "employee pension obligations", limit=1)
        assert len(pension) == 1
        assert "pension" in (await memory.get(GetMemoryEntryRequest(citation=pension[0]))).entry.text
        chinese = await search_pdf_pages(memory, "火星公司营业收入是多少？", limit=1)
        assert len(chinese) == 1
        assert "营业收入" in (await memory.get(GetMemoryEntryRequest(citation=chinese[0]))).entry.text
        assert await search_pdf_pages(memory, "orbital", limit=0) == ()
        assert await search_pdf_pages(memory, "  ", limit=10) == ()
        assert await search_pdf_pages(memory, "unfindablequuxxyz", limit=10) == ()
        all_hits = await search_pdf_pages(memory, "revenue", limit=20)
        assert len(all_hits) == 3
        assert len({citation.entry_id for citation in all_hits}) == len(all_hits)
        scope = await runtime.scopes.create(ScopeDraft(title="Empty", summary="Isolation", idempotency_key="empty"))
        assert await search_pdf_pages(runtime.memory.for_scope(scope.scope_id), "revenue") == ()
