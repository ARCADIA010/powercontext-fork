"""Regression tests for financial language over real scoped Memory and exact Sources."""

from pathlib import Path

import pytest
from selftest_support.source_acceptance import persist

from powercontext.builtin.runtime import GetMemoryEntryRequest, open_builtin_runtime
from powercontext.sources import SourceDefinitionRegistry, project_source_for_transport
from powercontext.sources.pdf_pages import PDF_PAGE_SOURCE_DEFINITION, PDFPageInput, search_pdf_pages
from powercontext.sources.projections import TEXT_EVIDENCE_PROJECTION_KEY


@pytest.mark.parametrize(
    ("question", "expected_pages"),
    [
        ("Where is D&A alongside capex for 2031 and 2030?", {1}),
        ("Where is capital expenditure and amortisation for 2031?", {1}),
        ("Find operating income in the earnings statement alongside D&A in the cash flow statement.", {1, 2}),
    ],
)
async def test_financial_query_aliases_retrieve_exact_pages_after_restart(
    tmp_path: Path, question: str, expected_pages: set[int]
) -> None:
    registry = SourceDefinitionRegistry((PDF_PAGE_SOURCE_DEFINITION,))
    texts = [
        "Consolidated Statement of Cash Flows\n2031 2030\nDepreciation and amortization 287 260\n"
        "Capital spending (625) (580)\nOperating Activities net cash 1400 1300",
        "Consolidated Statement of Income\n2031 2030\nOperating Profit 730 710\nNet revenue 3600 3400",
        *[
            f"Loan schedule number {index}\n2031 2030\nInterest covenants and contractual terms d d d. "
            "Operating cash compensation and earnings information for the analyst."
            for index in range(30)
        ],
    ]
    pages = [
        PDFPageInput(document_id="Example_2031", document_sha256="a" * 64, page_number=n, text=text)
        for n, text in enumerate(texts, 1)
    ]
    observations = [project_source_for_transport(registry, await registry.resolve(page)) for page in pages]
    config, scope_id, stored = await persist(tmp_path, PDF_PAGE_SOURCE_DEFINITION, observations)
    async with open_builtin_runtime(config) as runtime:
        memory = runtime.memory.for_scope(scope_id)
        citations = await search_pdf_pages(memory, question, limit=2)
        assert 0 < len(citations) <= 2
        locations = []
        for citation in citations:
            entry = (await memory.get(GetMemoryEntryRequest(citation=citation))).entry
            assert len(entry.sources) == 1 and entry.sources[0].source_type == "pdf-page"
            projection = stored[entry.sources[0].source_id].projection(TEXT_EVIDENCE_PROJECTION_KEY)
            assert entry.text == projection["content"].strip()
            assert projection["metadata"]["document_sha256"] == "a" * 64
            locations.append(projection["metadata"]["page_number"])
        assert expected_pages <= set(locations)
        assert len(set(locations)) == len(locations)
        assert await search_pdf_pages(memory, "quasistellar neutrino antimatter", limit=20) == ()
        assert await search_pdf_pages(memory, question, limit=0) == ()


async def test_financial_aliases_preserve_scope_isolation(tmp_path: Path) -> None:
    from powercontext.builtin.scope import ScopeDraft

    registry = SourceDefinitionRegistry((PDF_PAGE_SOURCE_DEFINITION,))
    value = PDFPageInput(
        document_id="Example",
        document_sha256="b" * 64,
        page_number=1,
        text="Consolidated Statement of Cash Flows\nCapital spending (625)\nDepreciation and amortization 287",
    )
    observation = project_source_for_transport(registry, await registry.resolve(value))
    config, scope_id, _ = await persist(tmp_path, PDF_PAGE_SOURCE_DEFINITION, [observation])
    async with open_builtin_runtime(config) as runtime:
        occupied = runtime.memory.for_scope(scope_id)
        assert len(await search_pdf_pages(occupied, "D&A and capex", limit=1)) == 1
        other = await runtime.scopes.create(ScopeDraft(title="Empty", summary="Isolated", idempotency_key="empty"))
        assert await search_pdf_pages(runtime.memory.for_scope(other.scope_id), "D&A and capex", limit=1) == ()
