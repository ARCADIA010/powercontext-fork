"""Document identity participates in retrieval without changing stored evidence."""

from pathlib import Path

import pytest
from selftest_support.source_acceptance import persist

from powercontext.builtin.runtime import GetMemoryEntryRequest, SearchMemoryRequest, open_builtin_runtime
from powercontext.sources import SourceDefinitionRegistry, project_source_for_transport
from powercontext.sources.pdf_pages import PDF_PAGE_SOURCE_DEFINITION, PDFPageInput, search_pdf_pages
from powercontext.sources.projections import TEXT_EVIDENCE_PROJECTION_KEY


@pytest.mark.parametrize(
    ("names", "target_index"),
    [
        (("issueralpha", "issuerbravo", "issuercharlie", "issuerdelta", "issuerecho"), 0),
        (("quartz", "violet", "maple", "cedar", "cobalt"), 0),
        (("quartz", "violet", "maple", "cedar", "cobalt"), 4),
    ],
)
async def test_five_documents_preserve_identity_after_restart(tmp_path: Path, names, target_index):
    registry = SourceDefinitionRegistry((PDF_PAGE_SOURCE_DEFINITION,))
    pages = []
    for index, name in enumerate(names):
        text = "Revenue 120 million. Expenses 90 million. Net profit 30 million."
        if index != target_index:
            text = "Revenue Revenue " + text
        pages.extend(
            PDFPageInput(document_id=name, document_sha256="a" * 64, page_number=n, text=text)
            for n in range(1, 2 if index == target_index else 6)
        )
    observations = [project_source_for_transport(registry, await registry.resolve(page)) for page in pages]
    config, scope, stored = await persist(tmp_path, PDF_PAGE_SOURCE_DEFINITION, observations)
    target = names[target_index]
    comparison = names[(target_index + 1) % len(names)]
    async with open_builtin_runtime(config) as runtime:
        memory = runtime.memory.for_scope(scope)
        route = await memory.search(SearchMemoryRequest(query=target, mode="fts", limit=100))
        assert len(route.hits) == 1

        async def locations(question, limit):
            citations = await search_pdf_pages(memory, question, limit=limit)
            assert len(citations) == limit
            result = []
            for citation in citations:
                entry = (await memory.get(GetMemoryEntryRequest(citation=citation))).entry
                assert len(entry.sources) == 1
                projection = stored[entry.sources[0].source_id].projection(TEXT_EVIDENCE_PROJECTION_KEY)
                assert entry.text == projection["content"].strip()
                metadata = projection["metadata"]
                assert metadata["document_sha256"] == "a" * 64
                result.append((metadata["document_id"], metadata["page_number"]))
            assert len(set(result)) == len(result)
            return result

        generic = await locations("What was revenue?", 20)
        targeted = await locations(f"What was {target} revenue?", 20)
        compared = await locations(f"Compare {target} and {comparison} revenue.", 2)
        assert len({doc for doc, _ in generic}) > 1
        assert (target, 1) in targeted, {"targeted": targeted, "compared": compared}
        assert {doc for doc, _ in compared} == {target, comparison}
