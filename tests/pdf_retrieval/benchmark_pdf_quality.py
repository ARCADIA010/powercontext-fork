"""Measure self-authored PDF queries through real scoped Runtime; never official labels.

Run with the competition interpreter and src:competition on PYTHONPATH.
--module-path can load a git-exported baseline pdf_pages.py for paired comparisons.
Temporary pages, traces and a reusable development database stay under .artifacts/.
"""

import argparse
import asyncio
import importlib.util
import json
import sys
import time
from pathlib import Path

from selftest_support.source_acceptance import persist

from powercontext.builtin.persistence.sqlite import SQLiteConfig
from powercontext.builtin.runtime import BuiltinConfig, GetMemoryEntryRequest, open_builtin_runtime
from powercontext.sources import SourceDefinitionRegistry, project_source_for_transport
from powercontext.sources.pdf_pages import PDF_PAGE_SOURCE_DEFINITION, search_pdf_pages

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / ".artifacts/pdf-quality"


class TraceMemory:
    def __init__(self, memory, locations):
        self.memory = memory
        self.locations = locations
        self.routes = []
        self.records = {}
        self.get_calls = 0
        self.text_locations = {
            (p["document_id"] + "\n" + p["text"]).strip(): p["page_number"]
            for p in json.loads((DATA / "baseline-pages.json").read_text())
        }

    async def search(self, request):
        started = time.perf_counter()
        result = await self.memory.search(request)
        self.routes.append({
            "query": request.query,
            "limit": request.limit,
            "hits": len(result.hits),
            "seconds": round(time.perf_counter() - started, 5),
            "results": [
                {"entry_id": h.entry_id, "fts_rrf_score": h.score, "physical_page": self.text_locations.get(h.text)}
                for h in result.hits
            ],
        })
        return result

    async def get(self, request):
        self.get_calls += 1
        result = await self.memory.get(request)
        self.records[result.entry.entry_id] = result.entry
        return result

    def location(self, entry):
        return self.locations[entry.sources[0].source_id]


async def main():  # noqa: C901
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", required=True)
    parser.add_argument("--split", choices=["dev", "heldout", "negative", "all"], default="dev")
    parser.add_argument("--module-path")
    parser.add_argument(
        "--pdf",
        type=Path,
        default=ROOT / "competition/challenges/source-03-pdf-retrieval/workload/public/pdfs/PEPSICO_2022_10K.pdf",
    )
    parser.add_argument("--questions", type=Path, default=Path(__file__).with_name("pdf_quality_questions.json"))
    args = parser.parse_args()
    search = search_pdf_pages
    if args.module_path:
        spec = importlib.util.spec_from_file_location("pdf_quality_measured", args.module_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        search = module.search_pdf_pages
    DATA.mkdir(parents=True, exist_ok=True)
    db = DATA / "corpus"
    db.mkdir(exist_ok=True)
    from powercontext.sources.pdf_pages import PDFDocumentInput, parse_pdf_document

    pages = parse_pdf_document(PDFDocumentInput(document_id=args.pdf.stem, content=args.pdf.read_bytes()))
    (DATA / "baseline-pages.json").write_text(json.dumps([p.model_dump() for p in pages], ensure_ascii=False))
    registry = SourceDefinitionRegistry((PDF_PAGE_SOURCE_DEFINITION,))
    observations = []
    locations = {}
    for page in pages:
        source = await registry.resolve(page)
        locations[source.name] = page.page_number
        observations.append(project_source_for_transport(registry, source))
    state_file = db / "state.json"
    if state_file.exists():
        state = json.loads(state_file.read_text())
        config = BuiltinConfig(database=SQLiteConfig(url=state["database_url"]))
        scope_id = state["scope_id"]
    else:
        config, scope_id, _ = await persist(db, PDF_PAGE_SOURCE_DEFINITION, observations)
        state_file.write_text(json.dumps({"database_url": str(config.database.url), "scope_id": scope_id}))
    questions = json.loads(args.questions.read_text())
    rows = []
    async with open_builtin_runtime(config) as runtime:
        for case in questions:
            if args.split != "all" and case["split"] != args.split:
                continue
            proxy = TraceMemory(runtime.memory.for_scope(scope_id), locations)
            scores = {}

            def profile(frame, event, value, scores=scores):
                if event == "return" and frame.f_code.co_name == "score" and frame.f_code.co_filename.endswith(".py"):
                    item = frame.f_locals.get("item")
                    if item is not None:
                        citation = getattr(item, "citation", None) if not isinstance(item, tuple) else item[0]
                        if citation is not None and hasattr(citation, "entry_id"):
                            scores[citation.entry_id] = value

            started = time.perf_counter()
            sys.setprofile(profile)
            try:
                citations = await search(proxy, case["question"], limit=20)
            finally:
                sys.setprofile(None)
            elapsed = time.perf_counter() - started
            returned = []
            ranked_scores = []
            for citation in citations:
                entry = (await proxy.memory.get(GetMemoryEntryRequest(citation=citation))).entry
                returned.append(proxy.location(entry))
                ranked_scores.append({"page": proxy.location(entry), "lexical_score": scores.get(citation.entry_id)})
            for route in proxy.routes:
                for hit in route["results"]:
                    entry = proxy.records.get(hit["entry_id"])
                    hit["physical_page"] = hit.get("physical_page") if entry is None else proxy.location(entry)
            expected = set(case["evidence_pages"])
            candidate_pages = {
                hit["physical_page"]
                for route in proxy.routes
                for hit in route["results"]
                if hit["physical_page"] is not None
            }
            row = {
                **case,
                "returned_pages": returned,
                "ranked_scores": ranked_scores,
                "candidate_pages": sorted(candidate_pages),
                "candidate_count": len(candidate_pages),
                "candidate_recall": None if not expected else len(expected & candidate_pages) / len(expected),
                "recall_at_20": None if not expected else len(expected & set(returned)) / len(expected),
                "recall_at_5": None if not expected else len(expected & set(returned[:5])) / len(expected),
                "search_calls": len(proxy.routes),
                "get_calls": proxy.get_calls,
                "seconds": round(elapsed, 5),
                "routes": [
                    {
                        **{k: v for k, v in route.items() if k != "results"},
                        "evidence_pages_in_route": sorted({
                            h["physical_page"] for h in route["results"] if h["physical_page"] in expected
                        }),
                        "first_3": route["results"][:3],
                    }
                    for route in proxy.routes
                ],
            }
            rows.append(row)
            print(
                json.dumps(
                    {k: v for k, v in row.items() if k not in {"routes", "candidate_pages"}}, ensure_ascii=False
                ),
                flush=True,
            )
    summary = []
    for split in ["dev", "heldout", "negative"]:
        group = [r for r in rows if r["split"] == split]
        if not group:
            continue
        denominator = sum(len(r["evidence_pages"]) for r in group)
        summary.append({
            "split": split,
            "questions": len(group),
            "evidence_pages": denominator,
            "candidate_recall": None
            if not denominator
            else sum(len(set(r["evidence_pages"]) & set(r["candidate_pages"])) for r in group) / denominator,
            "recall_at_20": None
            if not denominator
            else sum(len(set(r["evidence_pages"]) & set(r["returned_pages"])) for r in group) / denominator,
            "recall_at_5": None
            if not denominator
            else sum(len(set(r["evidence_pages"]) & set(r["returned_pages"][:5])) for r in group) / denominator,
            "multi_page_complete": sum(
                len(r["evidence_pages"]) > 1 and set(r["evidence_pages"]) <= set(r["returned_pages"]) for r in group
            ),
            "empty_results": sum(not r["returned_pages"] for r in group),
            "mean_seconds": sum(r["seconds"] for r in group) / len(group),
        })
    report = {
        "label": args.label,
        "summary": summary,
        "questions": rows,
        "note": "Self-authored questions and physical-page labels. Not official evidence-page Recall@20 or extraction thresholds.",
    }
    (DATA / (args.label + ".json")).write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print("SUMMARY " + json.dumps(summary), flush=True)


asyncio.run(main())
