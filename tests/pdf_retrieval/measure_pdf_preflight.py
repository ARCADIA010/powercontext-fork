import importlib.util
import io
import json
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path

from pypdf import PdfReader

from powercontext.sources import pdf_pages as current

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / ".artifacts/pdf-contract-fix"
OUT.mkdir(parents=True, exist_ok=True)
BASE = "d58522a84403e78c95628a5aa4538b1d9f1a148f"
source = subprocess.check_output([
    shutil.which("git") or "/usr/bin/git",
    "-C",
    str(ROOT),
    "show",
    BASE + ":src/powercontext/sources/pdf_pages.py",
])
path = OUT / "baseline_pdf_pages.py"
path.write_bytes(source)
spec = importlib.util.spec_from_file_location("baseline_pdf_pages", path)
baseline = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = baseline
spec.loader.exec_module(baseline)
raw = (ROOT / "competition/challenges/source-03-pdf-retrieval/workload/public/pdfs/PEPSICO_2022_10K.pdf").read_bytes()
times = {"baseline": [], "preflight": []}
reference = None
for repeat in range(3):
    for name, module in (
        [("baseline", baseline), ("preflight", current)]
        if repeat % 2 == 0
        else [("preflight", current), ("baseline", baseline)]
    ):
        start = time.perf_counter()
        pages = module.parse_pdf_document(module.PDFDocumentInput(document_id="PEPSICO_2022_10K", content=raw))
        times[name].append(time.perf_counter() - start)
        records = [page.model_dump() for page in pages]
        if reference is None:
            reference = records
        assert records == reference, (repeat, name)
reader = PdfReader(io.BytesIO(raw), strict=True)
pages = tuple(reader.pages)
start = time.perf_counter()
for number, page in enumerate(pages, 1):
    current._validate_page_flate(page, number)
preflight_only = time.perf_counter() - start
medians = {name: statistics.median(samples) for name, samples in times.items()}
result = {
    "base_sha": BASE,
    "pages": len(reference),
    "all_fields_equal_on_all_six_parses": True,
    "seconds": times,
    "median_seconds": medians,
    "median_delta_seconds": medians["preflight"] - medians["baseline"],
    "preflight_only_seconds_fresh_reader_pages_already_resolved": preflight_only,
    "note": "Local paired samples; page resolution excluded from standalone preflight, not a platform resource guarantee.",
}
(OUT / "parse_measurements.json").write_text(json.dumps(result, indent=2) + chr(10))
print(json.dumps(result, indent=2), flush=True)
