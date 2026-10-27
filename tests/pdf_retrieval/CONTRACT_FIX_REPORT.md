# PDF contract fix validation

Base: d58522a84403e78c95628a5aa4538b1d9f1a148f. Worktree: /root/projects/pc-pdf-contract-fix; branch feat/pdf-contract-fix. No official tests, dependencies, Memory limit or page text changed.

## Direct Contents Flate integrity

The parser validates direct Flate Contents after reader-local decryption, before pypdf recovery. It checks framing/checksum and EOF, discards bounded output chunks, respects the existing output limit, and fails atomically with ValueError. No global patches, warning interception or configuration writes. Complete streams with trailing bytes remain valid.

Before fix: 7 failed, 9 passed in 1.43s. Six damaged later-page cases (header, truncation, checksum; single stream and Contents arrays) failed with DID NOT RAISE ValueError; the concurrent mix also accepted damaged documents. After fix: 32 passed in 12.52s (19 new and 13 existing). Coverage includes blank Contents variants, empty-password permissions, /Fl warnings, trailing bytes, parallel readers and chunked resource limits.

All 503 pages matched the base parser in every PDFPageInput field on six full parses. Alternating local times in seconds: base [6.454619, 6.699151, 6.760674], fixed [6.951628, 6.229601, 5.443331]. Medians 6.699151 / 6.229601: the negative difference reflects noise, not a speedup claim. Standalone preflight took 0.027327s with pages already resolved on a fresh reader; page resolution is excluded.

Tests use the existing competition interpreter with PYTHONDONTWRITEBYTECODE=1, PYTEST_DISABLE_PLUGIN_AUTOLOAD=1, PYTHONPATH=<worktree>/src:/root/projects/powercontext/competition, pytest -q -c competition/pytest.ini --confcutdir=tests/pdf_retrieval and independent basetemp under .artifacts/pdf-contract-fix. Raw logs there: h1-before.log, h1-after.log, parse-measurements.log, parse_measurements.json. Reproduce measurements with tests/pdf_retrieval/measure_pdf_preflight.py.

Focused Ruff check/format, ty with explicit shared interpreter, and git diff --check passed. No dependency sync.

Limits: only direct Contents Flate streams including single-filter arrays. Filter chains, Forms, font/ToUnicode, object/xref streams and other warning-based omissions retain prior behavior. Private EncodedStreamObject._data access is isolated and tested with pypdf 6.19.0. Platform quality/resource gates and the unique cause of 0/50 remain unknown.
