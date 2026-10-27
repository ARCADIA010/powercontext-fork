# PDF contract fix validation

Base: d58522a84403e78c95628a5aa4538b1d9f1a148f. Worktree: /root/projects/pc-pdf-contract-fix; branch feat/pdf-contract-fix. No official tests, dependencies, Memory limit or page text changed.

## Direct Contents Flate integrity

The parser validates direct Flate Contents after reader-local decryption, before pypdf recovery. It checks framing/checksum and EOF, discards bounded output chunks, respects the existing output limit, and fails atomically with ValueError. No global patches, warning interception or configuration writes. Complete streams with trailing bytes remain valid.

Before fix: 7 failed, 9 passed in 1.43s. Six damaged later-page cases (header, truncation, checksum; single stream and Contents arrays) failed with DID NOT RAISE ValueError; the concurrent mix also accepted damaged documents. After fix: 32 passed in 12.52s (19 new and 13 existing). Coverage includes blank Contents variants, empty-password permissions, /Fl warnings, trailing bytes, parallel readers and chunked resource limits.

All 503 pages matched the base parser in every PDFPageInput field on six full parses. Alternating local times in seconds: base [6.454619, 6.699151, 6.760674], fixed [6.951628, 6.229601, 5.443331]. Medians 6.699151 / 6.229601: the negative difference reflects noise, not a speedup claim. Standalone preflight took 0.027327s with pages already resolved on a fresh reader; page resolution is excluded.

Tests use the existing competition interpreter with PYTHONDONTWRITEBYTECODE=1, PYTEST_DISABLE_PLUGIN_AUTOLOAD=1, PYTHONPATH=<worktree>/src:/root/projects/powercontext/competition, pytest -q -c competition/pytest.ini --confcutdir=tests/pdf_retrieval and independent basetemp under .artifacts/pdf-contract-fix. Raw logs there: h1-before.log, h1-after.log, parse-measurements.log, parse_measurements.json. Reproduce measurements with tests/pdf_retrieval/measure_pdf_preflight.py.

Focused Ruff check/format, ty with explicit shared interpreter, and git diff --check passed. No dependency sync.

Limits: only direct Contents Flate streams including single-filter arrays. Filter chains, Forms, font/ToUnicode, object/xref streams and other warning-based omissions retain prior behavior. Private EncodedStreamObject._data access is isolated and tested with pypdf 6.19.0. Platform quality/resource gates and the unique cause of 0/50 remain unknown.

## Document identity and final acceptance

H3 changes only candidate term counts from body to complete hit.text. Phrase/title text remains body-only; no vocabulary, weights, Memory records or SQL changed. The five-document/21-page counterexample, arbitrary renaming and swapped target assignment failed before the change: 3 failed in 10.74s. All retained exact citations and returned the wrong documents. The failure was the assertion that the explicitly named target must be in the top 20. After the one-line change: 35 passed in 18.77s across all PDF tests, including unnamed queries and two-document comparisons. Raw outputs: h3-before.log and h3-after.log under .artifacts/pdf-contract-fix. Identity matching still requires lexical overlap; this does not guarantee official Recall@20.

Final official command, run from the worktree root:

    PYTEST_ADDOPTS=--basetemp=/root/projects/pc-pdf-contract-fix/.artifacts/pdf-contract-fix/official-tmp make -C competition PYTHON=/root/projects/powercontext/competition/.venv/bin/python test-pdf test-csv test-pi test-rerank test-evidence test-incremental test-count-trigger test-embedding test-lifecycle test-hybrid

Actual output, in command order (exit 0):

    PDF:           6 passed in 33.26s
    CSV:          17 passed in 2.70s
    Pi:           15 passed in 6.36s
    Rerank:       10 passed in 1.75s
    Evidence:      3 passed in 2.69s
    Incremental:  14 passed in 1.69s
    Count-trigger:17 passed in 262.62s (0:04:22)
    Embedding:     8 passed in 2.85s
    Lifecycle:     4 passed in 10.56s
    Hybrid:        1 passed in 34.14s

The Makefile exports PYTHONDONTWRITEBYTECODE=1 and uses this worktree as PRODUCT. Shared dependencies were read only; the official tests were not edited. Raw command output: .artifacts/pdf-contract-fix/official.log. No external SQL was introduced; all tests use public Runtime/Memory APIs.

H2 remains unchanged, including the 8192-byte durable Memory limit. Complex filters, Forms and font streams remain outside preflight coverage. Public acceptance and these regressions do not establish the platform's unique failure cause or a score of 50/50.
