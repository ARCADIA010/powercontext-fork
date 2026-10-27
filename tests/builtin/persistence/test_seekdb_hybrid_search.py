"""Real embedded seekdb regression; uses only deterministic local embeddings."""

from __future__ import annotations

from contextlib import contextmanager
from time import perf_counter

import pytest
from sqlalchemy import Engine, event

from powercontext.builtin.artifacts.memory import EmbeddingProfile, MemoryEntryInput, MemorySearchRequest
from powercontext.builtin.inference import EmbeddingResult
from powercontext.builtin.persistence.memory_index import CompositeMemoryIndex
from powercontext.builtin.persistence.oceanbase.memory_index import OceanBaseMemoryFTSIndex, OceanBaseMemoryVectorIndex
from powercontext.builtin.persistence.seekdb import SeekDBConfig
from powercontext.builtin.persistence.seekdb.memory_index import SeekDBMemoryIndex
from powercontext.builtin.runtime import BuiltinConfig, open_builtin_contexts
from powercontext.builtin.tags import MemoryEntryTagTarget, TagFilter

PROFILE = EmbeddingProfile(profile_id="local-hybrid", model="local", dimension=3, distance="l2", normalization="unit")


class LocalEmbedding:
    profile = PROFILE

    async def embed(self, texts: tuple[str, ...], /) -> EmbeddingResult:
        return EmbeddingResult(vectors=tuple(
            (1.0, 0.0, 0.0) if "alpha" in value.lower() else (0.0, 1.0, 0.0) for value in texts
        ))


@contextmanager
def statements():
    captured = []

    def record(_connection, _cursor, statement, parameters, _context, _executemany):
        captured.append((statement, parameters))

    event.listen(Engine, "before_cursor_execute", record)
    try:
        yield captured
    finally:
        event.remove(Engine, "before_cursor_execute", record)


def identity(hit):
    return (hit.memory_ref.artifact_id, hit.memory_ref.revision,
            hit.entry_id, hit.entry_version_id, hit.text, hit.distance)


@pytest.mark.asyncio
async def test_real_hybrid_filtered_channels_and_plan(tmp_path):
    async with open_builtin_contexts(
        BuiltinConfig(database=SeekDBConfig(path=tmp_path / "db")), embedding_model=LocalEmbedding()
    ) as contexts:
        scope = "hybrid-local-' OR 1=1 --"
        memory = (await contexts.get(scope)).artifacts.memory
        head = await memory.remember(memory=None, entries=(
            MemoryEntryInput(kind="fact", text="Alpha lexical and semantic evidence."),
            MemoryEntryInput(kind="fact", text="Beta evidence without the query token."),
        ), mode="append")
        head = await memory.remember(memory=head, entries=(
            MemoryEntryInput(kind="fact", text="More unchanged-head evidence."),
        ), mode="append")
        other = (await contexts.get("other-hybrid-local")).artifacts.memory
        await other.remember(memory=None, entries=(
            MemoryEntryInput(kind="fact", text="Alpha decoy in another scope."),
        ), mode="append")
        entries = await memory.entries(head)
        alpha = next(entry for entry in entries if entry.text.startswith("Alpha"))
        selected = "chosen' OR 1=1 --"
        target = MemoryEntryTagTarget(artifact_id=head.artifact_id, entry_id=alpha.entry_id)
        tags = await contexts.records.get_tags(scope, target)
        await contexts.records.replace_tags(scope, target, (selected, "second"), expected_etag=tags.etag)
        fts, vector = OceanBaseMemoryFTSIndex(), OceanBaseMemoryVectorIndex(PROFILE)
        unified = SeekDBMemoryIndex(fts, vector)
        baseline = CompositeMemoryIndex(fts, vector)
        for tag_filter in (None, TagFilter(tags=(selected,)), TagFilter(tags=(selected, "second"), match="all"),
                           TagFilter(tags=(selected, "absent"), match="any"),
                           TagFilter(tags=(selected, "absent"), match="all")):
            for candidate_limit in (1, 10):
                request = MemorySearchRequest(query="alpha", analyzed_query="alpha", memories=(head.as_ref(),),
                    candidate_limit=candidate_limit, mode="hybrid", query_vector=(1.0, 0.0, 0.0),
                    embedding_profile=PROFILE, tag_filter=tag_filter)
                async with contexts.database.transaction() as connection:
                    expected = await baseline.search(connection, scope, request)
                    with statements() as captured:
                        actual = await unified.search(connection, scope, request)
                    candidates = [(sql, params) for sql, params in captured
                                  if "AGAINST" in sql.upper() or "l2_distance" in sql.lower()]
                    assert len(candidates) == 1
                    assert [identity(hit) for hit in actual.fts] == [identity(hit) for hit in expected.fts]
                    assert {identity(hit) for hit in actual.vector} == {identity(hit) for hit in expected.vector}
                    assert len(actual.fts) <= candidate_limit and len(actual.vector) <= candidate_limit
                    assert all(hit.memory_ref == head.as_ref() for hit in actual.fts + actual.vector)
                    sql, params = candidates[0]
                    assert scope not in sql and selected not in sql
                    if candidate_limit == 1 and (tag_filter is None or len(tag_filter.tags) == 1):
                        # EXPLAIN prefixes the captured, parameterized product statement;
                        # values remain in the original separate driver parameter tuple.
                        plan = "\n".join(str(row[0]) for row in (
                            await connection.exec_driver_sql("EXPLAIN " + sql, params)
                        ).all())
                        print("\nTAGGED=" + str(tag_filter is not None) + "\n" + plan)
                        assert "TEXT RETRIEVAL SCAN" in plan and "VECTOR INDEX" in plan
        for mode in ("fts", "vector", "hybrid"):
            result = await memory.search("alpha", memories=(head,), mode=mode, limit=1)
            assert result.hits[0].entry_id == alpha.entry_id
            assert result.hits[0].text == alpha.text
        await memory.search("alpha", memories=(head,), mode="hybrid", limit=1)
        latencies = []
        for _ in range(5):
            start = perf_counter()
            result = await memory.search("alpha", memories=(head,), mode="hybrid", limit=1)
            latencies.append((perf_counter() - start) * 1000)
            assert result.hits[0].matched_by == ("fts", "vector")
        print("LOCAL_ONLY_FULL_SEARCH_MS=" + str(sorted(latencies)))
