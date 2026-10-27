# Copyright (c) 2026 OceanBase.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Offline lifecycle contracts, native visibility and transactional failure recovery."""

from __future__ import annotations

import asyncio

import pytest
from pydantic import ValidationError

from powercontext.artifacts import ArtifactRef
from powercontext.builtin.artifacts.memory import EmbeddingProfile, MemoryEntryInput
from powercontext.builtin.inference import EmbeddingResult
from powercontext.builtin.persistence.memory import RelationalMemoryBackend
from powercontext.builtin.persistence.sqlite import SQLiteConfig
from powercontext.builtin.records import ArtifactWrite
from powercontext.builtin.runtime import (
    BuiltinConfig,
    CaptureSource,
    ContextAssembly,
    ContextAssemblySection,
    GetMemoryEntryRequest,
    GetTopicMemoryRequest,
    PrepareContextRequest,
    RememberMemoryRequest,
    SearchMemoryRequest,
    SearchTopicMemoryRequest,
    open_builtin_runtime,
)
from powercontext.builtin.runtime.source_lifecycle import (
    SourceLifecycleError,
    SourceLifecycleEvent,
    SourceLifecycleStore,
)
from powercontext.builtin.scope import ScopeDraft


def event(sequence=1, available=False, node_id="source", reason="withdraw"):
    return SourceLifecycleEvent(node_id=node_id, revision=3, sequence=sequence, available=available, reason=reason)


async def topic(runtime, scope, title, detail="detailneedle evidence"):
    created = await runtime.records.for_scope(scope).create_artifact(
        "topic-memory", ArtifactWrite(content={"title": title, "summary": "summaryneedle evidence", "detail": detail})
    )
    return ArtifactRef(family="topic-memory", artifact_id=created.artifact_id, revision=created.revision)


async def build(runtime, key="scope"):
    scope = (await runtime.scopes.create(ScopeDraft(title=key, summary="evidence", idempotency_key=key))).scope_id
    service = runtime.lifecycle.for_scope(scope)
    source = (
        await runtime.sources.for_scope(scope).capture(
            CaptureSource(source_id=f"origin-{key}", content="origin", metadata={})
        )
    ).source_ref
    await service.register_source("source", source, revision=3)
    refs = {"source": source}
    for i in range(3):
        result = await runtime.memory.for_scope(scope).remember(
            RememberMemoryRequest(entries=(MemoryEntryInput(kind="fact", text=f"memoryneedle{i} evidence"),))
        )
        refs[f"m{i}"] = result.entry.citation
        await service.register_memory(f"m{i}", result.entry.citation, parents=("source",))
    refs["topic"] = await topic(runtime, scope, "topicneedle")
    await service.register_topic("topic", refs["topic"], parents=("m0", "m1"))
    return scope, refs


async def assert_visibility(runtime, scope, refs, available):
    service = runtime.lifecycle.for_scope(scope)
    memory = runtime.memory.for_scope(scope)
    page = await memory.list(include_inactive=True)
    assert len(page.entries) == 3
    assert all((entry.state == "active") == available for entry in page.entries)
    for i in range(3):
        ref = refs[f"m{i}"]
        original = await memory.get(GetMemoryEntryRequest(citation=ref))
        assert original.entry.entry_version_id == ref.entry_version_id
        assert original.entry.text == f"memoryneedle{i} evidence"
        assert await service.resolve(f"m{i}", purpose="audit") == ref
        assert bool((await memory.search(SearchMemoryRequest(query=f"memoryneedle{i}", mode="fts"))).hits) == available
    for query in ("topicneedle", "summaryneedle", "detailneedle"):
        page = await runtime.topic_memory.for_scope(scope).search(SearchTopicMemoryRequest(query=query))
        assert bool(page.hits) == available
        prepared = await runtime.context.for_scope(scope).prepare(
            PrepareContextRequest(
                query=query,
                max_bytes=4096,
                assembly=ContextAssembly(sections=(ContextAssemblySection(family="topic-memory", limit=2),)),
            )
        )
        assert ("topicneedle" in (prepared.content or "")) == available
    exact = await runtime.topic_memory.for_scope(scope).get(GetTopicMemoryRequest(artifact=refs["topic"]))
    assert exact.topic.as_ref() == refs["topic"]
    if available:
        assert await service.resolve("topic") == refs["topic"]
    else:
        with pytest.raises(SourceLifecycleError):
            await service.resolve("topic")


@pytest.mark.parametrize(
    ("field", "value"),
    [("node_id", x) for x in ("", "  ", 1, True)]
    + [("reason", x) for x in ("", "\t", 1, False)]
    + [(name, x) for name in ("revision", "sequence") for x in (0, -1, True, 1.0, "1")]
    + [("available", x) for x in (0, 1, "true", None)]
    + [("unexpected", True)],
)
def test_event_strict_validation(field, value):
    payload = {"node_id": "source", "revision": 3, "sequence": 1, "available": True, "reason": "restore"}
    payload[field] = value
    with pytest.raises(ValidationError):
        SourceLifecycleEvent(**payload)


def test_registration_and_event_identity_are_immutable():
    async def run():
        async with open_builtin_runtime(BuiltinConfig(database=SQLiteConfig())) as runtime:
            scope, refs = await build(runtime)
            other_scope, other = await build(runtime, "other")
            service = runtime.lifecycle.for_scope(scope)
            before = (await runtime.memory.for_scope(scope).list()).memory_ref
            await service.register_source("source", refs["source"], revision=3)
            await service.register_memory("m0", refs["m0"], parents=("source",))
            await service.register_topic("topic", refs["topic"], parents=("m0", "m1"))
            assert (await runtime.memory.for_scope(scope).list()).memory_ref == before
            for revision in (True, 3.0, "3", 0, 4):
                with pytest.raises(SourceLifecycleError):
                    await service.register_source("source", refs["source"], revision=revision)
            for parents, policy in (
                ((), "all"),
                (("missing",), "all"),
                (("x",), "any"),
                (("source", "source"), "all"),
                (("source",), "invalid"),
                ("source", "all"),
                ((1,), "all"),
            ):
                with pytest.raises(SourceLifecycleError):
                    await service.register_memory("x", refs["m0"], parents=parents, policy=policy)
            for ref in (other["m0"], refs["source"], ArtifactRef(family="memory", artifact_id="missing", revision=1)):
                with pytest.raises(SourceLifecycleError):
                    await service.register_memory("x", ref, parents=("source",))
            with pytest.raises(SourceLifecycleError):
                await service.register_source("x", other["source"], revision=3)
            with pytest.raises(SourceLifecycleError):
                await service.register_topic("x", other["topic"], parents=("source",))
            with pytest.raises(SourceLifecycleError):
                await service.register_topic("x", refs["m0"].memory_ref, parents=("source",))
            with pytest.raises(SourceLifecycleError):
                await service.register_memory("source", refs["m0"], parents=("m0",))
            with pytest.raises(SourceLifecycleError):
                await service.register_memory("m0", refs["m1"], parents=("source",))
            with pytest.raises(SourceLifecycleError):
                await service.register_topic("topic", refs["topic"], parents=("m1", "m0"))
            with pytest.raises(SourceLifecycleError):
                await service.register_topic("topic", refs["topic"], parents=("m0", "m1"), policy="any")
            await service.apply(event(7))
            after = (await runtime.memory.for_scope(scope).list(include_inactive=True)).memory_ref
            for invalid in (
                event(6),
                event(7, reason="different"),
                event(7, True),
                event(8, node_id="m0"),
                event(8, node_id="missing"),
                event(8).model_copy(update={"revision": 2}),
            ):
                with pytest.raises(SourceLifecycleError):
                    await service.apply(invalid)
            await service.apply(event(7))
            await service.apply(event(20))
            assert (await runtime.memory.for_scope(scope).list(include_inactive=True)).memory_ref == after
            await assert_visibility(runtime, other_scope, other, True)
            with pytest.raises(SourceLifecycleError):
                await service.resolve("missing", purpose="audit")
            with pytest.raises(SourceLifecycleError):
                await service.resolve("source", purpose="unknown")

    asyncio.run(run())


def test_scopes_restart_and_independent_source_sequences(tmp_path):
    async def run():
        config = BuiltinConfig(database=SQLiteConfig(url=f"sqlite+aiosqlite:///{tmp_path / 'scopes.db'}"))
        async with open_builtin_runtime(config) as runtime:
            scope, refs = await build(runtime)
            other_scope, other = await build(runtime, "other")
            service = runtime.lifecycle.for_scope(scope)
            await service.register_source("second", refs["source"], revision=3)
            any_topic = await topic(runtime, scope, "anyneedle", "anyevidence")
            await service.register_topic("any", any_topic, parents=("source", "second"), policy="any")
            await service.apply(event(9))
            assert await service.resolve("any") == any_topic
            await service.apply(event(1, node_id="second"))
            assert await service.resolve("any", purpose="audit") == any_topic
        async with open_builtin_runtime(config) as runtime:
            await assert_visibility(runtime, scope, refs, False)
            await assert_visibility(runtime, other_scope, other, True)
            service = runtime.lifecycle.for_scope(scope)
            before = (await runtime.memory.for_scope(scope).list(include_inactive=True)).memory_ref
            await service.apply(event(1, node_id="second"))
            assert (await runtime.memory.for_scope(scope).list(include_inactive=True)).memory_ref == before
            await service.apply(event(10, True))
            await assert_visibility(runtime, scope, refs, True)

    asyncio.run(run())


class InjectedFailure(RuntimeError):
    pass


@pytest.mark.parametrize("restore", [False, True])
@pytest.mark.parametrize("window", ["before-native", "after-native", "before-completion"])
@pytest.mark.parametrize("restart", [False, True])
def test_native_failure_rolls_back_sequence_and_partial_writes(tmp_path, monkeypatch, restore, window, restart):
    async def run():
        config = BuiltinConfig(database=SQLiteConfig(url=f"sqlite+aiosqlite:///{tmp_path / 'rollback.db'}"))
        async with open_builtin_runtime(config) as runtime:
            scope, refs = await build(runtime)
            service = runtime.lifecycle.for_scope(scope)
            if restore:
                await service.apply(event(1))
            update_event = event(2 if restore else 1, restore)
            before = (await runtime.memory.for_scope(scope).list(include_inactive=True)).memory_ref
            native_commit = RelationalMemoryBackend._commit
            save = SourceLifecycleStore.save
            with monkeypatch.context() as patch:

                async def fail_native(self, connection, value):
                    if window == "before-native":
                        raise InjectedFailure(window)
                    await native_commit(self, connection, value)
                    raise InjectedFailure(window)

                async def fail_completion(self, connection, scope_id, node, *, new=False):
                    if node.kind == "source":
                        raise InjectedFailure(window)
                    await save(self, connection, scope_id, node, new=new)

                if window == "before-completion":
                    patch.setattr(SourceLifecycleStore, "save", fail_completion)
                else:
                    patch.setattr(RelationalMemoryBackend, "_commit", fail_native)
                with pytest.raises(InjectedFailure, match=window):
                    await service.apply(update_event)
            assert (await runtime.memory.for_scope(scope).list(include_inactive=True)).memory_ref == before
            await assert_visibility(runtime, scope, refs, not restore)
            if not restart:
                await service.apply(update_event)
                await assert_visibility(runtime, scope, refs, restore)
        async with open_builtin_runtime(config) as runtime:
            service = runtime.lifecycle.for_scope(scope)
            await service.apply(update_event)
            await assert_visibility(runtime, scope, refs, restore)
            completed = (await runtime.memory.for_scope(scope).list(include_inactive=True)).memory_ref
            await service.apply(update_event)
            await service.apply(event(update_event.sequence + 3, restore))
            assert (await runtime.memory.for_scope(scope).list(include_inactive=True)).memory_ref == completed

    asyncio.run(run())


class _LocalEmbedding:
    profile = EmbeddingProfile(profile_id="lifecycle-test", model="offline", dimension=2)

    async def embed(self, texts, /):
        return EmbeddingResult(vectors=tuple((1.0, 0.0) for _ in texts))


@pytest.mark.parametrize("vector", [False, True])
def test_withdrawn_topics_are_excluded_before_candidate_limit_and_detail_recall(tmp_path, vector):
    async def run():
        config = BuiltinConfig(database=SQLiteConfig(url=f"sqlite+aiosqlite:///{tmp_path / 'topics.db'}"))
        async with open_builtin_runtime(config, embedding_model=_LocalEmbedding() if vector else None) as runtime:
            scope, _refs = await build(runtime)
            service = runtime.lifecycle.for_scope(scope)
            topics = [await topic(runtime, scope, "crowdneedle", "crowddetail evidence") for _ in range(36)]
            topics.sort(key=lambda ref: ref.artifact_id)
            survivor = topics[-1]
            for i, ref in enumerate(topics[:-1]):
                await service.register_topic(f"blocked{i}", ref, parents=("source",))
            await service.apply(event())
            for query in ("crowdneedle", "crowddetail"):
                hits = (
                    await runtime.topic_memory.for_scope(scope).search(SearchTopicMemoryRequest(query=query, limit=1))
                ).hits
                assert [hit.artifact_ref for hit in hits] == [survivor]
            page = await runtime.topic_memory.for_scope(scope).browse(limit=100)
            assert [item.artifact_ref for item in page] == [survivor]
        async with open_builtin_runtime(config, embedding_model=_LocalEmbedding() if vector else None) as runtime:
            service = runtime.lifecycle.for_scope(scope)
            for ref in topics:
                exact = await runtime.topic_memory.for_scope(scope).get(GetTopicMemoryRequest(artifact=ref))
                assert exact.topic.as_ref() == ref
            await service.apply(event(2, True))
            hits = (
                await runtime.topic_memory.for_scope(scope).search(
                    SearchTopicMemoryRequest(query="crowddetail", limit=20)
                )
            ).hits
            assert len(hits) == 20
            assert {hit.artifact_ref.artifact_id for hit in hits} <= {ref.artifact_id for ref in topics}
            page = await runtime.topic_memory.for_scope(scope).browse(limit=100)
            assert {item.artifact_ref.artifact_id for item in page} >= {ref.artifact_id for ref in topics}

    asyncio.run(run())
