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

from __future__ import annotations

import asyncio
import gc
from collections import Counter

import pytest

from powercontext.builtin.artifacts.memory import EmbeddingProfile
from powercontext.builtin.inference import EmbeddingResult, InferenceUsage
from powercontext.builtin.inference.coalescing import CoalescingEmbeddingModel
from powercontext.builtin.inference.usage import UsageReportingEmbeddingModel, bind_usage_reporter
from powercontext.builtin.statistics import ModelUsagePurpose


def vector(text: str) -> tuple[float, ...]:
    return (float(len(text)), float(sum(map(ord, text))))


class Provider:
    profile = EmbeddingProfile(profile_id="coalescing-test", model="offline", dimension=2)

    def __init__(self, *, blocked: bool = False) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.fail: set[str] = set()
        self.extra_vectors = 0
        if not blocked:
            self.release.set()

    async def embed(self, texts: tuple[str, ...], /) -> EmbeddingResult:
        self.calls.append(texts)
        self.started.set()
        await self.release.wait()
        if self.fail.intersection(texts):
            raise RuntimeError("provider failure")  # noqa: TRY003
        vectors = tuple(vector(text) for text in texts)
        if self.extra_vectors < 0:
            vectors = vectors[: self.extra_vectors]
        elif self.extra_vectors > 0:
            vectors += (vector("extra"),) * self.extra_vectors
        return EmbeddingResult(
            vectors=vectors,
            usage=InferenceUsage(requests=3, input_tokens=14, output_tokens=0),
        )


async def attached(model, texts):
    entered = asyncio.Event()

    async def call():
        entered.set()
        return await model.embed(texts)

    task = asyncio.create_task(call())
    await entered.wait()
    return task


@pytest.mark.parametrize("cache_size", [0, 8])
def test_late_waiter_shares_running_batch_and_full_usage(cache_size):
    async def scenario():
        base = Provider(blocked=True)
        model = CoalescingEmbeddingModel(base, coalesce_ms=0, cache_size=cache_size)
        assert model.profile == base.profile
        first = asyncio.create_task(model.embed(("alpha", "beta", "alpha")))
        await base.started.wait()
        late = await attached(model, ("beta", "alpha", "beta"))
        base.release.set()
        a, b = await asyncio.wait_for(asyncio.gather(first, late), 2)
        assert base.calls == [("alpha", "beta")]
        assert a.vectors == tuple(map(vector, ("alpha", "beta", "alpha")))
        assert b.vectors == tuple(map(vector, ("beta", "alpha", "beta")))
        assert a.usage == b.usage == InferenceUsage(requests=3, input_tokens=14, output_tokens=0)
        replay = await model.embed(("alpha",))
        assert replay.usage.requests == (0 if cache_size else 3)
        assert len(base.calls) == (1 if cache_size else 2)

    asyncio.run(scenario())


def test_per_caller_batch_dependencies_and_optional_tokens():
    class Metered(Provider):
        async def embed(self, texts, /):
            self.calls.append(texts)
            usage = {
                "a": InferenceUsage(requests=3, input_tokens=14, output_tokens=0),
                "c": InferenceUsage(requests=0, input_tokens=None, output_tokens=None),
                "e": InferenceUsage(requests=2, input_tokens=0, output_tokens=8),
            }[texts[0]]
            return EmbeddingResult(vectors=tuple(map(vector, texts)), usage=usage)

    async def scenario():
        base = Metered()
        model = CoalescingEmbeddingModel(base, max_batch_size=2, coalesce_ms=0)
        a, b, c = await asyncio.gather(
            model.embed(("a", "b", "a", "c", "d", "e")),
            model.embed(("b", "d", "f")),
            model.embed(("c", "d", "c")),
        )
        assert base.calls == [("a", "b"), ("c", "d"), ("e", "f")]
        assert a.usage == b.usage == InferenceUsage(requests=5, input_tokens=14, output_tokens=8)
        assert c.usage == InferenceUsage(requests=0)
        assert c.vectors == tuple(map(vector, ("c", "d", "c")))
        assert (await model.embed(("a", "f"))).usage == InferenceUsage(requests=0)

    asyncio.run(scenario())


def test_mixed_cache_and_inflight_still_charges_waiter():
    async def scenario():
        base = Provider()
        model = CoalescingEmbeddingModel(base, coalesce_ms=0)
        await model.embed(("cached",))
        base.started.clear()
        base.release.clear()
        first = asyncio.create_task(model.embed(("fresh", "other")))
        await base.started.wait()
        mixed = await attached(model, ("cached", "fresh", "fresh"))
        base.release.set()
        a, b = await asyncio.wait_for(asyncio.gather(first, mixed), 2)
        assert a.usage == b.usage == InferenceUsage(requests=3, input_tokens=14, output_tokens=0)
        assert b.vectors == tuple(map(vector, ("cached", "fresh", "fresh")))
        assert (await model.embed(("cached", "fresh"))).usage.requests == 0
        assert base.calls == [("cached",), ("fresh", "other")]

    asyncio.run(scenario())


def test_independent_batch_success_survives_failed_shared_batch():
    async def scenario():
        base = Provider()
        base.fail.add("bad")
        model = CoalescingEmbeddingModel(base, max_batch_size=1, coalesce_ms=0)
        results = await asyncio.wait_for(
            asyncio.gather(
                model.embed(("bad",)),
                model.embed(("good",)),
                model.embed(("bad", "good")),
                return_exceptions=True,
            ),
            2,
        )
        assert isinstance(results[0], RuntimeError)
        assert results[1].vectors == (vector("good"),)
        assert isinstance(results[2], RuntimeError)
        base.fail.clear()
        retry = await model.embed(("bad", "good"))
        assert retry.vectors == (vector("bad"), vector("good"))
        assert retry.usage.requests == 3
        assert Counter(text for batch in base.calls for text in batch) == {"bad": 2, "good": 1}

    asyncio.run(scenario())


@pytest.mark.parametrize("extra", [-1, 1])
def test_invalid_count_cannot_publish_partial_cache(extra):
    async def scenario():
        base = Provider()
        base.extra_vectors = extra
        model = CoalescingEmbeddingModel(base, coalesce_ms=0)
        with pytest.raises(ValueError):
            await model.embed(("first", "second"))
        base.extra_vectors = 0
        result = await model.embed(("second", "first"))
        assert result.vectors == (vector("second"), vector("first"))
        assert base.calls == [("first", "second"), ("second", "first")]

    asyncio.run(scenario())


@pytest.mark.parametrize("cancel_creator", [True, False])
@pytest.mark.parametrize("cache_size", [0, 4])
def test_cancelled_caller_cannot_cancel_shared_or_late_work(cancel_creator, cache_size):
    async def scenario():
        base = Provider(blocked=True)
        model = CoalescingEmbeddingModel(base, coalesce_ms=0, cache_size=cache_size)
        creator = asyncio.create_task(model.embed(("shared",)))
        await base.started.wait()
        late = await attached(model, ("shared",))
        cancelled, survivor = (creator, late) if cancel_creator else (late, creator)
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled
        another = await attached(model, ("shared", "shared"))
        base.release.set()
        a, b = await asyncio.wait_for(asyncio.gather(survivor, another), 2)
        assert a.vectors == (vector("shared"),)
        assert b.vectors == (vector("shared"), vector("shared"))
        assert a.usage == b.usage == InferenceUsage(requests=3, input_tokens=14, output_tokens=0)
        assert base.calls == [("shared",)]

    asyncio.run(scenario())


def test_cancel_all_then_provider_failure_is_observed_and_retryable():
    async def scenario():
        loop = asyncio.get_running_loop()
        unhandled = []
        loop.set_exception_handler(lambda _loop, context: unhandled.append(context))
        base = Provider(blocked=True)
        base.fail.add("bad")
        model = CoalescingEmbeddingModel(base, coalesce_ms=0)
        call = asyncio.create_task(model.embed(("bad",)))
        await base.started.wait()
        call.cancel()
        with pytest.raises(asyncio.CancelledError):
            await call
        base.release.set()
        # A surviving observer sees the same failure, then a later retry starts new work.
        observer = await attached(model, ("bad",))
        with pytest.raises(RuntimeError):
            await observer
        base.fail.clear()
        assert (await model.embed(("bad",))).vectors == (vector("bad"),)
        gc.collect()
        await asyncio.sleep(0)
        assert not unhandled

    asyncio.run(scenario())


def test_lru_hits_refresh_and_exact_text_is_preserved():
    async def scenario():
        base = Provider()
        model = CoalescingEmbeddingModel(base, coalesce_ms=0, cache_size=2)
        await model.embed(("Alpha", " alpha "))
        assert (await model.embed(("Alpha",))).usage.requests == 0
        await model.embed(("alpha",))
        assert (await model.embed(("Alpha",))).usage.requests == 0
        assert (await model.embed((" alpha ",))).usage.requests == 3
        assert base.calls == [("Alpha", " alpha "), ("alpha",), (" alpha ",)]
        assert (await model.embed(())).vectors == ()
        assert (await model.embed(())).usage.requests == 0

    asyncio.run(scenario())


def test_usage_reporting_wrapper_reports_each_shared_waiter():
    async def scenario():
        model = UsageReportingEmbeddingModel(CoalescingEmbeddingModel(Provider(), coalesce_ms=0))
        reported = []

        async def report(purpose, operation, usage):
            reported.append(usage)

        with bind_usage_reporter(report, embedding_purpose=next(iter(ModelUsagePurpose))):
            results = await asyncio.gather(model.embed(("same",)), model.embed(("same", "same")))
        assert reported == [result.usage for result in results]
        assert all(usage == InferenceUsage(requests=3, input_tokens=14, output_tokens=0) for usage in reported)

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "options",
    [
        {"max_batch_size": 0},
        {"max_batch_size": -1},
        {"cache_size": -1},
        {"coalesce_ms": -1},
        {"coalesce_ms": float("nan")},
        {"coalesce_ms": float("inf")},
    ],
)
def test_invalid_configuration(options):
    with pytest.raises(ValueError):
        CoalescingEmbeddingModel(Provider(), **options)


def test_equal_usage_from_different_batches_is_charged_separately():
    async def scenario():
        base = Provider()
        model = CoalescingEmbeddingModel(base, max_batch_size=1, coalesce_ms=0)
        first, second = await asyncio.gather(model.embed(("one", "two", "one")), model.embed(("two",)))
        assert first.usage == InferenceUsage(requests=6, input_tokens=28, output_tokens=0)
        assert second.usage == InferenceUsage(requests=3, input_tokens=14, output_tokens=0)

    asyncio.run(scenario())


def test_orphaned_failed_work_is_cleaned_and_retryable():
    class FinishingProvider(Provider):
        def __init__(self):
            super().__init__(blocked=True)
            self.finished = asyncio.Event()

        async def embed(self, texts, /):
            try:
                return await super().embed(texts)
            finally:
                self.finished.set()

    async def scenario():
        loop = asyncio.get_running_loop()
        unhandled = []
        loop.set_exception_handler(lambda _loop, context: unhandled.append(context))
        base = FinishingProvider()
        base.fail.add("orphan")
        model = CoalescingEmbeddingModel(base, coalesce_ms=0)
        creator = asyncio.create_task(model.embed(("orphan",)))
        await base.started.wait()
        creator.cancel()
        with pytest.raises(asyncio.CancelledError):
            await creator
        base.release.set()
        await base.finished.wait()
        base.fail.clear()
        result = await asyncio.wait_for(model.embed(("orphan",)), 2)
        assert result.vectors == (vector("orphan"),)
        assert base.calls == [("orphan",), ("orphan",)]
        gc.collect()
        await asyncio.sleep(0)
        assert unhandled == []

    asyncio.run(scenario())


def test_queued_creator_cancellation_does_not_remove_shared_work():
    async def scenario():
        base = Provider(blocked=True)
        model = CoalescingEmbeddingModel(base, coalesce_ms=0, cache_size=0)
        creator = await attached(model, ("queued",))
        creator.cancel()
        with pytest.raises(asyncio.CancelledError):
            await creator
        await base.started.wait()
        late = await attached(model, ("queued",))
        base.release.set()
        result = await asyncio.wait_for(late, 2)
        assert result.vectors == (vector("queued"),)
        assert base.calls == [("queued",)]
        assert result.usage == InferenceUsage(requests=3, input_tokens=14, output_tokens=0)

    asyncio.run(scenario())


def test_new_batch_can_finish_while_an_older_shared_batch_is_blocked():
    class IndependentlyBlocked(Provider):
        async def embed(self, texts, /):
            self.calls.append(texts)
            if "first" in texts:
                self.started.set()
                await self.release.wait()
            return EmbeddingResult(
                vectors=tuple(map(vector, texts)),
                usage=InferenceUsage(requests=4 if "first" in texts else 2),
            )

    async def scenario():
        base = IndependentlyBlocked(blocked=True)
        model = CoalescingEmbeddingModel(base, max_batch_size=2, coalesce_ms=0, cache_size=0)
        older = asyncio.create_task(model.embed(("first", "shared")))
        await base.started.wait()
        mixed = await attached(model, ("shared", "new"))
        independent = await asyncio.wait_for(model.embed(("independent",)), 2)
        assert independent.vectors == (vector("independent"),)
        assert independent.usage == InferenceUsage(requests=2)
        base.release.set()
        first, second = await asyncio.wait_for(asyncio.gather(older, mixed), 2)
        assert first.usage == InferenceUsage(requests=4)
        assert second.usage == InferenceUsage(requests=6)
        assert second.vectors == (vector("shared"), vector("new"))
        assert Counter(text for batch in base.calls for text in batch) == {
            "first": 1,
            "shared": 1,
            "new": 1,
            "independent": 1,
        }

    asyncio.run(scenario())
