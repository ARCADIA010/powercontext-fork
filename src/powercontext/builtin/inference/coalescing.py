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

"""Share pending embedding work and cache successful vectors within one event loop."""

from __future__ import annotations

import asyncio
import math
from collections import OrderedDict
from collections.abc import Coroutine
from dataclasses import dataclass
from typing import Any

from powercontext.builtin.inference.models import EmbeddingResult, EmbeddingVector, InferenceUsage
from powercontext.builtin.inference.protocols import EmbeddingModel


@dataclass(frozen=True, eq=False)
class _Batch:
    usage: InferenceUsage


_Work = asyncio.Future[tuple[EmbeddingVector, _Batch]]


def _observe_failure(future: asyncio.Future[Any]) -> None:
    # Work can outlive every waiter; still retrieve failures to avoid orphaned exceptions.
    if not future.cancelled():
        future.exception()


def _sum_tokens(values: tuple[int | None, ...]) -> int | None:
    reported = tuple(value for value in values if value is not None)
    return sum(reported) if reported else None


class CoalescingEmbeddingModel:
    """Deduplicate exact texts and attribute each awaited batch to every caller."""

    def __init__(
        self,
        model: EmbeddingModel,
        *,
        max_batch_size: int = 64,
        coalesce_ms: float = 5.0,
        cache_size: int = 1024,
    ) -> None:
        if type(max_batch_size) is not int or max_batch_size < 1:
            raise ValueError("max_batch_size must be a positive integer")  # noqa: TRY003
        if type(cache_size) is not int or cache_size < 0:
            raise ValueError("cache_size must be a non-negative integer")  # noqa: TRY003
        if not math.isfinite(coalesce_ms) or coalesce_ms < 0:
            raise ValueError("coalesce_ms must be finite and non-negative")  # noqa: TRY003
        self.profile = model.profile
        self._model = model
        self._max_batch_size = max_batch_size
        self._delay = coalesce_ms / 1000
        self._cache_size = cache_size
        self._cache: OrderedDict[str, EmbeddingVector] = OrderedDict()
        self._inflight: dict[str, _Work] = {}
        self._pending: dict[str, _Work] = {}
        self._dispatcher: asyncio.Task[None] | None = None
        self._tasks: set[asyncio.Task[None]] = set()

    def _spawn(self, coroutine: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
        task = asyncio.create_task(coroutine)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        task.add_done_callback(_observe_failure)
        return task

    async def embed(self, texts: tuple[str, ...], /) -> EmbeddingResult:
        """Restore input order, charging only distinct batches this call awaited."""

        loop = asyncio.get_running_loop()
        cached: dict[str, EmbeddingVector] = {}
        waiting: dict[str, _Work] = {}
        for text in texts:
            if text in cached or text in waiting:
                continue
            if text in self._cache:
                cached[text] = self._cache[text]
                self._cache.move_to_end(text)
                continue
            future = self._inflight.get(text)
            if future is None:
                future = loop.create_future()
                future.add_done_callback(_observe_failure)
                self._inflight[text] = future
                self._pending[text] = future
            waiting[text] = future
        if self._pending and self._dispatcher is None:
            self._dispatcher = self._spawn(self._dispatch())
        results = await asyncio.gather(*(asyncio.shield(future) for future in waiting.values()))
        batches: dict[_Batch, None] = {}
        for text, (vector, batch) in zip(waiting, results, strict=True):
            cached[text] = vector
            batches[batch] = None
        return EmbeddingResult(
            vectors=tuple(cached[text] for text in texts),
            usage=InferenceUsage(
                requests=sum(batch.usage.requests for batch in batches),
                input_tokens=_sum_tokens(tuple(batch.usage.input_tokens for batch in batches)),
                output_tokens=_sum_tokens(tuple(batch.usage.output_tokens for batch in batches)),
            ),
        )

    async def _dispatch(self) -> None:
        # Even a zero window yields so concurrent callers can register before a synchronous provider runs.
        await asyncio.sleep(self._delay)
        pending = tuple(self._pending.items())
        self._pending.clear()
        self._dispatcher = None
        for start in range(0, len(pending), self._max_batch_size):
            self._spawn(self._execute(pending[start : start + self._max_batch_size]))

    async def _execute(self, work: tuple[tuple[str, _Work], ...]) -> None:
        try:
            result = await self._model.embed(tuple(text for text, _ in work))
            if len(result.vectors) != len(work):
                raise ValueError("embedding vector count must match the batch text count")  # noqa: TRY003, TRY301
        except (Exception, asyncio.CancelledError) as error:
            for text, future in work:
                del self._inflight[text]
                future.set_exception(error)
            return
        batch = _Batch(result.usage)
        for (text, future), vector in zip(work, result.vectors, strict=True):
            if self._cache_size:
                self._cache[text] = vector
                self._cache.move_to_end(text)
                while len(self._cache) > self._cache_size:
                    self._cache.popitem(last=False)
            del self._inflight[text]
            future.set_result((vector, batch))
