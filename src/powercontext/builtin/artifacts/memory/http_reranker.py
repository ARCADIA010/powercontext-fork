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

"""HTTP scoring with identity-preserving, globally ordered Memory reranking."""

from __future__ import annotations

import math

import httpx
from pydantic import JsonValue

from powercontext.builtin.artifacts.memory.models import MemoryHit
from powercontext.builtin.artifacts.memory.reranking import MemoryRerankDecision, _validate_bounds
from powercontext.builtin.inference import InferenceUsage


class HTTPMemoryReranker:
    """Score bounded batches using a caller-owned client, then select globally."""

    policy_id: str

    def __init__(
        self,
        endpoint: str,
        *,
        client: httpx.AsyncClient,
        batch_size: int = 32,
        policy_id: str = "http-cross-encoder-v1",
    ) -> None:
        if type(batch_size) is not int or batch_size < 1:
            raise ValueError("batch_size must be a positive integer")  # noqa: TRY003
        self._endpoint = endpoint
        self._client = client
        self._batch_size = batch_size
        self.policy_id = policy_id

    async def rerank(
        self,
        query: str,
        candidates: tuple[MemoryHit, ...],
        limit: int,
        /,
    ) -> MemoryRerankDecision:
        """Validate all batch scores before selecting original candidate ranks."""

        if type(limit) is not int:
            raise ValueError("rerank limit must be an integer")  # noqa: TRY003
        _validate_bounds(len(candidates), limit)
        scores: dict[int, int | float] = {}
        requests = 0
        for start in range(0, len(candidates), self._batch_size):
            batch = candidates[start : start + self._batch_size]
            response = await self._client.post(
                self._endpoint,
                json={
                    "query": query,
                    "candidates": [
                        {"rank": rank, "text": candidate.text} for rank, candidate in enumerate(batch, start=start + 1)
                    ],
                },
            )
            requests += 1
            response.raise_for_status()
            scores.update(_validate_scores(response.json(), range(start + 1, start + len(batch) + 1)))
        selected = sorted(scores, key=lambda rank: (-scores[rank], rank))[:limit]
        return MemoryRerankDecision(selected_ranks=tuple(selected), usage=InferenceUsage(requests=requests))


def _validate_scores(payload: JsonValue, expected_ranks: range) -> dict[int, int | float]:
    if not isinstance(payload, dict) or set(payload) != {"scores"}:
        raise ValueError("rerank response must contain exactly scores")  # noqa: TRY003
    records = payload["scores"]
    if not isinstance(records, list):
        raise ValueError("rerank scores must be an array")  # noqa: TRY003, TRY004
    scores: dict[int, int | float] = {}
    for record in records:
        if not isinstance(record, dict) or set(record) != {"rank", "score"}:
            raise ValueError("each rerank score must contain exactly rank and score")  # noqa: TRY003
        rank, score = record["rank"], record["score"]
        if type(rank) is not int or rank not in expected_ranks or rank in scores:
            raise ValueError("rerank response must cover each batch rank exactly once")  # noqa: TRY003
        if not isinstance(score, int | float) or isinstance(score, bool):
            raise ValueError("rerank score must be a finite number")  # noqa: TRY003, TRY004
        if isinstance(score, float) and not math.isfinite(score):
            raise ValueError("rerank score must be a finite number")  # noqa: TRY003
        scores[rank] = score
    if len(scores) != len(expected_ranks):
        raise ValueError("rerank response must cover each batch rank exactly once")  # noqa: TRY003
    return scores
