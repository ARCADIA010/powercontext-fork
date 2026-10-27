"""Single-statement native Memory candidate retrieval for embedded seekdb."""

from __future__ import annotations

from pyobvector import VECTOR
from sqlalchemy import bindparam, text
from sqlalchemy.ext.asyncio import AsyncConnection

from powercontext.builtin.artifacts.memory import (
    CapabilityNotSupportedError,
    MemorySearchChannels,
    MemorySearchRequest,
)
from powercontext.builtin.artifacts.memory.canonical import validate_embedding
from powercontext.builtin.persistence.memory_index import CompositeMemoryIndex, memory_channel_hits
from powercontext.builtin.persistence.oceanbase.memory_index import OceanBaseMemoryFTSIndex, OceanBaseMemoryVectorIndex
from powercontext.builtin.persistence.tags import memory_tag_parameters

# Each derived table retains its own top-k before UNION ALL. The outer ordering
# preserves channel rank without depending on the execution order of UNION arms.
# Projection revisions are deliberately not predicates: unchanged entries can
# retain an older revision; memory_channel_hits uses the validated requested head.
_HYBRID_SQL = """
SELECT candidates.*, v.text
FROM (
    SELECT f.*, 'fts' AS channel, NULL AS distance FROM (
        SELECT h.memory_artifact_id, h.head_revision, h.entry_id, h.entry_version_id,
               MATCH(h.searchable_text) AGAINST (:query IN NATURAL LANGUAGE MODE) AS channel_score
        FROM pc_memory_entry_heads AS h
        WHERE h.scope_id = :scope_id AND h.memory_artifact_id IN :memory_artifact_ids
          AND MATCH(h.searchable_text) AGAINST (:query IN NATURAL LANGUAGE MODE)
          AND (:filter_tags = 0 OR (
              SELECT COUNT(*) FROM pc_artifact_tags AS tags
              WHERE tags.scope_id = h.scope_id AND tags.family = 'memory'
                AND tags.artifact_id = h.memory_artifact_id AND tags.target_type = 'memory_entry'
                AND tags.target_id = h.entry_id
                AND tags.tag_key_hash IN :tag_hashes AND tags.tag_key IN :tag_keys
          ) >= :tag_minimum)
        ORDER BY channel_score DESC, h.memory_artifact_id, h.entry_id, h.entry_version_id
        LIMIT :candidate_limit
    ) AS f
    UNION ALL
    SELECT a.memory_artifact_id, a.head_revision, a.entry_id, a.entry_version_id,
           -a.distance AS channel_score, 'vector' AS channel, a.distance FROM (
        SELECT m.memory_artifact_id, m.head_revision, m.entry_id, m.entry_version_id,
               l2_distance(m.embedding, :query_vector) AS distance
        FROM pc_memory_vector_entries AS m
        WHERE m.scope_id = :scope_id AND m.memory_artifact_id IN :memory_artifact_ids
          AND (:filter_tags = 0 OR (
              SELECT COUNT(*) FROM pc_artifact_tags AS tags
              WHERE tags.scope_id = m.scope_id AND tags.family = 'memory'
                AND tags.artifact_id = m.memory_artifact_id AND tags.target_type = 'memory_entry'
                AND tags.target_id = m.entry_id
                AND tags.tag_key_hash IN :tag_hashes AND tags.tag_key IN :tag_keys
          ) >= :tag_minimum)
        ORDER BY l2_distance(m.embedding, :query_vector) APPROXIMATE
        LIMIT :candidate_limit
    ) AS a
) AS candidates
JOIN pc_memory_entry_versions AS v
  ON v.scope_id = :scope_id AND v.memory_artifact_id = candidates.memory_artifact_id
 AND v.entry_version_id = candidates.entry_version_id
ORDER BY candidates.channel, candidates.channel_score DESC,
         candidates.memory_artifact_id, candidates.entry_id, candidates.entry_version_id
"""


class SeekDBMemoryIndex(CompositeMemoryIndex):
    """Reuse native index lifecycle and single-channel paths, unify hybrid reads."""

    def __init__(self, fts: OceanBaseMemoryFTSIndex, vector: OceanBaseMemoryVectorIndex) -> None:
        super().__init__(fts, vector)
        self._vector = vector
        self._hybrid_sql = text(_HYBRID_SQL).bindparams(
            bindparam("memory_artifact_ids", expanding=True),
            bindparam("tag_keys", expanding=True),
            bindparam("tag_hashes", expanding=True),
            bindparam("query_vector", type_=VECTOR(vector.profile.dimension)),
        )

    async def search(
        self, connection: AsyncConnection, scope_id: str, request: MemorySearchRequest, /
    ) -> MemorySearchChannels:
        if request.mode != "hybrid":
            return await super().search(connection, scope_id, request)
        if request.embedding_profile != self._vector.profile or request.query_vector is None:
            raise CapabilityNotSupportedError("embedding-profile")
        if not await self.vector_complete(connection, scope_id, request.memories, self._vector.profile):
            raise CapabilityNotSupportedError("vector")
        rows = (
            await connection.execute(
                self._hybrid_sql,
                {
                    "scope_id": scope_id,
                    "memory_artifact_ids": tuple(memory.artifact_id for memory in request.memories),
                    "query": request.analyzed_query,
                    "query_vector": validate_embedding(request.query_vector, dimension=self._vector.profile.dimension),
                    "candidate_limit": request.candidate_limit,
                    "filter_tags": int(request.tag_filter is not None),
                    "tag_keys": ("",),
                    "tag_hashes": (b"",),
                    "tag_minimum": 0,
                    **memory_tag_parameters(request.tag_filter),
                },
            )
        ).mappings().all()
        return MemorySearchChannels(
            fts=memory_channel_hits((row for row in rows if row["channel"] == "fts"), request.memories),
            vector=memory_channel_hits((row for row in rows if row["channel"] == "vector"), request.memories),
        )
