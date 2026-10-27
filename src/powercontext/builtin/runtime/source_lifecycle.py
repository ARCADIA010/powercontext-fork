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

"""Scope-local evidence DAGs with atomic native Memory state propagation.

Every mutation explicitly binds the graph, event identity, Memory manifest and
search projections to one database transaction. An exception (including task
cancellation) rolls back the event sequence together with all native effects.
A committed replay performs no native writes. No pending work lives in memory.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, StrictStr, field_validator
from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncConnection

from powercontext.artifacts import ArtifactRef
from powercontext.builtin.artifacts.memory import MemoryCitation
from powercontext.builtin.artifacts.memory.errors import InvalidMemoryCitationError, MemoryEntryNotFoundError
from powercontext.builtin.persistence.codec import dump_model, load_model
from powercontext.builtin.persistence.errors import RepositoryNotFoundError
from powercontext.builtin.persistence.tables import SOURCE_LIFECYCLE_TABLE
from powercontext.errors import ArtifactNotFoundError
from powercontext.sources import SourceRef

if TYPE_CHECKING:
    from powercontext.builtin.runtime.application import BuiltinRuntime
    from powercontext.builtin.runtime.relational import RelationalContexts

Reference = SourceRef | MemoryCitation | ArtifactRef


class SourceLifecycleError(ValueError):
    """A lifecycle registration, reference or event violates the scope contract."""

    def __init__(self, reason: str) -> None:
        super().__init__(f"Invalid source lifecycle operation: {reason}")


def _nonblank(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SourceLifecycleError(f"{label}-nonblank-string-required")
    return value


class SourceLifecycleEvent(BaseModel):
    """A strictly typed, replayable event for one registered Source version."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    node_id: StrictStr
    revision: StrictInt = Field(gt=0)
    sequence: StrictInt = Field(gt=0)
    available: StrictBool
    reason: StrictStr

    @field_validator("node_id", "reason")
    @classmethod
    def nonblank(cls, value: str) -> str:
        return _nonblank(value, "event text")


class _Node(BaseModel):
    node_id: str
    kind: Literal["source", "memory", "topic"]
    reference: Reference
    revision: int | None = None
    parents: tuple[str, ...] = ()
    policy: Literal["all", "any"] = "all"
    available: bool = True
    last_event: SourceLifecycleEvent | None = None

    def registration(self) -> dict[str, object]:
        return self.model_dump(exclude={"available", "last_event"})


class SourceLifecycleStore:
    """Relational storage and native effects sharing an explicit connection."""

    def __init__(self, contexts: RelationalContexts) -> None:
        self.contexts = contexts
        self.database = contexts.database

    async def load(self, connection: AsyncConnection, scope_id: str) -> dict[str, _Node]:
        rows = await connection.execute(
            select(SOURCE_LIFECYCLE_TABLE.c.payload).where(SOURCE_LIFECYCLE_TABLE.c.scope_id == scope_id)
        )
        nodes = (load_model(_Node, row[0], kind="lifecycle", name="node") for row in rows)
        return {node.node_id: node for node in nodes}

    async def save(self, connection: AsyncConnection, scope_id: str, node: _Node, *, new: bool = False) -> None:
        values = {
            "kind": node.kind,
            "artifact_id": node.reference.artifact_id if isinstance(node.reference, ArtifactRef) else None,
            "available": node.available,
            "payload": dump_model(node, kind="lifecycle", name=node.node_id),
        }
        if new:
            statement = insert(SOURCE_LIFECYCLE_TABLE).values(scope_id=scope_id, node_id=node.node_id, **values)
        else:
            statement = (
                update(SOURCE_LIFECYCLE_TABLE)
                .where(SOURCE_LIFECYCLE_TABLE.c.scope_id == scope_id, SOURCE_LIFECYCLE_TABLE.c.node_id == node.node_id)
                .values(**values)
            )
        await connection.execute(statement)

    async def validate_reference(self, connection: AsyncConnection, scope_id: str, node: _Node) -> None:
        try:
            if node.kind == "source":
                if not isinstance(node.reference, SourceRef):
                    raise SourceLifecycleError("source-reference-type")
                await self.contexts.repositories.sources.get(connection, scope_id, node.reference)
            elif node.kind == "memory":
                if not isinstance(node.reference, MemoryCitation):
                    raise SourceLifecycleError("memory-reference-type")
                service = self.contexts.lifecycle_memory(scope_id, connection)
                await service.validate_citation(node.reference)
            else:
                if not isinstance(node.reference, ArtifactRef):
                    raise SourceLifecycleError("topic-reference-type")
                await self.contexts.repositories.topic_memories.get_exact(connection, scope_id, node.reference)
        except (
            RepositoryNotFoundError,
            ArtifactNotFoundError,
            MemoryEntryNotFoundError,
            InvalidMemoryCitationError,
        ) as exc:
            raise SourceLifecycleError("reference-is-not-readable-in-this-scope") from exc

    async def sync_memory(self, connection: AsyncConnection, scope_id: str, node: _Node, reason: str) -> None:
        if node.kind != "memory":
            return
        if not isinstance(node.reference, MemoryCitation):
            raise SourceLifecycleError("memory-reference-type")
        service = self.contexts.lifecycle_memory(scope_id, connection)
        head, entries = await service.head_entries(node.reference.memory_ref.artifact_id)
        entry = next((entry for entry in entries if entry.entry_id == node.reference.entry_id), None)
        if entry is None:
            raise SourceLifecycleError("registered-Memory-entry-no-longer-exists-in-the-current-head")
        mutation = service.reactivate if node.available else service.forget
        await mutation(head, entries=(entry,), reason=reason)


class SourceLifecycleApplication:
    """Create a scoped lifecycle facade without retaining graph state in memory."""

    def __init__(self, runtime: BuiltinRuntime, store: SourceLifecycleStore | None) -> None:
        self.runtime = runtime
        self.store = store

    def for_scope(self, scope_id: str) -> ScopedSourceLifecycle:
        return ScopedSourceLifecycle(self.runtime, self.store, scope_id)


class ScopedSourceLifecycle:
    def __init__(self, runtime: BuiltinRuntime, store: SourceLifecycleStore | None, scope_id: str) -> None:
        self.runtime = runtime
        self.store = store
        self.scope_id = scope_id

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[tuple[SourceLifecycleStore, AsyncConnection]]:
        if self.store is None:
            raise SourceLifecycleError("lifecycle-storage-is-unavailable")
        async with (
            self.runtime._context(self.scope_id),
            self.runtime._locked(self.scope_id),
            self.store.database.transaction() as connection,
        ):
            yield self.store, connection

    async def register_source(self, node_id: str, source_ref: SourceRef, *, revision: int) -> None:
        _nonblank(node_id, "node_id")
        if not isinstance(source_ref, SourceRef) or type(revision) is not int or revision <= 0:
            raise SourceLifecycleError("SourceRef-and-a-positive-strict-integer-revision-are-required")
        await self._register(_Node(node_id=node_id, kind="source", reference=source_ref, revision=revision))

    async def register_memory(
        self, node_id: str, citation: MemoryCitation, *, parents: Sequence[str], policy: str = "all"
    ) -> None:
        if not isinstance(citation, MemoryCitation):
            raise SourceLifecycleError("MemoryCitation-is-required")
        await self._register(self._derived(node_id, "memory", citation, parents, policy))

    async def register_topic(
        self, node_id: str, artifact_ref: ArtifactRef, *, parents: Sequence[str], policy: str = "all"
    ) -> None:
        if not isinstance(artifact_ref, ArtifactRef) or artifact_ref.family != "topic-memory":
            raise SourceLifecycleError("a-topic-memory-ArtifactRef-is-required")
        await self._register(self._derived(node_id, "topic", artifact_ref, parents, policy))

    @staticmethod
    def _derived(
        node_id: str, kind: Literal["memory", "topic"], reference: Reference, parents: Sequence[str], policy: str
    ) -> _Node:
        _nonblank(node_id, "node_id")
        if isinstance(parents, (str, bytes)) or not isinstance(parents, Sequence) or not parents:
            raise SourceLifecycleError("parents-must-be-a-nonempty-sequence")
        for parent in parents:
            _nonblank(parent, "parent")
        if len(set(parents)) != len(parents) or node_id in parents:
            raise SourceLifecycleError("duplicate-or-self-parent")
        if policy not in ("all", "any"):
            raise SourceLifecycleError("policy-must-be-all-or-any")
        return _Node(
            node_id=node_id,
            kind=kind,
            reference=reference,
            parents=tuple(parents),
            policy="all" if policy == "all" else "any",
        )

    async def _register(self, node: _Node) -> None:
        async with self._transaction() as (store, connection):
            nodes = await store.load(connection, self.scope_id)
            existing = nodes.get(node.node_id)
            if existing is not None:
                if existing.registration() != node.registration():
                    raise SourceLifecycleError("conflicting-registration")
                return
            if any(parent not in nodes for parent in node.parents):
                raise SourceLifecycleError("parents-must-already-be-registered-in-this-scope")
            await store.validate_reference(connection, self.scope_id, node)
            if node.kind != "source":
                support = (nodes[parent].available for parent in node.parents)
                node.available = all(support) if node.policy == "all" else any(support)
                await store.sync_memory(connection, self.scope_id, node, "lifecycle registration")
            await store.save(connection, self.scope_id, node, new=True)

    async def apply(self, event: SourceLifecycleEvent) -> list[str]:
        if not isinstance(event, SourceLifecycleEvent):
            raise SourceLifecycleError("SourceLifecycleEvent-is-required")
        async with self._transaction() as (store, connection):
            nodes = await store.load(connection, self.scope_id)
            source = nodes.get(event.node_id)
            if source is None or source.kind != "source" or source.revision != event.revision:
                raise SourceLifecycleError("event-requires-a-registered-Source-with-matching-revision")
            previous = source.last_event
            if previous is not None and event.sequence <= previous.sequence:
                if event == previous:
                    return sorted(node.node_id for node in nodes.values() if node.available)
                raise SourceLifecycleError("obsolete-sequence-or-conflicting-event-replay")
            source.available = event.available
            source.last_event = event
            # Parents are immutable and must precede registration, so new edges
            # cannot create cycles. Evaluate by dependency, never by SQL row order.
            resolved = {name for name, node in nodes.items() if node.kind == "source"}
            remaining = set(nodes) - resolved
            while remaining:
                ready = sorted(name for name in remaining if set(nodes[name].parents) <= resolved)
                if not ready:
                    raise SourceLifecycleError("persisted-graph-contains-a-cycle-or-missing-parent")
                for name in ready:
                    node = nodes[name]
                    support = (nodes[parent].available for parent in node.parents)
                    available = all(support) if node.policy == "all" else any(support)
                    if node.available != available:
                        node.available = available
                        await store.sync_memory(connection, self.scope_id, node, event.reason)
                        await store.save(connection, self.scope_id, node)
                    resolved.add(name)
                    remaining.remove(name)
            # This write is the completion marker, in the same transaction as
            # every manifest/index and topic qualification change above.
            await store.save(connection, self.scope_id, source)
            return sorted(node.node_id for node in nodes.values() if node.available)

    async def resolve(self, node_id: str, *, purpose: str = "generation") -> Reference:
        _nonblank(node_id, "node_id")
        if purpose not in ("generation", "audit"):
            raise SourceLifecycleError("purpose-must-be-generation-or-audit")
        async with self._transaction() as (store, connection):
            node = (await store.load(connection, self.scope_id)).get(node_id)
            if node is None or (purpose == "generation" and not node.available):
                raise SourceLifecycleError("unknown-or-unavailable-node")
            return node.reference


__all__ = ["SourceLifecycleError", "SourceLifecycleEvent"]
