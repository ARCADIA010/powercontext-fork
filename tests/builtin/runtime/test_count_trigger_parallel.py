"""Count admission fairness and durable invocation transaction regressions."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from functools import partial

import pytest
from sqlalchemy import event

from powercontext.builtin.persistence.cursors import SourceCursorRepository
from powercontext.builtin.persistence.sources import SourceRepository
from powercontext.builtin.persistence.supervision import ArtifactProcessingBindingStateRepository
from powercontext.builtin.runtime.artifact_processing import ArtifactProcessingBinding, ArtifactProcessingSupervisor
from powercontext.builtin.runtime.processing_contracts import ArtifactProcessingWorkerCompletion
from powercontext.builtin.runtime.processing_discovery import pending_source_count_scopes
from powercontext.builtin.runtime.processing_execution import ScopeInvocation
from powercontext.builtin.sources import BUILTIN_SOURCE_REGISTRY, ContentSource, SourceCursor
from powercontext.sources import SourceMaterialization
from tests.builtin.runtime.test_processing_scheduler import _intent, _Launcher, _profile, _wait

_BINDING = "memory-source-window"


async def _capture(database, scope: str, count: int, *, start: int = 0) -> None:
    async with database.transaction() as connection:
        for position in range(start, start + count):
            await SourceRepository(BUILTIN_SOURCE_REGISTRY).add(
                connection,
                scope,
                ContentSource(
                    name=f"source-{position}",
                    content=f"Fact {position}",
                    materialization=SourceMaterialization.CAPTURED,
                ),
            )


def _binding(launcher, *, interval=None) -> ArtifactProcessingBinding:
    return ArtifactProcessingBinding(
        _BINDING,
        "memory",
        launcher,
        automatic_processing_interval=interval,
        source_count_scope_filter=partial(pending_source_count_scopes, binding_name=_BINDING, threshold=3),
    )


async def _consume_one(database, assignment) -> ArtifactProcessingWorkerCompletion:
    # Exercise scheduler authorization and cursor/ack atomically. These are
    # control-plane tests; native generation and real artifacts are covered by
    # the official acceptance suite, not substituted by this test launcher.
    invocation = ScopeInvocation(assignment)
    cursors = SourceCursorRepository()
    async with database.transaction() as connection:
        await invocation.start(connection)
        previous = await cursors.load(connection, assignment.scope_id, _BINDING)
        after = 0 if previous is None else previous.cursor.sequence
        sources = SourceRepository(BUILTIN_SOURCE_REGISTRY)
        rows = await sources.list(connection, assignment.scope_id, after=after, limit=1)
        through = rows[-1].journal_position
        high = await sources.journal_position(connection, assignment.scope_id)
        await cursors.save(
            connection,
            assignment.scope_id,
            _BINDING,
            SourceCursor(sequence=through),
            expected_generation=None if previous is None else previous.generation,
        )
        await invocation.complete(connection, remaining_work=through < high)
    return ArtifactProcessingWorkerCompletion()


def test_residual_count_restarts_without_new_capture_and_stops_below_threshold(tmp_path):
    async def scenario():
        async with _profile(tmp_path, "count-residual") as profile:
            await _capture(profile.database, "a", 5)
            launcher = _Launcher(profile.database, on_work=partial(_consume_one, profile.database))
            async with ArtifactProcessingSupervisor(
                database=profile.database,
                bindings=(_binding(launcher),),
                lease_mode="single-process",
                oceanbase_tick_seconds=0.05,
            ) as supervisor:
                await _wait(lambda: supervisor.family_status["memory"]["completed"] == 3)
                await asyncio.sleep(0.2)
                assert len(launcher.assignments) == 3
                intent = await _intent(profile.database, _BINDING, "a")
                assert intent.requested_generation == intent.handled_generation == 3
                assert intent.dirty_generation > intent.clean_generation
                async with profile.database.transaction() as connection:
                    cursor = await SourceCursorRepository().load(connection, "a", _BINDING)
                    assert cursor is not None and cursor.cursor.sequence == 3

    asyncio.run(scenario())


def test_unqualified_pages_do_not_starve_tail_and_stable_prefix_is_revisited_without_wake(tmp_path):
    async def scenario():
        async with _profile(tmp_path, "count-pagination") as profile:
            for index in range(105):
                await _capture(profile.database, f"scope-{index:03}", 1)
            await _capture(profile.database, "tail", 3)
            launcher = _Launcher(profile.database)
            async with ArtifactProcessingSupervisor(
                database=profile.database,
                bindings=(_binding(launcher),),
                lease_mode="single-process",
                oceanbase_tick_seconds=0.05,
            ) as supervisor:
                await _wait(lambda: supervisor.family_status["memory"]["completed"] == 1)
                assert [work.scope_id for work in launcher.assignments] == ["tail"]
                assert (await _intent(profile.database, _BINDING, "scope-000")).requested_generation == 0
                # The durable sequence remains unchanged; no facade notification.
                before = await _intent(profile.database, _BINDING, "scope-000")
                await _capture(profile.database, "scope-000", 2, start=1)
                await _wait(lambda: supervisor.family_status["memory"]["completed"] == 2)
                after = await _intent(profile.database, _BINDING, "scope-000")
                assert before.pending_sequence == after.pending_sequence
                assert after.requested_generation == after.handled_generation == 1
                assert [work.scope_id for work in launcher.assignments] == ["tail", "scope-000"]

    asyncio.run(scenario())


@pytest.mark.parametrize("first", ["count", "time"])
def test_count_and_time_coalesce_and_count_does_not_advance_time_checkpoint(tmp_path, first):
    async def scenario():
        async with _profile(tmp_path, "count-or") as profile:
            await _capture(profile.database, "a", 3)
            supervisor = ArtifactProcessingSupervisor(
                database=profile.database,
                bindings=(_binding(_Launcher(profile.database), interval=timedelta(days=1)),),
                lease_mode="single-process",
            )
            await supervisor._acquire()
            state = supervisor._families[_BINDING]
            try:
                if first == "count":
                    await supervisor._discover_count(state)
                    async with profile.database.transaction() as connection:
                        assert await ArtifactProcessingBindingStateRepository().load(connection, _BINDING) is None
                    await supervisor._discover_automatic(state)
                else:
                    await supervisor._discover_automatic(state)
                    async with profile.database.transaction() as connection:
                        before = await ArtifactProcessingBindingStateRepository().load(connection, _BINDING)
                    await supervisor._discover_count(state)
                    async with profile.database.transaction() as connection:
                        assert await ArtifactProcessingBindingStateRepository().load(connection, _BINDING) == before
                assert (await _intent(profile.database, _BINDING, "a")).requested_generation == 1
                assert tuple(state.ready) == ("a",)
            finally:
                await supervisor._lose_leadership()

    asyncio.run(scenario())


def test_count_admission_commit_failure_keeps_frontier_and_request_atomic(tmp_path):
    async def scenario():
        async with _profile(tmp_path, "count-rollback") as profile:
            await _capture(profile.database, "a", 3)
            supervisor = ArtifactProcessingSupervisor(
                database=profile.database,
                bindings=(_binding(_Launcher(profile.database)),),
                lease_mode="single-process",
            )
            await supervisor._acquire()
            state = supervisor._families[_BINDING]

            class InjectedRollback(Exception):
                pass

            def fail_commit(connection):
                raise InjectedRollback

            try:
                event.listen(profile.database.engine.sync_engine, "commit", fail_commit, once=True)
                try:
                    with pytest.raises(InjectedRollback):
                        await supervisor._discover_count(state)
                finally:
                    event.remove(profile.database.engine.sync_engine, "commit", fail_commit)
                assert (state.count_after, state.count_upper, state.next_count_at) == (0, None, 0.0)
                assert not state.ready
                assert (await _intent(profile.database, _BINDING, "a")).requested_generation == 0
                await supervisor._discover_count(state)
                assert (await _intent(profile.database, _BINDING, "a")).requested_generation == 1
                assert tuple(state.ready) == ("a",)
            finally:
                await supervisor._lose_leadership()

    asyncio.run(scenario())
