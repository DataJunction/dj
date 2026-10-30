"""
Tests for deployment heartbeats and the stale deployment sweep.
"""

import asyncio
import os
import socket
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from datajunction_server.database.deployment import Deployment
from datajunction_server.internal.deployment.liveness import (
    Heartbeat,
    get_worker_id,
    interrupt_deployments,
    sweep_forever,
    sweep_stale,
)
from datajunction_server.models.deployment import DeploymentResult, DeploymentStatus

INTERRUPTED_RESULTS = [
    DeploymentResult(
        name="interrupted",
        deploy_type=DeploymentResult.Type.GENERAL,
        message="Deployment interrupted: its server stopped before it finished",
        status=DeploymentResult.Status.FAILED,
        operation=DeploymentResult.Operation.UNKNOWN,
    ).model_dump(),
]


def _add_deployment(session, current_user, **fields) -> str:
    deployment_id = str(uuid.uuid4())
    session.add(
        Deployment(
            uuid=deployment_id,
            namespace="test",
            created_by_id=current_user.id,
            **fields,
        ),
    )
    return deployment_id


def _patch_context(session):
    @asynccontextmanager
    async def fake_session_context():
        yield session

    return patch(
        "datajunction_server.internal.deployment.liveness.session_context",
        fake_session_context,
    )


def _patch_db_url(func__postgres_container):
    return patch(
        "datajunction_server.internal.deployment.liveness.get_settings",
        return_value=SimpleNamespace(
            writer_db=SimpleNamespace(
                uri=func__postgres_container.get_connection_url(),
            ),
        ),
    )


async def _reload(session) -> dict[str, Deployment]:
    rows = (await session.execute(select(Deployment))).scalars().all()
    for row in rows:
        await session.refresh(row)
    return {str(row.uuid): row for row in rows}


def test_worker_id_is_stable():
    host, pid, suffix = get_worker_id().rsplit("-", 2)
    assert (host, pid, len(suffix)) == (socket.gethostname(), str(os.getpid()), 6)
    assert get_worker_id() == f"{host}-{pid}-{suffix}"


async def test_beat_refreshes_running_deployment(
    session,
    current_user,
    func__postgres_container,
):
    old = datetime(2020, 1, 1, tzinfo=UTC)
    mine = _add_deployment(
        session,
        current_user,
        status=DeploymentStatus.RUNNING,
        worker_id=get_worker_id(),
        heartbeat_at=old,
    )
    other_worker = _add_deployment(
        session,
        current_user,
        status=DeploymentStatus.RUNNING,
        worker_id="someone-else",
        heartbeat_at=old,
    )
    done = _add_deployment(
        session,
        current_user,
        status=DeploymentStatus.SUCCESS,
        worker_id=get_worker_id(),
        heartbeat_at=old,
    )
    await session.commit()

    before = datetime.now(UTC)
    with _patch_db_url(func__postgres_container):
        for deployment_id in (mine, other_worker, done):
            Heartbeat(deployment_id, 10).beat()

    rows = await _reload(session)
    assert rows[other_worker].heartbeat_at == old
    assert rows[done].heartbeat_at == old
    assert before <= rows[mine].heartbeat_at <= datetime.now(UTC)


def test_beat_failure_is_logged(caplog):
    with patch(
        "datajunction_server.internal.deployment.liveness.create_engine",
        side_effect=RuntimeError("db down"),
    ):
        Heartbeat("abc", 10).beat()
    assert caplog.messages == ["Heartbeat for abc failed: db down"]


async def test_heartbeat_thread_runs_until_stopped(
    session,
    current_user,
    func__postgres_container,
):
    old = datetime(2020, 1, 1, tzinfo=UTC)
    deployment_id = _add_deployment(
        session,
        current_user,
        status=DeploymentStatus.RUNNING,
        worker_id=get_worker_id(),
        heartbeat_at=old,
    )
    await session.commit()

    with _patch_db_url(func__postgres_container):
        heartbeat = Heartbeat(deployment_id, 0.05)
        heartbeat.start()
        for _ in range(300):
            await asyncio.sleep(0.1)
            row = (await _reload(session))[deployment_id]
            if row.heartbeat_at != old:
                break
        heartbeat.stop()
        heartbeat._thread.join(5)

    assert row.heartbeat_at > old
    assert heartbeat._thread.is_alive() is False


async def test_sweep_stale_marks_only_dead_deployments(session, current_user):
    now = datetime.now(UTC)
    old = now - timedelta(hours=1)
    stale_running = _add_deployment(
        session,
        current_user,
        status=DeploymentStatus.RUNNING,
        worker_id="w",
        heartbeat_at=old,
    )
    legacy_running = _add_deployment(
        session,
        current_user,
        status=DeploymentStatus.RUNNING,
        updated_at=old,
    )
    stale_pending = _add_deployment(
        session,
        current_user,
        status=DeploymentStatus.PENDING,
        created_at=old,
    )
    live_running = _add_deployment(
        session,
        current_user,
        status=DeploymentStatus.RUNNING,
        worker_id="w",
        heartbeat_at=now,
    )
    fresh_pending = _add_deployment(
        session,
        current_user,
        status=DeploymentStatus.PENDING,
        created_at=now,
    )
    old_success = _add_deployment(
        session,
        current_user,
        status=DeploymentStatus.SUCCESS,
        heartbeat_at=old,
        created_at=old,
        results=[{"name": "kept"}],
    )
    await session.commit()

    with _patch_context(session):
        interrupted = await sweep_stale(300)

    assert sorted(interrupted) == sorted(
        [stale_running, legacy_running, stale_pending],
    )
    rows = await _reload(session)
    assert {key: (row.status, row.results) for key, row in rows.items()} == {
        stale_running: (DeploymentStatus.FAILED, INTERRUPTED_RESULTS),
        legacy_running: (DeploymentStatus.FAILED, INTERRUPTED_RESULTS),
        stale_pending: (DeploymentStatus.FAILED, INTERRUPTED_RESULTS),
        live_running: (DeploymentStatus.RUNNING, {}),
        fresh_pending: (DeploymentStatus.PENDING, {}),
        old_success: (DeploymentStatus.SUCCESS, [{"name": "kept"}]),
    }


async def test_concurrent_sweeps_do_not_double_work(
    session,
    session_factory,
    current_user,
):
    stale = _add_deployment(
        session,
        current_user,
        status=DeploymentStatus.RUNNING,
        worker_id="w",
        heartbeat_at=datetime.now(UTC) - timedelta(hours=1),
    )
    await session.commit()

    @asynccontextmanager
    async def fresh_session_context():
        replica_session = await session_factory()
        try:
            yield replica_session
        finally:
            await replica_session.close()

    with patch(
        "datajunction_server.internal.deployment.liveness.session_context",
        fresh_session_context,
    ):
        first, second, third = await asyncio.gather(
            sweep_stale(300),
            sweep_stale(300),
            sweep_stale(300),
        )

    assert sorted(first + second + third) == [stale]


async def test_interrupt_ignores_finished(session, current_user):
    done = _add_deployment(session, current_user, status=DeploymentStatus.SUCCESS)
    await session.commit()
    with _patch_context(session):
        interrupted = await interrupt_deployments(Deployment.uuid == done)
    assert interrupted == []


async def test_sweep_forever_survives_errors():
    sleep = AsyncMock(side_effect=[None, None, asyncio.CancelledError()])
    sweep = AsyncMock(side_effect=[["a"], RuntimeError("boom"), []])
    with (
        patch(
            "datajunction_server.internal.deployment.liveness.get_settings",
            return_value=SimpleNamespace(
                deployment_stale_after=7,
                deployment_sweep_interval=3,
            ),
        ),
        patch("datajunction_server.internal.deployment.liveness.sweep_stale", sweep),
        patch("datajunction_server.internal.deployment.liveness.asyncio.sleep", sleep),
        pytest.raises(asyncio.CancelledError),
    ):
        await sweep_forever()
    assert sweep.await_args_list == [((7,),), ((7,),), ((7,),)]
    assert sleep.await_args_list == [((3,),), ((3,),), ((3,),)]
