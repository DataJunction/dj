"""Tests for filling the parse cache from stored queries."""

import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.user import User
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.parsing import cache, warm
from datajunction_server.sql.parsing.warm import (
    WarmSummary,
    warm_parse_cache,
    warm_parse_cache_if_enabled,
    warm_parse_cache_sync,
)


@pytest.fixture(autouse=True)
def empty_cache():
    cache._cache.clear()
    yield
    cache._cache.clear()


def add_node(
    session: AsyncSession,
    user: User,
    name: str,
    node_type: NodeType,
    queries: dict[str, str | None],
    current_version: str,
    deactivated: bool = False,
) -> None:
    node = Node(
        name=name,
        type=node_type,
        current_version=current_version,
        created_by_id=user.id,
        deactivated_at=datetime.datetime.now(datetime.UTC) if deactivated else None,
    )
    session.add(node)
    for version, query in queries.items():
        session.add(
            NodeRevision(
                node=node,
                name=name,
                type=node_type,
                display_name=name,
                version=version,
                query=query,
                created_by_id=user.id,
            ),
        )


async def seed(session: AsyncSession, user: User) -> None:
    add_node(
        session,
        user,
        "ns.t1",
        NodeType.TRANSFORM,
        {"v1": "SELECT 1 FROM a"},
        "v1",
    )
    add_node(
        session,
        user,
        "ns.t2",
        NodeType.TRANSFORM,
        {"v1": "SELECT 2 FROM old", "v2": "SELECT 2 FROM new"},
        "v2",
    )
    add_node(
        session,
        user,
        "ns.m1",
        NodeType.METRIC,
        {"v1": "SELECT COUNT(*) FROM b"},
        "v1",
    )
    add_node(session, user, "ns.d1", NodeType.DIMENSION, {"v1": "SELECT ("}, "v1")
    add_node(session, user, "ns.s1", NodeType.SOURCE, {"v1": "SELECT 9 FROM src"}, "v1")
    add_node(session, user, "ns.c1", NodeType.CUBE, {"v1": None}, "v1")
    add_node(
        session,
        user,
        "ns.gone",
        NodeType.TRANSFORM,
        {"v1": "SELECT 3 FROM gone"},
        "v1",
        deactivated=True,
    )
    await session.commit()


@pytest.mark.asyncio
async def test_warm_parses_current_queries_of_active_non_source_nodes(
    clean_session: AsyncSession,
    clean_current_user: User,
):
    await seed(clean_session, clean_current_user)
    summary = await warm_parse_cache(clean_session)
    assert summary.parsed == 3
    assert summary.skipped == 1
    assert summary.seconds >= 0
    assert set(cache._cache) == {
        ("SELECT 1 FROM a", False),
        ("SELECT 2 FROM new", False),
        ("SELECT COUNT(*) FROM b", False),
    }


@pytest.mark.asyncio
async def test_warm_reads_in_batches(
    clean_session: AsyncSession,
    clean_current_user: User,
):
    await seed(clean_session, clean_current_user)
    with patch.object(warm, "BATCH_SIZE", 1):
        summary = await warm_parse_cache(clean_session)
    assert (summary.parsed, summary.skipped) == (3, 1)
    assert len(cache._cache) == 3


@pytest.mark.asyncio
async def test_warm_keeps_the_newest_queries_when_the_cache_is_full(
    clean_session: AsyncSession,
    clean_current_user: User,
):
    await seed(clean_session, clean_current_user)
    with patch.object(cache, "PARSE_CACHE_SIZE", 2):
        summary = await warm_parse_cache(clean_session)
    assert (summary.parsed, summary.skipped) == (2, 1)
    assert set(cache._cache) == {
        ("SELECT COUNT(*) FROM b", False),
        ("SELECT 2 FROM new", False),
    }


@pytest.mark.asyncio
async def test_warm_with_no_nodes_does_nothing(clean_session: AsyncSession):
    summary = await warm_parse_cache(clean_session)
    assert (summary.parsed, summary.skipped) == (0, 0)
    assert cache._cache == {}


@pytest.mark.asyncio
async def test_new_engine_is_disposed_after_warming():
    engine = MagicMock()
    engine.dispose = AsyncMock()
    session = MagicMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    expected = WarmSummary(parsed=5, skipped=1, seconds=0.5)
    with (
        patch.object(
            warm.DatabaseSessionManager,
            "create_engine_and_session",
            return_value=(engine, MagicMock(return_value=session)),
        ) as create,
        patch.object(warm, "warm_parse_cache", AsyncMock(return_value=expected)) as run,
    ):
        assert await warm._warm_with_new_engine() == expected
    assert create.call_args.args[1] == "reader"
    run.assert_awaited_once_with(session)
    engine.dispose.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_new_engine_is_disposed_when_warming_fails():
    engine = MagicMock()
    engine.dispose = AsyncMock()
    session = MagicMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    with (
        patch.object(
            warm.DatabaseSessionManager,
            "create_engine_and_session",
            return_value=(engine, MagicMock(return_value=session)),
        ),
        patch.object(
            warm,
            "warm_parse_cache",
            AsyncMock(side_effect=RuntimeError("x")),
        ),
    ):
        with pytest.raises(RuntimeError, match="x"):
            await warm._warm_with_new_engine()
    engine.dispose.assert_awaited_once_with()


def test_sync_entry_point_returns_the_summary():
    expected = WarmSummary(parsed=2, skipped=0, seconds=0.1)
    with patch.object(warm, "_warm_with_new_engine", AsyncMock(return_value=expected)):
        assert warm_parse_cache_sync() == expected


@pytest.mark.asyncio
async def test_sync_entry_point_works_inside_a_running_loop():
    expected = WarmSummary(parsed=2, skipped=0, seconds=0.1)
    with patch.object(warm, "_warm_with_new_engine", AsyncMock(return_value=expected)):
        assert warm_parse_cache_sync() == expected


def test_nothing_happens_when_preload_is_off():
    settings = MagicMock(parse_cache_preload=False)
    with (
        patch.object(warm, "get_settings", return_value=settings),
        patch.object(warm, "warm_parse_cache_sync") as run,
    ):
        assert warm_parse_cache_if_enabled() is None
    run.assert_not_called()


def test_warms_when_preload_is_on():
    settings = MagicMock(parse_cache_preload=True)
    expected = WarmSummary(parsed=1, skipped=0, seconds=0.0)
    with (
        patch.object(warm, "get_settings", return_value=settings),
        patch.object(warm, "warm_parse_cache_sync", return_value=expected),
    ):
        assert warm_parse_cache_if_enabled() == expected
