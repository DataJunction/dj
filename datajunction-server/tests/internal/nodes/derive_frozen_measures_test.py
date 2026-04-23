"""
Unit tests for ``derive_frozen_measures_bulk`` — the batched derivation path
used by the deployment orchestrator. Exercises the cache-construction branches
(derived-metric expansion, deep-chain iterative expansion) and edge cases
(empty list, shared measures across metrics) that aren't reachable through
the per-metric ``derive_frozen_measures`` entry point.
"""

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import datajunction_server.sql.parsing.types as ct
from datajunction_server.database.column import Column
from datajunction_server.database.measure import FrozenMeasure
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.user import OAuthProvider, User
from datajunction_server.internal.nodes import derive_frozen_measures_bulk
from datajunction_server.models.node import NodeStatus
from datajunction_server.models.node_type import NodeType


@pytest_asyncio.fixture
async def user(session: AsyncSession) -> User:
    u = User(username="derive_fm_test_user", oauth_provider=OAuthProvider.BASIC)
    session.add(u)
    await session.commit()
    return u


async def _make_source(
    session: AsyncSession,
    user: User,
    name: str,
    columns: list[Column],
) -> Node:
    node = Node(
        name=name,
        type=NodeType.SOURCE,
        current_version="v1.0",
        created_by_id=user.id,
    )
    rev = NodeRevision(
        node=node,
        name=name,
        type=NodeType.SOURCE,
        version="v1.0",
        query=None,
        status=NodeStatus.VALID,
        columns=columns,
        created_by_id=user.id,
    )
    session.add_all([node, rev])
    await session.commit()
    return node


async def _make_metric(
    session: AsyncSession,
    user: User,
    name: str,
    query: str,
    parents: list[Node],
) -> Node:
    node = Node(
        name=name,
        type=NodeType.METRIC,
        current_version="v1.0",
        created_by_id=user.id,
    )
    rev = NodeRevision(
        node=node,
        name=name,
        type=NodeType.METRIC,
        version="v1.0",
        query=query,
        status=NodeStatus.VALID,
        parents=parents,
        created_by_id=user.id,
    )
    session.add_all([node, rev])
    await session.commit()
    return node


@pytest.mark.asyncio
async def test_empty_list_is_noop(session: AsyncSession):
    """An empty input returns immediately without touching the session."""
    before = (await session.execute(select(FrozenMeasure))).scalars().all()
    await derive_frozen_measures_bulk(session, [])
    after = (await session.execute(select(FrozenMeasure))).scalars().all()
    assert len(after) == len(before)


@pytest.mark.asyncio
async def test_base_metric_populates_derived_expression_and_measure(
    session: AsyncSession,
    user: User,
):
    """Single base metric → one FrozenMeasure keyed on its aggregation, plus
    a derived_expression on the metric revision."""
    src = await _make_source(
        session,
        user,
        "src",
        [Column(name="amount", type=ct.DoubleType(), order=0)],
    )
    metric = await _make_metric(
        session,
        user,
        "m.total_amount",
        "SELECT SUM(amount) FROM src",
        [src],
    )
    await session.refresh(metric, ["current"])
    await derive_frozen_measures_bulk(session, [metric.current.id])
    await session.commit()

    await session.refresh(metric.current, ["frozen_measures"])
    assert metric.current.derived_expression is not None
    assert len(metric.current.frozen_measures) >= 1
    assert any(fm.aggregation == "SUM" for fm in metric.current.frozen_measures)


@pytest.mark.asyncio
async def test_derived_metric_expands_parent_cache(
    session: AsyncSession,
    user: User,
):
    """A derived metric deployed on its own (parent metric pre-existing)
    exercises the first-loop cache expansion that walks from the deployed
    revision into its metric parents and their grandparent sources — the
    lines that populate parent_map entries for parent metrics and seed
    nodes_cache with the grandparent sources."""
    src = await _make_source(
        session,
        user,
        "src2",
        [Column(name="amount", type=ct.DoubleType(), order=0)],
    )
    base = await _make_metric(
        session,
        user,
        "m.base_total",
        "SELECT SUM(amount) FROM src2",
        [src],
    )
    derived = await _make_metric(
        session,
        user,
        "m.derived_double",
        "SELECT m.base_total * 2",
        [base],
    )
    await session.refresh(base, ["current"])
    await session.refresh(derived, ["current"])
    # Pre-derive the base so the derived metric's extract can resolve it.
    await derive_frozen_measures_bulk(session, [base.current.id])
    # Now derive the derived metric in isolation — its parent chain must be
    # reconstructed from the eager load + cache-build loop.
    await derive_frozen_measures_bulk(session, [derived.current.id])
    await session.commit()

    await session.refresh(derived.current, ["frozen_measures"])
    assert derived.current.derived_expression is not None
    # The derived metric inherits its base's components.
    assert len(derived.current.frozen_measures) >= 1


@pytest.mark.asyncio
async def test_deep_derived_chain_resolves(
    session: AsyncSession,
    user: User,
):
    """A 3-level derived metric chain (C derived from B derived from A
    derived from source S). The deployed metric C's parents are fully
    resolved through the 2-level eager load plus SQLAlchemy's session
    identity map (which already has A cached from the earlier bulk
    derive). Confirms depth-3 derivation works end-to-end."""
    src = await _make_source(
        session,
        user,
        "src3",
        [Column(name="amount", type=ct.DoubleType(), order=0)],
    )
    a = await _make_metric(
        session,
        user,
        "m.a_total",
        "SELECT SUM(amount) FROM src3",
        [src],
    )
    b = await _make_metric(session, user, "m.b_scaled", "SELECT m.a_total * 2", [a])
    c = await _make_metric(session, user, "m.c_offset", "SELECT m.b_scaled + 1", [b])
    await session.refresh(a, ["current"])
    await session.refresh(b, ["current"])
    await session.refresh(c, ["current"])
    # Derive A and B first so the chain has resolved measures for C's extractor.
    await derive_frozen_measures_bulk(session, [a.current.id])
    await derive_frozen_measures_bulk(session, [b.current.id])
    # Deriving C on its own must iteratively expand the parent chain.
    await derive_frozen_measures_bulk(session, [c.current.id])
    await session.commit()

    await session.refresh(c.current, ["frozen_measures"])
    assert c.current.derived_expression is not None
    assert len(c.current.frozen_measures) >= 1


@pytest.mark.asyncio
async def test_two_metrics_in_one_batch_both_get_measures(
    session: AsyncSession,
    user: User,
):
    """Batch derivation of multiple metrics in a single call wires each
    metric's FrozenMeasure links independently — both get populated, and
    the batch-fetch/link logic iterates each (rev, measures) pair."""
    src = await _make_source(
        session,
        user,
        "src4",
        [
            Column(name="amount", type=ct.DoubleType(), order=0),
            Column(name="cost", type=ct.DoubleType(), order=1),
        ],
    )
    m1 = await _make_metric(
        session,
        user,
        "m.sum_a",
        "SELECT SUM(amount) FROM src4",
        [src],
    )
    m2 = await _make_metric(
        session,
        user,
        "m.sum_c",
        "SELECT SUM(cost) FROM src4",
        [src],
    )
    await session.refresh(m1, ["current"])
    await session.refresh(m2, ["current"])

    await derive_frozen_measures_bulk(session, [m1.current.id, m2.current.id])
    await session.commit()

    await session.refresh(m1.current, ["frozen_measures"])
    await session.refresh(m2.current, ["frozen_measures"])
    assert m1.current.derived_expression is not None
    assert m2.current.derived_expression is not None
    assert len(m1.current.frozen_measures) >= 1
    assert len(m2.current.frozen_measures) >= 1
