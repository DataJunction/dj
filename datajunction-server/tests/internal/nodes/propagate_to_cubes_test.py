"""
Propagation of an upstream change into downstream cubes.

A cube used to be excluded from downstream propagation entirely, so an upstream
edit left it serving a materialized table built against a definition that no
longer existed. The only way to recompile it was a no-op edit to the cube itself.
These tests pin that an upstream change now bumps the cube, at the upstream's own
tier, whether or not the cube's shape moved.
"""

from unittest.mock import patch

import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

import datajunction_server.internal.nodes as nodes_module
from datajunction_server.database.node import Node
from datajunction_server.database.user import User
from datajunction_server.internal.nodes import _propagate_update_downstream
from datajunction_server.models.deployment import ChangeTier, version_change_tier

# The upstream transform, reduced to the three columns these tests care about so a
# change to it is easy to read. `price` and `where` are the two things they vary:
# `price` moves a column type the cube can see, `where` moves only which rows the
# cube counts.
FACT_QUERY = """SELECT
  repair_orders.repair_order_id,
  repair_orders.hard_hat_id,
  {price} AS price
FROM
  default.repair_orders repair_orders
JOIN
  default.repair_order_details repair_order_details
ON repair_orders.repair_order_id = repair_order_details.repair_order_id
{where}"""

FACT = "default.repair_orders_fact"
CUBE = "default.repairs_by_state"
UNRELATED_CUBE = "default.employment_by_state"


async def _patch_fact(client: AsyncClient, price: str, where: str = "") -> str:
    """Rewrite the upstream transform's query and return its new version."""
    response = await client.patch(
        f"/nodes/{FACT}",
        json={"query": FACT_QUERY.format(price=price, where=where)},
    )
    assert response.status_code == 200, response.text
    return response.json()["version"]


async def _version(client: AsyncClient, name: str) -> str:
    response = await client.get(f"/nodes/{name}")
    assert response.status_code == 200, response.text
    return response.json()["version"]


async def _columns(client: AsyncClient, name: str) -> list[tuple[str, str]]:
    response = await client.get(f"/nodes/{name}")
    assert response.status_code == 200, response.text
    return [(col["name"], col["type"]) for col in response.json()["columns"]]


async def _state(client: AsyncClient, name: str) -> tuple[str, str]:
    """The node's version and status, which move together on a breaking change."""
    response = await client.get(f"/nodes/{name}")
    assert response.status_code == 200, response.text
    body = response.json()
    return body["version"], body["status"]


@pytest_asyncio.fixture
async def client_with_cube_downstream(client_with_roads: AsyncClient) -> AsyncClient:
    """
    Roads, narrowed to a transform -> metric -> cube chain the tests can move.

    A second cube sits on a metric and a dimension that are not downstream of the
    transform at all, so "everything bumped" and "the right thing bumped" are
    distinguishable.
    """
    await _patch_fact(client_with_roads, price="repair_order_details.price")
    response = await client_with_roads.post(
        "/nodes/metric/",
        json={
            "name": "default.total_price",
            "description": "Total price",
            "mode": "published",
            "query": f"SELECT sum(price) FROM {FACT}",
        },
    )
    assert response.status_code == 201, response.text
    response = await client_with_roads.post(
        "/nodes/cube/",
        json={
            "name": CUBE,
            "metrics": ["default.total_price"],
            "dimensions": ["default.hard_hat.state"],
            "description": "Repairs by state",
            "mode": "published",
        },
    )
    assert response.status_code == 201, response.text
    response = await client_with_roads.post(
        "/nodes/cube/",
        json={
            "name": UNRELATED_CUBE,
            "metrics": ["default.avg_length_of_employment"],
            "dimensions": ["default.hard_hat.state"],
            "description": "Employment by state",
            "mode": "published",
        },
    )
    assert response.status_code == 201, response.text
    return client_with_roads


@pytest.mark.asyncio
async def test_upstream_column_change_bumps_and_recompiles_cube(
    client_with_cube_downstream: AsyncClient,
):
    """
    The reported bug: an upstream change that materially changes the cube left the
    cube's version alone, so it kept serving a table built against the old upstream.
    """
    client = client_with_cube_downstream
    assert await _version(client, CUBE) == "v1.0"
    assert await _columns(client, CUBE) == [
        ("default.total_price", "double"),
        ("default.hard_hat.state", "string"),
    ]

    assert (
        await _patch_fact(client, price="CAST(repair_order_details.price AS int)")
        == "v3.0"
    )

    assert await _version(client, CUBE) == "v2.0"
    # The cube's columns were re-resolved against the new upstream revision, not
    # copied forward: sum(int) is a bigint where sum(double) was a double.
    assert await _columns(client, CUBE) == [
        ("default.total_price", "bigint"),
        ("default.hard_hat.state", "string"),
    ]


@pytest.mark.asyncio
async def test_upstream_filter_change_bumps_cube_with_unchanged_shape(
    client_with_cube_downstream: AsyncClient,
):
    """
    An upstream filter change moves no metric, no dimension, no column type and no
    metric component identity -- `is_non_trivial_cube_change` returns False for it --
    yet every row in the cube's materialized table was computed under the old
    filter. The bump must not be gated on the cube's own shape.
    """
    client = client_with_cube_downstream
    columns_before = await _columns(client, CUBE)

    assert (
        await _patch_fact(
            client,
            price="repair_order_details.price",
            where="WHERE repair_order_details.discount > 0.0",
        )
        == "v3.0"
    )

    assert await _version(client, CUBE) == "v2.0"
    assert await _columns(client, CUBE) == columns_before
    assert columns_before == [
        ("default.total_price", "double"),
        ("default.hard_hat.state", "string"),
    ]


@pytest.mark.asyncio
async def test_upstream_minor_change_bumps_cube_minor(
    client_with_cube_downstream: AsyncClient,
):
    """A cube inherits the upstream's tier, so a minor upstream edit is minor here."""
    client = client_with_cube_downstream

    response = await client.patch(
        f"/nodes/{FACT}",
        json={"description": "Fact transform, redescribed"},
    )
    assert response.status_code == 200, response.text
    assert response.json()["version"] == "v2.1"

    assert await _version(client, CUBE) == "v1.1"


@pytest.mark.asyncio
async def test_cube_not_downstream_is_untouched(
    client_with_cube_downstream: AsyncClient,
):
    """Propagation reaches the cubes below the changed node and no others."""
    client = client_with_cube_downstream

    await _patch_fact(client, price="CAST(repair_order_details.price AS int)")

    assert await _version(client, CUBE) == "v2.0"
    assert await _version(client, UNRELATED_CUBE) == "v1.0"


@pytest.mark.asyncio
async def test_cube_failure_does_not_abort_remaining_downstreams(
    client_with_roads: AsyncClient,
):
    """
    Bumping a cube can trigger a materialization rebuild, so one cube that cannot be
    rebuilt must not cost the remaining downstreams their propagation.
    """
    await _patch_fact(client_with_roads, price="repair_order_details.price")
    response = await client_with_roads.post(
        "/nodes/metric/",
        json={
            "name": "default.total_price",
            "description": "Total price",
            "mode": "published",
            "query": f"SELECT sum(price) FROM {FACT}",
        },
    )
    assert response.status_code == 201, response.text
    for name in ("default.cube_one", "default.cube_two"):
        response = await client_with_roads.post(
            "/nodes/cube/",
            json={
                "name": name,
                "metrics": ["default.total_price"],
                "dimensions": ["default.hard_hat.state"],
                "description": "A cube",
                "mode": "published",
            },
        )
        assert response.status_code == 201, response.text

    real_save = nodes_module.save_new_cube_revision
    calls: list[str] = []

    async def fail_first(session, node_revision, *args, **kwargs):
        calls.append(node_revision.name)
        if len(calls) == 1:
            raise RuntimeError("cannot rebuild this cube")
        return await real_save(session, node_revision, *args, **kwargs)

    with patch.object(nodes_module, "save_new_cube_revision", fail_first):
        await _patch_fact(
            client_with_roads,
            price="CAST(repair_order_details.price AS int)",
        )

    assert len(calls) == 2
    versions = sorted(
        [
            await _version(client_with_roads, "default.cube_one"),
            await _version(client_with_roads, "default.cube_two"),
        ],
    )
    # The cube whose rebuild raised keeps its version; the one after it in the
    # propagation order is still bumped.
    assert versions == ["v1.0", "v2.0"]


@pytest.mark.asyncio
async def test_a_cube_failing_after_one_succeeded_keeps_the_success(
    client_with_roads: AsyncClient,
):
    """
    The rollback that recovers from a failed rebuild must not undo an earlier one.

    Failing the *first* cube says nothing about this: there is no committed work for
    the rollback to reach. `save_new_cube_revision` commits per cube, so the bump
    before the failure is already durable -- this pins that, because the recovery
    path rolls the session back and a single transaction spanning the walk would
    silently lose the earlier cube's revision.
    """
    await _patch_fact(client_with_roads, price="repair_order_details.price")
    response = await client_with_roads.post(
        "/nodes/metric/",
        json={
            "name": "default.total_price",
            "description": "Total price",
            "mode": "published",
            "query": f"SELECT sum(price) FROM {FACT}",
        },
    )
    assert response.status_code == 201, response.text
    for name in ("default.cube_one", "default.cube_two"):
        response = await client_with_roads.post(
            "/nodes/cube/",
            json={
                "name": name,
                "metrics": ["default.total_price"],
                "dimensions": ["default.hard_hat.state"],
                "description": "A cube",
                "mode": "published",
            },
        )
        assert response.status_code == 201, response.text

    real_save = nodes_module.save_new_cube_revision
    calls: list[str] = []

    async def fail_second(session, node_revision, *args, **kwargs):
        calls.append(node_revision.name)
        if len(calls) == 2:
            raise RuntimeError("cannot rebuild this cube")
        return await real_save(session, node_revision, *args, **kwargs)

    with patch.object(nodes_module, "save_new_cube_revision", fail_second):
        await _patch_fact(
            client_with_roads,
            price="CAST(repair_order_details.price AS int)",
        )

    assert len(calls) == 2
    versions = sorted(
        [
            await _version(client_with_roads, "default.cube_one"),
            await _version(client_with_roads, "default.cube_two"),
        ],
    )
    # The first cube's bump survived the rollback that the second one triggered.
    assert versions == ["v1.0", "v2.0"]


@pytest.mark.asyncio
async def test_no_tier_change_leaves_cube_alone(
    client_with_cube_downstream: AsyncClient,
    session: AsyncSession,
):
    """
    A propagation carrying no tier invents no cube revision. `bump_version` would
    hand back the version the cube already has, and a second revision at the same
    version is not a thing a cube can have.
    """
    client = client_with_cube_downstream
    user = (
        await session.execute(select(User).where(User.username == "dj"))
    ).scalar_one()
    fact = await Node.get_by_name(session, FACT, raise_if_not_exists=True)
    assert fact is not None

    async def discard_history(event, session):  # noqa: ARG001
        return None

    await _propagate_update_downstream(
        session=session,
        node=fact,
        current_user=user,
        save_history=discard_history,
        change_tier=ChangeTier.NONE,
    )

    assert await _version(client, CUBE) == "v1.0"


@pytest.mark.asyncio
async def test_column_order_backfill_upstream_does_not_rebuild_the_cube(
    client_with_cube_downstream: AsyncClient,
    session: AsyncSession,
):
    """
    The one upstream change that moves nothing at all.

    A stored column with no `order` is DJ's own bookkeeping. Revalidating the
    upstream fills it in against the current revision, so the upstream does not turn
    over -- and a node that does not turn over has nothing to propagate, which is
    what leaves the cube alone. Before the backfill was reclassified this earned the
    upstream a major bump, and every consumer of a bump would have followed.

    Pinned on the whole downstream chain rather than the cube alone, since the
    guarantee is "no revision, so nothing moved", not something specific to cubes.
    """
    client = client_with_cube_downstream
    await session.execute(
        text(
            """
            UPDATE "column" SET "order" = NULL
            WHERE node_revision_id IN (
                SELECT id FROM noderevision WHERE name = :name
            )
            """,
        ),
        {"name": FACT},
    )
    await session.commit()
    session.expire_all()

    response = await client.post(f"/nodes/{FACT}/validate/")
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "valid"

    assert [
        (name, await _version(client, name))
        for name in (FACT, "default.total_price", CUBE, UNRELATED_CUBE)
    ] == [
        (FACT, "v2.0"),
        ("default.total_price", "v1.0"),
        (CUBE, "v1.0"),
        (UNRELATED_CUBE, "v1.0"),
    ]


def test_version_change_tier():
    """`bump_version` read backwards, including the no-change case."""
    assert version_change_tier("v1.0", "v2.0") == ChangeTier.MAJOR
    assert version_change_tier("v1.3", "v2.0") == ChangeTier.MAJOR
    assert version_change_tier("v1.0", "v1.1") == ChangeTier.MINOR
    assert version_change_tier("v1.0", "v1.0") == ChangeTier.NONE


@pytest.mark.asyncio
async def test_a_removed_column_a_metric_uses_invalidates_it_and_the_cube(
    client_with_cube_downstream: AsyncClient,
):
    """
    Dropping a column the metric aggregates carries all the way to the cube.

    The transform stays valid -- its own query is fine -- while the metric can no
    longer infer a type for `sum(price)` and the cube built on that metric follows
    it down. Each bumps, so nothing is left quietly serving a definition that no
    longer resolves. Pinned because the failure is only visible downstream: the
    edit that causes it looks entirely successful at the node being edited.
    """
    client = client_with_cube_downstream
    response = await client.patch(
        f"/nodes/{FACT}",
        json={
            "query": (
                "SELECT repair_orders.repair_order_id, repair_orders.hard_hat_id "
                "FROM default.repair_orders repair_orders"
            ),
        },
    )
    assert response.status_code == 200, response.text

    assert await _state(client, FACT) == ("v3.0", "valid")
    assert await _state(client, "default.total_price") == ("v2.0", "invalid")
    assert await _state(client, CUBE) == ("v2.0", "invalid")
    # The cube that shares only a dimension with the edited transform is untouched.
    assert await _state(client, UNRELATED_CUBE) == ("v1.0", "valid")
