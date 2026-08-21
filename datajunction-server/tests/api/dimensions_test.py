"""
Tests for the dimensions API.
"""

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.dimensionlink import DimensionLink
from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.user import User
from datajunction_server.models.node_type import NodeType


@pytest.mark.asyncio
async def test_list_dimension(
    module__client_with_roads_and_acc_revenue: AsyncClient,
) -> None:
    """
    Test ``GET /dimensions/``.
    """
    response = await module__client_with_roads_and_acc_revenue.get(
        "/dimensions/?prefix=default",
    )
    data = response.json()

    assert response.status_code == 200

    results = {(dim["name"], dim["indegree"]) for dim in data}
    assert ("default.dispatcher", 3) in results
    assert ("default.repair_order", 2) in results
    assert ("default.hard_hat", 2) in results
    assert ("default.hard_hat_to_delete", 2) in results
    assert ("default.municipality_dim", 2) in results
    assert ("default.contractor", 1) in results
    assert ("default.us_state", 2) in results
    assert ("default.local_hard_hats", 0) in results
    assert ("default.local_hard_hats_1", 0) in results
    assert ("default.local_hard_hats_2", 0) in results
    assert ("default.payment_type", 0) in results
    assert ("default.account_type", 0) in results
    assert ("default.hard_hat_2", 0) in results


@pytest.mark.asyncio
async def test_main_branch_names_with_no_names(session: AsyncSession) -> None:
    """
    An empty name list resolves to an empty set without issuing a query.
    """
    assert await Node.main_branch_names(session, []) == set()


@pytest.mark.asyncio
async def test_list_dimension_limit(client_with_roads: AsyncClient) -> None:
    """
    ``limit`` caps the response to the highest-ranked dimensions, and omitting it
    still returns the full list.
    """
    full = await client_with_roads.get("/dimensions/")
    assert full.status_code == 200
    all_dims = full.json()
    assert len(all_dims) > 3

    limited = await client_with_roads.get("/dimensions/?limit=3")
    assert limited.status_code == 200
    assert limited.json() == all_dims[:3]


@pytest.mark.asyncio
async def test_list_dimension_ranks_main_branch_first(
    client_with_roads: AsyncClient,
    session: AsyncSession,
) -> None:
    """
    A dimension on a feature branch sorts below main-branch dimensions even when
    it is linked to more often, so a branch's copies cannot crowd out the
    authoritative ones.
    """
    current_user = (await User.get_by_username(session, "dj")) or (
        await session.get(User, 1)
    )
    session.add_all(
        [
            NodeNamespace(namespace="proj", default_branch="main"),
            NodeNamespace(
                namespace="proj.feature_x",
                parent_namespace="proj",
                git_branch="feature_x",
            ),
        ],
    )
    await session.commit()

    branch_dim = Node(
        name="proj.feature_x.busy_dim",
        type=NodeType.DIMENSION,
        namespace="proj.feature_x",
        created_by_id=current_user.id,
    )
    branch_dim_rev = NodeRevision(
        node=branch_dim,
        name=branch_dim.name,
        type=branch_dim.type,
        version="v1.0",
        created_by_id=current_user.id,
    )
    session.add_all([branch_dim, branch_dim_rev])
    await session.flush()
    branch_dim.current_version = "v1.0"

    # Link every roads fact revision at it so its indegree beats every main dim.
    roads_revisions = (
        await session.execute(
            NodeRevision.__table__.select().limit(20),  # type: ignore[attr-defined]
        )
    ).fetchall()
    for revision in roads_revisions:
        session.add(
            DimensionLink(
                dimension_id=branch_dim.id,
                node_revision_id=revision.id,
                join_sql=f"x.id = {branch_dim.name}.id",
            ),
        )
    await session.commit()

    response = await client_with_roads.get("/dimensions/")
    assert response.status_code == 200
    names = [dim["name"] for dim in response.json()]
    assert branch_dim.name in names, "branch dimension should still be listed"

    by_name = {dim["name"]: dim for dim in response.json()}
    branch_indegree = by_name[branch_dim.name]["indegree"]
    main_names = [name for name in names if not name.startswith("proj.feature_x.")]
    assert branch_indegree > 0, "fixture should give the branch dim a real indegree"
    # Every main-branch dimension outranks it, including ones linked to less often.
    assert names.index(branch_dim.name) == len(main_names)


@pytest.mark.asyncio
async def test_list_dimension_query_count(
    client_with_roads: AsyncClient,
    capture_queries: list[str],
) -> None:
    """
    ``GET /dimensions/`` must stay on a small, constant number of queries.

    The dimension picker in the UI loads this list in full on every mount, so
    the cost here is paid interactively. It used to hydrate every dimension node
    as a full ORM object -- pulling in columns, parents, dimension links and
    those links' target dimensions -- which ran into the hundreds of queries and
    tens of seconds on a large instance.
    """
    response = await client_with_roads.get("/dimensions/")
    assert response.status_code == 200

    # The endpoint's own work is three queries: list the dimension nodes,
    # aggregate their indegrees, and classify which sit on a default branch.
    node_queries = [
        query
        for query in capture_queries
        if "FROM node" in query or "FROM dimensionlink" in query
    ]
    assert len(node_queries) == 3, "\n\n".join(node_queries)

    # Everything else in the request is per-request auth/RBAC. Cap the total so
    # that a relationship quietly becoming eager-loaded (which would fan out
    # into extra batched selects against other tables) fails here.
    assert len(capture_queries) <= 9, "\n\n".join(capture_queries)


@pytest.mark.asyncio
async def test_list_nodes_with_dimension(
    module__client_with_roads_and_acc_revenue: AsyncClient,
) -> None:
    """
    Test ``GET /dimensions/{name}/nodes/``.
    """
    response = await module__client_with_roads_and_acc_revenue.get(
        "/dimensions/default.hard_hat/nodes/",
    )
    data = response.json()
    roads_nodes = {
        "default.repair_orders",
        "default.repair_order_details",
        "default.repair_order",
        "default.num_repair_orders",
        "default.num_unique_hard_hats_approx",
        "default.avg_repair_price",
        "default.repair_orders_fact",
        "default.total_repair_cost",
        "default.discounted_orders_rate",
        "default.total_repair_order_discounts",
        "default.avg_repair_order_discounts",
        "default.avg_time_to_dispatch",
    }
    assert {node["name"] for node in data} == roads_nodes

    response = await module__client_with_roads_and_acc_revenue.get(
        "/dimensions/default.repair_order/nodes/",
    )
    data = response.json()
    assert {node["name"] for node in data} == {
        "default.repair_order_details",
        "default.repair_orders",
    }

    response = await module__client_with_roads_and_acc_revenue.get(
        "/dimensions/default.us_state/nodes/",
    )
    data = response.json()
    assert {node["name"] for node in data} == {
        "default.avg_length_of_employment",
        "default.repair_orders",
        "default.repair_order_details",
        "default.hard_hat",
        "default.repair_order",
        "default.num_repair_orders",
        "default.num_unique_hard_hats_approx",
        "default.avg_repair_price",
        "default.repair_orders_fact",
        "default.total_repair_cost",
        "default.discounted_orders_rate",
        "default.total_repair_order_discounts",
        "default.avg_repair_order_discounts",
        "default.avg_time_to_dispatch",
        "default.contractors",
    }

    response = await module__client_with_roads_and_acc_revenue.get(
        "/dimensions/default.municipality_dim/nodes/",
    )
    data = response.json()
    assert {node["name"] for node in data} == roads_nodes

    response = await module__client_with_roads_and_acc_revenue.get(
        "/dimensions/default.contractor/nodes/",
    )
    data = response.json()
    assert {node["name"] for node in data} == {
        "default.repair_type",
    }

    response = await module__client_with_roads_and_acc_revenue.get(
        "/dimensions/default.municipality_dim/nodes/?node_type=metric",
    )
    data = response.json()
    assert {node["name"] for node in data} == {
        "default.num_repair_orders",
        "default.num_unique_hard_hats_approx",
        "default.avg_repair_price",
        "default.total_repair_cost",
        "default.discounted_orders_rate",
        "default.total_repair_order_discounts",
        "default.avg_repair_order_discounts",
        "default.avg_time_to_dispatch",
    }


@pytest.mark.asyncio
async def test_list_nodes_with_common_dimension(
    module__client_with_roads_and_acc_revenue: AsyncClient,
) -> None:
    """
    Test ``GET /dimensions/common/``.
    """
    response = await module__client_with_roads_and_acc_revenue.get(
        "/dimensions/common/?dimension=default.hard_hat",
    )
    data = response.json()
    roads_nodes = {
        "default.repair_order",
        "default.repair_orders",
        "default.repair_order_details",
        "default.num_repair_orders",
        "default.num_unique_hard_hats_approx",
        "default.avg_repair_price",
        "default.total_repair_cost",
        "default.repair_orders_fact",
        "default.discounted_orders_rate",
        "default.total_repair_order_discounts",
        "default.avg_repair_order_discounts",
        "default.avg_time_to_dispatch",
    }
    assert {node["name"] for node in data} == roads_nodes

    response = await module__client_with_roads_and_acc_revenue.get(
        "/dimensions/common/?dimension=default.hard_hat&dimension=default.us_state"
        "&dimension=default.dispatcher&dimension=default.municipality_dim",
    )
    data = response.json()
    assert {node["name"] for node in data} == roads_nodes

    response = await module__client_with_roads_and_acc_revenue.get(
        "/dimensions/common/?dimension=default.hard_hat&dimension=default.us_state"
        "&dimension=default.dispatcher&dimension=default.payment_type",
    )
    data = response.json()
    assert {node["name"] for node in data} == set()

    response = await module__client_with_roads_and_acc_revenue.get(
        "/dimensions/common/?dimension=default.hard_hat&dimension=default.us_state"
        "&dimension=default.dispatcher&node_type=metric",
    )
    data = response.json()
    assert {node["name"] for node in data} == {
        "default.num_repair_orders",
        "default.num_unique_hard_hats_approx",
        "default.avg_repair_price",
        "default.total_repair_cost",
        "default.discounted_orders_rate",
        "default.total_repair_order_discounts",
        "default.avg_repair_order_discounts",
        "default.avg_time_to_dispatch",
    }
