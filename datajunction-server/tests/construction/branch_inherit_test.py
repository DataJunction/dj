"""
Tests for SQL construction reads that fall back to the default branch.
"""

import json
import re
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import update
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.construction.build_v3.cube_matcher import (
    _metric_graph_has_reaggregate,
)
from datajunction_server.construction.build_v3.filter_requirements import (
    RequirementEntry,
    apply_node_filter_requirements,
    load_declared_entries,
)
from datajunction_server.construction.build_v3.loaders import (
    batch_load_nodes_with_dependencies,
    find_upstream_node_names,
)
from datajunction_server.construction.build_v3.node_query import build_node_sql_v3
from datajunction_server.construction.utils import get_dj_node, missing_to_parent
from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.errors import DJErrorException, DJException, DJNodeNotFound
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.decompose import MetricComponentExtractor
from datajunction_server.sql.parsing.ast import CompileContext
from datajunction_server.sql.parsing.backends.antlr4 import parse

from .build_v3 import assert_sql_equal

TRANSFORM_QUERY = "SELECT line_total, status FROM v3.order_details"
METRIC_QUERY = "SELECT SUM(line_total) FROM v3.order_details"
ORDER_DETAILS_CTE = """
WITH v3_order_details AS (
  SELECT o.order_id, oi.line_number, o.customer_id, o.order_date,
    o.from_location_id, o.to_location_id, o.status, oi.product_id,
    oi.quantity, oi.unit_price, oi.quantity * oi.unit_price AS line_total
  FROM default.v3.orders o
  JOIN default.v3.order_items oi ON o.order_id = oi.order_id
)
"""
REQUIRED = [{"dimension": "v3.date.date_id"}]


async def make_node(
    client: AsyncClient,
    kind: str,
    name: str,
    query: str,
    **fields,
) -> None:
    """
    Creates a node through the API.
    """
    response = await client.post(
        f"/nodes/{kind}/",
        json={
            "name": name,
            "description": "d",
            "query": query,
            "mode": "published",
            **fields,
        },
    )
    assert response.status_code == 201, response.text


async def deactivate(session: AsyncSession, name: str) -> None:
    """
    Marks a node deactivated.
    """
    await session.execute(
        update(Node).where(Node.name == name).values(deactivated_at=datetime.now(UTC)),
    )
    await session.commit()


@pytest_asyncio.fixture
async def branches(
    client_with_build_v3: AsyncClient,
    session: AsyncSession,
) -> AsyncClient:
    """
    A git root `shared` with branches `main` and `feat_x`, and a transform and
    a metric in `main` only.
    """
    client = client_with_build_v3
    for namespace in ("shared", "shared.main", "shared.feat_x"):
        response = await client.post(f"/namespaces/{namespace}")
        assert response.status_code in (200, 201, 409)
    root = await NodeNamespace.get(session, "shared")
    main = await NodeNamespace.get(session, "shared.main")
    branch = await NodeNamespace.get(session, "shared.feat_x")
    assert root and main and branch
    root.github_repo_path = "org/repo"
    root.default_branch = "main"
    main.parent_namespace = "shared"
    main.git_branch = "main"
    branch.parent_namespace = "shared"
    branch.git_branch = "feat-x"
    await session.commit()
    await make_node(client, "transform", "shared.main.big_orders", TRANSFORM_QUERY)
    await make_node(
        client,
        "metric",
        "shared.main.revenue",
        METRIC_QUERY,
        required_filters=REQUIRED,
    )
    return client


@pytest.mark.asyncio
async def test_missing_to_parent_skips_found_names(
    session: AsyncSession,
    branches: AsyncClient,
):
    """
    Names with a result need no lookup, and others map to the default branch.
    """
    assert (
        await missing_to_parent(
            session,
            ["shared.feat_x.big_orders"],
            ["shared.feat_x.big_orders"],
        )
        == {}
    )
    assert await missing_to_parent(
        session,
        ["shared.feat_x.big_orders", "v3.date"],
        ["v3.date"],
    ) == {"shared.feat_x.big_orders": "shared.main.big_orders"}


@pytest.mark.asyncio
async def test_get_dj_node_falls_back(session: AsyncSession, branches: AsyncClient):
    """
    A branch name with no node returns the default branch's current revision.
    """
    revision = await get_dj_node(session, "shared.feat_x.big_orders", inherit=True)
    assert revision.name == "shared.main.big_orders"
    assert revision.query == TRANSFORM_QUERY


@pytest.mark.asyncio
async def test_get_dj_node_branch_hit(session: AsyncSession, branches: AsyncClient):
    """
    A node the branch holds is returned over the default branch's.
    """
    await make_node(
        branches,
        "transform",
        "shared.feat_x.big_orders",
        "SELECT line_total FROM v3.order_details",
    )
    revision = await get_dj_node(session, "shared.feat_x.big_orders", inherit=True)
    assert revision.name == "shared.feat_x.big_orders"
    assert revision.query == "SELECT line_total FROM v3.order_details"


@pytest.mark.asyncio
async def test_get_dj_node_deactivated_blocks(
    session: AsyncSession,
    branches: AsyncClient,
):
    """
    A deactivated branch row keeps the default branch's node hidden.
    """
    await make_node(
        branches,
        "transform",
        "shared.feat_x.big_orders",
        "SELECT line_total FROM v3.order_details",
    )
    await deactivate(session, "shared.feat_x.big_orders")
    revision = await get_dj_node(session, "shared.feat_x.big_orders", inherit=True)
    assert revision.name == "shared.feat_x.big_orders"


@pytest.mark.asyncio
async def test_get_dj_node_kind_mismatch_does_not_fall_back(
    session: AsyncSession,
    branches: AsyncClient,
):
    """
    A branch node of another kind is a miss for that kind, not a fallback.
    """
    await make_node(
        branches,
        "transform",
        "shared.feat_x.revenue",
        "SELECT line_total FROM v3.order_details",
    )
    with pytest.raises(DJErrorException) as exc_info:
        await get_dj_node(
            session,
            "shared.feat_x.revenue",
            kinds={NodeType.METRIC},
            inherit=True,
        )
    assert exc_info.value.dj_error.message == (
        "No node `shared.feat_x.revenue` exists of kind metric."
    )


@pytest.mark.asyncio
async def test_get_dj_node_missing_everywhere(
    session: AsyncSession,
    branches: AsyncClient,
):
    """
    A name the default branch lacks too is still unknown.
    """
    with pytest.raises(DJErrorException) as exc_info:
        await get_dj_node(session, "shared.feat_x.nothing", inherit=True)
    assert exc_info.value.dj_error.message == (
        "No node `shared.feat_x.nothing` exists of kind ."
    )


@pytest.mark.asyncio
async def test_batch_load_falls_back(session: AsyncSession, branches: AsyncClient):
    """
    Nodes come back keyed by the requested name and the default branch's name.
    """
    nodes = await batch_load_nodes_with_dependencies(
        session,
        {"shared.feat_x.big_orders", "v3.order_details"},
        inherit=True,
    )
    assert {name: node.name for name, node in nodes.items()} == {
        "shared.feat_x.big_orders": "shared.main.big_orders",
        "shared.main.big_orders": "shared.main.big_orders",
        "v3.order_details": "v3.order_details",
    }
    assert (
        await batch_load_nodes_with_dependencies(
            session,
            ["shared.feat_x.nothing"],
            inherit=True,
        )
        == {}
    )


@pytest.mark.asyncio
async def test_batch_load_branch_hit_and_deactivated(
    session: AsyncSession,
    branches: AsyncClient,
):
    """
    A branch node is used as is, and a deactivated one blocks the fallback.
    """
    await make_node(
        branches,
        "transform",
        "shared.feat_x.big_orders",
        "SELECT line_total FROM v3.order_details",
    )
    nodes = await batch_load_nodes_with_dependencies(
        session,
        ["shared.feat_x.big_orders"],
        inherit=True,
    )
    assert {name: node.name for name, node in nodes.items()} == {
        "shared.feat_x.big_orders": "shared.feat_x.big_orders",
    }
    await deactivate(session, "shared.feat_x.big_orders")
    assert (
        await batch_load_nodes_with_dependencies(
            session,
            ["shared.feat_x.big_orders"],
            inherit=True,
        )
        == {}
    )


@pytest.mark.asyncio
async def test_find_upstream_falls_back(session: AsyncSession, branches: AsyncClient):
    """
    An inherited starting node is traced through the default branch's node
    and appears under both names.
    """
    main_names, main_map = await find_upstream_node_names(
        session,
        ["shared.main.big_orders"],
        inherit=True,
    )
    assert main_map["shared.main.big_orders"] == ["v3.order_details"]
    assert await find_upstream_node_names(
        session,
        ["shared.feat_x.big_orders"],
        inherit=True,
    ) == (
        main_names | {"shared.feat_x.big_orders"},
        {**main_map, "shared.feat_x.big_orders": ["v3.order_details"]},
    )


@pytest.mark.asyncio
async def test_find_upstream_branch_hit_and_deactivated(
    session: AsyncSession,
    branches: AsyncClient,
):
    """
    A branch node is traced as is, and a deactivated one blocks the fallback.
    """
    await make_node(
        branches,
        "transform",
        "shared.feat_x.big_orders",
        "SELECT line_total FROM v3.order_details",
    )
    names, parent_map = await find_upstream_node_names(
        session,
        ["shared.feat_x.big_orders"],
        inherit=True,
    )
    assert "shared.main.big_orders" not in names
    assert parent_map["shared.feat_x.big_orders"] == ["v3.order_details"]
    await deactivate(session, "shared.feat_x.big_orders")
    assert await find_upstream_node_names(
        session,
        ["shared.feat_x.big_orders"],
        inherit=True,
    ) == (
        set(),
        {},
    )


@pytest.mark.asyncio
async def test_find_upstream_missing_everywhere(
    session: AsyncSession,
    branches: AsyncClient,
):
    """
    A branch name the default branch lacks too adds nothing.
    """
    assert await find_upstream_node_names(
        session,
        ["shared.feat_x.nothing"],
        inherit=True,
    ) == (
        set(),
        {},
    )


@pytest.mark.asyncio
async def test_declared_entries_fall_back(
    session: AsyncSession,
    branches: AsyncClient,
):
    """
    Filter entries of an inherited metric are keyed by the requested name.
    """
    entry = RequirementEntry(
        attributes=("v3.date.date_id",),
        default=None,
        required=True,
        metrics=frozenset({"shared.feat_x.revenue"}),
    )
    assert await load_declared_entries(
        session,
        {"shared.feat_x.revenue"},
        inherit=True,
    ) == {
        "shared.feat_x.revenue": [entry],
    }
    assert (
        await load_declared_entries(session, {"shared.feat_x.nothing"}, inherit=True)
        == {}
    )


@pytest.mark.asyncio
async def test_declared_entries_branch_hit_and_deactivated(
    session: AsyncSession,
    branches: AsyncClient,
):
    """
    A branch metric keeps its own entries, and a deactivated one blocks the fallback.
    """
    await make_node(branches, "metric", "shared.feat_x.revenue", METRIC_QUERY)
    assert await load_declared_entries(
        session,
        {"shared.feat_x.revenue"},
        inherit=True,
    ) == {
        "shared.feat_x.revenue": [],
    }
    await deactivate(session, "shared.feat_x.revenue")
    assert (
        await load_declared_entries(session, {"shared.feat_x.revenue"}, inherit=True)
        == {}
    )


@pytest.mark.asyncio
async def test_node_filter_requirements_fall_back(
    session: AsyncSession,
    branches: AsyncClient,
):
    """
    An inherited metric keeps the default branch's filter requirements, and an
    inherited transform has none.
    """
    filters = ["v3.date.date_id = 20250101"]
    assert await apply_node_filter_requirements(
        session,
        "shared.feat_x.revenue",
        filters,
        inherit=True,
    ) == (filters, [])
    assert await apply_node_filter_requirements(
        session,
        "shared.feat_x.big_orders",
        ["a = 1"],
        inherit=True,
    ) == (["a = 1"], [])
    assert await apply_node_filter_requirements(
        session,
        "shared.feat_x.nothing",
        ["a = 1"],
        inherit=True,
    ) == (["a = 1"], [])


@pytest.mark.asyncio
async def test_node_filter_requirements_deactivated_blocks(
    session: AsyncSession,
    branches: AsyncClient,
):
    """
    A deactivated branch metric does not pick up the default branch's requirements.
    """
    await make_node(branches, "metric", "shared.feat_x.revenue", METRIC_QUERY)
    await deactivate(session, "shared.feat_x.revenue")
    assert await apply_node_filter_requirements(
        session,
        "shared.feat_x.revenue",
        ["a = 1"],
        inherit=True,
    ) == (["a = 1"], [])


async def set_reaggregate(session: AsyncSession, name: str, value: bool) -> None:
    """
    Sets the reaggregate flag on a node's revisions.
    """
    await session.execute(
        update(NodeRevision).where(NodeRevision.name == name).values(reaggregate=value),
    )
    await session.commit()


@pytest.mark.asyncio
async def test_reaggregate_check_falls_back(
    session: AsyncSession,
    branches: AsyncClient,
):
    """
    A branch name with no metric uses the default branch's reaggregate flag.
    """
    assert await _metric_graph_has_reaggregate(
        session,
        ["shared.feat_x.revenue"],
        inherit=True,
    ) is (False)
    await set_reaggregate(session, "shared.main.revenue", True)
    assert await _metric_graph_has_reaggregate(
        session,
        ["shared.feat_x.revenue"],
        inherit=True,
    ) is (True)


@pytest.mark.asyncio
async def test_reaggregate_check_branch_hit_and_deactivated(
    session: AsyncSession,
    branches: AsyncClient,
):
    """
    A branch metric is checked as is, and a deactivated one blocks the fallback.
    """
    await set_reaggregate(session, "shared.main.revenue", True)
    await make_node(branches, "metric", "shared.feat_x.revenue", METRIC_QUERY)
    assert await _metric_graph_has_reaggregate(
        session,
        ["shared.feat_x.revenue"],
        inherit=True,
    ) is (False)
    await deactivate(session, "shared.feat_x.revenue")
    assert await _metric_graph_has_reaggregate(
        session,
        ["shared.feat_x.revenue"],
        inherit=True,
    ) is (False)


@pytest.mark.asyncio
async def test_extractor_falls_back(session: AsyncSession, branches: AsyncClient):
    """
    An extractor for a branch name with no metric reads the default branch's.
    """
    main = await MetricComponentExtractor.from_node_name(
        "shared.main.revenue",
        session,
        inherit=True,
    )
    inherited = await MetricComponentExtractor.from_node_name(
        "shared.feat_x.revenue",
        session,
        inherit=True,
    )
    assert inherited._node_revision_id == main._node_revision_id


@pytest.mark.asyncio
async def test_extractor_branch_hit_and_deactivated(
    session: AsyncSession,
    branches: AsyncClient,
):
    """
    A branch metric is used as is, and a deactivated one blocks the fallback.
    """
    main = await MetricComponentExtractor.from_node_name(
        "shared.main.revenue",
        session,
        inherit=True,
    )
    await make_node(branches, "metric", "shared.feat_x.revenue", METRIC_QUERY)
    own = await MetricComponentExtractor.from_node_name(
        "shared.feat_x.revenue",
        session,
        inherit=True,
    )
    assert own._node_revision_id != main._node_revision_id
    await deactivate(session, "shared.feat_x.revenue")
    # A deactivated metric's current revision is still found by name.
    again = await MetricComponentExtractor.from_node_name(
        "shared.feat_x.revenue",
        session,
        inherit=True,
    )
    assert again._node_revision_id == own._node_revision_id


@pytest.mark.asyncio
async def test_extractor_missing_everywhere(
    session: AsyncSession,
    branches: AsyncClient,
):
    """
    A name the default branch lacks too raises.
    """
    with pytest.raises(NoResultFound):
        await MetricComponentExtractor.from_node_name(
            "shared.feat_x.nothing",
            session,
            inherit=True,
        )


async def warm(session: AsyncSession, name: str) -> Node:
    """
    Loads a node as the endpoint does, and keeps it in the session.
    """
    session.expunge_all()
    node = await Node.get_by_name(session, name, inherit=True)
    assert node is not None
    return node


def allow_all() -> MagicMock:
    """
    An access checker that allows everything.
    """
    checker = MagicMock()
    checker.check = AsyncMock()
    return checker


@pytest.mark.asyncio
async def test_node_sql_falls_back(session: AsyncSession, branches: AsyncClient):
    """
    A branch name with no node builds the default branch's node.
    """
    node = await warm(session, "shared.feat_x.big_orders")
    generated = await build_node_sql_v3(
        session,
        "shared.feat_x.big_orders",
        access_checker=allow_all(),
    )
    assert_sql_equal(
        generated.sql,
        ORDER_DETAILS_CTE + "SELECT line_total, status FROM v3_order_details",
    )
    assert node.name == "shared.main.big_orders"


@pytest.mark.asyncio
async def test_node_sql_branch_hit_and_deactivated(
    session: AsyncSession,
    branches: AsyncClient,
):
    """
    A branch node builds as is, and a deactivated one blocks the fallback.
    """
    await make_node(
        branches,
        "transform",
        "shared.feat_x.big_orders",
        "SELECT line_total FROM v3.order_details",
    )
    node = await warm(session, "shared.feat_x.big_orders")
    generated = await build_node_sql_v3(
        session,
        "shared.feat_x.big_orders",
        access_checker=allow_all(),
    )
    assert_sql_equal(
        generated.sql,
        ORDER_DETAILS_CTE + "SELECT line_total FROM v3_order_details",
    )
    assert node.name == "shared.feat_x.big_orders"
    await deactivate(session, "shared.feat_x.big_orders")
    with pytest.raises(DJNodeNotFound) as exc_info:
        await build_node_sql_v3(
            session,
            "shared.feat_x.big_orders",
            access_checker=allow_all(),
        )
    assert exc_info.value.message == (
        "A node with name `shared.feat_x.big_orders` does not exist."
    )


@pytest.mark.asyncio
async def test_metric_sql_falls_back(session: AsyncSession, branches: AsyncClient):
    """
    A branch metric name with no node builds through the metrics path.
    """
    filters = ["v3.date.date_id = 20250101"]
    node = await warm(session, "shared.feat_x.revenue")
    inherited = await build_node_sql_v3(
        session,
        "shared.feat_x.revenue",
        filters=filters,
        access_checker=allow_all(),
    )
    main = await build_node_sql_v3(
        session,
        "shared.main.revenue",
        filters=filters,
        access_checker=allow_all(),
    )
    assert inherited.sql == main.sql
    assert node.name == "shared.main.revenue"


@pytest.mark.asyncio
async def test_exact_lookups_do_not_fall_back(
    session: AsyncSession,
    branches: AsyncClient,
):
    """
    Without ``inherit`` every lookup keeps the exact name.
    """
    with pytest.raises(DJErrorException) as exc_info:
        await get_dj_node(session, "shared.feat_x.big_orders")
    assert exc_info.value.dj_error.message == (
        "No node `shared.feat_x.big_orders` exists of kind ."
    )
    assert (
        await batch_load_nodes_with_dependencies(session, ["shared.feat_x.big_orders"])
        == {}
    )
    assert await find_upstream_node_names(session, ["shared.feat_x.big_orders"]) == (
        set(),
        {},
    )
    assert await load_declared_entries(session, {"shared.feat_x.revenue"}) == {}
    assert await apply_node_filter_requirements(
        session,
        "shared.feat_x.revenue",
        ["a = 1"],
    ) == (["a = 1"], [])
    await set_reaggregate(session, "shared.main.revenue", True)
    assert (
        await _metric_graph_has_reaggregate(session, ["shared.feat_x.revenue"]) is False
    )
    with pytest.raises(NoResultFound):
        await MetricComponentExtractor.from_node_name("shared.feat_x.revenue", session)


@pytest.mark.asyncio
async def test_compile_inherit_option(session: AsyncSession, branches: AsyncClient):
    """
    Compiling falls back to the default branch only when asked to.
    """
    query = "SELECT line_total FROM shared.feat_x.big_orders"
    exact = CompileContext(session=session, exception=DJException())
    await parse(query).compile(exact)
    assert [
        re.sub(r"kind .*\.$", "kind K.", error.message)
        for error in exact.exception.errors
    ] == [
        *["No node `shared.feat_x.big_orders` exists of kind K."] * 5,
        "Column `line_total` does not exist on any valid table.",
        "No node `shared.feat_x.big_orders` exists of kind K.",
    ]

    inherited = CompileContext(session=session, exception=DJException(), inherit=True)
    await parse(query).compile(inherited)
    assert inherited.exception.errors == []


async def post_json(client: AsyncClient, path: str, body: dict, method: str = "post"):
    """
    Sends a request and returns its status and JSON body.
    """
    response = await getattr(client, method)(path, json=body)
    return response.status_code, response.json()


@pytest.mark.asyncio
async def test_create_rejects_default_branch_names(
    session: AsyncSession,
    branches: AsyncClient,
):
    """
    A node created in a branch cannot reference a node only the default
    branch holds, and fails as it does in a plain namespace.
    """
    await branches.post("/namespaces/plain")

    def body(name: str, parent: str) -> dict:
        return {
            "name": name,
            "description": "d",
            "query": f"SELECT line_total FROM {parent}",
            "mode": "published",
        }

    plain = await post_json(
        branches,
        "/nodes/transform/",
        body("plain.child", "plain.big_orders"),
    )
    branch = await post_json(
        branches,
        "/nodes/transform/",
        body("shared.feat_x.child", "shared.feat_x.big_orders"),
    )
    assert plain[0] == 422
    assert branch == (
        422,
        json.loads(
            json.dumps(plain[1]).replace(
                "plain.big_orders",
                "shared.feat_x.big_orders",
            ),
        ),
    )
    assert branch[1]["message"] == (
        "Node definition contains references to nodes that do not exist: "
        "shared.feat_x.big_orders\n"
        "Cannot resolve type of column `line_total` in SELECT  line_total \n"
        " FROM shared.feat_x.big_orders\n"
    )


def swap_names(payload: dict, pairs: dict[str, str]) -> dict:
    """
    Renames nodes in a response body.
    """
    text = json.dumps(payload)
    for old, new in pairs.items():
        text = text.replace(old, new)
    return json.loads(text)


@pytest.mark.asyncio
async def test_update_rejects_default_branch_names(
    session: AsyncSession,
    branches: AsyncClient,
):
    """
    Updating a branch node to reference a default-branch-only node fails as
    it does in a plain namespace.
    """
    await branches.post("/namespaces/plain")
    for name in ("plain.child", "shared.feat_x.child"):
        await make_node(branches, "transform", name, TRANSFORM_QUERY)

    plain = await post_json(
        branches,
        "/nodes/plain.child",
        {"query": "SELECT line_total FROM plain.big_orders"},
        method="patch",
    )
    branch = await post_json(
        branches,
        "/nodes/shared.feat_x.child",
        {"query": "SELECT line_total FROM shared.feat_x.big_orders"},
        method="patch",
    )
    assert plain[0] == 422
    assert branch == (
        422,
        swap_names(plain[1], {"plain.big_orders": "shared.feat_x.big_orders"}),
    )
    assert branch[1]["message"] == (
        "Node definition contains references to nodes that do not exist: "
        "shared.feat_x.big_orders\n"
        "Cannot resolve type of column `line_total` in SELECT  line_total \n"
        " FROM shared.feat_x.big_orders\n"
    )


@pytest.mark.asyncio
async def test_link_rejects_default_branch_names(
    session: AsyncSession,
    branches: AsyncClient,
):
    """
    Linking to a dimension only the default branch holds fails as it does in
    a plain namespace.
    """
    await branches.post("/namespaces/plain")
    for name in ("plain.child", "shared.feat_x.child"):
        await make_node(branches, "transform", name, TRANSFORM_QUERY)
    await make_node(
        branches,
        "dimension",
        "shared.main.dim_status",
        "SELECT status FROM v3.order_details",
        primary_key=["status"],
    )

    def link(node: str, dimension: str):
        return post_json(
            branches,
            f"/nodes/{node}/link",
            {
                "dimension_node": dimension,
                "join_type": "left",
                "join_on": f"{node}.status = {dimension}.status",
            },
        )

    plain = await link("plain.child", "plain.dim_status")
    branch = await link("shared.feat_x.child", "shared.feat_x.dim_status")
    assert plain[0] == 404
    assert branch == (
        404,
        swap_names(
            plain[1],
            {
                "plain.dim_status": "shared.feat_x.dim_status",
                "plain.child": "shared.feat_x.child",
            },
        ),
    )
    assert branch[1]["message"] == (
        "A node with name `shared.feat_x.dim_status` does not exist."
    )
