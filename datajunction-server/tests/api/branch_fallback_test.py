"""
Tests that read endpoints resolve branch names through the default branch.

The fixture builds git root `foo` with default branch `bar` (the namespaced
roads examples) and a branch `foo.feat_x` that holds only some nodes. Branch
nodes are created while `foo.feat_x` is a plain namespace; `link` then turns
it into a branch. A node the branch holds answers the same before and after
linking, a node it lacks answers like the default branch's node, and a
deactivated branch node stays not found.
"""

import json
import re
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.namespace import NodeNamespace

BRANCH = "foo.feat_x"
MAIN = "foo.bar"

NAMES = {
    "metric": {
        "hit": f"{BRANCH}.num_repair_orders",
        "fallback": f"{BRANCH}.total_repair_cost",
        "deleted": f"{BRANCH}.avg_repair_price",
    },
    "dimension": {
        "hit": f"{BRANCH}.hard_hat",
        "fallback": f"{BRANCH}.us_state",
        "deleted": f"{BRANCH}.dispatcher",
    },
    "cube": {
        "hit": f"{BRANCH}.cube_hit",
        "fallback": f"{BRANCH}.cube_main",
        "deleted": f"{BRANCH}.cube_gone",
    },
}

METRIC_QUERIES = {
    "num_repair_orders": "SELECT count(repair_order_id) FROM foo.bar.repair_orders",
    "avg_repair_price": "SELECT avg(price) FROM foo.bar.repair_order_details",
}
DIMENSION_QUERIES = {
    "hard_hat": "SELECT hard_hat_id, city FROM foo.bar.hard_hats",
    "dispatcher": "SELECT dispatcher_id, company_name FROM foo.bar.dispatchers",
}
CUBE_NAMES = ["cube_hit", "cube_main", "cube_gone"]


async def create_metric(client: AsyncClient, name: str, query: str) -> None:
    """
    Creates a metric node.
    """
    response = await client.post(
        "/nodes/metric/",
        json={
            "name": name,
            "description": f"Metric {name}",
            "query": query,
            "mode": "published",
        },
    )
    assert response.status_code == 201, response.json()


async def create_dimension(client: AsyncClient, name: str, query: str) -> None:
    """
    Creates a dimension node keyed on its first column.
    """
    response = await client.post(
        "/nodes/dimension/",
        json={
            "name": name,
            "description": f"Dimension {name}",
            "query": query,
            "primary_key": [query.split()[1].rstrip(",")],
            "mode": "published",
        },
    )
    assert response.status_code == 201, response.json()


async def create_cube(client: AsyncClient, name: str) -> None:
    """
    Creates a cube node over a default branch metric.
    """
    response = await client.post(
        "/nodes/cube/",
        json={
            "name": name,
            "description": f"Cube {name}",
            "metrics": ["foo.bar.num_repair_orders"],
            "dimensions": ["foo.bar.hard_hat.city"],
            "mode": "published",
        },
    )
    assert response.status_code == 201, response.json()


@pytest_asyncio.fixture
async def link(
    client: AsyncClient,
    session: AsyncSession,
) -> Callable[[], Awaitable[None]]:
    """
    Creates the branch's nodes and returns a function that links the branch.
    """
    assert (await client.post(f"/namespaces/{BRANCH}/")).status_code == 201
    for short in ("num_repair_orders", "avg_repair_price"):
        await create_metric(client, f"{BRANCH}.{short}", METRIC_QUERIES[short])
    for short in ("hard_hat", "dispatcher"):
        await create_dimension(client, f"{BRANCH}.{short}", DIMENSION_QUERIES[short])
    for short in CUBE_NAMES:
        await create_cube(client, f"{MAIN}.{short}")
    for short in ("cube_hit", "cube_gone"):
        await create_cube(client, f"{BRANCH}.{short}")
    for name in (NAMES["metric"]["deleted"], NAMES["dimension"]["deleted"]):
        assert (await client.delete(f"/nodes/{name}/")).status_code == 200
    assert (
        await client.delete(f"/nodes/{NAMES['cube']['deleted']}/")
    ).status_code == 200

    async def link_branch() -> None:
        session.add(
            NodeNamespace(
                namespace="foo",
                github_repo_path="org/repo",
                default_branch="bar",
            ),
        )
        await session.flush()
        for namespace, branch in ((MAIN, "bar"), (BRANCH, "feat-x")):
            await session.execute(
                update(NodeNamespace)
                .where(NodeNamespace.namespace == namespace)
                .values(git_branch=branch, parent_namespace="foo"),
            )
        await session.commit()

    return link_branch


def plain(body: Any) -> Any:
    """
    Drops git info, which changes when a namespace becomes a branch.
    """
    if isinstance(body, dict):
        return {key: plain(value) for key, value in body.items() if key != "git_info"}
    if isinstance(body, list):
        return [plain(item) for item in body]
    return body


def without_git(text: str) -> str:
    """
    Drops the git repository block from MCP node details.
    """
    return re.sub(r"Git Repository:\n(  .*\n)+\n", "", text)


def to_main(body: Any, name: str) -> Any:
    """
    Rewrites the branch name in a response to its default branch name.
    """
    main_name = name.replace(BRANCH, MAIN, 1)
    text = json.dumps(body).replace(name, main_name)
    text = text.replace(name.replace(".", "_"), main_name.replace(".", "_"))
    return json.loads(text)


# (method, path with {name}, query params, node kind)
ENDPOINTS = [
    ("GET", "/nodes/{name}/", None, "metric"),
    ("GET", "/nodes/{name}/", None, "dimension"),
    ("GET", "/nodes/{name}/checks/", None, "metric"),
    ("GET", "/nodes/{name}/revisions/", None, "metric"),
    ("GET", "/nodes/{name}/upstream/", None, "metric"),
    ("GET", "/nodes/{name}/dag/", None, "metric"),
    ("GET", "/nodes/{name}/dimension-dag/", None, "dimension"),
    ("GET", "/nodes/{name}/lineage/", None, "dimension"),
    ("GET", "/nodes/{name}/materializations/", None, "metric"),
    ("GET", "/nodes/{name}/availability/", None, "metric"),
    ("GET", "/metrics/{name}/", None, "metric"),
    ("GET", "/metrics/common/dimensions/", {"metric": "{name}"}, "metric"),
    ("GET", "/dimensions/{name}/nodes/", None, "dimension"),
    ("GET", "/dimensions/common/", {"dimension": "{name}"}, "dimension"),
    ("GET", "/nodes/{name}/hierarchies/", None, "dimension"),
    ("GET", "/cubes/{name}/materialization", None, "cube"),
    ("GET", "/cubes/{name}/dimensions/sql", None, "cube"),
    ("GET", "/cubes/{name}/view-ddl", None, "cube"),
    ("GET", "/preaggs/", {"node_name": "{name}"}, "metric"),
    ("GET", "/sql/", {"metrics": "{name}"}, "metric"),
]


async def call(
    client: AsyncClient,
    method: str,
    path: str,
    params: dict[str, str] | None,
    name: str,
):
    """
    Calls an endpoint with the node name filled in.
    """
    return await client.request(
        method,
        path.format(name=name),
        params={key: value.format(name=name) for key, value in (params or {}).items()},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("method, path, params, kind", ENDPOINTS)
async def test_endpoint_resolves_through_branch(
    client: AsyncClient,
    link: Callable[[], Awaitable[None]],
    method: str,
    path: str,
    params: dict[str, str] | None,
    kind: str,
):
    """
    A held node is unchanged, a missing node falls back, a deleted node is gone.
    """
    names = NAMES[kind]
    fallback = names["fallback"]
    main_name = fallback.replace(BRANCH, MAIN, 1)

    hit_before = await call(client, method, path, params, names["hit"])
    deleted_before = await call(client, method, path, params, names["deleted"])
    fallback_before = await call(client, method, path, params, fallback)
    assert fallback_before.status_code in (404, 422)

    await link()

    main_response = await call(client, method, path, params, main_name)
    assert main_response.status_code not in (404, 500)

    hit_after = await call(client, method, path, params, names["hit"])
    assert (hit_after.status_code, plain(hit_after.json())) == (
        hit_before.status_code,
        plain(hit_before.json()),
    )

    deleted_after = await call(client, method, path, params, names["deleted"])
    assert (deleted_after.status_code, plain(deleted_after.json())) == (
        deleted_before.status_code,
        plain(deleted_before.json()),
    )

    fallback_after = await call(client, method, path, params, fallback)
    assert fallback_after.status_code == main_response.status_code
    assert plain(to_main(fallback_after.json(), fallback)) == plain(
        main_response.json(),
    )


COMMON_DIMENSIONS_QUERY = """
query ($nodes: [String!]) {
  commonDimensions(nodes: $nodes) {
    name
    dimensionNode {
      name
    }
  }
}
"""


async def common_dimensions(client: AsyncClient, names: list[str]) -> Any:
    """
    Runs the commonDimensions query for the given nodes.
    """
    response = await client.post(
        "/graphql",
        json={"query": COMMON_DIMENSIONS_QUERY, "variables": {"nodes": names}},
    )
    assert response.status_code == 200
    return response.json()


@pytest.mark.asyncio
async def test_graphql_common_dimensions(
    client: AsyncClient,
    link: Callable[[], Awaitable[None]],
):
    """
    The commonDimensions query resolves branch names like the REST endpoints.
    """
    metrics = NAMES["metric"]
    main = await common_dimensions(client, ["foo.bar.total_repair_cost"])
    assert main["data"]["commonDimensions"]
    hit_before = await common_dimensions(client, [metrics["hit"]])
    deleted_before = await common_dimensions(client, [metrics["deleted"]])
    assert deleted_before == {"data": {"commonDimensions": []}}

    await link()

    assert await common_dimensions(client, [metrics["hit"]]) == hit_before
    assert await common_dimensions(client, [metrics["deleted"]]) == deleted_before
    assert await common_dimensions(client, [metrics["fallback"]]) == main


@pytest.mark.asyncio
async def test_graphql_loaders_and_resolvers(
    client: AsyncClient,
    session: AsyncSession,
    link: Callable[[], Awaitable[None]],
    monkeypatch: pytest.MonkeyPatch,
):
    """
    Node loaders and resolvers fall back for missing names only.
    """
    from contextlib import asynccontextmanager

    from datajunction_server.api.graphql import dataloaders
    from datajunction_server.api.graphql.resolvers.nodes import (
        get_metrics,
        get_node_by_name,
        resolve_metrics_and_dimensions,
    )
    from datajunction_server.api.graphql.scalars.sql import CubeDefinition

    @asynccontextmanager
    async def test_session(request, **kwargs):
        yield session

    monkeypatch.setattr(dataloaders, "session_context", test_session)
    await link()

    metrics = NAMES["metric"]
    asked = [metrics["hit"], metrics["fallback"], metrics["deleted"]]
    expected = [metrics["hit"], "foo.bar.total_repair_cost", None]

    def names(nodes):
        return [node.name if node else None for node in nodes]

    assert (
        names(await dataloaders.batch_load_nodes_by_name_only(asked, None)) == expected
    )
    assert (
        names(
            await dataloaders.batch_load_nodes([(name, None) for name in asked], None),
        )
        == expected
    )
    fields = json.dumps({"name": None})
    assert (
        names(
            await dataloaders.batch_load_nodes_with_fields(
                [(name, fields) for name in asked],
                None,
            ),
        )
        == expected
    )

    node = await get_node_by_name(session, {"type": None}, metrics["fallback"])
    assert node is not None
    assert node.name == "foo.bar.total_repair_cost"
    assert names(await get_metrics(session, asked)) == expected[:2]

    resolved = await resolve_metrics_and_dimensions(
        session,
        CubeDefinition(cube=NAMES["cube"]["fallback"]),  # type: ignore[call-arg]
    )
    assert resolved == (["foo.bar.num_repair_orders"], ["foo.bar.hard_hat.city"])


@pytest.mark.asyncio
async def test_mcp_tools(
    client: AsyncClient,
    session: AsyncSession,
    link: Callable[[], Awaitable[None]],
):
    """
    MCP read tools resolve branch names like the REST endpoints.
    """
    from datajunction_server.mcp import tools
    from datajunction_server.mcp.context import _session_var

    metric = NAMES["metric"]
    main_metric = "foo.bar.total_repair_cost"
    token = _session_var.set(session)
    try:
        before = {
            "details": await tools.get_node_details(metric["hit"]),
            "dims": await tools.get_node_dimensions(metric["hit"]),
            "lineage": await tools.get_node_lineage(metric["hit"]),
            "common": await tools.get_common(metrics=[metric["hit"]]),
            "deleted": await tools.get_node_details(metric["deleted"]),
            "deleted_dims": await tools.get_node_dimensions(metric["deleted"]),
        }
        main = {
            "details": await tools.get_node_details(main_metric),
            "dims": await tools.get_node_dimensions(main_metric),
            "lineage": await tools.get_node_lineage(main_metric),
            "common": await tools.get_common(metrics=[main_metric]),
        }
        await link()
        after = {
            "details": await tools.get_node_details(metric["hit"]),
            "dims": await tools.get_node_dimensions(metric["hit"]),
            "lineage": await tools.get_node_lineage(metric["hit"]),
            "common": await tools.get_common(metrics=[metric["hit"]]),
            "deleted": await tools.get_node_details(metric["deleted"]),
            "deleted_dims": await tools.get_node_dimensions(metric["deleted"]),
        }
        after["details"] = without_git(after["details"])
        assert after == before

        fallback = metric["fallback"]
        assert without_git(await tools.get_node_details(fallback)) == (
            without_git(main["details"])
        )
        assert await tools.get_node_dimensions(fallback) == (
            main["dims"].replace(main_metric, fallback)
        )
        assert await tools.get_node_lineage(fallback) == (
            main["lineage"].replace(main_metric, fallback)
        )
        assert await tools.get_common(metrics=[fallback]) == (
            main["common"].replace(main_metric, fallback)
        )

        dimension = NAMES["dimension"]["fallback"]
        main_dimension = "foo.bar.us_state"
        main_nodes = await tools.get_common(dimensions=[main_dimension])
        assert await tools.get_common(dimensions=[dimension]) == (
            main_nodes.replace(main_dimension, dimension)
        )
    finally:
        _session_var.reset(token)


@pytest.mark.asyncio
async def test_similarity_and_view_and_measures(
    client: AsyncClient,
    link: Callable[[], Awaitable[None]],
):
    """
    Similarity, semantic views and measures SQL resolve branch names.
    """
    metric = NAMES["metric"]
    cube = NAMES["cube"]
    main_cube = "foo.bar.cube_main"
    similarity = "/nodes/similarity/{}/{}"
    hit_before = await client.get(similarity.format(metric["hit"], metric["hit"]))
    assert (
        await client.get(
            similarity.format(metric["fallback"], metric["hit"]),
        )
        != hit_before
    )

    await link()

    main_similarity = await client.get(
        similarity.format("foo.bar.total_repair_cost", metric["hit"]),
    )
    assert main_similarity.status_code == 200
    assert (
        hit_before.json()
        == (await client.get(similarity.format(metric["hit"], metric["hit"]))).json()
    )
    fallback_similarity = await client.get(
        similarity.format(metric["fallback"], metric["hit"]),
    )
    assert fallback_similarity.json() == main_similarity.json()
    deleted_similarity = await client.get(
        similarity.format(metric["deleted"], metric["hit"]),
    )
    assert deleted_similarity.status_code == 404

    main_view = await client.post(f"/semantic/views/{main_cube}")
    assert main_view.status_code == 200
    view = await client.post(f"/semantic/views/{cube['fallback']}")
    assert view.json() == main_view.json()
    assert (await client.post(f"/semantic/views/{cube['hit']}")).status_code == 200

    main_sql = await client.get("/sql/measures/v3/", params={"cube": main_cube})
    assert main_sql.status_code == 200
    sql = await client.get("/sql/measures/v3/", params={"cube": cube["fallback"]})
    assert sql.json() == main_sql.json()

    values = "/cubes/{}/dimensions/data"
    missing = await client.get(values.format("foo.feat_x.no_such_cube"))
    assert missing.status_code == 404
