from httpx import AsyncClient
import pytest
import pytest_asyncio


@pytest_asyncio.fixture(scope="module")
async def module__client_with_system(
    module__client_with_roads: AsyncClient,
) -> AsyncClient:
    """
    Fixture to provide a client with system endpoints.
    """
    await module__client_with_roads.post("/register/table/dj_metadata/public/node")
    await module__client_with_roads.post(
        "/register/table/dj_metadata/public/noderevision",
    )
    await module__client_with_roads.post("/namespaces/system.dj")
    await module__client_with_roads.post(
        "/nodes/dimension",
        json={
            "name": "system.dj.nodes",
            "display_name": "Nodes",
            "description": "Nodes in the system",
            "query": """SELECT
    id,
    N.name,
    NR.display_name,
    cast(N.type AS STRING) type,
    N.namespace,
    N.created_by_id,
    N.created_at,
    CAST(TO_CHAR(CAST(N.created_at AS date), 'YYYYMMDD') AS integer) AS created_at_date,
    CAST(TO_CHAR(CAST(DATE_TRUNC('week', N.created_at) AS date), 'YYYYMMDD') AS integer) AS created_at_week,
    N.current_version,
    CASE WHEN deactivated_at IS NULL THEN true ELSE false END AS is_active,
    NR.status,
    NR.description,
    NR.id AS current_revision_id
  FROM source.dj_metadata.public.node N
  JOIN source.dj_metadata.public.noderevision NR ON NR.node_id = N.id AND NR.version = N.current_version""",
            "primary_key": ["id"],
            "mode": "published",
        },
    )
    await module__client_with_roads.post(
        "/nodes/dimension",
        json={
            "name": "system.dj.node_type",
            "display_name": "Node Type",
            "description": "node types",
            "query": """SELECT
    type, type_upper
  FROM
  VALUES
      ('source', 'SOURCE'),
      ('transform', 'TRANSFORM'),
      ('dimension', 'DIMENSION'),
      ('metric', 'METRIC'),
      ('cube', 'CUBE')
  AS t (type, type_upper)""",
            "primary_key": ["type_upper"],
            "mode": "published",
        },
    )
    await module__client_with_roads.post(
        "/nodes/system.dj.nodes/link",
        json={
            "dimension_node": "system.dj.node_type",
            "join_on": "system.dj.nodes.type = system.dj.node_type.type_upper",
        },
    )
    await module__client_with_roads.post(
        "/nodes/metric",
        json={
            "name": "system.dj.number_of_nodes",
            "display_name": "Number of Nodes",
            "description": "Number of nodes in the system",
            "query": "SELECT COUNT(*) AS count FROM system.dj.nodes",
            "mode": "published",
        },
    )
    return module__client_with_roads


@pytest.mark.asyncio
async def test_system_metrics(module__client_with_system: AsyncClient) -> None:
    """
    Test ``GET /system/metrics``.
    """
    response = await module__client_with_system.get("/system/metrics")
    data = response.json()
    assert data == ["system.dj.number_of_nodes"]


@pytest.mark.parametrize(
    "metric, dimensions, filters, expected",
    [
        (
            "system.dj.number_of_nodes",
            [],
            [],
            [
                [
                    {
                        "col": "system.dj.number_of_nodes",
                        "value": 41,
                    },
                ],
            ],
        ),
        (
            "system.dj.number_of_nodes",
            ["system.dj.node_type.type"],
            ["system.dj.nodes.is_active = true"],
            [
                [
                    {
                        "col": "system.dj.node_type.type",
                        "value": "dimension",
                    },
                    {
                        "col": "system.dj.number_of_nodes",
                        "value": 13,
                    },
                ],
                [
                    {
                        "col": "system.dj.node_type.type",
                        "value": "metric",
                    },
                    {
                        "col": "system.dj.number_of_nodes",
                        "value": 10,
                    },
                ],
                [
                    {
                        "col": "system.dj.node_type.type",
                        "value": "source",
                    },
                    {
                        "col": "system.dj.number_of_nodes",
                        "value": 15,
                    },
                ],
                [
                    {
                        "col": "system.dj.node_type.type",
                        "value": "transform",
                    },
                    {
                        "col": "system.dj.number_of_nodes",
                        "value": 3,
                    },
                ],
            ],
        ),
    ],
)
@pytest.mark.asyncio
async def test_system_metric_data(
    module__client_with_system: AsyncClient,
    metric: str,
    dimensions: list[str],
    filters: list[str],
    expected: list[list[dict]],
) -> None:
    """
    Test ``GET /system/data``.
    """
    response = await module__client_with_system.get(
        f"/system/data/{metric}",
        params={
            "dimensions": dimensions,
            "filters": filters,
        },
    )
    data = sorted(response.json(), key=lambda x: x[0]["value"])
    assert data == sorted(expected, key=lambda x: x[0]["value"])


@pytest.mark.asyncio
async def test_system_dimension_stats(module__client_with_system: AsyncClient) -> None:
    """
    Test ``GET /system/dimensions``.
    """
    response = await module__client_with_system.get("/system/dimensions")
    data = response.json()

    assert response.status_code == 200
    assert data == [
        {"name": "default.dispatcher", "indegree": 3, "cube_count": 0},
        {"name": "default.hard_hat_to_delete", "indegree": 2, "cube_count": 0},
        {"name": "default.us_state", "indegree": 2, "cube_count": 0},
        {"name": "default.municipality_dim", "indegree": 2, "cube_count": 0},
        {"name": "default.hard_hat", "indegree": 2, "cube_count": 0},
        {"name": "default.repair_order", "indegree": 2, "cube_count": 0},
        {"name": "default.contractor", "indegree": 1, "cube_count": 0},
        {"name": "system.dj.node_type", "indegree": 1, "cube_count": 0},
        {"name": "default.hard_hat_2", "indegree": 0, "cube_count": 0},
        {"name": "default.local_hard_hats", "indegree": 0, "cube_count": 0},
        {"name": "default.local_hard_hats_1", "indegree": 0, "cube_count": 0},
        {"name": "default.local_hard_hats_2", "indegree": 0, "cube_count": 0},
        {"name": "system.dj.nodes", "indegree": 0, "cube_count": 0},
    ]
