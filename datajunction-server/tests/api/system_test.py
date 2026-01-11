from httpx import AsyncClient
import pytest
import pytest_asyncio


@pytest_asyncio.fixture(scope="module", loop_scope="module")
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


@pytest.mark.asyncio
async def test_system_metric_data_no_dimensions(
    module__client_with_system: AsyncClient,
) -> None:
    """
    Test ``GET /system/data`` without dimensions.
    """
    response = await module__client_with_system.get(
        "/system/data/system.dj.number_of_nodes",
        params={
            "dimensions": [],
            "filters": [],
        },
    )
    data = response.json()
    assert len(data) == 1
    assert len(data[0]) == 1
    assert data[0][0]["col"] == "system.dj.number_of_nodes"
    # With all examples loaded, there will be more nodes than just roads
    assert data[0][0]["value"] >= 42


@pytest.mark.asyncio
async def test_system_metric_data_with_dimensions(
    module__client_with_system: AsyncClient,
) -> None:
    """
    Test ``GET /system/data`` with dimensions.
    """
    response = await module__client_with_system.get(
        "/system/data/system.dj.number_of_nodes",
        params={
            "dimensions": ["system.dj.node_type.type"],
            "filters": ["system.dj.nodes.is_active = true"],
        },
    )
    data = response.json()

    # Should have results for each node type
    type_values = {
        row[0]["value"] for row in data if row[0]["col"] == "system.dj.node_type.type"
    }
    assert "dimension" in type_values
    assert "metric" in type_values
    assert "source" in type_values
    assert "transform" in type_values

    # Each row should have counts >= the roads-only values
    for row in data:
        type_col = next(
            (c for c in row if c["col"] == "system.dj.node_type.type"),
            None,
        )
        count_col = next(
            (c for c in row if c["col"] == "system.dj.number_of_nodes"),
            None,
        )
        if type_col and count_col:
            if type_col["value"] == "dimension":
                assert count_col["value"] >= 13
            elif type_col["value"] == "metric":
                assert count_col["value"] >= 11
            elif type_col["value"] == "source":
                assert count_col["value"] >= 15
            elif type_col["value"] == "transform":
                assert count_col["value"] >= 3


@pytest.mark.asyncio
async def test_system_dimension_stats(module__client_with_system: AsyncClient) -> None:
    """
    Test ``GET /system/dimensions``.
    """
    response = await module__client_with_system.get("/system/dimensions")
    data = response.json()

    assert response.status_code == 200

    # With all examples, there will be more dimensions
    dim_names = {d["name"] for d in data}

    # These dimensions from roads example should be present
    assert "default.dispatcher" in dim_names
    assert "default.hard_hat" in dim_names
    assert "default.contractor" in dim_names
    assert "system.dj.node_type" in dim_names
    assert "system.dj.nodes" in dim_names

    # Verify structure of each dimension
    for dim in data:
        assert "name" in dim
        assert "indegree" in dim
        assert "cube_count" in dim
        assert isinstance(dim["indegree"], int)
        assert isinstance(dim["cube_count"], int)
