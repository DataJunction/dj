"""
Tests for the upstream nodes GraphQL query.
"""

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_upstream_nodes_overlapping_parents(
    module__client_with_roads: AsyncClient,
) -> None:
    """
    Test upstreams for multiple metrics where one metric's parent transform
    is also an upstream of another metric's parent transform.

    This creates a diamond pattern:
        source_data (SOURCE)
             to
        base_transform (TRANSFORM)
             to
        derived_transform (TRANSFORM)

        metric_on_base (METRIC) → base_transform
        metric_on_derived (METRIC) → derived_transform → base_transform
    """
    client = module__client_with_roads

    # Create source
    response = await client.post(
        "/nodes/source/",
        json={
            "name": "default.diamond_source",
            "description": "Source for diamond pattern test",
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "value", "type": "int"},
            ],
            "catalog": "default",
            "schema_": "roads",
            "table": "diamond_source",
            "mode": "published",
        },
    )
    assert response.status_code == 200

    # Create base transform (T1)
    response = await client.post(
        "/nodes/transform/",
        json={
            "name": "default.diamond_base_transform",
            "description": "Base transform",
            "query": "SELECT id, value FROM default.diamond_source",
            "mode": "published",
        },
    )
    assert response.status_code == 201

    # Create derived transform (T2) that depends on T1
    response = await client.post(
        "/nodes/transform/",
        json={
            "name": "default.diamond_derived_transform",
            "description": "Derived transform that depends on base",
            "query": "SELECT id, value * 2 as doubled FROM default.diamond_base_transform",
            "mode": "published",
        },
    )
    assert response.status_code == 201

    # Create metric M1 on base transform
    response = await client.post(
        "/nodes/metric/",
        json={
            "name": "default.diamond_metric_base",
            "description": "Metric on base transform",
            "query": "SELECT sum(value) FROM default.diamond_base_transform",
            "mode": "published",
        },
    )
    assert response.status_code == 201

    # Create metric M2 on derived transform
    response = await client.post(
        "/nodes/metric/",
        json={
            "name": "default.diamond_metric_derived",
            "description": "Metric on derived transform",
            "query": "SELECT sum(doubled) FROM default.diamond_derived_transform",
            "mode": "published",
        },
    )
    assert response.status_code == 201

    # Query upstreams for both metrics - this should hit the deduplication branch
    query = """
    {
        upstreamNodes(nodeNames: ["default.diamond_metric_base", "default.diamond_metric_derived"]) {
            name
            type
        }
    }
    """
    response = await client.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    upstream_names = {node["name"] for node in data["data"]["upstreamNodes"]}

    # Should include all upstreams, deduplicated
    assert "default.diamond_source" in upstream_names
    assert "default.diamond_base_transform" in upstream_names
    assert "default.diamond_derived_transform" in upstream_names

    # base_transform should appear exactly once (not duplicated)
    base_transform_count = sum(
        1
        for node in data["data"]["upstreamNodes"]
        if node["name"] == "default.diamond_base_transform"
    )
    assert base_transform_count == 1


@pytest.mark.asyncio
async def test_upstream_nodes(
    module__client_with_roads: AsyncClient,
) -> None:
    """
    Test finding upstream nodes by node type
    """

    # of SOURCE type
    query = """
    {
        upstreamNodes(nodeNames: ["default.repair_orders_fact"], nodeType: SOURCE) {
            name
            type
        }
    }
    """

    response = await module__client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    upstream_names = {node["name"] for node in data["data"]["upstreamNodes"]}
    assert "default.repair_orders" in upstream_names
    assert "default.repair_order_details" in upstream_names
    for node in data["data"]["upstreamNodes"]:
        assert node["type"] == "SOURCE"

    # of any type
    query = """
    {
        upstreamNodes(nodeNames: ["default.num_repair_orders"], nodeType: null) {
            name
            type
        }
    }
    """

    response = await module__client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    upstream_names = {node["name"] for node in data["data"]["upstreamNodes"]}
    # The metric should have the transform as an upstream
    assert "default.repair_orders_fact" in upstream_names


@pytest.mark.asyncio
async def test_upstream_nodes_multiple_inputs(
    module__client_with_roads: AsyncClient,
) -> None:
    """
    Test finding upstream nodes from multiple input nodes.
    """
    query = """
    {
        upstreamNodes(nodeNames: ["default.num_repair_orders", "default.avg_repair_price"], nodeType: SOURCE) {
            name
            type
        }
    }
    """

    response = await module__client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    upstream_names = {node["name"] for node in data["data"]["upstreamNodes"]}
    # Should include sources upstream of both metrics (deduplicated)
    assert "default.repair_orders" in upstream_names
    assert "default.repair_order_details" in upstream_names
    for node in data["data"]["upstreamNodes"]:
        assert node["type"] == "SOURCE"


@pytest.mark.asyncio
async def test_upstream_nodes_deactivated(
    module__client_with_roads: AsyncClient,
) -> None:
    """
    Test finding upstream nodes with and without deactivated nodes.
    """
    # First deactivate a node that is upstream of the metric
    response = await module__client_with_roads.delete(
        "/nodes/default.repair_orders_fact",
    )
    assert response.status_code == 200

    # Without deactivated nodes
    query = """
    {
        upstreamNodes(nodeNames: ["default.num_repair_orders"], nodeType: null, includeDeactivated: false) {
            name
            type
        }
    }
    """

    response = await module__client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    upstream_names = {node["name"] for node in data["data"]["upstreamNodes"]}
    assert "default.repair_orders_fact" not in upstream_names

    # With deactivated nodes
    query = """
    {
        upstreamNodes(nodeNames: ["default.num_repair_orders"], nodeType: null, includeDeactivated: true) {
            name
            type
        }
    }
    """

    response = await module__client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    upstream_names = {node["name"] for node in data["data"]["upstreamNodes"]}
    assert "default.repair_orders_fact" in upstream_names
