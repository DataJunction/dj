"""
Tests for the engine API.
"""

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_downstream_nodes(
    module__client_with_roads: AsyncClient,
) -> None:
    """
    Test finding nodes by node type
    """

    # of METRIC type
    query = """
    {
        downstreamNodes(nodeNames: ["default.repair_orders_fact"], nodeType: METRIC) {
            name
            type
            current {
                customMetadata
            }
        }
    }
    """

    response = await module__client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data["data"]["downstreamNodes"] == [
        {
            "name": "default.num_repair_orders",
            "type": "METRIC",
            "current": {"customMetadata": {"foo": "bar"}},
        },
        {
            "name": "default.avg_repair_price",
            "type": "METRIC",
            "current": {"customMetadata": None},
        },
        {
            "name": "default.total_repair_cost",
            "type": "METRIC",
            "current": {"customMetadata": None},
        },
        {
            "name": "default.discounted_orders_rate",
            "type": "METRIC",
            "current": {"customMetadata": None},
        },
        {
            "name": "default.total_repair_order_discounts",
            "type": "METRIC",
            "current": {"customMetadata": None},
        },
        {
            "name": "default.avg_repair_order_discounts",
            "type": "METRIC",
            "current": {"customMetadata": None},
        },
        {
            "name": "default.avg_time_to_dispatch",
            "type": "METRIC",
            "current": {"customMetadata": None},
        },
        {
            "current": {
                "customMetadata": None,
            },
            "name": "default.num_unique_hard_hats_approx",
            "type": "METRIC",
        },
    ]

    # of any type
    query = """
    {
        downstreamNodes(nodeNames: ["default.repair_order_details"], nodeType: null) {
            name
            type
        }
    }
    """

    response = await module__client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data["data"]["downstreamNodes"] == [
        {"name": "default.regional_level_agg", "type": "TRANSFORM"},
        {"name": "default.national_level_agg", "type": "TRANSFORM"},
        {"name": "default.repair_orders_fact", "type": "TRANSFORM"},
        {"name": "default.regional_repair_efficiency", "type": "METRIC"},
        {"name": "default.num_repair_orders", "type": "METRIC"},
        {"name": "default.avg_repair_price", "type": "METRIC"},
        {"name": "default.total_repair_cost", "type": "METRIC"},
        {"name": "default.discounted_orders_rate", "type": "METRIC"},
        {"name": "default.total_repair_order_discounts", "type": "METRIC"},
        {"name": "default.avg_repair_order_discounts", "type": "METRIC"},
        {"name": "default.avg_time_to_dispatch", "type": "METRIC"},
        {"name": "default.num_unique_hard_hats_approx", "type": "METRIC"},
    ]


@pytest.mark.asyncio
async def test_downstream_nodes_multiple_inputs(
    module__client_with_roads: AsyncClient,
) -> None:
    """
    Test finding downstream nodes from multiple input nodes.
    """
    query = """
    {
        downstreamNodes(nodeNames: ["default.repair_orders", "default.dispatchers"], nodeType: TRANSFORM) {
            name
            type
        }
    }
    """

    response = await module__client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    downstream_names = {node["name"] for node in data["data"]["downstreamNodes"]}
    # Should include transforms downstream of both sources
    assert "default.repair_orders_fact" in downstream_names


@pytest.mark.asyncio
async def test_downstream_nodes_deactivated(
    module__client_with_roads: AsyncClient,
) -> None:
    """
    Test finding downstream nodes with and without deactivated nodes.
    """
    response = await module__client_with_roads.delete(
        "/nodes/default.num_repair_orders",
    )
    assert response.status_code == 200

    query = """
    {
        downstreamNodes(nodeNames: ["default.repair_orders_fact"], nodeType: METRIC, includeDeactivated: false) {
            name
            type
        }
    }
    """

    response = await module__client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data["data"]["downstreamNodes"] == [
        {"name": "default.avg_repair_price", "type": "METRIC"},
        {"name": "default.total_repair_cost", "type": "METRIC"},
        {"name": "default.discounted_orders_rate", "type": "METRIC"},
        {"name": "default.total_repair_order_discounts", "type": "METRIC"},
        {"name": "default.avg_repair_order_discounts", "type": "METRIC"},
        {"name": "default.avg_time_to_dispatch", "type": "METRIC"},
        {"name": "default.num_unique_hard_hats_approx", "type": "METRIC"},
    ]

    query = """
    {
        downstreamNodes(nodeNames: ["default.repair_orders_fact"], nodeType: METRIC, includeDeactivated: true) {
            name
            type
        }
    }
    """

    response = await module__client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data["data"]["downstreamNodes"] == [
        {"name": "default.num_repair_orders", "type": "METRIC"},
        {"name": "default.avg_repair_price", "type": "METRIC"},
        {"name": "default.total_repair_cost", "type": "METRIC"},
        {"name": "default.discounted_orders_rate", "type": "METRIC"},
        {"name": "default.total_repair_order_discounts", "type": "METRIC"},
        {"name": "default.avg_repair_order_discounts", "type": "METRIC"},
        {"name": "default.avg_time_to_dispatch", "type": "METRIC"},
        {"name": "default.num_unique_hard_hats_approx", "type": "METRIC"},
    ]


@pytest.mark.asyncio
async def test_downstream_nodes_with_nested_fields(
    module__client_with_roads: AsyncClient,
) -> None:
    """
    Test downstream nodes query with nested fields that require database joins.
    This tests that load_node_options correctly builds options based on the
    requested GraphQL fields (tags, owners, current.columns, etc.).
    """
    # Query with many nested fields that require joins
    query = """
    {
        downstreamNodes(nodeNames: ["default.repair_order_details"], nodeType: TRANSFORM) {
            name
            type
            tags {
                name
                tagType
            }
            owners {
                username
            }
            current {
                displayName
                status
                description
                columns {
                    name
                    type
                }
                parents {
                    name
                }
            }
        }
    }
    """

    response = await module__client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    # Verify we got results
    downstreams = data["data"]["downstreamNodes"]
    assert len(downstreams) > 0

    # Find the repair_orders_fact transform
    repair_orders_fact = next(
        (n for n in downstreams if n["name"] == "default.repair_orders_fact"),
        None,
    )
    assert repair_orders_fact is not None

    # Verify nested fields are populated
    assert repair_orders_fact["type"] == "TRANSFORM"
    assert repair_orders_fact["current"] is not None
    assert repair_orders_fact["current"]["status"] == "VALID"
    assert repair_orders_fact["current"]["displayName"] == "Repair Orders Fact"

    # Verify columns are loaded (requires selectinload)
    columns = repair_orders_fact["current"]["columns"]
    assert columns is not None
    assert len(columns) > 0
    column_names = {c["name"] for c in columns}
    assert "repair_order_id" in column_names

    # Verify parents are loaded (requires selectinload)
    parents = repair_orders_fact["current"]["parents"]
    assert parents is not None
    assert len(parents) > 0
