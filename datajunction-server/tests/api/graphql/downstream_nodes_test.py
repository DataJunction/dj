"""
Tests for the engine API.
"""

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_downstream_nodes(
    client_with_roads: AsyncClient,
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

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    downstreams = data["data"]["downstreamNodes"]
    assert {(d["name"], d["type"]) for d in downstreams} == {
        ("default.avg_repair_order_discounts", "METRIC"),
        ("default.avg_repair_price", "METRIC"),
        ("default.avg_time_to_dispatch", "METRIC"),
        ("default.discounted_orders_rate", "METRIC"),
        ("default.num_repair_orders", "METRIC"),
        ("default.num_unique_hard_hats_approx", "METRIC"),
        ("default.total_repair_cost", "METRIC"),
        ("default.total_repair_order_discounts", "METRIC"),
    }
    # Verify customMetadata is present for the node that has it
    num_repair_orders = next(
        d for d in downstreams if d["name"] == "default.num_repair_orders"
    )
    assert num_repair_orders["current"]["customMetadata"] == {"foo": "bar"}

    # of any type
    query = """
    {
        downstreamNodes(nodeNames: ["default.repair_order_details"], nodeType: null) {
            name
            type
        }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert {(d["name"], d["type"]) for d in data["data"]["downstreamNodes"]} == {
        ("default.avg_repair_order_discounts", "METRIC"),
        ("default.avg_repair_price", "METRIC"),
        ("default.avg_time_to_dispatch", "METRIC"),
        ("default.discounted_orders_rate", "METRIC"),
        ("default.national_level_agg", "TRANSFORM"),
        ("default.num_repair_orders", "METRIC"),
        ("default.num_unique_hard_hats_approx", "METRIC"),
        ("default.regional_level_agg", "TRANSFORM"),
        ("default.regional_repair_efficiency", "METRIC"),
        ("default.repair_orders_fact", "TRANSFORM"),
        ("default.total_repair_cost", "METRIC"),
        ("default.total_repair_order_discounts", "METRIC"),
    }


@pytest.mark.asyncio
async def test_downstream_nodes_multiple_inputs(
    client_with_roads: AsyncClient,
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

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    downstream_names = {node["name"] for node in data["data"]["downstreamNodes"]}
    # Should include transforms downstream of both sources
    assert "default.repair_orders_fact" in downstream_names


@pytest.mark.asyncio
async def test_downstream_nodes_deactivated(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test finding downstream nodes with and without deactivated nodes.
    """
    response = await client_with_roads.delete(
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

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert {(d["name"], d["type"]) for d in data["data"]["downstreamNodes"]} == {
        ("default.avg_repair_order_discounts", "METRIC"),
        ("default.avg_repair_price", "METRIC"),
        ("default.avg_time_to_dispatch", "METRIC"),
        ("default.discounted_orders_rate", "METRIC"),
        ("default.num_unique_hard_hats_approx", "METRIC"),
        ("default.total_repair_cost", "METRIC"),
        ("default.total_repair_order_discounts", "METRIC"),
    }

    query = """
    {
        downstreamNodes(nodeNames: ["default.repair_orders_fact"], nodeType: METRIC, includeDeactivated: true) {
            name
            type
        }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert {(d["name"], d["type"]) for d in data["data"]["downstreamNodes"]} == {
        ("default.avg_repair_order_discounts", "METRIC"),
        ("default.avg_repair_price", "METRIC"),
        ("default.avg_time_to_dispatch", "METRIC"),
        ("default.discounted_orders_rate", "METRIC"),
        ("default.num_repair_orders", "METRIC"),
        ("default.num_unique_hard_hats_approx", "METRIC"),
        ("default.total_repair_cost", "METRIC"),
        ("default.total_repair_order_discounts", "METRIC"),
    }


@pytest.mark.asyncio
async def test_downstream_nodes_with_nested_fields(
    client_with_roads: AsyncClient,
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

    response = await client_with_roads.post("/graphql", json={"query": query})
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
