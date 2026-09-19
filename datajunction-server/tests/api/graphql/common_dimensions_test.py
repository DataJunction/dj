"""
Tests for the common dimensions query.
"""

from collections.abc import AsyncGenerator

import pytest
from httpx import AsyncClient

# ``capture_queries`` is a shared fixture defined in tests/conftest.py


@pytest.mark.asyncio
async def test_get_common_dimensions(
    client_with_roads: AsyncClient,
    capture_queries: AsyncGenerator[
        list[str],
        None,
    ],
) -> None:
    """
    Test getting common dimensions for a set of metrics
    """

    query = """
    {
      commonDimensions(nodes: ["default.num_repair_orders", "default.avg_repair_price"]) {
        name
        type
        attribute
        properties
        role
        dimensionNode {
          name
        }
      }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    # With all examples loaded, there may be more common dimensions
    assert len(data["data"]["commonDimensions"]) >= 40
    assert {
        "attribute": "company_name",
        "dimensionNode": {
            "name": "default.dispatcher",
        },
        "name": "default.dispatcher.company_name",
        "properties": [],
        "role": None,
        "type": "string",
    } in data["data"]["commonDimensions"]

    assert {
        "attribute": "dispatcher_id",
        "dimensionNode": {
            "name": "default.dispatcher",
        },
        "name": "default.dispatcher.dispatcher_id",
        "properties": [
            "primary_key",
        ],
        "role": None,
        "type": "int",
    } in data["data"]["commonDimensions"]
    assert len(capture_queries) <= 29  # type: ignore


@pytest.mark.asyncio
async def test_get_common_dimensions_with_full_dim_node(
    client_with_roads: AsyncClient,
    capture_queries: AsyncGenerator[
        list[str],
        None,
    ],
) -> None:
    """
    Test getting common dimensions and requesting a full dimension node for each
    """

    query = """
    {
      commonDimensions(nodes: ["default.num_repair_orders", "default.avg_repair_price"]) {
        name
        type
        attribute
        properties
        role
        dimensionNode {
          name
          current {
            columns {
              name
              attributes {
                attributeType{
                  name
                }
              }
            }
          }
          tags {
            name
          }
        }
      }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert len(data["data"]["commonDimensions"]) >= 40

    assert {
        "attribute": "state_name",
        "dimensionNode": {
            "current": {
                "columns": [
                    {
                        "attributes": [],
                        "name": "state_id",
                    },
                    {
                        "attributes": [],
                        "name": "state_name",
                    },
                    {
                        "attributes": [
                            {
                                "attributeType": {
                                    "name": "primary_key",
                                },
                            },
                        ],
                        "name": "state_short",
                    },
                    {
                        "attributes": [],
                        "name": "state_region",
                    },
                ],
            },
            "name": "default.us_state",
            "tags": [],
        },
        "name": "default.us_state.state_name",
        "properties": [],
        "role": None,
        "type": "string",
    } in data["data"]["commonDimensions"]
    assert len(capture_queries) <= 31  # type: ignore


@pytest.mark.asyncio
async def test_get_common_dimensions_non_metric_nodes(
    client_with_roads: AsyncClient,
):
    """
    Test getting common dimensions and requesting a full dimension node for each
    """

    query = """
    {
      commonDimensions(nodes: ["default.num_repair_orders", "default.repair_order_fact"]) {
        name
        type
        dimensionNode {
          name
        }
      }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert len(data["data"]["commonDimensions"]) >= 40

    assert {
        "dimensionNode": {
            "name": "default.us_state",
        },
        "name": "default.us_state.state_name",
        "type": "string",
    } in data["data"]["commonDimensions"]


@pytest.mark.asyncio
async def test_get_common_dimensions_unknown_node(
    client_with_roads: AsyncClient,
):
    """
    Unknown node names resolve to zero nodes, so common dimensions should be
    an empty list rather than raising an IndexError.
    """

    query = """
    {
      commonDimensions(nodes: ["default.does_not_exist"]) {
        name
      }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data == {"data": {"commonDimensions": []}}


@pytest.mark.asyncio
async def test_get_common_dimensions_empty_input(
    client_with_roads: AsyncClient,
):
    """
    Passing no nodes at all should return an empty list of common dimensions.
    """

    query = """
    {
      commonDimensions(nodes: []) {
        name
      }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data == {"data": {"commonDimensions": []}}
