"""
Tests for the common dimensions query.
"""

# pylint: disable=line-too-long
from typing import AsyncGenerator

import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession


@pytest_asyncio.fixture
async def capture_queries(
    module__session: AsyncSession,
) -> AsyncGenerator[list[str], None]:
    """
    Returns a list of strings, where each string represents a SQL statement
    captured during the test.
    """
    queries = []
    sync_engine = module__session.bind.sync_engine

    def before_cursor_execute(
        _conn,
        _cursor,
        statement,
        _parameters,
        _context,
        _executemany,
    ):
        queries.append(statement)

    # Attach event listener to capture queries
    event.listen(sync_engine, "before_cursor_execute", before_cursor_execute)

    yield queries

    # Detach event listener after the test
    event.remove(sync_engine, "before_cursor_execute", before_cursor_execute)


@pytest.mark.asyncio
async def test_get_common_dimensions(
    module__client_with_roads: AsyncClient,
    capture_queries: AsyncGenerator[  # pylint: disable=redefined-outer-name
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

    response = await module__client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert len(data["data"]["commonDimensions"]) == 40
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
    assert len(capture_queries) <= 11  # type: ignore


@pytest.mark.asyncio
async def test_get_common_dimensions_with_full_dim_node(
    module__client_with_roads: AsyncClient,
    capture_queries: AsyncGenerator[  # pylint: disable=redefined-outer-name
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

    response = await module__client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert len(data["data"]["commonDimensions"]) == 40

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
    assert len(capture_queries) > 200  # type: ignore
