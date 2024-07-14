"""
Tests for the engine API.
"""
import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_find_by_node_type(
    module__client_with_roads: AsyncClient,
) -> None:
    """
    Test finding nodes by node type
    """

    query = """
    {
        findNodes(nodeTypes: [TRANSFORM]) {
            name
            type
            tags {
                name
            }
            currentVersion
        }
    }
    """

    response = await module__client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data == {
        "data": {
            "findNodes": [
                {
                    "currentVersion": "v1.0",
                    "name": "default.regional_level_agg",
                    "tags": [],
                    "type": "TRANSFORM",
                },
                {
                    "currentVersion": "v1.0",
                    "name": "default.national_level_agg",
                    "tags": [],
                    "type": "TRANSFORM",
                },
                {
                    "currentVersion": "v1.0",
                    "name": "default.repair_orders_fact",
                    "tags": [],
                    "type": "TRANSFORM",
                },
            ],
        },
    }

    query = """
    {
        findNodes(nodeTypes: [CUBE]) {
            name
            type
            tags {
                name
            }
            currentVersion
        }
    }
    """

    response = await module__client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data == {"data": {"findNodes": []}}


@pytest.mark.asyncio
async def test_find_by_fragment(
    module__client_with_roads: AsyncClient,
) -> None:
    """
    Test finding nodes by fragment
    """
    query = """
    {
        findNodes(fragment: "repair_order_dis") {
            name
            type
            current {
                columns {
                    name
                    type
                }
            }
            currentVersion
        }
    }
    """

    response = await module__client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data == {
        "data": {
            "findNodes": [
                {
                    "current": {
                        "columns": [
                            {
                                "name": "default_DOT_avg_repair_order_discounts",
                                "type": "double",
                            },
                        ],
                    },
                    "currentVersion": "v1.0",
                    "name": "default.avg_repair_order_discounts",
                    "type": "METRIC",
                },
                {
                    "current": {
                        "columns": [
                            {
                                "name": "default_DOT_total_repair_order_discounts",
                                "type": "double",
                            },
                        ],
                    },
                    "currentVersion": "v1.0",
                    "name": "default.total_repair_order_discounts",
                    "type": "METRIC",
                },
            ],
        },
    }


@pytest.mark.asyncio
async def test_find_by_names(
    module__client_with_roads: AsyncClient,
) -> None:
    """
    Test finding nodes by their names
    """
    query = """
    {
        findNodes(names: ["default.regional_level_agg", "default.repair_orders"]) {
            name
            type
            current {
                columns {
                    name
                    type
                }
            }
            currentVersion
        }
    }
    """

    response = await module__client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data == {
        "data": {
            "findNodes": [
                {
                    "current": {
                        "columns": [
                            {
                                "name": "us_region_id",
                                "type": "int",
                            },
                            {
                                "name": "state_name",
                                "type": "string",
                            },
                            {
                                "name": "location_hierarchy",
                                "type": "string",
                            },
                            {
                                "name": "order_year",
                                "type": "int",
                            },
                            {
                                "name": "order_month",
                                "type": "int",
                            },
                            {
                                "name": "order_day",
                                "type": "int",
                            },
                            {
                                "name": "completed_repairs",
                                "type": "bigint",
                            },
                            {
                                "name": "total_repairs_dispatched",
                                "type": "bigint",
                            },
                            {
                                "name": "total_amount_in_region",
                                "type": "double",
                            },
                            {
                                "name": "avg_repair_amount_in_region",
                                "type": "double",
                            },
                            {
                                "name": "avg_dispatch_delay",
                                "type": "double",
                            },
                            {
                                "name": "unique_contractors",
                                "type": "bigint",
                            },
                        ],
                    },
                    "currentVersion": "v1.0",
                    "name": "default.regional_level_agg",
                    "type": "TRANSFORM",
                },
                {
                    "current": {
                        "columns": [
                            {
                                "name": "repair_order_id",
                                "type": "int",
                            },
                            {
                                "name": "municipality_id",
                                "type": "string",
                            },
                            {
                                "name": "hard_hat_id",
                                "type": "int",
                            },
                            {
                                "name": "order_date",
                                "type": "timestamp",
                            },
                            {
                                "name": "required_date",
                                "type": "timestamp",
                            },
                            {
                                "name": "dispatched_date",
                                "type": "timestamp",
                            },
                            {
                                "name": "dispatcher_id",
                                "type": "int",
                            },
                        ],
                    },
                    "currentVersion": "v1.0",
                    "name": "default.repair_orders",
                    "type": "SOURCE",
                },
            ],
        },
    }


@pytest.mark.asyncio
async def test_find_by_tags(
    module__client_with_roads: AsyncClient,
) -> None:
    """
    Test finding nodes by tags
    """

    query = """
    {
        findNodes(tags: ["random"]) {
            name
            type
            tags {
                name
            }
            currentVersion
        }
    }
    """

    response = await module__client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data == {"data": {"findNodes": []}}
