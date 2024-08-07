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
    assert data["data"]["findNodes"] == [
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
    ]

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
    assert data["data"]["findNodes"] == [
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
    ]


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
    assert data["data"]["findNodes"] == [
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
    ]


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
    assert data["data"]["findNodes"] == []


@pytest.mark.asyncio
async def test_find_source(
    module__client_with_roads: AsyncClient,
) -> None:
    """
    Test finding source nodes
    """

    query = """
    {
        findNodes(names: ["default.repair_type"]) {
            name
            type
            current {
                catalog {
                    name
                }
                schema_
                table
                status
                dimensionLinks {
                    joinSql
                    joinType
                    role
                    dimension {
                        name
                    }
                }
            }
        }
    }
    """

    response = await module__client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data["data"]["findNodes"] == [
        {
            "current": {
                "catalog": {
                    "name": "default",
                },
                "dimensionLinks": [
                    {
                        "dimension": {
                            "name": "default.contractor",
                        },
                        "joinSql": "default.repair_type.contractor_id = "
                        "default.contractor.contractor_id",
                        "joinType": "LEFT",
                        "role": None,
                    },
                ],
                "schema_": "roads",
                "status": "VALID",
                "table": "repair_type",
            },
            "name": "default.repair_type",
            "type": "SOURCE",
        },
    ]


@pytest.mark.asyncio
async def test_find_transform(
    module__client_with_roads: AsyncClient,
) -> None:
    """
    Test finding source nodes
    """

    query = """
    {
        findNodes(names: ["default.repair_orders_fact"]) {
            name
            type
            current {
                parents {
                    name
                }
                materializations {
                    name
                }
                availability {
                    temporalPartitions
                    minTemporalPartition
                    maxTemporalPartition
                }
                cubeMetrics {
                    name
                }
                cubeDimensions {
                    name
                }
            }
        }
    }
    """

    response = await module__client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data["data"]["findNodes"] == [
        {
            "current": {
                "availability": None,
                "cubeDimensions": [],
                "cubeMetrics": [],
                "materializations": [],
                "parents": [
                    {
                        "name": "default.repair_orders",
                    },
                    {
                        "name": "default.repair_order_details",
                    },
                ],
            },
            "name": "default.repair_orders_fact",
            "type": "TRANSFORM",
        },
    ]


@pytest.mark.asyncio
async def test_find_metric(
    module__client_with_roads: AsyncClient,
) -> None:
    """
    Test finding metrics
    """

    query = """
    {
        findNodes(names: ["default.regional_repair_efficiency"]) {
            name
            type
            current {
                parents {
                    name
                }
                metricMetadata {
                    unit {
                        name
                    }
                    direction
                }
                requiredDimensions {
                    name
                }
            }
        }
    }
    """

    response = await module__client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data["data"]["findNodes"] == [
        {
            "current": {
                "metricMetadata": None,
                "parents": [
                    {
                        "name": "default.regional_level_agg",
                    },
                    {
                        "name": "default.national_level_agg",
                    },
                ],
                "requiredDimensions": [],
            },
            "name": "default.regional_repair_efficiency",
            "type": "METRIC",
        },
    ]


@pytest.mark.asyncio
async def test_find_cubes(
    module__client_with_roads: AsyncClient,
) -> None:
    """
    Test finding cubes
    """
    response = await module__client_with_roads.post(
        "/nodes/cube/",
        json={
            "metrics": [
                "default.num_repair_orders",
                "default.avg_repair_price",
                "default.total_repair_cost",
            ],
            "dimensions": [
                "default.hard_hat.city",
                "default.hard_hat.state",
                "default.dispatcher.company_name",
            ],
            "filters": ["default.hard_hat.state='AZ'"],
            "description": "Cube of various metrics related to repairs",
            "mode": "published",
            "name": "default.repairs_cube",
        },
    )
    query = """
    {
        findNodes(nodeTypes: [CUBE]) {
            name
            type
            current {
                cubeMetrics {
                    name
                    description
                }
                cubeDimensions {
                    name
                    dimensionNode {
                        name
                    }
                }
            }
        }
    }
    """

    response = await module__client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data["data"]["findNodes"] == [
        {
            "current": {
                "cubeDimensions": [
                    {
                        "dimensionNode": {
                            "name": "default.hard_hat",
                        },
                        "name": "default.hard_hat.city",
                    },
                    {
                        "dimensionNode": {
                            "name": "default.hard_hat",
                        },
                        "name": "default.hard_hat.state",
                    },
                    {
                        "dimensionNode": {
                            "name": "default.dispatcher",
                        },
                        "name": "default.dispatcher.company_name",
                    },
                ],
                "cubeMetrics": [
                    {
                        "description": "Number of repair orders",
                        "name": "default.num_repair_orders",
                    },
                    {
                        "description": "Average repair price",
                        "name": "default.avg_repair_price",
                    },
                    {
                        "description": "Total repair cost",
                        "name": "default.total_repair_cost",
                    },
                ],
            },
            "name": "default.repairs_cube",
            "type": "CUBE",
        },
    ]


@pytest.mark.asyncio
async def test_find_node_with_revisions(
    module__client_with_roads: AsyncClient,
) -> None:
    """
    Test finding nodes with revisions
    """

    query = """
    {
        findNodes(nodeTypes: [TRANSFORM]) {
            name
            type
            revisions {
                displayName
                dimensionLinks {
                    dimension {
                        name
                    }
                    joinSql
                }
            }
            currentVersion
        }
    }
    """
    response = await module__client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data["data"]["findNodes"] == [
        {
            "currentVersion": "v1.0",
            "name": "default.regional_level_agg",
            "revisions": [
                {
                    "dimensionLinks": [],
                    "displayName": "Default: Regional Level Agg",
                },
            ],
            "type": "TRANSFORM",
        },
        {
            "currentVersion": "v1.0",
            "name": "default.national_level_agg",
            "revisions": [
                {
                    "dimensionLinks": [],
                    "displayName": "Default: National Level Agg",
                },
            ],
            "type": "TRANSFORM",
        },
        {
            "currentVersion": "v1.0",
            "name": "default.repair_orders_fact",
            "revisions": [
                {
                    "dimensionLinks": [
                        {
                            "dimension": {
                                "name": "default.municipality_dim",
                            },
                            "joinSql": "default.repair_orders_fact.municipality_id = "
                            "default.municipality_dim.municipality_id",
                        },
                        {
                            "dimension": {
                                "name": "default.hard_hat",
                            },
                            "joinSql": "default.repair_orders_fact.hard_hat_id = "
                            "default.hard_hat.hard_hat_id",
                        },
                        {
                            "dimension": {
                                "name": "default.hard_hat_to_delete",
                            },
                            "joinSql": "default.repair_orders_fact.hard_hat_id = "
                            "default.hard_hat_to_delete.hard_hat_id",
                        },
                        {
                            "dimension": {
                                "name": "default.dispatcher",
                            },
                            "joinSql": "default.repair_orders_fact.dispatcher_id = "
                            "default.dispatcher.dispatcher_id",
                        },
                    ],
                    "displayName": "Repair Orders Fact",
                },
            ],
            "type": "TRANSFORM",
        },
    ]
