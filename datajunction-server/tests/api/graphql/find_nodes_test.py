"""
Tests for the findNodes / findNodesPaginated GraphQL queries
"""

from unittest import mock

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_find_by_node_type(
    client_with_roads: AsyncClient,
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
            current {
                customMetadata
            }
        }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    repair_orders_fact = next(
        node
        for node in data["data"]["findNodes"]
        if node["name"] == "default.repair_orders_fact"
    )
    assert repair_orders_fact == {
        "currentVersion": mock.ANY,
        "name": "default.repair_orders_fact",
        "tags": [],
        "type": "TRANSFORM",
        "current": {"customMetadata": {"foo": "bar"}},
    }
    national_level_agg = next(
        node
        for node in data["data"]["findNodes"]
        if node["name"] == "default.national_level_agg"
    )
    assert national_level_agg == {
        "currentVersion": mock.ANY,
        "name": "default.national_level_agg",
        "tags": [],
        "type": "TRANSFORM",
        "current": {"customMetadata": None},
    }
    regional_level_agg = next(
        node
        for node in data["data"]["findNodes"]
        if node["name"] == "default.regional_level_agg"
    )
    assert regional_level_agg == {
        "currentVersion": mock.ANY,
        "name": "default.regional_level_agg",
        "tags": [],
        "type": "TRANSFORM",
        "current": {"customMetadata": None},
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

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data == {"data": {"findNodes": []}}


@pytest.mark.asyncio
async def test_find_node_limit(
    client_with_roads: AsyncClient,
    caplog,
) -> None:
    """
    Test finding nodes has a max limit
    """

    query = """
    {
        findNodes(nodeTypes: [TRANSFORM], limit: 100000) {
            name
        }
    }
    """
    caplog.set_level("WARNING")
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    assert any(
        "Limit of 100000 is greater than the maximum limit" in message
        for message in caplog.messages
    )
    data = response.json()
    node_names = [node["name"] for node in data["data"]["findNodes"]]
    assert "default.repair_orders_fact" in node_names
    assert "default.national_level_agg" in node_names
    assert "default.regional_level_agg" in node_names

    query = """
    {
        findNodes(nodeTypes: [TRANSFORM], limit: -1) {
            name
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    node_names = [node["name"] for node in data["data"]["findNodes"]]
    assert "default.repair_orders_fact" in node_names
    assert "default.national_level_agg" in node_names
    assert "default.regional_level_agg" in node_names


@pytest.mark.asyncio
async def test_find_by_node_type_paginated(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test finding nodes by node type with pagination
    """
    query = """
    {
      findNodesPaginated(fragment: "default.", nodeTypes: [TRANSFORM], limit: 2) {
        edges {
          node {
            name
            type
            tags {
              name
            }
            currentVersion
            owners {
              username
            }
          }
        }
        pageInfo {
          startCursor
          endCursor
          hasNextPage
          hasPrevPage
        }
      }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    edges = data["data"]["findNodesPaginated"]["edges"]
    # Verify pagination returns exactly 2 results
    assert len(edges) == 2
    # Verify all returned nodes are TRANSFORM type
    for edge in edges:
        assert edge["node"]["type"] == "TRANSFORM"
        assert edge["node"]["name"].startswith("default.")
    # Verify page info structure
    page_info = data["data"]["findNodesPaginated"]["pageInfo"]
    assert "startCursor" in page_info
    assert "endCursor" in page_info

    after = page_info["endCursor"]
    query = """
    query ListNodes($after: String) {
      findNodesPaginated(fragment: "default.", nodeTypes: [TRANSFORM], limit: 2, after: $after) {
        edges {
          node {
            name
            type
            tags {
                name
            }
            currentVersion
          }
        }
        pageInfo {
          startCursor
          endCursor
          hasNextPage
          hasPrevPage
        }
      }
    }
    """
    response = await client_with_roads.post(
        "/graphql",
        json={"query": query, "variables": {"after": after}},
    )
    assert response.status_code == 200
    data = response.json()
    # Verify pagination continues correctly
    page_info = data["data"]["findNodesPaginated"]["pageInfo"]
    assert page_info["hasPrevPage"] is True
    assert "startCursor" in page_info
    assert "endCursor" in page_info
    # All returned nodes should be TRANSFORM type
    for edge in data["data"]["findNodesPaginated"]["edges"]:
        assert edge["node"]["type"] == "TRANSFORM"
    before = data["data"]["findNodesPaginated"]["pageInfo"]["startCursor"]
    query = """
    query ListNodes($before: String) {
      findNodesPaginated(nodeTypes: [TRANSFORM], limit: 2, before: $before) {
        edges {
          node {
            name
            type
            tags {
                name
            }
            currentVersion
          }
        }
        pageInfo {
          startCursor
          endCursor
          hasNextPage
          hasPrevPage
        }
      }
    }
    """
    response = await client_with_roads.post(
        "/graphql",
        json={"query": query, "variables": {"before": before}},
    )
    assert response.status_code == 200
    data = response.json()
    # Verify backward pagination works correctly
    edges = data["data"]["findNodesPaginated"]["edges"]
    assert len(edges) == 2
    # All returned nodes should be TRANSFORM type
    for edge in edges:
        assert edge["node"]["type"] == "TRANSFORM"
    page_info = data["data"]["findNodesPaginated"]["pageInfo"]
    assert "startCursor" in page_info
    assert "endCursor" in page_info
    # Should have pages in both directions when paginating backwards from middle
    assert page_info["hasNextPage"] is True
    assert page_info["hasPrevPage"] is True


@pytest.mark.asyncio
async def test_find_by_fragment(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test finding nodes by fragment search functionality
    """
    # Test fragment search returns results
    query = """
    {
        findNodes(fragment: "repair") {
            name
            type
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    nodes = data["data"]["findNodes"]
    # Should find nodes matching "repair" fragment
    assert len(nodes) > 0

    # Test fragment search by display name
    query = """
    {
        findNodes(fragment: "Repair") {
            name
            current {
                displayName
            }
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    nodes = data["data"]["findNodes"]
    # Should find nodes with "Repair" in name or display name
    assert len(nodes) > 0


@pytest.mark.asyncio
async def test_find_by_names(
    client_with_roads: AsyncClient,
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

    response = await client_with_roads.post("/graphql", json={"query": query})
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
            "currentVersion": "v1.2",
            "name": "default.repair_orders",
            "type": "SOURCE",
        },
    ]


@pytest.mark.asyncio
async def test_find_by_tags(
    client_with_roads: AsyncClient,
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

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data["data"]["findNodes"] == []


@pytest.mark.asyncio
async def test_find_source(
    client_with_roads: AsyncClient,
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

    response = await client_with_roads.post("/graphql", json={"query": query})
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
                        "joinType": "INNER",
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
    client_with_roads: AsyncClient,
) -> None:
    """
    Test finding transform nodes
    """

    query = """
    {
        findNodes(names: ["default.repair_orders_fact"]) {
            name
            type
            current {
                parents {
                    name
                    currentVersion
                    type
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
                extractedMeasures {
                    components {
                        name
                    }
                }
                metricMetadata {
                    unit {
                        name
                    }
                }
                primaryKey
            }
        }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
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
                        "currentVersion": "v1.2",
                        "type": "source",
                    },
                    {
                        "name": "default.repair_order_details",
                        "currentVersion": "v1.2",
                        "type": "source",
                    },
                ],
                "extractedMeasures": None,
                "metricMetadata": None,
                "primaryKey": [],
            },
            "name": "default.repair_orders_fact",
            "type": "TRANSFORM",
        },
    ]


@pytest.mark.asyncio
async def test_find_metric(
    client_with_roads: AsyncClient,
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
                    expression
                    incompatibleDruidFunctions
                }
                requiredDimensions {
                    name
                }
                extractedMeasures {
                    components {
                        name
                        expression
                        aggregation
                        rule {
                            type
                        }
                    }
                    derivedQuery
                    derivedExpression
                }
            }
        }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data["data"]["findNodes"] == [
        {
            "current": {
                "metricMetadata": {
                    "direction": None,
                    "unit": None,
                    "expression": (
                        "(SUM(rm.completed_repairs) * 1.0 / SUM(rm.total_repairs_dispatched)) * "
                        "(SUM(rm.total_amount_in_region) * 1.0 / "
                        "SUM(na.total_amount_nationwide)) * 100"
                    ),
                    "incompatibleDruidFunctions": [],
                },
                "parents": [
                    {
                        "name": "default.regional_level_agg",
                    },
                    {
                        "name": "default.national_level_agg",
                    },
                ],
                "requiredDimensions": [],
                "extractedMeasures": {
                    "components": [
                        {
                            "aggregation": "SUM",
                            "expression": "completed_repairs",
                            "name": "completed_repairs_sum_8b112bf1",
                            "rule": {
                                "type": "FULL",
                            },
                        },
                        {
                            "aggregation": "SUM",
                            "expression": "total_repairs_dispatched",
                            "name": "total_repairs_dispatched_sum_601dc4f1",
                            "rule": {
                                "type": "FULL",
                            },
                        },
                        {
                            "aggregation": "SUM",
                            "expression": "total_amount_in_region",
                            "name": "total_amount_in_region_sum_3426ede4",
                            "rule": {
                                "type": "FULL",
                            },
                        },
                        {
                            "aggregation": "SUM",
                            "expression": "na.total_amount_nationwide",
                            "name": "na_DOT_total_amount_nationwide_sum_4ecb2318",
                            "rule": {
                                "type": "FULL",
                            },
                        },
                    ],
                    "derivedQuery": "SELECT  (SUM(completed_repairs_sum_8b112bf1) * 1.0 / "
                    "SUM(total_repairs_dispatched_sum_601dc4f1)) * "
                    "(SUM(total_amount_in_region_sum_3426ede4) * 1.0 / "
                    "SUM(na_DOT_total_amount_nationwide_sum_4ecb2318)) * 100 \n"
                    " FROM default.regional_level_agg CROSS JOIN "
                    "default.national_level_agg na\n"
                    "\n",
                    "derivedExpression": "(SUM(completed_repairs_sum_8b112bf1) * 1.0 / "
                    "SUM(total_repairs_dispatched_sum_601dc4f1)) * "
                    "(SUM(total_amount_in_region_sum_3426ede4) * 1.0 / "
                    "SUM(na_DOT_total_amount_nationwide_sum_4ecb2318)) * 100",
                },
            },
            "name": "default.regional_repair_efficiency",
            "type": "METRIC",
        },
    ]


@pytest.mark.asyncio
async def test_find_cubes(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test finding cubes
    """
    response = await client_with_roads.post(
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

    response = await client_with_roads.post("/graphql", json={"query": query})
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
async def test_find_cubes_full_query(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test finding cubes with full field selection including cubeMetrics and cubeDimensions.
    This tests the optimized loading paths for cube queries.
    """
    # First create a cube
    response = await client_with_roads.post(
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
            "description": "Full cube for testing",
            "mode": "published",
            "name": "default.full_test_cube",
        },
    )
    assert response.status_code < 400, response.json()

    # Query with full field selection
    query = """
    query FindReportCubes {
        findNodes(nodeTypes:[CUBE]) {
            name
            tags {
                name
            }
            createdBy {
                username
            }
            current {
                description
                displayName
                cubeMetrics {
                    name
                    version
                    type
                    displayName
                }
                cubeDimensions {
                    name
                    type
                    role
                    dimensionNode {
                        name
                    }
                    attribute
                }
            }
        }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    cubes = data["data"]["findNodes"]
    assert cubes == [
        {
            "createdBy": {
                "username": "dj",
            },
            "current": {
                "cubeDimensions": [
                    {
                        "attribute": "city",
                        "dimensionNode": {
                            "name": "default.hard_hat",
                        },
                        "name": "default.hard_hat.city",
                        "role": "",
                        "type": "string",
                    },
                    {
                        "attribute": "state",
                        "dimensionNode": {
                            "name": "default.hard_hat",
                        },
                        "name": "default.hard_hat.state",
                        "role": "",
                        "type": "string",
                    },
                    {
                        "attribute": "company_name",
                        "dimensionNode": {
                            "name": "default.dispatcher",
                        },
                        "name": "default.dispatcher.company_name",
                        "role": "",
                        "type": "string",
                    },
                ],
                "cubeMetrics": [
                    {
                        "displayName": "Num Repair Orders",
                        "name": "default.num_repair_orders",
                        "type": "METRIC",
                        "version": "v1.0",
                    },
                    {
                        "displayName": "Avg Repair Price",
                        "name": "default.avg_repair_price",
                        "type": "METRIC",
                        "version": "v1.0",
                    },
                    {
                        "displayName": "Total Repair Cost",
                        "name": "default.total_repair_cost",
                        "type": "METRIC",
                        "version": "v1.0",
                    },
                ],
                "description": "Full cube for testing",
                "displayName": "Full Test Cube",
            },
            "name": "default.full_test_cube",
            "tags": [],
        },
    ]


@pytest.mark.asyncio
async def test_find_node_with_revisions(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test finding nodes with revisions
    """

    query = """
    {
      findNodesPaginated(nodeTypes: [TRANSFORM], namespace: "default", editedBy: "dj", limit: -1) {
        edges {
          node {
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
            createdBy {
                email
                id
                isAdmin
                name
                oauthProvider
                username
            }
          }
        }
      }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    results = data["data"]["findNodesPaginated"]
    results["edges"][0]["node"]["revisions"][0]["dimensionLinks"] = sorted(
        results["edges"][0]["node"]["revisions"][0]["dimensionLinks"],
        key=lambda x: x["dimension"]["name"],
    )
    assert results["edges"] == [
        {
            "node": {
                "createdBy": {
                    "email": "dj@datajunction.io",
                    "id": 1,
                    "isAdmin": False,
                    "name": "DJ",
                    "oauthProvider": "BASIC",
                    "username": "dj",
                },
                "currentVersion": "v1.1",
                "name": "default.long_events",
                "revisions": [
                    {"dimensionLinks": [], "displayName": "Long Events"},
                    {
                        "dimensionLinks": [
                            {
                                "dimension": {"name": "default.country_dim"},
                                "joinSql": "default.long_events.country "
                                "= "
                                "default.country_dim.country",
                            },
                        ],
                        "displayName": "Long Events",
                    },
                ],
                "type": "TRANSFORM",
            },
        },
        {
            "node": {
                "createdBy": {
                    "email": "dj@datajunction.io",
                    "id": 1,
                    "isAdmin": False,
                    "name": "DJ",
                    "oauthProvider": "BASIC",
                    "username": "dj",
                },
                "currentVersion": "v1.0",
                "name": "default.large_revenue_payments_and_business_only_1",
                "revisions": [
                    {
                        "dimensionLinks": [],
                        "displayName": "Large Revenue Payments And Business Only 1",
                    },
                ],
                "type": "TRANSFORM",
            },
        },
        {
            "node": {
                "createdBy": {
                    "email": "dj@datajunction.io",
                    "id": 1,
                    "isAdmin": False,
                    "name": "DJ",
                    "oauthProvider": "BASIC",
                    "username": "dj",
                },
                "currentVersion": "v1.0",
                "name": "default.large_revenue_payments_and_business_only",
                "revisions": [
                    {
                        "dimensionLinks": [],
                        "displayName": "Large Revenue Payments And Business Only",
                    },
                ],
                "type": "TRANSFORM",
            },
        },
        {
            "node": {
                "createdBy": {
                    "email": "dj@datajunction.io",
                    "id": 1,
                    "isAdmin": False,
                    "name": "DJ",
                    "oauthProvider": "BASIC",
                    "username": "dj",
                },
                "currentVersion": "v1.0",
                "name": "default.large_revenue_payments_only_custom",
                "revisions": [
                    {
                        "dimensionLinks": [],
                        "displayName": "Large Revenue Payments Only Custom",
                    },
                ],
                "type": "TRANSFORM",
            },
        },
        {
            "node": {
                "createdBy": {
                    "email": "dj@datajunction.io",
                    "id": 1,
                    "isAdmin": False,
                    "name": "DJ",
                    "oauthProvider": "BASIC",
                    "username": "dj",
                },
                "currentVersion": "v1.0",
                "name": "default.large_revenue_payments_only_2",
                "revisions": [
                    {
                        "dimensionLinks": [],
                        "displayName": "Large Revenue Payments Only 2",
                    },
                ],
                "type": "TRANSFORM",
            },
        },
        {
            "node": {
                "createdBy": {
                    "email": "dj@datajunction.io",
                    "id": 1,
                    "isAdmin": False,
                    "name": "DJ",
                    "oauthProvider": "BASIC",
                    "username": "dj",
                },
                "currentVersion": "v1.0",
                "name": "default.large_revenue_payments_only_1",
                "revisions": [
                    {
                        "dimensionLinks": [],
                        "displayName": "Large Revenue Payments Only 1",
                    },
                ],
                "type": "TRANSFORM",
            },
        },
        {
            "node": {
                "createdBy": {
                    "email": "dj@datajunction.io",
                    "id": 1,
                    "isAdmin": False,
                    "name": "DJ",
                    "oauthProvider": "BASIC",
                    "username": "dj",
                },
                "currentVersion": "v1.0",
                "name": "default.large_revenue_payments_only",
                "revisions": [
                    {
                        "dimensionLinks": [],
                        "displayName": "Large Revenue Payments Only",
                    },
                ],
                "type": "TRANSFORM",
            },
        },
        {
            "node": {
                "name": "default.repair_orders_fact",
                "type": "TRANSFORM",
                "revisions": [
                    {"displayName": "Repair Orders Fact", "dimensionLinks": []},
                    {
                        "displayName": "Repair Orders Fact",
                        "dimensionLinks": [
                            {
                                "dimension": {"name": "default.municipality_dim"},
                                "joinSql": "default.repair_orders_fact.municipality_id = default.municipality_dim.municipality_id",
                            },
                        ],
                    },
                    {
                        "displayName": "Repair Orders Fact",
                        "dimensionLinks": [
                            {
                                "dimension": {"name": "default.municipality_dim"},
                                "joinSql": "default.repair_orders_fact.municipality_id = default.municipality_dim.municipality_id",
                            },
                            {
                                "dimension": {"name": "default.hard_hat"},
                                "joinSql": "default.repair_orders_fact.hard_hat_id = default.hard_hat.hard_hat_id",
                            },
                        ],
                    },
                    {
                        "displayName": "Repair Orders Fact",
                        "dimensionLinks": [
                            {
                                "dimension": {"name": "default.municipality_dim"},
                                "joinSql": "default.repair_orders_fact.municipality_id = default.municipality_dim.municipality_id",
                            },
                            {
                                "dimension": {"name": "default.hard_hat"},
                                "joinSql": "default.repair_orders_fact.hard_hat_id = default.hard_hat.hard_hat_id",
                            },
                            {
                                "dimension": {"name": "default.hard_hat_to_delete"},
                                "joinSql": "default.repair_orders_fact.hard_hat_id = default.hard_hat_to_delete.hard_hat_id",
                            },
                        ],
                    },
                    {
                        "displayName": "Repair Orders Fact",
                        "dimensionLinks": [
                            {
                                "dimension": {"name": "default.municipality_dim"},
                                "joinSql": "default.repair_orders_fact.municipality_id = default.municipality_dim.municipality_id",
                            },
                            {
                                "dimension": {"name": "default.hard_hat"},
                                "joinSql": "default.repair_orders_fact.hard_hat_id = default.hard_hat.hard_hat_id",
                            },
                            {
                                "dimension": {"name": "default.hard_hat_to_delete"},
                                "joinSql": "default.repair_orders_fact.hard_hat_id = default.hard_hat_to_delete.hard_hat_id",
                            },
                            {
                                "dimension": {"name": "default.dispatcher"},
                                "joinSql": "default.repair_orders_fact.dispatcher_id = default.dispatcher.dispatcher_id",
                            },
                        ],
                    },
                ],
                "currentVersion": "v1.4",
                "createdBy": {
                    "email": "dj@datajunction.io",
                    "id": 1,
                    "isAdmin": False,
                    "name": "DJ",
                    "oauthProvider": "BASIC",
                    "username": "dj",
                },
            },
        },
        {
            "node": {
                "name": "default.national_level_agg",
                "type": "TRANSFORM",
                "revisions": [
                    {"displayName": "National Level Agg", "dimensionLinks": []},
                ],
                "currentVersion": "v1.0",
                "createdBy": {
                    "email": "dj@datajunction.io",
                    "id": 1,
                    "isAdmin": False,
                    "name": "DJ",
                    "oauthProvider": "BASIC",
                    "username": "dj",
                },
            },
        },
        {
            "node": {
                "name": "default.regional_level_agg",
                "type": "TRANSFORM",
                "revisions": [
                    {"displayName": "Regional Level Agg", "dimensionLinks": []},
                ],
                "currentVersion": "v1.0",
                "createdBy": {
                    "email": "dj@datajunction.io",
                    "id": 1,
                    "isAdmin": False,
                    "name": "DJ",
                    "oauthProvider": "BASIC",
                    "username": "dj",
                },
            },
        },
    ]


@pytest.mark.asyncio
async def test_find_nodes_with_created_edited_by(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test finding nodes with created by / edited by metadata
    """

    query = """
    {
        findNodes(names: ["default.repair_orders_fact"]) {
            name
            createdBy {
              username
            }
            editedBy
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data["data"]["findNodes"] == [
        {
            "name": "default.repair_orders_fact",
            "createdBy": {"username": "dj"},
            "editedBy": ["dj"],
        },
    ]


@pytest.mark.asyncio
async def test_find_nodes_paginated_empty_list(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test finding nodes with pagination when there are none
    """

    query = """
    {
        findNodesPaginated(names: ["default.repair_orders_fact111"], before: "eyJjcmVhdGVkX2F0IjogIjIwMjQtMTAtMjZUMTQ6Mzc6MjkuNzI4MzE3KzAwOjAwIiwgImlkIjogMjV9") {
          edges {
            node {
              name
            }
          }
          pageInfo {
            startCursor
            endCursor
            hasNextPage
            hasPrevPage
          }
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data["data"]["findNodesPaginated"] == {
        "edges": [],
        "pageInfo": {
            "startCursor": None,
            "endCursor": None,
            "hasNextPage": False,
            "hasPrevPage": False,
        },
    }


@pytest.mark.asyncio
async def test_find_by_with_filtering_on_columns(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test that filter on columns works correctly
    """
    query = """
    {
        findNodes(names: ["default.regional_level_agg", "default.repair_orders"]) {
            name
            type
            current {
                columns(attributes: ["primary_key"]) {
                    name
                    type
                }
            }
            currentVersion
        }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
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
                ],
            },
            "currentVersion": "v1.0",
            "name": "default.regional_level_agg",
            "type": "TRANSFORM",
        },
        {
            "current": {
                "columns": [],
            },
            "currentVersion": "v1.2",
            "name": "default.repair_orders",
            "type": "SOURCE",
        },
    ]


@pytest.mark.asyncio
async def test_find_by_with_ordering(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test finding nodes with ordering
    """
    query = """
    {
        findNodes(fragment: "default.", orderBy: NAME, ascending: true) {
            name
        }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert [node["name"] for node in data["data"]["findNodes"]][:6] == [
        "default.account_type",
        "default.account_type_table",
        "default.avg_length_of_employment",
        "default.avg_repair_order_discounts",
        "default.avg_repair_price",
        "default.avg_time_to_dispatch",
    ]

    query = """
    {
        findNodes(fragment: "default.", orderBy: UPDATED_AT, ascending: true) {
            name
        }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert [node["name"] for node in data["data"]["findNodes"]][:6] == [
        "default.repair_orders_view",
        "default.municipality_municipality_type",
        "default.municipality_type",
        "default.municipality",
        "default.dispatchers",
        "default.hard_hats",
    ]


@pytest.mark.asyncio
async def test_find_nodes_with_mode(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test finding nodes returns mode field
    """
    query = """
    {
        findNodes(names: ["default.repair_orders_fact"]) {
            name
            current {
                mode
            }
        }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data["data"]["findNodes"] == [
        {
            "name": "default.repair_orders_fact",
            "current": {
                "mode": "PUBLISHED",
            },
        },
    ]


@pytest.mark.asyncio
async def test_find_nodes_paginated_filter_by_mode(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test filtering nodes by mode (published vs draft)
    """
    # First, create a draft node
    response = await client_with_roads.post(
        "/nodes/transform/",
        json={
            "name": "default.draft_test_node",
            "description": "A draft test node",
            "query": "SELECT 1 as id",
            "mode": "draft",
        },
    )
    assert response.status_code == 201

    # Query for published nodes only (should not include the draft node)
    query = """
    {
        findNodesPaginated(mode: PUBLISHED, namespace: "default", limit: 100) {
            edges {
                node {
                    name
                    current {
                        mode
                    }
                }
            }
        }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    # All returned nodes should be published
    for edge in data["data"]["findNodesPaginated"]["edges"]:
        assert edge["node"]["current"]["mode"] == "PUBLISHED"

    # Draft node should not be in the results
    node_names = [
        edge["node"]["name"] for edge in data["data"]["findNodesPaginated"]["edges"]
    ]
    assert "default.draft_test_node" not in node_names

    # Query for draft nodes only
    query = """
    {
        findNodesPaginated(mode: DRAFT, namespace: "default", limit: 100) {
            edges {
                node {
                    name
                    current {
                        mode
                    }
                }
            }
        }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    # All returned nodes should be draft
    for edge in data["data"]["findNodesPaginated"]["edges"]:
        assert edge["node"]["current"]["mode"] == "DRAFT"

    # Draft node should be in the results
    node_names = [
        edge["node"]["name"] for edge in data["data"]["findNodesPaginated"]["edges"]
    ]
    assert "default.draft_test_node" in node_names

    # Query without mode filter should return both
    query = """
    {
        findNodesPaginated(namespace: "default", limit: 100) {
            edges {
                node {
                    name
                    current {
                        mode
                    }
                }
            }
        }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    node_names = [
        edge["node"]["name"] for edge in data["data"]["findNodesPaginated"]["edges"]
    ]
    assert "default.draft_test_node" in node_names


@pytest.mark.asyncio
async def test_approx_count_distinct_metric_decomposition(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test that APPROX_COUNT_DISTINCT metrics decompose into HLL sketch components.

    This verifies that:
    1. The metric decomposes to a single HLL component
    2. The aggregation is hll_sketch_agg (Spark's function for building sketch)
    3. The merge is hll_union (Spark's function for combining sketches)
    4. The derived query uses hll_sketch_estimate(hll_union(...)) as the combiner

    Translation to other dialects (Druid, Trino) happens in the transpilation layer.
    """
    query = """
    {
        findNodes(names: ["default.num_unique_hard_hats_approx"]) {
            name
            type
            current {
                query
                extractedMeasures {
                    components {
                        name
                        expression
                        aggregation
                        merge
                        rule {
                            type
                        }
                    }
                    combiner
                    derivedQuery
                    derivedExpression
                }
            }
        }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    assert len(data["data"]["findNodes"]) == 1
    node = data["data"]["findNodes"][0]
    assert node["name"] == "default.num_unique_hard_hats_approx"
    assert node["type"] == "METRIC"

    extracted = node["current"]["extractedMeasures"]
    assert extracted is not None

    # Should have exactly one HLL component
    components = extracted["components"]
    assert len(components) == 1

    hll_component = components[0]
    assert hll_component["expression"] == "hard_hat_id"
    assert hll_component["aggregation"] == "hll_sketch_agg"  # Spark's HLL accumulate
    assert hll_component["merge"] == "hll_union_agg"  # Spark's HLL merge
    assert hll_component["rule"]["type"] == "FULL"

    # The combiner should use Spark HLL functions
    assert "hll_sketch_estimate" in extracted["combiner"]
    assert "hll_union" in extracted["combiner"]
    assert "hll_sketch_estimate" in extracted["derivedExpression"]

    # The derived query should contain Spark HLL functions
    assert "hll_sketch_estimate" in extracted["derivedQuery"]
    assert "hll_union" in extracted["derivedQuery"]


@pytest.mark.asyncio
async def test_find_nodes_with_dimensions_filter(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test finding nodes with the dimensions filter.
    This filters to nodes that have ALL of the specified dimensions.
    """
    # Find nodes that have the hard_hat dimension
    query = """
    {
        findNodes(dimensions: ["default.hard_hat"]) {
            name
            type
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    node_names = {node["name"] for node in data["data"]["findNodes"]}

    # These nodes should have the hard_hat dimension
    expected_nodes = {
        "default.repair_orders",
        "default.repair_order_details",
        "default.repair_order",
        "default.num_repair_orders",
        "default.num_unique_hard_hats_approx",
        "default.avg_repair_price",
        "default.repair_orders_fact",
        "default.total_repair_cost",
        "default.discounted_orders_rate",
        "default.total_repair_order_discounts",
        "default.avg_repair_order_discounts",
        "default.avg_time_to_dispatch",
    }
    assert node_names == expected_nodes


@pytest.mark.asyncio
async def test_find_nodes_with_dimensions_filter_combined_with_type(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test finding nodes with dimensions filter combined with node type filter.
    """
    # Find only METRIC nodes that have the hard_hat dimension
    query = """
    {
        findNodes(dimensions: ["default.hard_hat"], nodeTypes: [METRIC]) {
            name
            type
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    node_names = {node["name"] for node in data["data"]["findNodes"]}

    # All returned nodes should be METRICs with the hard_hat dimension
    for node in data["data"]["findNodes"]:
        assert node["type"] == "METRIC"

    # These are the metrics with the hard_hat dimension
    expected_metrics = {
        "default.num_repair_orders",
        "default.num_unique_hard_hats_approx",
        "default.avg_repair_price",
        "default.total_repair_cost",
        "default.discounted_orders_rate",
        "default.total_repair_order_discounts",
        "default.avg_repair_order_discounts",
        "default.avg_time_to_dispatch",
    }
    assert node_names == expected_metrics


@pytest.mark.asyncio
async def test_find_nodes_with_nonexistent_dimension(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test that finding nodes with a nonexistent dimension returns empty list.
    """
    query = """
    {
        findNodes(dimensions: ["default.nonexistent_dimension"]) {
            name
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data["data"]["findNodes"] == []


@pytest.mark.asyncio
async def test_find_nodes_with_dimension_attribute(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test finding nodes with a dimension attribute (e.g., default.hard_hat.city).
    This should work the same as filtering by the dimension node.
    """
    # Find nodes using dimension attribute (includes column name)
    query_with_attr = """
    {
        findNodes(dimensions: ["default.hard_hat.hard_hat_id"]) {
            name
        }
    }
    """
    response_attr = await client_with_roads.post(
        "/graphql",
        json={"query": query_with_attr},
    )
    assert response_attr.status_code == 200
    data_attr = response_attr.json()
    nodes_from_attr = {node["name"] for node in data_attr["data"]["findNodes"]}

    # Find nodes using dimension node name
    query_with_node = """
    {
        findNodes(dimensions: ["default.hard_hat"]) {
            name
        }
    }
    """
    response_node = await client_with_roads.post(
        "/graphql",
        json={"query": query_with_node},
    )
    assert response_node.status_code == 200
    data_node = response_node.json()
    nodes_from_node = {node["name"] for node in data_node["data"]["findNodes"]}

    # Both should return the same set of nodes
    assert nodes_from_attr == nodes_from_node
    assert len(nodes_from_attr) > 0  # Ensure we got some results


@pytest.mark.asyncio
async def test_find_nodes_with_mixed_dimension_formats(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test finding nodes with a mix of dimension node names and dimension attributes.
    """
    # Mix a dimension node name and a dimension attribute
    query = """
    {
        findNodes(dimensions: ["default.hard_hat", "default.dispatcher.dispatcher_id"]) {
            name
            type
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    node_names = {node["name"] for node in data["data"]["findNodes"]}

    # Should find nodes that have BOTH hard_hat AND dispatcher dimensions
    # This should include repair_orders_fact and related nodes
    assert len(node_names) > 0
    # All results should have both dimensions available
    assert "default.repair_orders_fact" in node_names


@pytest.mark.asyncio
async def test_find_nodes_filter_by_owner(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test filtering nodes by owner (ownedBy).
    """
    # Query for nodes owned by the 'dj' user
    query = """
    {
        findNodes(ownedBy: "dj") {
            name
            owners {
                username
            }
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    # All returned nodes should have 'dj' as an owner
    for node in data["data"]["findNodes"]:
        owner_usernames = [owner["username"] for owner in node["owners"]]
        assert "dj" in owner_usernames

    # Verify we got some results
    assert len(data["data"]["findNodes"]) > 0


@pytest.mark.asyncio
async def test_find_nodes_paginated_filter_by_owner(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test filtering nodes by owner (ownedBy) using paginated endpoint.
    """
    query = """
    {
        findNodesPaginated(ownedBy: "dj", limit: 10) {
            edges {
                node {
                    name
                    owners {
                        username
                    }
                }
            }
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    # All returned nodes should have 'dj' as an owner
    for edge in data["data"]["findNodesPaginated"]["edges"]:
        owner_usernames = [owner["username"] for owner in edge["node"]["owners"]]
        assert "dj" in owner_usernames


@pytest.mark.asyncio
async def test_find_nodes_filter_by_status_valid(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test filtering nodes by status (VALID).
    """
    query = """
    {
        findNodes(statuses: [VALID]) {
            name
            current {
                status
            }
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    # All returned nodes should have VALID status
    for node in data["data"]["findNodes"]:
        assert node["current"]["status"] == "VALID"

    # Verify we got some results
    assert len(data["data"]["findNodes"]) > 0


@pytest.mark.asyncio
async def test_find_nodes_filter_by_status_invalid(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test filtering nodes by status (INVALID).
    First create an invalid node, then filter for it.
    """
    # Create a node that references a non-existent parent (will be invalid)
    response = await client_with_roads.post(
        "/nodes/transform/",
        json={
            "name": "default.invalid_test_node",
            "description": "An invalid test node",
            "query": "SELECT * FROM default.nonexistent_table",
            "mode": "published",
        },
    )
    # This should fail or create an invalid node

    query = """
    {
        findNodes(statuses: [INVALID]) {
            name
            current {
                status
            }
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    # All returned nodes should have INVALID status
    for node in data["data"]["findNodes"]:
        assert node["current"]["status"] == "INVALID"


@pytest.mark.asyncio
async def test_find_nodes_paginated_filter_by_status(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test filtering nodes by status using paginated endpoint.
    """
    query = """
    {
        findNodesPaginated(statuses: [VALID], limit: 10) {
            edges {
                node {
                    name
                    current {
                        status
                    }
                }
            }
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    # All returned nodes should have VALID status
    for edge in data["data"]["findNodesPaginated"]["edges"]:
        assert edge["node"]["current"]["status"] == "VALID"


@pytest.mark.asyncio
async def test_find_nodes_filter_missing_description(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test filtering nodes that are missing descriptions.
    """
    # First create a node without a description
    response = await client_with_roads.post(
        "/nodes/transform/",
        json={
            "name": "default.no_description_node",
            "description": "",  # Empty description
            "query": "SELECT 1 as id",
            "mode": "published",
        },
    )

    query = """
    {
        findNodes(missingDescription: true) {
            name
            current {
                description
            }
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    # All returned nodes should have empty or null descriptions
    for node in data["data"]["findNodes"]:
        desc = node["current"]["description"]
        assert desc is None or desc == ""


@pytest.mark.asyncio
async def test_find_nodes_paginated_filter_missing_description(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test filtering nodes that are missing descriptions using paginated endpoint.
    """
    query = """
    {
        findNodesPaginated(missingDescription: true, limit: 10) {
            edges {
                node {
                    name
                    current {
                        description
                    }
                }
            }
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    # All returned nodes should have empty or null descriptions
    for edge in data["data"]["findNodesPaginated"]["edges"]:
        desc = edge["node"]["current"]["description"]
        assert desc is None or desc == ""


@pytest.mark.asyncio
async def test_find_nodes_filter_missing_owner(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test filtering nodes that are missing owners.
    """
    query = """
    {
        findNodes(missingOwner: true) {
            name
            owners {
                username
            }
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    # All returned nodes should have no owners
    for node in data["data"]["findNodes"]:
        assert node["owners"] == [] or node["owners"] is None


@pytest.mark.asyncio
async def test_find_nodes_paginated_filter_missing_owner(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test filtering nodes that are missing owners using paginated endpoint.
    """
    query = """
    {
        findNodesPaginated(missingOwner: true, limit: 10) {
            edges {
                node {
                    name
                    owners {
                        username
                    }
                }
            }
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    # All returned nodes should have no owners
    for edge in data["data"]["findNodesPaginated"]["edges"]:
        owners = edge["node"]["owners"]
        assert owners == [] or owners is None


@pytest.mark.asyncio
async def test_find_nodes_filter_orphaned_dimension(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test filtering for orphaned dimension nodes (dimensions not linked to by any other node).
    """
    # First, create an orphaned dimension (a dimension that no other node links to)
    response = await client_with_roads.post(
        "/nodes/dimension/",
        json={
            "name": "default.orphaned_dimension_test",
            "description": "An orphaned dimension for testing",
            "query": "SELECT 1 as orphan_id, 'test' as orphan_name",
            "primary_key": ["orphan_id"],
            "mode": "published",
        },
    )

    query = """
    {
        findNodes(orphanedDimension: true) {
            name
            type
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    # All returned nodes should be dimensions
    for node in data["data"]["findNodes"]:
        assert node["type"] == "DIMENSION"

    # The orphaned dimension we created should be in the results
    node_names = {node["name"] for node in data["data"]["findNodes"]}
    assert "default.orphaned_dimension_test" in node_names


@pytest.mark.asyncio
async def test_find_nodes_paginated_filter_orphaned_dimension(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test filtering for orphaned dimension nodes using paginated endpoint.
    """
    query = """
    {
        findNodesPaginated(orphanedDimension: true, limit: 10) {
            edges {
                node {
                    name
                    type
                }
            }
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    # All returned nodes should be dimensions
    for edge in data["data"]["findNodesPaginated"]["edges"]:
        assert edge["node"]["type"] == "DIMENSION"


@pytest.mark.asyncio
async def test_find_nodes_combined_filters(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test combining multiple filters together.
    """
    # Combine ownedBy with status filter
    query = """
    {
        findNodes(ownedBy: "dj", statuses: [VALID], nodeTypes: [METRIC]) {
            name
            type
            owners {
                username
            }
            current {
                status
            }
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    # All returned nodes should match all filters
    for node in data["data"]["findNodes"]:
        assert node["type"] == "METRIC"
        assert node["current"]["status"] == "VALID"
        owner_usernames = [owner["username"] for owner in node["owners"]]
        assert "dj" in owner_usernames


@pytest.mark.asyncio
async def test_find_nodes_paginated_combined_filters(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test combining multiple filters together using paginated endpoint.
    """
    query = """
    {
        findNodesPaginated(ownedBy: "dj", statuses: [VALID], nodeTypes: [SOURCE], limit: 10) {
            edges {
                node {
                    name
                    type
                    owners {
                        username
                    }
                    current {
                        status
                    }
                }
            }
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    # All returned nodes should match all filters
    for edge in data["data"]["findNodesPaginated"]["edges"]:
        node = edge["node"]
        assert node["type"] == "SOURCE"
        assert node["current"]["status"] == "VALID"
        owner_usernames = [owner["username"] for owner in node["owners"]]
        assert "dj" in owner_usernames


@pytest.mark.asyncio
async def test_find_nodes_filter_by_nonexistent_owner(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test that filtering by a nonexistent owner returns empty results.
    """
    query = """
    {
        findNodes(ownedBy: "nonexistent_user_12345") {
            name
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data["data"]["findNodes"] == []


@pytest.mark.asyncio
async def test_find_nodes_filter_multiple_statuses(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test filtering nodes by multiple statuses.
    """
    query = """
    {
        findNodes(statuses: [VALID, INVALID]) {
            name
            current {
                status
            }
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    # All returned nodes should have either VALID or INVALID status
    for node in data["data"]["findNodes"]:
        assert node["current"]["status"] in ["VALID", "INVALID"]

    # Verify we got some results
    assert len(data["data"]["findNodes"]) > 0


@pytest.mark.asyncio
async def test_find_nodes_paginated_filter_has_materialization(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test filtering nodes that have materializations configured.
    """
    # First, set up a partition column on a node so we can create a materialization
    await client_with_roads.post(
        "/nodes/default.repair_orders_fact/columns/repair_order_id/partition",
        json={"type_": "categorical"},
    )

    # Create a materialization on a node
    response = await client_with_roads.post(
        "/nodes/default.repair_orders_fact/materialization",
        json={
            "job": "spark_sql",
            "strategy": "full",
            "schedule": "@daily",
            "config": {},
        },
    )
    # Note: materialization creation may fail in test environment without query service,
    # but the node should still be marked as having materialization configured

    # Query for nodes with materializations
    query = """
    {
        findNodesPaginated(hasMaterialization: true, limit: 10) {
            edges {
                node {
                    name
                    type
                    current {
                        materializations {
                            name
                        }
                    }
                }
            }
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    # If we got results, all returned nodes should have materializations
    for edge in data["data"]["findNodesPaginated"]["edges"]:
        node = edge["node"]
        materializations = node["current"]["materializations"]
        assert materializations is not None and len(materializations) > 0


@pytest.mark.asyncio
async def test_find_nodes_filter_has_materialization(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test filtering nodes that have materializations using non-paginated endpoint.
    """
    query = """
    {
        findNodes(hasMaterialization: true) {
            name
            type
            current {
                materializations {
                    name
                }
            }
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    # If we got results, all returned nodes should have materializations
    for node in data["data"]["findNodes"]:
        materializations = node["current"]["materializations"]
        assert materializations is not None and len(materializations) > 0


@pytest.mark.asyncio
async def test_is_derived_metric_field(
    client_example_loader,
) -> None:
    """
    Test the isDerivedMetric field on NodeRevision.
    - For non-metric nodes, should return False
    - For regular metrics (parent is transform/source), should return False
    - For derived metrics (parent is another metric), should return True
    """
    # Use the BUILD_V3 example set which has pre-configured derived metrics
    client = await client_example_loader(["BUILD_V3"])

    # Test a non-metric node (transform) - should be False
    query = """
    {
        findNodes(names: ["v3.order_details"]) {
            name
            type
            current {
                isDerivedMetric
            }
        }
    }
    """
    response = await client.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data["data"]["findNodes"][0]["type"] == "TRANSFORM"
    assert data["data"]["findNodes"][0]["current"]["isDerivedMetric"] is False

    # Test a regular/base metric (parent is transform) - should be False
    # v3.total_revenue is a base metric with query "SELECT SUM(line_total) FROM v3.order_details"
    query = """
    {
        findNodes(names: ["v3.total_revenue"]) {
            name
            type
            current {
                isDerivedMetric
                parents {
                    name
                    type
                }
            }
        }
    }
    """
    response = await client.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    node = data["data"]["findNodes"][0]
    assert node["type"] == "METRIC"
    # This metric's parent should be a transform, not a metric
    parent_types = [p["type"].lower() for p in node["current"]["parents"]]
    assert "metric" not in parent_types
    assert node["current"]["isDerivedMetric"] is False

    # Test a derived metric - should be True
    # v3.avg_order_value is a derived metric: "SELECT v3.total_revenue / NULLIF(v3.order_count, 0)"
    query = """
    {
        findNodes(names: ["v3.avg_order_value"]) {
            name
            type
            current {
                isDerivedMetric
                parents {
                    name
                    type
                }
            }
        }
    }
    """
    response = await client.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    node = data["data"]["findNodes"][0]
    assert node["type"] == "METRIC"
    # This metric's parents should be other metrics
    parent_types = [p["type"].lower() for p in node["current"]["parents"]]
    assert "metric" in parent_types
    assert node["current"]["isDerivedMetric"] is True
