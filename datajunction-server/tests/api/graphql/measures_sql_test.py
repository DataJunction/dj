"""
Tests for generate SQL queries
"""

from unittest import mock

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_measures_sql(
    module__client_with_roads: AsyncClient,
):
    """
    Test requesting measures SQL for a set of metrics, dimensions, and filters
    """

    query = """
    query GetMeasuresSQL($metrics: [String!], $dimensions: [String!], $filters: [String!]) {
      measuresSql(
        cube: {metrics: $metrics, dimensions: $dimensions, filters: $filters}
        preaggregate: true
      ) {
        sql
        node {
          name
        }
        columns {
          name
          semanticType
          semanticEntity {
            name
            node
            column
          }
        }
        dialect
        upstreamTables
        errors {
          message
        }
      }
    }
    """

    response = await module__client_with_roads.post(
        "/graphql",
        json={
            "query": query,
            "variables": {
                "metrics": ["default.num_repair_orders", "default.avg_repair_price"],
                "dimensions": ["default.us_state.state_name"],
                "filters": ["default.us_state.state_name = 'AZ'"],
            },
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data["data"]["measuresSql"]) == 1

    assert data["data"]["measuresSql"][0] == {
        "columns": [
            {
                "name": "default_DOT_us_state_DOT_state_name",
                "semanticEntity": {
                    "column": "state_name",
                    "name": "default.us_state.state_name",
                    "node": "default.us_state",
                },
                "semanticType": "DIMENSION",
            },
            {
                "name": "repair_order_id_count_bd241964",
                "semanticEntity": {
                    "column": "repair_order_id_count_bd241964",
                    "name": "default.repair_orders_fact.repair_order_id_count_bd241964",
                    "node": "default.repair_orders_fact",
                },
                "semanticType": "MEASURE",
            },
            {
                "name": "price_count_935e7117",
                "semanticEntity": {
                    "column": "price_count_935e7117",
                    "name": "default.repair_orders_fact.price_count_935e7117",
                    "node": "default.repair_orders_fact",
                },
                "semanticType": "MEASURE",
            },
            {
                "name": "price_sum_935e7117",
                "semanticEntity": {
                    "column": "price_sum_935e7117",
                    "name": "default.repair_orders_fact.price_sum_935e7117",
                    "node": "default.repair_orders_fact",
                },
                "semanticType": "MEASURE",
            },
        ],
        "dialect": "SPARK",
        "errors": [],
        "node": {
            "name": "default.repair_orders_fact",
        },
        "sql": mock.ANY,
        "upstreamTables": [
            "default.roads.repair_orders",
            "default.roads.repair_order_details",
            "default.roads.hard_hats",
            "default.roads.us_states",
        ],
    }


@pytest.mark.asyncio
async def test_materialization_plan(
    module__client_with_roads: AsyncClient,
):
    """
    Test requesting materialization plan for a set of metrics, dimensions, and filters
    """

    query = """
    query MaterializationPlan($metrics: [String!]!, $dimensions: [String!]!, $filters: [String!]) {
      materializationPlan(
        cube: {metrics: $metrics, dimensions: $dimensions, filters: $filters}
      ) {
        units {
          upstream {
            name
            version
          }
          measures {
            name
            aggregation
            expression
            rule {
              type
              level
            }
          }
          grainDimensions {
            name
            version
          }
          filters
          filterRefs {
            name
            version
          }
        }
      }
    }
    """

    response = await module__client_with_roads.post(
        "/graphql",
        json={
            "query": query,
            "variables": {
                "metrics": ["default.num_repair_orders", "default.avg_repair_price"],
                "dimensions": [
                    "default.us_state.state_name",
                    "default.hard_hat.last_name",
                ],
                "filters": [
                    "default.us_state.state_name = 'AZ' OR default.hard_hat.first_name = 'B'",
                ],
            },
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data["data"]["materializationPlan"] == {
        "units": [
            {
                "filterRefs": [
                    {
                        "name": "default.us_state.state_name",
                        "version": mock.ANY,
                    },
                    {
                        "name": "default.hard_hat.first_name",
                        "version": mock.ANY,
                    },
                ],
                "filters": [
                    "default.us_state.state_name = 'AZ' OR "
                    "default.hard_hat.first_name = 'B'",
                ],
                "grainDimensions": [
                    {
                        "name": "default.us_state",
                        "version": mock.ANY,
                    },
                    {
                        "name": "default.hard_hat",
                        "version": mock.ANY,
                    },
                ],
                "measures": [
                    {
                        "aggregation": "COUNT",
                        "expression": "repair_order_id",
                        "name": "repair_order_id_count_bd241964",
                        "rule": {
                            "level": None,
                            "type": "FULL",
                        },
                    },
                    {
                        "aggregation": "COUNT",
                        "expression": "price",
                        "name": "price_count_935e7117",
                        "rule": {
                            "level": None,
                            "type": "FULL",
                        },
                    },
                    {
                        "aggregation": "SUM",
                        "expression": "price",
                        "name": "price_sum_935e7117",
                        "rule": {
                            "level": None,
                            "type": "FULL",
                        },
                    },
                ],
                "upstream": {
                    "name": "default.repair_orders_fact",
                    "version": mock.ANY,
                },
            },
        ],
    }
