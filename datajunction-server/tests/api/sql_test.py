"""Tests for the /sql/ endpoint"""
import datetime
from typing import Callable, List, Optional

# pylint: disable=line-too-long,too-many-lines
# pylint: disable=C0302
import duckdb
import pytest
from httpx import AsyncClient, Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.api.main import app
from datajunction_server.database.column import Column
from datajunction_server.database.database import Database
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.queryrequest import QueryBuildType, QueryRequest
from datajunction_server.internal.access.authorization import validate_access
from datajunction_server.models import access
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.sql.parsing.types import StringType
from tests.sql.utils import assert_query_strings_equal, compare_query_strings


@pytest.mark.asyncio
async def test_sql(
    session: AsyncSession,
    client: AsyncClient,
) -> None:
    """
    Test ``GET /sql/{name}/``.
    """
    database = Database(name="test", URI="blah://", tables=[])

    source_node = Node(
        name="default.my_table",
        type=NodeType.SOURCE,
        current_version="1",
    )
    source_node_rev = NodeRevision(
        name=source_node.name,
        node=source_node,
        version="1",
        schema_="rev",
        table="my_table",
        columns=[Column(name="one", type=StringType(), order=0)],
        type=NodeType.SOURCE,
    )

    node = Node(name="default.a_metric", type=NodeType.METRIC, current_version="1")
    node_revision = NodeRevision(
        name=node.name,
        node=node,
        version="1",
        query="SELECT COUNT(*) FROM default.my_table",
        type=NodeType.METRIC,
    )
    node_revision.parents = [source_node]
    session.add(database)
    session.add(node_revision)
    session.add(source_node_rev)
    await session.commit()

    response = (await client.get("/sql/default.a_metric/")).json()
    assert compare_query_strings(
        response["sql"],
        "SELECT  COUNT(*) default_DOT_a_metric \n FROM rev.my_table AS default_DOT_my_table\n",
    )
    assert response["columns"] == [
        {
            "column": "a_metric",
            "name": "default_DOT_a_metric",
            "node": "default",
            "type": "bigint",
            "semantic_type": None,
            "semantic_entity": "default.a_metric",
        },
    ]
    assert response["dialect"] is None

    # Check that this query request has been saved
    query_request = (await session.execute(select(QueryRequest))).scalars().all()
    assert len(query_request) == 1
    assert query_request[0].nodes == ["default.a_metric@1"]
    assert query_request[0].dimensions == []
    assert query_request[0].filters == []
    assert query_request[0].orderby == []
    assert query_request[0].limit is None
    assert query_request[0].query_type == QueryBuildType.NODE
    assert compare_query_strings(query_request[0].query, response["sql"])
    assert query_request[0].columns == response["columns"]


@pytest.fixture
def transform_node_sql_request(client_with_roads: AsyncClient):
    """
    Request SQL for a transform node
    GET `/sql/default.repair_orders_fact`
    """

    async def _make_request() -> Response:
        transform_node_sql_params = {
            "dimensions": [
                "default.dispatcher.company_name",
                "default.hard_hat.state",
                "default.hard_hat.hard_hat_id",
            ],
            "filters": ["default.hard_hat.state = 'CA'"],
            "limit": 200,
            "orderby": ["default.dispatcher.company_name ASC"],
        }
        response = await client_with_roads.get(
            "/sql/default.repair_orders_fact",
            params=transform_node_sql_params,
        )
        return response

    return _make_request


@pytest.fixture
def measures_sql_request(client_with_roads: AsyncClient):
    """
    Request measures SQL for some metrics
    GET `/sql/measures`
    """

    async def _make_request() -> Response:
        measures_sql_params = {
            "metrics": ["default.num_repair_orders", "default.total_repair_cost"],
            "dimensions": [
                "default.dispatcher.company_name",
                "default.hard_hat.state",
            ],
            "filters": ["default.hard_hat.state = 'CA'"],
            "include_all_columns": True,
        }
        response = await client_with_roads.get(
            "/sql/measures",
            params=measures_sql_params,
        )
        return response

    return _make_request


@pytest.fixture
def metrics_sql_request(client_with_roads: AsyncClient):
    """
    Request metrics SQL for some metrics
    GET `/sql`
    """

    async def _make_request() -> Response:
        metrics_sql_params = {
            "metrics": ["default.num_repair_orders", "default.total_repair_cost"],
            "dimensions": [
                "default.dispatcher.company_name",
                "default.hard_hat.state",
            ],
            "filters": ["default.hard_hat.state = 'CA'"],
            "orderby": ["default.num_repair_orders DESC", "default.hard_hat.state"],
            "limit": 50,
        }
        return await client_with_roads.get(
            "/sql",
            params=metrics_sql_params,
        )

    return _make_request


@pytest.fixture
def update_transform_node(client_with_roads: AsyncClient):
    """
    Update transform node with a simplified query
    """

    async def _make_request() -> Response:
        response = await client_with_roads.patch(
            "/nodes/default.repair_orders_fact",
            json={
                "query": """SELECT
          repair_orders.repair_order_id,
          repair_orders.municipality_id,
          repair_orders.hard_hat_id,
          repair_orders.dispatcher_id,
          repair_orders.order_date,
          repair_orders.dispatched_date,
          repair_orders.required_date,
          repair_order_details.discount,
          repair_order_details.price * repair_order_details.quantity AS total_repair_cost
        FROM
          default.repair_orders repair_orders
        JOIN
          default.repair_order_details repair_order_details
        ON repair_orders.repair_order_id = repair_order_details.repair_order_id""",
            },
        )
        return response

    return _make_request


async def get_query_requests(session: AsyncSession, query_type: QueryBuildType):
    """
    Get all query requests of the specific query type
    """
    return (
        (
            await session.execute(
                select(QueryRequest)
                .where(
                    QueryRequest.query_type == query_type,
                )
                .order_by(QueryRequest.created_at.asc()),
            )
        )
        .scalars()
        .all()
    )


@pytest.mark.asyncio
async def test_saving_node_sql_requests(  # pylint: disable=too-many-statements
    session: AsyncSession,
    client_with_roads: AsyncClient,
    transform_node_sql_request,  # pylint: disable=redefined-outer-name
    update_transform_node,  # pylint: disable=redefined-outer-name
) -> None:
    """
    Test different scenarios involving saving and reusing cached query requests when
    requesting node SQL for a transform.
    """
    response = await transform_node_sql_request()

    # Check that this query request has been saved
    query_request = await get_query_requests(session, QueryBuildType.NODE)
    assert len(query_request) == 1
    assert query_request[0].nodes == ["default.repair_orders_fact@v1.0"]
    assert query_request[0].parents == [
        "default.repair_order_details@v1.0",
        "default.repair_orders@v1.0",
    ]
    assert query_request[0].dimensions == [
        "default.dispatcher.company_name@v1.0",
        "default.hard_hat.state@v1.0",
        "default.hard_hat.hard_hat_id@v1.0",
    ]
    assert query_request[0].filters == ["default.hard_hat.state@v1.0 = 'CA'"]
    assert query_request[0].orderby == [
        "default.dispatcher.company_name@v1.0 ASC",
    ]
    assert query_request[0].limit == 200
    assert query_request[0].query_type == QueryBuildType.NODE
    assert query_request[0].query.strip() == response.json()["sql"].strip()
    assert query_request[0].columns == response.json()["columns"]

    # Requesting it again should reuse the saved request
    await transform_node_sql_request()
    query_requests = await get_query_requests(session, QueryBuildType.NODE)
    assert len(query_requests) == 1

    # Update the transform node to a query that invalidates the SQL request
    response = await client_with_roads.patch(
        "/nodes/default.repair_orders_fact",
        json={
            "query": """SELECT
      repair_orders.repair_order_id
    FROM
      default.repair_orders repair_orders""",
        },
    )
    assert response.status_code == 200

    # This should now trigger error messages when requesting SQL
    response = await transform_node_sql_request()
    assert (
        "Cannot resolve type of column default.repair_orders_fact.hard_hat_id"
        in response.json()["message"]
    )

    # Update the transform node with a new query
    response = await update_transform_node()
    assert response.status_code == 200

    response = (await transform_node_sql_request()).json()

    # Check that the node update triggered an updated query request to be saved. Note that
    # default.repair_orders_fact's version gets bumped, but default.hard_hat will stay the same
    query_request = await get_query_requests(session, QueryBuildType.NODE)
    assert len(query_request) == 2
    assert query_request[1].nodes == ["default.repair_orders_fact@v3.0"]
    assert query_request[1].parents == [
        "default.repair_order_details@v1.0",
        "default.repair_orders@v1.0",
    ]
    assert query_request[1].dimensions == [
        "default.dispatcher.company_name@v1.0",
        "default.hard_hat.state@v1.0",
        "default.hard_hat.hard_hat_id@v1.0",
    ]
    assert query_request[1].filters == ["default.hard_hat.state@v1.0 = 'CA'"]
    assert query_request[1].orderby == [
        "default.dispatcher.company_name@v1.0 ASC",
    ]
    assert query_request[1].limit == 200
    assert query_request[1].query_type == QueryBuildType.NODE
    assert query_request[1].query.strip() == response["sql"].strip()
    assert query_request[1].columns == response["columns"]

    # Update the dimension node default.hard_hat with a new query
    response = await client_with_roads.patch(
        "/nodes/default.hard_hat",
        json={"query": "SELECT hard_hat_id, state FROM default.hard_hats"},
    )
    assert response.status_code == 200

    # Request the same node query again
    response = (await transform_node_sql_request()).json()

    # Check that the dimension node update triggered an updated query request to be saved. Now
    # default.repair_orders_fact's version remains the same, but default.hard_hat gets bumped
    query_request = await get_query_requests(session, QueryBuildType.NODE)
    assert len(query_request) == 3
    assert query_request[2].nodes == ["default.repair_orders_fact@v3.0"]
    assert query_request[2].parents == [
        "default.repair_order_details@v1.0",
        "default.repair_orders@v1.0",
    ]
    assert query_request[2].dimensions == [
        "default.dispatcher.company_name@v1.0",
        "default.hard_hat.state@v2.0",
        "default.hard_hat.hard_hat_id@v2.0",
    ]
    assert query_request[2].filters == ["default.hard_hat.state@v2.0 = 'CA'"]
    assert query_request[2].orderby == [
        "default.dispatcher.company_name@v1.0 ASC",
    ]
    assert query_request[2].limit == 200
    assert query_request[2].query_type == QueryBuildType.NODE
    assert query_request[2].query.strip() == response["sql"].strip()
    assert query_request[2].columns == response["columns"]

    # Remove a dimension node link to default.hard_hat
    response = await client_with_roads.request(
        "DELETE",
        "/nodes/default.repair_orders_fact/link",
        json={
            "dimension_node": "default.hard_hat",
        },
    )
    assert response.status_code == 201

    # Now requesting metrics SQL with default.hard_hat.state should fail
    response = (await transform_node_sql_request()).json()
    assert response["message"] == (
        "default.hard_hat.hard_hat_id, default.hard_hat.state are not available dimensions "
        "on default.repair_orders_fact"
    )


@pytest.mark.asyncio
async def test_saving_measures_sql_requests(
    session: AsyncSession,
    client_with_roads: AsyncClient,
    measures_sql_request,  # pylint: disable=redefined-outer-name
    update_transform_node,  # pylint: disable=redefined-outer-name
) -> None:
    """
    Test saving query request while requesting measures SQL for a set of metrics + dimensions
    + filters. It also checks that additional arguments like `include_all_columns` are recorded
    in the query request key.
    - Requesting metrics SQL (for a set of metrics + dimensions)
    """
    response = (await measures_sql_request()).json()

    # Check that the measures SQL request was saved
    query_requests = await get_query_requests(session, QueryBuildType.MEASURES)
    assert len(query_requests) == 1
    assert query_requests[0].nodes == [
        "default.num_repair_orders@v1.0",
        "default.total_repair_cost@v1.0",
    ]
    assert query_requests[0].dimensions == [
        "default.dispatcher.company_name@v1.0",
        "default.hard_hat.state@v1.0",
    ]
    assert query_requests[0].filters == ["default.hard_hat.state@v1.0 = 'CA'"]
    assert query_requests[0].orderby == []
    assert query_requests[0].limit is None
    assert query_requests[0].query_type == QueryBuildType.MEASURES
    assert query_requests[0].other_args == {"include_all_columns": True}
    assert query_requests[0].query.strip() == response["sql"].strip()
    assert query_requests[0].columns == response["columns"]

    # Requesting it again should reuse the saved request
    await measures_sql_request()
    query_requests = await get_query_requests(session, QueryBuildType.MEASURES)
    assert len(query_requests) == 1

    # Update the underlying transform behind the metrics
    response = await update_transform_node()
    assert response.status_code == 200
    response = (await measures_sql_request()).json()

    # Check that the measures SQL request was saved
    query_requests = await get_query_requests(session, QueryBuildType.MEASURES)
    assert len(query_requests) == 2
    assert query_requests[1].nodes == [
        "default.num_repair_orders@v1.0",
        "default.total_repair_cost@v1.0",
    ]
    assert query_requests[1].parents == [
        "default.repair_order_details@v1.0",
        "default.repair_orders@v1.0",
        "default.repair_orders_fact@v2.0",
    ]
    assert query_requests[1].dimensions == [
        "default.dispatcher.company_name@v1.0",
        "default.hard_hat.state@v1.0",
    ]
    assert query_requests[1].filters == ["default.hard_hat.state@v1.0 = 'CA'"]
    assert query_requests[1].orderby == []
    assert query_requests[1].limit is None
    assert query_requests[1].query_type == QueryBuildType.MEASURES
    assert query_requests[1].other_args == {"include_all_columns": True}
    assert query_requests[1].query.strip() == response["sql"].strip()
    assert query_requests[1].columns == response["columns"]

    # Remove a dimension node link to default.hard_hat
    response = await client_with_roads.request(
        "DELETE",
        "/nodes/default.repair_orders_fact/link",
        json={
            "dimension_node": "default.hard_hat",
        },
    )
    assert response.status_code == 201

    # And requesting measures SQL with default.hard_hat.state should fail
    response = (await measures_sql_request()).json()
    assert response["message"] == (
        "default.hard_hat.state are not available dimensions on default.num_repair_orders, default.total_repair_cost"
    )


@pytest.mark.asyncio
async def test_saving_metrics_sql_requests(  # pylint: disable=too-many-statements
    session: AsyncSession,
    client_with_roads: AsyncClient,
    metrics_sql_request,  # pylint: disable=redefined-outer-name
    update_transform_node,  # pylint: disable=redefined-outer-name
) -> None:
    """
    Requesting metrics SQL for a set of metrics + dimensions + filters
    """
    response = (await metrics_sql_request()).json()

    # Check that the metrics SQL request was saved
    query_request = await get_query_requests(session, QueryBuildType.METRICS)
    assert len(query_request) == 1
    assert query_request[0].nodes == [
        "default.num_repair_orders@v1.0",
        "default.total_repair_cost@v1.0",
    ]
    assert query_request[0].parents == [
        "default.repair_order_details@v1.0",
        "default.repair_orders@v1.0",
        "default.repair_orders_fact@v1.0",
    ]
    assert query_request[0].dimensions == [
        "default.dispatcher.company_name@v1.0",
        "default.hard_hat.state@v1.0",
    ]
    assert query_request[0].filters == ["default.hard_hat.state@v1.0 = 'CA'"]
    assert query_request[0].orderby == [
        "default.num_repair_orders@v1.0 DESC",
        "default.hard_hat.state@v1.0",
    ]
    assert query_request[0].limit == 50
    assert query_request[0].engine_name is None
    assert query_request[0].engine_version is None
    assert query_request[0].other_args == {}

    assert query_request[0].query_type == QueryBuildType.METRICS
    assert query_request[0].query.strip() == response["sql"].strip()
    assert query_request[0].columns == response["columns"]

    # Requesting it again should reuse the cached values
    await metrics_sql_request()
    query_request = await get_query_requests(session, QueryBuildType.METRICS)
    assert len(query_request) == 1

    # Patch the underlying transform with a new query
    await update_transform_node()

    # Now requesting metrics SQL should not use the cached entry
    response = (await metrics_sql_request()).json()

    # Check that a new metrics SQL request was saved
    query_request = await get_query_requests(session, QueryBuildType.METRICS)
    assert len(query_request) == 2
    assert query_request[1].nodes == [
        "default.num_repair_orders@v1.0",
        "default.total_repair_cost@v1.0",
    ]
    assert query_request[1].parents == [
        "default.repair_order_details@v1.0",
        "default.repair_orders@v1.0",
        "default.repair_orders_fact@v2.0",
    ]
    assert query_request[1].dimensions == [
        "default.dispatcher.company_name@v1.0",
        "default.hard_hat.state@v1.0",
    ]
    assert query_request[1].filters == ["default.hard_hat.state@v1.0 = 'CA'"]
    assert query_request[1].orderby == [
        "default.num_repair_orders@v1.0 DESC",
        "default.hard_hat.state@v1.0",
    ]
    assert query_request[1].limit == 50
    assert query_request[1].engine_name is None
    assert query_request[1].engine_version is None
    assert query_request[1].other_args == {}

    assert query_request[1].query_type == QueryBuildType.METRICS
    assert query_request[1].query.strip() == response["sql"].strip()
    assert query_request[1].columns == response["columns"]

    # Request metrics SQL without dimensions, filters, limit or orderby
    metrics_sql_params = {
        "metrics": ["default.num_repair_orders", "default.total_repair_cost"],
    }
    response = (
        await client_with_roads.get(
            "/sql",
            params=metrics_sql_params,
        )
    ).json()

    # Check the query request entry that was saved
    query_request = await get_query_requests(session, QueryBuildType.METRICS)
    assert len(query_request) == 3
    assert query_request[2].nodes == [
        "default.num_repair_orders@v1.0",
        "default.total_repair_cost@v1.0",
    ]
    assert query_request[2].parents == [
        "default.repair_order_details@v1.0",
        "default.repair_orders@v1.0",
        "default.repair_orders_fact@v2.0",
    ]
    assert query_request[2].dimensions == []
    assert query_request[2].filters == []
    assert query_request[2].orderby == []
    assert query_request[2].limit is None
    assert query_request[2].engine_name is None
    assert query_request[2].engine_version is None
    assert query_request[2].other_args == {}

    assert query_request[2].query_type == QueryBuildType.METRICS
    assert query_request[2].query.strip() == response["sql"].strip()
    assert query_request[2].columns == response["columns"]

    # Request it again and check that no new entry was created
    await client_with_roads.get(
        "/sql",
        params=metrics_sql_params,
    )
    query_request = await get_query_requests(session, QueryBuildType.METRICS)
    assert len(query_request) == 3


@pytest.mark.parametrize(
    "groups, node_name, dimensions, filters, sql, columns, rows",
    [
        # querying on source node with filter on joinable dimension
        (
            ["ROADS"],
            "default.repair_orders",
            ["default.hard_hat.state"],
            ["default.hard_hat.state='NY'"],
            """
            SELECT default_DOT_repair_orders.repair_order_id default_DOT_repair_orders_DOT_repair_order_id,
              default_DOT_repair_orders.municipality_id default_DOT_repair_orders_DOT_municipality_id,
              default_DOT_repair_orders.hard_hat_id default_DOT_repair_orders_DOT_hard_hat_id,
              default_DOT_repair_orders.order_date default_DOT_repair_orders_DOT_order_date,
              default_DOT_repair_orders.required_date default_DOT_repair_orders_DOT_required_date,
              default_DOT_repair_orders.dispatched_date default_DOT_repair_orders_DOT_dispatched_date,
              default_DOT_repair_orders.dispatcher_id default_DOT_repair_orders_DOT_dispatcher_id,
              default_DOT_hard_hat.state default_DOT_hard_hat_DOT_state
            FROM roads.repair_orders AS default_DOT_repair_orders
              LEFT JOIN (
                SELECT default_DOT_repair_orders.repair_order_id,
                  default_DOT_repair_orders.municipality_id,
                  default_DOT_repair_orders.hard_hat_id,
                  default_DOT_repair_orders.dispatcher_id
                FROM roads.repair_orders AS default_DOT_repair_orders
              ) AS default_DOT_repair_order ON default_DOT_repair_orders.repair_order_id = default_DOT_repair_order.repair_order_id
              LEFT JOIN (
                SELECT default_DOT_hard_hats.hard_hat_id,
                  default_DOT_hard_hats.state
                FROM roads.hard_hats AS default_DOT_hard_hats
              ) AS default_DOT_hard_hat ON default_DOT_repair_order.hard_hat_id = default_DOT_hard_hat.hard_hat_id
            WHERE default_DOT_hard_hat.state = 'NY'
            """,
            [
                {
                    "column": "repair_order_id",
                    "name": "default_DOT_repair_orders_DOT_repair_order_id",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.repair_order_id",
                    "semantic_type": None,
                    "type": "int",
                },
                {
                    "column": "municipality_id",
                    "name": "default_DOT_repair_orders_DOT_municipality_id",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.municipality_id",
                    "semantic_type": None,
                    "type": "string",
                },
                {
                    "column": "hard_hat_id",
                    "name": "default_DOT_repair_orders_DOT_hard_hat_id",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.hard_hat_id",
                    "semantic_type": None,
                    "type": "int",
                },
                {
                    "column": "order_date",
                    "name": "default_DOT_repair_orders_DOT_order_date",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.order_date",
                    "semantic_type": None,
                    "type": "timestamp",
                },
                {
                    "column": "required_date",
                    "name": "default_DOT_repair_orders_DOT_required_date",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.required_date",
                    "semantic_type": None,
                    "type": "timestamp",
                },
                {
                    "column": "dispatched_date",
                    "name": "default_DOT_repair_orders_DOT_dispatched_date",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.dispatched_date",
                    "semantic_type": None,
                    "type": "timestamp",
                },
                {
                    "column": "dispatcher_id",
                    "name": "default_DOT_repair_orders_DOT_dispatcher_id",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.dispatcher_id",
                    "semantic_type": None,
                    "type": "int",
                },
                {
                    "column": "state",
                    "name": "default_DOT_hard_hat_DOT_state",
                    "node": "default.hard_hat",
                    "semantic_entity": "default.hard_hat.state",
                    "semantic_type": None,
                    "type": "string",
                },
            ],
            [
                [
                    10021,
                    "Philadelphia",
                    7,
                    "2007-05-10",
                    "2009-08-27",
                    "2007-12-01",
                    3,
                    "NY",
                ],
            ],
        ),
        # querying source node with filters directly on the node
        (
            ["ROADS"],
            "default.repair_orders",
            [],
            ["default.repair_orders.order_date='2009-08-14'"],
            """
              SELECT  default_DOT_repair_orders.repair_order_id default_DOT_repair_orders_DOT_repair_order_id,
                default_DOT_repair_orders.municipality_id default_DOT_repair_orders_DOT_municipality_id,
                default_DOT_repair_orders.hard_hat_id default_DOT_repair_orders_DOT_hard_hat_id,
                default_DOT_repair_orders.order_date default_DOT_repair_orders_DOT_order_date,
                default_DOT_repair_orders.required_date default_DOT_repair_orders_DOT_required_date,
                default_DOT_repair_orders.dispatched_date default_DOT_repair_orders_DOT_dispatched_date,
                default_DOT_repair_orders.dispatcher_id default_DOT_repair_orders_DOT_dispatcher_id
              FROM roads.repair_orders AS default_DOT_repair_orders
              WHERE  default_DOT_repair_orders.order_date = '2009-08-14'
            """,
            [
                {
                    "column": "repair_order_id",
                    "name": "default_DOT_repair_orders_DOT_repair_order_id",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.repair_order_id",
                    "semantic_type": None,
                    "type": "int",
                },
                {
                    "column": "municipality_id",
                    "name": "default_DOT_repair_orders_DOT_municipality_id",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.municipality_id",
                    "semantic_type": None,
                    "type": "string",
                },
                {
                    "column": "hard_hat_id",
                    "name": "default_DOT_repair_orders_DOT_hard_hat_id",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.hard_hat_id",
                    "semantic_type": None,
                    "type": "int",
                },
                {
                    "column": "order_date",
                    "name": "default_DOT_repair_orders_DOT_order_date",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.order_date",
                    "semantic_type": None,
                    "type": "timestamp",
                },
                {
                    "column": "required_date",
                    "name": "default_DOT_repair_orders_DOT_required_date",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.required_date",
                    "semantic_type": None,
                    "type": "timestamp",
                },
                {
                    "column": "dispatched_date",
                    "name": "default_DOT_repair_orders_DOT_dispatched_date",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.dispatched_date",
                    "semantic_type": None,
                    "type": "timestamp",
                },
                {
                    "column": "dispatcher_id",
                    "name": "default_DOT_repair_orders_DOT_dispatcher_id",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.dispatcher_id",
                    "semantic_type": None,
                    "type": "int",
                },
            ],
            [],
        ),
        # querying transform node with filters on joinable dimension
        (
            ["EVENT"],
            "default.long_events",
            [],
            ["default.country_dim.events_cnt >= 20"],
            """
            SELECT  default_DOT_long_events.event_id default_DOT_long_events_DOT_event_id,
    default_DOT_long_events.event_latency default_DOT_long_events_DOT_event_latency,
    default_DOT_long_events.device_id default_DOT_long_events_DOT_device_id,
    default_DOT_long_events.country default_DOT_long_events_DOT_country,
    default_DOT_country_dim.events_cnt default_DOT_country_dim_DOT_events_cnt
 FROM (SELECT  default_DOT_event_source.event_id,
    default_DOT_event_source.event_latency,
    default_DOT_event_source.device_id,
    default_DOT_event_source.country
 FROM logs.log_events AS default_DOT_event_source
 WHERE  default_DOT_event_source.event_latency > 1000000)
 AS default_DOT_long_events LEFT JOIN (SELECT  default_DOT_event_source.country,
    COUNT( DISTINCT default_DOT_event_source.event_id) AS events_cnt
 FROM logs.log_events AS default_DOT_event_source
 GROUP BY  default_DOT_event_source.country)
 AS default_DOT_country_dim ON default_DOT_long_events.country = default_DOT_country_dim.country
 WHERE  default_DOT_country_dim.events_cnt >= 20
            """,
            [
                {
                    "column": "event_id",
                    "name": "default_DOT_long_events_DOT_event_id",
                    "node": "default.long_events",
                    "semantic_entity": "default.long_events.event_id",
                    "semantic_type": None,
                    "type": "int",
                },
                {
                    "column": "event_latency",
                    "name": "default_DOT_long_events_DOT_event_latency",
                    "node": "default.long_events",
                    "semantic_entity": "default.long_events.event_latency",
                    "semantic_type": None,
                    "type": "int",
                },
                {
                    "column": "device_id",
                    "name": "default_DOT_long_events_DOT_device_id",
                    "node": "default.long_events",
                    "semantic_entity": "default.long_events.device_id",
                    "semantic_type": None,
                    "type": "int",
                },
                {
                    "column": "country",
                    "name": "default_DOT_long_events_DOT_country",
                    "node": "default.long_events",
                    "semantic_entity": "default.long_events.country",
                    "semantic_type": None,
                    "type": "string",
                },
                {
                    "column": "events_cnt",
                    "name": "default_DOT_country_dim_DOT_events_cnt",
                    "node": "default.country_dim",
                    "semantic_entity": "default.country_dim.events_cnt",
                    "semantic_type": None,
                    "type": "bigint",
                },
            ],
            [],
        ),
        # querying transform node with filters on dimension PK column
        # * it should not join in the dimension but use the FK column on transform instead
        # * it should push down the filter to the parent transform
        (
            ["EVENT"],
            "default.long_events",
            [],
            [
                "default.country_dim.country = 'ABCD'",
            ],  # country is PK of default.country_dim
            """
                SELECT  default_DOT_long_events.event_id default_DOT_long_events_DOT_event_id,
                    default_DOT_long_events.event_latency default_DOT_long_events_DOT_event_latency,
                    default_DOT_long_events.device_id default_DOT_long_events_DOT_device_id,
                    default_DOT_long_events.country default_DOT_country_dim_DOT_country
                FROM (SELECT  default_DOT_event_source.event_id,
                    default_DOT_event_source.event_latency,
                    default_DOT_event_source.device_id,
                    default_DOT_event_source.country
                FROM logs.log_events AS default_DOT_event_source
                WHERE  default_DOT_event_source.country = 'ABCD' AND default_DOT_event_source.event_latency > 1000000)
                AS default_DOT_long_events
                WHERE  default_DOT_long_events.country = 'ABCD' AND default_DOT_long_events.country = 'ABCD'
                """,
            [
                {
                    "column": "event_id",
                    "name": "default_DOT_long_events_DOT_event_id",
                    "node": "default.long_events",
                    "type": "int",
                    "semantic_type": None,
                    "semantic_entity": "default.long_events.event_id",
                },
                {
                    "column": "event_latency",
                    "name": "default_DOT_long_events_DOT_event_latency",
                    "node": "default.long_events",
                    "type": "int",
                    "semantic_type": None,
                    "semantic_entity": "default.long_events.event_latency",
                },
                {
                    "column": "device_id",
                    "name": "default_DOT_long_events_DOT_device_id",
                    "node": "default.long_events",
                    "type": "int",
                    "semantic_type": None,
                    "semantic_entity": "default.long_events.device_id",
                },
                {
                    "column": "country",
                    "name": "default_DOT_country_dim_DOT_country",
                    "node": "default.country_dim",
                    "type": "string",
                    "semantic_type": None,
                    "semantic_entity": "default.country_dim.country",
                },
            ],
            [],
        ),
        # querying transform node with filters directly on the node
        (
            ["EVENT"],
            "default.long_events",
            [],
            ["default.long_events.device_id = 'Android'"],
            """
            SELECT  default_DOT_long_events.event_id default_DOT_long_events_DOT_event_id,
                default_DOT_long_events.event_latency default_DOT_long_events_DOT_event_latency,
                default_DOT_long_events.device_id default_DOT_long_events_DOT_device_id,
                default_DOT_long_events.country default_DOT_long_events_DOT_country
             FROM (SELECT  default_DOT_event_source.event_id,
                default_DOT_event_source.event_latency,
                default_DOT_event_source.device_id,
                default_DOT_event_source.country
             FROM logs.log_events AS default_DOT_event_source
             WHERE  default_DOT_event_source.event_latency > 1000000)
             AS default_DOT_long_events
             WHERE  default_DOT_long_events.device_id = 'Android'
            """,
            [
                {
                    "column": "event_id",
                    "name": "default_DOT_long_events_DOT_event_id",
                    "node": "default.long_events",
                    "type": "int",
                    "semantic_type": None,
                    "semantic_entity": "default.long_events.event_id",
                },
                {
                    "column": "event_latency",
                    "name": "default_DOT_long_events_DOT_event_latency",
                    "node": "default.long_events",
                    "type": "int",
                    "semantic_type": None,
                    "semantic_entity": "default.long_events.event_latency",
                },
                {
                    "column": "device_id",
                    "name": "default_DOT_long_events_DOT_device_id",
                    "node": "default.long_events",
                    "type": "int",
                    "semantic_type": None,
                    "semantic_entity": "default.long_events.device_id",
                },
                {
                    "column": "country",
                    "name": "default_DOT_long_events_DOT_country",
                    "node": "default.long_events",
                    "type": "string",
                    "semantic_type": None,
                    "semantic_entity": "default.long_events.country",
                },
            ],
            [],
        ),
        (
            ["ROADS"],
            "default.municipality",
            [],
            ["default.municipality.state_id = 'CA'"],
            """
            SELECT
              default_DOT_municipality.municipality_id default_DOT_municipality_DOT_municipality_id,
              default_DOT_municipality.contact_name default_DOT_municipality_DOT_contact_name,
              default_DOT_municipality.contact_title default_DOT_municipality_DOT_contact_title,
              default_DOT_municipality.local_region default_DOT_municipality_DOT_local_region,
              default_DOT_municipality.phone default_DOT_municipality_DOT_phone,
              default_DOT_municipality.state_id default_DOT_municipality_DOT_state_id
            FROM roads.municipality AS default_DOT_municipality
            WHERE  default_DOT_municipality.state_id = 'CA'
            """,
            [
                {
                    "column": "municipality_id",
                    "name": "default_DOT_municipality_DOT_municipality_id",
                    "node": "default.municipality",
                    "semantic_entity": "default.municipality.municipality_id",
                    "semantic_type": None,
                    "type": "string",
                },
                {
                    "column": "contact_name",
                    "name": "default_DOT_municipality_DOT_contact_name",
                    "node": "default.municipality",
                    "semantic_entity": "default.municipality.contact_name",
                    "semantic_type": None,
                    "type": "string",
                },
                {
                    "column": "contact_title",
                    "name": "default_DOT_municipality_DOT_contact_title",
                    "node": "default.municipality",
                    "semantic_entity": "default.municipality.contact_title",
                    "semantic_type": None,
                    "type": "string",
                },
                {
                    "column": "local_region",
                    "name": "default_DOT_municipality_DOT_local_region",
                    "node": "default.municipality",
                    "semantic_entity": "default.municipality.local_region",
                    "semantic_type": None,
                    "type": "string",
                },
                {
                    "column": "phone",
                    "name": "default_DOT_municipality_DOT_phone",
                    "node": "default.municipality",
                    "semantic_entity": "default.municipality.phone",
                    "semantic_type": None,
                    "type": "string",
                },
                {
                    "column": "state_id",
                    "name": "default_DOT_municipality_DOT_state_id",
                    "node": "default.municipality",
                    "semantic_entity": "default.municipality.state_id",
                    "semantic_type": None,
                    "type": "int",
                },
            ],
            [],
        ),
        (
            ["ROADS"],
            "default.num_repair_orders",
            [],
            [],
            """SELECT  count(default_DOT_repair_orders_fact.repair_order_id) default_DOT_num_repair_orders
 FROM (SELECT  repair_orders.repair_order_id,
    repair_orders.municipality_id,
    repair_orders.hard_hat_id,
    repair_orders.dispatcher_id,
    repair_orders.order_date,
    repair_orders.dispatched_date,
    repair_orders.required_date,
    repair_order_details.discount,
    repair_order_details.price,
    repair_order_details.quantity,
    repair_order_details.repair_type_id,
    repair_order_details.price * repair_order_details.quantity AS total_repair_cost,
    repair_orders.dispatched_date - repair_orders.order_date AS time_to_dispatch,
    repair_orders.dispatched_date - repair_orders.required_date AS dispatch_delay
 FROM roads.repair_orders AS repair_orders JOIN roads.repair_order_details AS repair_order_details ON repair_orders.repair_order_id = repair_order_details.repair_order_id)
 AS default_DOT_repair_orders_fact""",
            [
                {
                    "column": "default_DOT_num_repair_orders",
                    "name": "default_DOT_num_repair_orders",
                    "node": "default.num_repair_orders",
                    "type": "bigint",
                    "semantic_type": None,
                    "semantic_entity": "default.num_repair_orders.default_DOT_num_repair_orders",
                },
            ],
            [[25]],
        ),
        (
            ["ROADS"],
            "default.num_repair_orders",
            ["default.hard_hat.state"],
            [
                "default.repair_orders_fact.dispatcher_id=1",
                "default.hard_hat.state='AZ'",
            ],
            """
            SELECT  count(default_DOT_repair_orders_fact.repair_order_id) default_DOT_num_repair_orders,
                default_DOT_hard_hat.state default_DOT_hard_hat_DOT_state
             FROM (SELECT  repair_orders.repair_order_id,
                repair_orders.municipality_id,
                repair_orders.hard_hat_id,
                repair_orders.dispatcher_id,
                repair_orders.order_date,
                repair_orders.dispatched_date,
                repair_orders.required_date,
                repair_order_details.discount,
                repair_order_details.price,
                repair_order_details.quantity,
                repair_order_details.repair_type_id,
                repair_order_details.price * repair_order_details.quantity AS total_repair_cost,
                repair_orders.dispatched_date - repair_orders.order_date AS time_to_dispatch,
                repair_orders.dispatched_date - repair_orders.required_date AS dispatch_delay
             FROM roads.repair_orders AS repair_orders JOIN roads.repair_order_details AS repair_order_details ON repair_orders.repair_order_id = repair_order_details.repair_order_id)
             AS default_DOT_repair_orders_fact LEFT JOIN (SELECT  default_DOT_hard_hats.hard_hat_id,
                default_DOT_hard_hats.state
             FROM roads.hard_hats AS default_DOT_hard_hats)
             AS default_DOT_hard_hat ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
             WHERE  default_DOT_repair_orders_fact.dispatcher_id = 1 AND default_DOT_hard_hat.state = 'AZ'
             GROUP BY  default_DOT_hard_hat.state
            """,
            [
                {
                    "column": "default_DOT_num_repair_orders",
                    "name": "default_DOT_num_repair_orders",
                    "node": "default.num_repair_orders",
                    "type": "bigint",
                    "semantic_type": None,
                    "semantic_entity": "default.num_repair_orders.default_DOT_num_repair_orders",
                },
                {
                    "column": "state",
                    "name": "default_DOT_hard_hat_DOT_state",
                    "node": "default.hard_hat",
                    "type": "string",
                    "semantic_type": None,
                    "semantic_entity": "default.hard_hat.state",
                },
            ],
            [],
        ),
        (
            ["ROADS"],
            "default.num_repair_orders",
            [
                "default.hard_hat.city",
                "default.hard_hat.last_name",
                "default.dispatcher.company_name",
                "default.municipality_dim.local_region",
            ],
            [
                "default.dispatcher.dispatcher_id=1",
                "default.hard_hat.state != 'AZ'",
                "default.dispatcher.phone = '4082021022'",
                "default.repair_orders_fact.order_date >= '2020-01-01'",
            ],
            """
            SELECT  count(default_DOT_repair_orders_fact.repair_order_id) default_DOT_num_repair_orders,
                default_DOT_hard_hat.city default_DOT_hard_hat_DOT_city,
                default_DOT_hard_hat.last_name default_DOT_hard_hat_DOT_last_name,
                default_DOT_dispatcher.company_name default_DOT_dispatcher_DOT_company_name,
                default_DOT_municipality_dim.local_region default_DOT_municipality_dim_DOT_local_region
             FROM (SELECT  repair_orders.repair_order_id,
                repair_orders.municipality_id,
                repair_orders.hard_hat_id,
                repair_orders.dispatcher_id,
                repair_orders.order_date,
                repair_orders.dispatched_date,
                repair_orders.required_date,
                repair_order_details.discount,
                repair_order_details.price,
                repair_order_details.quantity,
                repair_order_details.repair_type_id,
                repair_order_details.price * repair_order_details.quantity AS total_repair_cost,
                repair_orders.dispatched_date - repair_orders.order_date AS time_to_dispatch,
                repair_orders.dispatched_date - repair_orders.required_date AS dispatch_delay
             FROM roads.repair_orders AS repair_orders JOIN roads.repair_order_details AS repair_order_details ON repair_orders.repair_order_id = repair_order_details.repair_order_id
             WHERE  repair_orders.dispatcher_id = 1)
             AS default_DOT_repair_orders_fact LEFT JOIN (SELECT  default_DOT_dispatchers.dispatcher_id,
                default_DOT_dispatchers.company_name,
                default_DOT_dispatchers.phone
             FROM roads.dispatchers AS default_DOT_dispatchers)
             AS default_DOT_dispatcher ON default_DOT_repair_orders_fact.dispatcher_id = default_DOT_dispatcher.dispatcher_id
            LEFT JOIN (SELECT  default_DOT_hard_hats.hard_hat_id,
                default_DOT_hard_hats.last_name,
                default_DOT_hard_hats.city,
                default_DOT_hard_hats.state
             FROM roads.hard_hats AS default_DOT_hard_hats)
             AS default_DOT_hard_hat ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
            LEFT JOIN (SELECT  m.municipality_id AS municipality_id,
                m.local_region
             FROM roads.municipality AS m LEFT JOIN roads.municipality_municipality_type AS mmt ON m.municipality_id = mmt.municipality_id
            LEFT JOIN roads.municipality_type AS mt ON mmt.municipality_type_id = mt.municipality_type_desc)
             AS default_DOT_municipality_dim ON default_DOT_repair_orders_fact.municipality_id = default_DOT_municipality_dim.municipality_id
             WHERE  default_DOT_repair_orders_fact.dispatcher_id = 1 AND default_DOT_repair_orders_fact.dispatcher_id = 1 AND default_DOT_hard_hat.state != 'AZ' AND default_DOT_dispatcher.phone = '4082021022' AND default_DOT_repair_orders_fact.order_date >= '2020-01-01'
             GROUP BY  default_DOT_hard_hat.city, default_DOT_hard_hat.last_name, default_DOT_dispatcher.company_name, default_DOT_municipality_dim.local_region
            """,
            [
                {
                    "name": "default_DOT_num_repair_orders",
                    "column": "default_DOT_num_repair_orders",
                    "node": "default.num_repair_orders",
                    "type": "bigint",
                    "semantic_type": None,
                    "semantic_entity": "default.num_repair_orders.default_DOT_num_repair_orders",
                },
                {
                    "name": "default_DOT_hard_hat_DOT_city",
                    "column": "city",
                    "node": "default.hard_hat",
                    "type": "string",
                    "semantic_type": None,
                    "semantic_entity": "default.hard_hat.city",
                },
                {
                    "name": "default_DOT_hard_hat_DOT_last_name",
                    "column": "last_name",
                    "node": "default.hard_hat",
                    "type": "string",
                    "semantic_type": None,
                    "semantic_entity": "default.hard_hat.last_name",
                },
                {
                    "name": "default_DOT_dispatcher_DOT_company_name",
                    "column": "company_name",
                    "node": "default.dispatcher",
                    "type": "string",
                    "semantic_type": None,
                    "semantic_entity": "default.dispatcher.company_name",
                },
                {
                    "name": "default_DOT_municipality_dim_DOT_local_region",
                    "column": "local_region",
                    "node": "default.municipality_dim",
                    "type": "string",
                    "semantic_type": None,
                    "semantic_entity": "default.municipality_dim.local_region",
                },
            ],
            [],
        ),
        # metric with second-order dimension
        (
            ["ROADS"],
            "default.avg_repair_price",
            ["default.hard_hat.city"],
            [],
            """
            SELECT  avg(default_DOT_repair_orders_fact.price) default_DOT_avg_repair_price,
                default_DOT_hard_hat.city default_DOT_hard_hat_DOT_city
            FROM (SELECT  repair_orders.repair_order_id,
                repair_orders.municipality_id,
                repair_orders.hard_hat_id,
                repair_orders.dispatcher_id,
                repair_orders.order_date,
                repair_orders.dispatched_date,
                repair_orders.required_date,
                repair_order_details.discount,
                repair_order_details.price,
                repair_order_details.quantity,
                repair_order_details.repair_type_id,
                repair_order_details.price * repair_order_details.quantity AS total_repair_cost,
                repair_orders.dispatched_date - repair_orders.order_date AS time_to_dispatch,
                repair_orders.dispatched_date - repair_orders.required_date AS dispatch_delay
             FROM roads.repair_orders AS repair_orders JOIN roads.repair_order_details AS repair_order_details ON repair_orders.repair_order_id = repair_order_details.repair_order_id)
             AS default_DOT_repair_orders_fact LEFT JOIN (SELECT  default_DOT_hard_hats.hard_hat_id,
                default_DOT_hard_hats.city,
                default_DOT_hard_hats.state
             FROM roads.hard_hats AS default_DOT_hard_hats)
             AS default_DOT_hard_hat ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
             GROUP BY  default_DOT_hard_hat.city
            """,
            [
                {
                    "column": "default_DOT_avg_repair_price",
                    "name": "default_DOT_avg_repair_price",
                    "node": "default.avg_repair_price",
                    "type": "double",
                    "semantic_type": None,
                    "semantic_entity": "default.avg_repair_price.default_DOT_avg_repair_price",
                },
                {
                    "column": "city",
                    "name": "default_DOT_hard_hat_DOT_city",
                    "node": "default.hard_hat",
                    "type": "string",
                    "semantic_type": None,
                    "semantic_entity": "default.hard_hat.city",
                },
            ],
            [
                [54672.75, "Jersey City"],
                [76555.33333333333, "Billerica"],
                [64190.6, "Southgate"],
                [65682.0, "Phoenix"],
                [54083.5, "Southampton"],
                [65595.66666666667, "Powder Springs"],
                [39301.5, "Middletown"],
                [70418.0, "Muskogee"],
                [53374.0, "Niagara Falls"],
            ],
        ),
        # metric with multiple nth order dimensions that can share some of the joins
        (
            ["ROADS"],
            "default.avg_repair_price",
            ["default.hard_hat.city", "default.dispatcher.company_name"],
            [],
            """
              SELECT  avg(default_DOT_repair_orders_fact.price) default_DOT_avg_repair_price,
                default_DOT_hard_hat.city default_DOT_hard_hat_DOT_city,
                default_DOT_dispatcher.company_name default_DOT_dispatcher_DOT_company_name
              FROM (SELECT  repair_orders.repair_order_id,
                repair_orders.municipality_id,
                repair_orders.hard_hat_id,
                repair_orders.dispatcher_id,
                repair_orders.order_date,
                repair_orders.dispatched_date,
                repair_orders.required_date,
                repair_order_details.discount,
                repair_order_details.price,
                repair_order_details.quantity,
                repair_order_details.repair_type_id,
                repair_order_details.price * repair_order_details.quantity AS total_repair_cost,
                repair_orders.dispatched_date - repair_orders.order_date AS time_to_dispatch,
                repair_orders.dispatched_date - repair_orders.required_date AS dispatch_delay
             FROM roads.repair_orders AS repair_orders JOIN roads.repair_order_details AS repair_order_details ON repair_orders.repair_order_id = repair_order_details.repair_order_id)
             AS default_DOT_repair_orders_fact LEFT JOIN (SELECT  default_DOT_dispatchers.dispatcher_id,
                default_DOT_dispatchers.company_name
             FROM roads.dispatchers AS default_DOT_dispatchers)
             AS default_DOT_dispatcher ON default_DOT_repair_orders_fact.dispatcher_id = default_DOT_dispatcher.dispatcher_id
            LEFT JOIN (SELECT  default_DOT_hard_hats.hard_hat_id,
                default_DOT_hard_hats.city,
                default_DOT_hard_hats.state
             FROM roads.hard_hats AS default_DOT_hard_hats)
             AS default_DOT_hard_hat ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
             GROUP BY  default_DOT_hard_hat.city, default_DOT_dispatcher.company_name
            """,
            [
                {
                    "column": "default_DOT_avg_repair_price",
                    "name": "default_DOT_avg_repair_price",
                    "node": "default.avg_repair_price",
                    "type": "double",
                    "semantic_type": None,
                    "semantic_entity": "default.avg_repair_price.default_DOT_avg_repair_price",
                },
                {
                    "column": "city",
                    "name": "default_DOT_hard_hat_DOT_city",
                    "node": "default.hard_hat",
                    "type": "string",
                    "semantic_type": None,
                    "semantic_entity": "default.hard_hat.city",
                },
                {
                    "column": "company_name",
                    "name": "default_DOT_dispatcher_DOT_company_name",
                    "node": "default.dispatcher",
                    "type": "string",
                    "semantic_type": None,
                    "semantic_entity": "default.dispatcher.company_name",
                },
            ],
            [
                [63708.0, "Jersey City", "Federal Roads Group"],
                [67253.0, "Billerica", "Pothole Pete"],
                [57332.5, "Southgate", "Asphalts R Us"],
                [51661.0, "Jersey City", "Pothole Pete"],
                [76463.0, "Phoenix", "Asphalts R Us"],
                [81206.5, "Billerica", "Asphalts R Us"],
                [63918.0, "Southampton", "Asphalts R Us"],
                [59499.5, "Southgate", "Federal Roads Group"],
                [27222.0, "Southampton", "Federal Roads Group"],
                [62597.0, "Southampton", "Pothole Pete"],
                [54901.0, "Phoenix", "Federal Roads Group"],
                [66929.5, "Powder Springs", "Asphalts R Us"],
                [39301.5, "Middletown", "Federal Roads Group"],
                [70418.0, "Muskogee", "Federal Roads Group"],
                [62928.0, "Powder Springs", "Pothole Pete"],
                [53374.0, "Niagara Falls", "Federal Roads Group"],
                [87289.0, "Southgate", "Pothole Pete"],
            ],
        ),
        # dimension with aliased join key should just use the alias directly
        (
            ["ROADS"],
            "default.num_repair_orders",
            ["default.us_state.state_short"],
            [],
            """
            SELECT  count(default_DOT_repair_orders_fact.repair_order_id) default_DOT_num_repair_orders,
                default_DOT_us_state.state_short default_DOT_us_state_DOT_state_short
            FROM (SELECT  repair_orders.repair_order_id,
                repair_orders.municipality_id,
                repair_orders.hard_hat_id,
                repair_orders.dispatcher_id,
                repair_orders.order_date,
                repair_orders.dispatched_date,
                repair_orders.required_date,
                repair_order_details.discount,
                repair_order_details.price,
                repair_order_details.quantity,
                repair_order_details.repair_type_id,
                repair_order_details.price * repair_order_details.quantity AS total_repair_cost,
                repair_orders.dispatched_date - repair_orders.order_date AS time_to_dispatch,
                repair_orders.dispatched_date - repair_orders.required_date AS dispatch_delay
             FROM roads.repair_orders AS repair_orders JOIN roads.repair_order_details AS repair_order_details ON repair_orders.repair_order_id = repair_order_details.repair_order_id)
             AS default_DOT_repair_orders_fact LEFT JOIN (SELECT  default_DOT_hard_hats.hard_hat_id,
                default_DOT_hard_hats.state
             FROM roads.hard_hats AS default_DOT_hard_hats)
             AS default_DOT_hard_hat ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
             LEFT JOIN (SELECT  s.state_abbr AS state_short
              FROM roads.us_states AS s)
             AS default_DOT_us_state ON default_DOT_hard_hat.state = default_DOT_us_state.state_short
             GROUP BY  default_DOT_us_state.state_short
            """,
            [
                {
                    "column": "default_DOT_num_repair_orders",
                    "name": "default_DOT_num_repair_orders",
                    "node": "default.num_repair_orders",
                    "type": "bigint",
                    "semantic_type": None,
                    "semantic_entity": "default.num_repair_orders.default_DOT_num_repair_orders",
                },
                {
                    "column": "state_short",
                    "name": "default_DOT_us_state_DOT_state_short",
                    "node": "default.us_state",
                    "type": "string",
                    "semantic_type": None,
                    "semantic_entity": "default.us_state.state_short",
                },
            ],
            [
                [2, "AZ"],
                [2, "CT"],
                [3, "GA"],
                [3, "MA"],
                [5, "MI"],
                [4, "NJ"],
                [1, "NY"],
                [1, "OK"],
                [4, "PA"],
            ],
        ),
        # querying on source node while pulling in joinable dimension
        # (should not group by the dimension attribute)
        (
            ["ROADS"],
            "default.repair_orders",
            ["default.hard_hat.state"],
            ["default.hard_hat.state='NY'"],
            """
            SELECT  default_DOT_repair_orders.repair_order_id default_DOT_repair_orders_DOT_repair_order_id,
                default_DOT_repair_orders.municipality_id default_DOT_repair_orders_DOT_municipality_id,
                default_DOT_repair_orders.hard_hat_id default_DOT_repair_orders_DOT_hard_hat_id,
                default_DOT_repair_orders.order_date default_DOT_repair_orders_DOT_order_date,
                default_DOT_repair_orders.required_date default_DOT_repair_orders_DOT_required_date,
                default_DOT_repair_orders.dispatched_date default_DOT_repair_orders_DOT_dispatched_date,
                default_DOT_repair_orders.dispatcher_id default_DOT_repair_orders_DOT_dispatcher_id,
                default_DOT_hard_hat.state default_DOT_hard_hat_DOT_state
             FROM roads.repair_orders AS default_DOT_repair_orders LEFT JOIN (SELECT  default_DOT_repair_orders.repair_order_id,
                default_DOT_repair_orders.municipality_id,
                default_DOT_repair_orders.hard_hat_id,
                default_DOT_repair_orders.dispatcher_id
             FROM roads.repair_orders AS default_DOT_repair_orders)
             AS default_DOT_repair_order ON default_DOT_repair_orders.repair_order_id = default_DOT_repair_order.repair_order_id
            LEFT JOIN (SELECT  default_DOT_hard_hats.hard_hat_id,
                default_DOT_hard_hats.state
             FROM roads.hard_hats AS default_DOT_hard_hats)
             AS default_DOT_hard_hat ON default_DOT_repair_order.hard_hat_id = default_DOT_hard_hat.hard_hat_id
             WHERE  default_DOT_hard_hat.state = 'NY'
            """,
            [
                {
                    "column": "repair_order_id",
                    "name": "default_DOT_repair_orders_DOT_repair_order_id",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.repair_order_id",
                    "semantic_type": None,
                    "type": "int",
                },
                {
                    "column": "municipality_id",
                    "name": "default_DOT_repair_orders_DOT_municipality_id",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.municipality_id",
                    "semantic_type": None,
                    "type": "string",
                },
                {
                    "column": "hard_hat_id",
                    "name": "default_DOT_repair_orders_DOT_hard_hat_id",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.hard_hat_id",
                    "semantic_type": None,
                    "type": "int",
                },
                {
                    "column": "order_date",
                    "name": "default_DOT_repair_orders_DOT_order_date",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.order_date",
                    "semantic_type": None,
                    "type": "timestamp",
                },
                {
                    "column": "required_date",
                    "name": "default_DOT_repair_orders_DOT_required_date",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.required_date",
                    "semantic_type": None,
                    "type": "timestamp",
                },
                {
                    "column": "dispatched_date",
                    "name": "default_DOT_repair_orders_DOT_dispatched_date",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.dispatched_date",
                    "semantic_type": None,
                    "type": "timestamp",
                },
                {
                    "column": "dispatcher_id",
                    "name": "default_DOT_repair_orders_DOT_dispatcher_id",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.dispatcher_id",
                    "semantic_type": None,
                    "type": "int",
                },
                {
                    "column": "state",
                    "name": "default_DOT_hard_hat_DOT_state",
                    "node": "default.hard_hat",
                    "semantic_entity": "default.hard_hat.state",
                    "semantic_type": None,
                    "type": "string",
                },
            ],
            [
                [
                    10021,
                    "Philadelphia",
                    7,
                    "2007-05-10",
                    "2009-08-27",
                    "2007-12-01",
                    3,
                    "NY",
                ],
            ],
        ),
        # metric without any dimensions or filters should return single aggregate value
        (
            ["ROADS"],
            "default.num_repair_orders",
            [],
            [],
            """
                SELECT  count(default_DOT_repair_orders_fact.repair_order_id) default_DOT_num_repair_orders
                 FROM (SELECT  repair_orders.repair_order_id,
                    repair_orders.municipality_id,
                    repair_orders.hard_hat_id,
                    repair_orders.dispatcher_id,
                    repair_orders.order_date,
                    repair_orders.dispatched_date,
                    repair_orders.required_date,
                    repair_order_details.discount,
                    repair_order_details.price,
                    repair_order_details.quantity,
                    repair_order_details.repair_type_id,
                    repair_order_details.price * repair_order_details.quantity AS total_repair_cost,
                    repair_orders.dispatched_date - repair_orders.order_date AS time_to_dispatch,
                    repair_orders.dispatched_date - repair_orders.required_date AS dispatch_delay
                 FROM roads.repair_orders AS repair_orders JOIN roads.repair_order_details AS repair_order_details ON repair_orders.repair_order_id = repair_order_details.repair_order_id)
                 AS default_DOT_repair_orders_fact
                """,
            [
                {
                    "column": "default_DOT_num_repair_orders",
                    "name": "default_DOT_num_repair_orders",
                    "node": "default.num_repair_orders",
                    "type": "bigint",
                    "semantic_type": None,
                    "semantic_entity": "default.num_repair_orders.default_DOT_num_repair_orders",
                },
            ],
            [
                [25],
            ],
        ),
    ],
)
@pytest.mark.asyncio
async def test_sql_with_filters(  # pylint: disable=too-many-arguments
    groups: List[str],
    node_name,
    dimensions,
    filters,
    sql,
    columns,
    rows,
    client_with_query_service_example_loader: Callable[
        [Optional[List[str]]],
        AsyncClient,
    ],
):
    """
    Test ``GET /sql/{node_name}/`` with various filters and dimensions.
    """
    custom_client = await client_with_query_service_example_loader(groups)
    response = await custom_client.get(
        f"/sql/{node_name}/",
        params={"dimensions": dimensions, "filters": filters},
    )
    data = response.json()
    print("QUERYY", data["sql"])
    assert_query_strings_equal(data["sql"], sql)
    assert data["columns"] == columns

    # Run the query against local duckdb file if it's part of the roads model
    if "ROADS" in groups:
        response = await custom_client.get(
            f"/data/{node_name}/",
            params={"dimensions": dimensions, "filters": filters},
        )
        data = response.json()
        assert data["results"][0]["rows"] == rows


@pytest.mark.parametrize(
    "node_name, dimensions, filters, orderby, sql",
    [
        # querying on source node with filter on joinable dimension
        (
            "foo.bar.repair_orders",
            [],
            ["foo.bar.hard_hat.state='CA'"],
            [],
            """
            SELECT  foo_DOT_bar_DOT_repair_orders.repair_order_id foo_DOT_bar_DOT_repair_orders_DOT_repair_order_id,
                foo_DOT_bar_DOT_repair_orders.municipality_id foo_DOT_bar_DOT_repair_orders_DOT_municipality_id,
                foo_DOT_bar_DOT_repair_orders.hard_hat_id foo_DOT_bar_DOT_repair_orders_DOT_hard_hat_id,
                foo_DOT_bar_DOT_repair_orders.order_date foo_DOT_bar_DOT_repair_orders_DOT_order_date,
                foo_DOT_bar_DOT_repair_orders.required_date foo_DOT_bar_DOT_repair_orders_DOT_required_date,
                foo_DOT_bar_DOT_repair_orders.dispatched_date foo_DOT_bar_DOT_repair_orders_DOT_dispatched_date,
                foo_DOT_bar_DOT_repair_orders.dispatcher_id foo_DOT_bar_DOT_repair_orders_DOT_dispatcher_id,
                foo_DOT_bar_DOT_hard_hat.state foo_DOT_bar_DOT_hard_hat_DOT_state
             FROM roads.repair_orders AS foo_DOT_bar_DOT_repair_orders LEFT JOIN (SELECT  foo_DOT_bar_DOT_repair_orders.repair_order_id,
                foo_DOT_bar_DOT_repair_orders.municipality_id,
                foo_DOT_bar_DOT_repair_orders.hard_hat_id,
                foo_DOT_bar_DOT_repair_orders.dispatcher_id
             FROM roads.repair_orders AS foo_DOT_bar_DOT_repair_orders)
             AS foo_DOT_bar_DOT_repair_order ON foo_DOT_bar_DOT_repair_orders.repair_order_id = foo_DOT_bar_DOT_repair_order.repair_order_id
            LEFT JOIN (SELECT  foo_DOT_bar_DOT_hard_hats.hard_hat_id,
                foo_DOT_bar_DOT_hard_hats.state
             FROM roads.hard_hats AS foo_DOT_bar_DOT_hard_hats)
             AS foo_DOT_bar_DOT_hard_hat ON foo_DOT_bar_DOT_repair_order.hard_hat_id = foo_DOT_bar_DOT_hard_hat.hard_hat_id
             WHERE  foo_DOT_bar_DOT_hard_hat.state = 'CA'
            """,
        ),
        # querying source node with filters directly on the node
        (
            "foo.bar.repair_orders",
            [],
            ["foo.bar.repair_orders.order_date='2009-08-14'"],
            [],
            """
            SELECT
              foo_DOT_bar_DOT_repair_orders.repair_order_id foo_DOT_bar_DOT_repair_orders_DOT_repair_order_id,
              foo_DOT_bar_DOT_repair_orders.municipality_id foo_DOT_bar_DOT_repair_orders_DOT_municipality_id,
              foo_DOT_bar_DOT_repair_orders.hard_hat_id foo_DOT_bar_DOT_repair_orders_DOT_hard_hat_id,
              foo_DOT_bar_DOT_repair_orders.order_date foo_DOT_bar_DOT_repair_orders_DOT_order_date,
              foo_DOT_bar_DOT_repair_orders.required_date foo_DOT_bar_DOT_repair_orders_DOT_required_date,
              foo_DOT_bar_DOT_repair_orders.dispatched_date foo_DOT_bar_DOT_repair_orders_DOT_dispatched_date,
              foo_DOT_bar_DOT_repair_orders.dispatcher_id foo_DOT_bar_DOT_repair_orders_DOT_dispatcher_id
            FROM roads.repair_orders AS foo_DOT_bar_DOT_repair_orders
            WHERE
              foo_DOT_bar_DOT_repair_orders.order_date = '2009-08-14'
            """,
        ),
        (
            "foo.bar.num_repair_orders",
            [],
            [],
            [],
            """
            SELECT
              count(foo_DOT_bar_DOT_repair_orders.repair_order_id) AS foo_DOT_bar_DOT_num_repair_orders
            FROM roads.repair_orders AS foo_DOT_bar_DOT_repair_orders
            """,
        ),
        (
            "foo.bar.num_repair_orders",
            ["foo.bar.hard_hat.state"],
            ["foo.bar.repair_orders.dispatcher_id=1", "foo.bar.hard_hat.state='AZ'"],
            [],
            """
            SELECT  count(foo_DOT_bar_DOT_repair_orders.repair_order_id) AS foo_DOT_bar_DOT_num_repair_orders,
                foo_DOT_bar_DOT_hard_hat.state foo_DOT_bar_DOT_hard_hat_DOT_state
             FROM roads.repair_orders AS foo_DOT_bar_DOT_repair_orders LEFT JOIN (SELECT  foo_DOT_bar_DOT_repair_orders.repair_order_id,
                foo_DOT_bar_DOT_repair_orders.municipality_id,
                foo_DOT_bar_DOT_repair_orders.hard_hat_id,
                foo_DOT_bar_DOT_repair_orders.dispatcher_id
             FROM roads.repair_orders AS foo_DOT_bar_DOT_repair_orders)
             AS foo_DOT_bar_DOT_repair_order ON foo_DOT_bar_DOT_repair_orders.repair_order_id = foo_DOT_bar_DOT_repair_order.repair_order_id
            LEFT JOIN (SELECT  foo_DOT_bar_DOT_hard_hats.hard_hat_id,
                foo_DOT_bar_DOT_hard_hats.state
             FROM roads.hard_hats AS foo_DOT_bar_DOT_hard_hats)
             AS foo_DOT_bar_DOT_hard_hat ON foo_DOT_bar_DOT_repair_order.hard_hat_id = foo_DOT_bar_DOT_hard_hat.hard_hat_id
             WHERE  foo_DOT_bar_DOT_repair_orders.dispatcher_id = 1 AND foo_DOT_bar_DOT_hard_hat.state = 'AZ'
             GROUP BY  foo_DOT_bar_DOT_hard_hat.state
            """,
        ),
        (
            "foo.bar.num_repair_orders",
            [
                "foo.bar.hard_hat.city",
                "foo.bar.hard_hat.last_name",
                "foo.bar.dispatcher.company_name",
                "foo.bar.municipality_dim.local_region",
            ],
            [
                "foo.bar.repair_orders.dispatcher_id=1",
                "foo.bar.hard_hat.state != 'AZ'",
                "foo.bar.dispatcher.phone = '4082021022'",
                "foo.bar.repair_orders.order_date >= '2020-01-01'",
            ],
            ["foo.bar.hard_hat.last_name"],
            """
            SELECT  count(foo_DOT_bar_DOT_repair_orders.repair_order_id) AS foo_DOT_bar_DOT_num_repair_orders,
                foo_DOT_bar_DOT_hard_hat.city foo_DOT_bar_DOT_hard_hat_DOT_city,
                foo_DOT_bar_DOT_hard_hat.last_name foo_DOT_bar_DOT_hard_hat_DOT_last_name,
                foo_DOT_bar_DOT_dispatcher.company_name foo_DOT_bar_DOT_dispatcher_DOT_company_name,
                foo_DOT_bar_DOT_municipality_dim.local_region foo_DOT_bar_DOT_municipality_dim_DOT_local_region
             FROM roads.repair_orders AS foo_DOT_bar_DOT_repair_orders LEFT JOIN (SELECT  foo_DOT_bar_DOT_repair_orders.repair_order_id,
                foo_DOT_bar_DOT_repair_orders.municipality_id,
                foo_DOT_bar_DOT_repair_orders.hard_hat_id,
                foo_DOT_bar_DOT_repair_orders.dispatcher_id
             FROM roads.repair_orders AS foo_DOT_bar_DOT_repair_orders)
             AS foo_DOT_bar_DOT_repair_order ON foo_DOT_bar_DOT_repair_orders.repair_order_id = foo_DOT_bar_DOT_repair_order.repair_order_id
            LEFT JOIN (SELECT  foo_DOT_bar_DOT_dispatchers.dispatcher_id,
                foo_DOT_bar_DOT_dispatchers.company_name,
                foo_DOT_bar_DOT_dispatchers.phone
             FROM roads.dispatchers AS foo_DOT_bar_DOT_dispatchers)
             AS foo_DOT_bar_DOT_dispatcher ON foo_DOT_bar_DOT_repair_order.dispatcher_id = foo_DOT_bar_DOT_dispatcher.dispatcher_id
            LEFT JOIN (SELECT  foo_DOT_bar_DOT_hard_hats.hard_hat_id,
                foo_DOT_bar_DOT_hard_hats.last_name,
                foo_DOT_bar_DOT_hard_hats.city,
                foo_DOT_bar_DOT_hard_hats.state
             FROM roads.hard_hats AS foo_DOT_bar_DOT_hard_hats)
             AS foo_DOT_bar_DOT_hard_hat ON foo_DOT_bar_DOT_repair_order.hard_hat_id = foo_DOT_bar_DOT_hard_hat.hard_hat_id
            LEFT JOIN (SELECT  m.municipality_id AS municipality_id,
                m.local_region
             FROM roads.municipality AS m LEFT JOIN roads.municipality_municipality_type AS mmt ON m.municipality_id = mmt.municipality_id
            LEFT JOIN roads.municipality_type AS mt ON mmt.municipality_type_id = mt.municipality_type_desc)
             AS foo_DOT_bar_DOT_municipality_dim ON foo_DOT_bar_DOT_repair_order.municipality_id = foo_DOT_bar_DOT_municipality_dim.municipality_id
             WHERE  foo_DOT_bar_DOT_repair_orders.dispatcher_id = 1 AND foo_DOT_bar_DOT_hard_hat.state != 'AZ' AND foo_DOT_bar_DOT_dispatcher.phone = '4082021022' AND foo_DOT_bar_DOT_repair_orders.order_date >= '2020-01-01'
             GROUP BY  foo_DOT_bar_DOT_hard_hat.city, foo_DOT_bar_DOT_hard_hat.last_name, foo_DOT_bar_DOT_dispatcher.company_name, foo_DOT_bar_DOT_municipality_dim.local_region
            ORDER BY foo_DOT_bar_DOT_hard_hat.last_name
            """,
        ),
        (
            "foo.bar.avg_repair_price",
            ["foo.bar.hard_hat.city"],
            [],
            [],
            """
            SELECT
              avg(foo_DOT_bar_DOT_repair_order_details.price) foo_DOT_bar_DOT_avg_repair_price,
              foo_DOT_bar_DOT_hard_hat.city foo_DOT_bar_DOT_hard_hat_DOT_city
            FROM roads.repair_order_details AS foo_DOT_bar_DOT_repair_order_details
            LEFT JOIN (
              SELECT
                foo_DOT_bar_DOT_repair_orders.repair_order_id,
                foo_DOT_bar_DOT_repair_orders.municipality_id,
                foo_DOT_bar_DOT_repair_orders.hard_hat_id,
                foo_DOT_bar_DOT_repair_orders.dispatcher_id
              FROM roads.repair_orders AS foo_DOT_bar_DOT_repair_orders
            ) AS foo_DOT_bar_DOT_repair_order
            ON foo_DOT_bar_DOT_repair_order_details.repair_order_id
               = foo_DOT_bar_DOT_repair_order.repair_order_id
            LEFT JOIN (
              SELECT
                foo_DOT_bar_DOT_hard_hats.hard_hat_id,
                foo_DOT_bar_DOT_hard_hats.city
              FROM roads.hard_hats AS foo_DOT_bar_DOT_hard_hats
            ) AS foo_DOT_bar_DOT_hard_hat
            ON foo_DOT_bar_DOT_repair_order.hard_hat_id = foo_DOT_bar_DOT_hard_hat.hard_hat_id
            GROUP BY foo_DOT_bar_DOT_hard_hat.city
            """,
        ),
    ],
)
@pytest.mark.asyncio
async def test_sql_with_filters_on_namespaced_nodes(  # pylint: disable=R0913
    node_name,
    dimensions,
    filters,
    orderby,
    sql,
    client_with_namespaced_roads: AsyncClient,
):
    """
    Test ``GET /sql/{node_name}/`` with various filters and dimensions using a
    version of the DJ roads database with namespaces.
    """
    response = await client_with_namespaced_roads.get(
        f"/sql/{node_name}/",
        params={"dimensions": dimensions, "filters": filters, "orderby": orderby},
    )
    data = response.json()
    print("QUERYYY", data["sql"])
    assert str(parse(str(data["sql"]))) == str(parse(str(sql)))


@pytest.mark.asyncio
async def test_sql_with_filters_orderby_no_access(  # pylint: disable=R0913
    client_with_namespaced_roads: AsyncClient,
):
    """
    Test ``GET /sql/{node_name}/`` with various filters and dimensions using a
    version of the DJ roads database with namespaces.
    """

    def validate_access_override():
        def _validate_access(access_control: access.AccessControl):
            access_control.deny_all()

        return _validate_access

    app.dependency_overrides[validate_access] = validate_access_override

    node_name = "foo.bar.num_repair_orders"
    dimensions = [
        "foo.bar.hard_hat.city",
        "foo.bar.hard_hat.last_name",
        "foo.bar.dispatcher.company_name",
        "foo.bar.municipality_dim.local_region",
    ]
    filters = [
        "foo.bar.repair_orders.dispatcher_id=1",
        "foo.bar.hard_hat.state != 'AZ'",
        "foo.bar.dispatcher.phone = '4082021022'",
        "foo.bar.repair_orders.order_date >= '2020-01-01'",
    ]
    orderby = ["foo.bar.hard_hat.last_name"]
    response = await client_with_namespaced_roads.get(
        f"/sql/{node_name}/",
        params={"dimensions": dimensions, "filters": filters, "orderby": orderby},
    )
    data = response.json()
    assert sorted(list(data["message"])) == sorted(
        list(
            "Authorization of User `dj` for this request failed."
            "\nThe following requests were denied:\nread:node/foo.bar.dispatcher, "
            "read:node/foo.bar.repair_orders, read:node/foo.bar.municipality_dim, "
            "read:node/foo.bar.num_repair_orders, read:node/foo.bar.hard_hat.",
        ),
    )
    assert data["errors"][0]["code"] == 500
    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_cross_join_unnest(
    client_example_loader: Callable[[Optional[List[str]]], AsyncClient],
):
    """
    Verify cross join unnest on a joined in dimension works
    """
    custom_client = await client_example_loader(["LATERAL_VIEW"])
    await custom_client.post(
        "/nodes/basic.corrected_patches/columns/color_id/"
        "?dimension=basic.paint_colors_trino&dimension_column=color_id",
    )
    response = await custom_client.get(
        "/sql/basic.avg_luminosity_patches/",
        params={
            "filters": [],
            "dimensions": [
                "basic.paint_colors_trino.color_id",
                "basic.paint_colors_trino.color_name",
            ],
        },
    )
    expected = """
    SELECT
      paint_colors_trino.color_id basic_DOT_avg_luminosity_patches_DOT_color_id,
      basic_DOT_paint_colors_trino.color_name basic_DOT_avg_luminosity_patches_DOT_color_name,
      AVG(basic_DOT_corrected_patches.luminosity) AS basic_DOT_avg_luminosity_patches_DOT_basic_DOT_avg_luminosity_patches
    FROM (
      SELECT
        CAST(basic_DOT_patches.color_id AS VARCHAR) color_id,
        basic_DOT_patches.color_name,
        basic_DOT_patches.garishness,
        basic_DOT_patches.luminosity,
        basic_DOT_patches.opacity
      FROM basic.patches AS basic_DOT_patches
    ) AS basic_DOT_corrected_patches
    LEFT JOIN (
      SELECT
        t.color_name color_name,
        t.color_id
      FROM (
        SELECT
          basic_DOT_murals.id,
          basic_DOT_murals.colors
        FROM basic.murals AS basic_DOT_murals
      ) murals
      CROSS JOIN UNNEST(murals.colors) t( color_id, color_name)
    ) AS basic_DOT_paint_colors_trino
    ON basic_DOT_corrected_patches.color_id = basic_DOT_paint_colors_trino.color_id
    GROUP BY
      paint_colors_trino.color_id,
      basic_DOT_paint_colors_trino.color_name
    """
    query = response.json()["sql"]
    compare_query_strings(query, expected)


@pytest.mark.asyncio
async def test_lateral_view_explode(
    client_example_loader: Callable[[Optional[List[str]]], AsyncClient],
):
    """
    Verify lateral view explode on a joined in dimension works
    """
    custom_client = await client_example_loader(["LATERAL_VIEW"])
    await custom_client.post(
        "/nodes/basic.corrected_patches/columns/color_id/"
        "?dimension=basic.paint_colors_spark&dimension_column=color_id",
    )
    response = await custom_client.get(
        "/sql/basic.avg_luminosity_patches/",
        params={
            "filters": [],
            "dimensions": [
                "basic.paint_colors_spark.color_id",
                "basic.paint_colors_spark.color_name",
            ],
            "limit": 5,
        },
    )
    expected = """
    SELECT  AVG(basic_DOT_corrected_patches.luminosity) basic_DOT_avg_luminosity_patches,
        basic_DOT_paint_colors_spark.color_id basic_DOT_paint_colors_spark_DOT_color_id,
        basic_DOT_paint_colors_spark.color_name basic_DOT_paint_colors_spark_DOT_color_name
     FROM (SELECT  CAST(basic_DOT_patches.color_id AS STRING) color_id,
        basic_DOT_patches.color_name,
        basic_DOT_patches.opacity,
        basic_DOT_patches.luminosity,
        basic_DOT_patches.garishness
     FROM basic.patches AS basic_DOT_patches)
     AS basic_DOT_corrected_patches LEFT JOIN (SELECT  color_id,
        color_name
     FROM (SELECT  basic_DOT_murals.id,
        EXPLODE(basic_DOT_murals.colors) AS ( color_id, color_name)
     FROM basic.murals AS basic_DOT_murals

    ))
     AS basic_DOT_paint_colors_spark ON basic_DOT_corrected_patches.color_id = basic_DOT_paint_colors_spark.color_id
     GROUP BY  basic_DOT_paint_colors_spark.color_id, basic_DOT_paint_colors_spark.color_name

    LIMIT 5
    """
    query = response.json()["sql"]
    compare_query_strings(query, expected)


@pytest.mark.asyncio
async def test_get_sql_for_metrics_failures(client_with_account_revenue: AsyncClient):
    """
    Test failure modes when getting sql for multiple metrics.
    """
    # Getting sql for no metrics fails appropriately
    response = await client_with_account_revenue.get(
        "/sql/",
        params={
            "metrics": [],
            "dimensions": ["default.account_type.account_type_name"],
            "filters": [],
        },
    )
    assert response.status_code == 422
    data = response.json()
    assert data == {
        "message": "At least one metric is required",
        "errors": [],
        "warnings": [],
    }

    # Getting sql for metric with no dimensions works
    response = await client_with_account_revenue.get(
        "/sql/",
        params={
            "metrics": ["default.number_of_account_types"],
            "dimensions": [],
            "filters": [],
        },
    )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_get_sql_for_metrics_no_access(client_with_roads: AsyncClient):
    """
    Test getting sql for multiple metrics.
    """

    def validate_access_override():
        def _validate_access(access_control: access.AccessControl):
            if access_control.state == "direct":
                access_control.approve_all()
            else:
                access_control.deny_all()

        return _validate_access

    app.dependency_overrides[validate_access] = validate_access_override

    response = await client_with_roads.get(
        "/sql/",
        params={
            "metrics": ["default.discounted_orders_rate", "default.num_repair_orders"],
            "dimensions": [
                "default.hard_hat.country",
                "default.hard_hat.postal_code",
                "default.hard_hat.city",
                "default.hard_hat.state",
                "default.dispatcher.company_name",
                "default.municipality_dim.local_region",
            ],
            "filters": ["default.hard_hat.city = 'Las Vegas'"],
            "orderby": [],
            "limit": 100,
        },
    )
    data = response.json()
    # assert "Authorization of User `dj` for this request failed.\n" in data["message"]
    assert "The following requests were denied:\n" in data["message"]
    assert "read:node/default.municipality_dim" in data["message"]
    assert "read:node/default.dispatcher" in data["message"]
    assert "read:node/default.repair_orders_fact" in data["message"]
    assert "read:node/default.hard_hat" in data["message"]
    assert data["errors"][0]["code"] == 500
    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_get_sql_for_metrics(client_with_roads: AsyncClient):
    """
    Test getting sql for multiple metrics.
    """

    response = await client_with_roads.get(
        "/sql/",
        params={
            "metrics": ["default.discounted_orders_rate", "default.num_repair_orders"],
            "dimensions": [
                "default.hard_hat.country",
                "default.hard_hat.postal_code",
                "default.hard_hat.city",
                "default.hard_hat.state",
                "default.dispatcher.company_name",
                "default.municipality_dim.local_region",
            ],
            "filters": [],
            "orderby": [
                "default.hard_hat.country",
                "default.num_repair_orders",
                "default.dispatcher.company_name",
                "default.discounted_orders_rate",
            ],
            "limit": 100,
        },
    )
    data = response.json()
    expected_sql = """
    WITH
    default_DOT_repair_orders_fact AS (SELECT  default_DOT_hard_hat.country default_DOT_hard_hat_DOT_country,
        default_DOT_hard_hat.postal_code default_DOT_hard_hat_DOT_postal_code,
        default_DOT_hard_hat.city default_DOT_hard_hat_DOT_city,
        default_DOT_hard_hat.state default_DOT_hard_hat_DOT_state,
        default_DOT_dispatcher.company_name default_DOT_dispatcher_DOT_company_name,
        default_DOT_municipality_dim.local_region default_DOT_municipality_dim_DOT_local_region,
        CAST(sum(if(default_DOT_repair_orders_fact.discount > 0.0, 1, 0)) AS DOUBLE) / count(*) AS default_DOT_discounted_orders_rate,
        count(default_DOT_repair_orders_fact.repair_order_id) default_DOT_num_repair_orders
     FROM (SELECT  repair_orders.repair_order_id,
        repair_orders.municipality_id,
        repair_orders.hard_hat_id,
        repair_orders.dispatcher_id,
        repair_orders.order_date,
        repair_orders.dispatched_date,
        repair_orders.required_date,
        repair_order_details.discount,
        repair_order_details.price,
        repair_order_details.quantity,
        repair_order_details.repair_type_id,
        repair_order_details.price * repair_order_details.quantity AS total_repair_cost,
        repair_orders.dispatched_date - repair_orders.order_date AS time_to_dispatch,
        repair_orders.dispatched_date - repair_orders.required_date AS dispatch_delay
     FROM roads.repair_orders AS repair_orders JOIN roads.repair_order_details AS repair_order_details ON repair_orders.repair_order_id = repair_order_details.repair_order_id)
     AS default_DOT_repair_orders_fact LEFT JOIN (SELECT  default_DOT_dispatchers.dispatcher_id,
        default_DOT_dispatchers.company_name
     FROM roads.dispatchers AS default_DOT_dispatchers)
     AS default_DOT_dispatcher ON default_DOT_repair_orders_fact.dispatcher_id = default_DOT_dispatcher.dispatcher_id
     LEFT JOIN (SELECT  default_DOT_hard_hats.hard_hat_id,
        default_DOT_hard_hats.city,
        default_DOT_hard_hats.state,
        default_DOT_hard_hats.postal_code,
        default_DOT_hard_hats.country
     FROM roads.hard_hats AS default_DOT_hard_hats)
     AS default_DOT_hard_hat ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
    LEFT JOIN (SELECT  m.municipality_id AS municipality_id,
        m.local_region
    FROM roads.municipality AS m LEFT JOIN roads.municipality_municipality_type AS mmt ON m.municipality_id = mmt.municipality_id
    LEFT JOIN roads.municipality_type AS mt ON mmt.municipality_type_id = mt.municipality_type_desc)
     AS default_DOT_municipality_dim ON default_DOT_repair_orders_fact.municipality_id = default_DOT_municipality_dim.municipality_id
    GROUP BY  default_DOT_hard_hat.country, default_DOT_hard_hat.postal_code, default_DOT_hard_hat.city, default_DOT_hard_hat.state, default_DOT_dispatcher.company_name, default_DOT_municipality_dim.local_region)

    SELECT  default_DOT_repair_orders_fact.default_DOT_discounted_orders_rate,
        default_DOT_repair_orders_fact.default_DOT_num_repair_orders,
        default_DOT_repair_orders_fact.default_DOT_hard_hat_DOT_country,
        default_DOT_repair_orders_fact.default_DOT_hard_hat_DOT_postal_code,
        default_DOT_repair_orders_fact.default_DOT_hard_hat_DOT_city,
        default_DOT_repair_orders_fact.default_DOT_hard_hat_DOT_state,
        default_DOT_repair_orders_fact.default_DOT_dispatcher_DOT_company_name,
        default_DOT_repair_orders_fact.default_DOT_municipality_dim_DOT_local_region
     FROM default_DOT_repair_orders_fact
    ORDER BY default_DOT_hard_hat_DOT_country, default_DOT_num_repair_orders, default_DOT_dispatcher_DOT_company_name, default_DOT_discounted_orders_rate
    LIMIT 100
    """
    assert_query_strings_equal(data["sql"], expected_sql)
    assert data["columns"] == [
        {
            "column": "default_DOT_discounted_orders_rate",
            "name": "default_DOT_discounted_orders_rate",
            "node": "default.discounted_orders_rate",
            "semantic_entity": "default.discounted_orders_rate.default_DOT_discounted_orders_rate",
            "semantic_type": "metric",
            "type": "double",
        },
        {
            "column": "default_DOT_num_repair_orders",
            "name": "default_DOT_num_repair_orders",
            "node": "default.num_repair_orders",
            "semantic_entity": "default.num_repair_orders.default_DOT_num_repair_orders",
            "semantic_type": "metric",
            "type": "bigint",
        },
        {
            "column": "country",
            "name": "default_DOT_hard_hat_DOT_country",
            "node": "default.hard_hat",
            "semantic_entity": "default.hard_hat.country",
            "semantic_type": "dimension",
            "type": "string",
        },
        {
            "column": "postal_code",
            "name": "default_DOT_hard_hat_DOT_postal_code",
            "node": "default.hard_hat",
            "semantic_entity": "default.hard_hat.postal_code",
            "semantic_type": "dimension",
            "type": "string",
        },
        {
            "column": "city",
            "name": "default_DOT_hard_hat_DOT_city",
            "node": "default.hard_hat",
            "semantic_entity": "default.hard_hat.city",
            "semantic_type": "dimension",
            "type": "string",
        },
        {
            "column": "state",
            "name": "default_DOT_hard_hat_DOT_state",
            "node": "default.hard_hat",
            "semantic_entity": "default.hard_hat.state",
            "semantic_type": "dimension",
            "type": "string",
        },
        {
            "column": "company_name",
            "name": "default_DOT_dispatcher_DOT_company_name",
            "node": "default.dispatcher",
            "semantic_entity": "default.dispatcher.company_name",
            "semantic_type": "dimension",
            "type": "string",
        },
        {
            "column": "local_region",
            "name": "default_DOT_municipality_dim_DOT_local_region",
            "node": "default.municipality_dim",
            "semantic_entity": "default.municipality_dim.local_region",
            "semantic_type": "dimension",
            "type": "string",
        },
    ]


@pytest.mark.asyncio
async def test_get_sql_including_dimension_ids(client_with_roads: AsyncClient):
    """
    Test getting SQL when there are dimensions ids included
    """
    response = await client_with_roads.get(
        "/sql/",
        params={
            "metrics": ["default.avg_repair_price", "default.total_repair_cost"],
            "dimensions": [
                "default.dispatcher.company_name",
                "default.dispatcher.dispatcher_id",
            ],
            "filters": [],
        },
    )
    assert response.status_code == 200
    data = response.json()
    expected = """
        WITH
        default_DOT_repair_orders_fact AS (SELECT  default_DOT_repair_orders_fact.dispatcher_id default_DOT_dispatcher_DOT_dispatcher_id,
            default_DOT_dispatcher.company_name default_DOT_dispatcher_DOT_company_name,
            avg(default_DOT_repair_orders_fact.price) default_DOT_avg_repair_price,
            sum(default_DOT_repair_orders_fact.total_repair_cost) default_DOT_total_repair_cost
         FROM (SELECT  repair_orders.repair_order_id,
            repair_orders.municipality_id,
            repair_orders.hard_hat_id,
            repair_orders.dispatcher_id,
            repair_orders.order_date,
            repair_orders.dispatched_date,
            repair_orders.required_date,
            repair_order_details.discount,
            repair_order_details.price,
            repair_order_details.quantity,
            repair_order_details.repair_type_id,
            repair_order_details.price * repair_order_details.quantity AS total_repair_cost,
            repair_orders.dispatched_date - repair_orders.order_date AS time_to_dispatch,
            repair_orders.dispatched_date - repair_orders.required_date AS dispatch_delay
         FROM roads.repair_orders AS repair_orders JOIN roads.repair_order_details AS repair_order_details ON repair_orders.repair_order_id = repair_order_details.repair_order_id)
         AS default_DOT_repair_orders_fact LEFT JOIN (SELECT  default_DOT_dispatchers.dispatcher_id,
            default_DOT_dispatchers.company_name
         FROM roads.dispatchers AS default_DOT_dispatchers)
         AS default_DOT_dispatcher ON default_DOT_repair_orders_fact.dispatcher_id = default_DOT_dispatcher.dispatcher_id
         GROUP BY  default_DOT_dispatcher.company_name, default_DOT_repair_orders_fact.dispatcher_id
        )

        SELECT  default_DOT_repair_orders_fact.default_DOT_avg_repair_price,
            default_DOT_repair_orders_fact.default_DOT_total_repair_cost,
            default_DOT_repair_orders_fact.default_DOT_dispatcher_DOT_dispatcher_id,
            default_DOT_repair_orders_fact.default_DOT_dispatcher_DOT_company_name
         FROM default_DOT_repair_orders_fact"""
    assert_query_strings_equal(data["sql"], expected)

    response = await client_with_roads.get(
        "/sql/",
        params={
            "metrics": ["default.avg_repair_price", "default.total_repair_cost"],
            "dimensions": [
                "default.hard_hat.hard_hat_id",
                "default.hard_hat.first_name",
            ],
            "filters": [],
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert compare_query_strings(
        data["sql"],
        """
        WITH
        default_DOT_repair_orders_fact AS (SELECT  default_DOT_repair_orders_fact.hard_hat_id default_DOT_hard_hat_DOT_hard_hat_id,
            default_DOT_hard_hat.first_name default_DOT_hard_hat_DOT_first_name,
            avg(default_DOT_repair_orders_fact.price) default_DOT_avg_repair_price,
            sum(default_DOT_repair_orders_fact.total_repair_cost) default_DOT_total_repair_cost
     FROM (SELECT  repair_orders.repair_order_id,
        repair_orders.municipality_id,
        repair_orders.hard_hat_id,
        repair_orders.dispatcher_id,
        repair_orders.order_date,
        repair_orders.dispatched_date,
        repair_orders.required_date,
        repair_order_details.discount,
        repair_order_details.price,
        repair_order_details.quantity,
        repair_order_details.repair_type_id,
        repair_order_details.price * repair_order_details.quantity AS total_repair_cost,
        repair_orders.dispatched_date - repair_orders.order_date AS time_to_dispatch,
        repair_orders.dispatched_date - repair_orders.required_date AS dispatch_delay
     FROM roads.repair_orders AS repair_orders JOIN roads.repair_order_details AS repair_order_details ON repair_orders.repair_order_id = repair_order_details.repair_order_id)
         AS default_DOT_repair_orders_fact LEFT JOIN (SELECT  default_DOT_hard_hats.hard_hat_id,
            default_DOT_hard_hats.first_name,
            default_DOT_hard_hats.state
         FROM roads.hard_hats AS default_DOT_hard_hats)
         AS default_DOT_hard_hat ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
         GROUP BY  default_DOT_repair_orders_fact.hard_hat_id, default_DOT_hard_hat.first_name
        )
        SELECT  default_DOT_repair_orders_fact.default_DOT_avg_repair_price,
            default_DOT_repair_orders_fact.default_DOT_total_repair_cost,
            default_DOT_repair_orders_fact.default_DOT_hard_hat_DOT_hard_hat_id,
            default_DOT_repair_orders_fact.default_DOT_hard_hat_DOT_first_name
         FROM default_DOT_repair_orders_fact
        """,
    )


@pytest.mark.asyncio
async def test_get_sql_including_dimensions_with_disambiguated_columns(
    client_with_roads: AsyncClient,
    duckdb_conn: duckdb.DuckDBPyConnection,  # pylint: disable=c-extension-no-member
):
    """
    Test getting SQL that includes dimensions with SQL that has to disambiguate projection columns with prefixes
    """
    response = await client_with_roads.get(
        "/sql/",
        params={
            "metrics": ["default.total_repair_cost"],
            "dimensions": [
                "default.municipality_dim.state_id",
                "default.municipality_dim.municipality_type_id",
                "default.municipality_dim.municipality_type_desc",
                "default.municipality_dim.municipality_id",
            ],
            "filters": [],
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data["columns"] == [
        {
            "column": "default_DOT_total_repair_cost",
            "name": "default_DOT_total_repair_cost",
            "node": "default.total_repair_cost",
            "semantic_entity": "default.total_repair_cost.default_DOT_total_repair_cost",
            "semantic_type": "metric",
            "type": "double",
        },
        {
            "column": "municipality_id",
            "name": "default_DOT_municipality_dim_DOT_municipality_id",
            "node": "default.municipality_dim",
            "semantic_entity": "default.municipality_dim.municipality_id",
            "semantic_type": "dimension",
            "type": "string",
        },
        {
            "column": "state_id",
            "name": "default_DOT_municipality_dim_DOT_state_id",
            "node": "default.municipality_dim",
            "semantic_entity": "default.municipality_dim.state_id",
            "semantic_type": "dimension",
            "type": "int",
        },
        {
            "column": "municipality_type_id",
            "name": "default_DOT_municipality_dim_DOT_municipality_type_id",
            "node": "default.municipality_dim",
            "semantic_entity": "default.municipality_dim.municipality_type_id",
            "semantic_type": "dimension",
            "type": "string",
        },
        {
            "column": "municipality_type_desc",
            "name": "default_DOT_municipality_dim_DOT_municipality_type_desc",
            "node": "default.municipality_dim",
            "semantic_entity": "default.municipality_dim.municipality_type_desc",
            "semantic_type": "dimension",
            "type": "string",
        },
    ]
    assert str(parse(data["sql"])) == str(
        parse(
            """
        WITH
        default_DOT_repair_orders_fact AS (SELECT  default_DOT_repair_orders_fact.municipality_id default_DOT_municipality_dim_DOT_municipality_id,
            default_DOT_municipality_dim.state_id default_DOT_municipality_dim_DOT_state_id,
            default_DOT_municipality_dim.municipality_type_id default_DOT_municipality_dim_DOT_municipality_type_id,
            default_DOT_municipality_dim.municipality_type_desc default_DOT_municipality_dim_DOT_municipality_type_desc,
            sum(default_DOT_repair_orders_fact.total_repair_cost) default_DOT_total_repair_cost
    FROM (SELECT  repair_orders.repair_order_id,
      repair_orders.municipality_id,
      repair_orders.hard_hat_id,
      repair_orders.dispatcher_id,
      repair_orders.order_date,
      repair_orders.dispatched_date,
      repair_orders.required_date,
      repair_order_details.discount,
      repair_order_details.price,
      repair_order_details.quantity,
      repair_order_details.repair_type_id,
      repair_order_details.price * repair_order_details.quantity AS total_repair_cost,
      repair_orders.dispatched_date - repair_orders.order_date AS time_to_dispatch,
      repair_orders.dispatched_date - repair_orders.required_date AS dispatch_delay
    FROM roads.repair_orders AS repair_orders JOIN roads.repair_order_details AS repair_order_details ON repair_orders.repair_order_id = repair_order_details.repair_order_id)
    AS default_DOT_repair_orders_fact LEFT JOIN (SELECT  m.municipality_id AS municipality_id,
      m.state_id,
      mmt.municipality_type_id AS municipality_type_id,
      mt.municipality_type_desc AS municipality_type_desc
    FROM roads.municipality AS m LEFT JOIN roads.municipality_municipality_type AS mmt ON m.municipality_id = mmt.municipality_id
    LEFT JOIN roads.municipality_type AS mt ON mmt.municipality_type_id = mt.municipality_type_desc)
         AS default_DOT_municipality_dim ON default_DOT_repair_orders_fact.municipality_id = default_DOT_municipality_dim.municipality_id
    GROUP BY  default_DOT_municipality_dim.state_id, default_DOT_municipality_dim.municipality_type_id, default_DOT_municipality_dim.municipality_type_desc, default_DOT_repair_orders_fact.municipality_id)

        SELECT  default_DOT_repair_orders_fact.default_DOT_total_repair_cost,
            default_DOT_repair_orders_fact.default_DOT_municipality_dim_DOT_municipality_id,
            default_DOT_repair_orders_fact.default_DOT_municipality_dim_DOT_state_id,
            default_DOT_repair_orders_fact.default_DOT_municipality_dim_DOT_municipality_type_id,
            default_DOT_repair_orders_fact.default_DOT_municipality_dim_DOT_municipality_type_desc
         FROM default_DOT_repair_orders_fact
        """,
        ),
    )
    result = duckdb_conn.sql(data["sql"])
    assert result.fetchall() == [
        (285627.0, "New York", 33, "A", None),
        (18497.0, "Dallas", 44, "A", None),
        (76463.0, "San Antonio", 44, "A", None),
        (1135603.0, "Philadelphia", 39, "B", None),
    ]

    response = await client_with_roads.get(
        "/sql/",
        params={
            "metrics": ["default.avg_repair_price", "default.total_repair_cost"],
            "dimensions": [
                "default.hard_hat.hard_hat_id",
            ],
            "filters": [],
        },
    )
    assert response.status_code == 200
    data = response.json()
    expected = """WITH
        default_DOT_repair_orders_fact AS (SELECT  default_DOT_repair_orders_fact.hard_hat_id default_DOT_hard_hat_DOT_hard_hat_id,
            avg(default_DOT_repair_orders_fact.price) default_DOT_avg_repair_price,
            sum(default_DOT_repair_orders_fact.total_repair_cost) default_DOT_total_repair_cost
         FROM (SELECT  repair_orders.repair_order_id,
            repair_orders.municipality_id,
            repair_orders.hard_hat_id,
            repair_orders.dispatcher_id,
            repair_orders.order_date,
            repair_orders.dispatched_date,
            repair_orders.required_date,
            repair_order_details.discount,
            repair_order_details.price,
            repair_order_details.quantity,
            repair_order_details.repair_type_id,
            repair_order_details.price * repair_order_details.quantity AS total_repair_cost,
            repair_orders.dispatched_date - repair_orders.order_date AS time_to_dispatch,
            repair_orders.dispatched_date - repair_orders.required_date AS dispatch_delay
         FROM roads.repair_orders AS repair_orders JOIN roads.repair_order_details AS repair_order_details ON repair_orders.repair_order_id = repair_order_details.repair_order_id)
         AS default_DOT_repair_orders_fact
         GROUP BY  default_DOT_repair_orders_fact.hard_hat_id)

        SELECT  default_DOT_repair_orders_fact.default_DOT_avg_repair_price,
            default_DOT_repair_orders_fact.default_DOT_total_repair_cost,
            default_DOT_repair_orders_fact.default_DOT_hard_hat_DOT_hard_hat_id
         FROM default_DOT_repair_orders_fact"""

    assert str(parse(data["sql"])) == str(parse(expected))

    result = duckdb_conn.sql(data["sql"])
    assert result.fetchall() == [
        (54672.75, 218691.0, 1),
        (39301.5, 78603.0, 2),
        (76555.33333333333, 229666.0, 3),
        (54083.5, 216334.0, 4),
        (64190.6, 320953.0, 5),
        (65595.66666666667, 196787.0, 6),
        (53374.0, 53374.0, 7),
        (65682.0, 131364.0, 8),
        (70418.0, 70418.0, 9),
    ]


@pytest.mark.asyncio
async def test_get_sql_for_metrics_filters_validate_dimensions(
    client_with_namespaced_roads: AsyncClient,
):
    """
    Test that we extract the columns from filters to validate that they are from shared dimensions
    """
    response = await client_with_namespaced_roads.get(
        "/sql/",
        params={
            "metrics": ["foo.bar.num_repair_orders", "foo.bar.avg_repair_price"],
            "dimensions": [
                "foo.bar.hard_hat.country",
            ],
            "filters": ["default.hard_hat.city = 'Las Vegas'"],
            "limit": 10,
        },
    )
    data = response.json()
    assert data["message"] == (
        "The filter `default.hard_hat.city = 'Las Vegas'` references the dimension "
        "attribute `default.hard_hat.city`, which is not available on every metric and "
        "thus cannot be included."
    )


@pytest.mark.asyncio
async def test_get_sql_for_metrics_orderby_not_in_dimensions(
    client_example_loader: Callable[[Optional[List[str]]], AsyncClient],
):
    """
    Test that we extract the columns from filters to validate that they are from shared dimensions
    """
    custom_client = await client_example_loader(["ROADS", "NAMESPACED_ROADS"])
    response = await custom_client.get(
        "/sql/",
        params={
            "metrics": ["foo.bar.num_repair_orders", "foo.bar.avg_repair_price"],
            "dimensions": [
                "foo.bar.hard_hat.country",
            ],
            "orderby": ["default.hard_hat.city"],
            "limit": 10,
        },
    )
    data = response.json()
    assert data["message"] == (
        "Columns ['default.hard_hat.city'] in order by "
        "clause must also be specified in the metrics or dimensions"
    )


@pytest.mark.asyncio
async def test_get_sql_for_metrics_orderby_not_in_dimensions_no_access(
    client_example_loader: Callable[[Optional[List[str]]], AsyncClient],
):
    """
    Test that we extract the columns from filters to validate that they are from shared dimensions
    """

    def validate_access_override():
        def _validate_access(access_control: access.AccessControl):
            for request in access_control.requests:
                if (
                    request.access_object.resource_type == access.ResourceType.NODE
                    and request.access_object.name
                    in (
                        "foo.bar.avg_repair_price",
                        "default.hard_hat.city",
                    )
                ):
                    request.deny()
                else:
                    request.approve()

        return _validate_access

    app.dependency_overrides[validate_access] = validate_access_override
    custom_client = await client_example_loader(["ROADS", "NAMESPACED_ROADS"])
    response = await custom_client.get(
        "/sql/",
        params={
            "metrics": ["foo.bar.num_repair_orders", "foo.bar.avg_repair_price"],
            "dimensions": [
                "foo.bar.hard_hat.country",
            ],
            "orderby": ["default.hard_hat.city"],
            "limit": 10,
        },
    )
    data = response.json()
    assert data["message"] == (
        "Columns ['default.hard_hat.city'] in order by "
        "clause must also be specified in the metrics or dimensions"
    )
    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_sql_structs(client_with_roads: AsyncClient):
    """
    Create a transform with structs and verify that metric expressions that reference these
    structs, along with grouping by dimensions that reference these structs will work when
    building metrics SQL.
    """
    await client_with_roads.post(
        "/nodes/transform",
        json={
            "name": "default.simple_agg",
            "description": "simple agg",
            "mode": "published",
            "query": """SELECT
  EXTRACT(YEAR FROM ro.relevant_dates.order_dt) AS order_year,
  EXTRACT(MONTH FROM ro.relevant_dates.order_dt) AS order_month,
  EXTRACT(DAY FROM ro.relevant_dates.order_dt) AS order_day,
  SUM(DATEDIFF(ro.relevant_dates.dispatched_dt, ro.relevant_dates.order_dt)) AS dispatch_delay_sum,
  COUNT(ro.repair_order_id) AS repair_orders_cnt
FROM (
  SELECT
    repair_order_id,
    STRUCT(required_date as required_dt, order_date as order_dt, dispatched_date as dispatched_dt) relevant_dates
  FROM default.repair_orders
) AS ro
GROUP BY
  EXTRACT(YEAR FROM ro.relevant_dates.order_dt),
  EXTRACT(MONTH FROM ro.relevant_dates.order_dt),
  EXTRACT(DAY FROM ro.relevant_dates.order_dt)""",
        },
    )

    await client_with_roads.post(
        "/nodes/metric",
        json={
            "name": "default.average_dispatch_delay",
            "description": "average dispatch delay",
            "mode": "published",
            "query": """select SUM(D.dispatch_delay_sum)/SUM(D.repair_orders_cnt) from default.simple_agg D""",
        },
    )
    dimension_attr = [
        {
            "namespace": "system",
            "name": "dimension",
        },
    ]
    for column in ["order_year", "order_month", "order_day"]:
        await client_with_roads.post(
            f"/nodes/default.simple_agg/columns/{column}/attributes/",
            json=dimension_attr,
        )
    sql_params = {
        "metrics": ["default.average_dispatch_delay"],
        "dimensions": [
            "default.simple_agg.order_year",
            "default.simple_agg.order_month",
            "default.simple_agg.order_day",
        ],
        "filters": ["default.simple_agg.order_year = 2020"],
    }

    expected = """WITH
    default_DOT_simple_agg AS (SELECT  default_DOT_simple_agg.order_year default_DOT_simple_agg_DOT_order_year,
    default_DOT_simple_agg.order_month default_DOT_simple_agg_DOT_order_month,
    default_DOT_simple_agg.order_day default_DOT_simple_agg_DOT_order_day,
    SUM(default_DOT_simple_agg.dispatch_delay_sum) / SUM(default_DOT_simple_agg.repair_orders_cnt) default_DOT_average_dispatch_delay
FROM (SELECT  EXTRACT(YEAR, ro.relevant_dates.order_dt) AS order_year,
    EXTRACT(MONTH, ro.relevant_dates.order_dt) AS order_month,
    EXTRACT(DAY, ro.relevant_dates.order_dt) AS order_day,
    SUM(DATEDIFF(ro.relevant_dates.dispatched_dt, ro.relevant_dates.order_dt)) AS dispatch_delay_sum,
    COUNT(ro.repair_order_id) AS repair_orders_cnt
 FROM (SELECT  default_DOT_repair_orders.repair_order_id,
    struct(default_DOT_repair_orders.required_date AS required_dt, default_DOT_repair_orders.order_date AS order_dt, default_DOT_repair_orders.dispatched_date AS dispatched_dt) relevant_dates
 FROM roads.repair_orders AS default_DOT_repair_orders) AS ro
 GROUP BY  EXTRACT(YEAR, ro.relevant_dates.order_dt), EXTRACT(MONTH, ro.relevant_dates.order_dt), EXTRACT(DAY, ro.relevant_dates.order_dt))
 AS default_DOT_simple_agg
 WHERE  default_DOT_simple_agg.order_year = 2020
 GROUP BY  default_DOT_simple_agg.order_year, default_DOT_simple_agg.order_month, default_DOT_simple_agg.order_day)

SELECT  default_DOT_simple_agg.default_DOT_average_dispatch_delay,
    default_DOT_simple_agg.default_DOT_simple_agg_DOT_order_year,
    default_DOT_simple_agg.default_DOT_simple_agg_DOT_order_month,
    default_DOT_simple_agg.default_DOT_simple_agg_DOT_order_day
 FROM default_DOT_simple_agg"""
    response = await client_with_roads.get("/sql", params=sql_params)
    data = response.json()
    assert str(parse(str(expected))) == str(parse(str(data["sql"])))

    # Test the same query string but with `ro` as a CTE
    await client_with_roads.patch(
        "/nodes/transform",
        json={
            "name": "default.simple_agg",
            "query": """WITH ro as (
  SELECT
    repair_order_id,
    STRUCT(required_date as required_dt, order_date as order_dt, dispatched_date as dispatched_dt) relevant_dates
  FROM default.repair_orders
)
SELECT
  EXTRACT(YEAR FROM ro.relevant_dates.order_dt) AS order_year,
  EXTRACT(MONTH FROM ro.relevant_dates.order_dt) AS order_month,
  EXTRACT(DAY FROM ro.relevant_dates.order_dt) AS order_day,
  SUM(DATEDIFF(ro.relevant_dates.dispatched_dt, ro.relevant_dates.order_dt)) AS dispatch_delay_sum,
  COUNT(ro.repair_order_id) AS repair_orders_cnt
FROM ro
GROUP BY
  EXTRACT(YEAR FROM ro.relevant_dates.order_dt),
  EXTRACT(MONTH FROM ro.relevant_dates.order_dt),
  EXTRACT(DAY FROM ro.relevant_dates.order_dt)""",
        },
    )

    response = await client_with_roads.get("/sql", params=sql_params)
    data = response.json()
    assert compare_query_strings(data["sql"], expected)


@pytest.mark.parametrize(
    "metrics, dimensions, filters, sql, columns, rows",
    [
        # One metric with two measures + one local dimension. Both referenced measures should
        # show up in the generated measures SQL
        (
            ["default.total_repair_order_discounts"],
            ["default.dispatcher.dispatcher_id"],
            [],
            """WITH
            default_DOT_repair_orders_fact AS (SELECT  default_DOT_repair_orders_fact.dispatcher_id default_DOT_dispatcher_DOT_dispatcher_id,
                default_DOT_repair_orders_fact.discount default_DOT_repair_orders_fact_DOT_discount,
                default_DOT_repair_orders_fact.price default_DOT_repair_orders_fact_DOT_price
             FROM (SELECT  repair_orders.repair_order_id,
                repair_orders.municipality_id,
                repair_orders.hard_hat_id,
                repair_orders.dispatcher_id,
                repair_orders.order_date,
                repair_orders.dispatched_date,
                repair_orders.required_date,
                repair_order_details.discount,
                repair_order_details.price,
                repair_order_details.quantity,
                repair_order_details.repair_type_id,
                repair_order_details.price * repair_order_details.quantity AS total_repair_cost,
                repair_orders.dispatched_date - repair_orders.order_date AS time_to_dispatch,
                repair_orders.dispatched_date - repair_orders.required_date AS dispatch_delay
             FROM roads.repair_orders AS repair_orders JOIN roads.repair_order_details AS repair_order_details ON repair_orders.repair_order_id = repair_order_details.repair_order_id)
             AS default_DOT_repair_orders_fact)
            SELECT  default_DOT_repair_orders_fact.default_DOT_repair_orders_fact_DOT_discount,
                default_DOT_repair_orders_fact.default_DOT_repair_orders_fact_DOT_price,
                default_DOT_repair_orders_fact.default_DOT_dispatcher_DOT_dispatcher_id
             FROM default_DOT_repair_orders_fact""",
            [
                {
                    "column": "discount",
                    "name": "default_DOT_repair_orders_fact_DOT_discount",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.discount",
                    "semantic_type": "measure",
                    "type": "float",
                },
                {
                    "column": "price",
                    "name": "default_DOT_repair_orders_fact_DOT_price",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.price",
                    "semantic_type": "measure",
                    "type": "float",
                },
                {
                    "column": "dispatcher_id",
                    "name": "default_DOT_dispatcher_DOT_dispatcher_id",
                    "node": "default.dispatcher",
                    "semantic_entity": "default.dispatcher.dispatcher_id",
                    "semantic_type": "dimension",
                    "type": "int",
                },
            ],
            [
                (0.05000000074505806, 63708.0, 3),
                (0.05000000074505806, 67253.0, 1),
                (0.05000000074505806, 66808.0, 2),
                (0.05000000074505806, 18497.0, 1),
                (0.05000000074505806, 76463.0, 2),
                (0.05000000074505806, 87858.0, 2),
                (0.05000000074505806, 63918.0, 2),
                (0.05000000074505806, 21083.0, 3),
                (0.05000000074505806, 74555.0, 2),
                (0.05000000074505806, 27222.0, 3),
                (0.05000000074505806, 73600.0, 1),
                (0.009999999776482582, 54901.0, 3),
                (0.009999999776482582, 51594.0, 1),
                (0.009999999776482582, 65114.0, 2),
                (0.009999999776482582, 48919.0, 3),
                (0.009999999776482582, 70418.0, 3),
                (0.009999999776482582, 29684.0, 3),
                (0.009999999776482582, 62928.0, 1),
                (0.009999999776482582, 97916.0, 3),
                (0.009999999776482582, 44120.0, 1),
                (0.009999999776482582, 53374.0, 3),
                (0.009999999776482582, 87289.0, 1),
                (0.009999999776482582, 92366.0, 1),
                (0.009999999776482582, 47857.0, 2),
                (0.009999999776482582, 68745.0, 2),
            ],
        ),
        # Two metrics with overlapping measures + one joinable dimension
        (
            [
                "default.total_repair_order_discounts",
                "default.avg_repair_order_discounts",
            ],
            ["default.dispatcher.dispatcher_id"],
            [],
            """WITH
            default_DOT_repair_orders_fact AS (SELECT  default_DOT_repair_orders_fact.dispatcher_id default_DOT_dispatcher_DOT_dispatcher_id,
                default_DOT_repair_orders_fact.discount default_DOT_repair_orders_fact_DOT_discount,
                default_DOT_repair_orders_fact.price default_DOT_repair_orders_fact_DOT_price
             FROM (SELECT  repair_orders.repair_order_id,
                repair_orders.municipality_id,
                repair_orders.hard_hat_id,
                repair_orders.dispatcher_id,
                repair_orders.order_date,
                repair_orders.dispatched_date,
                repair_orders.required_date,
                repair_order_details.discount,
                repair_order_details.price,
                repair_order_details.quantity,
                repair_order_details.repair_type_id,
                repair_order_details.price * repair_order_details.quantity AS total_repair_cost,
                repair_orders.dispatched_date - repair_orders.order_date AS time_to_dispatch,
                repair_orders.dispatched_date - repair_orders.required_date AS dispatch_delay
             FROM roads.repair_orders AS repair_orders JOIN roads.repair_order_details AS repair_order_details ON repair_orders.repair_order_id = repair_order_details.repair_order_id)
             AS default_DOT_repair_orders_fact)
            SELECT  default_DOT_repair_orders_fact.default_DOT_repair_orders_fact_DOT_discount,
                default_DOT_repair_orders_fact.default_DOT_repair_orders_fact_DOT_price,
                default_DOT_repair_orders_fact.default_DOT_dispatcher_DOT_dispatcher_id
             FROM default_DOT_repair_orders_fact""",
            [
                {
                    "column": "discount",
                    "name": "default_DOT_repair_orders_fact_DOT_discount",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.discount",
                    "semantic_type": "measure",
                    "type": "float",
                },
                {
                    "column": "price",
                    "name": "default_DOT_repair_orders_fact_DOT_price",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.price",
                    "semantic_type": "measure",
                    "type": "float",
                },
                {
                    "column": "dispatcher_id",
                    "name": "default_DOT_dispatcher_DOT_dispatcher_id",
                    "node": "default.dispatcher",
                    "semantic_entity": "default.dispatcher.dispatcher_id",
                    "semantic_type": "dimension",
                    "type": "int",
                },
            ],
            [
                (0.05000000074505806, 63708.0, 3),
                (0.05000000074505806, 67253.0, 1),
                (0.05000000074505806, 66808.0, 2),
                (0.05000000074505806, 18497.0, 1),
                (0.05000000074505806, 76463.0, 2),
                (0.05000000074505806, 87858.0, 2),
                (0.05000000074505806, 63918.0, 2),
                (0.05000000074505806, 21083.0, 3),
                (0.05000000074505806, 74555.0, 2),
                (0.05000000074505806, 27222.0, 3),
                (0.05000000074505806, 73600.0, 1),
                (0.009999999776482582, 54901.0, 3),
                (0.009999999776482582, 51594.0, 1),
                (0.009999999776482582, 65114.0, 2),
                (0.009999999776482582, 48919.0, 3),
                (0.009999999776482582, 70418.0, 3),
                (0.009999999776482582, 29684.0, 3),
                (0.009999999776482582, 62928.0, 1),
                (0.009999999776482582, 97916.0, 3),
                (0.009999999776482582, 44120.0, 1),
                (0.009999999776482582, 53374.0, 3),
                (0.009999999776482582, 87289.0, 1),
                (0.009999999776482582, 92366.0, 1),
                (0.009999999776482582, 47857.0, 2),
                (0.009999999776482582, 68745.0, 2),
            ],
        ),
        # Two metrics with different measures + two dimensions from different sources
        (
            ["default.avg_time_to_dispatch", "default.total_repair_cost"],
            ["default.us_state.state_name", "default.dispatcher.company_name"],
            ["default.us_state.state_name = 'New Jersey'"],
            """WITH
default_DOT_repair_orders_fact AS (SELECT  default_DOT_repair_orders_fact.total_repair_cost default_DOT_repair_orders_fact_DOT_total_repair_cost,
    default_DOT_repair_orders_fact.time_to_dispatch default_DOT_repair_orders_fact_DOT_time_to_dispatch,
    default_DOT_us_state.state_name default_DOT_us_state_DOT_state_name,
    default_DOT_dispatcher.company_name default_DOT_dispatcher_DOT_company_name
 FROM (SELECT  repair_orders.repair_order_id,
    repair_orders.municipality_id,
    repair_orders.hard_hat_id,
    repair_orders.dispatcher_id,
    repair_orders.order_date,
    repair_orders.dispatched_date,
    repair_orders.required_date,
    repair_order_details.discount,
    repair_order_details.price,
    repair_order_details.quantity,
    repair_order_details.repair_type_id,
    repair_order_details.price * repair_order_details.quantity AS total_repair_cost,
    repair_orders.dispatched_date - repair_orders.order_date AS time_to_dispatch,
    repair_orders.dispatched_date - repair_orders.required_date AS dispatch_delay
 FROM roads.repair_orders AS repair_orders JOIN roads.repair_order_details AS repair_order_details ON repair_orders.repair_order_id = repair_order_details.repair_order_id)
 AS default_DOT_repair_orders_fact LEFT JOIN (SELECT  default_DOT_dispatchers.dispatcher_id,
    default_DOT_dispatchers.company_name
 FROM roads.dispatchers AS default_DOT_dispatchers)
 AS default_DOT_dispatcher ON default_DOT_repair_orders_fact.dispatcher_id = default_DOT_dispatcher.dispatcher_id
LEFT JOIN (SELECT  default_DOT_hard_hats.hard_hat_id,
    default_DOT_hard_hats.state
 FROM roads.hard_hats AS default_DOT_hard_hats)
 AS default_DOT_hard_hat ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
LEFT JOIN (SELECT  s.state_name,
    s.state_abbr AS state_short
 FROM roads.us_states AS s)
 AS default_DOT_us_state ON default_DOT_hard_hat.state = default_DOT_us_state.state_short
 WHERE  default_DOT_us_state.state_name = 'New Jersey')

SELECT  default_DOT_repair_orders_fact.default_DOT_repair_orders_fact_DOT_total_repair_cost,
    default_DOT_repair_orders_fact.default_DOT_repair_orders_fact_DOT_time_to_dispatch,
    default_DOT_repair_orders_fact.default_DOT_us_state_DOT_state_name,
    default_DOT_repair_orders_fact.default_DOT_dispatcher_DOT_company_name
 FROM default_DOT_repair_orders_fact""",
            [
                {
                    "column": "total_repair_cost",
                    "name": "default_DOT_repair_orders_fact_DOT_total_repair_cost",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.total_repair_cost",
                    "semantic_type": "measure",
                    "type": "float",
                },
                {
                    "column": "time_to_dispatch",
                    "name": "default_DOT_repair_orders_fact_DOT_time_to_dispatch",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.time_to_dispatch",
                    "semantic_type": "measure",
                    "type": "timestamp",
                },
                {
                    "column": "state_name",
                    "name": "default_DOT_us_state_DOT_state_name",
                    "node": "default.us_state",
                    "semantic_entity": "default.us_state.state_name",
                    "semantic_type": "dimension",
                    "type": "string",
                },
                {
                    "column": "company_name",
                    "name": "default_DOT_dispatcher_DOT_company_name",
                    "node": "default.dispatcher",
                    "semantic_entity": "default.dispatcher.company_name",
                    "semantic_type": "dimension",
                    "type": "string",
                },
            ],
            [
                (92366.0, 204, "New Jersey", "Pothole Pete"),
                (44120.0, 196, "New Jersey", "Pothole Pete"),
                (18497.0, 146, "New Jersey", "Pothole Pete"),
                (63708.0, 150, "New Jersey", "Federal Roads Group"),
            ],
        ),
        (
            # Two parent transforms (requires COALESCE of dimensions across them), no filters
            [
                "default.avg_time_to_dispatch",
                "default.total_repair_cost",
                "default.num_repair_orders",
                "default.total_repair_order_discounts",
                "default.avg_length_of_employment",
                "default.avg_repair_price",
            ],
            ["default.us_state.state_name"],
            [],
            """WITH
default_DOT_repair_orders_fact AS (SELECT  default_DOT_repair_orders_fact.repair_order_id default_DOT_repair_orders_fact_DOT_repair_order_id,
    default_DOT_repair_orders_fact.discount default_DOT_repair_orders_fact_DOT_discount,
    default_DOT_repair_orders_fact.price default_DOT_repair_orders_fact_DOT_price,
    default_DOT_repair_orders_fact.total_repair_cost default_DOT_repair_orders_fact_DOT_total_repair_cost,
    default_DOT_repair_orders_fact.time_to_dispatch default_DOT_repair_orders_fact_DOT_time_to_dispatch,
    default_DOT_us_state.state_name default_DOT_us_state_DOT_state_name
 FROM (SELECT  repair_orders.repair_order_id,
    repair_orders.municipality_id,
    repair_orders.hard_hat_id,
    repair_orders.dispatcher_id,
    repair_orders.order_date,
    repair_orders.dispatched_date,
    repair_orders.required_date,
    repair_order_details.discount,
    repair_order_details.price,
    repair_order_details.quantity,
    repair_order_details.repair_type_id,
    repair_order_details.price * repair_order_details.quantity AS total_repair_cost,
    repair_orders.dispatched_date - repair_orders.order_date AS time_to_dispatch,
    repair_orders.dispatched_date - repair_orders.required_date AS dispatch_delay
 FROM roads.repair_orders AS repair_orders JOIN roads.repair_order_details AS repair_order_details ON repair_orders.repair_order_id = repair_order_details.repair_order_id)
 AS default_DOT_repair_orders_fact LEFT JOIN (SELECT  default_DOT_hard_hats.hard_hat_id,
    default_DOT_hard_hats.state
 FROM roads.hard_hats AS default_DOT_hard_hats)
 AS default_DOT_hard_hat ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
LEFT JOIN (SELECT  s.state_name,
    s.state_abbr AS state_short
 FROM roads.us_states AS s)
 AS default_DOT_us_state ON default_DOT_hard_hat.state = default_DOT_us_state.state_short),
default_DOT_hard_hat AS (SELECT  default_DOT_hard_hat.hire_date default_DOT_hard_hat_DOT_hire_date,
    default_DOT_us_state.state_name default_DOT_us_state_DOT_state_name
 FROM (SELECT  default_DOT_hard_hats.hard_hat_id,
    default_DOT_hard_hats.last_name,
    default_DOT_hard_hats.first_name,
    default_DOT_hard_hats.title,
    default_DOT_hard_hats.birth_date,
    default_DOT_hard_hats.hire_date,
    default_DOT_hard_hats.address,
    default_DOT_hard_hats.city,
    default_DOT_hard_hats.state,
    default_DOT_hard_hats.postal_code,
    default_DOT_hard_hats.country,
    default_DOT_hard_hats.manager,
    default_DOT_hard_hats.contractor_id
 FROM roads.hard_hats AS default_DOT_hard_hats)
 AS default_DOT_hard_hat LEFT JOIN (SELECT  s.state_name,
    s.state_abbr AS state_short
 FROM roads.us_states AS s)
 AS default_DOT_us_state ON default_DOT_hard_hat.state = default_DOT_us_state.state_short)

SELECT  default_DOT_repair_orders_fact.default_DOT_repair_orders_fact_DOT_repair_order_id,
    default_DOT_repair_orders_fact.default_DOT_repair_orders_fact_DOT_discount,
    default_DOT_repair_orders_fact.default_DOT_repair_orders_fact_DOT_price,
    default_DOT_repair_orders_fact.default_DOT_repair_orders_fact_DOT_total_repair_cost,
    default_DOT_repair_orders_fact.default_DOT_repair_orders_fact_DOT_time_to_dispatch,
    default_DOT_hard_hat.default_DOT_hard_hat_DOT_hire_date,
    COALESCE(default_DOT_repair_orders_fact.default_DOT_us_state_DOT_state_name, default_DOT_hard_hat.default_DOT_us_state_DOT_state_name) default_DOT_us_state_DOT_state_name
 FROM default_DOT_repair_orders_fact FULL OUTER JOIN default_DOT_hard_hat ON default_DOT_repair_orders_fact.default_DOT_us_state_DOT_state_name = default_DOT_hard_hat.default_DOT_us_state_DOT_state_name""",
            [
                {
                    "column": "repair_order_id",
                    "name": "default_DOT_repair_orders_fact_DOT_repair_order_id",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.repair_order_id",
                    "semantic_type": "measure",
                    "type": "int",
                },
                {
                    "column": "discount",
                    "name": "default_DOT_repair_orders_fact_DOT_discount",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.discount",
                    "semantic_type": "measure",
                    "type": "float",
                },
                {
                    "column": "price",
                    "name": "default_DOT_repair_orders_fact_DOT_price",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.price",
                    "semantic_type": "measure",
                    "type": "float",
                },
                {
                    "column": "total_repair_cost",
                    "name": "default_DOT_repair_orders_fact_DOT_total_repair_cost",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.total_repair_cost",
                    "semantic_type": "measure",
                    "type": "float",
                },
                {
                    "column": "time_to_dispatch",
                    "name": "default_DOT_repair_orders_fact_DOT_time_to_dispatch",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.time_to_dispatch",
                    "semantic_type": "measure",
                    "type": "timestamp",
                },
                {
                    "column": "hire_date",
                    "name": "default_DOT_hard_hat_DOT_hire_date",
                    "node": "default.hard_hat",
                    "semantic_entity": "default.hard_hat.hire_date",
                    "semantic_type": "measure",
                    "type": "timestamp",
                },
                {
                    "column": "state_name",
                    "name": "default_DOT_us_state_DOT_state_name",
                    "node": "default.us_state",
                    "semantic_entity": "default.us_state.state_name",
                    "semantic_type": "dimension",
                    "type": "string",
                },
            ],
            [
                (
                    10012,
                    0.009999999776482582,
                    54901.0,
                    54901.0,
                    217,
                    datetime.date(2013, 3, 5),
                    "Arizona",
                ),
                (
                    10017,
                    0.009999999776482582,
                    29684.0,
                    29684.0,
                    231,
                    datetime.date(2003, 4, 14),
                    "Connecticut",
                ),
                (
                    10025,
                    0.009999999776482582,
                    68745.0,
                    68745.0,
                    203,
                    datetime.date(2013, 10, 17),
                    "Georgia",
                ),
                (
                    10009,
                    0.05000000074505806,
                    74555.0,
                    74555.0,
                    220,
                    datetime.date(1990, 7, 2),
                    "Massachusetts",
                ),
                (
                    10024,
                    0.009999999776482582,
                    47857.0,
                    47857.0,
                    204,
                    datetime.date(2012, 1, 13),
                    "Michigan",
                ),
                (
                    10023,
                    0.009999999776482582,
                    92366.0,
                    92366.0,
                    204,
                    datetime.date(2009, 2, 6),
                    "New Jersey",
                ),
                (
                    10021,
                    0.009999999776482582,
                    53374.0,
                    53374.0,
                    205,
                    datetime.date(2013, 1, 2),
                    "New York",
                ),
                (
                    10016,
                    0.009999999776482582,
                    70418.0,
                    70418.0,
                    232,
                    datetime.date(2020, 11, 15),
                    "Oklahoma",
                ),
                (
                    10013,
                    0.009999999776482582,
                    51594.0,
                    51594.0,
                    216,
                    datetime.date(2003, 2, 2),
                    "Pennsylvania",
                ),
                (
                    10005,
                    0.05000000074505806,
                    76463.0,
                    76463.0,
                    145,
                    datetime.date(2013, 3, 5),
                    "Arizona",
                ),
                (
                    10015,
                    0.009999999776482582,
                    48919.0,
                    48919.0,
                    233,
                    datetime.date(2003, 4, 14),
                    "Connecticut",
                ),
                (
                    10018,
                    0.009999999776482582,
                    62928.0,
                    62928.0,
                    230,
                    datetime.date(2013, 10, 17),
                    "Georgia",
                ),
                (
                    10006,
                    0.05000000074505806,
                    87858.0,
                    87858.0,
                    144,
                    datetime.date(1990, 7, 2),
                    "Massachusetts",
                ),
                (
                    10022,
                    0.009999999776482582,
                    87289.0,
                    87289.0,
                    204,
                    datetime.date(2012, 1, 13),
                    "Michigan",
                ),
                (
                    10020,
                    0.009999999776482582,
                    44120.0,
                    44120.0,
                    196,
                    datetime.date(2009, 2, 6),
                    "New Jersey",
                ),
                (
                    10011,
                    0.05000000074505806,
                    73600.0,
                    73600.0,
                    218,
                    datetime.date(2003, 2, 2),
                    "Pennsylvania",
                ),
                (
                    10014,
                    0.009999999776482582,
                    65114.0,
                    65114.0,
                    216,
                    datetime.date(2013, 10, 17),
                    "Georgia",
                ),
                (
                    10002,
                    0.05000000074505806,
                    67253.0,
                    67253.0,
                    149,
                    datetime.date(1990, 7, 2),
                    "Massachusetts",
                ),
                (
                    10019,
                    0.009999999776482582,
                    97916.0,
                    97916.0,
                    199,
                    datetime.date(2012, 1, 13),
                    "Michigan",
                ),
                (
                    10004,
                    0.05000000074505806,
                    18497.0,
                    18497.0,
                    146,
                    datetime.date(2009, 2, 6),
                    "New Jersey",
                ),
                (
                    10010,
                    0.05000000074505806,
                    27222.0,
                    27222.0,
                    219,
                    datetime.date(2003, 2, 2),
                    "Pennsylvania",
                ),
                (
                    10008,
                    0.05000000074505806,
                    21083.0,
                    21083.0,
                    223,
                    datetime.date(2012, 1, 13),
                    "Michigan",
                ),
                (
                    10001,
                    0.05000000074505806,
                    63708.0,
                    63708.0,
                    150,
                    datetime.date(2009, 2, 6),
                    "New Jersey",
                ),
                (
                    10007,
                    0.05000000074505806,
                    63918.0,
                    63918.0,
                    224,
                    datetime.date(2003, 2, 2),
                    "Pennsylvania",
                ),
                (
                    10003,
                    0.05000000074505806,
                    66808.0,
                    66808.0,
                    146,
                    datetime.date(2012, 1, 13),
                    "Michigan",
                ),
            ],
        ),
    ],
)
@pytest.mark.asyncio
async def test_measures_sql_with_filters(  # pylint: disable=too-many-arguments
    metrics,
    dimensions,
    filters,
    sql,
    columns,
    rows,
    client_with_roads: AsyncClient,
    duckdb_conn: duckdb.DuckDBPyConnection,  # pylint: disable=c-extension-no-member
):
    """
    Test ``GET /sql/measures`` with various metrics, filters, and dimensions.
    """
    sql_params = {
        "metrics": metrics,
        "dimensions": dimensions,
        "filters": filters,
    }
    response = await client_with_roads.get("/sql/measures", params=sql_params)
    data = response.json()
    assert_query_strings_equal(data["sql"], sql)
    result = duckdb_conn.sql(data["sql"])
    assert result.fetchall() == rows
    assert data["columns"] == columns


@pytest.mark.asyncio
async def test_filter_pushdowns(
    client_with_roads: AsyncClient,
):
    """
    Pushing down filters should use the column names and not the column aliases
    """
    response = await client_with_roads.patch(
        "/nodes/default.repair_orders_fact",
        json={
            "query": "SELECT repair_orders.hard_hat_id AS hh_id "
            "FROM default.repair_orders repair_orders",
        },
    )
    assert response.status_code == 200

    response = await client_with_roads.post(
        "/nodes/default.repair_orders_fact/link",
        json={
            "dimension_node": "default.hard_hat",
            "join_type": "left",
            "join_on": (
                "default.repair_orders_fact.hh_id = default.hard_hat.hard_hat_id"
            ),
        },
    )
    assert response.status_code == 201

    response = await client_with_roads.get(
        "/sql/default.repair_orders_fact",
        params={
            "dimensions": ["default.hard_hat.hard_hat_id"],
            "filters": [
                "default.hard_hat.hard_hat_id IN (123, 13) AND "
                "default.hard_hat.hard_hat_id = 123 OR default.hard_hat.hard_hat_id = 13",
            ],
        },
    )
    assert str(parse(response.json()["sql"])) == str(
        parse(
            """
            SELECT
              default_DOT_repair_orders_fact.hh_id default_DOT_repair_orders_fact_DOT_hh_id,
              default_DOT_repair_orders_fact.hh_id default_DOT_hard_hat_DOT_hard_hat_id
            FROM (
              SELECT
                repair_orders.hard_hat_id AS hh_id
              FROM roads.repair_orders AS repair_orders
            ) AS default_DOT_repair_orders_fact
            WHERE  default_DOT_repair_orders_fact.hh_id IN (123, 13)
              AND default_DOT_repair_orders_fact.hh_id = 123
              OR default_DOT_repair_orders_fact.hh_id = 13
              AND default_DOT_repair_orders_fact.hh_id IN (123, 13)
              AND default_DOT_repair_orders_fact.hh_id = 123
              OR default_DOT_repair_orders_fact.hh_id = 13
            """,
        ),
    )
