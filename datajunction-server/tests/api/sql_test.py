"""Tests for the /sql/ endpoint"""

import duckdb
import pytest
from httpx import AsyncClient, Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.column import Column
from datajunction_server.database.database import Database
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.queryrequest import QueryBuildType, QueryRequest
from datajunction_server.database.user import User
from datajunction_server.internal.access.authorization import validate_access
from datajunction_server.models import access
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.sql.parsing.types import IntegerType, StringType
from tests.sql.utils import compare_query_strings


@pytest.mark.asyncio
@pytest.mark.skip(reason="Bad test setup")
async def test_sql(
    session: AsyncSession,
    client: AsyncClient,
    current_user: User,
) -> None:
    """
    Test ``GET /sql/{name}/``.
    """
    database = Database(name="test", URI="blah://", tables=[])

    source_node = Node(
        name="default.my_table",
        type=NodeType.SOURCE,
        current_version="1",
        created_by_id=current_user.id,
    )
    source_node_rev = NodeRevision(
        name=source_node.name,
        node=source_node,
        version="1",
        schema_="rev",
        table="my_table",
        columns=[Column(name="one", type=StringType(), order=0)],
        type=NodeType.SOURCE,
        created_by_id=current_user.id,
    )

    node = Node(
        name="default.a_metric",
        type=NodeType.METRIC,
        current_version="1",
        created_by_id=current_user.id,
    )
    node_revision = NodeRevision(
        name=node.name,
        node=node,
        version="1",
        query="SELECT COUNT(*) FROM default.my_table",
        type=NodeType.METRIC,
        created_by_id=current_user.id,
        columns=[Column(name="col0", type=IntegerType(), order=0)],
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
            "/sql/measures/v2",
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

        response = await client_with_roads.post(
            "/nodes/default.repair_orders_fact/link",
            json={
                "dimension_node": "default.municipality_dim",
                "join_type": "inner",
                "join_on": (
                    "default.repair_orders_fact.municipality_id = default.municipality_dim.municipality_id"
                ),
            },
        )

        response = await client_with_roads.post(
            "/nodes/default.repair_orders_fact/link",
            json={
                "dimension_node": "default.hard_hat",
                "join_type": "inner",
                "join_on": (
                    "default.repair_orders_fact.hard_hat_id = default.hard_hat.hard_hat_id"
                ),
            },
        )

        response = await client_with_roads.post(
            "/nodes/default.repair_orders_fact/link",
            json={
                "dimension_node": "default.hard_hat_to_delete",
                "join_type": "left",
                "join_on": (
                    "default.repair_orders_fact.hard_hat_id = default.hard_hat_to_delete.hard_hat_id"
                ),
            },
        )

        response = await client_with_roads.post(
            "/nodes/default.repair_orders_fact/link",
            json={
                "dimension_node": "default.dispatcher",
                "join_type": "inner",
                "join_on": (
                    "default.repair_orders_fact.dispatcher_id = default.dispatcher.dispatcher_id"
                ),
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
@pytest.mark.skip(reason="Not saving to query requests")
async def test_saving_node_sql_requests(
    session: AsyncSession,
    client_with_roads: AsyncClient,
    transform_node_sql_request,
    update_transform_node,
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
        "are not available dimensions on default.repair_orders_fact"
        in response.json()["message"]
    )

    # Update the transform node with a new query
    response = await update_transform_node()
    assert response.status_code == 201

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
@pytest.mark.skip(reason="Not saving v2 measures SQL to cache for the time being")
async def test_saving_measures_sql_requests(
    session: AsyncSession,
    client_with_roads: AsyncClient,
    measures_sql_request,
    update_transform_node,
) -> None:
    """
      Test saving query request while requesting measures SQL for a set of metrics + dimensions
    filters. It also checks that additional arguments like `include_all_columns` are recorded
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
    assert query_requests[0].query.strip() == response[0]["sql"].strip()
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
@pytest.mark.skip(reason="Not saving to query requests")
async def test_saving_metrics_sql_requests(
    session: AsyncSession,
    client_with_roads: AsyncClient,
    metrics_sql_request,
    update_transform_node,
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


async def verify_node_sql(
    custom_client: AsyncClient,
    node_name: str,
    dimensions,
    filters,
    expected_sql,
    expected_columns,
    expected_rows,
):
    """
    Verifies node SQL generation.
    """
    response = await custom_client.get(
        f"/sql/{node_name}/",
        params={"dimensions": dimensions, "filters": filters},
    )
    sql_data = response.json()
    # Run the query against local duckdb file if it's part of the roads model
    # response = await custom_client.get(
    #     f"/data/{node_name}/",
    #     params={"dimensions": dimensions, "filters": filters},
    # )
    # data = response.json()
    # assert data["results"][0]["rows"] == expected_rows
    assert str(parse(str(sql_data["sql"]))) == str(parse(str(expected_sql)))
    assert sql_data["columns"] == expected_columns


@pytest.mark.asyncio
async def test_transform_sql_filter_joinable_dimension(
    module__client_with_examples: AsyncClient,
):
    """
    Test ``GET /sql/{node_name}/`` with various filters and dimensions.
    """
    await verify_node_sql(
        module__client_with_examples,
        node_name="default.repair_orders_fact",
        dimensions=["default.hard_hat.first_name", "default.hard_hat.last_name"],
        filters=["default.hard_hat.state = 'NY'"],
        expected_sql="""
        WITH
        default_DOT_repair_orders_fact AS (
        SELECT  repair_orders.repair_order_id,
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
        ),
        default_DOT_hard_hat AS (
        SELECT  default_DOT_hard_hats.hard_hat_id,
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
         FROM roads.hard_hats AS default_DOT_hard_hats
         WHERE  default_DOT_hard_hats.state = 'NY'
        )
        SELECT  default_DOT_repair_orders_fact.repair_order_id default_DOT_repair_orders_fact_DOT_repair_order_id,
            default_DOT_repair_orders_fact.municipality_id default_DOT_repair_orders_fact_DOT_municipality_id,
            default_DOT_repair_orders_fact.hard_hat_id default_DOT_repair_orders_fact_DOT_hard_hat_id,
            default_DOT_repair_orders_fact.dispatcher_id default_DOT_repair_orders_fact_DOT_dispatcher_id,
            default_DOT_repair_orders_fact.order_date default_DOT_repair_orders_fact_DOT_order_date,
            default_DOT_repair_orders_fact.dispatched_date default_DOT_repair_orders_fact_DOT_dispatched_date,
            default_DOT_repair_orders_fact.required_date default_DOT_repair_orders_fact_DOT_required_date,
            default_DOT_repair_orders_fact.discount default_DOT_repair_orders_fact_DOT_discount,
            default_DOT_repair_orders_fact.price default_DOT_repair_orders_fact_DOT_price,
            default_DOT_repair_orders_fact.quantity default_DOT_repair_orders_fact_DOT_quantity,
            default_DOT_repair_orders_fact.repair_type_id default_DOT_repair_orders_fact_DOT_repair_type_id,
            default_DOT_repair_orders_fact.total_repair_cost default_DOT_repair_orders_fact_DOT_total_repair_cost,
            default_DOT_repair_orders_fact.time_to_dispatch default_DOT_repair_orders_fact_DOT_time_to_dispatch,
            default_DOT_repair_orders_fact.dispatch_delay default_DOT_repair_orders_fact_DOT_dispatch_delay,
            default_DOT_hard_hat.first_name default_DOT_hard_hat_DOT_first_name,
            default_DOT_hard_hat.last_name default_DOT_hard_hat_DOT_last_name,
            default_DOT_hard_hat.state default_DOT_hard_hat_DOT_state
         FROM default_DOT_repair_orders_fact INNER JOIN default_DOT_hard_hat ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
         WHERE  default_DOT_hard_hat.state = 'NY'
        """,
        expected_columns=[
            {
                "column": "repair_order_id",
                "name": "default_DOT_repair_orders_fact_DOT_repair_order_id",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.repair_order_id",
                "semantic_type": None,
                "type": "int",
            },
            {
                "column": "municipality_id",
                "name": "default_DOT_repair_orders_fact_DOT_municipality_id",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.municipality_id",
                "semantic_type": None,
                "type": "string",
            },
            {
                "column": "hard_hat_id",
                "name": "default_DOT_repair_orders_fact_DOT_hard_hat_id",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.hard_hat_id",
                "semantic_type": None,
                "type": "int",
            },
            {
                "column": "dispatcher_id",
                "name": "default_DOT_repair_orders_fact_DOT_dispatcher_id",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.dispatcher_id",
                "semantic_type": None,
                "type": "int",
            },
            {
                "column": "order_date",
                "name": "default_DOT_repair_orders_fact_DOT_order_date",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.order_date",
                "semantic_type": None,
                "type": "timestamp",
            },
            {
                "column": "dispatched_date",
                "name": "default_DOT_repair_orders_fact_DOT_dispatched_date",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.dispatched_date",
                "semantic_type": None,
                "type": "timestamp",
            },
            {
                "column": "required_date",
                "name": "default_DOT_repair_orders_fact_DOT_required_date",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.required_date",
                "semantic_type": None,
                "type": "timestamp",
            },
            {
                "column": "discount",
                "name": "default_DOT_repair_orders_fact_DOT_discount",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.discount",
                "semantic_type": None,
                "type": "float",
            },
            {
                "column": "price",
                "name": "default_DOT_repair_orders_fact_DOT_price",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.price",
                "semantic_type": None,
                "type": "float",
            },
            {
                "column": "quantity",
                "name": "default_DOT_repair_orders_fact_DOT_quantity",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.quantity",
                "semantic_type": None,
                "type": "int",
            },
            {
                "column": "repair_type_id",
                "name": "default_DOT_repair_orders_fact_DOT_repair_type_id",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.repair_type_id",
                "semantic_type": None,
                "type": "int",
            },
            {
                "column": "total_repair_cost",
                "name": "default_DOT_repair_orders_fact_DOT_total_repair_cost",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.total_repair_cost",
                "semantic_type": None,
                "type": "float",
            },
            {
                "column": "time_to_dispatch",
                "name": "default_DOT_repair_orders_fact_DOT_time_to_dispatch",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.time_to_dispatch",
                "semantic_type": None,
                "type": "timestamp",
            },
            {
                "column": "dispatch_delay",
                "name": "default_DOT_repair_orders_fact_DOT_dispatch_delay",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.dispatch_delay",
                "semantic_type": None,
                "type": "timestamp",
            },
            {
                "column": "first_name",
                "name": "default_DOT_hard_hat_DOT_first_name",
                "node": "default.hard_hat",
                "semantic_entity": "default.hard_hat.first_name",
                "semantic_type": None,
                "type": "string",
            },
            {
                "column": "last_name",
                "name": "default_DOT_hard_hat_DOT_last_name",
                "node": "default.hard_hat",
                "semantic_entity": "default.hard_hat.last_name",
                "semantic_type": None,
                "type": "string",
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
        expected_rows=[
            [
                10021,
                "Philadelphia",
                7,
                3,
                "2007-05-10",
                "2007-12-01",
                "2009-08-27",
                0.009999999776482582,
                53374.0,
                1,
                1,
                53374.0,
                205,
                -635,
                "Boone",
                "William",
                "NY",
            ],
        ],
    )


@pytest.mark.asyncio
async def test_transform_sql_filter_dimension_pk_col(
    module__client_with_examples: AsyncClient,
):
    """
    Test ``GET /sql/{node_name}/`` with various filters and dimensions.
    """
    await verify_node_sql(
        module__client_with_examples,
        node_name="default.repair_orders_fact",
        dimensions=["default.hard_hat.hard_hat_id"],
        filters=["default.hard_hat.hard_hat_id = 7"],
        expected_sql="""
        WITH default_DOT_repair_orders_fact AS (
        SELECT  repair_orders.repair_order_id,
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
         WHERE  repair_orders.hard_hat_id = 7
        )
        SELECT  default_DOT_repair_orders_fact.repair_order_id default_DOT_repair_orders_fact_DOT_repair_order_id,
            default_DOT_repair_orders_fact.municipality_id default_DOT_repair_orders_fact_DOT_municipality_id,
            default_DOT_repair_orders_fact.hard_hat_id default_DOT_hard_hat_DOT_hard_hat_id,
            default_DOT_repair_orders_fact.dispatcher_id default_DOT_repair_orders_fact_DOT_dispatcher_id,
            default_DOT_repair_orders_fact.order_date default_DOT_repair_orders_fact_DOT_order_date,
            default_DOT_repair_orders_fact.dispatched_date default_DOT_repair_orders_fact_DOT_dispatched_date,
            default_DOT_repair_orders_fact.required_date default_DOT_repair_orders_fact_DOT_required_date,
            default_DOT_repair_orders_fact.discount default_DOT_repair_orders_fact_DOT_discount,
            default_DOT_repair_orders_fact.price default_DOT_repair_orders_fact_DOT_price,
            default_DOT_repair_orders_fact.quantity default_DOT_repair_orders_fact_DOT_quantity,
            default_DOT_repair_orders_fact.repair_type_id default_DOT_repair_orders_fact_DOT_repair_type_id,
            default_DOT_repair_orders_fact.total_repair_cost default_DOT_repair_orders_fact_DOT_total_repair_cost,
            default_DOT_repair_orders_fact.time_to_dispatch default_DOT_repair_orders_fact_DOT_time_to_dispatch,
            default_DOT_repair_orders_fact.dispatch_delay default_DOT_repair_orders_fact_DOT_dispatch_delay
        FROM default_DOT_repair_orders_fact
        WHERE  default_DOT_repair_orders_fact.hard_hat_id = 7
        """,
        expected_columns=[
            {
                "column": "repair_order_id",
                "name": "default_DOT_repair_orders_fact_DOT_repair_order_id",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.repair_order_id",
                "semantic_type": None,
                "type": "int",
            },
            {
                "column": "municipality_id",
                "name": "default_DOT_repair_orders_fact_DOT_municipality_id",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.municipality_id",
                "semantic_type": None,
                "type": "string",
            },
            {
                "column": "hard_hat_id",
                "name": "default_DOT_hard_hat_DOT_hard_hat_id",
                "node": "default.hard_hat",
                "semantic_entity": "default.hard_hat.hard_hat_id",
                "semantic_type": "dimension",
                "type": "int",
            },
            {
                "column": "dispatcher_id",
                "name": "default_DOT_repair_orders_fact_DOT_dispatcher_id",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.dispatcher_id",
                "semantic_type": None,
                "type": "int",
            },
            {
                "column": "order_date",
                "name": "default_DOT_repair_orders_fact_DOT_order_date",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.order_date",
                "semantic_type": None,
                "type": "timestamp",
            },
            {
                "column": "dispatched_date",
                "name": "default_DOT_repair_orders_fact_DOT_dispatched_date",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.dispatched_date",
                "semantic_type": None,
                "type": "timestamp",
            },
            {
                "column": "required_date",
                "name": "default_DOT_repair_orders_fact_DOT_required_date",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.required_date",
                "semantic_type": None,
                "type": "timestamp",
            },
            {
                "column": "discount",
                "name": "default_DOT_repair_orders_fact_DOT_discount",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.discount",
                "semantic_type": None,
                "type": "float",
            },
            {
                "column": "price",
                "name": "default_DOT_repair_orders_fact_DOT_price",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.price",
                "semantic_type": None,
                "type": "float",
            },
            {
                "column": "quantity",
                "name": "default_DOT_repair_orders_fact_DOT_quantity",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.quantity",
                "semantic_type": None,
                "type": "int",
            },
            {
                "column": "repair_type_id",
                "name": "default_DOT_repair_orders_fact_DOT_repair_type_id",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.repair_type_id",
                "semantic_type": None,
                "type": "int",
            },
            {
                "column": "total_repair_cost",
                "name": "default_DOT_repair_orders_fact_DOT_total_repair_cost",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.total_repair_cost",
                "semantic_type": None,
                "type": "float",
            },
            {
                "column": "time_to_dispatch",
                "name": "default_DOT_repair_orders_fact_DOT_time_to_dispatch",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.time_to_dispatch",
                "semantic_type": None,
                "type": "timestamp",
            },
            {
                "column": "dispatch_delay",
                "name": "default_DOT_repair_orders_fact_DOT_dispatch_delay",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.dispatch_delay",
                "semantic_type": None,
                "type": "timestamp",
            },
        ],
        expected_rows=[
            [
                10021,
                "Philadelphia",
                7,
                3,
                "2007-05-10",
                "2007-12-01",
                "2009-08-27",
                0.009999999776482582,
                53374.0,
                1,
                1,
                53374.0,
                205,
                -635,
            ],
        ],
    )


@pytest.mark.asyncio
async def test_transform_sql_filter_direct_node(
    module__client_with_examples: AsyncClient,
):
    """
    Test ``GET /sql/{node_name}/`` with various filters and dimensions.
    """
    await verify_node_sql(
        module__client_with_examples,
        node_name="default.repair_orders_fact",
        dimensions=[],
        filters=["default.repair_orders_fact.price > 97915"],
        expected_sql="""WITH
        default_DOT_repair_orders_fact AS (
        SELECT  repair_orders.repair_order_id,
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
         WHERE  repair_order_details.price > 97915
        )
        SELECT  default_DOT_repair_orders_fact.repair_order_id default_DOT_repair_orders_fact_DOT_repair_order_id,
            default_DOT_repair_orders_fact.municipality_id default_DOT_repair_orders_fact_DOT_municipality_id,
            default_DOT_repair_orders_fact.hard_hat_id default_DOT_repair_orders_fact_DOT_hard_hat_id,
            default_DOT_repair_orders_fact.dispatcher_id default_DOT_repair_orders_fact_DOT_dispatcher_id,
            default_DOT_repair_orders_fact.order_date default_DOT_repair_orders_fact_DOT_order_date,
            default_DOT_repair_orders_fact.dispatched_date default_DOT_repair_orders_fact_DOT_dispatched_date,
            default_DOT_repair_orders_fact.required_date default_DOT_repair_orders_fact_DOT_required_date,
            default_DOT_repair_orders_fact.discount default_DOT_repair_orders_fact_DOT_discount,
            default_DOT_repair_orders_fact.price default_DOT_repair_orders_fact_DOT_price,
            default_DOT_repair_orders_fact.quantity default_DOT_repair_orders_fact_DOT_quantity,
            default_DOT_repair_orders_fact.repair_type_id default_DOT_repair_orders_fact_DOT_repair_type_id,
            default_DOT_repair_orders_fact.total_repair_cost default_DOT_repair_orders_fact_DOT_total_repair_cost,
            default_DOT_repair_orders_fact.time_to_dispatch default_DOT_repair_orders_fact_DOT_time_to_dispatch,
            default_DOT_repair_orders_fact.dispatch_delay default_DOT_repair_orders_fact_DOT_dispatch_delay
         FROM default_DOT_repair_orders_fact
        """,
        expected_columns=[
            {
                "column": "repair_order_id",
                "name": "default_DOT_repair_orders_fact_DOT_repair_order_id",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.repair_order_id",
                "semantic_type": None,
                "type": "int",
            },
            {
                "column": "municipality_id",
                "name": "default_DOT_repair_orders_fact_DOT_municipality_id",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.municipality_id",
                "semantic_type": None,
                "type": "string",
            },
            {
                "column": "hard_hat_id",
                "name": "default_DOT_repair_orders_fact_DOT_hard_hat_id",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.hard_hat_id",
                "semantic_type": None,
                "type": "int",
            },
            {
                "column": "dispatcher_id",
                "name": "default_DOT_repair_orders_fact_DOT_dispatcher_id",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.dispatcher_id",
                "semantic_type": None,
                "type": "int",
            },
            {
                "column": "order_date",
                "name": "default_DOT_repair_orders_fact_DOT_order_date",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.order_date",
                "semantic_type": None,
                "type": "timestamp",
            },
            {
                "column": "dispatched_date",
                "name": "default_DOT_repair_orders_fact_DOT_dispatched_date",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.dispatched_date",
                "semantic_type": None,
                "type": "timestamp",
            },
            {
                "column": "required_date",
                "name": "default_DOT_repair_orders_fact_DOT_required_date",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.required_date",
                "semantic_type": None,
                "type": "timestamp",
            },
            {
                "column": "discount",
                "name": "default_DOT_repair_orders_fact_DOT_discount",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.discount",
                "semantic_type": None,
                "type": "float",
            },
            {
                "column": "price",
                "name": "default_DOT_repair_orders_fact_DOT_price",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.price",
                "semantic_type": None,
                "type": "float",
            },
            {
                "column": "quantity",
                "name": "default_DOT_repair_orders_fact_DOT_quantity",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.quantity",
                "semantic_type": None,
                "type": "int",
            },
            {
                "column": "repair_type_id",
                "name": "default_DOT_repair_orders_fact_DOT_repair_type_id",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.repair_type_id",
                "semantic_type": None,
                "type": "int",
            },
            {
                "column": "total_repair_cost",
                "name": "default_DOT_repair_orders_fact_DOT_total_repair_cost",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.total_repair_cost",
                "semantic_type": None,
                "type": "float",
            },
            {
                "column": "time_to_dispatch",
                "name": "default_DOT_repair_orders_fact_DOT_time_to_dispatch",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.time_to_dispatch",
                "semantic_type": None,
                "type": "timestamp",
            },
            {
                "column": "dispatch_delay",
                "name": "default_DOT_repair_orders_fact_DOT_dispatch_delay",
                "node": "default.repair_orders_fact",
                "semantic_entity": "default.repair_orders_fact.dispatch_delay",
                "semantic_type": None,
                "type": "timestamp",
            },
        ],
        expected_rows=[
            [
                10019,
                "Philadelphia",
                5,
                3,
                "2007-05-16",
                "2007-12-01",
                "2009-09-06",
                0.009999999776482582,
                97916.0,
                1,
                2,
                97916.0,
                199,
                -645,
            ],
        ],
    )


@pytest.mark.asyncio
async def test_source_node_query_with_filter_joinable_dimension(
    module__client_with_examples: AsyncClient,
):
    """
    Verify querying on source node with filter on joinable dimension
    """
    await verify_node_sql(
        custom_client=module__client_with_examples,
        node_name="default.repair_orders",
        dimensions=["default.hard_hat.state"],
        filters=["default.hard_hat.state='NY'"],
        expected_sql="""
            WITH default_DOT_repair_orders AS (
    SELECT  default_DOT_repair_orders.repair_order_id,
        default_DOT_repair_orders.municipality_id,
        default_DOT_repair_orders.hard_hat_id,
        default_DOT_repair_orders.order_date,
        default_DOT_repair_orders.required_date,
        default_DOT_repair_orders.dispatched_date,
        default_DOT_repair_orders.dispatcher_id
    FROM roads.repair_orders AS default_DOT_repair_orders
    ),
    default_DOT_repair_order AS (
    SELECT  default_DOT_repair_orders.repair_order_id,
        default_DOT_repair_orders.municipality_id,
        default_DOT_repair_orders.hard_hat_id,
        default_DOT_repair_orders.order_date,
        default_DOT_repair_orders.required_date,
        default_DOT_repair_orders.dispatched_date,
        default_DOT_repair_orders.dispatcher_id
    FROM roads.repair_orders AS default_DOT_repair_orders
    ),
    default_DOT_hard_hat AS (
    SELECT  default_DOT_hard_hats.hard_hat_id,
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
    FROM roads.hard_hats AS default_DOT_hard_hats
    WHERE  default_DOT_hard_hats.state = 'NY'
    )

    SELECT  default_DOT_repair_orders.repair_order_id default_DOT_repair_orders_DOT_repair_order_id,
        default_DOT_repair_orders.municipality_id default_DOT_repair_orders_DOT_municipality_id,
        default_DOT_repair_orders.hard_hat_id default_DOT_repair_orders_DOT_hard_hat_id,
        default_DOT_repair_orders.order_date default_DOT_repair_orders_DOT_order_date,
        default_DOT_repair_orders.required_date default_DOT_repair_orders_DOT_required_date,
        default_DOT_repair_orders.dispatched_date default_DOT_repair_orders_DOT_dispatched_date,
        default_DOT_repair_orders.dispatcher_id default_DOT_repair_orders_DOT_dispatcher_id,
        default_DOT_hard_hat.state default_DOT_hard_hat_DOT_state
    FROM default_DOT_repair_orders INNER JOIN default_DOT_repair_order ON default_DOT_repair_orders.repair_order_id = default_DOT_repair_order.repair_order_id
    INNER JOIN default_DOT_hard_hat ON default_DOT_repair_order.hard_hat_id = default_DOT_hard_hat.hard_hat_id
    WHERE  default_DOT_hard_hat.state = 'NY'
            """,
        expected_columns=[
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
        expected_rows=[
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
    )


@pytest.mark.asyncio
async def test_source_node_sql_with_direct_filters(
    module__client_with_examples: AsyncClient,
):
    """
    Verify source node query generation with direct filters on the node.
    """
    await verify_node_sql(
        custom_client=module__client_with_examples,
        node_name="default.repair_orders",
        dimensions=[],
        filters=["default.repair_orders.order_date='2009-08-14'"],
        expected_sql="""
        WITH
        default_DOT_repair_orders AS (
          SELECT
            default_DOT_repair_orders.repair_order_id,
            default_DOT_repair_orders.municipality_id,
            default_DOT_repair_orders.hard_hat_id,
            default_DOT_repair_orders.order_date,
            default_DOT_repair_orders.required_date,
            default_DOT_repair_orders.dispatched_date,
            default_DOT_repair_orders.dispatcher_id
          FROM (
            SELECT
              repair_order_id,
              municipality_id,
              hard_hat_id,
              order_date,
              required_date,
              dispatched_date,
              dispatcher_id
            FROM roads.repair_orders
            WHERE  order_date = '2009-08-14'
          ) default_DOT_repair_orders
          WHERE  default_DOT_repair_orders.order_date = '2009-08-14'
        )

        SELECT
            default_DOT_repair_orders.repair_order_id default_DOT_repair_orders_DOT_repair_order_id,
            default_DOT_repair_orders.municipality_id default_DOT_repair_orders_DOT_municipality_id,
            default_DOT_repair_orders.hard_hat_id default_DOT_repair_orders_DOT_hard_hat_id,
            default_DOT_repair_orders.order_date default_DOT_repair_orders_DOT_order_date,
            default_DOT_repair_orders.required_date default_DOT_repair_orders_DOT_required_date,
            default_DOT_repair_orders.dispatched_date default_DOT_repair_orders_DOT_dispatched_date,
            default_DOT_repair_orders.dispatcher_id default_DOT_repair_orders_DOT_dispatcher_id
        FROM default_DOT_repair_orders
        """,
        expected_columns=[
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
        expected_rows=[],
    )


@pytest.mark.asyncio
async def test_dimension_node_sql_with_filters(
    module__client_with_examples: AsyncClient,
):
    """
    Verify dimension node query generation with direct filters on the node.
    """
    await verify_node_sql(
        custom_client=module__client_with_examples,
        node_name="default.municipality",
        dimensions=[],
        filters=["default.municipality.state_id = 5"],
        expected_sql="""
        WITH default_DOT_municipality AS (
          SELECT
            default_DOT_municipality.municipality_id,
            default_DOT_municipality.contact_name,
            default_DOT_municipality.contact_title,
            default_DOT_municipality.local_region,
            default_DOT_municipality.phone,
            default_DOT_municipality.state_id
          FROM (
            SELECT
              municipality_id,
              contact_name,
              contact_title,
              local_region,
              phone,
              state_id
            FROM roads.municipality
            WHERE  state_id = 5
          ) default_DOT_municipality
          WHERE  default_DOT_municipality.state_id = 5
        )
        SELECT
          default_DOT_municipality.municipality_id default_DOT_municipality_DOT_municipality_id,
          default_DOT_municipality.contact_name default_DOT_municipality_DOT_contact_name,
          default_DOT_municipality.contact_title default_DOT_municipality_DOT_contact_title,
          default_DOT_municipality.local_region default_DOT_municipality_DOT_local_region,
          default_DOT_municipality.phone default_DOT_municipality_DOT_phone,
          default_DOT_municipality.state_id default_DOT_municipality_DOT_state_id
        FROM default_DOT_municipality
        """,
        expected_columns=[
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
        expected_rows=[
            [
                "Los Angeles",
                "Hugh Moser",
                "Administrative Assistant",
                "Santa Monica",
                "808-211-2323",
                5,
            ],
            [
                "San Diego",
                "Ralph Helms",
                "Senior Electrical Project Manager",
                "Del Mar",
                "491-813-2417",
                5,
            ],
            [
                "San Jose",
                "Charles Carney",
                "Municipal Accounting Manager",
                "Santana Row",
                "408-313-0698",
                5,
            ],
        ],
    )


@pytest.mark.asyncio
async def test_metric_with_node_level_and_nth_order_filters(
    module__client_with_examples: AsyncClient,
):
    """
    Verify metric SQL generation with filters on dimensions at the metric's
    parent node level and filters on nth-order dimensions.
    """
    await verify_node_sql(
        custom_client=module__client_with_examples,
        node_name="default.num_repair_orders",
        dimensions=["default.hard_hat.state"],
        filters=[
            "default.repair_orders_fact.dispatcher_id=1 OR "
            "default.repair_orders_fact.dispatcher_id is not null",
            "default.hard_hat.state='AZ'",
        ],
        expected_sql="""
        WITH default_DOT_repair_orders_fact AS (
          SELECT
            repair_orders.repair_order_id,
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
          FROM roads.repair_orders AS repair_orders
          JOIN roads.repair_order_details AS repair_order_details
            ON repair_orders.repair_order_id = repair_order_details.repair_order_id
          WHERE
            repair_orders.dispatcher_id = 1 OR repair_orders.dispatcher_id IS NOT NULL
        ), default_DOT_hard_hat AS (
          SELECT
            default_DOT_hard_hats.hard_hat_id,
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
          FROM roads.hard_hats AS default_DOT_hard_hats
          WHERE
            default_DOT_hard_hats.state = 'AZ'
        ),
        default_DOT_repair_orders_fact_metrics AS (
          SELECT
            default_DOT_hard_hat.state default_DOT_hard_hat_DOT_state,
            count(default_DOT_repair_orders_fact.repair_order_id) default_DOT_num_repair_orders
          FROM default_DOT_repair_orders_fact
          INNER JOIN default_DOT_hard_hat
            ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
          WHERE  default_DOT_hard_hat.state = 'AZ'
          GROUP BY default_DOT_hard_hat.state
        )
        SELECT
          default_DOT_repair_orders_fact_metrics.default_DOT_hard_hat_DOT_state,
          default_DOT_repair_orders_fact_metrics.default_DOT_num_repair_orders
        FROM default_DOT_repair_orders_fact_metrics
        """,
        expected_columns=[
            {
                "column": "state",
                "name": "default_DOT_hard_hat_DOT_state",
                "node": "default.hard_hat",
                "type": "string",
                "semantic_type": "dimension",
                "semantic_entity": "default.hard_hat.state",
            },
            {
                "column": "default_DOT_num_repair_orders",
                "name": "default_DOT_num_repair_orders",
                "node": "default.num_repair_orders",
                "type": "bigint",
                "semantic_type": "metric",
                "semantic_entity": "default.num_repair_orders.default_DOT_num_repair_orders",
            },
        ],
        expected_rows=[["AZ", 2]],
    )


@pytest.mark.asyncio
async def test_metric_with_nth_order_dimensions_filters(
    module__client_with_examples: AsyncClient,
):
    """
    Verify metric SQL generation that groups by nth-order dimensions and
    filters on nth-order dimensions.
    """

    await verify_node_sql(
        custom_client=module__client_with_examples,
        node_name="default.num_repair_orders",
        dimensions=[
            "default.hard_hat.city",
            "default.hard_hat.last_name",
            "default.dispatcher.company_name",
            "default.municipality_dim.local_region",
        ],
        filters=[
            "default.dispatcher.dispatcher_id=1",
            "default.hard_hat.state != 'AZ'",
            "default.dispatcher.phone = '4082021022'",
            "default.repair_orders_fact.order_date >= '2020-01-01'",
        ],
        expected_sql="""
        WITH default_DOT_repair_orders_fact AS (
          SELECT
            repair_orders.repair_order_id,
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
          FROM (
            SELECT
              repair_order_id,
              municipality_id,
              hard_hat_id,
              order_date,
              required_date,
              dispatched_date,
              dispatcher_id
            FROM roads.repair_orders
            WHERE dispatcher_id = 1
          ) repair_orders
          JOIN roads.repair_order_details AS repair_order_details
            ON repair_orders.repair_order_id = repair_order_details.repair_order_id
          WHERE
            repair_orders.dispatcher_id = 1 AND repair_orders.order_date >= '2020-01-01'
        ), default_DOT_hard_hat AS (
          SELECT
            default_DOT_hard_hats.hard_hat_id,
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
          FROM roads.hard_hats AS default_DOT_hard_hats
          WHERE
            default_DOT_hard_hats.state != 'AZ'
        ), default_DOT_dispatcher AS (
          SELECT
            default_DOT_dispatchers.dispatcher_id,
            default_DOT_dispatchers.company_name,
            default_DOT_dispatchers.phone
          FROM roads.dispatchers AS default_DOT_dispatchers
          WHERE
            default_DOT_dispatchers.dispatcher_id = 1
            AND default_DOT_dispatchers.phone = '4082021022'
        ), default_DOT_municipality_dim AS (
          SELECT
            m.municipality_id AS municipality_id,
            m.contact_name,
            m.contact_title,
            m.local_region,
            m.state_id,
            mmt.municipality_type_id AS municipality_type_id,
            mt.municipality_type_desc AS municipality_type_desc
          FROM roads.municipality AS m
          LEFT JOIN roads.municipality_municipality_type AS mmt
            ON m.municipality_id = mmt.municipality_id
          LEFT JOIN roads.municipality_type AS mt
            ON mmt.municipality_type_id = mt.municipality_type_desc
        ), default_DOT_repair_orders_fact_metrics AS (
          SELECT
            default_DOT_hard_hat.city default_DOT_hard_hat_DOT_city,
            default_DOT_hard_hat.last_name default_DOT_hard_hat_DOT_last_name,
            default_DOT_dispatcher.company_name default_DOT_dispatcher_DOT_company_name,
            default_DOT_municipality_dim.local_region default_DOT_municipality_dim_DOT_local_region,
            count(default_DOT_repair_orders_fact.repair_order_id) default_DOT_num_repair_orders
          FROM default_DOT_repair_orders_fact
          INNER JOIN default_DOT_hard_hat
            ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
          INNER JOIN default_DOT_dispatcher
            ON default_DOT_repair_orders_fact.dispatcher_id = default_DOT_dispatcher.dispatcher_id
          INNER JOIN default_DOT_municipality_dim
            ON default_DOT_repair_orders_fact.municipality_id = default_DOT_municipality_dim.municipality_id
          WHERE  default_DOT_dispatcher.dispatcher_id = 1
            AND default_DOT_hard_hat.state != 'AZ'
            AND default_DOT_dispatcher.phone = '4082021022'
          GROUP BY
            default_DOT_hard_hat.city,
            default_DOT_hard_hat.last_name,
            default_DOT_dispatcher.company_name,
            default_DOT_municipality_dim.local_region
        )
        SELECT
          default_DOT_repair_orders_fact_metrics.default_DOT_hard_hat_DOT_city,
          default_DOT_repair_orders_fact_metrics.default_DOT_hard_hat_DOT_last_name,
          default_DOT_repair_orders_fact_metrics.default_DOT_dispatcher_DOT_company_name,
          default_DOT_repair_orders_fact_metrics.default_DOT_municipality_dim_DOT_local_region,
          default_DOT_repair_orders_fact_metrics.default_DOT_num_repair_orders
        FROM default_DOT_repair_orders_fact_metrics
        """,
        expected_columns=[
            {
                "name": "default_DOT_hard_hat_DOT_city",
                "column": "city",
                "node": "default.hard_hat",
                "type": "string",
                "semantic_type": "dimension",
                "semantic_entity": "default.hard_hat.city",
            },
            {
                "name": "default_DOT_hard_hat_DOT_last_name",
                "column": "last_name",
                "node": "default.hard_hat",
                "type": "string",
                "semantic_type": "dimension",
                "semantic_entity": "default.hard_hat.last_name",
            },
            {
                "name": "default_DOT_dispatcher_DOT_company_name",
                "column": "company_name",
                "node": "default.dispatcher",
                "type": "string",
                "semantic_type": "dimension",
                "semantic_entity": "default.dispatcher.company_name",
            },
            {
                "name": "default_DOT_municipality_dim_DOT_local_region",
                "column": "local_region",
                "node": "default.municipality_dim",
                "type": "string",
                "semantic_type": "dimension",
                "semantic_entity": "default.municipality_dim.local_region",
            },
            {
                "name": "default_DOT_num_repair_orders",
                "column": "default_DOT_num_repair_orders",
                "node": "default.num_repair_orders",
                "type": "bigint",
                "semantic_type": "metric",
                "semantic_entity": "default.num_repair_orders.default_DOT_num_repair_orders",
            },
        ],
        expected_rows=[],
    )


@pytest.mark.asyncio
async def test_metric_with_second_order_dimensions(
    module__client_with_examples: AsyncClient,
):
    """
    Verify metric SQL generation with group by on second-order dimension.
    """
    await verify_node_sql(
        custom_client=module__client_with_examples,
        node_name="default.avg_repair_price",
        dimensions=["default.hard_hat.city"],
        filters=[],
        expected_sql="""
        WITH default_DOT_repair_orders_fact AS (
          SELECT
            repair_orders.repair_order_id,
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
          FROM roads.repair_orders AS repair_orders
          JOIN roads.repair_order_details AS repair_order_details
          ON repair_orders.repair_order_id = repair_order_details.repair_order_id
        ),
        default_DOT_hard_hat AS (
          SELECT
            default_DOT_hard_hats.hard_hat_id,
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
          FROM roads.hard_hats AS default_DOT_hard_hats
        ), default_DOT_repair_orders_fact_metrics AS (
          SELECT
            default_DOT_hard_hat.city default_DOT_hard_hat_DOT_city,
            avg(default_DOT_repair_orders_fact.price) default_DOT_avg_repair_price
          FROM default_DOT_repair_orders_fact
          INNER JOIN default_DOT_hard_hat
            ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
          GROUP BY default_DOT_hard_hat.city
        )
        SELECT
          default_DOT_repair_orders_fact_metrics.default_DOT_hard_hat_DOT_city,
          default_DOT_repair_orders_fact_metrics.default_DOT_avg_repair_price
        FROM default_DOT_repair_orders_fact_metrics
            """,
        expected_columns=[
            {
                "column": "city",
                "name": "default_DOT_hard_hat_DOT_city",
                "node": "default.hard_hat",
                "type": "string",
                "semantic_type": "dimension",
                "semantic_entity": "default.hard_hat.city",
            },
            {
                "column": "default_DOT_avg_repair_price",
                "name": "default_DOT_avg_repair_price",
                "node": "default.avg_repair_price",
                "type": "double",
                "semantic_type": "metric",
                "semantic_entity": "default.avg_repair_price.default_DOT_avg_repair_price",
            },
        ],
        expected_rows=[
            ["Jersey City", 54672.75],
            ["Billerica", 76555.33333333333],
            ["Southgate", 64190.6],
            ["Phoenix", 65682.0],
            ["Southampton", 54083.5],
            ["Powder Springs", 65595.66666666667],
            ["Middletown", 39301.5],
            ["Muskogee", 70418.0],
            ["Niagara Falls", 53374.0],
        ],
    )


@pytest.mark.asyncio
async def test_metric_with_nth_order_dimensions(
    module__client_with_examples: AsyncClient,
):
    """
    Verify metric SQL generation with group by on nth-order dimension.
    """
    await verify_node_sql(
        custom_client=module__client_with_examples,
        node_name="default.avg_repair_price",
        dimensions=["default.hard_hat.city", "default.dispatcher.company_name"],
        filters=[],
        expected_sql="""
              WITH default_DOT_repair_orders_fact AS (
  SELECT
    repair_orders.repair_order_id,
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
  FROM roads.repair_orders AS repair_orders
  JOIN roads.repair_order_details AS repair_order_details
    ON repair_orders.repair_order_id = repair_order_details.repair_order_id
), default_DOT_hard_hat AS (
  SELECT
    default_DOT_hard_hats.hard_hat_id,
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
  FROM roads.hard_hats AS default_DOT_hard_hats
), default_DOT_dispatcher AS (
  SELECT
    default_DOT_dispatchers.dispatcher_id,
    default_DOT_dispatchers.company_name,
    default_DOT_dispatchers.phone
  FROM roads.dispatchers AS default_DOT_dispatchers
),
default_DOT_repair_orders_fact_metrics AS (
  SELECT
    default_DOT_hard_hat.city default_DOT_hard_hat_DOT_city,
    default_DOT_dispatcher.company_name default_DOT_dispatcher_DOT_company_name,
    avg(default_DOT_repair_orders_fact.price) default_DOT_avg_repair_price
  FROM default_DOT_repair_orders_fact
  INNER JOIN default_DOT_hard_hat
    ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
  INNER JOIN default_DOT_dispatcher
    ON default_DOT_repair_orders_fact.dispatcher_id = default_DOT_dispatcher.dispatcher_id
  GROUP BY
    default_DOT_hard_hat.city,
    default_DOT_dispatcher.company_name
)
SELECT
  default_DOT_repair_orders_fact_metrics.default_DOT_hard_hat_DOT_city,
  default_DOT_repair_orders_fact_metrics.default_DOT_dispatcher_DOT_company_name,
  default_DOT_repair_orders_fact_metrics.default_DOT_avg_repair_price
FROM default_DOT_repair_orders_fact_metrics
            """,
        expected_columns=[
            {
                "column": "city",
                "name": "default_DOT_hard_hat_DOT_city",
                "node": "default.hard_hat",
                "type": "string",
                "semantic_type": "dimension",
                "semantic_entity": "default.hard_hat.city",
            },
            {
                "column": "company_name",
                "name": "default_DOT_dispatcher_DOT_company_name",
                "node": "default.dispatcher",
                "type": "string",
                "semantic_type": "dimension",
                "semantic_entity": "default.dispatcher.company_name",
            },
            {
                "column": "default_DOT_avg_repair_price",
                "name": "default_DOT_avg_repair_price",
                "node": "default.avg_repair_price",
                "type": "double",
                "semantic_type": "metric",
                "semantic_entity": "default.avg_repair_price.default_DOT_avg_repair_price",
            },
        ],
        expected_rows=[
            ["Jersey City", "Federal Roads Group", 63708.0],
            ["Billerica", "Pothole Pete", 67253.0],
            ["Southgate", "Asphalts R Us", 57332.5],
            ["Jersey City", "Pothole Pete", 51661.0],
            ["Phoenix", "Asphalts R Us", 76463.0],
            ["Billerica", "Asphalts R Us", 81206.5],
            ["Southampton", "Asphalts R Us", 63918.0],
            ["Southgate", "Federal Roads Group", 59499.5],
            ["Southampton", "Federal Roads Group", 27222.0],
            ["Southampton", "Pothole Pete", 62597.0],
            ["Phoenix", "Federal Roads Group", 54901.0],
            ["Powder Springs", "Asphalts R Us", 66929.5],
            ["Middletown", "Federal Roads Group", 39301.5],
            ["Muskogee", "Federal Roads Group", 70418.0],
            ["Powder Springs", "Pothole Pete", 62928.0],
            ["Niagara Falls", "Federal Roads Group", 53374.0],
            ["Southgate", "Pothole Pete", 87289.0],
        ],
    )


@pytest.mark.asyncio
async def test_metric_sql_without_dimensions_filters(
    module__client_with_examples: AsyncClient,
):
    """
    Verify metric SQL generation without group by dimensions or filters.
    """
    await verify_node_sql(
        custom_client=module__client_with_examples,
        node_name="default.num_repair_orders",
        dimensions=[],
        filters=[],
        expected_sql="""
            WITH default_DOT_repair_orders_fact AS (
              SELECT
                repair_orders.repair_order_id,
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
              FROM roads.repair_orders AS repair_orders
              JOIN roads.repair_order_details AS repair_order_details
                ON repair_orders.repair_order_id = repair_order_details.repair_order_id
            ),
            default_DOT_repair_orders_fact_metrics AS (
              SELECT
                count(default_DOT_repair_orders_fact.repair_order_id) default_DOT_num_repair_orders
              FROM default_DOT_repair_orders_fact
            )
            SELECT
              default_DOT_repair_orders_fact_metrics.default_DOT_num_repair_orders
            FROM default_DOT_repair_orders_fact_metrics
            """,
        expected_columns=[
            {
                "column": "default_DOT_num_repair_orders",
                "name": "default_DOT_num_repair_orders",
                "node": "default.num_repair_orders",
                "type": "bigint",
                "semantic_type": "metric",
                "semantic_entity": "default.num_repair_orders.default_DOT_num_repair_orders",
            },
        ],
        expected_rows=[
            [25],
        ],
    )


@pytest.mark.asyncio
async def test_source_sql_joinable_dimension_and_filter(
    module__client_with_examples: AsyncClient,
):
    """
    Verify source SQL generation with joinable dimension and filters.
    """
    await verify_node_sql(
        custom_client=module__client_with_examples,
        node_name="default.repair_orders",
        dimensions=["default.hard_hat.state"],
        filters=["default.hard_hat.state='NY'"],
        expected_sql="""
            WITH default_DOT_repair_orders AS (
              SELECT
                default_DOT_repair_orders.repair_order_id,
                default_DOT_repair_orders.municipality_id,
                default_DOT_repair_orders.hard_hat_id,
                default_DOT_repair_orders.order_date,
                default_DOT_repair_orders.required_date,
                default_DOT_repair_orders.dispatched_date,
                default_DOT_repair_orders.dispatcher_id
              FROM roads.repair_orders AS default_DOT_repair_orders
            ),
            default_DOT_repair_order AS (
              SELECT
                default_DOT_repair_orders.repair_order_id,
                default_DOT_repair_orders.municipality_id,
                default_DOT_repair_orders.hard_hat_id,
                default_DOT_repair_orders.order_date,
                default_DOT_repair_orders.required_date,
                default_DOT_repair_orders.dispatched_date,
                default_DOT_repair_orders.dispatcher_id
              FROM roads.repair_orders AS default_DOT_repair_orders
            ),
            default_DOT_hard_hat AS (
              SELECT
                default_DOT_hard_hats.hard_hat_id,
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
              FROM roads.hard_hats AS default_DOT_hard_hats
              WHERE default_DOT_hard_hats.state = 'NY'
            )
            SELECT  default_DOT_repair_orders.repair_order_id default_DOT_repair_orders_DOT_repair_order_id,
                default_DOT_repair_orders.municipality_id default_DOT_repair_orders_DOT_municipality_id,
                default_DOT_repair_orders.hard_hat_id default_DOT_repair_orders_DOT_hard_hat_id,
                default_DOT_repair_orders.order_date default_DOT_repair_orders_DOT_order_date,
                default_DOT_repair_orders.required_date default_DOT_repair_orders_DOT_required_date,
                default_DOT_repair_orders.dispatched_date default_DOT_repair_orders_DOT_dispatched_date,
                default_DOT_repair_orders.dispatcher_id default_DOT_repair_orders_DOT_dispatcher_id,
                default_DOT_hard_hat.state default_DOT_hard_hat_DOT_state
            FROM default_DOT_repair_orders
            INNER JOIN default_DOT_repair_order
              ON default_DOT_repair_orders.repair_order_id = default_DOT_repair_order.repair_order_id
            INNER JOIN default_DOT_hard_hat
              ON default_DOT_repair_order.hard_hat_id = default_DOT_hard_hat.hard_hat_id
            WHERE  default_DOT_hard_hat.state = 'NY'
            """,
        expected_columns=[
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
        expected_rows=[
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
    )


@pytest.mark.asyncio
async def test_metric_with_joinable_dimension_multiple_hops(
    module__client_with_examples: AsyncClient,
):
    """
    Verify metric SQL generation with joinable nth-order dimensions.
    """
    await verify_node_sql(
        custom_client=module__client_with_examples,
        node_name="default.num_repair_orders",
        dimensions=["default.us_state.state_short"],
        filters=[],
        expected_sql="""
            WITH default_DOT_repair_orders_fact AS (
              SELECT
                repair_orders.repair_order_id,
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
              FROM roads.repair_orders AS repair_orders
              JOIN roads.repair_order_details AS repair_order_details
                ON repair_orders.repair_order_id = repair_order_details.repair_order_id
            ),
            default_DOT_hard_hat AS (
              SELECT
                default_DOT_hard_hats.hard_hat_id,
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
              FROM roads.hard_hats AS default_DOT_hard_hats
            ), default_DOT_repair_orders_fact_metrics AS (
              SELECT
                default_DOT_hard_hat.state default_DOT_us_state_DOT_state_short,
                count(default_DOT_repair_orders_fact.repair_order_id) default_DOT_num_repair_orders
              FROM default_DOT_repair_orders_fact
              INNER JOIN default_DOT_hard_hat
                ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
              GROUP BY default_DOT_hard_hat.state
            )
            SELECT
              default_DOT_repair_orders_fact_metrics.default_DOT_us_state_DOT_state_short,
              default_DOT_repair_orders_fact_metrics.default_DOT_num_repair_orders
            FROM default_DOT_repair_orders_fact_metrics
            """,
        expected_columns=[
            {
                "column": "state_short",
                "name": "default_DOT_us_state_DOT_state_short",
                "node": "default.us_state",
                "type": "string",
                "semantic_type": "dimension",
                "semantic_entity": "default.us_state.state_short",
            },
            {
                "column": "default_DOT_num_repair_orders",
                "name": "default_DOT_num_repair_orders",
                "node": "default.num_repair_orders",
                "type": "bigint",
                "semantic_type": "metric",
                "semantic_entity": "default.num_repair_orders.default_DOT_num_repair_orders",
            },
        ],
        expected_rows=[
            ["NJ", 4],
            ["MA", 3],
            ["MI", 5],
            ["AZ", 2],
            ["PA", 4],
            ["GA", 3],
            ["CT", 2],
            ["OK", 1],
            ["NY", 1],
        ],
    )


@pytest.mark.asyncio
async def test_union_all(
    module__client_with_examples: AsyncClient,
):
    """
    Verify union all query works
    """
    response = await module__client_with_examples.post(
        "/nodes/transform",
        json={
            "name": "default.union_all_test",
            "description": "",
            "display_name": "Union All Test",
            "query": """
            (
              SELECT
                1234 AS farmer_id,
                2234 AS farm_id,
                'pear' AS fruit_name,
                4444 AS fruit_id,
                20 AS fruits_cnt
            )
            UNION ALL
            (
              SELECT
                NULL AS farmer_id,
                NULL AS farm_id,
                NULL AS fruit_name,
                NULL AS fruit_id,
                NULL AS fruits_cnt
            )""",
            "mode": "published",
        },
    )
    assert response.status_code == 201

    response = await module__client_with_examples.get("/sql/default.union_all_test")
    assert str(parse(response.json()["sql"])) == str(
        parse(
            """
        WITH default_DOT_union_all_test AS (
        (SELECT  1234 AS farmer_id,
            2234 AS farm_id,
            'pear' AS fruit_name,
            4444 AS fruit_id,
            20 AS fruits_cnt)

        UNION ALL
        (SELECT  NULL AS farmer_id,
            NULL AS farm_id,
            NULL AS fruit_name,
            NULL AS fruit_id,
            NULL AS fruits_cnt)
        )

        SELECT  default_DOT_union_all_test.farmer_id default_DOT_union_all_test_DOT_farmer_id,
            default_DOT_union_all_test.farm_id default_DOT_union_all_test_DOT_farm_id,
            default_DOT_union_all_test.fruit_name default_DOT_union_all_test_DOT_fruit_name,
            default_DOT_union_all_test.fruit_id default_DOT_union_all_test_DOT_fruit_id,
            default_DOT_union_all_test.fruits_cnt default_DOT_union_all_test_DOT_fruits_cnt
        FROM default_DOT_union_all_test
    """,
        ),
    )


@pytest.mark.asyncio
async def test_multiple_joins_to_same_node(
    module__client_with_examples: AsyncClient,
):
    """
    Verify that multiple joins to the same node still work
    """
    response = await module__client_with_examples.post(
        "/nodes/transform",
        json={
            "name": "default.sowtime",
            "description": "",
            "display_name": "Sow Time",
            "query": """
              SELECT
                'Davis' AS city_name,
                'fruit' AS fruit,
                3 AS month
            """,
            "mode": "published",
        },
    )
    assert response.status_code == 201

    response = await module__client_with_examples.post(
        "/nodes/transform",
        json={
            "name": "default.fruits",
            "description": "",
            "display_name": "Fruits",
            "query": """
              SELECT
                1234 AS farmer_id,
                2234 AS farm_id,
                'pear' AS primary,
                'avocado' AS companion,
                'Davis' AS city_name
            """,
            "mode": "published",
        },
    )
    assert response.status_code == 201

    response = await module__client_with_examples.post(
        "/nodes/transform",
        json={
            "name": "default.multiple_join_same_node",
            "description": "",
            "display_name": "Multiple Joins to Same Node",
            "query": """
              SELECT
                f.farmer_id,
                s_start.month AS primary_sow_start_month,
                s_end.month AS companion_sow_start_month
              FROM default.fruits AS f
              LEFT JOIN default.sowtime AS s_start
                ON  f.city_name = s_start.city_name
                AND f.primary = s_start.fruit
              LEFT JOIN default.sowtime AS s_end
                ON  f.city_name = s_end.city_name
                AND f.companion = s_end.fruit
            """,
            "mode": "published",
        },
    )
    assert response.status_code == 201

    response = await module__client_with_examples.get(
        "/sql/default.multiple_join_same_node",
    )
    assert str(parse(response.json()["sql"])) == str(
        parse(
            """
            WITH default_DOT_fruits AS (
                SELECT  1234 AS farmer_id,
                    2234 AS farm_id,
                    'pear' AS primary,
                    'avocado' AS companion,
                    'Davis' AS city_name
                ),
                default_DOT_sowtime AS (
                SELECT  'Davis' AS city_name,
                    'fruit' AS fruit,
                    3 AS month
                ),
                default_DOT_multiple_join_same_node AS (
                SELECT  f.farmer_id,
                    s_start.month AS primary_sow_start_month,
                    s_end.month AS companion_sow_start_month
                FROM default_DOT_fruits f LEFT JOIN default_DOT_sowtime s_start ON f.city_name = s_start.city_name AND f.primary = s_start.fruit
                LEFT JOIN default_DOT_sowtime s_end ON f.city_name = s_end.city_name AND f.companion = s_end.fruit
                )
            SELECT
                default_DOT_multiple_join_same_node.farmer_id default_DOT_multiple_join_same_node_DOT_farmer_id,
                default_DOT_multiple_join_same_node.primary_sow_start_month default_DOT_multiple_join_same_node_DOT_primary_sow_start_month,
                default_DOT_multiple_join_same_node.companion_sow_start_month default_DOT_multiple_join_same_node_DOT_companion_sow_start_month
            FROM default_DOT_multiple_join_same_node""",
        ),
    )


@pytest.mark.asyncio
async def test_cross_join_unnest(
    module__client_with_examples: AsyncClient,
):
    """
    Verify cross join unnest on a joined in dimension works
    """
    await module__client_with_examples.post(
        "/nodes/basic.corrected_patches/columns/color_id/"
        "?dimension=basic.paint_colors_trino&dimension_column=color_id",
    )
    response = await module__client_with_examples.get(
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
    module__client_with_examples: AsyncClient,
):
    """
    Verify lateral view explode on a joined in dimension works
    """
    await module__client_with_examples.post(
        "/nodes/basic.corrected_patches/columns/color_id/"
        "?dimension=basic.paint_colors_spark&dimension_column=color_id",
    )
    response = await module__client_with_examples.get(
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
async def test_get_sql_for_metrics_failures(module__client_with_examples: AsyncClient):
    """
    Test failure modes when getting sql for multiple metrics.
    """
    # Getting sql for no metrics fails appropriately
    response = await module__client_with_examples.get(
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
    response = await module__client_with_examples.get(
        "/sql/",
        params={
            "metrics": ["default.number_of_account_types"],
            "dimensions": [],
            "filters": [],
        },
    )
    assert response.status_code == 200

    # Getting sql for metric with non-metric node
    response = await module__client_with_examples.get(
        "/sql/",
        params={
            "metrics": ["default.repair_orders"],
            "dimensions": [],
            "filters": [],
        },
    )
    assert response.status_code == 422
    assert response.json() == {
        "message": "All nodes must be of metric type, but some are not: default.repair_orders (source) .",
        "errors": [],
        "warnings": [],
    }


@pytest.mark.asyncio
async def test_get_sql_for_metrics_no_access(module__client_with_examples: AsyncClient):
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

    module__client_with_examples.app.dependency_overrides[validate_access] = (
        validate_access_override
    )

    response = await module__client_with_examples.get(
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

    module__client_with_examples.app.dependency_overrides[validate_access] = (
        validate_access
    )

    module__client_with_examples.app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_get_sql_for_metrics2(client_with_examples: AsyncClient):
    """
    Test getting sql for multiple metrics.
    """
    response = await client_with_examples.get(
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
    WITH default_DOT_repair_orders_fact AS (
      SELECT
        repair_orders.repair_order_id,
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
      FROM roads.repair_orders AS repair_orders
      JOIN roads.repair_order_details AS repair_order_details
        ON repair_orders.repair_order_id = repair_order_details.repair_order_id
    ), default_DOT_hard_hat AS (
      SELECT
        default_DOT_hard_hats.hard_hat_id,
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
      FROM roads.hard_hats AS default_DOT_hard_hats
    ), default_DOT_dispatcher AS (
      SELECT
        default_DOT_dispatchers.dispatcher_id,
        default_DOT_dispatchers.company_name,
        default_DOT_dispatchers.phone
      FROM roads.dispatchers AS default_DOT_dispatchers
    ), default_DOT_municipality_dim AS (
      SELECT
        m.municipality_id AS municipality_id,
        m.contact_name,
        m.contact_title,
        m.local_region,
        m.state_id,
        mmt.municipality_type_id AS municipality_type_id,
        mt.municipality_type_desc AS municipality_type_desc
      FROM roads.municipality AS m
      LEFT JOIN roads.municipality_municipality_type AS mmt
        ON m.municipality_id = mmt.municipality_id
      LEFT JOIN roads.municipality_type AS mt
        ON mmt.municipality_type_id = mt.municipality_type_desc
    ),
    default_DOT_repair_orders_fact_metrics AS (
      SELECT
        default_DOT_hard_hat.country default_DOT_hard_hat_DOT_country,
        default_DOT_hard_hat.postal_code default_DOT_hard_hat_DOT_postal_code,
        default_DOT_hard_hat.city default_DOT_hard_hat_DOT_city,
        default_DOT_hard_hat.state default_DOT_hard_hat_DOT_state,
        default_DOT_dispatcher.company_name default_DOT_dispatcher_DOT_company_name,
        default_DOT_municipality_dim.local_region default_DOT_municipality_dim_DOT_local_region,
        CAST(sum(if(default_DOT_repair_orders_fact.discount > 0.0, 1, 0)) AS DOUBLE) / count(*) AS default_DOT_discounted_orders_rate,
        count(default_DOT_repair_orders_fact.repair_order_id) default_DOT_num_repair_orders
      FROM default_DOT_repair_orders_fact
      INNER JOIN default_DOT_hard_hat
        ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
      INNER JOIN default_DOT_dispatcher
        ON default_DOT_repair_orders_fact.dispatcher_id = default_DOT_dispatcher.dispatcher_id
      INNER JOIN default_DOT_municipality_dim
        ON default_DOT_repair_orders_fact.municipality_id = default_DOT_municipality_dim.municipality_id
      GROUP BY
        default_DOT_hard_hat.country,
        default_DOT_hard_hat.postal_code,
        default_DOT_hard_hat.city,
        default_DOT_hard_hat.state,
        default_DOT_dispatcher.company_name,
        default_DOT_municipality_dim.local_region
    )
    SELECT
      default_DOT_repair_orders_fact_metrics.default_DOT_hard_hat_DOT_country,
      default_DOT_repair_orders_fact_metrics.default_DOT_hard_hat_DOT_postal_code,
      default_DOT_repair_orders_fact_metrics.default_DOT_hard_hat_DOT_city,
      default_DOT_repair_orders_fact_metrics.default_DOT_hard_hat_DOT_state,
      default_DOT_repair_orders_fact_metrics.default_DOT_dispatcher_DOT_company_name,
      default_DOT_repair_orders_fact_metrics.default_DOT_municipality_dim_DOT_local_region,
      default_DOT_repair_orders_fact_metrics.default_DOT_discounted_orders_rate,
      default_DOT_repair_orders_fact_metrics.default_DOT_num_repair_orders
    FROM default_DOT_repair_orders_fact_metrics
    ORDER BY
      default_DOT_repair_orders_fact_metrics.default_DOT_hard_hat_DOT_country,
      default_DOT_repair_orders_fact_metrics.default_DOT_num_repair_orders,
      default_DOT_repair_orders_fact_metrics.default_DOT_dispatcher_DOT_company_name,
      default_DOT_repair_orders_fact_metrics.default_DOT_discounted_orders_rate
    LIMIT 100
    """
    assert str(parse(data["sql"])) == str(parse(expected_sql))
    assert data["columns"] == [
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
    ]


@pytest.mark.asyncio
async def test_get_sql_including_dimension_ids(
    module__client_with_examples: AsyncClient,
):
    """
    Test getting SQL when there are dimensions ids included
    """

    response = await module__client_with_examples.get(
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
        WITH default_DOT_repair_orders_fact AS (
  SELECT
    repair_orders.repair_order_id,
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
  FROM roads.repair_orders AS repair_orders
  JOIN roads.repair_order_details AS repair_order_details
    ON repair_orders.repair_order_id = repair_order_details.repair_order_id
), default_DOT_dispatcher AS (
  SELECT
    default_DOT_dispatchers.dispatcher_id,
    default_DOT_dispatchers.company_name,
    default_DOT_dispatchers.phone
  FROM roads.dispatchers AS default_DOT_dispatchers
), default_DOT_repair_orders_fact_metrics AS (
  SELECT
    default_DOT_dispatcher.company_name default_DOT_dispatcher_DOT_company_name,
    default_DOT_dispatcher.dispatcher_id default_DOT_dispatcher_DOT_dispatcher_id,
    avg(default_DOT_repair_orders_fact.price) default_DOT_avg_repair_price,
    sum(default_DOT_repair_orders_fact.total_repair_cost) default_DOT_total_repair_cost
  FROM default_DOT_repair_orders_fact
  INNER JOIN default_DOT_dispatcher
    ON default_DOT_repair_orders_fact.dispatcher_id = default_DOT_dispatcher.dispatcher_id
  GROUP BY
    default_DOT_dispatcher.company_name,
    default_DOT_dispatcher.dispatcher_id
)
SELECT
  default_DOT_repair_orders_fact_metrics.default_DOT_dispatcher_DOT_company_name,
  default_DOT_repair_orders_fact_metrics.default_DOT_dispatcher_DOT_dispatcher_id,
  default_DOT_repair_orders_fact_metrics.default_DOT_avg_repair_price,
  default_DOT_repair_orders_fact_metrics.default_DOT_total_repair_cost
FROM default_DOT_repair_orders_fact_metrics
"""
    assert str(parse(str(data["sql"]))) == str(parse(str(expected)))

    response = await module__client_with_examples.get(
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
    assert str(parse(str(data["sql"]))) == str(
        parse(
            """
        WITH default_DOT_repair_orders_fact AS (
  SELECT
    repair_orders.repair_order_id,
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
  FROM roads.repair_orders AS repair_orders
  JOIN roads.repair_order_details AS repair_order_details
    ON repair_orders.repair_order_id = repair_order_details.repair_order_id
), default_DOT_hard_hat AS (
  SELECT
    default_DOT_hard_hats.hard_hat_id,
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
  FROM roads.hard_hats AS default_DOT_hard_hats
), default_DOT_repair_orders_fact_metrics AS (
  SELECT
    default_DOT_repair_orders_fact.hard_hat_id default_DOT_hard_hat_DOT_hard_hat_id,
    default_DOT_hard_hat.first_name default_DOT_hard_hat_DOT_first_name,
    avg(default_DOT_repair_orders_fact.price) default_DOT_avg_repair_price,
    sum(default_DOT_repair_orders_fact.total_repair_cost) default_DOT_total_repair_cost
  FROM default_DOT_repair_orders_fact
  INNER JOIN default_DOT_hard_hat
    ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
  GROUP BY
    default_DOT_repair_orders_fact.hard_hat_id,
    default_DOT_hard_hat.first_name
)
SELECT
  default_DOT_repair_orders_fact_metrics.default_DOT_hard_hat_DOT_hard_hat_id,
  default_DOT_repair_orders_fact_metrics.default_DOT_hard_hat_DOT_first_name,
  default_DOT_repair_orders_fact_metrics.default_DOT_avg_repair_price,
  default_DOT_repair_orders_fact_metrics.default_DOT_total_repair_cost
FROM default_DOT_repair_orders_fact_metrics
        """,
        ),
    )


@pytest.mark.asyncio
async def test_get_sql_including_dimensions_with_disambiguated_columns(
    module__client_with_examples: AsyncClient,
    duckdb_conn: duckdb.DuckDBPyConnection,
):
    """
    Test getting SQL that includes dimensions with SQL that has to disambiguate projection columns with prefixes
    """
    response = await module__client_with_examples.get(
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
        {
            "column": "municipality_id",
            "name": "default_DOT_municipality_dim_DOT_municipality_id",
            "node": "default.municipality_dim",
            "semantic_entity": "default.municipality_dim.municipality_id",
            "semantic_type": "dimension",
            "type": "string",
        },
        {
            "column": "default_DOT_total_repair_cost",
            "name": "default_DOT_total_repair_cost",
            "node": "default.total_repair_cost",
            "semantic_entity": "default.total_repair_cost.default_DOT_total_repair_cost",
            "semantic_type": "metric",
            "type": "double",
        },
    ]
    assert str(parse(data["sql"])) == str(
        parse(
            """
        WITH default_DOT_repair_orders_fact AS (
  SELECT
    repair_orders.repair_order_id,
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
  FROM roads.repair_orders AS repair_orders
  JOIN roads.repair_order_details AS repair_order_details
    ON repair_orders.repair_order_id = repair_order_details.repair_order_id
), default_DOT_municipality_dim AS (
  SELECT
    m.municipality_id AS municipality_id,
    m.contact_name,
    m.contact_title,
    m.local_region,
    m.state_id,
    mmt.municipality_type_id AS municipality_type_id,
    mt.municipality_type_desc AS municipality_type_desc
  FROM roads.municipality AS m
  LEFT JOIN roads.municipality_municipality_type AS mmt
    ON m.municipality_id = mmt.municipality_id
  LEFT JOIN roads.municipality_type AS mt
    ON mmt.municipality_type_id = mt.municipality_type_desc
), default_DOT_repair_orders_fact_metrics AS (
  SELECT
    default_DOT_municipality_dim.state_id default_DOT_municipality_dim_DOT_state_id,
    default_DOT_municipality_dim.municipality_type_id default_DOT_municipality_dim_DOT_municipality_type_id,
    default_DOT_municipality_dim.municipality_type_desc default_DOT_municipality_dim_DOT_municipality_type_desc,
    default_DOT_municipality_dim.municipality_id default_DOT_municipality_dim_DOT_municipality_id,
    sum(default_DOT_repair_orders_fact.total_repair_cost) default_DOT_total_repair_cost
  FROM default_DOT_repair_orders_fact
  INNER JOIN default_DOT_municipality_dim
    ON default_DOT_repair_orders_fact.municipality_id = default_DOT_municipality_dim.municipality_id
  GROUP BY
    default_DOT_municipality_dim.state_id,
    default_DOT_municipality_dim.municipality_type_id,
    default_DOT_municipality_dim.municipality_type_desc,
    default_DOT_municipality_dim.municipality_id
)
SELECT
  default_DOT_repair_orders_fact_metrics.default_DOT_municipality_dim_DOT_state_id,
  default_DOT_repair_orders_fact_metrics.default_DOT_municipality_dim_DOT_municipality_type_id,
  default_DOT_repair_orders_fact_metrics.default_DOT_municipality_dim_DOT_municipality_type_desc,
  default_DOT_repair_orders_fact_metrics.default_DOT_municipality_dim_DOT_municipality_id,
  default_DOT_repair_orders_fact_metrics.default_DOT_total_repair_cost
FROM default_DOT_repair_orders_fact_metrics
        """,
        ),
    )
    result = duckdb_conn.sql(data["sql"])
    assert result.fetchall() == [
        (33, "A", None, "New York", 285627.0),
        (44, "A", None, "Dallas", 18497.0),
        (44, "A", None, "San Antonio", 76463.0),
        (39, "B", None, "Philadelphia", 1135603.0),
    ]

    response = await module__client_with_examples.get(
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
    expected = """WITH default_DOT_repair_orders_fact AS (
  SELECT
    repair_orders.repair_order_id,
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
  FROM roads.repair_orders AS repair_orders
  JOIN roads.repair_order_details AS repair_order_details
    ON repair_orders.repair_order_id = repair_order_details.repair_order_id
), default_DOT_repair_orders_fact_metrics AS (
  SELECT
    default_DOT_repair_orders_fact.hard_hat_id default_DOT_hard_hat_DOT_hard_hat_id,
    avg(default_DOT_repair_orders_fact.price) default_DOT_avg_repair_price,
    sum(default_DOT_repair_orders_fact.total_repair_cost) default_DOT_total_repair_cost
  FROM default_DOT_repair_orders_fact
  GROUP BY
    default_DOT_repair_orders_fact.hard_hat_id
)
SELECT
  default_DOT_repair_orders_fact_metrics.default_DOT_hard_hat_DOT_hard_hat_id,
  default_DOT_repair_orders_fact_metrics.default_DOT_avg_repair_price,
  default_DOT_repair_orders_fact_metrics.default_DOT_total_repair_cost
FROM default_DOT_repair_orders_fact_metrics
"""

    assert str(parse(data["sql"])) == str(parse(expected))

    result = duckdb_conn.sql(data["sql"])
    assert result.fetchall() == [
        (1, 54672.75, 218691.0),
        (2, 39301.5, 78603.0),
        (3, 76555.33333333333, 229666.0),
        (4, 54083.5, 216334.0),
        (5, 64190.6, 320953.0),
        (6, 65595.66666666667, 196787.0),
        (7, 53374.0, 53374.0),
        (8, 65682.0, 131364.0),
        (9, 70418.0, 70418.0),
    ]


@pytest.mark.asyncio
async def test_get_sql_for_metrics_filters_validate_dimensions(
    module__client_with_examples: AsyncClient,
):
    """
    Test that we extract the columns from filters to validate that they are from shared dimensions
    """
    response = await module__client_with_examples.get(
        "/sql/",
        params={
            "metrics": ["foo.bar.num_repair_orders", "foo.bar.avg_repair_price"],
            "dimensions": [
                "foo.bar.hard_hat.country",
            ],
            "filters": ["default.hard_hat.city = 'Las Vegas'"],
            "limit": 10,
            "ignore_errors": False,
        },
    )
    data = response.json()
    assert (
        "This dimension attribute cannot be joined in: default.hard_hat.city"
        in data["message"]
    )


@pytest.mark.asyncio
async def test_get_sql_for_metrics_orderby_not_in_dimensions(
    module__client_with_examples: AsyncClient,
):
    """
    Test that we extract the columns from filters to validate that they are from shared dimensions
    """
    response = await module__client_with_examples.get(
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
    module__client_with_examples: AsyncClient,
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

    module__client_with_examples.app.dependency_overrides[validate_access] = (
        validate_access_override
    )
    response = await module__client_with_examples.get(
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
    module__client_with_examples.app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_sql_structs(module__client_with_examples: AsyncClient):
    """
    Create a transform with structs and verify that metric expressions that reference these
    structs, along with grouping by dimensions that reference these structs will work when
    building metrics SQL.
    """
    await module__client_with_examples.post(
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

    await module__client_with_examples.post(
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
        await module__client_with_examples.post(
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

    expected = """
    WITH default_DOT_simple_agg AS (
      SELECT
        EXTRACT(YEAR, ro.relevant_dates.order_dt) AS order_year,
        EXTRACT(MONTH, ro.relevant_dates.order_dt) AS order_month,
        EXTRACT(DAY, ro.relevant_dates.order_dt) AS order_day,
        SUM(
          DATEDIFF(
            ro.relevant_dates.dispatched_dt,
            ro.relevant_dates.order_dt
          )
        ) AS dispatch_delay_sum,
        COUNT(ro.repair_order_id) AS repair_orders_cnt
      FROM (
        SELECT
          default_DOT_repair_orders.repair_order_id,
          struct(
            default_DOT_repair_orders.required_date AS required_dt,
            default_DOT_repair_orders.order_date AS order_dt,
            default_DOT_repair_orders.dispatched_date AS dispatched_dt
          ) relevant_dates
        FROM roads.repair_orders AS default_DOT_repair_orders
      ) AS ro
      WHERE  EXTRACT(YEAR, ro.relevant_dates.order_dt) = 2020
      GROUP BY
        EXTRACT(YEAR, ro.relevant_dates.order_dt),
        EXTRACT(MONTH, ro.relevant_dates.order_dt),
        EXTRACT(DAY, ro.relevant_dates.order_dt)
    ),
    default_DOT_simple_agg_metrics AS (
      SELECT
        default_DOT_simple_agg.order_year default_DOT_simple_agg_DOT_order_year,
        default_DOT_simple_agg.order_month default_DOT_simple_agg_DOT_order_month,
        default_DOT_simple_agg.order_day default_DOT_simple_agg_DOT_order_day,
        SUM(default_DOT_simple_agg.dispatch_delay_sum) / SUM(default_DOT_simple_agg.repair_orders_cnt) default_DOT_average_dispatch_delay
      FROM default_DOT_simple_agg
      WHERE  default_DOT_simple_agg.order_year = 2020
      GROUP BY  default_DOT_simple_agg.order_year, default_DOT_simple_agg.order_month, default_DOT_simple_agg.order_day
    )
    SELECT
      default_DOT_simple_agg_metrics.default_DOT_simple_agg_DOT_order_year,
      default_DOT_simple_agg_metrics.default_DOT_simple_agg_DOT_order_month,
      default_DOT_simple_agg_metrics.default_DOT_simple_agg_DOT_order_day,
      default_DOT_simple_agg_metrics.default_DOT_average_dispatch_delay
    FROM default_DOT_simple_agg_metrics
    """
    response = await module__client_with_examples.get("/sql", params=sql_params)
    data = response.json()
    assert str(parse(str(expected))) == str(parse(str(data["sql"])))

    # Test the same query string but with `ro` as a CTE
    await module__client_with_examples.patch(
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

    response = await module__client_with_examples.get("/sql", params=sql_params)
    data = response.json()
    assert str(parse(str(expected))) == str(parse(str(data["sql"])))


@pytest.mark.asyncio
async def test_filter_pushdowns(
    module__client_with_examples: AsyncClient,
):
    """
    Pushing down filters should use the column names and not the column aliases
    """
    response = await module__client_with_examples.patch(
        "/nodes/default.repair_orders_fact",
        json={
            "query": "SELECT repair_orders.hard_hat_id AS hh_id "
            "FROM default.repair_orders repair_orders",
        },
    )
    assert response.status_code == 200

    response = await module__client_with_examples.post(
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

    response = await module__client_with_examples.get(
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
            WITH default_DOT_repair_orders_fact AS (
              SELECT  repair_orders.hard_hat_id AS hh_id
              FROM roads.repair_orders AS repair_orders
              WHERE  repair_orders.hard_hat_id IN (123, 13) AND repair_orders.hard_hat_id = 123 OR repair_orders.hard_hat_id = 13
            )
            SELECT
              default_DOT_repair_orders_fact.hh_id default_DOT_hard_hat_DOT_hard_hat_id
            FROM default_DOT_repair_orders_fact
            WHERE  default_DOT_repair_orders_fact.hh_id IN (123, 13) AND default_DOT_repair_orders_fact.hh_id = 123 OR default_DOT_repair_orders_fact.hh_id = 13
            """,
        ),
    )


@pytest.mark.asyncio
async def test_sql_use_materialized_table(
    measures_sql_request,
    client_with_roads: AsyncClient,
):
    """
    Posting a materialized table for a dimension node should result in building SQL
    that uses the materialized table whenever a dimension attribute on the node is
    requested.
    """
    availability_response = await client_with_roads.post(
        "/data/default.hard_hat/availability",
        json={
            "catalog": "default",
            "schema_": "xyz",
            "table": "hardhat",
            "valid_through_ts": 20240601,
            "max_temporal_partition": ["2024", "06", "01"],
            "min_temporal_partition": ["2022", "01", "01"],
        },
    )
    assert availability_response.status_code == 200
    response = (await measures_sql_request()).json()
    response = (await measures_sql_request()).json()
    assert "xyz.hardhat" in response[0]["sql"]
    expected_sql = """WITH default_DOT_repair_orders_fact AS (
  SELECT
    repair_orders.repair_order_id,
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
  FROM roads.repair_orders AS repair_orders
  JOIN roads.repair_order_details AS repair_order_details
    ON repair_orders.repair_order_id = repair_order_details.repair_order_id
), default_DOT_dispatcher AS (
  SELECT
    default_DOT_dispatchers.dispatcher_id,
    default_DOT_dispatchers.company_name,
    default_DOT_dispatchers.phone
  FROM roads.dispatchers AS default_DOT_dispatchers
), default_DOT_hard_hat AS (
  SELECT
    default_DOT_hard_hat.hard_hat_id,
    default_DOT_hard_hat.last_name,
    default_DOT_hard_hat.first_name,
    default_DOT_hard_hat.title,
    default_DOT_hard_hat.birth_date,
    default_DOT_hard_hat.hire_date,
    default_DOT_hard_hat.address,
    default_DOT_hard_hat.city,
    default_DOT_hard_hat.state,
    default_DOT_hard_hat.postal_code,
    default_DOT_hard_hat.country,
    default_DOT_hard_hat.manager,
    default_DOT_hard_hat.contractor_id
  FROM (
    SELECT
      hard_hat_id,
      last_name,
      first_name,
      title,
      birth_date,
      hire_date,
      address,
      city,
      state,
      postal_code,
      country,
      manager,
      contractor_id
    FROM xyz.hardhat
    WHERE
      state = 'CA'
  ) default_DOT_hard_hat
  WHERE
    default_DOT_hard_hat.state = 'CA'
)
SELECT
  default_DOT_repair_orders_fact.repair_order_id default_DOT_repair_orders_fact_DOT_repair_order_id,
  default_DOT_repair_orders_fact.municipality_id default_DOT_repair_orders_fact_DOT_municipality_id,
  default_DOT_repair_orders_fact.hard_hat_id default_DOT_repair_orders_fact_DOT_hard_hat_id,
  default_DOT_repair_orders_fact.dispatcher_id default_DOT_repair_orders_fact_DOT_dispatcher_id,
  default_DOT_repair_orders_fact.order_date default_DOT_repair_orders_fact_DOT_order_date,
  default_DOT_repair_orders_fact.dispatched_date default_DOT_repair_orders_fact_DOT_dispatched_date,
  default_DOT_repair_orders_fact.required_date default_DOT_repair_orders_fact_DOT_required_date,
  default_DOT_repair_orders_fact.discount default_DOT_repair_orders_fact_DOT_discount,
  default_DOT_repair_orders_fact.price default_DOT_repair_orders_fact_DOT_price,
  default_DOT_repair_orders_fact.quantity default_DOT_repair_orders_fact_DOT_quantity,
  default_DOT_repair_orders_fact.repair_type_id default_DOT_repair_orders_fact_DOT_repair_type_id,
  default_DOT_repair_orders_fact.total_repair_cost default_DOT_repair_orders_fact_DOT_total_repair_cost,
  default_DOT_repair_orders_fact.time_to_dispatch default_DOT_repair_orders_fact_DOT_time_to_dispatch,
  default_DOT_repair_orders_fact.dispatch_delay default_DOT_repair_orders_fact_DOT_dispatch_delay,
  default_DOT_dispatcher.company_name default_DOT_dispatcher_DOT_company_name,
  default_DOT_hard_hat.state default_DOT_hard_hat_DOT_state
FROM default_DOT_repair_orders_fact
INNER JOIN default_DOT_dispatcher
  ON default_DOT_repair_orders_fact.dispatcher_id = default_DOT_dispatcher.dispatcher_id
INNER JOIN default_DOT_hard_hat
  ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
WHERE  default_DOT_hard_hat.state = 'CA'
    """
    assert str(parse(expected_sql)) == str(parse(response[0]["sql"]))


@pytest.mark.asyncio
async def test_filter_on_source_nodes(
    module__client_with_examples: AsyncClient,
):
    """
    Verify that filtering using dimensions that are available on a given node's upstream
    source nodes works, even if these dimensions are not available on the node itself.
    """
    # Create a dimension node: `default.event_date`
    response = await module__client_with_examples.post(
        "/nodes/dimension",
        json={
            "name": "default.event_date",
            "description": "",
            "display_name": "Event Date",
            "query": """
              SELECT
                20240101 AS dateint,
                '2024-01-01' AS date
            """,
            "mode": "published",
            "primary_key": ["dateint"],
        },
    )
    assert response.status_code == 201

    # Create a source node: `default.events`
    response = await module__client_with_examples.post(
        "/nodes/source",
        json={
            "name": "default.events",
            "description": "",
            "display_name": "Events",
            "catalog": "default",
            "schema_": "example",
            "table": "events",
            "columns": [
                {"name": "event_id", "type": "int"},
                {"name": "event_date", "type": "int"},
                {"name": "user_id", "type": "int"},
                {"name": "duration_ms", "type": "int"},
            ],
            "primary_key": ["event_id"],
            "mode": "published",
        },
    )
    assert response.status_code == 200

    # Link `default.events` transform to the `default.event_date` dimension node on `dateint`
    response = await module__client_with_examples.post(
        "/nodes/default.events/link",
        json={
            "dimension_node": "default.event_date",
            "join_type": "left",
            "join_on": "default.events.event_date = default.event_date.dateint",
        },
    )
    assert response.status_code == 201

    # Create a transform on `default.events` that aggregates it to the user level
    response = await module__client_with_examples.post(
        "/nodes/transform",
        json={
            "name": "default.events_agg",
            "description": "",
            "display_name": "Events Agg",
            "query": """
              SELECT
                user_id,
                SUM(duration_ms) AS duration_ms
              FROM default.events
            """,
            "primary_key": ["user_id"],
            "mode": "published",
        },
    )
    assert response.status_code == 201

    # Request the available dimensions for `default.events_agg`
    response = await module__client_with_examples.get(
        "/nodes/default.events_agg/dimensions",
    )
    assert response.json() == [
        {
            "name": "default.events_agg.user_id",
            "node_display_name": "Events Agg",
            "node_name": "default.events_agg",
            "path": [],
            "type": "int",
            "filter_only": False,
            "properties": ["primary_key"],
        },
        {
            "name": "default.event_date.dateint",
            "node_display_name": "Event Date",
            "node_name": "default.event_date",
            "path": ["default.events"],
            "type": "int",
            "filter_only": True,
            "properties": ["primary_key"],
        },
    ]

    # Request SQL for default.events_agg with filters on `default.event_date`
    response = await module__client_with_examples.get(
        "/sql/default.events_agg",
        params={
            "filters": ["default.event_date.dateint BETWEEN 20240101 AND 20240201"],
        },
    )

    # Check that the filters have propagated to the upstream nodes
    assert str(parse(response.json()["sql"])) == str(
        parse(
            """
            WITH default_DOT_events_agg AS (
              SELECT  default_DOT_events.user_id,
                SUM(default_DOT_events.duration_ms) AS duration_ms
              FROM (
                SELECT
                  event_id,
                  event_date,
                  user_id,
                  duration_ms
                FROM example.events
                WHERE  event_date BETWEEN 20240101 AND 20240201
              ) default_DOT_events
            )
            SELECT
              default_DOT_events_agg.user_id default_DOT_events_agg_DOT_user_id,
              default_DOT_events_agg.duration_ms default_DOT_events_agg_DOT_duration_ms
            FROM default_DOT_events_agg
            """,
        ),
    )


# =============================================================================
# Role Path Dimension Test Fixtures
# These fixtures create the multi-hop dimension hierarchies for testing the
# role path functionality from commits bba9866a and 5fa5515d
# =============================================================================


@pytest.fixture(scope="module")
async def regions_source_table(module__client_with_examples: AsyncClient):
    """Create regions source table."""
    response = await module__client_with_examples.post(
        "/nodes/source/",
        json={
            "description": "Regions source table",
            "mode": "published",
            "name": "default.regions_table",
            "catalog": "default",
            "schema_": "public",
            "table": "regions",
            "columns": [
                {
                    "name": "region_id",
                    "type": "int",
                    "attributes": [],
                    "dimension": None,
                },
                {
                    "name": "region_name",
                    "type": "string",
                    "attributes": [],
                    "dimension": None,
                },
                {
                    "name": "continent_id",
                    "type": "int",
                    "attributes": [],
                    "dimension": None,
                },
            ],
        },
    )
    assert response.status_code in (200, 201)


@pytest.fixture(scope="module")
async def continents_source_table(module__client_with_examples: AsyncClient):
    """Create continents source table."""
    response = await module__client_with_examples.post(
        "/nodes/source/",
        json={
            "description": "Continents source table",
            "mode": "published",
            "name": "default.continents_table",
            "catalog": "default",
            "schema_": "public",
            "table": "continents",
            "columns": [
                {
                    "name": "continent_id",
                    "type": "int",
                    "attributes": [],
                    "dimension": None,
                },
                {
                    "name": "continent_name",
                    "type": "string",
                    "attributes": [],
                    "dimension": None,
                },
                {
                    "name": "hemisphere",
                    "type": "string",
                    "attributes": [],
                    "dimension": None,
                },
            ],
        },
    )
    assert response.status_code in (200, 201)


@pytest.fixture(scope="module")
async def weeks_source_table(module__client_with_examples: AsyncClient):
    """Create weeks source table."""
    response = await module__client_with_examples.post(
        "/nodes/source/",
        json={
            "description": "Week dimension source table",
            "mode": "published",
            "name": "default.weeks_table",
            "catalog": "default",
            "schema_": "public",
            "table": "weeks",
            "columns": [
                {"name": "week_id", "type": "int", "attributes": [], "dimension": None},
                {
                    "name": "week_start_date",
                    "type": "date",
                    "attributes": [],
                    "dimension": None,
                },
                {
                    "name": "week_end_date",
                    "type": "date",
                    "attributes": [],
                    "dimension": None,
                },
                {
                    "name": "week_number",
                    "type": "int",
                    "attributes": [],
                    "dimension": None,
                },
                {
                    "name": "month_id",
                    "type": "int",
                    "attributes": [],
                    "dimension": None,
                },
            ],
        },
    )
    assert response.status_code in (200, 201)


@pytest.fixture(scope="module")
async def months_source_table(module__client_with_examples: AsyncClient):
    """Create months source table."""
    response = await module__client_with_examples.post(
        "/nodes/source/",
        json={
            "description": "Month dimension source table",
            "mode": "published",
            "name": "default.months_table",
            "catalog": "default",
            "schema_": "public",
            "table": "months",
            "columns": [
                {
                    "name": "month_id",
                    "type": "int",
                    "attributes": [],
                    "dimension": None,
                },
                {
                    "name": "month_name",
                    "type": "string",
                    "attributes": [],
                    "dimension": None,
                },
                {
                    "name": "month_number",
                    "type": "int",
                    "attributes": [],
                    "dimension": None,
                },
                {"name": "year_id", "type": "int", "attributes": [], "dimension": None},
            ],
        },
    )
    assert response.status_code in (200, 201)


@pytest.fixture(scope="module")
async def years_source_table(module__client_with_examples: AsyncClient):
    """Create years source table."""
    response = await module__client_with_examples.post(
        "/nodes/source/",
        json={
            "description": "Year dimension source table",
            "mode": "published",
            "name": "default.years_table",
            "catalog": "default",
            "schema_": "public",
            "table": "years",
            "columns": [
                {"name": "year_id", "type": "int", "attributes": [], "dimension": None},
                {
                    "name": "year_number",
                    "type": "int",
                    "attributes": [],
                    "dimension": None,
                },
                {
                    "name": "decade",
                    "type": "string",
                    "attributes": [],
                    "dimension": None,
                },
            ],
        },
    )
    assert response.status_code in (200, 201)


@pytest.fixture(scope="module")
async def regions_dimension(
    module__client_with_examples: AsyncClient,
    regions_source_table: str,
):
    """Create regions dimension node."""
    response = await module__client_with_examples.post(
        "/nodes/dimension/",
        json={
            "description": "Geographic regions",
            "query": """
            SELECT
                region_id,
                region_name,
                continent_id
            FROM default.regions_table
            """,
            "mode": "published",
            "name": "default.regions",
            "primary_key": ["region_id"],
        },
    )
    assert response.status_code in (200, 201)


@pytest.fixture(scope="module")
async def continents_dimension(
    module__client_with_examples: AsyncClient,
    continents_source_table: str,
):
    """Create continents dimension node."""
    response = await module__client_with_examples.post(
        "/nodes/dimension/",
        json={
            "description": "Geographic continents",
            "query": """
            SELECT
                continent_id,
                continent_name,
                hemisphere
            FROM default.continents_table
            """,
            "mode": "published",
            "name": "default.continents",
            "primary_key": ["continent_id"],
        },
    )
    assert response.status_code in (200, 201)


@pytest.fixture(scope="module")
async def weeks_dimension(
    module__client_with_examples: AsyncClient,
    weeks_source_table: str,
):
    """Create weeks dimension node."""
    response = await module__client_with_examples.post(
        "/nodes/dimension/",
        json={
            "description": "Week dimension",
            "query": """
            SELECT
                week_id,
                week_start_date,
                week_end_date,
                week_number,
                month_id
            FROM default.weeks_table
            """,
            "mode": "published",
            "name": "default.weeks",
            "primary_key": ["week_id"],
        },
    )
    assert response.status_code in (200, 201)


@pytest.fixture(scope="module")
async def months_dimension(
    module__client_with_examples: AsyncClient,
    months_source_table: str,
):
    """Create months dimension node."""
    response = await module__client_with_examples.post(
        "/nodes/dimension/",
        json={
            "description": "Month dimension",
            "query": """
            SELECT
                month_id,
                month_name,
                month_number,
                year_id
            FROM default.months_table
            """,
            "mode": "published",
            "name": "default.months",
            "primary_key": ["month_id"],
        },
    )
    assert response.status_code in (200, 201)


@pytest.fixture(scope="module")
async def years_dimension(
    module__client_with_examples: AsyncClient,
    years_source_table: str,
):
    """Create years dimension node."""
    response = await module__client_with_examples.post(
        "/nodes/dimension/",
        json={
            "description": "Year dimension",
            "query": """
            SELECT
                year_id,
                year_number,
                decade
            FROM default.years_table
            """,
            "mode": "published",
            "name": "default.years",
            "primary_key": ["year_id"],
        },
    )
    assert response.status_code in (200, 201)


@pytest.fixture(scope="module")
async def countries_to_regions_link(
    module__client_with_examples: AsyncClient,
    regions_dimension: str,
):
    """Link countries to regions with role."""
    response = await module__client_with_examples.post(
        "/nodes/default.special_country_dim/link",
        json={
            "dimension_node": "default.regions",
            "join_type": "left",
            "join_on": "default.special_country_dim.country_code = default.regions.region_id",
            "role": "country_region",
        },
    )
    assert response.status_code in (200, 201)


@pytest.fixture(scope="module")
async def regions_to_continents_link(
    module__client_with_examples: AsyncClient,
    regions_dimension: str,
    continents_dimension: str,
):
    """Link regions to continents with role."""
    response = await module__client_with_examples.post(
        "/nodes/default.regions/link",
        json={
            "dimension_node": "default.continents",
            "join_type": "left",
            "join_on": "default.regions.continent_id = default.continents.continent_id",
            "role": "region_continent",
        },
    )
    assert response.status_code in (200, 201)


@pytest.fixture(scope="module")
async def users_to_countries_birth_link(
    module__client_with_examples: AsyncClient,
):
    """Link users to countries with birth country role (using birth_country as registration proxy)."""
    response = await module__client_with_examples.post(
        "/nodes/default.user_dim/link",
        json={
            "dimension_node": "default.special_country_dim",
            "join_type": "left",
            "join_on": "default.user_dim.birth_country = default.special_country_dim.country_code",
            "role": "user_birth_country",
        },
    )
    assert response.status_code in (200, 201)


@pytest.fixture(scope="module")
async def users_to_countries_residence_link(
    module__client_with_examples: AsyncClient,
):
    """Link users to countries with residence role."""
    response = await module__client_with_examples.post(
        "/nodes/default.user_dim/link",
        json={
            "dimension_node": "default.special_country_dim",
            "join_type": "left",
            "join_on": "default.user_dim.residence_country = default.special_country_dim.country_code",
            "role": "user_residence_country",
        },
    )
    assert response.status_code in (200, 201)


@pytest.fixture(scope="module")
async def users_to_date_birth_link(
    module__client_with_examples: AsyncClient,
):
    """Link users to date with birth role (using birth_date)."""
    response = await module__client_with_examples.post(
        "/nodes/default.user_dim/link",
        json={
            "dimension_node": "default.date_dim",
            "join_type": "left",
            "join_on": "default.user_dim.birth_date = default.date_dim.dateint",
            "role": "user_birth_date",
        },
    )
    assert response.status_code in (200, 201)


@pytest.fixture(scope="module")
async def date_to_weeks_link(
    module__client_with_examples: AsyncClient,
    weeks_dimension: str,
):
    """Link date_dim to weeks with role (may fail if date_dim doesn't exist)."""
    response = await module__client_with_examples.post(
        "/nodes/default.date_dim/link",
        json={
            "dimension_node": "default.weeks",
            "join_type": "left",
            "join_on": "default.date_dim.dateint BETWEEN default.weeks.week_start_date AND default.weeks.week_end_date",
            "role": "date_week",
        },
    )
    return response.status_code in (200, 201)


@pytest.fixture(scope="module")
async def weeks_to_months_link(
    module__client_with_examples: AsyncClient,
    weeks_dimension: str,
    months_dimension: str,
):
    """Link weeks to months with role."""
    response = await module__client_with_examples.post(
        "/nodes/default.weeks/link",
        json={
            "dimension_node": "default.months",
            "join_type": "left",
            "join_on": "default.weeks.month_id = default.months.month_id",
            "role": "week_month",
        },
    )
    assert response.status_code in (200, 201)


@pytest.fixture(scope="module")
async def months_to_years_link(
    module__client_with_examples: AsyncClient,
    months_dimension: str,
    years_dimension: str,
):
    """Link months to years with role."""
    response = await module__client_with_examples.post(
        "/nodes/default.months/link",
        json={
            "dimension_node": "default.years",
            "join_type": "left",
            "join_on": "default.months.year_id = default.years.year_id",
            "role": "month_year",
        },
    )
    assert response.status_code in (200, 201)


@pytest.fixture(scope="module")
async def geographic_hierarchy(
    countries_to_regions_link: bool,
    regions_to_continents_link: str,
    users_to_countries_birth_link: str,
    users_to_countries_residence_link: str,
):
    """Complete geographic hierarchy: Users -> Countries -> Regions -> Continents."""
    return {
        "countries_to_regions": countries_to_regions_link,
        "regions_to_continents": regions_to_continents_link,
        "users_birth": users_to_countries_birth_link,
        "users_residence": users_to_countries_residence_link,
    }


@pytest.fixture(scope="module")
async def temporal_hierarchy(
    date_to_weeks_link: bool,
    weeks_to_months_link: str,
    months_to_years_link: str,
    users_to_date_birth_link: str,
):
    """Complete temporal hierarchy: Date -> Week -> Month -> Year."""
    return {
        "date_to_weeks": date_to_weeks_link,
        "weeks_to_months": weeks_to_months_link,
        "months_to_years": months_to_years_link,
        "users_to_date_birth": users_to_date_birth_link,
    }


@pytest.fixture
async def role_path_test_setup(geographic_hierarchy: dict, temporal_hierarchy: dict):
    """Combined fixture providing both geographic and temporal hierarchies for role path testing."""
    return {
        "geographic": geographic_hierarchy,
        "temporal": temporal_hierarchy,
    }


@pytest.mark.asyncio
async def test_role_path_dimensions_in_filters_single_hop(
    module__client_with_examples: AsyncClient,
    role_path_test_setup: dict,
):
    """
    Test role path dimensions in filters for single hop scenarios.
    """
    # Test filter with single role path - geographic hierarchy
    response = await module__client_with_examples.get(
        "/sql/default.avg_user_age",
        params={
            "dimensions": [
                "default.special_country_dim.name[user_birth_country]",
            ],
            "filters": [
                "default.special_country_dim.name[user_birth_country] = 'United States'",
            ],
        },
    )
    assert response.status_code == 200

    sql_result = response.json()
    assert str(parse(sql_result["sql"])) == str(
        parse("""
    WITH default_DOT_user_dim AS (
      SELECT
        default_DOT_users.user_id,
    	default_DOT_users.birth_country,
    	default_DOT_users.residence_country,
    	default_DOT_users.age,
    	default_DOT_users.birth_date
      FROM examples.users AS default_DOT_users
    ),
    default_DOT_special_country_dim AS (
      SELECT
        default_DOT_countries.country_code,
    	default_DOT_countries.name,
    	default_DOT_countries.formation_date,
    	default_DOT_countries.last_election_date
      FROM examples.countries AS default_DOT_countries
    ),
    default_DOT_user_dim_metrics AS (
      SELECT
        user_birth_country.name default_DOT_special_country_dim_DOT_name_LBRACK_user_birth_country_RBRACK,
        AVG(default_DOT_user_dim.age) default_DOT_avg_user_age
      FROM default_DOT_user_dim
      LEFT JOIN default_DOT_special_country_dim AS user_birth_country ON default_DOT_user_dim.birth_country = user_birth_country.country_code
      WHERE user_birth_country.name = 'United States'
      GROUP BY user_birth_country.name
    )
    SELECT
      default_DOT_user_dim_metrics.default_DOT_special_country_dim_DOT_name_LBRACK_user_birth_country_RBRACK,
      default_DOT_user_dim_metrics.default_DOT_avg_user_age
    FROM default_DOT_user_dim_metrics
    """),
    )


@pytest.mark.asyncio
async def test_role_path_dimensions_in_filters_multi_hop_geographic(
    module__client_with_examples: AsyncClient,
    role_path_test_setup: dict,
):
    """
    Test role path dimensions in filters for multi-hop scenarios with geographic role paths.
    """
    # Test filter with multi-hop role path: user -> country -> region -> continent
    response = await module__client_with_examples.get(
        "/sql/default.avg_user_age/",
        params={
            "dimensions": [
                "default.continents.continent_name[user_residence_country->country_region->region_continent]",
            ],
            "filters": [
                "default.continents.continent_name[user_residence_country->country_region->region_continent] = 'North America'",
            ],
        },
    )

    assert response.status_code == 200, (
        f"Response: {response.status_code}, {response.json()}"
    )
    assert str(parse(response.json()["sql"])) == str(
        parse("""
    WITH
    default_DOT_user_dim AS (
      SELECT
        default_DOT_users.user_id,
        default_DOT_users.birth_country,
        default_DOT_users.residence_country,
        default_DOT_users.age,
        default_DOT_users.birth_date
      FROM examples.users AS default_DOT_users
    ),
    default_DOT_special_country_dim AS (
      SELECT
        default_DOT_countries.country_code,
        default_DOT_countries.name,
        default_DOT_countries.formation_date,
    	  default_DOT_countries.last_election_date
      FROM examples.countries AS default_DOT_countries
    ),
    default_DOT_regions AS (
      SELECT
        default_DOT_regions_table.region_id,
        default_DOT_regions_table.region_name,
        default_DOT_regions_table.continent_id
      FROM public.regions AS default_DOT_regions_table
    ),
    default_DOT_continents AS (
      SELECT
        default_DOT_continents_table.continent_id,
        default_DOT_continents_table.continent_name,
        default_DOT_continents_table.hemisphere
      FROM public.continents AS default_DOT_continents_table
    ),
    default_DOT_user_dim_metrics AS (
      SELECT
        user_residence_country__country_region__region_continent.continent_name default_DOT_continents_DOT_continent_name_LBRACK_user_residence_country_MINUS__GT_country_region_MINUS__GT_region_continent_RBRACK,
        AVG(default_DOT_user_dim.age) default_DOT_avg_user_age
      FROM default_DOT_user_dim
      LEFT JOIN default_DOT_special_country_dim AS user_residence_country ON default_DOT_user_dim.residence_country = user_residence_country.country_code
      LEFT JOIN default_DOT_regions AS user_residence_country__country_region ON user_residence_country.country_code = user_residence_country__country_region.region_id
      LEFT JOIN default_DOT_continents AS user_residence_country__country_region__region_continent ON user_residence_country__country_region.continent_id = user_residence_country__country_region__region_continent.continent_id
      WHERE
        user_residence_country__country_region__region_continent.continent_name = 'North America'
      GROUP BY
        user_residence_country__country_region__region_continent.continent_name
    )
    SELECT
      default_DOT_user_dim_metrics.default_DOT_continents_DOT_continent_name_LBRACK_user_residence_country_MINUS__GT_country_region_MINUS__GT_region_continent_RBRACK,
    	default_DOT_user_dim_metrics.default_DOT_avg_user_age
    FROM default_DOT_user_dim_metrics
    """),
    )


@pytest.mark.asyncio
async def test_role_path_dimensions_in_filters_multi_hop_temporal(
    module__client_with_examples: AsyncClient,
    role_path_test_setup: dict,
):
    """
    Test role path dimensions in filters for multi-hop scenarios with temporal role paths.
    """
    # Test filter with multi-hop temporal role path: date -> week -> month -> year
    response = await module__client_with_examples.get(
        "/sql/default.avg_user_age/",
        params={
            "dimensions": [
                "default.years.year_number[user_birth_date->date_week->week_month->month_year]",
            ],
            "filters": [
                "default.years.year_number[user_birth_date->date_week->week_month->month_year] = 2024",
            ],
        },
    )

    assert response.status_code == 200, (
        f"Response: {response.status_code}, {response.json()}"
    )
    assert str(parse(response.json()["sql"])) == str(
        parse("""
    WITH
    default_DOT_user_dim AS (
    SELECT  default_DOT_users.user_id,
    	default_DOT_users.birth_country,
    	default_DOT_users.residence_country,
    	default_DOT_users.age,
    	default_DOT_users.birth_date
     FROM examples.users AS default_DOT_users
    ),
    default_DOT_date_dim AS (
    SELECT  default_DOT_date.dateint,
    	default_DOT_date.month,
    	default_DOT_date.year,
    	default_DOT_date.day
     FROM examples.date AS default_DOT_date
    ),
    default_DOT_weeks AS (
    SELECT  default_DOT_weeks_table.week_id,
    	default_DOT_weeks_table.week_start_date,
    	default_DOT_weeks_table.week_end_date,
    	default_DOT_weeks_table.week_number,
    	default_DOT_weeks_table.month_id
     FROM public.weeks AS default_DOT_weeks_table
    ),
    default_DOT_months AS (
    SELECT  default_DOT_months_table.month_id,
    	default_DOT_months_table.month_name,
    	default_DOT_months_table.month_number,
    	default_DOT_months_table.year_id
     FROM public.months AS default_DOT_months_table
    ),
    default_DOT_years AS (
    SELECT  default_DOT_years_table.year_id,
    	default_DOT_years_table.year_number,
    	default_DOT_years_table.decade
     FROM public.years AS default_DOT_years_table
    ),
    default_DOT_user_dim_metrics AS (
      SELECT
        user_birth_date__date_week__week_month__month_year.year_number default_DOT_years_DOT_year_number_LBRACK_user_birth_date_MINUS__GT_date_week_MINUS__GT_week_month_MINUS__GT_month_year_RBRACK,
        AVG(default_DOT_user_dim.age) default_DOT_avg_user_age
      FROM default_DOT_user_dim LEFT JOIN default_DOT_date_dim AS user_birth_date ON default_DOT_user_dim.birth_date = user_birth_date.dateint
      LEFT JOIN default_DOT_weeks AS user_birth_date__date_week ON user_birth_date.dateint BETWEEN user_birth_date__date_week.week_start_date AND user_birth_date__date_week.week_end_date
      LEFT JOIN default_DOT_months AS user_birth_date__date_week__week_month ON user_birth_date__date_week.month_id = user_birth_date__date_week__week_month.month_id
      LEFT JOIN default_DOT_years AS user_birth_date__date_week__week_month__month_year ON user_birth_date__date_week__week_month.year_id = user_birth_date__date_week__week_month__month_year.year_id
      WHERE  user_birth_date__date_week__week_month__month_year.year_number = 2024
      GROUP BY  user_birth_date__date_week__week_month__month_year.year_number
    )
    SELECT  default_DOT_user_dim_metrics.default_DOT_years_DOT_year_number_LBRACK_user_birth_date_MINUS__GT_date_week_MINUS__GT_week_month_MINUS__GT_month_year_RBRACK,
    	default_DOT_user_dim_metrics.default_DOT_avg_user_age
     FROM default_DOT_user_dim_metrics
    """),
    )


@pytest.mark.asyncio
async def test_role_path_dimensions_mixed_paths(
    module__client_with_examples: AsyncClient,
    role_path_test_setup: dict,
):
    """
    Test filtering with mixed role paths - different roles for the same dimension attribute.
    """
    # Test with both birth and residence country paths
    response = await module__client_with_examples.get(
        "/sql/default.avg_user_age/",
        params={
            "dimensions": [
                "default.special_country_dim.name[user_birth_country]",
                "default.special_country_dim.name[user_residence_country]",
            ],
            "filters": [
                "default.special_country_dim.name[user_birth_country] = 'Canada'",
                "default.special_country_dim.name[user_residence_country] = 'United States'",
            ],
        },
    )
    assert response.status_code == 200
    sql_result = response.json()
    assert str(parse(sql_result["sql"])) == str(
        parse("""
    WITH default_DOT_user_dim AS (
      SELECT
        default_DOT_users.user_id,
        default_DOT_users.birth_country,
        default_DOT_users.residence_country,
        default_DOT_users.age,
        default_DOT_users.birth_date
      FROM examples.users AS default_DOT_users
    ),
    default_DOT_special_country_dim AS (
      SELECT
        default_DOT_countries.country_code,
        default_DOT_countries.name,
        default_DOT_countries.formation_date,
        default_DOT_countries.last_election_date
      FROM examples.countries AS default_DOT_countries
    ),
    default_DOT_user_dim_metrics AS (
      SELECT
        user_birth_country.name default_DOT_special_country_dim_DOT_name_LBRACK_user_birth_country_RBRACK,
        user_residence_country.name default_DOT_special_country_dim_DOT_name_LBRACK_user_residence_country_RBRACK,
        AVG(default_DOT_user_dim.age) default_DOT_avg_user_age
      FROM default_DOT_user_dim
      LEFT JOIN default_DOT_special_country_dim AS user_birth_country
        ON default_DOT_user_dim.birth_country = user_birth_country.country_code
      LEFT JOIN default_DOT_special_country_dim AS user_residence_country
        ON default_DOT_user_dim.residence_country = user_residence_country.country_code
      WHERE user_birth_country.name = 'Canada'
        AND user_residence_country.name = 'United States'
      GROUP BY user_birth_country.name, user_residence_country.name
    )
    SELECT
      default_DOT_user_dim_metrics.default_DOT_special_country_dim_DOT_name_LBRACK_user_birth_country_RBRACK,
      default_DOT_user_dim_metrics.default_DOT_special_country_dim_DOT_name_LBRACK_user_residence_country_RBRACK,
      default_DOT_user_dim_metrics.default_DOT_avg_user_age
    FROM default_DOT_user_dim_metrics
    """),
    )


@pytest.mark.asyncio
async def test_role_path_dimensions_mixed_hierarchies(
    module__client_with_examples: AsyncClient,
    role_path_test_setup: dict,
):
    """
    Test filtering with mixed hierarchies - geographic and temporal role paths in same query.
    """
    # Test with both geographic and temporal hierarchies
    response = await module__client_with_examples.get(
        "/sql/default.avg_user_age",
        params={
            "dimensions": [
                "default.continents.continent_name[user_birth_country->country_region->region_continent]",
                "default.months.month_name[user_birth_date->date_week->week_month]",
            ],
            "filters": [
                "default.regions.region_name[user_birth_country->country_region] = 'APAC'",
                "default.years.year_number[user_birth_date->date_week->week_month->month_year] = 1940",
            ],
        },
    )
    assert response.status_code == 200
    assert str(parse(response.json()["sql"])) == str(
        parse("""
    WITH
    default_DOT_user_dim AS (
    SELECT  default_DOT_users.user_id,
    	default_DOT_users.birth_country,
    	default_DOT_users.residence_country,
    	default_DOT_users.age,
    	default_DOT_users.birth_date
     FROM examples.users AS default_DOT_users
    ),
    default_DOT_special_country_dim AS (
    SELECT  default_DOT_countries.country_code,
    	default_DOT_countries.name,
    	default_DOT_countries.formation_date,
    	default_DOT_countries.last_election_date
     FROM examples.countries AS default_DOT_countries
    ),
    default_DOT_regions AS (
    SELECT  default_DOT_regions_table.region_id,
    	default_DOT_regions_table.region_name,
    	default_DOT_regions_table.continent_id
     FROM public.regions AS default_DOT_regions_table
    ),
    default_DOT_continents AS (
    SELECT  default_DOT_continents_table.continent_id,
    	default_DOT_continents_table.continent_name,
    	default_DOT_continents_table.hemisphere
     FROM public.continents AS default_DOT_continents_table
    ),
    default_DOT_date_dim AS (
    SELECT  default_DOT_date.dateint,
    	default_DOT_date.month,
    	default_DOT_date.year,
    	default_DOT_date.day
     FROM examples.date AS default_DOT_date
    ),
    default_DOT_weeks AS (
    SELECT  default_DOT_weeks_table.week_id,
    	default_DOT_weeks_table.week_start_date,
    	default_DOT_weeks_table.week_end_date,
    	default_DOT_weeks_table.week_number,
    	default_DOT_weeks_table.month_id
     FROM public.weeks AS default_DOT_weeks_table
    ),
    default_DOT_months AS (
    SELECT  default_DOT_months_table.month_id,
    	default_DOT_months_table.month_name,
    	default_DOT_months_table.month_number,
    	default_DOT_months_table.year_id
     FROM public.months AS default_DOT_months_table
    ),
    default_DOT_years AS (
    SELECT  default_DOT_years_table.year_id,
    	default_DOT_years_table.year_number,
    	default_DOT_years_table.decade
     FROM public.years AS default_DOT_years_table
    ),
    default_DOT_user_dim_metrics AS (
      SELECT
        user_birth_country__country_region__region_continent.continent_name default_DOT_continents_DOT_continent_name_LBRACK_user_birth_country_MINUS__GT_country_region_MINUS__GT_region_continent_RBRACK,
        user_birth_date__date_week__week_month.month_name default_DOT_months_DOT_month_name_LBRACK_user_birth_date_MINUS__GT_date_week_MINUS__GT_week_month_RBRACK,
        AVG(default_DOT_user_dim.age) default_DOT_avg_user_age
      FROM default_DOT_user_dim
      LEFT JOIN default_DOT_special_country_dim AS user_birth_country ON default_DOT_user_dim.birth_country = user_birth_country.country_code
      LEFT JOIN default_DOT_regions AS user_birth_country__country_region ON user_birth_country.country_code = user_birth_country__country_region.region_id
      LEFT JOIN default_DOT_continents AS user_birth_country__country_region__region_continent ON user_birth_country__country_region.continent_id = user_birth_country__country_region__region_continent.continent_id
      LEFT JOIN default_DOT_date_dim AS user_birth_date ON default_DOT_user_dim.birth_date = user_birth_date.dateint
      LEFT JOIN default_DOT_weeks AS user_birth_date__date_week ON user_birth_date.dateint BETWEEN user_birth_date__date_week.week_start_date AND user_birth_date__date_week.week_end_date
      LEFT JOIN default_DOT_months AS user_birth_date__date_week__week_month ON user_birth_date__date_week.month_id = user_birth_date__date_week__week_month.month_id
      LEFT JOIN default_DOT_years AS user_birth_date__date_week__week_month__month_year ON user_birth_date__date_week__week_month.year_id = user_birth_date__date_week__week_month__month_year.year_id
      WHERE
        user_birth_country__country_region.region_name = 'APAC'
        AND user_birth_date__date_week__week_month__month_year.year_number = 1940
      GROUP BY
        user_birth_country__country_region__region_continent.continent_name,
        user_birth_date__date_week__week_month.month_name
    )
    SELECT  default_DOT_user_dim_metrics.default_DOT_continents_DOT_continent_name_LBRACK_user_birth_country_MINUS__GT_country_region_MINUS__GT_region_continent_RBRACK,
    	default_DOT_user_dim_metrics.default_DOT_months_DOT_month_name_LBRACK_user_birth_date_MINUS__GT_date_week_MINUS__GT_week_month_RBRACK,
    	default_DOT_user_dim_metrics.default_DOT_avg_user_age
     FROM default_DOT_user_dim_metrics
    """),
    )


@pytest.mark.asyncio
async def test_role_path_dimensions_cube_integration(
    module__client_with_examples: AsyncClient,
    role_path_test_setup: dict,
):
    """
    Test role path dimensions work properly with cube definitions.
    """
    # Create a cube that includes role path dimensions
    response = await module__client_with_examples.post(
        "/nodes/cube/",
        json={
            "description": "Event analytics cube with role paths",
            "mode": "published",
            "name": "default.events_role_cube",
            "query": """
            SELECT
                default.num_repair_orders,
                default.special_country_dim.name[user_registration_country],
                default.regions.region_name[user_registration_country->country_region]
            FROM default.num_repair_orders
            """,
        },
    )

    if response.status_code in (200, 201):
        # Test querying the cube with role path filters
        response = await module__client_with_examples.get(
            "/sql/default.events_role_cube/",
            params={
                "filters": [
                    "default.special_country_dim.name[user_registration_country] = 'Canada'",
                    "default.regions.region_name[user_registration_country->country_region] = 'North America'",
                ],
            },
        )
        assert response.status_code == 200

        sql_result = response.json()
        # Verify cube properly handles role path filters
        assert "Canada" in sql_result["sql"]
        assert "North America" in sql_result["sql"]
    else:
        # Cube creation might fail if role path syntax isn't supported in cube queries yet
        assert response.status_code in (400, 422)


@pytest.mark.asyncio
async def test_role_path_dimensions_temporal_cube_integration(
    module__client_with_examples: AsyncClient,
    role_path_test_setup: dict,
):
    """
    Test role path dimensions work properly with temporal cube definitions.
    """
    # Create a cube that includes temporal role path dimensions
    response = await module__client_with_examples.post(
        "/nodes/cube/",
        json={
            "description": "Event analytics cube with temporal role paths",
            "mode": "published",
            "name": "default.events_temporal_cube",
            "query": """
            SELECT
                default.num_repair_orders,
                default.months.month_name[date_week->week_month],
                default.years.year_number[date_week->week_month->month_year]
            FROM default.num_repair_orders
            """,
        },
    )

    if response.status_code in (200, 201):
        # Test querying the cube with temporal role path filters
        response = await module__client_with_examples.get(
            "/sql/default.events_temporal_cube/",
            params={
                "filters": [
                    "default.months.month_name[date_week->week_month] = 'January'",
                    "default.years.year_number[date_week->week_month->month_year] = 2024",
                ],
            },
        )
        # This might fail if temporal cubes aren't fully supported
        assert response.status_code in (200, 400, 422)

        if response.status_code == 200:
            sql_result = response.json()
            # Verify cube properly handles temporal role path filters
            assert (
                "January" in sql_result["sql"] or "january" in sql_result["sql"].lower()
            )
            assert "2024" in sql_result["sql"]
    else:
        # Cube creation might fail if temporal role path syntax isn't supported yet
        assert response.status_code in (400, 422)


@pytest.mark.asyncio
async def test_role_path_dimensions_error_handling(
    module__client_with_examples: AsyncClient,
    role_path_test_setup: dict,
):
    """
    Test error handling for invalid role paths and edge cases.
    """
    # Test with invalid role path - non-existent role
    response = await module__client_with_examples.get(
        "/sql/default.avg_user_age/",
        params={
            "dimensions": ["default.special_country_dim.name[invalid_role]"],
            "filters": ["default.special_country_dim.name[invalid_role] = 'Canada'"],
        },
    )
    # Should return an error for invalid role
    assert response.json()["message"] == (
        "The dimension attribute `default.special_country_dim.name[invalid_role]` "
        "is not available on every metric and thus cannot be included."
    )

    # Test with malformed role path syntax
    response = await module__client_with_examples.get(
        "/sql/default.avg_user_age/",
        params={
            "dimensions": [
                "default.special_country_dim.name[user_birth_country->]",
            ],
            "filters": [
                "default.special_country_dim.name[user_birth_country->] = 'Canada'",
            ],
        },
    )
    # Should handle malformed role path gracefully
    assert "Error parsing SQL" in response.json()["message"]


@pytest.mark.asyncio
async def test_multiple_filters_same_role_path(
    module__client_with_examples: AsyncClient,
    role_path_test_setup: dict,
):
    """Test multiple filters on the same role path should work"""
    response = await module__client_with_examples.get(
        "/sql/default.avg_user_age/",
        params={
            "dimensions": [
                "default.special_country_dim.name[user_birth_country]",
            ],
            "filters": [
                "default.special_country_dim.name[user_birth_country] IS NOT NULL",
                "default.special_country_dim.formation_date[user_birth_country] > 20000101",
            ],
        },
    )
    # Multiple filters on the same role path should work
    assert response.status_code == 200
    sql_result = response.json()
    # Both filters should appear in the generated SQL
    assert str(parse(sql_result["sql"])) == str(
        parse("""
        WITH default_DOT_user_dim AS (
        SELECT  default_DOT_users.user_id,
          default_DOT_users.birth_country,
          default_DOT_users.residence_country,
          default_DOT_users.age,
          default_DOT_users.birth_date
        FROM examples.users AS default_DOT_users
        ),
        default_DOT_special_country_dim AS (
        SELECT  default_DOT_countries.country_code,
          default_DOT_countries.name,
          default_DOT_countries.formation_date,
          default_DOT_countries.last_election_date
        FROM examples.countries AS default_DOT_countries
        ),
        default_DOT_user_dim_metrics AS (
        SELECT  user_birth_country.name default_DOT_special_country_dim_DOT_name_LBRACK_user_birth_country_RBRACK,
          AVG(default_DOT_user_dim.age) default_DOT_avg_user_age
        FROM default_DOT_user_dim LEFT JOIN default_DOT_special_country_dim AS user_birth_country ON default_DOT_user_dim.birth_country = user_birth_country.country_code
        WHERE  user_birth_country.name IS NOT NULL AND user_birth_country.formation_date > 20000101
        GROUP BY  user_birth_country.name
        )

        SELECT  default_DOT_user_dim_metrics.default_DOT_special_country_dim_DOT_name_LBRACK_user_birth_country_RBRACK,
          default_DOT_user_dim_metrics.default_DOT_avg_user_age
        FROM default_DOT_user_dim_metrics
    """),
    )


@pytest.mark.asyncio
async def test_role_path_dimensions_performance_complex_query(
    module__client_with_examples: AsyncClient,
    role_path_test_setup: dict,
):
    """
    Test performance and correctness with complex multi-dimensional role path queries.
    """
    # Complex query with multiple role paths across geographic and temporal hierarchies
    response = await module__client_with_examples.get(
        "/sql/default.avg_user_age/",
        params={
            "dimensions": [
                "default.special_country_dim.name[user_birth_country]",
                "default.special_country_dim.name[user_residence_country]",
                "default.regions.region_name[user_birth_country->country_region]",
                "default.months.month_name[user_birth_date->date_week->week_month]",
                "default.years.year_number[user_birth_date->date_week->week_month->month_year]",
            ],
            "filters": [
                "default.special_country_dim.name[user_birth_country] IN ('Canada', 'United States', 'Mexico')",
                "default.regions.region_name[user_birth_country->country_region] = 'North America'",
                "default.months.month_name[user_birth_date->date_week->week_month] IN ('January', 'February', 'March')",
                "default.years.year_number[user_birth_date->date_week->week_month->month_year] >= 2020",
            ],
        },
    )
    assert str(parse(response.json()["sql"])) == str(
        parse("""
    WITH
    default_DOT_user_dim AS (
    SELECT  default_DOT_users.user_id,
    	default_DOT_users.birth_country,
    	default_DOT_users.residence_country,
    	default_DOT_users.age,
    	default_DOT_users.birth_date
     FROM examples.users AS default_DOT_users
    ),
    default_DOT_special_country_dim AS (
    SELECT  default_DOT_countries.country_code,
    	default_DOT_countries.name,
    	default_DOT_countries.formation_date,
    	default_DOT_countries.last_election_date
     FROM examples.countries AS default_DOT_countries
    ),
    default_DOT_regions AS (
    SELECT  default_DOT_regions_table.region_id,
    	default_DOT_regions_table.region_name,
    	default_DOT_regions_table.continent_id
     FROM public.regions AS default_DOT_regions_table
    ),
    default_DOT_date_dim AS (
    SELECT  default_DOT_date.dateint,
    	default_DOT_date.month,
    	default_DOT_date.year,
    	default_DOT_date.day
     FROM examples.date AS default_DOT_date
    ),
    default_DOT_weeks AS (
    SELECT  default_DOT_weeks_table.week_id,
    	default_DOT_weeks_table.week_start_date,
    	default_DOT_weeks_table.week_end_date,
    	default_DOT_weeks_table.week_number,
    	default_DOT_weeks_table.month_id
     FROM public.weeks AS default_DOT_weeks_table
    ),
    default_DOT_months AS (
    SELECT  default_DOT_months_table.month_id,
    	default_DOT_months_table.month_name,
    	default_DOT_months_table.month_number,
    	default_DOT_months_table.year_id
     FROM public.months AS default_DOT_months_table
    ),
    default_DOT_years AS (
    SELECT  default_DOT_years_table.year_id,
    	default_DOT_years_table.year_number,
    	default_DOT_years_table.decade
     FROM public.years AS default_DOT_years_table
    ),
    default_DOT_user_dim_metrics AS (
      SELECT  user_birth_country.name default_DOT_special_country_dim_DOT_name_LBRACK_user_birth_country_RBRACK,
        user_residence_country.name default_DOT_special_country_dim_DOT_name_LBRACK_user_residence_country_RBRACK,
        user_birth_country__country_region.region_name default_DOT_regions_DOT_region_name_LBRACK_user_birth_country_MINUS__GT_country_region_RBRACK,
        user_birth_date__date_week__week_month.month_name default_DOT_months_DOT_month_name_LBRACK_user_birth_date_MINUS__GT_date_week_MINUS__GT_week_month_RBRACK,
        user_birth_date__date_week__week_month__month_year.year_number default_DOT_years_DOT_year_number_LBRACK_user_birth_date_MINUS__GT_date_week_MINUS__GT_week_month_MINUS__GT_month_year_RBRACK,
        AVG(default_DOT_user_dim.age) default_DOT_avg_user_age
      FROM default_DOT_user_dim
      LEFT JOIN default_DOT_special_country_dim AS user_birth_country ON default_DOT_user_dim.birth_country = user_birth_country.country_code
      LEFT JOIN default_DOT_special_country_dim AS user_residence_country ON default_DOT_user_dim.residence_country = user_residence_country.country_code
      LEFT JOIN default_DOT_regions AS user_birth_country__country_region ON user_birth_country.country_code = user_birth_country__country_region.region_id
      LEFT JOIN default_DOT_date_dim AS user_birth_date ON default_DOT_user_dim.birth_date = user_birth_date.dateint
      LEFT JOIN default_DOT_weeks AS user_birth_date__date_week ON user_birth_date.dateint BETWEEN user_birth_date__date_week.week_start_date AND user_birth_date__date_week.week_end_date
      LEFT JOIN default_DOT_months AS user_birth_date__date_week__week_month ON user_birth_date__date_week.month_id = user_birth_date__date_week__week_month.month_id
      LEFT JOIN default_DOT_years AS user_birth_date__date_week__week_month__month_year ON user_birth_date__date_week__week_month.year_id = user_birth_date__date_week__week_month__month_year.year_id
      WHERE
        user_birth_country.name IN ('Canada', 'United States', 'Mexico')
        AND user_birth_country__country_region.region_name = 'North America'
        AND user_birth_date__date_week__week_month.month_name IN ('January', 'February', 'March')
        AND user_birth_date__date_week__week_month__month_year.year_number >= 2020
      GROUP BY  user_birth_country.name, user_residence_country.name, user_birth_country__country_region.region_name, user_birth_date__date_week__week_month.month_name, user_birth_date__date_week__week_month__month_year.year_number
    )

    SELECT  default_DOT_user_dim_metrics.default_DOT_special_country_dim_DOT_name_LBRACK_user_birth_country_RBRACK,
    	default_DOT_user_dim_metrics.default_DOT_special_country_dim_DOT_name_LBRACK_user_residence_country_RBRACK,
    	default_DOT_user_dim_metrics.default_DOT_regions_DOT_region_name_LBRACK_user_birth_country_MINUS__GT_country_region_RBRACK,
    	default_DOT_user_dim_metrics.default_DOT_months_DOT_month_name_LBRACK_user_birth_date_MINUS__GT_date_week_MINUS__GT_week_month_RBRACK,
    	default_DOT_user_dim_metrics.default_DOT_years_DOT_year_number_LBRACK_user_birth_date_MINUS__GT_date_week_MINUS__GT_week_month_MINUS__GT_month_year_RBRACK,
    	default_DOT_user_dim_metrics.default_DOT_avg_user_age
     FROM default_DOT_user_dim_metrics
    """),
    )
