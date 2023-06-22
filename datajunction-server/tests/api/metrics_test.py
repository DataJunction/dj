"""
Tests for the metrics API.
"""
from fastapi.testclient import TestClient
from sqlmodel import Session, select

from datajunction_server.models import AttributeType, ColumnAttribute
from datajunction_server.models.column import Column
from datajunction_server.models.database import Database
from datajunction_server.models.node import Node, NodeRevision, NodeType
from datajunction_server.models.table import Table
from datajunction_server.sql.parsing.types import FloatType, IntegerType, StringType
from tests.sql.utils import compare_query_strings


def test_read_metrics(client_with_examples: TestClient) -> None:
    """
    Test ``GET /metrics/``.
    """
    response = client_with_examples.get("/metrics/")
    data = response.json()

    assert response.status_code == 200
    assert len(data) > 10


def test_read_metric(session: Session, client: TestClient) -> None:
    """
    Test ``GET /metric/{node_id}/``.
    """
    client.get("/attributes/")
    dimension_attribute = session.exec(
        select(AttributeType).where(AttributeType.name == "dimension"),
    ).one()
    parent_rev = NodeRevision(
        name="parent",
        version="1",
        tables=[
            Table(
                database=Database(name="test", URI="sqlite://"),
                table="A",
                columns=[
                    Column(name="ds", type=StringType()),
                    Column(name="user_id", type=IntegerType()),
                    Column(name="foo", type=FloatType()),
                ],
            ),
        ],
        columns=[
            Column(
                name="ds",
                type=StringType(),
                attributes=[ColumnAttribute(attribute_type=dimension_attribute)],
            ),
            Column(
                name="user_id",
                type=IntegerType(),
                attributes=[ColumnAttribute(attribute_type=dimension_attribute)],
            ),
            Column(
                name="foo",
                type=FloatType(),
                attributes=[ColumnAttribute(attribute_type=dimension_attribute)],
            ),
        ],
    )
    parent_node = Node(
        name=parent_rev.name,
        namespace="default",
        type=NodeType.SOURCE,
        current_version="1",
    )
    parent_rev.node = parent_node

    child_node = Node(
        name="child",
        namespace="default",
        type=NodeType.METRIC,
        current_version="1",
    )
    child_rev = NodeRevision(
        name=child_node.name,
        node=child_node,
        version="1",
        query="SELECT COUNT(*) FROM parent",
        parents=[parent_node],
    )

    session.add(child_rev)
    session.commit()

    response = client.get("/metrics/child/")
    data = response.json()

    assert response.status_code == 200
    assert data["name"] == "child"
    assert data["query"] == "SELECT COUNT(*) FROM parent"
    assert data["dimensions"] == [
        {"name": "parent.ds", "type": "string", "path": ""},
        {"name": "parent.foo", "type": "float", "path": ""},
        {"name": "parent.user_id", "type": "int", "path": ""},
    ]


def test_read_metrics_errors(session: Session, client: TestClient) -> None:
    """
    Test errors on ``GET /metrics/{node_id}/``.
    """
    database = Database(name="test", URI="sqlite://")
    node = Node(
        name="a-metric",
        namespace="default",
        type=NodeType.TRANSFORM,
        current_version="1",
    )
    node_revision = NodeRevision(
        name=node.name,
        node=node,
        version="1",
        query="SELECT 1 AS col",
    )
    session.add(database)
    session.add(node_revision)
    session.execute("CREATE TABLE my_table (one TEXT)")
    session.commit()

    response = client.get("/metrics/foo")
    assert response.status_code == 404
    data = response.json()
    assert data["message"] == "A node with name `foo` does not exist."

    response = client.get("/metrics/a-metric")
    assert response.status_code == 400
    assert response.json() == {"detail": "Not a metric node: `a-metric`"}


def test_common_dimensions(
    client_with_examples: TestClient,
) -> None:
    """
    Test ``GET /metrics/common/dimensions``.
    """
    client_with_examples.post(
        "/nodes/default.hard_hat/columns/birth_date/"
        "?dimension=default.date_dim&dimension_column=dateint",
    )
    client_with_examples.post(
        "/nodes/default.hard_hat/columns/hire_date/"
        "?dimension=default.date_dim&dimension_column=dateint",
    )
    response = client_with_examples.get(
        "/metrics/common/dimensions?"
        "metric=default.total_repair_order_discounts"
        "&metric=default.total_repair_cost",
    )
    assert response.status_code == 200

    assert response.json() == [
        {"name": "default.date_dim.dateint", "path": "birth_date", "type": "timestamp"},
        {"name": "default.date_dim.dateint", "path": "hire_date", "type": "timestamp"},
        {"name": "default.date_dim.day", "path": "birth_date", "type": "int"},
        {"name": "default.date_dim.day", "path": "hire_date", "type": "int"},
        {"name": "default.date_dim.month", "path": "birth_date", "type": "int"},
        {"name": "default.date_dim.month", "path": "hire_date", "type": "int"},
        {"name": "default.date_dim.year", "path": "birth_date", "type": "int"},
        {"name": "default.date_dim.year", "path": "hire_date", "type": "int"},
        {
            "name": "default.dispatcher.company_name",
            "path": "dispatcher_id",
            "type": "string",
        },
        {
            "name": "default.dispatcher.dispatcher_id",
            "path": "dispatcher_id",
            "type": "int",
        },
        {"name": "default.dispatcher.phone", "path": "dispatcher_id", "type": "string"},
        {"name": "default.hard_hat.address", "path": "hard_hat_id", "type": "string"},
        {
            "name": "default.hard_hat.birth_date",
            "path": "hard_hat_id",
            "type": "timestamp",
        },
        {"name": "default.hard_hat.city", "path": "hard_hat_id", "type": "string"},
        {
            "name": "default.hard_hat.contractor_id",
            "path": "hard_hat_id",
            "type": "int",
        },
        {"name": "default.hard_hat.country", "path": "hard_hat_id", "type": "string"},
        {
            "name": "default.hard_hat.first_name",
            "path": "hard_hat_id",
            "type": "string",
        },
        {"name": "default.hard_hat.hard_hat_id", "path": "hard_hat_id", "type": "int"},
        {
            "name": "default.hard_hat.hire_date",
            "path": "hard_hat_id",
            "type": "timestamp",
        },
        {"name": "default.hard_hat.last_name", "path": "hard_hat_id", "type": "string"},
        {"name": "default.hard_hat.manager", "path": "hard_hat_id", "type": "int"},
        {
            "name": "default.hard_hat.postal_code",
            "path": "hard_hat_id",
            "type": "string",
        },
        {"name": "default.hard_hat.state", "path": "hard_hat_id", "type": "string"},
        {"name": "default.hard_hat.title", "path": "hard_hat_id", "type": "string"},
        {
            "name": "default.municipality_dim.contact_name",
            "path": "municipality_id",
            "type": "string",
        },
        {
            "name": "default.municipality_dim.contact_title",
            "path": "municipality_id",
            "type": "string",
        },
        {
            "name": "default.municipality_dim.local_region",
            "path": "municipality_id",
            "type": "string",
        },
        {
            "name": "default.municipality_dim.municipality_id",
            "path": "municipality_id",
            "type": "string",
        },
        {
            "name": "default.municipality_dim.municipality_type_desc",
            "path": "municipality_id",
            "type": "string",
        },
        {
            "name": "default.municipality_dim.municipality_type_id",
            "path": "municipality_id",
            "type": "string",
        },
        {
            "name": "default.municipality_dim.state_id",
            "path": "municipality_id",
            "type": "int",
        },
        {
            "name": "default.repair_order.dispatched_date",
            "path": "repair_order_id",
            "type": "timestamp",
        },
        {
            "name": "default.repair_order.dispatcher_id",
            "path": "repair_order_id",
            "type": "int",
        },
        {
            "name": "default.repair_order.hard_hat_id",
            "path": "repair_order_id",
            "type": "int",
        },
        {
            "name": "default.repair_order.municipality_id",
            "path": "repair_order_id",
            "type": "string",
        },
        {
            "name": "default.repair_order.order_date",
            "path": "repair_order_id",
            "type": "timestamp",
        },
        {
            "name": "default.repair_order.repair_order_id",
            "path": "repair_order_id",
            "type": "int",
        },
        {
            "name": "default.repair_order.required_date",
            "path": "repair_order_id",
            "type": "timestamp",
        },
        {
            "name": "default.repair_order_details.repair_order_id",
            "path": "",
            "type": "int",
        },
        {"name": "default.us_state.state_id", "path": "state", "type": "int"},
        {"name": "default.us_state.state_name", "path": "state", "type": "string"},
        {"name": "default.us_state.state_region", "path": "state", "type": "int"},
        {
            "name": "default.us_state.state_region_description",
            "path": "state",
            "type": "string",
        },
        {"name": "default.us_state.state_short", "path": "state", "type": "string"},
    ]


def test_raise_common_dimensions_not_a_metric_node(
    client_with_examples: TestClient,
) -> None:
    """
    Test raising ``GET /metrics/common/dimensions`` when not a metric node
    """
    response = client_with_examples.get(
        "/metrics/common/dimensions?"
        "metric=default.total_repair_order_discounts"
        "&metric=default.payment_type",
    )
    assert response.status_code == 500
    assert response.json()["message"] == "Not a metric node: default.payment_type"


def test_raise_common_dimensions_metric_not_found(
    client_with_examples: TestClient,
) -> None:
    """
    Test raising ``GET /metrics/common/dimensions`` when metric not found
    """
    response = client_with_examples.get(
        "/metrics/common/dimensions?metric=default.foo&metric=default.bar",
    )
    assert response.status_code == 500
    assert response.json() == {
        "message": "Metric node not found: default.foo\nMetric node not found: default.bar",
        "errors": [
            {
                "code": 203,
                "message": "Metric node not found: default.foo",
                "debug": None,
                "context": "",
            },
            {
                "code": 203,
                "message": "Metric node not found: default.bar",
                "debug": None,
                "context": "",
            },
        ],
        "warnings": [],
    }


def test_get_dimensions(client_with_examples: TestClient):
    """
    Testing get dimensions for a metric
    """
    response = client_with_examples.get("/metrics/default.avg_repair_price/")

    data = response.json()
    assert data["dimensions"] == [
        {
            "path": "dispatcher_id",
            "name": "default.dispatcher.company_name",
            "type": "string",
        },
        {
            "path": "dispatcher_id",
            "name": "default.dispatcher.dispatcher_id",
            "type": "int",
        },
        {"path": "dispatcher_id", "name": "default.dispatcher.phone", "type": "string"},
        {"path": "hard_hat_id", "name": "default.hard_hat.address", "type": "string"},
        {
            "path": "hard_hat_id",
            "name": "default.hard_hat.birth_date",
            "type": "timestamp",
        },
        {"path": "hard_hat_id", "name": "default.hard_hat.city", "type": "string"},
        {
            "path": "hard_hat_id",
            "name": "default.hard_hat.contractor_id",
            "type": "int",
        },
        {"path": "hard_hat_id", "name": "default.hard_hat.country", "type": "string"},
        {
            "path": "hard_hat_id",
            "name": "default.hard_hat.first_name",
            "type": "string",
        },
        {"path": "hard_hat_id", "name": "default.hard_hat.hard_hat_id", "type": "int"},
        {
            "path": "hard_hat_id",
            "name": "default.hard_hat.hire_date",
            "type": "timestamp",
        },
        {"path": "hard_hat_id", "name": "default.hard_hat.last_name", "type": "string"},
        {"path": "hard_hat_id", "name": "default.hard_hat.manager", "type": "int"},
        {
            "path": "hard_hat_id",
            "name": "default.hard_hat.postal_code",
            "type": "string",
        },
        {"path": "hard_hat_id", "name": "default.hard_hat.state", "type": "string"},
        {"path": "hard_hat_id", "name": "default.hard_hat.title", "type": "string"},
        {
            "path": "municipality_id",
            "name": "default.municipality_dim.contact_name",
            "type": "string",
        },
        {
            "path": "municipality_id",
            "name": "default.municipality_dim.contact_title",
            "type": "string",
        },
        {
            "path": "municipality_id",
            "name": "default.municipality_dim.local_region",
            "type": "string",
        },
        {
            "path": "municipality_id",
            "name": "default.municipality_dim.municipality_id",
            "type": "string",
        },
        {
            "path": "municipality_id",
            "name": "default.municipality_dim.municipality_type_desc",
            "type": "string",
        },
        {
            "path": "municipality_id",
            "name": "default.municipality_dim.municipality_type_id",
            "type": "string",
        },
        {
            "path": "municipality_id",
            "name": "default.municipality_dim.state_id",
            "type": "int",
        },
        {
            "path": "repair_order_id",
            "name": "default.repair_order.dispatched_date",
            "type": "timestamp",
        },
        {
            "path": "repair_order_id",
            "name": "default.repair_order.dispatcher_id",
            "type": "int",
        },
        {
            "path": "repair_order_id",
            "name": "default.repair_order.hard_hat_id",
            "type": "int",
        },
        {
            "path": "repair_order_id",
            "name": "default.repair_order.municipality_id",
            "type": "string",
        },
        {
            "path": "repair_order_id",
            "name": "default.repair_order.order_date",
            "type": "timestamp",
        },
        {
            "path": "repair_order_id",
            "name": "default.repair_order.repair_order_id",
            "type": "int",
        },
        {
            "path": "repair_order_id",
            "name": "default.repair_order.required_date",
            "type": "timestamp",
        },
        {
            "path": "",
            "name": "default.repair_order_details.repair_order_id",
            "type": "int",
        },
        {"path": "state", "name": "default.us_state.state_id", "type": "int"},
        {"path": "state", "name": "default.us_state.state_name", "type": "string"},
        {"path": "state", "name": "default.us_state.state_region", "type": "int"},
        {
            "path": "state",
            "name": "default.us_state.state_region_description",
            "type": "string",
        },
        {"path": "state", "name": "default.us_state.state_short", "type": "string"},
    ]


def test_get_multi_link_dimensions(
    client_with_examples: TestClient,
):
    """
    In some cases, the same dimension may be linked to different columns on a node.
    The returned dimension attributes should contain a prefix of the column in order to
    distinguish between the source of the dimension.
    """
    client_with_examples.post(
        "/nodes/default.hard_hat/columns/birth_date/"
        "?dimension=default.date_dim&dimension_column=dateint",
    )
    client_with_examples.post(
        "/nodes/default.hard_hat/columns/hire_date/"
        "?dimension=default.date_dim&dimension_column=dateint",
    )
    response = client_with_examples.get("/metrics/default.num_repair_orders/")
    assert response.json()["dimensions"] == [
        {"path": "birth_date", "name": "default.date_dim.dateint", "type": "timestamp"},
        {"path": "hire_date", "name": "default.date_dim.dateint", "type": "timestamp"},
        {"path": "birth_date", "name": "default.date_dim.day", "type": "int"},
        {"path": "hire_date", "name": "default.date_dim.day", "type": "int"},
        {"path": "birth_date", "name": "default.date_dim.month", "type": "int"},
        {"path": "hire_date", "name": "default.date_dim.month", "type": "int"},
        {"path": "birth_date", "name": "default.date_dim.year", "type": "int"},
        {"path": "hire_date", "name": "default.date_dim.year", "type": "int"},
        {
            "path": "dispatcher_id",
            "name": "default.dispatcher.company_name",
            "type": "string",
        },
        {
            "path": "dispatcher_id",
            "name": "default.dispatcher.dispatcher_id",
            "type": "int",
        },
        {"path": "dispatcher_id", "name": "default.dispatcher.phone", "type": "string"},
        {"path": "hard_hat_id", "name": "default.hard_hat.address", "type": "string"},
        {
            "path": "hard_hat_id",
            "name": "default.hard_hat.birth_date",
            "type": "timestamp",
        },
        {"path": "hard_hat_id", "name": "default.hard_hat.city", "type": "string"},
        {
            "path": "hard_hat_id",
            "name": "default.hard_hat.contractor_id",
            "type": "int",
        },
        {"path": "hard_hat_id", "name": "default.hard_hat.country", "type": "string"},
        {
            "path": "hard_hat_id",
            "name": "default.hard_hat.first_name",
            "type": "string",
        },
        {"path": "hard_hat_id", "name": "default.hard_hat.hard_hat_id", "type": "int"},
        {
            "path": "hard_hat_id",
            "name": "default.hard_hat.hire_date",
            "type": "timestamp",
        },
        {"path": "hard_hat_id", "name": "default.hard_hat.last_name", "type": "string"},
        {"path": "hard_hat_id", "name": "default.hard_hat.manager", "type": "int"},
        {
            "path": "hard_hat_id",
            "name": "default.hard_hat.postal_code",
            "type": "string",
        },
        {"path": "hard_hat_id", "name": "default.hard_hat.state", "type": "string"},
        {"path": "hard_hat_id", "name": "default.hard_hat.title", "type": "string"},
        {
            "path": "municipality_id",
            "name": "default.municipality_dim.contact_name",
            "type": "string",
        },
        {
            "path": "municipality_id",
            "name": "default.municipality_dim.contact_title",
            "type": "string",
        },
        {
            "path": "municipality_id",
            "name": "default.municipality_dim.local_region",
            "type": "string",
        },
        {
            "path": "municipality_id",
            "name": "default.municipality_dim.municipality_id",
            "type": "string",
        },
        {
            "path": "municipality_id",
            "name": "default.municipality_dim.municipality_type_desc",
            "type": "string",
        },
        {
            "path": "municipality_id",
            "name": "default.municipality_dim.municipality_type_id",
            "type": "string",
        },
        {
            "path": "municipality_id",
            "name": "default.municipality_dim.state_id",
            "type": "int",
        },
        {"path": "", "name": "default.repair_orders.dispatcher_id", "type": "int"},
        {"path": "", "name": "default.repair_orders.hard_hat_id", "type": "int"},
        {"path": "", "name": "default.repair_orders.municipality_id", "type": "string"},
        {"path": "state", "name": "default.us_state.state_id", "type": "int"},
        {"path": "state", "name": "default.us_state.state_name", "type": "string"},
        {"path": "state", "name": "default.us_state.state_region", "type": "int"},
        {
            "path": "state",
            "name": "default.us_state.state_region_description",
            "type": "string",
        },
        {"path": "state", "name": "default.us_state.state_short", "type": "string"},
    ]


def test_type_inference_structs(client_with_examples: TestClient):
    """
    Testing type resolution for structs select
    """
    client_with_examples.post(
        "/nodes/source/",
        json={
            "columns": [
                {
                    "name": "counts",
                    "type": "struct<a string, b bigint>",
                },
            ],
            "description": "Collection of dreams",
            "mode": "published",
            "name": "basic.dreams",
            "catalog": "public",
            "schema_": "basic",
            "table": "dreams",
        },
    )

    response = client_with_examples.post(
        "/nodes/metric/",
        json={
            "query": "SELECT SUM(counts.b) FROM basic.dreams",
            "description": "Dream Counts",
            "mode": "published",
            "name": "basic.dream_count",
        },
    )
    response.json()


def test_metric_expression_auto_aliased(client_with_examples: TestClient):
    """
    Testing that a metric's expression column is automatically aliased
    """
    client_with_examples.post(
        "/nodes/source/",
        json={
            "columns": [
                {
                    "name": "counts",
                    "type": "struct<a string, b bigint>",
                },
            ],
            "description": "Collection of dreams",
            "mode": "published",
            "name": "basic.dreams",
            "catalog": "public",
            "schema_": "basic",
            "table": "dreams",
        },
    )

    response = client_with_examples.post(
        "/nodes/metric/",
        json={
            "query": "SELECT SUM(counts.b) + SUM(counts.b) FROM basic.dreams",
            "description": "Dream Counts",
            "mode": "published",
            "name": "basic.dream_count",
        },
    )
    assert response.status_code == 201
    assert compare_query_strings(
        response.json()["query"],
        "SELECT  SUM(counts.b) + SUM(counts.b) basic_DOT_dream_count \n FROM basic.dreams\n",
    )


def test_raise_on_malformated_expression_alias(client_with_examples: TestClient):
    """
    Testing raising when an invalid alias is used for a metric expression
    """
    client_with_examples.post(
        "/nodes/source/",
        json={
            "columns": [
                {
                    "name": "counts",
                    "type": "struct<a string, b bigint>",
                },
            ],
            "description": "Collection of dreams",
            "mode": "published",
            "name": "basic.dreams",
            "catalog": "public",
            "schema_": "basic",
            "table": "dreams",
        },
    )

    response = client_with_examples.post(
        "/nodes/metric/",
        json={
            "query": "SELECT SUM(counts.b) as foo FROM basic.dreams",
            "description": "Dream Counts",
            "mode": "published",
            "name": "basic.dream_count",
        },
    )
    assert response.status_code == 422
    assert (
        "Invalid Metric. The expression in the projection cannot "
        "have alias different from the node name. Got `foo` "
        "but expected `basic_DOT_dream_count`"
    ) in response.json()["message"]


def test_raise_on_multiple_expressions(client_with_examples: TestClient):
    """
    Testing raising when there is more than one expression
    """
    client_with_examples.post(
        "/nodes/source/",
        json={
            "columns": [
                {
                    "name": "counts",
                    "type": "struct<a string, b bigint>",
                },
            ],
            "description": "Collection of dreams",
            "mode": "published",
            "name": "basic.dreams",
            "catalog": "public",
            "schema_": "basic",
            "table": "dreams",
        },
    )

    response = client_with_examples.post(
        "/nodes/metric/",
        json={
            "query": "SELECT SUM(counts.b), COUNT(counts.b) FROM basic.dreams",
            "description": "Dream Counts",
            "mode": "published",
            "name": "basic.dream_count",
        },
    )
    assert response.status_code == 422
    assert (
        "Metric queries can only have a single expression, found 2"
    ) in response.json()["message"]
