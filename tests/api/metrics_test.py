"""
Tests for the metrics API.
"""
from fastapi.testclient import TestClient
from sqlmodel import Session, select

from dj.models import AttributeType, ColumnAttribute
from dj.models.column import Column
from dj.models.database import Database
from dj.models.node import Node, NodeRevision, NodeType
from dj.models.table import Table
from dj.sql.parsing.types import FloatType, IntegerType, StringType


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
    assert data["dimensions"] == ["parent.ds", "parent.foo", "parent.user_id"]


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
    response = client_with_examples.get(
        "/metrics/common/dimensions?"
        "metric=default.total_repair_order_discounts"
        "&metric=default.total_repair_cost",
    )
    assert response.status_code == 200
    assert set(response.json()) == set(
        [
            "default.dispatcher.company_name",
            "default.dispatcher.dispatcher_id",
            "default.dispatcher.phone",
            "default.hard_hat.address",
            "default.hard_hat.birth_date",
            "default.hard_hat.city",
            "default.hard_hat.contractor_id",
            "default.hard_hat.country",
            "default.hard_hat.first_name",
            "default.hard_hat.hard_hat_id",
            "default.hard_hat.hire_date",
            "default.hard_hat.last_name",
            "default.hard_hat.manager",
            "default.hard_hat.postal_code",
            "default.hard_hat.state",
            "default.hard_hat.title",
            "default.municipality_dim.contact_name",
            "default.municipality_dim.contact_title",
            "default.municipality_dim.local_region",
            "default.municipality_dim.municipality_id",
            "default.municipality_dim.municipality_type_desc",
            "default.municipality_dim.municipality_type_id",
            "default.municipality_dim.state_id",
            "default.repair_order.dispatched_date",
            "default.repair_order.dispatcher_id",
            "default.repair_order.hard_hat_id",
            "default.repair_order.municipality_id",
            "default.repair_order.order_date",
            "default.repair_order.repair_order_id",
            "default.repair_order.required_date",
            "default.us_state.state_id",
            "default.us_state.state_name",
            "default.us_state.state_region",
            "default.us_state.state_region_description",
            "default.us_state.state_short",
        ],
    )


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
        "default.dispatcher.company_name",
        "default.dispatcher.dispatcher_id",
        "default.dispatcher.phone",
        "default.hard_hat.address",
        "default.hard_hat.birth_date",
        "default.hard_hat.city",
        "default.hard_hat.contractor_id",
        "default.hard_hat.country",
        "default.hard_hat.first_name",
        "default.hard_hat.hard_hat_id",
        "default.hard_hat.hire_date",
        "default.hard_hat.last_name",
        "default.hard_hat.manager",
        "default.hard_hat.postal_code",
        "default.hard_hat.state",
        "default.hard_hat.title",
        "default.municipality_dim.contact_name",
        "default.municipality_dim.contact_title",
        "default.municipality_dim.local_region",
        "default.municipality_dim.municipality_id",
        "default.municipality_dim.municipality_type_desc",
        "default.municipality_dim.municipality_type_id",
        "default.municipality_dim.state_id",
        "default.repair_order.dispatched_date",
        "default.repair_order.dispatcher_id",
        "default.repair_order.hard_hat_id",
        "default.repair_order.municipality_id",
        "default.repair_order.order_date",
        "default.repair_order.repair_order_id",
        "default.repair_order.required_date",
        "default.us_state.state_id",
        "default.us_state.state_name",
        "default.us_state.state_region",
        "default.us_state.state_region_description",
        "default.us_state.state_short",
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
