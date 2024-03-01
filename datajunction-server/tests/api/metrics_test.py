# pylint: disable=too-many-lines
"""
Tests for the metrics API.
"""

from fastapi.testclient import TestClient
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from datajunction_server.database.attributetype import AttributeType, ColumnAttribute
from datajunction_server.database.column import Column
from datajunction_server.database.database import Database
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.parsing.types import FloatType, IntegerType, StringType


def test_read_metrics(client_with_roads: TestClient) -> None:
    """
    Test ``GET /metrics/``.
    """
    response = client_with_roads.get("/metrics/")
    data = response.json()

    assert response.status_code == 200
    assert len(data) > 5

    response = client_with_roads.get("/metrics/default.num_repair_orders")
    data = response.json()
    assert data["metric_metadata"] == {
        "direction": "higher_is_better",
        "unit": {
            "abbreviation": None,
            "category": None,
            "description": None,
            "label": "Dollar",
            "name": "DOLLAR",
        },
    }
    assert data["upstream_node"] == "default.repair_orders_fact"
    assert data["expression"] == "count(repair_order_id)"


def test_read_metric(session: Session, client: TestClient) -> None:
    """
    Test ``GET /metric/{node_id}/``.
    """
    client.get("/attributes/")
    dimension_attribute = session.execute(
        select(AttributeType).where(AttributeType.name == "dimension"),
    ).scalar_one()
    parent_rev = NodeRevision(
        name="parent",
        type=NodeType.SOURCE,
        version="1",
        columns=[
            Column(
                name="ds",
                type=StringType(),
                attributes=[ColumnAttribute(attribute_type=dimension_attribute)],
                order=0,
            ),
            Column(
                name="user_id",
                type=IntegerType(),
                attributes=[ColumnAttribute(attribute_type=dimension_attribute)],
                order=2,
            ),
            Column(
                name="foo",
                type=FloatType(),
                attributes=[ColumnAttribute(attribute_type=dimension_attribute)],
                order=3,
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
        type=child_node.type,
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
        {
            "is_primary_key": False,
            "name": "parent.ds",
            "node_display_name": "Parent",
            "node_name": "parent",
            "path": [],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "parent.foo",
            "node_display_name": "Parent",
            "node_name": "parent",
            "path": [],
            "type": "float",
        },
        {
            "is_primary_key": False,
            "name": "parent.user_id",
            "node_display_name": "Parent",
            "node_name": "parent",
            "path": [],
            "type": "int",
        },
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
        type=node.type,
        node=node,
        version="1",
        query="SELECT 1 AS col",
    )
    session.add(database)
    session.add(node_revision)
    session.execute(text("CREATE TABLE my_table (one TEXT)"))
    session.commit()

    response = client.get("/metrics/foo")
    assert response.status_code == 404
    data = response.json()
    assert data["message"] == "A node with name `foo` does not exist."

    response = client.get("/metrics/a-metric")
    assert response.status_code == 400
    assert response.json() == {"detail": "Not a metric node: `a-metric`"}


def test_common_dimensions(
    client_with_roads: TestClient,
) -> None:
    """
    Test ``GET /metrics/common/dimensions``.
    """
    response = client_with_roads.get(
        "/metrics/common/dimensions?"
        "metric=default.total_repair_order_discounts"
        "&metric=default.total_repair_cost",
    )
    assert response.status_code == 200

    assert response.json() == [
        {
            "is_primary_key": False,
            "name": "default.dispatcher.company_name",
            "node_display_name": "Default: Dispatcher",
            "node_name": "default.dispatcher",
            "path": ["default.repair_orders_fact"],
            "type": "string",
        },
        {
            "is_primary_key": True,
            "name": "default.dispatcher.dispatcher_id",
            "node_display_name": "Default: Dispatcher",
            "node_name": "default.dispatcher",
            "path": ["default.repair_orders_fact"],
            "type": "int",
        },
        {
            "is_primary_key": False,
            "name": "default.dispatcher.phone",
            "node_display_name": "Default: Dispatcher",
            "node_name": "default.dispatcher",
            "path": ["default.repair_orders_fact"],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "default.hard_hat.address",
            "node_display_name": "Default: Hard Hat",
            "node_name": "default.hard_hat",
            "path": ["default.repair_orders_fact"],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "default.hard_hat.birth_date",
            "node_display_name": "Default: Hard Hat",
            "node_name": "default.hard_hat",
            "path": ["default.repair_orders_fact"],
            "type": "timestamp",
        },
        {
            "is_primary_key": False,
            "name": "default.hard_hat.city",
            "node_display_name": "Default: Hard Hat",
            "node_name": "default.hard_hat",
            "path": ["default.repair_orders_fact"],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "default.hard_hat.contractor_id",
            "node_display_name": "Default: Hard Hat",
            "node_name": "default.hard_hat",
            "path": ["default.repair_orders_fact"],
            "type": "int",
        },
        {
            "is_primary_key": False,
            "name": "default.hard_hat.country",
            "node_display_name": "Default: Hard Hat",
            "node_name": "default.hard_hat",
            "path": ["default.repair_orders_fact"],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "default.hard_hat.first_name",
            "node_display_name": "Default: Hard Hat",
            "node_name": "default.hard_hat",
            "path": ["default.repair_orders_fact"],
            "type": "string",
        },
        {
            "is_primary_key": True,
            "name": "default.hard_hat.hard_hat_id",
            "node_display_name": "Default: Hard Hat",
            "node_name": "default.hard_hat",
            "path": ["default.repair_orders_fact"],
            "type": "int",
        },
        {
            "is_primary_key": False,
            "name": "default.hard_hat.hire_date",
            "node_display_name": "Default: Hard Hat",
            "node_name": "default.hard_hat",
            "path": ["default.repair_orders_fact"],
            "type": "timestamp",
        },
        {
            "is_primary_key": False,
            "name": "default.hard_hat.last_name",
            "node_display_name": "Default: Hard Hat",
            "node_name": "default.hard_hat",
            "path": ["default.repair_orders_fact"],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "default.hard_hat.manager",
            "node_display_name": "Default: Hard Hat",
            "node_name": "default.hard_hat",
            "path": ["default.repair_orders_fact"],
            "type": "int",
        },
        {
            "is_primary_key": False,
            "name": "default.hard_hat.postal_code",
            "node_display_name": "Default: Hard Hat",
            "node_name": "default.hard_hat",
            "path": ["default.repair_orders_fact"],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "default.hard_hat.state",
            "node_display_name": "Default: Hard Hat",
            "node_name": "default.hard_hat",
            "path": ["default.repair_orders_fact"],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "default.hard_hat.title",
            "node_display_name": "Default: Hard Hat",
            "node_name": "default.hard_hat",
            "path": ["default.repair_orders_fact"],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "default.municipality_dim.contact_name",
            "node_display_name": "Default: Municipality Dim",
            "node_name": "default.municipality_dim",
            "path": ["default.repair_orders_fact"],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "default.municipality_dim.contact_title",
            "node_display_name": "Default: Municipality Dim",
            "node_name": "default.municipality_dim",
            "path": ["default.repair_orders_fact"],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "default.municipality_dim.local_region",
            "node_display_name": "Default: Municipality Dim",
            "node_name": "default.municipality_dim",
            "path": ["default.repair_orders_fact"],
            "type": "string",
        },
        {
            "is_primary_key": True,
            "name": "default.municipality_dim.municipality_id",
            "node_display_name": "Default: Municipality Dim",
            "node_name": "default.municipality_dim",
            "path": ["default.repair_orders_fact"],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "default.municipality_dim.municipality_type_desc",
            "node_display_name": "Default: Municipality Dim",
            "node_name": "default.municipality_dim",
            "path": ["default.repair_orders_fact"],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "default.municipality_dim.municipality_type_id",
            "node_display_name": "Default: Municipality Dim",
            "node_name": "default.municipality_dim",
            "path": ["default.repair_orders_fact"],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "default.municipality_dim.state_id",
            "node_display_name": "Default: Municipality Dim",
            "node_name": "default.municipality_dim",
            "path": ["default.repair_orders_fact"],
            "type": "int",
        },
        {
            "is_primary_key": False,
            "name": "default.us_state.state_id",
            "node_display_name": "Default: Us State",
            "node_name": "default.us_state",
            "path": [
                "default.repair_orders_fact",
                "default.hard_hat",
            ],
            "type": "int",
        },
        {
            "is_primary_key": False,
            "name": "default.us_state.state_name",
            "node_display_name": "Default: Us State",
            "node_name": "default.us_state",
            "path": [
                "default.repair_orders_fact",
                "default.hard_hat",
            ],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "default.us_state.state_region",
            "node_display_name": "Default: Us State",
            "node_name": "default.us_state",
            "path": [
                "default.repair_orders_fact",
                "default.hard_hat",
            ],
            "type": "int",
        },
        {
            "is_primary_key": True,
            "name": "default.us_state.state_short",
            "node_display_name": "Default: Us State",
            "node_name": "default.us_state",
            "path": [
                "default.repair_orders_fact",
                "default.hard_hat",
            ],
            "type": "string",
        },
    ]


def test_no_common_dimensions(
    client_example_loader: TestClient,
) -> None:
    """
    Test getting common dimensions for metrics that have none in common
    """
    custom_client = client_example_loader(["BASIC", "ROADS"])
    response = custom_client.get(
        "/metrics/common/dimensions?"
        "metric=basic.num_comments&metric=default.total_repair_order_discounts"
        "&metric=default.total_repair_cost",
    )
    assert response.status_code == 200
    assert response.json() == []


def test_raise_common_dimensions_not_a_metric_node(
    client_example_loader: TestClient,
) -> None:
    """
    Test raising ``GET /metrics/common/dimensions`` when not a metric node
    """
    custom_client = client_example_loader(["ROADS", "ACCOUNT_REVENUE"])
    response = custom_client.get(
        "/metrics/common/dimensions?"
        "metric=default.total_repair_order_discounts"
        "&metric=default.payment_type",
    )
    assert response.status_code == 500
    assert response.json()["message"] == "Not a metric node: default.payment_type"


def test_raise_common_dimensions_metric_not_found(
    client_with_service_setup: TestClient,
) -> None:
    """
    Test raising ``GET /metrics/common/dimensions`` when metric not found
    """
    response = client_with_service_setup.get(
        "/metrics/common/dimensions?metric=default.foo&metric=default.bar",
    )
    assert response.status_code == 500
    assert response.json() == {
        "errors": [
            {
                "code": 203,
                "context": "",
                "debug": None,
                "message": "Metric nodes not found: default.foo,default.bar",
            },
        ],
        "message": "Metric nodes not found: default.foo,default.bar",
        "warnings": [],
    }


def test_get_dimensions(client_with_roads: TestClient):
    """
    Testing get dimensions for a metric
    """
    response = client_with_roads.get("/metrics/default.avg_repair_price/")

    data = response.json()
    assert data["dimensions"] == [
        {
            "is_primary_key": False,
            "name": "default.dispatcher.company_name",
            "node_display_name": "Default: Dispatcher",
            "node_name": "default.dispatcher",
            "path": ["default.repair_orders_fact"],
            "type": "string",
        },
        {
            "is_primary_key": True,
            "name": "default.dispatcher.dispatcher_id",
            "node_display_name": "Default: Dispatcher",
            "node_name": "default.dispatcher",
            "path": ["default.repair_orders_fact"],
            "type": "int",
        },
        {
            "is_primary_key": False,
            "name": "default.dispatcher.phone",
            "node_display_name": "Default: Dispatcher",
            "node_name": "default.dispatcher",
            "path": ["default.repair_orders_fact"],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "default.hard_hat.address",
            "node_display_name": "Default: Hard Hat",
            "node_name": "default.hard_hat",
            "path": ["default.repair_orders_fact"],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "default.hard_hat.birth_date",
            "node_display_name": "Default: Hard Hat",
            "node_name": "default.hard_hat",
            "path": ["default.repair_orders_fact"],
            "type": "timestamp",
        },
        {
            "is_primary_key": False,
            "name": "default.hard_hat.city",
            "node_display_name": "Default: Hard Hat",
            "node_name": "default.hard_hat",
            "path": ["default.repair_orders_fact"],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "default.hard_hat.contractor_id",
            "node_display_name": "Default: Hard Hat",
            "node_name": "default.hard_hat",
            "path": ["default.repair_orders_fact"],
            "type": "int",
        },
        {
            "is_primary_key": False,
            "name": "default.hard_hat.country",
            "node_display_name": "Default: Hard Hat",
            "node_name": "default.hard_hat",
            "path": ["default.repair_orders_fact"],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "default.hard_hat.first_name",
            "node_display_name": "Default: Hard Hat",
            "node_name": "default.hard_hat",
            "path": ["default.repair_orders_fact"],
            "type": "string",
        },
        {
            "is_primary_key": True,
            "name": "default.hard_hat.hard_hat_id",
            "node_display_name": "Default: Hard Hat",
            "node_name": "default.hard_hat",
            "path": ["default.repair_orders_fact"],
            "type": "int",
        },
        {
            "is_primary_key": False,
            "name": "default.hard_hat.hire_date",
            "node_display_name": "Default: Hard Hat",
            "node_name": "default.hard_hat",
            "path": ["default.repair_orders_fact"],
            "type": "timestamp",
        },
        {
            "is_primary_key": False,
            "name": "default.hard_hat.last_name",
            "node_display_name": "Default: Hard Hat",
            "node_name": "default.hard_hat",
            "path": ["default.repair_orders_fact"],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "default.hard_hat.manager",
            "node_display_name": "Default: Hard Hat",
            "node_name": "default.hard_hat",
            "path": ["default.repair_orders_fact"],
            "type": "int",
        },
        {
            "is_primary_key": False,
            "name": "default.hard_hat.postal_code",
            "node_display_name": "Default: Hard Hat",
            "node_name": "default.hard_hat",
            "path": ["default.repair_orders_fact"],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "default.hard_hat.state",
            "node_display_name": "Default: Hard Hat",
            "node_name": "default.hard_hat",
            "path": ["default.repair_orders_fact"],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "default.hard_hat.title",
            "node_display_name": "Default: Hard Hat",
            "node_name": "default.hard_hat",
            "path": ["default.repair_orders_fact"],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "default.municipality_dim.contact_name",
            "node_display_name": "Default: Municipality Dim",
            "node_name": "default.municipality_dim",
            "path": ["default.repair_orders_fact"],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "default.municipality_dim.contact_title",
            "node_display_name": "Default: Municipality Dim",
            "node_name": "default.municipality_dim",
            "path": ["default.repair_orders_fact"],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "default.municipality_dim.local_region",
            "node_display_name": "Default: Municipality Dim",
            "node_name": "default.municipality_dim",
            "path": ["default.repair_orders_fact"],
            "type": "string",
        },
        {
            "is_primary_key": True,
            "name": "default.municipality_dim.municipality_id",
            "node_display_name": "Default: Municipality Dim",
            "node_name": "default.municipality_dim",
            "path": ["default.repair_orders_fact"],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "default.municipality_dim.municipality_type_desc",
            "node_display_name": "Default: Municipality Dim",
            "node_name": "default.municipality_dim",
            "path": ["default.repair_orders_fact"],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "default.municipality_dim.municipality_type_id",
            "node_display_name": "Default: Municipality Dim",
            "node_name": "default.municipality_dim",
            "path": ["default.repair_orders_fact"],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "default.municipality_dim.state_id",
            "node_display_name": "Default: Municipality Dim",
            "node_name": "default.municipality_dim",
            "path": ["default.repair_orders_fact"],
            "type": "int",
        },
        {
            "is_primary_key": False,
            "name": "default.us_state.state_id",
            "node_display_name": "Default: Us State",
            "node_name": "default.us_state",
            "path": [
                "default.repair_orders_fact",
                "default.hard_hat",
            ],
            "type": "int",
        },
        {
            "is_primary_key": False,
            "name": "default.us_state.state_name",
            "node_display_name": "Default: Us State",
            "node_name": "default.us_state",
            "path": [
                "default.repair_orders_fact",
                "default.hard_hat",
            ],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "default.us_state.state_region",
            "node_display_name": "Default: Us State",
            "node_name": "default.us_state",
            "path": [
                "default.repair_orders_fact",
                "default.hard_hat",
            ],
            "type": "int",
        },
        {
            "is_primary_key": True,
            "name": "default.us_state.state_short",
            "node_display_name": "Default: Us State",
            "node_name": "default.us_state",
            "path": [
                "default.repair_orders_fact",
                "default.hard_hat",
            ],
            "type": "string",
        },
    ]


def test_get_multi_link_dimensions(
    client_example_loader: TestClient,
):
    """
    In some cases, the same dimension may be linked to different columns on a node.
    The returned dimension attributes should the join path between the given dimension
    attribute and the original node, in order to help disambiguate the source of the dimension.
    """
    custom_client = client_example_loader(["DIMENSION_LINK"])
    response = custom_client.get("/metrics/default.avg_user_age/")
    assert response.json()["dimensions"] == [
        {
            "is_primary_key": True,
            "name": "default.date_dim.dateint[birth_country->formation_date]",
            "node_display_name": "Default: Date Dim",
            "node_name": "default.date_dim",
            "path": [
                "default.user_dim.birth_country",
                "default.special_country_dim.formation_date",
            ],
            "type": "int",
        },
        {
            "is_primary_key": True,
            "name": "default.date_dim.dateint[birth_country->last_election_date]",
            "node_display_name": "Default: Date Dim",
            "node_name": "default.date_dim",
            "path": [
                "default.user_dim.birth_country",
                "default.special_country_dim.last_election_date",
            ],
            "type": "int",
        },
        {
            "is_primary_key": True,
            "name": "default.date_dim.dateint[residence_country->formation_date]",
            "node_display_name": "Default: Date Dim",
            "node_name": "default.date_dim",
            "path": [
                "default.user_dim.residence_country",
                "default.special_country_dim.formation_date",
            ],
            "type": "int",
        },
        {
            "is_primary_key": True,
            "name": "default.date_dim.dateint[residence_country->last_election_date]",
            "node_display_name": "Default: Date Dim",
            "node_name": "default.date_dim",
            "path": [
                "default.user_dim.residence_country",
                "default.special_country_dim.last_election_date",
            ],
            "type": "int",
        },
        {
            "is_primary_key": False,
            "name": "default.date_dim.day[birth_country->formation_date]",
            "node_display_name": "Default: Date Dim",
            "node_name": "default.date_dim",
            "path": [
                "default.user_dim.birth_country",
                "default.special_country_dim.formation_date",
            ],
            "type": "int",
        },
        {
            "is_primary_key": False,
            "name": "default.date_dim.day[birth_country->last_election_date]",
            "node_display_name": "Default: Date Dim",
            "node_name": "default.date_dim",
            "path": [
                "default.user_dim.birth_country",
                "default.special_country_dim.last_election_date",
            ],
            "type": "int",
        },
        {
            "is_primary_key": False,
            "name": "default.date_dim.day[residence_country->formation_date]",
            "node_display_name": "Default: Date Dim",
            "node_name": "default.date_dim",
            "path": [
                "default.user_dim.residence_country",
                "default.special_country_dim.formation_date",
            ],
            "type": "int",
        },
        {
            "is_primary_key": False,
            "name": "default.date_dim.day[residence_country->last_election_date]",
            "node_display_name": "Default: Date Dim",
            "node_name": "default.date_dim",
            "path": [
                "default.user_dim.residence_country",
                "default.special_country_dim.last_election_date",
            ],
            "type": "int",
        },
        {
            "is_primary_key": False,
            "name": "default.date_dim.month[birth_country->formation_date]",
            "node_display_name": "Default: Date Dim",
            "node_name": "default.date_dim",
            "path": [
                "default.user_dim.birth_country",
                "default.special_country_dim.formation_date",
            ],
            "type": "int",
        },
        {
            "is_primary_key": False,
            "name": "default.date_dim.month[birth_country->last_election_date]",
            "node_display_name": "Default: Date Dim",
            "node_name": "default.date_dim",
            "path": [
                "default.user_dim.birth_country",
                "default.special_country_dim.last_election_date",
            ],
            "type": "int",
        },
        {
            "is_primary_key": False,
            "name": "default.date_dim.month[residence_country->formation_date]",
            "node_display_name": "Default: Date Dim",
            "node_name": "default.date_dim",
            "path": [
                "default.user_dim.residence_country",
                "default.special_country_dim.formation_date",
            ],
            "type": "int",
        },
        {
            "is_primary_key": False,
            "name": "default.date_dim.month[residence_country->last_election_date]",
            "node_display_name": "Default: Date Dim",
            "node_name": "default.date_dim",
            "path": [
                "default.user_dim.residence_country",
                "default.special_country_dim.last_election_date",
            ],
            "type": "int",
        },
        {
            "is_primary_key": False,
            "name": "default.date_dim.year[birth_country->formation_date]",
            "node_display_name": "Default: Date Dim",
            "node_name": "default.date_dim",
            "path": [
                "default.user_dim.birth_country",
                "default.special_country_dim.formation_date",
            ],
            "type": "int",
        },
        {
            "is_primary_key": False,
            "name": "default.date_dim.year[birth_country->last_election_date]",
            "node_display_name": "Default: Date Dim",
            "node_name": "default.date_dim",
            "path": [
                "default.user_dim.birth_country",
                "default.special_country_dim.last_election_date",
            ],
            "type": "int",
        },
        {
            "is_primary_key": False,
            "name": "default.date_dim.year[residence_country->formation_date]",
            "node_display_name": "Default: Date Dim",
            "node_name": "default.date_dim",
            "path": [
                "default.user_dim.residence_country",
                "default.special_country_dim.formation_date",
            ],
            "type": "int",
        },
        {
            "is_primary_key": False,
            "name": "default.date_dim.year[residence_country->last_election_date]",
            "node_display_name": "Default: Date Dim",
            "node_name": "default.date_dim",
            "path": [
                "default.user_dim.residence_country",
                "default.special_country_dim.last_election_date",
            ],
            "type": "int",
        },
        {
            "is_primary_key": True,
            "name": "default.special_country_dim.country_code[birth_country]",
            "node_display_name": "Default: Special Country Dim",
            "node_name": "default.special_country_dim",
            "path": ["default.user_dim.birth_country"],
            "type": "string",
        },
        {
            "is_primary_key": True,
            "name": "default.special_country_dim.country_code[residence_country]",
            "node_display_name": "Default: Special Country Dim",
            "node_name": "default.special_country_dim",
            "path": ["default.user_dim.residence_country"],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "default.special_country_dim.formation_date[birth_country]",
            "node_display_name": "Default: Special Country Dim",
            "node_name": "default.special_country_dim",
            "path": ["default.user_dim.birth_country"],
            "type": "int",
        },
        {
            "is_primary_key": False,
            "name": "default.special_country_dim.formation_date[residence_country]",
            "node_display_name": "Default: Special Country Dim",
            "node_name": "default.special_country_dim",
            "path": ["default.user_dim.residence_country"],
            "type": "int",
        },
        {
            "is_primary_key": False,
            "name": "default.special_country_dim.last_election_date[birth_country]",
            "node_display_name": "Default: Special Country Dim",
            "node_name": "default.special_country_dim",
            "path": ["default.user_dim.birth_country"],
            "type": "int",
        },
        {
            "is_primary_key": False,
            "name": "default.special_country_dim.last_election_date[residence_country]",
            "node_display_name": "Default: Special Country Dim",
            "node_name": "default.special_country_dim",
            "path": ["default.user_dim.residence_country"],
            "type": "int",
        },
        {
            "is_primary_key": False,
            "name": "default.special_country_dim.name[birth_country]",
            "node_display_name": "Default: Special Country Dim",
            "node_name": "default.special_country_dim",
            "path": ["default.user_dim.birth_country"],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "default.special_country_dim.name[residence_country]",
            "node_display_name": "Default: Special Country Dim",
            "node_name": "default.special_country_dim",
            "path": ["default.user_dim.residence_country"],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "default.user_dim.age",
            "node_display_name": "Default: User Dim",
            "node_name": "default.user_dim",
            "path": [],
            "type": "int",
        },
        {
            "is_primary_key": False,
            "name": "default.user_dim.birth_country",
            "node_display_name": "Default: User Dim",
            "node_name": "default.user_dim",
            "path": [],
            "type": "string",
        },
        {
            "is_primary_key": False,
            "name": "default.user_dim.residence_country",
            "node_display_name": "Default: User Dim",
            "node_name": "default.user_dim",
            "path": [],
            "type": "string",
        },
        {
            "is_primary_key": True,
            "name": "default.user_dim.user_id",
            "node_display_name": "Default: User Dim",
            "node_name": "default.user_dim",
            "path": [],
            "type": "int",
        },
    ]


def test_type_inference_structs(client_with_service_setup: TestClient):
    """
    Testing type resolution for structs select
    """
    client_with_service_setup.post(
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

    response = client_with_service_setup.post(
        "/nodes/metric/",
        json={
            "query": "SELECT SUM(counts.b) FROM basic.dreams",
            "description": "Dream Counts",
            "mode": "published",
            "name": "basic.dream_count",
        },
    )
    response.json()


def test_metric_expression_auto_aliased(client_with_service_setup: TestClient):
    """
    Testing that a metric's expression column is automatically aliased
    """
    client_with_service_setup.post("/namespaces/basic")
    client_with_service_setup.post(
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

    response = client_with_service_setup.post(
        "/nodes/metric/",
        json={
            "query": "SELECT SUM(counts.b) + SUM(counts.b) FROM basic.dreams",
            "description": "Dream Counts",
            "mode": "published",
            "name": "basic.dream_count",
        },
    )
    assert response.status_code == 201
    data = response.json()
    assert data["query"] == "SELECT SUM(counts.b) + SUM(counts.b) FROM basic.dreams"
    assert data["columns"] == [
        {
            "attributes": [],
            "dimension": None,
            "display_name": "Basic: Dream Count",
            "name": "basic_DOT_dream_count",
            "partition": None,
            "type": "bigint",
        },
    ]


def test_raise_on_malformated_expression_alias(client_with_service_setup: TestClient):
    """
    Test that using an invalid alias for a metric expression is saved, but the alias
    is overridden when creating the column name
    """
    client_with_service_setup.post(
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

    response = client_with_service_setup.post(
        "/nodes/metric/",
        json={
            "query": "SELECT SUM(counts.b) as foo FROM basic.dreams",
            "description": "Dream Counts",
            "mode": "published",
            "name": "basic.dream_count",
        },
    )
    assert response.status_code == 201
    data = response.json()
    assert data["query"] == "SELECT SUM(counts.b) as foo FROM basic.dreams"
    assert data["columns"][0]["name"] == "basic_DOT_dream_count"


def test_raise_on_multiple_expressions(client_with_service_setup: TestClient):
    """
    Testing raising when there is more than one expression
    """
    client_with_service_setup.post("/namespaces/basic")
    client_with_service_setup.post(
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

    response = client_with_service_setup.post(
        "/nodes/metric/",
        json={
            "query": "SELECT SUM(counts.b), COUNT(counts.b) FROM basic.dreams",
            "description": "Dream Counts",
            "mode": "published",
            "name": "basic.dream_count",
        },
    )
    assert response.status_code == 400
    assert (
        "Metric queries can only have a single expression, found 2"
    ) in response.json()["message"]


def test_list_metric_metadata(client: TestClient):
    """
    Test listing metric metadata values
    """
    metric_metadata_options = client.get("/metrics/metadata").json()
    assert metric_metadata_options == {
        "directions": ["higher_is_better", "lower_is_better", "neutral"],
        "units": [
            {
                "abbreviation": None,
                "category": "",
                "description": None,
                "label": "Unknown",
                "name": "unknown",
            },
            {
                "abbreviation": None,
                "category": "",
                "description": None,
                "label": "Unitless",
                "name": "unitless",
            },
            {
                "abbreviation": "%",
                "category": "",
                "description": "A ratio expressed as a number out of 100. Values "
                "range from 0 to 100.",
                "label": "Percentage",
                "name": "percentage",
            },
            {
                "abbreviation": "",
                "category": "",
                "description": "A ratio that compares a part to a whole. Values "
                "range from 0 to 1.",
                "label": "Proportion",
                "name": "proportion",
            },
            {
                "abbreviation": "$",
                "category": "currency",
                "description": None,
                "label": "Dollar",
                "name": "dollar",
            },
            {
                "abbreviation": "s",
                "category": "time",
                "description": None,
                "label": "Second",
                "name": "second",
            },
            {
                "abbreviation": "m",
                "category": "time",
                "description": None,
                "label": "Minute",
                "name": "minute",
            },
            {
                "abbreviation": "h",
                "category": "time",
                "description": None,
                "label": "Hour",
                "name": "hour",
            },
            {
                "abbreviation": "d",
                "category": "time",
                "description": None,
                "label": "Day",
                "name": "day",
            },
            {
                "abbreviation": "w",
                "category": "time",
                "description": None,
                "label": "Week",
                "name": "week",
            },
            {
                "abbreviation": "mo",
                "category": "time",
                "description": None,
                "label": "Month",
                "name": "month",
            },
            {
                "abbreviation": "y",
                "category": "time",
                "description": None,
                "label": "Year",
                "name": "year",
            },
        ],
    }
