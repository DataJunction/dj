# pylint: disable=too-many-lines
"""
Tests for the nodes API.
"""
import re
from typing import Any, Dict
from unittest import mock
from unittest.mock import call
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from pytest_mock import MockerFixture
from sqlalchemy import select
from sqlalchemy.orm import Session

from datajunction_server.database import Catalog
from datajunction_server.database.column import Column
from datajunction_server.database.node import Node, NodeRelationship, NodeRevision
from datajunction_server.errors import DJDoesNotExistException
from datajunction_server.internal.materializations import decompose_expression
from datajunction_server.models.node import NodeStatus
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.partition import PartitionBackfill
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.sql.dag import get_upstream_nodes
from datajunction_server.sql.parsing import ast, types
from datajunction_server.sql.parsing.types import IntegerType, StringType, TimestampType
from tests.sql.utils import compare_query_strings


def materialization_compare(response, expected):
    """Compares two materialization lists of json
    configs paying special attention to query comparison"""
    for materialization_response, materialization_expected in zip(response, expected):
        assert compare_query_strings(
            materialization_response["config"]["query"],
            materialization_expected["config"]["query"],
        )
        del materialization_response["config"]["query"]
        del materialization_expected["config"]["query"]
        assert materialization_response == materialization_expected


def test_read_node(client_with_roads: TestClient) -> None:
    """
    Test ``GET /nodes/{node_id}``.
    """
    response = client_with_roads.get("/nodes/default.repair_orders/")
    data = response.json()

    assert response.status_code == 200
    assert data["version"] == "v1.0"
    assert data["node_id"] == 1
    assert data["node_revision_id"] == 1
    assert data["type"] == "source"

    response = client_with_roads.get("/nodes/default.nothing/")
    data = response.json()

    assert response.status_code == 404
    assert data["message"] == "A node with name `default.nothing` does not exist."

    # Check that getting nodes via prefixes works
    response = client_with_roads.get("/nodes/?prefix=default.ha")
    data = response.json()
    assert set(data) == {
        "default.hard_hats",
        "default.hard_hat_state",
        "default.hard_hat",
    }


def test_read_nodes(session: Session, client: TestClient) -> None:
    """
    Test ``GET /nodes/``.
    """
    node1 = Node(
        name="not-a-metric",
        type=NodeType.SOURCE,
        current_version="1",
    )
    node_rev1 = NodeRevision(
        node=node1,
        version="1",
        name=node1.name,
        type=node1.type,
    )
    node2 = Node(
        name="also-not-a-metric",
        type=NodeType.TRANSFORM,
        current_version="1",
    )
    node_rev2 = NodeRevision(
        name=node2.name,
        node=node2,
        version="1",
        query="SELECT 42 AS answer",
        type=node2.type,
        columns=[
            Column(name="answer", type=IntegerType(), order=0),
        ],
    )
    node3 = Node(name="a-metric", type=NodeType.METRIC, current_version="1")
    node_rev3 = NodeRevision(
        name=node3.name,
        node=node3,
        version="1",
        query="SELECT COUNT(*) FROM my_table",
        columns=[
            Column(name="_col0", type=IntegerType(), order=0),
        ],
        type=node3.type,
    )
    session.add(node_rev1)
    session.add(node_rev2)
    session.add(node_rev3)
    session.commit()

    response = client.get("/nodes/")
    data = response.json()

    assert response.status_code == 200
    assert len(data) == 3
    assert set(data) == {"not-a-metric", "also-not-a-metric", "a-metric"}

    response = client.get("/nodes?node_type=metric")
    data = response.json()

    assert response.status_code == 200
    assert len(data) == 1
    assert set(data) == {"a-metric"}


def test_get_nodes_with_details(client_with_examples: TestClient):
    """
    Test getting all nodes with some details
    """
    response = client_with_examples.get("/nodes/details/")
    assert response.ok
    data = response.json()
    assert {d["name"] for d in data} == {
        "default.country_dim",
        "foo.bar.us_state",
        "basic.paint_colors_trino",
        "foo.bar.us_region",
        "default.avg_repair_order_discounts",
        "foo.bar.hard_hats",
        "foo.bar.dispatchers",
        "foo.bar.us_states",
        "default.sales",
        "basic.patches",
        "foo.bar.local_hard_hats",
        "default.date_dim",
        "foo.bar.hard_hat",
        "default.long_events_distinct_countries",
        "default.repair_type",
        "default.total_repair_order_discounts",
        "default.total_repair_cost",
        "basic.source.comments",
        "foo.bar.municipality_type",
        "default.large_revenue_payments_and_business_only",
        "default.payment_type_table",
        "default.local_hard_hats",
        "default.dispatcher",
        "foo.bar.repair_orders",
        "basic.transform.country_agg",
        "foo.bar.hard_hat_state",
        "foo.bar.municipality_dim",
        "foo.bar.repair_order_details",
        "foo.bar.dispatcher",
        "default.dispatchers",
        "dbt.source.stripe.payments",
        "default.national_level_agg",
        "default.us_region",
        "default.repair_order_details",
        "default.contractor",
        "foo.bar.total_repair_order_discounts",
        "default.repair_orders",
        "basic.paint_colors_spark",
        "default.long_events",
        "default.items",
        "default.special_country_dim",
        "default.avg_user_age",
        "foo.bar.contractor",
        "basic.avg_luminosity_patches",
        "default.countries",
        "default.discounted_orders_rate",
        "default.municipality_municipality_type",
        "default.user_dim",
        "basic.corrected_patches",
        "basic.num_users",
        "default.regional_level_agg",
        "default.revenue",
        "foo.bar.contractors",
        "foo.bar.avg_repair_order_discounts",
        "foo.bar.municipality",
        "dbt.source.jaffle_shop.customers",
        "foo.bar.repair_order",
        "default.account_type",
        "foo.bar.avg_time_to_dispatch",
        "basic.dimension.users",
        "dbt.dimension.customers",
        "basic.num_comments",
        "default.us_state",
        "default.hard_hats",
        "default.items_sold_count",
        "default.users",
        "default.avg_repair_price",
        "basic.murals",
        "default.avg_length_of_employment",
        "default.municipality_type",
        "default.hard_hat_state",
        "default.num_repair_orders",
        "basic.source.users",
        "default.date",
        "default.us_states",
        "foo.bar.total_repair_cost",
        "default.device_ids_count",
        "dbt.source.jaffle_shop.orders",
        "dbt.transform.customer_agg",
        "default.regional_repair_efficiency",
        "foo.bar.num_repair_orders",
        "default.hard_hat",
        "foo.bar.municipality_municipality_type",
        "basic.dimension.countries",
        "default.number_of_account_types",
        "default.municipality",
        "default.payment_type",
        "default.municipality_dim",
        "default.contractors",
        "default.total_profit",
        "default.account_type_table",
        "default.repair_order",
        "foo.bar.avg_length_of_employment",
        "foo.bar.avg_repair_price",
        "default.avg_time_to_dispatch",
        "default.event_source",
        "foo.bar.repair_type",
        "default.large_revenue_payments_only",
        "default.repair_orders_fact",
    }


class TestNodeCRUD:  # pylint: disable=too-many-public-methods
    """
    Test node CRUD
    """

    @pytest.fixture
    def create_dimension_node_payload(self) -> Dict[str, Any]:
        """
        Payload for creating a dimension node.
        """

        return {
            "description": "Country dimension",
            "query": "SELECT country, COUNT(1) AS user_cnt "
            "FROM basic.source.users GROUP BY country",
            "mode": "published",
            "name": "default.countries",
            "primary_key": ["country"],
        }

    @pytest.fixture
    def create_invalid_transform_node_payload(self) -> Dict[str, Any]:
        """
        Payload for creating a transform node.
        """

        return {
            "name": "default.country_agg",
            "query": "SELECT country, COUNT(DISTINCT id) AS num_users FROM comments",
            "mode": "published",
            "description": "Distinct users per country",
            "columns": [
                {"name": "country", "type": "string"},
                {"name": "num_users", "type": "int"},
            ],
        }

    @pytest.fixture
    def create_transform_node_payload(self) -> Dict[str, Any]:
        """
        Payload for creating a transform node.
        """

        return {
            "name": "default.country_agg",
            "query": "SELECT country, COUNT(DISTINCT id) AS num_users FROM basic.source.users",
            "mode": "published",
            "description": "Distinct users per country",
            "columns": [
                {"name": "country", "type": "string"},
                {"name": "num_users", "type": "int"},
            ],
        }

    @pytest.fixture
    def catalog(self, session: Session) -> Catalog:
        """
        A database fixture.
        """

        catalog = Catalog(name="prod", uuid=uuid4())
        session.add(catalog)
        session.commit()
        return catalog

    @pytest.fixture
    def source_node(self, session: Session) -> Node:
        """
        A source node fixture.
        """
        node = Node(
            name="basic.source.users",
            type=NodeType.SOURCE,
            current_version="v1",
        )
        node_revision = NodeRevision(
            node=node,
            name=node.name,
            catalog_id=1,
            type=node.type,
            version="v1",
            columns=[
                Column(name="id", type=IntegerType(), order=0),
                Column(name="full_name", type=StringType(), order=1),
                Column(name="age", type=IntegerType(), order=2),
                Column(name="country", type=StringType(), order=3),
                Column(name="gender", type=StringType(), order=4),
                Column(name="preferred_language", type=StringType(), order=5),
            ],
        )
        session.add(node_revision)
        session.commit()
        return node

    def test_create_dimension_without_catalog(
        self,
        client_with_roads,
    ):
        """
        Test that creating a dimension that's purely query-based and therefore
        doesn't reference a catalog works.
        """
        response = client_with_roads.post(
            "/nodes/dimension/",
            json={
                "description": "Title",
                "query": (
                    "SELECT 0 AS title_code, 'Agha' AS title "
                    "UNION ALL SELECT 1, 'Abbot' "
                    "UNION ALL SELECT 2, 'Akhoond' "
                    "UNION ALL SELECT 3, 'Apostle'"
                ),
                "mode": "published",
                "name": "default.title",
                "primary_key": ["title"],
            },
        )
        assert response.json()["columns"] == [
            {
                "attributes": [],
                "dimension": None,
                "display_name": "Title Code",
                "name": "title_code",
                "type": "int",
                "partition": None,
            },
            {
                "attributes": [
                    {"attribute_type": {"name": "primary_key", "namespace": "system"}},
                ],
                "dimension": None,
                "display_name": "Title",
                "name": "title",
                "type": "string",
                "partition": None,
            },
        ]

        # Link the dimension to a column on the source node
        response = client_with_roads.post(
            "/nodes/default.hard_hats/columns/title/"
            "?dimension=default.title&dimension_column=title",
        )
        assert response.ok
        response = client_with_roads.get("/nodes/default.hard_hats/")
        assert {
            "attributes": [],
            "dimension": {"name": "default.title"},
            "display_name": "Title",
            "name": "title",
            "type": "string",
            "partition": None,
        } in response.json()["columns"]

    def test_deleting_node(
        self,
        client_with_basic: TestClient,
    ):
        """
        Test deleting a node
        """
        # Delete a node
        response = client_with_basic.delete("/nodes/basic.source.users/")
        assert response.status_code == 200
        # Check that then retrieving the node returns an error
        response = client_with_basic.get("/nodes/basic.source.users/")
        assert not response.ok
        assert response.json() == {
            "message": "A node with name `basic.source.users` does not exist.",
            "errors": [],
            "warnings": [],
        }
        # All downstream nodes should be invalid
        expected_downstreams = [
            "basic.dimension.users",
            "basic.transform.country_agg",
            "basic.dimension.countries",
            "basic.num_users",
        ]
        for downstream in expected_downstreams:
            response = client_with_basic.get(f"/nodes/{downstream}/")
            assert response.json()["status"] == NodeStatus.INVALID

            # The downstreams' status change should be recorded in their histories
            response = client_with_basic.get(f"/history?node={downstream}")
            assert [
                (activity["pre"], activity["post"], activity["details"])
                for activity in response.json()
                if activity["activity_type"] == "status_change"
            ] == [
                (
                    {"status": "valid"},
                    {"status": "invalid"},
                    {"upstream_node": "basic.source.users"},
                ),
            ]

        # Trying to create the node again should work.
        response = client_with_basic.post(
            "/nodes/source/",
            json={
                "name": "basic.source.users",
                "description": "A user table",
                "columns": [
                    {"name": "id", "type": "int"},
                    {"name": "full_name", "type": "string"},
                    {"name": "age", "type": "int"},
                    {"name": "country", "type": "string"},
                    {"name": "gender", "type": "string"},
                    {"name": "preferred_language", "type": "string"},
                    {"name": "secret_number", "type": "float"},
                    {"name": "created_at", "type": "timestamp"},
                    {"name": "post_processing_timestamp", "type": "timestamp"},
                ],
                "mode": "published",
                "catalog": "public",
                "schema_": "basic",
                "table": "dim_users",
            },
        )
        assert response.ok

        # The deletion action should be recorded in the node's history
        response = client_with_basic.get("/history?node=basic.source.users")
        history = response.json()
        assert history == [
            {
                "activity_type": "create",
                "node": "basic.source.users",
                "created_at": mock.ANY,
                "details": {},
                "entity_name": "basic.source.users",
                "entity_type": "node",
                "id": mock.ANY,
                "post": {},
                "pre": {},
                "user": "dj",
            },
            {
                "activity_type": "delete",
                "node": "basic.source.users",
                "created_at": mock.ANY,
                "details": {},
                "entity_name": "basic.source.users",
                "entity_type": "node",
                "id": mock.ANY,
                "post": {},
                "pre": {},
                "user": "dj",
            },
            {
                "id": mock.ANY,
                "entity_type": "node",
                "entity_name": "basic.source.users",
                "node": "basic.source.users",
                "activity_type": "update",
                "user": "dj",
                "pre": {},
                "post": {},
                "details": {"version": "v2.0"},
                "created_at": mock.ANY,
            },
            {
                "id": mock.ANY,
                "entity_type": "node",
                "entity_name": "basic.source.users",
                "node": "basic.source.users",
                "activity_type": "restore",
                "user": "dj",
                "pre": {},
                "post": {},
                "details": {},
                "created_at": mock.ANY,
            },
        ]

    def test_deleting_source_upstream_from_metric(
        self,
        client: TestClient,
    ):
        """
        Test deleting a source that's upstream from a metric
        """
        response = client.post("/catalogs/", json={"name": "warehouse"})
        assert response.ok
        response = client.post("/namespaces/default/")
        assert response.ok
        response = client.post(
            "/nodes/source/",
            json={
                "name": "default.users",
                "description": "A user table",
                "columns": [
                    {"name": "id", "type": "int"},
                    {"name": "full_name", "type": "string"},
                    {"name": "age", "type": "int"},
                    {"name": "country", "type": "string"},
                    {"name": "gender", "type": "string"},
                    {"name": "preferred_language", "type": "string"},
                    {"name": "secret_number", "type": "float"},
                    {"name": "created_at", "type": "timestamp"},
                    {"name": "post_processing_timestamp", "type": "timestamp"},
                ],
                "mode": "published",
                "catalog": "warehouse",
                "schema_": "db",
                "table": "users",
            },
        )
        assert response.ok
        response = client.post(
            "/nodes/metric/",
            json={
                "description": "Total number of users",
                "query": "SELECT COUNT(DISTINCT id) FROM default.users",
                "mode": "published",
                "name": "default.num_users",
            },
        )
        assert response.ok
        # Delete the source node
        response = client.delete("/nodes/default.users/")
        assert response.ok
        # The downstream metric should have an invalid status
        assert (
            client.get("/nodes/default.num_users/").json()["status"]
            == NodeStatus.INVALID
        )
        response = client.get("/history?node=default.num_users")
        assert [
            (activity["pre"], activity["post"], activity["details"])
            for activity in response.json()
            if activity["activity_type"] == "status_change"
        ] == [
            (
                {"status": "valid"},
                {"status": "invalid"},
                {"upstream_node": "default.users"},
            ),
        ]

        # Restore the source node
        response = client.post("/nodes/default.users/restore/")
        assert response.ok
        # Retrieving the restored node should work
        response = client.get("/nodes/default.users/")
        assert response.ok
        # The downstream metric should have been changed to valid
        response = client.get("/nodes/default.num_users/")
        assert response.json()["status"] == NodeStatus.VALID
        # Check activity history of downstream metric
        response = client.get("/history?node=default.num_users")
        assert [
            (activity["pre"], activity["post"], activity["details"])
            for activity in response.json()
            if activity["activity_type"] == "status_change"
        ] == [
            (
                {"status": "valid"},
                {"status": "invalid"},
                {"upstream_node": "default.users"},
            ),
            (
                {"status": "invalid"},
                {"status": "valid"},
                {"upstream_node": "default.users"},
            ),
        ]

    def test_deleting_transform_upstream_from_metric(
        self,
        client: TestClient,
    ):
        """
        Test deleting a transform that's upstream from a metric
        """
        response = client.post("/catalogs/", json={"name": "warehouse"})
        assert response.ok
        response = client.post("/namespaces/default/")
        assert response.ok
        response = client.post(
            "/nodes/source/",
            json={
                "name": "default.users",
                "description": "A user table",
                "columns": [
                    {"name": "id", "type": "int"},
                    {"name": "full_name", "type": "string"},
                    {"name": "age", "type": "int"},
                    {"name": "country", "type": "string"},
                    {"name": "gender", "type": "string"},
                    {"name": "preferred_language", "type": "string"},
                    {"name": "secret_number", "type": "float"},
                    {"name": "created_at", "type": "timestamp"},
                    {"name": "post_processing_timestamp", "type": "timestamp"},
                ],
                "mode": "published",
                "catalog": "warehouse",
                "schema_": "db",
                "table": "users",
            },
        )
        assert response.ok
        response = client.post(
            "/nodes/transform/",
            json={
                "name": "default.us_users",
                "description": "US users",
                "query": """
                    SELECT
                    id,
                    full_name,
                    age,
                    country,
                    gender,
                    preferred_language,
                    secret_number,
                    created_at,
                    post_processing_timestamp
                    FROM default.users
                    WHERE country = 'US'
                """,
                "mode": "published",
            },
        )
        assert response.ok
        response = client.post(
            "/nodes/metric/",
            json={
                "description": "Total number of US users",
                "query": "SELECT COUNT(DISTINCT id) FROM default.us_users",
                "mode": "published",
                "name": "default.num_us_users",
            },
        )
        assert response.ok
        # Create an invalid draft downstream node
        # so we can test that it stays invalid
        # when the upstream node is restored
        response = client.post(
            "/nodes/metric/",
            json={
                "description": "An invalid node downstream of default.us_users",
                "query": "SELECT COUNT(DISTINCT non_existent_column) FROM default.us_users",
                "mode": "draft",
                "name": "default.invalid_metric",
            },
        )
        assert response.ok
        response = client.get("/nodes/default.invalid_metric/")
        assert response.ok
        assert response.json()["status"] == NodeStatus.INVALID
        # Delete the transform node
        response = client.delete("/nodes/default.us_users/")
        assert response.ok
        # Retrieving the deleted node should respond that the node doesn't exist
        assert client.get("/nodes/default.us_users/").json()["message"] == (
            "A node with name `default.us_users` does not exist."
        )
        # The downstream metrics should have an invalid status
        assert (
            client.get("/nodes/default.num_us_users/").json()["status"]
            == NodeStatus.INVALID
        )
        assert (
            client.get("/nodes/default.invalid_metric/").json()["status"]
            == NodeStatus.INVALID
        )

        # Check history of downstream metrics
        response = client.get("/history?node=default.num_us_users")
        assert [
            (activity["pre"], activity["post"], activity["details"])
            for activity in response.json()
            if activity["activity_type"] == "status_change"
        ] == [
            (
                {"status": "valid"},
                {"status": "invalid"},
                {"upstream_node": "default.us_users"},
            ),
        ]
        # No change recorded here because the metric was already invalid
        response = client.get("/history?node=default.invalid_metric")
        assert [
            (activity["pre"], activity["post"])
            for activity in response.json()
            if activity["activity_type"] == "status_change"
        ] == []

        # Restore the transform node
        response = client.post("/nodes/default.us_users/restore/")
        assert response.ok
        # Retrieving the restored node should work
        response = client.get("/nodes/default.us_users/")
        assert response.ok
        # Check history of the restored node
        response = client.get("/history?node=default.us_users")
        history = response.json()
        assert [
            (activity["activity_type"], activity["entity_type"]) for activity in history
        ] == [("create", "node"), ("delete", "node"), ("restore", "node")]

        # This downstream metric should have been changed to valid
        response = client.get("/nodes/default.num_us_users/")
        assert response.json()["status"] == NodeStatus.VALID
        # Check history of downstream metric
        response = client.get("/history?node=default.num_us_users")
        assert [
            (activity["pre"], activity["post"], activity["details"])
            for activity in response.json()
            if activity["activity_type"] == "status_change"
        ] == [
            (
                {"status": "valid"},
                {"status": "invalid"},
                {"upstream_node": "default.us_users"},
            ),
            (
                {"status": "invalid"},
                {"status": "valid"},
                {"upstream_node": "default.us_users"},
            ),
        ]

        # The other downstream metric should have remained invalid
        response = client.get("/nodes/default.invalid_metric/")
        assert response.json()["status"] == NodeStatus.INVALID
        # Check history of downstream metric
        response = client.get("/history?node=default.invalid_metric")
        assert [
            (activity["pre"], activity["post"])
            for activity in response.json()
            if activity["activity_type"] == "status_change"
        ] == []

    def test_deleting_linked_dimension(
        self,
        client: TestClient,
    ):
        """
        Test deleting a dimension that's linked to columns on other nodes
        """
        response = client.post("/catalogs/", json={"name": "warehouse"})
        assert response.ok
        response = client.post("/namespaces/default/")
        assert response.ok
        response = client.post(
            "/nodes/source/",
            json={
                "name": "default.users",
                "description": "A user table",
                "columns": [
                    {"name": "id", "type": "int"},
                    {"name": "full_name", "type": "string"},
                    {"name": "age", "type": "int"},
                    {"name": "country", "type": "string"},
                    {"name": "gender", "type": "string"},
                    {"name": "preferred_language", "type": "string"},
                    {"name": "secret_number", "type": "float"},
                    {"name": "created_at", "type": "timestamp"},
                    {"name": "post_processing_timestamp", "type": "timestamp"},
                ],
                "mode": "published",
                "catalog": "warehouse",
                "schema_": "db",
                "table": "users",
            },
        )
        assert response.ok
        response = client.post(
            "/nodes/dimension/",
            json={
                "name": "default.us_users",
                "description": "US users",
                "query": """
                    SELECT
                    id,
                    full_name,
                    age,
                    country,
                    gender,
                    preferred_language,
                    secret_number,
                    created_at,
                    post_processing_timestamp
                    FROM default.users
                    WHERE country = 'US'
                """,
                "primary_key": ["id"],
                "mode": "published",
            },
        )
        assert response.ok
        response = client.post(
            "/nodes/source/",
            json={
                "name": "default.messages",
                "description": "A table of user messages",
                "columns": [
                    {"name": "id", "type": "int"},
                    {"name": "user_id", "type": "int"},
                    {"name": "message", "type": "int"},
                    {"name": "posted_at", "type": "timestamp"},
                ],
                "mode": "published",
                "catalog": "warehouse",
                "schema_": "db",
                "table": "messages",
            },
        )
        assert response.ok
        # Create a metric on the source node
        response = client.post(
            "/nodes/metric/",
            json={
                "description": "Total number of user messages",
                "query": "SELECT COUNT(DISTINCT id) FROM default.messages",
                "mode": "published",
                "name": "default.num_messages",
            },
        )
        assert response.ok

        # Create a metric on the source node w/ bound dimensions
        response = client.post(
            "/nodes/metric/",
            json={
                "description": "Total number of user messages by id",
                "query": "SELECT COUNT(DISTINCT id) FROM default.messages",
                "mode": "published",
                "name": "default.num_messages_id",
                "required_dimensions": ["user_id"],
            },
        )
        assert response.ok

        # Create a metric w/ bound dimensions that to not exist
        with pytest.raises(Exception) as exc:
            response = client.post(
                "/nodes/metric/",
                json={
                    "description": "Total number of user messages by id",
                    "query": "SELECT COUNT(DISTINCT id) FROM default.messages",
                    "mode": "published",
                    "name": "default.num_messages_id",
                    "required_dimensions": ["default.nothin.id"],
                },
            )
            assert "required dimensions that are not on parent nodes" in str(exc)

        # Create a metric on the source node w/ an invalid bound dimension
        response = client.post(
            "/nodes/metric/",
            json={
                "description": "Total number of user messages by id",
                "query": "SELECT COUNT(DISTINCT id) FROM default.messages",
                "mode": "published",
                "name": "default.num_messages_id_invalid_dimension",
                "required_dimensions": ["default.messages.foo"],
            },
        )
        assert response.status_code == 400
        assert response.json() == {
            "message": "Node definition contains references to "
            "columns as required dimensions that are not on parent nodes.",
            "errors": [
                {
                    "code": 206,
                    "message": "Node definition contains references to columns "
                    "as required dimensions that are not on parent nodes.",
                    "debug": {"invalid_required_dimensions": ["default.messages.foo"]},
                    "context": "",
                },
            ],
            "warnings": [],
        }

        # Link the dimension to a column on the source node
        response = client.post(
            "/nodes/default.messages/columns/user_id/"
            "?dimension=default.us_users&dimension_column=id",
        )
        assert response.ok
        # The dimension's attributes should now be available to the metric
        response = client.get("/metrics/default.num_messages/")
        assert response.ok
        assert response.json()["dimensions"] == [
            {
                "is_primary_key": False,
                "name": "default.us_users.age",
                "node_display_name": "Default: Us Users",
                "node_name": "default.us_users",
                "path": ["default.messages.user_id"],
                "type": "int",
            },
            {
                "is_primary_key": False,
                "name": "default.us_users.country",
                "node_display_name": "Default: Us Users",
                "node_name": "default.us_users",
                "path": ["default.messages.user_id"],
                "type": "string",
            },
            {
                "is_primary_key": False,
                "name": "default.us_users.created_at",
                "node_display_name": "Default: Us Users",
                "node_name": "default.us_users",
                "path": ["default.messages.user_id"],
                "type": "timestamp",
            },
            {
                "is_primary_key": False,
                "name": "default.us_users.full_name",
                "node_display_name": "Default: Us Users",
                "node_name": "default.us_users",
                "path": ["default.messages.user_id"],
                "type": "string",
            },
            {
                "is_primary_key": False,
                "name": "default.us_users.gender",
                "node_display_name": "Default: Us Users",
                "node_name": "default.us_users",
                "path": ["default.messages.user_id"],
                "type": "string",
            },
            {
                "is_primary_key": True,
                "name": "default.us_users.id",
                "node_display_name": "Default: Us Users",
                "node_name": "default.us_users",
                "path": ["default.messages.user_id"],
                "type": "int",
            },
            {
                "is_primary_key": False,
                "name": "default.us_users.post_processing_timestamp",
                "node_display_name": "Default: Us Users",
                "node_name": "default.us_users",
                "path": ["default.messages.user_id"],
                "type": "timestamp",
            },
            {
                "is_primary_key": False,
                "name": "default.us_users.preferred_language",
                "node_display_name": "Default: Us Users",
                "node_name": "default.us_users",
                "path": ["default.messages.user_id"],
                "type": "string",
            },
            {
                "is_primary_key": False,
                "name": "default.us_users.secret_number",
                "node_display_name": "Default: Us Users",
                "node_name": "default.us_users",
                "path": ["default.messages.user_id"],
                "type": "float",
            },
        ]

        # Check history of the node with column dimension link
        response = client.get(
            "/history?node=default.messages",
        )
        history = response.json()
        assert [
            (activity["activity_type"], activity["entity_type"]) for activity in history
        ] == [("create", "node"), ("create", "link")]

        # Delete the dimension node
        response = client.delete("/nodes/default.us_users/")
        assert response.ok
        # Retrieving the deleted node should respond that the node doesn't exist
        assert client.get("/nodes/default.us_users/").json()["message"] == (
            "A node with name `default.us_users` does not exist."
        )
        # The deleted dimension's attributes should no longer be available to the metric
        response = client.get("/metrics/default.num_messages/")
        assert response.ok
        assert [] == response.json()["dimensions"]
        # The metric should still be VALID
        response = client.get("/nodes/default.num_messages/")
        assert response.json()["status"] == NodeStatus.VALID
        # Restore the dimension node
        response = client.post("/nodes/default.us_users/restore/")
        assert response.ok
        # Retrieving the restored node should work
        response = client.get("/nodes/default.us_users/")
        assert response.ok
        # The dimension's attributes should now once again show for the linked metric
        response = client.get("/metrics/default.num_messages/")
        assert response.ok
        assert response.json()["dimensions"] == [
            {
                "is_primary_key": False,
                "name": "default.us_users.age",
                "node_display_name": "Default: Us Users",
                "node_name": "default.us_users",
                "path": ["default.messages.user_id"],
                "type": "int",
            },
            {
                "is_primary_key": False,
                "name": "default.us_users.country",
                "node_display_name": "Default: Us Users",
                "node_name": "default.us_users",
                "path": ["default.messages.user_id"],
                "type": "string",
            },
            {
                "is_primary_key": False,
                "name": "default.us_users.created_at",
                "node_display_name": "Default: Us Users",
                "node_name": "default.us_users",
                "path": ["default.messages.user_id"],
                "type": "timestamp",
            },
            {
                "is_primary_key": False,
                "name": "default.us_users.full_name",
                "node_display_name": "Default: Us Users",
                "node_name": "default.us_users",
                "path": ["default.messages.user_id"],
                "type": "string",
            },
            {
                "is_primary_key": False,
                "name": "default.us_users.gender",
                "node_display_name": "Default: Us Users",
                "node_name": "default.us_users",
                "path": ["default.messages.user_id"],
                "type": "string",
            },
            {
                "is_primary_key": True,
                "name": "default.us_users.id",
                "node_display_name": "Default: Us Users",
                "node_name": "default.us_users",
                "path": ["default.messages.user_id"],
                "type": "int",
            },
            {
                "is_primary_key": False,
                "name": "default.us_users.post_processing_timestamp",
                "node_display_name": "Default: Us Users",
                "node_name": "default.us_users",
                "path": ["default.messages.user_id"],
                "type": "timestamp",
            },
            {
                "is_primary_key": False,
                "name": "default.us_users.preferred_language",
                "node_display_name": "Default: Us Users",
                "node_name": "default.us_users",
                "path": ["default.messages.user_id"],
                "type": "string",
            },
            {
                "is_primary_key": False,
                "name": "default.us_users.secret_number",
                "node_display_name": "Default: Us Users",
                "node_name": "default.us_users",
                "path": ["default.messages.user_id"],
                "type": "float",
            },
        ]
        # The metric should still be VALID
        response = client.get("/nodes/default.num_messages/")
        assert response.json()["status"] == NodeStatus.VALID

    def test_restoring_an_already_active_node(
        self,
        client: TestClient,
    ):
        """
        Test raising when restoring an already active node
        """
        response = client.post("/catalogs/", json={"name": "warehouse"})
        assert response.ok
        response = client.post("/namespaces/default/")
        assert response.ok
        response = client.post(
            "/nodes/source/",
            json={
                "name": "default.users",
                "description": "A user table",
                "columns": [
                    {"name": "id", "type": "int"},
                    {"name": "full_name", "type": "string"},
                    {"name": "age", "type": "int"},
                    {"name": "country", "type": "string"},
                    {"name": "gender", "type": "string"},
                    {"name": "preferred_language", "type": "string"},
                    {"name": "secret_number", "type": "float"},
                    {"name": "created_at", "type": "timestamp"},
                    {"name": "post_processing_timestamp", "type": "timestamp"},
                ],
                "mode": "published",
                "catalog": "warehouse",
                "schema_": "db",
                "table": "users",
            },
        )
        assert response.ok
        response = client.post("/nodes/default.users/restore/")
        assert not response.ok
        assert response.json() == {
            "message": "Cannot restore `default.users`, node already active.",
            "errors": [],
            "warnings": [],
        }

    def verify_complete_hard_delete(
        self,
        session: Session,
        client_with_roads: TestClient,
        node_name: str,
    ):
        """
        Verify that after hard deleting a node, all node revisions and node relationship
        references are removed.
        """
        # Record its upstream nodes
        upstream_names = [
            node.name for node in get_upstream_nodes(session, node_name=node_name)
        ]

        # Hard delete the node
        response = client_with_roads.delete(f"/nodes/{node_name}/hard/")
        assert response.ok

        # Check that all revisions (and their relations) for the node have been deleted
        nodes = (
            session.execute(select(Node).where(Node.name == node_name))
            .unique()
            .scalars()
            .all()
        )
        revisions = (
            session.execute(
                select(NodeRevision).where(NodeRevision.name == node_name),
            )
            .unique()
            .scalars()
            .all()
        )
        relations = (
            session.execute(
                select(NodeRelationship).where(
                    NodeRelationship.child_id.in_(  # type: ignore  # pylint: disable=no-member
                        [rev.id for rev in revisions],
                    ),
                ),
            )
            .unique()
            .scalars()
            .all()
        )
        assert nodes == []
        assert revisions == []
        assert relations == []

        # Check that upstreams and downstreams of the node still remain
        upstreams = (
            session.execute(
                select(Node).where(
                    Node.name.in_(upstream_names),  # type: ignore  # pylint: disable=no-member
                ),
            )
            .unique()
            .scalars()
            .all()
        )
        assert len(upstreams) == len(upstream_names)

    def test_hard_deleting_node_with_versions(
        self,
        client_with_roads: TestClient,
        session: Session,
    ):
        """
        Test that hard deleting a node will remove all previous node revisions.
        """
        # Create a few revisions for the `default.repair_order` dimension
        client_with_roads.patch(
            "/nodes/default.repair_order/",
            json={"query": """SELECT repair_order_id FROM default.repair_orders"""},
        )
        client_with_roads.patch(
            "/nodes/default.repair_order/",
            json={
                "query": """SELECT
                        repair_order_id,
                        municipality_id,
                        hard_hat_id,
                        order_date,
                        required_date,
                        dispatched_date,
                        dispatcher_id
                        FROM default.repair_orders""",
            },
        )
        response = client_with_roads.get("/nodes/default.repair_order")
        assert response.json()["version"] == "v3.0"

        # Hard delete all nodes and verify after each delete
        default_nodes = client_with_roads.get("/namespaces/default/").json()
        for node_name in default_nodes:
            self.verify_complete_hard_delete(
                session,
                client_with_roads,
                node_name["name"],
            )

        # Check that all nodes under the `default` namespace and their revisions have been deleted
        nodes = (
            session.execute(select(Node).where(Node.namespace == "default"))
            .unique()
            .scalars()
            .all()
        )
        assert len(nodes) == 0

        revisions = (
            session.execute(
                select(NodeRevision).where(
                    NodeRevision.name.like("default%"),  # type: ignore # pylint: disable=no-member
                ),
            )
            .unique()
            .scalars()
            .all()
        )
        assert len(revisions) == 0

    def test_hard_deleting_a_node(
        self,
        client_with_roads: TestClient,
    ):
        """
        Test raising when restoring an already active node
        """
        # Hard deleting a node causes downstream nodes to become invalid
        response = client_with_roads.delete("/nodes/default.repair_orders/hard/")
        assert response.ok
        data = response.json()
        data["impact"] = sorted(data["impact"], key=lambda x: x["name"])
        assert data == {
            "impact": [
                {
                    "effect": "downstream node is now invalid",
                    "name": "default.avg_repair_order_discounts",
                    "status": "invalid",
                },
                {
                    "effect": "downstream node is now invalid",
                    "name": "default.avg_repair_price",
                    "status": "invalid",
                },
                {
                    "effect": "downstream node is now invalid",
                    "name": "default.avg_time_to_dispatch",
                    "status": "invalid",
                },
                {
                    "effect": "downstream node is now invalid",
                    "name": "default.discounted_orders_rate",
                    "status": "invalid",
                },
                {
                    "effect": "downstream node is now invalid",
                    "name": "default.num_repair_orders",
                    "status": "invalid",
                },
                {
                    "effect": "downstream node is now invalid",
                    "name": "default.regional_level_agg",
                    "status": "invalid",
                },
                {
                    "effect": "downstream node is now invalid",
                    "name": "default.regional_repair_efficiency",
                    "status": "invalid",
                },
                {
                    "effect": "downstream node is now invalid",
                    "name": "default.repair_order",
                    "status": "invalid",
                },
                {
                    "effect": "downstream node is now invalid",
                    "name": "default.repair_orders_fact",
                    "status": "invalid",
                },
                {
                    "effect": "downstream node is now invalid",
                    "name": "default.total_repair_cost",
                    "status": "invalid",
                },
                {
                    "effect": "downstream node is now invalid",
                    "name": "default.total_repair_order_discounts",
                    "status": "invalid",
                },
            ],
            "message": "The node `default.repair_orders` has been completely removed.",
        }

        # Hard deleting a dimension creates broken links
        response = client_with_roads.delete("/nodes/default.repair_order/hard/")
        assert response.ok
        assert response.json() == {
            "impact": [
                {
                    "effect": "broken link",
                    "name": "default.repair_order_details",
                    "status": "valid",
                },
                {
                    "effect": "broken link",
                    "name": "default.regional_level_agg",
                    "status": "invalid",
                },
                {
                    "effect": "broken link",
                    "name": "default.national_level_agg",
                    "status": "valid",
                },
                {
                    "effect": "broken link",
                    "name": "default.repair_orders_fact",
                    "status": "invalid",
                },
                {
                    "effect": "broken link",
                    "name": "default.regional_repair_efficiency",
                    "status": "invalid",
                },
                {
                    "effect": "broken link",
                    "name": "default.num_repair_orders",
                    "status": "invalid",
                },
                {
                    "effect": "broken link",
                    "name": "default.avg_repair_price",
                    "status": "invalid",
                },
                {
                    "effect": "broken link",
                    "name": "default.total_repair_cost",
                    "status": "invalid",
                },
                {
                    "effect": "broken link",
                    "name": "default.discounted_orders_rate",
                    "status": "invalid",
                },
                {
                    "effect": "broken link",
                    "name": "default.total_repair_order_discounts",
                    "status": "invalid",
                },
                {
                    "effect": "broken link",
                    "name": "default.avg_repair_order_discounts",
                    "status": "invalid",
                },
                {
                    "effect": "broken link",
                    "name": "default.avg_time_to_dispatch",
                    "status": "invalid",
                },
            ],
            "message": "The node `default.repair_order` has been completely removed.",
        }

        # Hard deleting an unlinked node has no impact
        response = client_with_roads.delete(
            "/nodes/default.regional_repair_efficiency/hard/",
        )
        assert response.ok
        assert response.json() == {
            "message": "The node `default.regional_repair_efficiency` has been completely removed.",
            "impact": [],
        }

        # Hard delete a metric
        response = client_with_roads.delete(
            "/nodes/default.avg_repair_order_discounts/hard/",
        )
        assert response.ok
        assert response.json() == {
            "message": "The node `default.avg_repair_order_discounts` has been completely removed.",
            "impact": [],
        }

    def test_register_table_without_query_service(
        self,
        client: TestClient,
    ):
        """
        Trying to register a table without a query service set up should fail.
        """
        response = client.post("/register/table/foo/bar/baz/")
        data = response.json()
        assert data["message"] == (
            "Registering tables requires that a query "
            "service is configured for table columns inference"
        )
        assert response.status_code == 500

    def test_create_source_node_with_query_service(
        self,
        client_with_query_service_example_loader,
    ):
        """
        Creating a source node without columns but with a query service set should
        result in the source node columns being inferred via the query service.
        """
        custom_client = client_with_query_service_example_loader(["BASIC"])
        response = custom_client.post(
            "/register/table/public/basic/comments/",
        )
        data = response.json()
        assert data["name"] == "source.public.basic.comments"
        assert data["type"] == "source"
        assert data["display_name"] == "source.public.basic.comments"
        assert data["version"] == "v1.0"
        assert data["status"] == "valid"
        assert data["mode"] == "published"
        assert data["catalog"]["name"] == "public"
        assert data["schema_"] == "basic"
        assert data["table"] == "comments"
        assert data["columns"] == [
            {
                "name": "id",
                "type": "int",
                "display_name": "Id",
                "attributes": [],
                "dimension": None,
                "partition": None,
            },
            {
                "name": "user_id",
                "type": "int",
                "display_name": "User Id",
                "attributes": [],
                "dimension": None,
                "partition": None,
            },
            {
                "name": "timestamp",
                "type": "timestamp",
                "display_name": "Timestamp",
                "attributes": [],
                "dimension": None,
                "partition": None,
            },
            {
                "name": "text",
                "type": "string",
                "display_name": "Text",
                "attributes": [],
                "dimension": None,
                "partition": None,
            },
        ]
        assert response.status_code == 201

    def test_refresh_source_node(
        self,
        client_with_query_service_example_loader,
    ):
        """
        Refresh a source node with a query service
        """
        custom_client = client_with_query_service_example_loader(["ROADS"])
        response = custom_client.post(
            "/nodes/default.repair_orders/refresh/",
        )
        data = response.json()

        # Columns have changed, so the new node revision should be bumped to a new
        # version with an additional `ratings` column. Existing dimension links remain
        new_columns = [
            {
                "attributes": [],
                "dimension": {"name": "default.repair_order"},
                "display_name": "Repair Order Id",
                "name": "repair_order_id",
                "type": "int",
                "partition": None,
            },
            {
                "attributes": [],
                "dimension": None,
                "display_name": "Municipality Id",
                "name": "municipality_id",
                "type": "string",
                "partition": None,
            },
            {
                "attributes": [],
                "dimension": None,
                "display_name": "Hard Hat Id",
                "name": "hard_hat_id",
                "type": "int",
                "partition": None,
            },
            {
                "attributes": [],
                "dimension": None,
                "display_name": "Order Date",
                "name": "order_date",
                "type": "timestamp",
                "partition": None,
            },
            {
                "attributes": [],
                "dimension": None,
                "display_name": "Required Date",
                "name": "required_date",
                "type": "timestamp",
                "partition": None,
            },
            {
                "attributes": [],
                "dimension": None,
                "display_name": "Dispatched Date",
                "name": "dispatched_date",
                "type": "timestamp",
                "partition": None,
            },
            {
                "attributes": [],
                "dimension": None,
                "display_name": "Dispatcher Id",
                "name": "dispatcher_id",
                "type": "int",
                "partition": None,
            },
            {
                "attributes": [],
                "dimension": None,
                "display_name": "Rating",
                "name": "rating",
                "type": "int",
                "partition": None,
            },
        ]

        assert data["version"] == "v2.0"
        assert data["columns"] == new_columns
        assert response.status_code == 201

        response = custom_client.get("/history?node=default.repair_orders")
        history = response.json()
        assert [
            (activity["activity_type"], activity["entity_type"]) for activity in history
        ] == [("create", "node"), ("create", "link"), ("refresh", "node")]

        # Refresh it again, but this time no columns will have changed so
        # verify that the node revision stays the same
        response = custom_client.post(
            "/nodes/default.repair_orders/refresh/",
        )
        data_second = response.json()
        assert data_second["version"] == "v2.0"
        assert data_second["node_revision_id"] == data["node_revision_id"]
        assert data_second["columns"] == new_columns

    def test_refresh_source_node_with_problems(
        self,
        client_with_query_service_example_loader,
        query_service_client: QueryServiceClient,
        mocker: MockerFixture,
    ):
        """
        Refresh a source node with a query service and find that no columns are returned.
        """
        custom_client = client_with_query_service_example_loader(["ROADS"])
        response = custom_client.post(
            "/nodes/default.repair_orders/refresh/",
        )
        data = response.json()

        the_good_columns = query_service_client.get_columns_for_table(
            "default",
            "roads",
            "repair_orders",
        )

        # Columns have changed, so the new node revision should be bumped to a new version
        assert data["version"] == "v2.0"
        assert len(data["columns"]) == 8
        assert response.status_code == 201
        assert data["status"] == "valid"
        assert data["missing_table"] is False

        response = custom_client.get("/history?node=default.repair_orders")
        history = response.json()
        assert [
            (activity["activity_type"], activity["entity_type"]) for activity in history
        ] == [("create", "node"), ("create", "link"), ("refresh", "node")]

        # Refresh it again, but this time no columns are found
        mocker.patch.object(
            query_service_client,
            "get_columns_for_table",
            lambda *args: [],
        )
        response = custom_client.post(
            "/nodes/default.repair_orders/refresh/",
        )
        data_second = response.json()
        assert data_second["version"] == "v3.0"
        assert data_second["node_revision_id"] != data["node_revision_id"]
        assert len(data_second["columns"]) == 8
        assert data_second["status"] == "valid"
        assert data_second["missing_table"] is True

        # Refresh it again, but this time the table is missing
        mocker.patch.object(
            query_service_client,
            "get_columns_for_table",
            lambda *args: (_ for _ in ()).throw(
                DJDoesNotExistException(message="Table not found: foo.bar.baz"),
            ),
        )
        response = custom_client.post(
            "/nodes/default.repair_orders/refresh/",
        )
        data_third = response.json()
        assert data_third["version"] == "v4.0"
        assert data_third["node_revision_id"] != data_second["node_revision_id"]
        assert len(data_third["columns"]) == 8
        assert data_third["status"] == "valid"
        assert data_third["missing_table"] is True

        # Refresh it again, back to normal state
        mocker.patch.object(
            query_service_client,
            "get_columns_for_table",
            lambda *args: the_good_columns,
        )
        response = custom_client.post(
            "/nodes/default.repair_orders/refresh/",
        )
        data_fourth = response.json()
        assert data_fourth["version"] == "v5.0"
        assert data_fourth["node_revision_id"] != data_second["node_revision_id"]
        assert len(data_fourth["columns"]) == 8
        assert data_fourth["status"] == "valid"
        assert data_fourth["missing_table"] is False

    def test_create_update_source_node(
        self,
        client_with_basic: TestClient,
    ) -> None:
        """
        Test creating and updating a source node
        """
        basic_source_comments = {
            "name": "basic.source.comments",
            "description": "A fact table with comments",
            "columns": [
                {"name": "id", "type": "int"},
                {
                    "name": "user_id",
                    "type": "int",
                    "dimension": "basic.dimension.users",
                },
                {"name": "timestamp", "type": "timestamp"},
                {"name": "text", "type": "string"},
            ],
            "mode": "published",
            "catalog": "public",
            "schema_": "basic",
            "table": "comments",
        }

        # Trying to create it again should fail
        response = client_with_basic.post(
            "/nodes/source/",
            json=basic_source_comments,
        )
        data = response.json()
        assert (
            data["message"]
            == "A node with name `basic.source.comments` already exists."
        )
        assert response.status_code == 409

        # Update node with a new description should create a new revision
        response = client_with_basic.patch(
            f"/nodes/{basic_source_comments['name']}/",
            json={
                "description": "New description",
                "display_name": "Comments facts",
            },
        )
        data = response.json()

        assert data["name"] == "basic.source.comments"
        assert data["display_name"] == "Comments facts"
        assert data["type"] == "source"
        assert data["version"] == "v1.1"
        assert data["description"] == "New description"

        # Try to update node with no changes
        response = client_with_basic.patch(
            f"/nodes/{basic_source_comments['name']}/",
            json={"description": "New description", "display_name": "Comments facts"},
        )
        new_data = response.json()
        assert data == new_data

        # Try to update a node with a table that has different columns
        response = client_with_basic.patch(
            f"/nodes/{basic_source_comments['name']}/",
            json={
                "columns": [
                    {"name": "id", "type": "int"},
                    {
                        "name": "user_id",
                        "type": "int",
                        "dimension": "basic.dimension.users",
                    },
                    {"name": "timestamp", "type": "timestamp"},
                    {"name": "text_v2", "type": "string"},
                ],
            },
        )
        data = response.json()
        assert data["version"] == "v2.0"
        assert data["columns"] == [
            {
                "attributes": [],
                "dimension": None,
                "display_name": "Id",
                "name": "id",
                "type": "int",
                "partition": None,
            },
            {
                "attributes": [],
                "dimension": None,
                "display_name": "User Id",
                "name": "user_id",
                "type": "int",
                "partition": None,
            },
            {
                "attributes": [],
                "dimension": None,
                "display_name": "Timestamp",
                "name": "timestamp",
                "type": "timestamp",
                "partition": None,
            },
            {
                "attributes": [],
                "dimension": None,
                "display_name": "Text V2",
                "name": "text_v2",
                "type": "string",
                "partition": None,
            },
        ]

    def test_update_nonexistent_node(
        self,
        client: TestClient,
    ) -> None:
        """
        Test updating a non-existent node.
        """

        response = client.patch(
            "/nodes/something/",
            json={"description": "new"},
        )
        data = response.json()
        assert response.status_code == 404
        assert data["message"] == "A node with name `something` does not exist."

    def test_raise_on_source_node_with_no_catalog(
        self,
        client: TestClient,
    ) -> None:
        """
        Test raise on source node with no catalog
        """
        response = client.post(
            "/nodes/source/",
            json={
                "name": "basic.source.comments",
                "description": "A fact table with comments",
                "columns": [
                    {"name": "id", "type": "int"},
                    {
                        "name": "user_id",
                        "type": "int",
                        "dimension": "basic.dimension.users",
                    },
                    {"name": "timestamp", "type": "timestamp"},
                    {"name": "text", "type": "string"},
                ],
                "mode": "published",
            },
        )
        assert not response.ok
        assert response.json() == {
            "detail": [
                {
                    "loc": ["body", "catalog"],
                    "msg": "field required",
                    "type": "value_error.missing",
                },
                {
                    "loc": ["body", "schema_"],
                    "msg": "field required",
                    "type": "value_error.missing",
                },
                {
                    "loc": ["body", "table"],
                    "msg": "field required",
                    "type": "value_error.missing",
                },
            ],
        }

    def test_create_invalid_transform_node(
        self,
        catalog: Catalog,  # pylint: disable=unused-argument
        source_node: Node,  # pylint: disable=unused-argument
        client: TestClient,
        create_invalid_transform_node_payload: Dict[str, Any],
    ) -> None:
        """
        Test creating an invalid transform node in draft and published modes.
        """
        client.post("/namespaces/default/")
        response = client.post(
            "/nodes/transform/",
            json=create_invalid_transform_node_payload,
        )
        data = response.json()
        assert response.status_code == 400
        assert data["message"].startswith(
            "Node definition contains references to nodes that do not exist",
        )

    def test_create_node_with_type_inference_failure(
        self,
        client_with_namespaced_roads: TestClient,
    ):
        """
        Attempting to create a published metric where type inference fails should raise
        an appropriate error and fail.
        """
        response = client_with_namespaced_roads.post(
            "/nodes/metric/",
            json={
                "description": "Average length of employment",
                "query": (
                    "SELECT avg(NOW() - hire_date + 1) as "
                    "default_DOT_avg_length_of_employment_plus_one "
                    "FROM foo.bar.hard_hats"
                ),
                "mode": "published",
                "name": "default.avg_length_of_employment_plus_one",
            },
        )
        data = response.json()
        assert data == {
            "message": (
                "Unable to infer type for some columns on node "
                "`default.avg_length_of_employment_plus_one`.\n\n"
                "\t* Incompatible types in binary operation NOW() - "
                "foo.bar.hard_hats.hire_date + 1. Got left timestamp, rig"
            ),
            "errors": [
                {
                    "code": 302,
                    "message": (
                        "Unable to infer type for some columns on node "
                        "`default.avg_length_of_employment_plus_one`.\n\n"
                        "\t* Incompatible types in binary operation NOW() - "
                        "foo.bar.hard_hats.hire_date + 1. Got left timestamp, rig"
                    ),
                    "debug": {
                        "columns": {
                            "default_DOT_avg_length_of_employment_plus_one": (
                                "Incompatible types in binary operation NOW() - "
                                "foo.bar.hard_hats.hire_date + 1. Got left timestamp, right int."
                            ),
                        },
                        "errors": [],
                    },
                    "context": "",
                },
            ],
            "warnings": [],
        }

    def test_create_update_transform_node(
        self,
        catalog: Catalog,  # pylint: disable=unused-argument
        source_node: Node,  # pylint: disable=unused-argument
        client: TestClient,
        create_transform_node_payload: Dict[str, Any],
    ) -> None:
        """
        Test creating and updating a transform node that references an existing source.
        """

        client.post("/namespaces/default/")
        # Create a transform node
        response = client.post(
            "/nodes/transform/",
            json=create_transform_node_payload,
        )
        data = response.json()
        assert data["name"] == "default.country_agg"
        assert data["display_name"] == "Default: Country Agg"
        assert data["type"] == "transform"
        assert data["description"] == "Distinct users per country"
        assert (
            data["query"]
            == "SELECT country, COUNT(DISTINCT id) AS num_users FROM basic.source.users"
        )
        assert data["status"] == "valid"
        assert data["columns"] == [
            {
                "attributes": [],
                "dimension": None,
                "display_name": "Country",
                "name": "country",
                "type": "string",
                "partition": None,
            },
            {
                "attributes": [],
                "dimension": None,
                "display_name": "Num Users",
                "name": "num_users",
                "type": "bigint",
                "partition": None,
            },
        ]

        assert data["parents"] == [{"name": "basic.source.users"}]

        # Update the transform node with two minor changes
        response = client.patch(
            "/nodes/default.country_agg/",
            json={
                "description": "Some new description",
                "display_name": "Default: Country Aggregation by User",
            },
        )
        data = response.json()
        assert data["name"] == "default.country_agg"
        assert data["display_name"] == "Default: Country Aggregation by User"
        assert data["type"] == "transform"
        assert data["version"] == "v1.1"
        assert data["description"] == "Some new description"
        assert (
            data["query"]
            == "SELECT country, COUNT(DISTINCT id) AS num_users FROM basic.source.users"
        )
        assert data["status"] == "valid"
        assert data["parents"] == [{"name": "basic.source.users"}]

        # Try to update with a new query that references a non-existent source
        response = client.patch(
            "/nodes/default.country_agg/",
            json={
                "query": "SELECT country, COUNT(DISTINCT id) AS num_users FROM comments",
            },
        )
        data = response.json()
        assert data["message"].startswith(
            "Node definition contains references to nodes that do not exist",
        )

        # Try to update with a new query that references an existing source
        response = client.patch(
            "/nodes/default.country_agg/",
            json={
                "query": "SELECT country, COUNT(DISTINCT id) AS num_users, "
                "COUNT(*) AS num_entries FROM basic.source.users",
            },
        )
        data = response.json()
        assert data["version"] == "v2.0"
        assert (
            data["query"] == "SELECT country, COUNT(DISTINCT id) AS num_users, "
            "COUNT(*) AS num_entries FROM basic.source.users"
        )
        assert data["columns"] == [
            {
                "attributes": [],
                "dimension": None,
                "display_name": "Country",
                "name": "country",
                "type": "string",
                "partition": None,
            },
            {
                "attributes": [],
                "dimension": None,
                "display_name": "Num Users",
                "name": "num_users",
                "type": "bigint",
                "partition": None,
            },
            {
                "attributes": [],
                "dimension": None,
                "display_name": "Num Entries",
                "name": "num_entries",
                "type": "bigint",
                "partition": None,
            },
        ]

        assert data["status"] == "valid"
        assert data["parents"] == [{"name": "basic.source.users"}]

        # Verify that asking for revisions for a non-existent transform fails
        response = client.get("/nodes/random_transform/revisions/")
        data = response.json()
        assert data["message"] == "A node with name `random_transform` does not exist."

        # Verify that all historical revisions are available for the node
        response = client.get("/nodes/default.country_agg/revisions/")
        data = response.json()
        assert {rev["version"]: rev["query"] for rev in data} == {
            "v1.0": "SELECT country, COUNT(DISTINCT id) AS num_users FROM basic.source.users",
            "v1.1": "SELECT country, COUNT(DISTINCT id) AS num_users FROM basic.source.users",
            "v2.0": "SELECT country, COUNT(DISTINCT id) AS num_users, COUNT(*) AS num_entries "
            "FROM basic.source.users",
        }
        assert {rev["version"]: rev["columns"] for rev in data} == {
            "v1.0": [
                {
                    "attributes": [],
                    "dimension": None,
                    "display_name": "Country",
                    "name": "country",
                    "type": "string",
                    "partition": None,
                },
                {
                    "attributes": [],
                    "dimension": None,
                    "display_name": "Num Users",
                    "name": "num_users",
                    "type": "bigint",
                    "partition": None,
                },
            ],
            "v1.1": [
                {
                    "attributes": [],
                    "dimension": None,
                    "display_name": "Country",
                    "name": "country",
                    "type": "string",
                    "partition": None,
                },
                {
                    "attributes": [],
                    "dimension": None,
                    "display_name": "Num Users",
                    "name": "num_users",
                    "type": "bigint",
                    "partition": None,
                },
            ],
            "v2.0": [
                {
                    "attributes": [],
                    "dimension": None,
                    "display_name": "Country",
                    "name": "country",
                    "type": "string",
                    "partition": None,
                },
                {
                    "attributes": [],
                    "dimension": None,
                    "display_name": "Num Users",
                    "name": "num_users",
                    "type": "bigint",
                    "partition": None,
                },
                {
                    "attributes": [],
                    "dimension": None,
                    "display_name": "Num Entries",
                    "name": "num_entries",
                    "type": "bigint",
                    "partition": None,
                },
            ],
        }

    def test_update_metric_node(self, client_with_roads: TestClient):
        """
        Verify that during metric node updates, if the query changes, DJ will automatically
        alias the metric column. If this aliased query is the same as the current revision's
        query, DJ won't promote the version.
        """
        response = client_with_roads.patch(
            "/nodes/default.total_repair_cost/",
            json={
                "query": (
                    "SELECT sum(repair_orders_fact.total_repair_cost) "
                    "FROM default.repair_orders_fact repair_orders_fact"
                ),
                "metric_metadata": {
                    "kind": "count",
                    "direction": "higher_is_better",
                    "unit": "dollar",
                },
            },
        )
        node_data = response.json()
        assert node_data["query"] == (
            "SELECT sum(repair_orders_fact.total_repair_cost) "
            "FROM default.repair_orders_fact repair_orders_fact"
        )
        response = client_with_roads.get("/metrics/default.total_repair_cost")
        metric_data = response.json()
        assert metric_data["metric_metadata"] == {
            "direction": "higher_is_better",
            "unit": {
                "abbreviation": None,
                "category": None,
                "description": None,
                "label": "Dollar",
                "name": "DOLLAR",
            },
        }

        response = client_with_roads.get("/nodes/default.total_repair_cost")
        assert response.json()["version"] == "v1.1"

        response = client_with_roads.patch(
            "/nodes/default.total_repair_cost/",
            json={
                "query": "SELECT count(price) FROM default.repair_order_details",
                "required_dimensions": ["repair_order_id"],
            },
        )
        node_data = response.json()
        assert node_data["query"] == (
            "SELECT count(price) FROM default.repair_order_details"
        )
        response = client_with_roads.get("/nodes/default.total_repair_cost")
        data = response.json()
        assert data["version"] == "v2.0"
        response = client_with_roads.get("/metrics/default.total_repair_cost")
        data = response.json()
        assert data["required_dimensions"] == ["repair_order_id"]

    def test_create_dimension_node_fails(
        self,
        catalog: Catalog,  # pylint: disable=unused-argument
        source_node: Node,  # pylint: disable=unused-argument
        client: TestClient,
    ):
        """
        Test various failure cases for dimension node creation.
        """
        client.post("/namespaces/default/")
        response = client.post(
            "/nodes/dimension/",
            json={
                "description": "Country dimension",
                "query": "SELECT country, COUNT(1) AS user_cnt "
                "FROM basic.source.users GROUP BY country",
                "mode": "published",
                "name": "countries",
            },
        )
        assert (
            response.json()["message"] == "Dimension nodes must define a primary key!"
        )

        response = client.post(
            "/nodes/dimension/",
            json={
                "description": "Country dimension",
                "query": "SELECT country, COUNT(1) AS user_cnt "
                "FROM basic.source.users GROUP BY country",
                "mode": "published",
                "name": "default.countries",
                "primary_key": ["country", "id"],
            },
        )
        assert response.json()["message"] == (
            "Some columns in the primary key [country,id] were not "
            "found in the list of available columns for the node "
            "default.countries."
        )

    def test_create_update_dimension_node(
        self,
        catalog: Catalog,  # pylint: disable=unused-argument
        source_node: Node,  # pylint: disable=unused-argument
        client: TestClient,
        create_dimension_node_payload: Dict[str, Any],
    ) -> None:
        """
        Test creating and updating a dimension node that references an existing source.
        """
        client.post("/namespaces/default/")
        response = client.post(
            "/nodes/dimension/",
            json=create_dimension_node_payload,
        )
        data = response.json()

        assert response.status_code == 201
        assert data["name"] == "default.countries"
        assert data["display_name"] == "Default: Countries"
        assert data["type"] == "dimension"
        assert data["version"] == "v1.0"
        assert data["description"] == "Country dimension"
        assert (
            data["query"] == "SELECT country, COUNT(1) AS user_cnt "
            "FROM basic.source.users GROUP BY country"
        )
        assert data["columns"] == [
            {
                "attributes": [
                    {"attribute_type": {"name": "primary_key", "namespace": "system"}},
                ],
                "dimension": None,
                "display_name": "Country",
                "name": "country",
                "type": "string",
                "partition": None,
            },
            {
                "attributes": [],
                "dimension": None,
                "display_name": "User Cnt",
                "name": "user_cnt",
                "type": "bigint",
                "partition": None,
            },
        ]

        # Test updating the dimension node with a new query
        response = client.patch(
            "/nodes/default.countries/",
            json={"query": "SELECT country FROM basic.source.users GROUP BY country"},
        )
        data = response.json()
        # Should result in a major version update due to the query change
        assert data["version"] == "v2.0"

        # The columns should have been updated
        assert data["columns"] == [
            {
                "attributes": [
                    {"attribute_type": {"name": "primary_key", "namespace": "system"}},
                ],
                "dimension": None,
                "display_name": "Country",
                "name": "country",
                "type": "string",
                "partition": None,
            },
        ]

        # Test updating the dimension node with a new primary key
        response = client.patch(
            "/nodes/default.countries/",
            json={
                "query": "SELECT country, SUM(age) as sum_age, count(1) AS num_users "
                "FROM basic.source.users GROUP BY country",
                "primary_key": ["sum_age"],
            },
        )
        data = response.json()
        # Should result in a major version update
        assert data["version"] == "v3.0"
        assert data["columns"] == [
            {
                "attributes": [],
                "dimension": None,
                "display_name": "Country",
                "name": "country",
                "type": "string",
                "partition": None,
            },
            {
                "attributes": [
                    {"attribute_type": {"name": "primary_key", "namespace": "system"}},
                ],
                "dimension": None,
                "display_name": "Sum Age",
                "name": "sum_age",
                "type": "bigint",
                "partition": None,
            },
            {
                "attributes": [],
                "dimension": None,
                "display_name": "Num Users",
                "name": "num_users",
                "type": "bigint",
                "partition": None,
            },
        ]

        response = client.patch(
            "/nodes/default.countries/",
            json={
                "primary_key": ["country"],
            },
        )
        data = response.json()
        assert data["columns"] == [
            {
                "attributes": [
                    {"attribute_type": {"name": "primary_key", "namespace": "system"}},
                ],
                "dimension": None,
                "display_name": "Country",
                "name": "country",
                "type": "string",
                "partition": None,
            },
            {
                "attributes": [],
                "dimension": None,
                "display_name": "Sum Age",
                "name": "sum_age",
                "type": "bigint",
                "partition": None,
            },
            {
                "attributes": [],
                "dimension": None,
                "display_name": "Num Users",
                "name": "num_users",
                "type": "bigint",
                "partition": None,
            },
        ]

    def test_raise_on_multi_catalog_node(self, client_example_loader):
        """
        Test raising when trying to select from multiple catalogs
        """
        custom_client = client_example_loader(["BASIC", "ACCOUNT_REVENUE"])
        response = custom_client.post(
            "/nodes/transform/",
            json={
                "query": (
                    "SELECT payment_id, payment_amount, customer_id, account_type "
                    "FROM default.revenue r LEFT JOIN basic.source.comments b on r.id = b.id"
                ),
                "description": "Multicatalog",
                "mode": "published",
                "name": "default.multicatalog",
            },
        )
        assert (
            "Cannot create nodes with multi-catalog dependencies"
            in response.json()["message"]
        )

    def test_updating_node_to_invalid_draft(
        self,
        catalog: Catalog,  # pylint: disable=unused-argument
        source_node: Node,  # pylint: disable=unused-argument
        client: TestClient,
        create_dimension_node_payload: Dict[str, Any],
    ) -> None:
        """
        Test creating an invalid node in draft mode
        """
        client.post("/namespaces/default/")
        response = client.post(
            "/nodes/dimension/",
            json=create_dimension_node_payload,
        )
        data = response.json()

        assert response.status_code == 201
        assert data["name"] == "default.countries"
        assert data["display_name"] == "Default: Countries"
        assert data["type"] == "dimension"
        assert data["version"] == "v1.0"
        assert data["description"] == "Country dimension"
        assert (
            data["query"] == "SELECT country, COUNT(1) AS user_cnt "
            "FROM basic.source.users GROUP BY country"
        )
        assert data["columns"] == [
            {
                "attributes": [
                    {"attribute_type": {"name": "primary_key", "namespace": "system"}},
                ],
                "dimension": None,
                "display_name": "Country",
                "name": "country",
                "type": "string",
                "partition": None,
            },
            {
                "attributes": [],
                "dimension": None,
                "display_name": "User Cnt",
                "name": "user_cnt",
                "type": "bigint",
                "partition": None,
            },
        ]

        response = client.patch(
            "/nodes/default.countries/",
            json={"mode": "draft"},
        )
        assert response.status_code == 200

        # Test updating the dimension node with an invalid query
        response = client.patch(
            "/nodes/default.countries/",
            json={"query": "SELECT country FROM missing_parent GROUP BY country"},
        )
        assert response.status_code == 200

        # Check that node is now a draft with an invalid status
        response = client.get("/nodes/default.countries")
        assert response.status_code == 200
        data = response.json()
        assert data["mode"] == "draft"
        assert data["status"] == "invalid"

    def test_upsert_materialization_config(  # pylint: disable=too-many-arguments
        self,
        client_with_query_service_example_loader,
    ) -> None:
        """
        Test creating & updating materialization config for a node.
        """
        custom_client = client_with_query_service_example_loader(["BASIC"])
        # Setting the materialization config for a source node should fail
        response = custom_client.post(
            "/nodes/basic.source.comments/materialization/",
            json={
                "job": "spark_sql",
                "schedule": "0 * * * *",
                "config": {},
                "strategy": "full",
            },
        )
        assert response.status_code == 400
        assert (
            response.json()["message"]
            == "Cannot set materialization config for source node `basic.source.comments`!"
        )

        # Setting the materialization config for a materialization job type that
        # doesn't exist should fail
        response = custom_client.post(
            "/nodes/basic.transform.country_agg/materialization/",
            json={
                "job": "something",
                "strategy": "full",
                "config": {},
                "schedule": "0 * * * *",
            },
        )
        assert response.status_code == 404
        data = response.json()
        assert data["message"] == (
            "Materialization job type `SOMETHING` not found. Available job "
            "types: ['SPARK_SQL', 'DRUID_CUBE']"
        )

    def test_node_with_struct(self, client_with_roads: TestClient):
        """
        Test that building a query string with structs yields a correctly formatted struct
        reference.
        """
        response = client_with_roads.post(
            "/nodes/transform/",
            json={
                "description": "Regional level agg with structs",
                "query": """SELECT
    usr.us_region_id,
    us.state_name,
    CONCAT(us.state_name, '-', usr.us_region_description) AS location_hierarchy,
    EXTRACT(YEAR FROM ro.order_date) AS order_year,
    EXTRACT(MONTH FROM ro.order_date) AS order_month,
    EXTRACT(DAY FROM ro.order_date) AS order_day,
    STRUCT(
        COUNT(DISTINCT CASE WHEN ro.dispatched_date IS NOT NULL THEN ro.repair_order_id ELSE NULL END) AS completed_repairs,
        COUNT(DISTINCT ro.repair_order_id) AS total_repairs_dispatched,
        SUM(rd.price * rd.quantity) AS total_amount_in_region,
        AVG(rd.price * rd.quantity) AS avg_repair_amount_in_region,
        AVG(DATEDIFF(ro.dispatched_date, ro.order_date)) AS avg_dispatch_delay,
        COUNT(DISTINCT c.contractor_id) AS unique_contractors
    ) AS measures
FROM default.repair_orders ro
JOIN
    default.municipality m ON ro.municipality_id = m.municipality_id
JOIN
    default.us_states us ON m.state_id = us.state_id
JOIN
    default.us_states us ON m.state_id = us.state_id
JOIN
    default.us_region usr ON us.state_region = usr.us_region_id
JOIN
    default.repair_order_details rd ON ro.repair_order_id = rd.repair_order_id
JOIN
    default.repair_type rt ON rd.repair_type_id = rt.repair_type_id
JOIN
    default.contractors c ON rt.contractor_id = c.contractor_id
GROUP BY
    usr.us_region_id,
    EXTRACT(YEAR FROM ro.order_date),
    EXTRACT(MONTH FROM ro.order_date),
    EXTRACT(DAY FROM ro.order_date)""",
                "mode": "published",
                "name": "default.regional_level_agg_structs",
                "primary_key": [
                    "us_region_id",
                    "state_name",
                    "order_year",
                    "order_month",
                    "order_day",
                ],
            },
        )
        assert {
            "attributes": [],
            "dimension": None,
            "name": "measures",
            "type": "struct<completed_repairs: bigint, "
            "total_repairs_dispatched: bigint, "
            "total_amount_in_region: double, "
            "avg_repair_amount_in_region: double, "
            "avg_dispatch_delay: double, unique_contractors: "
            "bigint>",
            "display_name": "Measures",
            "partition": None,
        } in response.json()["columns"]

        client_with_roads.post(
            "/nodes/transform/",
            json={
                "description": "Total Repair Amounts during the COVID-19 Pandemic",
                "name": "default.total_amount_in_region_from_struct_transform",
                "query": "SELECT location_hierarchy, SUM(IF(order_year = 2020, "
                "measures.total_amount_in_region, 0)) "
                "col0 FROM default.regional_level_agg_structs",
                "mode": "published",
            },
        )
        response = client_with_roads.get(
            "/sql/default.total_amount_in_region_from_struct_transform?filters="
            "&dimensions=location_hierarchy",
        )
        expected = """SELECT
          default_DOT_total_amount_in_region_from_struct_transform.location_hierarchy default_DOT_total_amount_in_region_from_struct_transform_DOT_location_hierarchy,
    default_DOT_total_amount_in_region_from_struct_transform.col0 default_DOT_total_amount_in_region_from_struct_transform_DOT_col0
 FROM (SELECT  default_DOT_regional_level_agg_structs.location_hierarchy,
    SUM(IF(default_DOT_regional_level_agg_structs.order_year = 2020, default_DOT_regional_level_agg_structs.measures.total_amount_in_region, 0)) col0
 FROM (SELECT  default_DOT_us_region.us_region_id,
    default_DOT_us_states.state_name,
    CONCAT(default_DOT_us_states.state_name, '-', default_DOT_us_region.us_region_description) AS location_hierarchy,
    EXTRACT(YEAR, default_DOT_repair_orders.order_date) AS order_year,
    EXTRACT(MONTH, default_DOT_repair_orders.order_date) AS order_month,
    EXTRACT(DAY, default_DOT_repair_orders.order_date) AS order_day,
    struct(COUNT( DISTINCT CASE
        WHEN default_DOT_repair_orders.dispatched_date IS NOT NULL THEN default_DOT_repair_orders.repair_order_id
        ELSE NULL
    END) AS completed_repairs, COUNT( DISTINCT default_DOT_repair_orders.repair_order_id) AS total_repairs_dispatched, SUM(default_DOT_repair_order_details.price * default_DOT_repair_order_details.quantity) AS total_amount_in_region, AVG(default_DOT_repair_order_details.price * default_DOT_repair_order_details.quantity) AS avg_repair_amount_in_region, AVG(DATEDIFF(default_DOT_repair_orders.dispatched_date, default_DOT_repair_orders.order_date)) AS avg_dispatch_delay, COUNT( DISTINCT default_DOT_contractors.contractor_id) AS unique_contractors) AS measures
 FROM roads.repair_orders AS default_DOT_repair_orders JOIN roads.municipality AS default_DOT_municipality ON default_DOT_repair_orders.municipality_id = default_DOT_municipality.municipality_id
JOIN roads.us_states AS default_DOT_us_states ON default_DOT_municipality.state_id = default_DOT_us_states.state_id
JOIN roads.us_states AS default_DOT_us_states ON default_DOT_municipality.state_id = default_DOT_us_states.state_id
JOIN roads.us_region AS default_DOT_us_region ON default_DOT_us_states.state_region = default_DOT_us_region.us_region_id
JOIN roads.repair_order_details AS default_DOT_repair_order_details ON default_DOT_repair_orders.repair_order_id = default_DOT_repair_order_details.repair_order_id
JOIN roads.repair_type AS default_DOT_repair_type ON default_DOT_repair_order_details.repair_type_id = default_DOT_repair_type.repair_type_id
JOIN roads.contractors AS default_DOT_contractors ON default_DOT_repair_type.contractor_id = default_DOT_contractors.contractor_id
 GROUP BY  default_DOT_us_region.us_region_id, EXTRACT(YEAR, default_DOT_repair_orders.order_date), EXTRACT(MONTH, default_DOT_repair_orders.order_date), EXTRACT(DAY, default_DOT_repair_orders.order_date))
 AS default_DOT_regional_level_agg_structs)
 AS default_DOT_total_amount_in_region_from_struct_transform"""
        assert compare_query_strings(response.json()["sql"], expected)

    def test_node_with_incremental_time_materialization(
        self,
        client_with_query_service_example_loader,
        query_service_client,
    ) -> None:
        """
        1. Create a transform node that uses dj_logical_timestamp (i.e., it is
           meant to be incrementally materialized).
        2. Create a metric node that references the above transform.
        3. When SQL for the metric is requested without the transform having been materialized,
           the request will fail.
        """
        custom_client = client_with_query_service_example_loader(["ROADS"])
        custom_client.post(
            "/nodes/transform/",
            json={
                "description": "Repair orders transform (partitioned)",
                "query": """
                    SELECT
                        repair_order_id,
                        municipality_id,
                        hard_hat_id,
                        order_date,
                        required_date,
                        dispatched_date,
                        dispatcher_id
                    FROM default.repair_orders
                    """,
                "mode": "published",
                "name": "default.repair_orders_partitioned",
                "primary_key": ["repair_order_id"],
            },
        )
        # Mark one of the columns as a time partition
        custom_client.post(
            "/nodes/default.repair_orders_partitioned/columns/dispatched_date/partition",
            json={
                "type_": "temporal",
                "granularity": "day",
                "format": "yyyyMMdd",
            },
        )

        # Set an incremental time materialization config with a lookback window of 100 days
        custom_client.post(
            "/nodes/default.repair_orders_partitioned/materialization/",
            json={
                "job": "spark_sql",
                "strategy": "incremental_time",
                "config": {
                    "lookback_window": "100 DAYS",
                },
                "schedule": "0 * * * *",
            },
        )

        args, _ = query_service_client.materialize.call_args_list[0]  # type: ignore
        format_regex = r"\${(?P<capture>[^}]+)}"
        match = re.search(format_regex, args[0].query)
        assert match and match.group("capture") == "dj_logical_timestamp"
        query = re.sub(format_regex, "DJ_LOGICAL_TIMESTAMP()", args[0].query)
        expected_query = """
        SELECT
          repair_order_id,
          municipality_id,
          hard_hat_id,
          order_date,
          required_date,
          dispatched_date,
          dispatcher_id
        FROM (
          SELECT
            default_DOT_repair_orders.repair_order_id,
            default_DOT_repair_orders.municipality_id,
            default_DOT_repair_orders.hard_hat_id,
            default_DOT_repair_orders.order_date,
            default_DOT_repair_orders.required_date,
            default_DOT_repair_orders.dispatched_date,
            default_DOT_repair_orders.dispatcher_id
          FROM roads.repair_orders AS default_DOT_repair_orders
        ) AS default_DOT_repair_orders_partitioned
        WHERE
          dispatched_date BETWEEN CAST(
              DATE_FORMAT(
                CAST(DJ_LOGICAL_TIMESTAMP() AS TIMESTAMP) - INTERVAL 100 DAYS, 'yyyyMMdd'
              ) AS TIMESTAMP
            )
            AND CAST(
              DATE_FORMAT(
                CAST(DJ_LOGICAL_TIMESTAMP() AS TIMESTAMP),
                'yyyyMMdd'
              ) AS TIMESTAMP
            )
        """
        compare_query_strings(query, expected_query)

        # Set an incremental time materialization config without a lookback window
        # (defaults to 1 day)
        custom_client.post(
            "/nodes/default.repair_orders_partitioned/materialization/",
            json={
                "job": "spark_sql",
                "strategy": "incremental_time",
                "config": {},
                "schedule": "0 * * * *",
            },
        )

        args, _ = query_service_client.materialize.call_args_list[0]  # type: ignore
        match = re.search(format_regex, args[0].query)
        assert match and match.group("capture") == "dj_logical_timestamp"
        query = re.sub(format_regex, "DJ_LOGICAL_TIMESTAMP()", args[0].query)
        expected_query = """
        SELECT
          repair_order_id,
          municipality_id,
          hard_hat_id,
          order_date,
          required_date,
          dispatched_date,
          dispatcher_id
        FROM (
          SELECT
            default_DOT_repair_orders.repair_order_id,
            default_DOT_repair_orders.municipality_id,
            default_DOT_repair_orders.hard_hat_id,
            default_DOT_repair_orders.order_date,
            default_DOT_repair_orders.required_date,
            default_DOT_repair_orders.dispatched_date,
            default_DOT_repair_orders.dispatcher_id
          FROM roads.repair_orders AS default_DOT_repair_orders
        ) AS default_DOT_repair_orders_partitioned
        WHERE  dispatched_date = CAST(
          DATE_FORMAT(
            CAST(DJ_LOGICAL_TIMESTAMP() AS TIMESTAMP),
            'yyyyMMdd'
          ) AS TIMESTAMP
        )
        """
        compare_query_strings(query, expected_query)

    def test_node_with_dj_logical_timestamp(
        self,
        client_with_query_service_example_loader,
    ) -> None:
        """
        1. Create a transform node that uses dj_logical_timestamp (i.e., it is
           meant to be incrementally materialized).
        2. Create a metric node that references the above transform.
        3. When SQL for the metric is requested without the transform having been materialized,
           the request will fail.
        """
        custom_client = client_with_query_service_example_loader(["ROADS"])
        custom_client.post(
            "/nodes/transform/",
            json={
                "description": "Repair orders transform (partitioned)",
                "query": """
                        SELECT
                        repair_order_id,
                        municipality_id,
                        hard_hat_id,
                        order_date,
                        required_date,
                        dispatched_date,
                        dispatcher_id,
                        dj_logical_timestamp('%Y%m%d') as date_partition
                        FROM default.repair_orders
                        WHERE date_format(order_date, 'yyyyMMdd') = dj_logical_timestamp('%Y%m%d')
                    """,
                "mode": "published",
                "name": "default.repair_orders_partitioned",
                "primary_key": ["repair_order_id"],
            },
        )
        custom_client.post(
            "/nodes/default.repair_orders_partitioned/columns/hard_hat_id/"
            "?dimension=default.hard_hat&dimension_column=hard_hat_id",
        )

        custom_client.post(
            "/nodes/metric/",
            json={
                "description": "Number of repair orders",
                "query": "SELECT count(repair_order_id) FROM default.repair_orders_partitioned",
                "mode": "published",
                "name": "default.num_repair_orders_partitioned",
            },
        )
        response = custom_client.get(
            "/sql?metrics=default.num_repair_orders_partitioned"
            "&dimensions=default.hard_hat.last_name",
        )
        format_regex = r"\${(?P<capture>[^}]+)}"

        result_sql = response.json()["sql"]

        match = re.search(format_regex, result_sql)
        assert match and match.group("capture") == "dj_logical_timestamp"
        query = re.sub(format_regex, "FORMATTED", result_sql)
        compare_query_strings(
            query,
            """WITH
m0_default_DOT_num_repair_orders_partitioned AS (SELECT  default_DOT_hard_hat.last_name,
        count(default_DOT_repair_orders_partitioned.repair_order_id)
        default_DOT_num_repair_orders_partitioned
 FROM (SELECT  FORMATTED AS date_partition,
        default_DOT_repair_orders.dispatched_date,
        default_DOT_repair_orders.dispatcher_id,
        default_DOT_repair_orders.hard_hat_id,
        default_DOT_repair_orders.municipality_id,
        default_DOT_repair_orders.order_date,
        default_DOT_repair_orders.repair_order_id,
        default_DOT_repair_orders.required_date
 FROM roads.repair_orders AS default_DOT_repair_orders
 WHERE  date_format(default_DOT_repair_orders.order_date, 'yyyyMMdd') = FORMATTED)
 AS default_DOT_repair_orders_partitioned LEFT OUTER JOIN
 (SELECT  default_DOT_hard_hats.hard_hat_id,
        default_DOT_hard_hats.last_name,
        default_DOT_hard_hats.state
 FROM roads.hard_hats AS default_DOT_hard_hats)
 AS default_DOT_hard_hat ON
 default_DOT_repair_orders_partitioned.hard_hat_id = default_DOT_hard_hat.hard_hat_id
 GROUP BY  default_DOT_hard_hat.last_name
)

SELECT  m0_default_DOT_num_repair_orders_partitioned.default_DOT_num_repair_orders_partitioned,
        m0_default_DOT_num_repair_orders_partitioned.last_name
 FROM m0_default_DOT_num_repair_orders_partitioned""",
        )

        custom_client.post(
            "/engines/",
            json={
                "name": "spark",
                "version": "2.4.4",
                "dialect": "spark",
            },
        )

        # Setting the materialization config should succeed
        response = custom_client.post(
            "/nodes/default.repair_orders_partitioned/materialization/",
            json={
                "job": "spark_sql",
                "strategy": "full",
                "config": {
                    "partitions": [],
                },
                "schedule": "0 * * * *",
            },
        )
        data = response.json()
        assert (
            data["message"] == "Successfully updated materialization config named "
            "`spark_sql__full` for node `default.repair_orders_partitioned`"
        )

        response = custom_client.get(
            "/nodes/default.repair_orders_partitioned",
        )
        result_sql = response.json()["materializations"][0]["config"]["query"]
        match = re.search(format_regex, result_sql)
        assert match and match.group("capture") == "dj_logical_timestamp"
        query = re.sub(format_regex, "FORMATTED", result_sql)
        compare_query_strings(
            query,
            "SELECT  FORMATTED AS date_partition,\n\t"
            "default_DOT_repair_orders.dispatched_date,\n\t"
            "default_DOT_repair_orders.dispatcher_id,\n\t"
            "default_DOT_repair_orders.hard_hat_id,\n\t"
            "default_DOT_repair_orders.municipality_id,\n\t"
            "default_DOT_repair_orders.order_date,\n\t"
            "default_DOT_repair_orders.repair_order_id,\n\t"
            "default_DOT_repair_orders.required_date \n"
            " FROM roads.repair_orders AS "
            "default_DOT_repair_orders \n"
            " WHERE  "
            "date_format(default_DOT_repair_orders.order_date, "
            "'yyyyMMdd') = FORMATTED\n\n",
        )

    def test_update_node_query_with_materializations(
        self,
        client_with_query_service_example_loader,
    ):
        """
        Testing updating a node's query when the node already has materializations. The node's
        materializations should be updated based on the new query and rescheduled.
        """
        custom_client = client_with_query_service_example_loader(["BASIC"])
        custom_client.post(
            "/engines/",
            json={
                "name": "spark",
                "version": "2.4.4",
                "dialect": "spark",
            },
        )

        custom_client.post(
            "/nodes/basic.transform.country_agg/materialization/",
            json={
                "job": "spark_sql",
                "strategy": "full",
                "config": {
                    "spark": {},
                },
                "schedule": "0 * * * *",
            },
        )
        custom_client.patch(
            "/nodes/basic.transform.country_agg/",
            json={
                "query": (
                    "SELECT country, COUNT(DISTINCT id) AS num_users, "
                    "COUNT(DISTINCT preferred_language) AS languages "
                    "FROM basic.source.users GROUP BY 1"
                ),
            },
        )
        response = custom_client.get("/nodes/basic.transform.country_agg")
        assert response.json()["version"] == "v2.0"
        response = custom_client.get("/nodes/basic.transform.country_agg/")
        node_output = response.json()
        assert node_output["materializations"] == [
            {
                "backfills": [],
                "config": {
                    "columns": [
                        {
                            "column": None,
                            "name": "country",
                            "node": None,
                            "semantic_entity": None,
                            "semantic_type": None,
                            "type": "string",
                        },
                        {
                            "column": None,
                            "name": "num_users",
                            "node": None,
                            "semantic_entity": None,
                            "semantic_type": None,
                            "type": "bigint",
                        },
                        {
                            "column": None,
                            "name": "languages",
                            "node": None,
                            "semantic_entity": None,
                            "semantic_type": None,
                            "type": "bigint",
                        },
                    ],
                    "lookback_window": None,
                    "query": "SELECT  basic_DOT_transform_DOT_country_agg.country,\n"
                    "\tbasic_DOT_transform_DOT_country_agg.num_users,\n"
                    "\tbasic_DOT_transform_DOT_country_agg.languages \n"
                    " FROM (SELECT  basic_DOT_source_DOT_users.country,\n"
                    "\tCOUNT( DISTINCT basic_DOT_source_DOT_users.id) AS "
                    "num_users,\n"
                    "\tCOUNT( DISTINCT "
                    "basic_DOT_source_DOT_users.preferred_language) AS "
                    "languages \n"
                    " FROM basic.dim_users AS basic_DOT_source_DOT_users \n"
                    " GROUP BY  1)\n"
                    " AS basic_DOT_transform_DOT_country_agg\n"
                    "\n",
                    "spark": {},
                    "upstream_tables": ["public.basic.dim_users"],
                },
                "strategy": "full",
                "job": "SparkSqlMaterializationJob",
                "name": "spark_sql__full",
                "schedule": "0 * * * *",
            },
        ]

    def test_add_materialization_success(
        self,
        client_with_query_service: TestClient,
        query_service_client: QueryServiceClient,
    ):
        """
        Verifies success cases of adding materialization config.
        """
        # Create the engine and check the existing transform node
        client_with_query_service.post(
            "/engines/",
            json={
                "name": "spark",
                "version": "2.4.4",
                "dialect": "spark",
            },
        )

        response = client_with_query_service.get("/nodes/basic.transform.country_agg/")
        old_node_data = response.json()
        assert old_node_data["version"] == "v1.0"
        assert old_node_data["materializations"] == []

        client_with_query_service.post(
            "/nodes/basic.transform.country_agg/columns/"
            "basic_DOT_transform_DOT_country_agg_DOT_country/partition",
            json={
                "type_": "categorical",
                "expression": "",
            },
        )

        # Setting the materialization config should succeed
        response = client_with_query_service.post(
            "/nodes/basic.transform.country_agg/materialization/",
            json={
                "job": "spark_sql",
                "strategy": "full",
                "config": {},
                "schedule": "0 * * * *",
            },
        )
        data = response.json()
        assert (
            data["message"] == "Successfully updated materialization config named "
            "`spark_sql__full` for node `basic.transform.country_agg`"
        )

        # Check history of the node with materialization
        response = client_with_query_service.get(
            "/history?node=basic.transform.country_agg",
        )
        history = response.json()
        assert [
            (activity["activity_type"], activity["entity_type"]) for activity in history
        ] == [("create", "node"), ("create", "materialization")]

        # Setting it again should inform that it already exists
        response = client_with_query_service.post(
            "/nodes/basic.transform.country_agg/materialization/",
            json={
                "job": "spark_sql",
                "strategy": "full",
                "config": {},
                "schedule": "0 * * * *",
            },
        )
        assert response.json() == {
            "info": {
                "output_tables": ["common.a", "common.b"],
                "urls": ["http://fake.url/job"],
            },
            "message": "The same materialization config with name "
            "`spark_sql__full` already exists for node "
            "`basic.transform.country_agg` so no update was performed.",
        }

        response = client_with_query_service.delete(
            "/nodes/basic.transform.country_agg/materializations/"
            "?materialization_name=spark_sql__full",
        )
        assert response.json() == {
            "message": "The materialization named `spark_sql__full` on node "
            "`basic.transform.country_agg` has been successfully deactivated",
        }

        # Setting it again should inform that it already exists but was reactivated
        response = client_with_query_service.post(
            "/nodes/basic.transform.country_agg/materialization/",
            json={
                "job": "spark_sql",
                "strategy": "full",
                "config": {},
                "schedule": "0 * * * *",
            },
        )
        assert response.json()["message"] == (
            "The same materialization config with name `spark_sql__full` already "
            "exists for node `basic.transform.country_agg` but was deactivated. It has "
            "now been restored."
        )
        response = client_with_query_service.get(
            "/history?node=basic.transform.country_agg",
        )
        assert [
            (
                activity["activity_type"],
                activity["entity_type"],
                activity["entity_name"],
            )
            for activity in response.json()
        ] == [
            ("create", "node", "basic.transform.country_agg"),
            ("create", "materialization", "spark_sql__full"),
            ("delete", "materialization", "spark_sql__full"),
            ("restore", "materialization", "spark_sql__full"),
        ]

        # Setting the materialization config without partitions should succeed
        response = client_with_query_service.post(
            "/nodes/basic.transform.country_agg/materialization/",
            json={
                "job": "spark_sql",
                "strategy": "full",
                "config": {},
                "schedule": "0 * * * *",
            },
        )
        data = response.json()
        assert (
            data["message"]
            == "The same materialization config with name `spark_sql__full` already "
            "exists for node `basic.transform.country_agg` so no update was performed."
        )

        # Reading the node should yield the materialization config
        response = client_with_query_service.get("/nodes/basic.transform.country_agg/")
        data = response.json()
        assert data["version"] == "v1.0"
        materialization_compare(
            data["materializations"],
            [
                {
                    "backfills": [],
                    "name": "spark_sql__full",
                    "strategy": "full",
                    "config": {
                        "columns": [
                            {
                                "column": None,
                                "name": "country",
                                "node": None,
                                "type": "string",
                                "semantic_type": None,
                                "semantic_entity": None,
                            },
                            {
                                "column": None,
                                "name": "num_users",
                                "node": None,
                                "type": "bigint",
                                "semantic_type": None,
                                "semantic_entity": None,
                            },
                        ],
                        "query": """SELECT  basic_DOT_transform_DOT_country_agg.country,
    basic_DOT_transform_DOT_country_agg.num_users
 FROM (SELECT  basic_DOT_source_DOT_users.country,
    COUNT( DISTINCT basic_DOT_source_DOT_users.id) AS num_users
 FROM basic.dim_users AS basic_DOT_source_DOT_users
 GROUP BY  1)
 AS basic_DOT_transform_DOT_country_agg""",
                        "spark": {},
                        "upstream_tables": ["public.basic.dim_users"],
                        "lookback_window": None,
                    },
                    "schedule": "0 * * * *",
                    "job": "SparkSqlMaterializationJob",
                },
                {
                    "backfills": [],
                    "config": {
                        "columns": [
                            {
                                "column": None,
                                "name": "country",
                                "node": None,
                                "type": "string",
                                "semantic_type": None,
                                "semantic_entity": None,
                            },
                            {
                                "column": None,
                                "name": "num_users",
                                "node": None,
                                "type": "bigint",
                                "semantic_type": None,
                                "semantic_entity": None,
                            },
                        ],
                        "partitions": [],
                        "query": """SELECT  basic_DOT_transform_DOT_country_agg.country,
    basic_DOT_transform_DOT_country_agg.num_users
 FROM (SELECT  basic_DOT_source_DOT_users.country,
    COUNT( DISTINCT basic_DOT_source_DOT_users.id) AS num_users
 FROM basic.dim_users AS basic_DOT_source_DOT_users
 GROUP BY  1)
 AS basic_DOT_transform_DOT_country_agg""",
                        "spark": {},
                        "upstream_tables": ["public.basic.dim_users"],
                    },
                    "strategy": "full",
                    "job": "SparkSqlMaterializationJob",
                    "name": "default",
                    "schedule": "0 * * * *",
                },
            ],
        )

        # Set both temporal and categorical partitions on node
        response = client_with_query_service.post(
            "/nodes/default.hard_hat/columns/birth_date/partition",
            json={
                "type_": "temporal",
                "granularity": "day",
                "format": "yyyyMMdd",
            },
        )
        # assert response.json() == {}

        client_with_query_service.post(
            "/nodes/default.hard_hat/columns/contractor_id/partition",
            json={
                "type_": "categorical",
            },
        )

        client_with_query_service.post(
            "/nodes/default.hard_hat/columns/country/partition",
            json={
                "type_": "categorical",
            },
        )

        # Setting the materialization config should succeed and it should reschedule
        # the materialization with the temporal partition
        response = client_with_query_service.post(
            "/nodes/default.hard_hat/materialization/",
            json={
                "job": "spark_sql",
                "strategy": "full",
                "config": {},
                "schedule": "0 * * * *",
            },
        )
        data = response.json()
        assert (
            data["message"] == "Successfully updated materialization config named "
            "`spark_sql__full__birth_date` for node `default.hard_hat`"
        )
        expected_query = (
            "SELECT hard_hat_id, last_name, first_name, title, birth_date, hire_date, "
            "address, city, state, postal_code, country, manager, contractor_id FROM "
            "(SELECT default_DOT_hard_hats.hard_hat_id, default_DOT_hard_hats.last_name, "
            "default_DOT_hard_hats.first_name, default_DOT_hard_hats.title, "
            "default_DOT_hard_hats.birth_date, default_DOT_hard_hats.hire_date, "
            "default_DOT_hard_hats.address, default_DOT_hard_hats.city, "
            "default_DOT_hard_hats.state, default_DOT_hard_hats.postal_code, "
            "default_DOT_hard_hats.country, default_DOT_hard_hats.manager, "
            "default_DOT_hard_hats.contractor_id FROM roads.hard_hats AS "
            "default_DOT_hard_hats ) AS default_DOT_hard_hat WHERE birth_date = "
            "CAST(DATE_FORMAT(CAST(${dj_logical_timestamp} AS TIMESTAMP), 'yyyyMMdd') AS "
            "TIMESTAMP)"
        )
        args, _ = query_service_client.materialize.call_args_list[1]  # type: ignore
        assert (
            re.compile(r"\s+").sub(" ", args[0].query).strip()
            == re.compile(r"\s+").sub(" ", expected_query).strip()
        )

        # Check that the temporal partition is appended onto the list of partitions in the
        # materialization config but is not included directly in the materialization query
        response = client_with_query_service.get("/nodes/default.hard_hat/")
        data = response.json()
        assert data["version"] == "v1.0"
        materialization_compare(
            data["materializations"],
            [
                {
                    "backfills": [],
                    "name": "spark_sql__full__birth_date",
                    "strategy": "full",
                    "config": {
                        "columns": [
                            {
                                "name": "hard_hat_id",
                                "type": "int",
                                "column": None,
                                "node": None,
                                "semantic_type": None,
                                "semantic_entity": None,
                            },
                            {
                                "name": "last_name",
                                "type": "string",
                                "column": None,
                                "node": None,
                                "semantic_type": None,
                                "semantic_entity": None,
                            },
                            {
                                "name": "first_name",
                                "type": "string",
                                "column": None,
                                "node": None,
                                "semantic_type": None,
                                "semantic_entity": None,
                            },
                            {
                                "name": "title",
                                "type": "string",
                                "column": None,
                                "node": None,
                                "semantic_type": None,
                                "semantic_entity": None,
                            },
                            {
                                "name": "birth_date",
                                "type": "timestamp",
                                "column": None,
                                "node": None,
                                "semantic_type": None,
                                "semantic_entity": None,
                            },
                            {
                                "name": "hire_date",
                                "type": "timestamp",
                                "column": None,
                                "node": None,
                                "semantic_type": None,
                                "semantic_entity": None,
                            },
                            {
                                "name": "address",
                                "type": "string",
                                "column": None,
                                "node": None,
                                "semantic_type": None,
                                "semantic_entity": None,
                            },
                            {
                                "name": "city",
                                "type": "string",
                                "column": None,
                                "node": None,
                                "semantic_type": None,
                                "semantic_entity": None,
                            },
                            {
                                "name": "state",
                                "type": "string",
                                "column": None,
                                "node": None,
                                "semantic_type": None,
                                "semantic_entity": None,
                            },
                            {
                                "name": "postal_code",
                                "type": "string",
                                "column": None,
                                "node": None,
                                "semantic_type": None,
                                "semantic_entity": None,
                            },
                            {
                                "name": "country",
                                "type": "string",
                                "column": None,
                                "node": None,
                                "semantic_type": None,
                                "semantic_entity": None,
                            },
                            {
                                "name": "manager",
                                "type": "int",
                                "column": None,
                                "node": None,
                                "semantic_type": None,
                                "semantic_entity": None,
                            },
                            {
                                "name": "contractor_id",
                                "type": "int",
                                "column": None,
                                "node": None,
                                "semantic_type": None,
                                "semantic_entity": None,
                            },
                        ],
                        "query": """SELECT  default_DOT_hard_hat.hard_hat_id,
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
 AS default_DOT_hard_hat""",
                        "spark": {},
                        "lookback_window": None,
                        "upstream_tables": ["default.roads.hard_hats"],
                    },
                    "schedule": "0 * * * *",
                    "job": "SparkSqlMaterializationJob",
                },
            ],
        )

        # Check listing materializations of the node
        response = client_with_query_service.get(
            "/nodes/default.hard_hat/materializations/",
        )
        materialization_compare(
            response.json(),
            [
                {
                    "backfills": [],
                    "config": {
                        "columns": [
                            {
                                "name": "hard_hat_id",
                                "type": "int",
                                "column": None,
                                "node": None,
                                "semantic_type": None,
                                "semantic_entity": None,
                            },
                            {
                                "name": "last_name",
                                "type": "string",
                                "column": None,
                                "node": None,
                                "semantic_type": None,
                                "semantic_entity": None,
                            },
                            {
                                "name": "first_name",
                                "type": "string",
                                "column": None,
                                "node": None,
                                "semantic_type": None,
                                "semantic_entity": None,
                            },
                            {
                                "name": "title",
                                "type": "string",
                                "column": None,
                                "node": None,
                                "semantic_type": None,
                                "semantic_entity": None,
                            },
                            {
                                "name": "birth_date",
                                "type": "timestamp",
                                "column": None,
                                "node": None,
                                "semantic_type": None,
                                "semantic_entity": None,
                            },
                            {
                                "name": "hire_date",
                                "type": "timestamp",
                                "column": None,
                                "node": None,
                                "semantic_type": None,
                                "semantic_entity": None,
                            },
                            {
                                "name": "address",
                                "type": "string",
                                "column": None,
                                "node": None,
                                "semantic_type": None,
                                "semantic_entity": None,
                            },
                            {
                                "name": "city",
                                "type": "string",
                                "column": None,
                                "node": None,
                                "semantic_type": None,
                                "semantic_entity": None,
                            },
                            {
                                "name": "state",
                                "type": "string",
                                "column": None,
                                "node": None,
                                "semantic_type": None,
                                "semantic_entity": None,
                            },
                            {
                                "name": "postal_code",
                                "type": "string",
                                "column": None,
                                "node": None,
                                "semantic_type": None,
                                "semantic_entity": None,
                            },
                            {
                                "name": "country",
                                "type": "string",
                                "column": None,
                                "node": None,
                                "semantic_type": None,
                                "semantic_entity": None,
                            },
                            {
                                "name": "manager",
                                "type": "int",
                                "column": None,
                                "node": None,
                                "semantic_type": None,
                                "semantic_entity": None,
                            },
                            {
                                "name": "contractor_id",
                                "type": "int",
                                "column": None,
                                "node": None,
                                "semantic_type": None,
                                "semantic_entity": None,
                            },
                        ],
                        "query": """SELECT  default_DOT_hard_hat.hard_hat_id,
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
 AS default_DOT_hard_hat""",
                        "spark": {},
                        "lookback_window": None,
                        "upstream_tables": ["default.roads.hard_hats"],
                    },
                    "strategy": "full",
                    "job": "SparkSqlMaterializationJob",
                    "name": "spark_sql__full__birth_date",
                    "output_tables": ["common.a", "common.b"],
                    "schedule": "0 * * * *",
                    "urls": ["http://fake.url/job"],
                },
            ],
        )

        # Kick off backfill for this materialization
        response = client_with_query_service.post(
            "/nodes/default.hard_hat/materializations/spark_sql__full__birth_date/backfill",
            json={
                "column_name": "birth_date",
                "range": ["20230101", "20230201"],
            },
        )
        assert query_service_client.run_backfill.call_args_list == [  # type: ignore
            call(
                "default.hard_hat",
                "spark_sql__full__birth_date",
                PartitionBackfill(
                    column_name="birth_date",
                    values=None,
                    range=["20230101", "20230201"],
                ),
            ),
        ]
        assert response.json() == {"output_tables": [], "urls": ["http://fake.url/job"]}

    def test_update_column_display_name(self, client_with_roads: TestClient):
        """
        Test that updating a column display name works.
        """
        response = client_with_roads.patch(
            url="/nodes/default.hard_hat/columns/hard_hat_id",
            params={"display_name": "test"},
        )
        assert response.status_code == 201
        assert response.json() == {
            "attributes": [
                {"attribute_type": {"name": "primary_key", "namespace": "system"}},
            ],
            "dimension": None,
            "display_name": "test",
            "name": "hard_hat_id",
            "type": "int",
            "partition": None,
        }

    def test_backfill_failures(self, client_with_query_service):
        """Run backfill failure modes"""

        # Kick off backfill for non-existent materalization
        response = client_with_query_service.post(
            "/nodes/default.hard_hat/materializations/non_existent/backfill",
            json={
                "column_name": "birth_date",
                "range": ["20230101", "20230201"],
            },
        )
        assert (
            response.json()["message"]
            == "Materialization with name non_existent not found"
        )


class TestNodeColumnsAttributes:
    """
    Test ``POST /nodes/{name}/attributes/``.
    """

    @pytest.fixture
    def create_source_node_payload(self) -> Dict[str, Any]:
        """
        Payload for creating a source node.
        """

        return {
            "name": "comments",
            "description": "A fact table with comments",
            "type": "source",
            "columns": [
                {"name": "id", "type": "int"},
                {
                    "name": "user_id",
                    "type": "int",
                    "dimension": "basic.dimension.users",
                },
                {"name": "event_timestamp", "type": "timestamp"},
                {"name": "post_processing_timestamp", "type": "timestamp"},
                {"name": "text", "type": "string"},
            ],
            "mode": "published",
        }

    @pytest.fixture
    def catalog(self, session: Session) -> Catalog:
        """
        A catalog fixture.
        """

        catalog = Catalog(name="postgres", uuid=uuid4())
        session.add(catalog)
        session.commit()
        return catalog

    @pytest.fixture
    def source_node(self, session: Session) -> Node:
        """
        A source node fixture.
        """
        node = Node(
            name="basic.source.users",
            type=NodeType.SOURCE,
            current_version="1",
        )
        node_revision = NodeRevision(
            node=node,
            name=node.name,
            type=node.type,
            version="1",
            columns=[
                Column(name="id", type=IntegerType()),
                Column(name="created_at", type=TimestampType()),
                Column(name="full_name", type=StringType()),
                Column(name="age", type=IntegerType()),
                Column(name="country", type=StringType()),
                Column(name="gender", type=StringType()),
                Column(name="preferred_language", type=StringType()),
            ],
        )
        session.add(node_revision)
        session.commit()
        return node

    def set_id_primary_key(self, client_with_basic: TestClient):
        """
        Helper function to set id as primary key on basic.dimension.users
        """
        response = client_with_basic.post(
            "/nodes/basic.dimension.users/columns/id/attributes/",
            json=[
                {
                    "namespace": "system",
                    "name": "primary_key",
                },
            ],
        )
        data = response.json()
        assert data == [
            {
                "name": "id",
                "type": "int",
                "display_name": "Id",
                "attributes": [
                    {"attribute_type": {"name": "primary_key", "namespace": "system"}},
                ],
                "dimension": None,
                "partition": None,
            },
        ]

    def test_set_column_attributes(
        self,
        client_with_basic: TestClient,
    ):
        """
        Validate that setting column attributes on the node works.
        """
        # Set id as primary key
        self.set_id_primary_key(client_with_basic)

        # Can set again (idempotent)
        self.set_id_primary_key(client_with_basic)

        # Set column attributes
        response = client_with_basic.post(
            "/nodes/basic.dimension.users/columns/id/attributes/",
            json=[
                {
                    "namespace": "system",
                    "name": "primary_key",
                },
            ],
        )
        data = response.json()
        assert data == [
            {
                "name": "id",
                "type": "int",
                "display_name": "Id",
                "attributes": [
                    {"attribute_type": {"name": "primary_key", "namespace": "system"}},
                ],
                "dimension": None,
                "partition": None,
            },
        ]

        response = client_with_basic.post(
            "/nodes/basic.dimension.users/columns/created_at/attributes/",
            json=[
                {
                    "namespace": "system",
                    "name": "effective_time",
                },
            ],
        )
        data = response.json()
        assert data == [
            {
                "name": "created_at",
                "type": "timestamp",
                "display_name": "Created At",
                "attributes": [
                    {
                        "attribute_type": {
                            "name": "effective_time",
                            "namespace": "system",
                        },
                    },
                ],
                "dimension": None,
                "partition": None,
            },
        ]

        # Remove primary key attribute from column
        response = client_with_basic.post(
            "/nodes/basic.source.comments/columns/id/attributes",
            json=[],
        )
        data = response.json()
        assert data == [
            {
                "name": "id",
                "type": "int",
                "display_name": "Id",
                "attributes": [],
                "dimension": None,
                "partition": None,
            },
        ]

    def test_set_columns_attributes_failed(self, client_with_basic: TestClient):
        """
        Test setting column attributes with different failure modes.
        """
        response = client_with_basic.post(
            "/nodes/basic.source.comments/columns/event_timestamp/attributes/",
            json=[
                {
                    "name": "effective_time",
                    "namespace": "system",
                },
            ],
        )
        data = response.json()
        assert response.status_code == 500
        assert (
            data["message"]
            == "Attribute type `system.effective_time` not allowed on node type `source`!"
        )

        client_with_basic.get(
            "/nodes/basic.source.comments/",
        )

        response = client_with_basic.post(
            "/nodes/basic.source.comments/columns/nonexistent_col/attributes/",
            json=[
                {
                    "name": "primary_key",
                },
            ],
        )
        assert response.status_code == 404
        data = response.json()
        assert data == {
            "message": "Column `nonexistent_col` does not exist on node `basic.source.comments`!",
            "errors": [],
            "warnings": [],
        }

        response = client_with_basic.post(
            "/nodes/basic.source.comments/columns/id/attributes/",
            json=[
                {
                    "name": "nonexistent_attribute",
                },
            ],
        )
        assert response.status_code == 404
        data = response.json()
        assert data == {
            "message": "Attribute type `system.nonexistent_attribute` does not exist!",
            "errors": [],
            "warnings": [],
        }

        response = client_with_basic.post(
            "/nodes/basic.source.comments/columns/user_id/attributes/",
            json=[
                {
                    "name": "primary_key",
                },
            ],
        )
        assert response.status_code == 201
        data = response.json()
        assert [col for col in data if col["attributes"]] == [
            {
                "name": "user_id",
                "type": "int",
                "display_name": "User Id",
                "attributes": [
                    {"attribute_type": {"name": "primary_key", "namespace": "system"}},
                ],
                "dimension": {"name": "basic.dimension.users"},
                "partition": None,
            },
        ]

        client_with_basic.post(
            "/nodes/basic.source.comments/columns/event_timestamp/attributes/",
            json=[
                {
                    "name": "event_time",
                },
            ],
        )

        response = client_with_basic.post(
            "/nodes/basic.source.comments/columns/post_processing_timestamp/attributes/",
            json=[
                {
                    "name": "event_time",
                },
            ],
        )
        data = response.json()
        assert data == {
            "message": "The column attribute `event_time` is scoped to be unique to the "
            "`['node', 'column_type']` level, but there is more than one column"
            " tagged with it: `event_timestamp, post_processing_timestamp`",
            "errors": [],
            "warnings": [],
        }

        client_with_basic.post(
            "/nodes/basic.source.comments/columns/event_timestamp/attributes/",
            json=[],
        )

        response = client_with_basic.get("/nodes/basic.source.comments/")
        data = response.json()
        assert data["columns"] == [
            {
                "name": "id",
                "type": "int",
                "display_name": "Id",
                "attributes": [],
                "dimension": None,
                "partition": None,
            },
            {
                "name": "user_id",
                "type": "int",
                "display_name": "User Id",
                "attributes": [
                    {"attribute_type": {"namespace": "system", "name": "primary_key"}},
                ],
                "dimension": {"name": "basic.dimension.users"},
                "partition": None,
            },
            {
                "name": "timestamp",
                "type": "timestamp",
                "display_name": "Timestamp",
                "attributes": [],
                "dimension": None,
                "partition": None,
            },
            {
                "name": "text",
                "type": "string",
                "display_name": "Text",
                "attributes": [],
                "dimension": None,
                "partition": None,
            },
            {
                "name": "event_timestamp",
                "type": "timestamp",
                "display_name": "Event Timestamp",
                "attributes": [],
                "dimension": None,
                "partition": None,
            },
            {
                "name": "created_at",
                "type": "timestamp",
                "display_name": "Created At",
                "attributes": [],
                "dimension": None,
                "partition": None,
            },
            {
                "name": "post_processing_timestamp",
                "type": "timestamp",
                "display_name": "Post Processing Timestamp",
                "attributes": [],
                "dimension": None,
                "partition": None,
            },
        ]


class TestValidateNodes:  # pylint: disable=too-many-public-methods
    """
    Test ``POST /nodes/validate/``.
    """

    def test_validating_a_valid_node(
        self,
        client_with_account_revenue: TestClient,
    ) -> None:
        """
        Test validating a valid node
        """
        response = client_with_account_revenue.post(
            "/nodes/validate/",
            json={
                "name": "foo",
                "description": "This is my foo transform node!",
                "query": "SELECT payment_id FROM default.large_revenue_payments_only",
                "type": "transform",
            },
        )
        data = response.json()

        assert response.status_code == 200
        assert len(data) == 6
        assert data["columns"] == [
            {
                "attributes": [],
                "dimension": None,
                "display_name": "Payment Id",
                "name": "payment_id",
                "partition": None,
                "type": "int",
            },
        ]
        assert data["status"] == "valid"
        assert data["dependencies"][0]["name"] == "default.large_revenue_payments_only"
        assert data["message"] == "Node `foo` is valid."
        assert data["missing_parents"] == []
        assert data["errors"] == []

    def test_validating_an_invalid_node(self, client: TestClient) -> None:
        """
        Test validating an invalid node
        """

        response = client.post(
            "/nodes/validate/",
            json={
                "name": "foo",
                "description": "This is my foo transform node!",
                "query": "SELECT bar FROM large_revenue_payments_only",
                "type": "transform",
            },
        )
        data = response.json()
        assert data["message"] == "Node `foo` is invalid."
        assert [
            e
            for e in data["errors"]
            if e
            == {
                "code": 301,
                "message": "Node definition contains references to nodes that do not exist: "
                "large_revenue_payments_only",
                "debug": {"missing_parents": ["large_revenue_payments_only"]},
                "context": "",
            }
        ]

    def test_validating_invalid_sql(self, client: TestClient) -> None:
        """
        Test validating an invalid node with invalid SQL
        """

        response = client.post(
            "/nodes/validate/",
            json={
                "name": "foo",
                "description": "This is my foo transform node!",
                "query": "SUPER invalid SQL query",
                "type": "transform",
            },
        )
        data = response.json()

        assert response.status_code == 422
        assert data["message"] == "Node `foo` is invalid."
        assert data["status"] == "invalid"
        assert data["errors"] == [
            {
                "code": 201,
                "message": (
                    "('Parse error 1:0:', \"mismatched input 'SUPER' expecting "
                    "{'(', 'ADD', 'ALTER', 'ANALYZE', 'CACHE', 'CLEAR', 'COMMENT', "
                    "'COMMIT', 'CREATE', 'DELETE', 'DESC', 'DESCRIBE', 'DFS', 'DROP', "
                    "'EXPLAIN', 'EXPORT', 'FROM', 'GRANT', 'IMPORT', 'INSERT', "
                    "'LIST', 'LOAD', 'LOCK', 'MAP', 'MERGE', 'MSCK', 'REDUCE', "
                    "'REFRESH', 'REPAIR', 'REPLACE', 'RESET', 'REVOKE', 'ROLLBACK', "
                    "'SELECT', 'SET', 'SHOW', 'START', 'TABLE', 'TRUNCATE', 'UNCACHE', "
                    "'UNLOCK', 'UPDATE', 'USE', 'VALUES', 'WITH'}\")"
                ),
                "debug": None,
                "context": "",
            },
        ]

    def test_validating_with_missing_parents(self, client: TestClient) -> None:
        """
        Test validating a node with a query that has missing parents
        """

        response = client.post(
            "/nodes/validate/",
            json={
                "name": "foo",
                "description": "This is my foo transform node!",
                "query": "SELECT 1 FROM node_that_does_not_exist",
                "type": "transform",
            },
        )
        data = response.json()

        assert response.status_code == 422
        assert data == {
            "message": "Node `foo` is invalid.",
            "status": "invalid",
            "dependencies": [],
            "missing_parents": ["node_that_does_not_exist"],
            "columns": [
                {
                    "attributes": [],
                    "dimension": None,
                    "display_name": "Col0",
                    "name": "col0",
                    "partition": None,
                    "type": "int",
                },
            ],
            "errors": [
                {
                    "code": 301,
                    "message": "Node definition contains references to nodes that do not exist: "
                    "node_that_does_not_exist",
                    "debug": {"missing_parents": ["node_that_does_not_exist"]},
                    "context": "",
                },
            ],
        }

    def test_allowing_missing_parents_for_draft_nodes(self, client: TestClient) -> None:
        """
        Test validating a draft node that's allowed to have missing parents
        """

        response = client.post(
            "/nodes/validate/",
            json={
                "name": "foo",
                "description": "This is my foo transform node!",
                "query": "SELECT 1 FROM node_that_does_not_exist",
                "type": "transform",
                "mode": "draft",
            },
        )
        data = response.json()

        assert response.status_code == 422
        assert data["message"] == "Node `foo` is invalid."
        assert data["status"] == "invalid"
        assert data["columns"] == [
            {
                "attributes": [],
                "dimension": None,
                "display_name": "Col0",
                "name": "col0",
                "partition": None,
                "type": "int",
            },
        ]
        assert data["missing_parents"] == ["node_that_does_not_exist"]
        assert data["errors"] == [
            {
                "code": 301,
                "context": "",
                "debug": {"missing_parents": ["node_that_does_not_exist"]},
                "message": "Node definition contains references to nodes that do not exist: "
                "node_that_does_not_exist",
            },
        ]

    def test_raise_when_trying_to_validate_a_source_node(
        self,
        client: TestClient,
    ) -> None:
        """
        Test validating a source node which is not possible
        """

        response = client.post(
            "/nodes/validate/",
            json={
                "name": "foo",
                "description": "This is my foo source node!",
                "type": "source",
                "columns": [
                    {"name": "payment_id", "type": "int"},
                    {"name": "payment_amount", "type": "float"},
                    {"name": "customer_id", "type": "int"},
                    {"name": "account_type", "type": "int"},
                ],
                "tables": [
                    {
                        "database_id": 1,
                        "catalog": "test",
                        "schema": "accounting",
                        "table": "revenue",
                    },
                ],
            },
        )
        data = response.json()

        assert response.status_code == 500
        assert data == {
            "message": "Source nodes cannot be validated",
            "errors": [],
            "warnings": [],
        }

    def test_adding_dimensions_to_node_columns(
        self,
        client_example_loader,
    ):
        """
        Test linking dimensions to node columns
        """
        custom_client = client_example_loader(["ACCOUNT_REVENUE", "BASIC"])
        # Attach the payment_type dimension to the payment_type column on the revenue node
        response = custom_client.post(
            "/nodes/default.revenue/columns/payment_type/?dimension=default.payment_type",
        )
        data = response.json()
        assert data == {
            "message": (
                "Dimension node default.payment_type has been successfully "
                "linked to column payment_type on node default.revenue"
            ),
        }
        response = custom_client.get("/nodes/default.revenue")
        data = response.json()
        assert [
            col["dimension"]["name"] for col in data["columns"] if col["dimension"]
        ] == ["default.payment_type"]

        # Check that after deleting the dimension link, none of the columns have links
        response = custom_client.delete(
            "/nodes/default.revenue/columns/payment_type/?dimension=default.payment_type",
        )
        data = response.json()
        assert data == {
            "message": (
                "The dimension link on the node default.revenue's payment_type to "
                "default.payment_type has been successfully removed."
            ),
        }
        response = custom_client.get("/nodes/default.revenue")
        data = response.json()
        assert all(col["dimension"] is None for col in data["columns"])
        response = custom_client.get("/history?node=default.revenue")
        assert [
            (activity["activity_type"], activity["entity_type"])
            for activity in response.json()
        ] == [("create", "node"), ("create", "link"), ("delete", "link")]

        # Removing the dimension link again will result in no change
        response = custom_client.delete(
            "/nodes/default.revenue/columns/payment_type/?dimension=default.payment_type",
        )
        data = response.json()
        assert response.status_code == 304
        assert data == {
            "message": "No change was made to payment_type on node default.revenue as the"
            " specified dimension link to default.payment_type on None was not found.",
        }
        # Check history again, no change
        response = custom_client.get("/history?node=default.revenue")
        assert [
            (activity["activity_type"], activity["entity_type"])
            for activity in response.json()
        ] == [("create", "node"), ("create", "link"), ("delete", "link")]

        # Check that the proper error is raised when the column doesn't exist
        response = custom_client.post(
            "/nodes/default.revenue/columns/non_existent_column/?dimension=default.payment_type",
        )
        assert response.status_code == 404
        data = response.json()
        assert data["message"] == (
            "Column non_existent_column does not exist on node default.revenue"
        )

        # Add a dimension including a specific dimension column name
        response = custom_client.post(
            "/nodes/default.revenue/columns/payment_type/"
            "?dimension=default.payment_type"
            "&dimension_column=payment_type_name",
        )
        assert response.status_code == 422
        data = response.json()
        assert data["message"] == (
            "The column payment_type has type int and is being linked "
            "to the dimension default.payment_type via the dimension column "
            "payment_type_name, which has type string. These column "
            "types are incompatible and the dimension cannot be linked"
        )

        response = custom_client.post(
            "/nodes/default.revenue/columns/payment_type/?dimension=basic.dimension.users",
        )
        data = response.json()
        assert data["message"] == (
            "Cannot add dimension to column, because catalogs do not match: default, public"
        )

    def test_update_node_with_dimension_links(self, client_with_roads: TestClient):
        """
        When a node is updated with a new query, the original dimension links and attributes
        on its columns should be preserved where possible (that is, where the new and old
        columns have the same names).
        """
        client_with_roads.patch(
            "/nodes/default.hard_hat/",
            json={
                "query": """
                SELECT
                    hard_hat_id,
                    title,
                    state
                FROM default.hard_hats
                """,
            },
        )
        response = client_with_roads.get("/history?node=default.hard_hat")
        history = response.json()
        assert [
            (activity["activity_type"], activity["entity_type"]) for activity in history
        ] == [
            ("create", "node"),
            ("set_attribute", "column_attribute"),
            ("create", "link"),
            ("update", "node"),
        ]

        response = client_with_roads.get("/nodes/default.hard_hat").json()
        assert response["columns"] == [
            {
                "attributes": [
                    {"attribute_type": {"name": "primary_key", "namespace": "system"}},
                ],
                "dimension": None,
                "display_name": "Hard Hat Id",
                "name": "hard_hat_id",
                "type": "int",
                "partition": None,
            },
            {
                "attributes": [],
                "dimension": None,
                "display_name": "Title",
                "name": "title",
                "type": "string",
                "partition": None,
            },
            {
                "attributes": [],
                "dimension": {"name": "default.us_state"},
                "display_name": "State",
                "name": "state",
                "type": "string",
                "partition": None,
            },
        ]

        # Check history of the node with column attribute set
        response = client_with_roads.get(
            "/history?node=default.hard_hat",
        )
        history = response.json()
        assert [
            (activity["activity_type"], activity["entity_type"]) for activity in history
        ] == [
            ("create", "node"),
            ("set_attribute", "column_attribute"),
            ("create", "link"),
            ("update", "node"),
        ]

    def test_update_dimension_remove_pk_column(self, client_with_roads: TestClient):
        """
        When a dimension node is updated with a new query that removes the original primary key
        column, either a new primary key must be set or the node will be set to invalid.
        """
        response = client_with_roads.patch(
            "/nodes/default.hard_hat/",
            json={
                "query": """
                SELECT
                    title,
                    state
                FROM default.hard_hats
                """,
                # "primary_key": ["title"],
            },
        )
        assert response.json()["status"] == "invalid"
        response = client_with_roads.patch(
            "/nodes/default.hard_hat/",
            json={
                "query": """
                SELECT
                    title,
                    state
                FROM default.hard_hats
                """,
                "primary_key": ["title"],
            },
        )
        assert response.json()["status"] == "valid"

    def test_node_downstreams(self, client_with_event: TestClient):
        """
        Test getting downstream nodes of different node types.
        """
        response = client_with_event.get(
            "/nodes/default.event_source/downstream/?node_type=metric",
        )
        data = response.json()
        assert {node["name"] for node in data} == {
            "default.long_events_distinct_countries",
            "default.device_ids_count",
        }

        response = client_with_event.get(
            "/nodes/default.event_source/downstream/?node_type=transform",
        )
        data = response.json()
        assert {node["name"] for node in data} == {"default.long_events"}

        response = client_with_event.get(
            "/nodes/default.event_source/downstream/?node_type=dimension",
        )
        data = response.json()
        assert {node["name"] for node in data} == {"default.country_dim"}

        response = client_with_event.get("/nodes/default.event_source/downstream/")
        data = response.json()
        assert {node["name"] for node in data} == {
            "default.long_events_distinct_countries",
            "default.device_ids_count",
            "default.long_events",
            "default.country_dim",
        }

        response = client_with_event.get(
            "/nodes/default.device_ids_count/downstream/",
        )
        data = response.json()
        assert data == []

        response = client_with_event.get("/nodes/default.long_events/downstream/")
        data = response.json()
        assert {node["name"] for node in data} == {
            "default.long_events_distinct_countries",
        }

    def test_node_upstreams(self, client_with_event: TestClient):
        """
        Test getting upstream nodes of different node types.
        """
        response = client_with_event.get(
            "/nodes/default.long_events_distinct_countries/upstream/",
        )
        data = response.json()
        assert {node["name"] for node in data} == {
            "default.event_source",
            "default.long_events",
        }

    def test_list_node_dag(self, client_example_loader):
        """
        Test getting the DAG for a node
        """
        custom_client = client_example_loader(["EVENT", "ROADS"])
        response = custom_client.get(
            "/nodes/default.long_events_distinct_countries/dag",
        )
        data = response.json()
        assert {node["name"] for node in data} == {
            "default.country_dim",
            "default.event_source",
            "default.long_events",
            "default.long_events_distinct_countries",
        }

        response = custom_client.get("/nodes/default.num_repair_orders/dag")
        data = response.json()
        assert {node["name"] for node in data} == {
            "default.dispatcher",
            "default.hard_hat",
            "default.municipality_dim",
            "default.num_repair_orders",
            "default.repair_order_details",
            "default.repair_orders",
            "default.repair_orders_fact",
            "default.us_state",
        }

    def test_node_column_lineage(self, client_with_roads: TestClient):
        """
        Test endpoint to retrieve a node's column-level lineage
        """
        response = client_with_roads.get(
            "/nodes/default.num_repair_orders/lineage/",
        )
        assert response.json() == [
            {
                "column_name": "default_DOT_num_repair_orders",
                "display_name": "Default: Num Repair Orders",
                "lineage": [
                    {
                        "column_name": "repair_order_id",
                        "display_name": "Repair Orders Fact",
                        "lineage": [
                            {
                                "column_name": "repair_order_id",
                                "display_name": "default.roads.repair_orders",
                                "lineage": [],
                                "node_name": "default.repair_orders",
                                "node_type": "source",
                            },
                        ],
                        "node_name": "default.repair_orders_fact",
                        "node_type": "transform",
                    },
                ],
                "node_name": "default.num_repair_orders",
                "node_type": "metric",
            },
        ]

        client_with_roads.post(
            "/nodes/metric/",
            json={
                "name": "default.discounted_repair_orders",
                "query": (
                    """
                    SELECT
                      cast(sum(if(discount > 0.0, 1, 0)) as double) / count(repair_order_id)
                    FROM default.repair_order_details
                    """
                ),
                "mode": "published",
                "description": "Discounted Repair Orders",
            },
        )
        response = client_with_roads.get(
            "/nodes/default.discounted_repair_orders/lineage/",
        )
        assert response.json() == [
            {
                "column_name": "default_DOT_discounted_repair_orders",
                "node_name": "default.discounted_repair_orders",
                "node_type": "metric",
                "display_name": "Default: Discounted Repair Orders",
                "lineage": [
                    {
                        "column_name": "repair_order_id",
                        "node_name": "default.repair_order_details",
                        "node_type": "source",
                        "display_name": "default.roads.repair_order_details",
                        "lineage": [],
                    },
                    {
                        "column_name": "discount",
                        "node_name": "default.repair_order_details",
                        "node_type": "source",
                        "display_name": "default.roads.repair_order_details",
                        "lineage": [],
                    },
                ],
            },
        ]

    def test_revalidating_existing_nodes(self, client_with_roads: TestClient):
        """
        Test revalidating all example nodes and confirm that they are set to valid
        """
        for node in client_with_roads.get("/nodes/").json():
            status = client_with_roads.post(
                f"/nodes/{node}/validate/",
            ).json()["status"]
            assert status == "valid"
        # Confirm that they still show as valid server-side
        for node in client_with_roads.get("/nodes/").json():
            node = client_with_roads.get(f"/nodes/{node}").json()
            assert node["status"] == "valid"

    def test_lineage_on_complex_transforms(self, client_with_roads: TestClient):
        """
        Test metric lineage on more complex transforms and metrics
        """
        response = client_with_roads.get("/nodes/default.regional_level_agg/").json()
        assert response["columns"] == [
            {
                "attributes": [
                    {"attribute_type": {"name": "primary_key", "namespace": "system"}},
                ],
                "dimension": None,
                "display_name": "Us Region Id",
                "name": "us_region_id",
                "type": "int",
                "partition": None,
            },
            {
                "attributes": [
                    {"attribute_type": {"name": "primary_key", "namespace": "system"}},
                ],
                "dimension": None,
                "display_name": "State Name",
                "name": "state_name",
                "type": "string",
                "partition": None,
            },
            {
                "attributes": [],
                "dimension": None,
                "display_name": "Location Hierarchy",
                "name": "location_hierarchy",
                "type": "string",
                "partition": None,
            },
            {
                "attributes": [
                    {"attribute_type": {"name": "primary_key", "namespace": "system"}},
                ],
                "dimension": None,
                "display_name": "Order Year",
                "name": "order_year",
                "type": "int",
                "partition": None,
            },
            {
                "attributes": [
                    {"attribute_type": {"name": "primary_key", "namespace": "system"}},
                ],
                "dimension": None,
                "display_name": "Order Month",
                "name": "order_month",
                "type": "int",
                "partition": None,
            },
            {
                "attributes": [
                    {"attribute_type": {"name": "primary_key", "namespace": "system"}},
                ],
                "dimension": None,
                "display_name": "Order Day",
                "name": "order_day",
                "type": "int",
                "partition": None,
            },
            {
                "attributes": [],
                "dimension": None,
                "display_name": "Completed Repairs",
                "name": "completed_repairs",
                "type": "bigint",
                "partition": None,
            },
            {
                "attributes": [],
                "dimension": None,
                "display_name": "Total Repairs Dispatched",
                "name": "total_repairs_dispatched",
                "type": "bigint",
                "partition": None,
            },
            {
                "attributes": [],
                "dimension": None,
                "display_name": "Total Amount In Region",
                "name": "total_amount_in_region",
                "type": "double",
                "partition": None,
            },
            {
                "attributes": [],
                "dimension": None,
                "display_name": "Avg Repair Amount In Region",
                "name": "avg_repair_amount_in_region",
                "type": "double",
                "partition": None,
            },
            {
                "attributes": [],
                "dimension": None,
                "display_name": "Avg Dispatch Delay",
                "name": "avg_dispatch_delay",
                "type": "double",
                "partition": None,
            },
            {
                "attributes": [],
                "dimension": None,
                "display_name": "Unique Contractors",
                "name": "unique_contractors",
                "type": "bigint",
                "partition": None,
            },
        ]

        response = client_with_roads.get(
            "/nodes/default.regional_repair_efficiency/",
        ).json()
        assert response["columns"] == [
            {
                "attributes": [],
                "dimension": None,
                "display_name": "Default: Regional Repair Efficiency",
                "name": "default_DOT_regional_repair_efficiency",
                "type": "double",
                "partition": None,
            },
        ]
        response = client_with_roads.get(
            "/nodes/default.regional_repair_efficiency/lineage/",
        ).json()
        assert response == [
            {
                "column_name": "default_DOT_regional_repair_efficiency",
                "node_name": "default.regional_repair_efficiency",
                "node_type": "metric",
                "display_name": "Default: Regional Repair Efficiency",
                "lineage": [
                    {
                        "column_name": "total_amount_nationwide",
                        "node_name": "default.national_level_agg",
                        "node_type": "transform",
                        "display_name": "Default: National Level Agg",
                        "lineage": [
                            {
                                "column_name": "quantity",
                                "node_name": "default.repair_order_details",
                                "node_type": "source",
                                "display_name": "default.roads.repair_order_details",
                                "lineage": [],
                            },
                            {
                                "column_name": "price",
                                "node_name": "default.repair_order_details",
                                "node_type": "source",
                                "display_name": "default.roads.repair_order_details",
                                "lineage": [],
                            },
                        ],
                    },
                    {
                        "column_name": "total_amount_in_region",
                        "node_name": "default.regional_level_agg",
                        "node_type": "transform",
                        "display_name": "Default: Regional Level Agg",
                        "lineage": [
                            {
                                "column_name": "quantity",
                                "node_name": "default.repair_order_details",
                                "node_type": "source",
                                "display_name": "default.roads.repair_order_details",
                                "lineage": [],
                            },
                            {
                                "column_name": "price",
                                "node_name": "default.repair_order_details",
                                "node_type": "source",
                                "display_name": "default.roads.repair_order_details",
                                "lineage": [],
                            },
                        ],
                    },
                    {
                        "column_name": "total_repairs_dispatched",
                        "node_name": "default.regional_level_agg",
                        "node_type": "transform",
                        "display_name": "Default: Regional Level Agg",
                        "lineage": [
                            {
                                "column_name": "repair_order_id",
                                "node_name": "default.repair_orders",
                                "node_type": "source",
                                "display_name": "default.roads.repair_orders",
                                "lineage": [],
                            },
                        ],
                    },
                    {
                        "column_name": "completed_repairs",
                        "node_name": "default.regional_level_agg",
                        "node_type": "transform",
                        "display_name": "Default: Regional Level Agg",
                        "lineage": [
                            {
                                "column_name": "repair_order_id",
                                "node_name": "default.repair_orders",
                                "node_type": "source",
                                "display_name": "default.roads.repair_orders",
                                "lineage": [],
                            },
                            {
                                "column_name": "dispatched_date",
                                "node_name": "default.repair_orders",
                                "node_type": "source",
                                "display_name": "default.roads.repair_orders",
                                "lineage": [],
                            },
                        ],
                    },
                ],
            },
        ]


def test_node_similarity(session: Session, client: TestClient):
    """
    Test determining node similarity based on their queries
    """
    source_data = Node(
        name="source_data",
        type=NodeType.SOURCE,
        current_version="1",
    )
    source_data_rev = NodeRevision(
        node=source_data,
        version="1",
        name=source_data.name,
        type=source_data.type,
    )
    a_transform = Node(
        name="a_transform",
        type=NodeType.TRANSFORM,
        current_version="1",
    )
    a_transform_rev = NodeRevision(
        name=a_transform.name,
        node=a_transform,
        version="1",
        query="SELECT 1 as num",
        type=a_transform.type,
        columns=[
            Column(name="num", type=IntegerType()),
        ],
    )
    another_transform = Node(
        name="another_transform",
        type=NodeType.TRANSFORM,
        current_version="1",
    )
    another_transform_rev = NodeRevision(
        name=another_transform.name,
        node=another_transform,
        version="1",
        query="SELECT 1 as num",
        type=another_transform.type,
        columns=[
            Column(name="num", type=IntegerType()),
        ],
    )
    yet_another_transform = Node(
        name="yet_another_transform",
        type=NodeType.TRANSFORM,
        current_version="1",
    )
    yet_another_transform_rev = NodeRevision(
        name=yet_another_transform.name,
        node=yet_another_transform,
        version="1",
        query="SELECT 2 as num",
        type=yet_another_transform.type,
        columns=[
            Column(name="num", type=IntegerType()),
        ],
    )
    session.add(source_data_rev)
    session.add(a_transform_rev)
    session.add(another_transform_rev)
    session.add(yet_another_transform_rev)
    session.commit()

    response = client.get("/nodes/similarity/a_transform/another_transform")
    assert response.status_code == 200
    data = response.json()
    assert data["similarity"] == 1.0

    response = client.get("/nodes/similarity/a_transform/yet_another_transform")
    assert response.status_code == 200
    data = response.json()
    assert data["similarity"] == 0.75

    response = client.get("/nodes/similarity/yet_another_transform/another_transform")
    assert response.status_code == 200
    data = response.json()
    assert data["similarity"] == 0.75

    # Check that the proper error is raised when using a source node
    response = client.get("/nodes/similarity/a_transform/source_data")
    assert response.status_code == 409
    data = response.json()
    assert data == {
        "message": "Cannot determine similarity of source nodes",
        "errors": [],
        "warnings": [],
    }


def test_resolving_downstream_status(client_with_service_setup: TestClient) -> None:
    """
    Test creating and updating a source node
    """
    # Create draft transform and metric nodes with missing parents
    transform1 = {
        "name": "default.comments_by_migrated_users",
        "description": "Comments by users who have already migrated",
        "query": "SELECT id, user_id FROM default.comments WHERE text LIKE '%migrated%'",
        "mode": "draft",
    }

    transform2 = {
        "name": "default.comments_by_users_pending_a_migration",
        "description": "Comments by users who have a migration pending",
        "query": "SELECT id, user_id FROM default.comments WHERE text LIKE '%migration pending%'",
        "mode": "draft",
    }

    transform3 = {
        "name": "default.comments_by_users_partially_migrated",
        "description": "Comments by users are partially migrated",
        "query": (
            "SELECT p.id, p.user_id FROM default.comments_by_users_pending_a_migration p "
            "INNER JOIN default.comments_by_migrated_users m ON p.user_id = m.user_id"
        ),
        "mode": "draft",
    }

    transform4 = {
        "name": "default.comments_by_banned_users",
        "description": "Comments by users are partially migrated",
        "query": (
            "SELECT id, user_id FROM default.comments AS comment "
            "INNER JOIN default.banned_users AS banned_users "
            "ON comments.user_id = banned_users.banned_user_id"
        ),
        "mode": "draft",
    }

    transform5 = {
        "name": "default.comments_by_users_partially_migrated_sample",
        "description": "Sample of comments by users are partially migrated",
        "query": "SELECT id, user_id, foo FROM default.comments_by_users_partially_migrated",
        "mode": "draft",
    }

    metric1 = {
        "name": "default.number_of_migrated_users",
        "description": "Number of migrated users",
        "query": "SELECT COUNT(DISTINCT user_id) FROM default.comments_by_migrated_users",
        "mode": "draft",
    }

    metric2 = {
        "name": "default.number_of_users_with_pending_migration",
        "description": "Number of users with a migration pending",
        "query": (
            "SELECT COUNT(DISTINCT user_id) FROM "
            "default.comments_by_users_pending_a_migration"
        ),
        "mode": "draft",
    }

    metric3 = {
        "name": "default.number_of_users_partially_migrated",
        "description": "Number of users partially migrated",
        "query": "SELECT COUNT(DISTINCT user_id) FROM default.comments_by_users_partially_migrated",
        "mode": "draft",
    }

    for node, node_type in [
        (transform1, NodeType.TRANSFORM),
        (transform2, NodeType.TRANSFORM),
        (transform3, NodeType.TRANSFORM),
        (transform4, NodeType.TRANSFORM),
        (transform5, NodeType.TRANSFORM),
        (metric1, NodeType.METRIC),
        (metric2, NodeType.METRIC),
        (metric3, NodeType.METRIC),
    ]:
        response = client_with_service_setup.post(
            f"/nodes/{node_type.value}/",  # pylint: disable=no-member
            json=node,
        )
        assert response.status_code == 201
        data = response.json()
        assert data["name"] == node["name"]
        assert data["mode"] == node["mode"]
        assert data["status"] == "invalid"

    # Add the missing parent
    missing_parent_node = {
        "name": "default.comments",
        "description": "A fact table with comments",
        "columns": [
            {"name": "id", "type": "int"},
            {"name": "user_id", "type": "int"},
            {"name": "timestamp", "type": "timestamp"},
            {"name": "text", "type": "string"},
        ],
        "mode": "published",
        "catalog": "public",
        "schema_": "basic",
        "table": "comments",
    }

    response = client_with_service_setup.post(
        "/nodes/source/",
        json=missing_parent_node,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == missing_parent_node["name"]
    assert data["mode"] == missing_parent_node["mode"]
    assert data["status"] == "valid"

    # Check that downstream nodes have now been switched to a "valid" status
    for node in [transform1, transform2, transform3, metric1, metric2, metric3]:
        response = client_with_service_setup.get(f"/nodes/{node['name']}/")
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == node["name"]
        assert data["mode"] == node["mode"]  # make sure the mode hasn't been changed
        assert (
            data["status"] == "valid"
        )  # make sure the node's status has been updated to valid

    # Check that nodes still not valid have an invalid status
    for node in [transform4, transform5]:
        response = client_with_service_setup.get(f"/nodes/{node['name']}/")
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == node["name"]
        assert data["mode"] == node["mode"]  # make sure the mode hasn't been changed
        assert data["status"] == "invalid"


def test_decompose_expression():
    """
    Verify metric expression decomposition into measures for cubes
    """
    res = decompose_expression(ast.Number(value=5.5))
    assert res == (ast.Number(value=5.5), [])

    # Decompose `avg(orders)`
    res = decompose_expression(
        ast.Function(ast.Name("avg"), args=[ast.Column(ast.Name("orders"))]),
    )
    assert str(res[0]) == "sum(orders3845127662_sum) / count(orders3845127662_count)"
    assert [measure.alias_or_name.name for measure in res[1]] == [
        "orders3845127662_sum",
        "orders3845127662_count",
    ]

    # Decompose `avg(orders) + 5.5`
    res = decompose_expression(
        ast.BinaryOp(
            left=ast.Function(ast.Name("avg"), args=[ast.Column(ast.Name("orders"))]),
            right=ast.Number(value=5.5),
            op=ast.BinaryOpKind.Plus,
        ),
    )
    assert (
        str(res[0]) == "sum(orders3845127662_sum) / count(orders3845127662_count) + 5.5"
    )
    assert [measure.alias_or_name.name for measure in res[1]] == [
        "orders3845127662_sum",
        "orders3845127662_count",
    ]

    # Decompose `max(avg(orders_a) + avg(orders_b))`
    res = decompose_expression(
        ast.Function(
            ast.Name("max"),
            args=[
                ast.BinaryOp(
                    op=ast.BinaryOpKind.Plus,
                    left=ast.Function(
                        ast.Name("avg"),
                        args=[ast.Column(ast.Name("orders_a"))],
                    ),
                    right=ast.Function(
                        ast.Name("avg"),
                        args=[ast.Column(ast.Name("orders_b"))],
                    ),
                ),
            ],
        ),
    )
    assert (
        str(res[0])
        == "max(sum(orders_a1170126662_sum) / count(orders_a1170126662_count) "
        "+ sum(orders_b3703039740_sum) / count(orders_b3703039740_count))"
    )

    # Decompose `sum(max(orders))`
    res = decompose_expression(
        ast.Function(
            ast.Name("sum"),
            args=[
                ast.Function(
                    ast.Name("max"),
                    args=[ast.Column(ast.Name("orders"))],
                ),
            ],
        ),
    )
    assert str(res[0]) == "sum(max(orders3845127662_max))"
    assert [measure.alias_or_name.name for measure in res[1]] == [
        "orders3845127662_max",
    ]

    # Decompose `(max(orders) + min(validations))/sum(total)`
    res = decompose_expression(
        ast.BinaryOp(
            left=ast.BinaryOp(
                left=ast.Function(
                    ast.Name("max"),
                    args=[ast.Column(ast.Name("orders"))],
                ),
                right=ast.Function(
                    ast.Name("min"),
                    args=[ast.Column(ast.Name("validations"))],
                ),
                op=ast.BinaryOpKind.Plus,
            ),
            right=ast.Function(ast.Name("sum"), args=[ast.Column(ast.Name("total"))]),
            op=ast.BinaryOpKind.Divide,
        ),
    )
    assert (
        str(res[0])
        == "max(orders3845127662_max) + min(validations2970758927_min) / sum(total3257917790_sum)"
    )
    assert [measure.alias_or_name.name for measure in res[1]] == [
        "orders3845127662_max",
        "validations2970758927_min",
        "total3257917790_sum",
    ]

    # Decompose `cast(sum(coalesce(has_ordered, 0.0)) as double)/count(total)`
    res = decompose_expression(
        ast.BinaryOp(
            left=ast.Cast(
                expression=ast.Function(
                    name=ast.Name("sum"),
                    args=[
                        ast.Function(
                            ast.Name("coalesce"),
                            args=[ast.Column(ast.Name("has_ordered")), ast.Number(0.0)],
                        ),
                    ],
                ),
                data_type=types.DoubleType(),
            ),
            right=ast.Function(
                name=ast.Name("sum"),
                args=[ast.Column(ast.Name("total"))],
            ),
            op=ast.BinaryOpKind.Divide,
        ),
    )
    assert str(res[0]) == "sum(has_ordered2766370626_sum) / sum(total3257917790_sum)"
    assert [measure.alias_or_name.name for measure in res[1]] == [
        "has_ordered2766370626_sum",
        "total3257917790_sum",
    ]


def test_list_dimension_attributes(client_with_roads: TestClient) -> None:
    """
    Test that listing dimension attributes for any node works.
    """
    response = client_with_roads.get("/nodes/default.regional_level_agg/dimensions/")
    assert response.ok
    assert response.json() == [
        {
            "is_primary_key": True,
            "name": "default.regional_level_agg.order_day",
            "node_display_name": "Default: Regional Level Agg",
            "node_name": "default.regional_level_agg",
            "path": [],
            "type": "int",
        },
        {
            "is_primary_key": True,
            "name": "default.regional_level_agg.order_month",
            "node_display_name": "Default: Regional Level Agg",
            "node_name": "default.regional_level_agg",
            "path": [],
            "type": "int",
        },
        {
            "is_primary_key": True,
            "name": "default.regional_level_agg.order_year",
            "node_display_name": "Default: Regional Level Agg",
            "node_name": "default.regional_level_agg",
            "path": [],
            "type": "int",
        },
        {
            "is_primary_key": True,
            "name": "default.regional_level_agg.state_name",
            "node_display_name": "Default: Regional Level Agg",
            "node_name": "default.regional_level_agg",
            "path": [],
            "type": "string",
        },
        {
            "is_primary_key": True,
            "name": "default.regional_level_agg.us_region_id",
            "node_display_name": "Default: Regional Level Agg",
            "node_name": "default.regional_level_agg",
            "path": [],
            "type": "int",
        },
    ]


def test_set_column_partition(client_with_roads: TestClient):
    """
    Test setting temporal and categorical partitions on node
    """
    # Set hire_date to temporal
    response = client_with_roads.post(
        "/nodes/default.hard_hat/columns/hire_date/partition",
        json={
            "type_": "temporal",
            "granularity": "hour",
            "format": "yyyyMMddHH",
        },
    )
    assert response.json() == {
        "attributes": [],
        "dimension": None,
        "display_name": "Hire Date",
        "name": "hire_date",
        "partition": {
            "expression": None,
            "format": "yyyyMMddHH",
            "type_": "temporal",
            "granularity": "hour",
        },
        "type": "timestamp",
    }

    # Set state to categorical
    response = client_with_roads.post(
        "/nodes/default.hard_hat/columns/state/partition",
        json={
            "type_": "categorical",
        },
    )
    assert response.json() == {
        "attributes": [],
        "dimension": {"name": "default.us_state"},
        "display_name": "State",
        "name": "state",
        "partition": {
            "expression": None,
            "type_": "categorical",
            "format": None,
            "granularity": None,
        },
        "type": "string",
    }

    # Attempt to set country to temporal (missing granularity)
    response = client_with_roads.post(
        "/nodes/default.hard_hat/columns/country/partition",
        json={
            "type_": "temporal",
        },
    )
    assert (
        response.json()["message"]
        == "The granularity must be provided for temporal partitions. One of: "
        "['SECOND', 'MINUTE', 'HOUR', 'DAY', 'WEEK', 'MONTH', 'QUARTER', "
        "'YEAR']"
    )

    # Attempt to set country to temporal (missing format)
    response = client_with_roads.post(
        "/nodes/default.hard_hat/columns/country/partition",
        json={
            "type_": "temporal",
            "granularity": "day",
        },
    )
    assert (
        response.json()["message"]
        == "The temporal partition column's datetime format must be provided."
    )

    # Set country to temporal
    client_with_roads.post(
        "/nodes/default.hard_hat/columns/country/partition",
        json={
            "type_": "temporal",
            "granularity": "day",
            "format": "yyyyMMdd",
        },
    )

    # Update country to categorical
    response = client_with_roads.post(
        "/nodes/default.hard_hat/columns/country/partition",
        json={
            "type_": "categorical",
            "expression": "",
        },
    )
    assert response.json() == {
        "attributes": [],
        "dimension": None,
        "display_name": "Country",
        "name": "country",
        "partition": {
            "expression": None,
            "type_": "categorical",
            "format": None,
            "granularity": None,
        },
        "type": "string",
    }


def test_delete_recreate_for_all_nodes(client_with_roads: TestClient):
    """
    Test deleting and recreating for all node types
    """
    # Delete a source node
    client_with_roads.delete("/nodes/default.dispatchers")
    # Recreating it should succeed
    response = client_with_roads.post(
        "/nodes/source",
        json={
            "columns": [
                {"name": "dispatcher_id", "type": "int"},
                {"name": "company_name", "type": "string"},
                {"name": "phone", "type": "string"},
            ],
            "description": "Information on dispatchers",
            "mode": "published",
            "name": "default.dispatchers",
            "catalog": "default",
            "schema_": "roads",
            "table": "dispatchers",
        },
    )
    assert response.json()["version"] == "v2.0"
    response = client_with_roads.get("/history?node=default.dispatchers")
    assert [activity["activity_type"] for activity in response.json()] == [
        "create",
        "delete",
        "update",
        "restore",
    ]
    client_with_roads.patch(
        "/nodes/default.dispatcher",
        json={"primary_key": ["dispatcher_id"]},
    )

    # Delete a dimension node
    client_with_roads.delete("/nodes/default.us_state")
    # Trying to create a transform node with the same name will fail
    response = client_with_roads.post(
        "/nodes/transform",
        json={
            "description": "US state transform",
            "query": """SELECT
  state_id,
  state_name,
  state_abbr AS state_short
FROM default.us_states s
LEFT JOIN default.us_region r
ON s.state_region = r.us_region_id""",
            "mode": "published",
            "name": "default.us_state",
            "primary_key": ["state_id"],
        },
    )
    assert response.json()["message"] == (
        "A node with name `default.us_state` of a `dimension` type existed "
        "before. If you want to re-create it with a different type, you "
        "need to remove all traces of the previous node with a hard delete call: "
        "DELETE /nodes/{node_name}/hard"
    )
    # Trying to create a dimension node with the same name but an updated query will succeed
    response = client_with_roads.post(
        "/nodes/dimension",
        json={
            "description": "US state",
            "query": """SELECT
  state_id,
  state_name,
  state_abbr AS state_short
FROM default.us_states s
LEFT JOIN default.us_region r
ON s.state_region = r.us_region_id""",
            "mode": "published",
            "name": "default.us_state",
            "primary_key": ["state_id"],
        },
    )
    node_data = response.json()
    assert node_data["version"] == "v2.0"
    response = client_with_roads.get("/history?node=default.us_state")
    assert [activity["activity_type"] for activity in response.json()] == [
        "create",
        "set_attribute",
        "delete",
        "update",
        "restore",
    ]

    create_cube_payload = {
        "metrics": [
            "default.num_repair_orders",
            "default.avg_repair_price",
            "default.total_repair_cost",
        ],
        "dimensions": [
            "default.hard_hat.country",
            "default.dispatcher.company_name",
            "default.municipality_dim.local_region",
        ],
        "filters": ["default.hard_hat.state='AZ'"],
        "description": "Cube of various metrics related to repairs",
        "mode": "published",
        "name": "default.repairs_cube",
    }
    client_with_roads.post(
        "/nodes/cube/",
        json=create_cube_payload,
    )
    client_with_roads.delete("/nodes/default.repairs_cube")
    client_with_roads.post(
        "/nodes/cube/",
        json=create_cube_payload,
    )
    response = client_with_roads.get("/history?node=default.repairs_cube")
    assert [activity["activity_type"] for activity in response.json()] == [
        "create",
        "delete",
        "restore",
    ]
