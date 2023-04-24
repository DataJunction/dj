# pylint: disable=too-many-lines
"""
Tests for the nodes API.
"""
from typing import Any, Dict

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from dj.models import Database, Table
from dj.models.column import Column
from dj.models.node import Node, NodeRevision, NodeStatus, NodeType
from dj.sql.parsing.types import IntegerType, StringType, TimestampType


def test_read_node(client_with_examples: TestClient) -> None:
    """
    Test ``GET /nodes/{node_id}``.
    """
    response = client_with_examples.get("/nodes/repair_orders/")
    data = response.json()

    assert response.status_code == 200
    assert data["version"] == "v1.0"
    assert data["node_id"] == 1
    assert data["node_revision_id"] == 1
    assert data["type"] == "source"

    response = client_with_examples.get("/nodes/nothing/")
    data = response.json()

    assert response.status_code == 404
    assert data["message"] == "A node with name `nothing` does not exist."


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
            Column(name="answer", type=IntegerType()),
        ],
    )
    node3 = Node(name="a-metric", type=NodeType.METRIC, current_version="1")
    node_rev3 = NodeRevision(
        name=node3.name,
        node=node3,
        version="1",
        query="SELECT COUNT(*) FROM my_table",
        columns=[
            Column(name="_col0", type=IntegerType()),
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

    nodes = {node["name"]: node for node in data}
    assert nodes["not-a-metric"]["query"] is None
    assert nodes["not-a-metric"]["version"] == "1"
    assert nodes["not-a-metric"]["display_name"] == "Not-A-Metric"
    assert not nodes["not-a-metric"]["columns"]
    assert nodes["not-a-metric"]["parents"] == []

    assert nodes["also-not-a-metric"]["query"] == "SELECT 42 AS answer"
    assert nodes["also-not-a-metric"]["display_name"] == "Also-Not-A-Metric"
    assert nodes["also-not-a-metric"]["columns"] == [
        {
            "name": "answer",
            "type": "int",
            "attributes": [],
            "dimension": None,
        },
    ]

    assert nodes["a-metric"]["query"] == "SELECT COUNT(*) FROM my_table"
    assert nodes["a-metric"]["display_name"] == "A-Metric"
    assert nodes["a-metric"]["columns"] == [
        {
            "name": "_col0",
            "type": "int",
            "attributes": [],
            "dimension": None,
        },
    ]
    assert nodes["a-metric"]["parents"] == []


class TestCreateOrUpdateNodes:
    """
    Test ``POST /nodes/`` and ``PUT /nodes/{name}``.
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
            "name": "countries",
            "primary_key": ["country"],
        }

    @pytest.fixture
    def create_invalid_transform_node_payload(self) -> Dict[str, Any]:
        """
        Payload for creating a transform node.
        """

        return {
            "name": "country_agg",
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
            "name": "country_agg",
            "query": "SELECT country, COUNT(DISTINCT id) AS num_users FROM basic.source.users",
            "mode": "published",
            "description": "Distinct users per country",
            "columns": [
                {"name": "country", "type": "string"},
                {"name": "num_users", "type": "int"},
            ],
        }

    @pytest.fixture
    def database(self, session: Session) -> Database:
        """
        A database fixture.
        """

        database = Database(name="postgres", URI="postgres://")
        session.add(database)
        session.commit()
        return database

    @pytest.fixture
    def source_node(self, session: Session, database: Database) -> Node:
        """
        A source node fixture.
        """

        table = Table(
            database=database,
            table="A",
            columns=[
                Column(name="ds", type=StringType()),
                Column(name="user_id", type=IntegerType()),
            ],
        )
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
            tables=[table],
            columns=[
                Column(name="id", type=IntegerType()),
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

    def test_delete_node(
        self,
        client_with_examples: TestClient,
    ):
        """
        Test deleting a node
        """
        response = client_with_examples.delete("/nodes/basic.source.users/")
        assert response.status_code == 204

        # All downstream nodes should be invalid
        expected_downstreams = [
            "basic.dimension.users",
            "basic.transform.country_agg",
            "basic.dimension.countries",
            "basic.num_users",
        ]
        for downstream in expected_downstreams:
            response = client_with_examples.get(f"/nodes/{downstream}/")
            assert response.json()["status"] == NodeStatus.INVALID

        node_with_link = client_with_examples.get("/nodes/repair_order_details/").json()
        assert [
            col["dimension"]["name"]
            for col in node_with_link["columns"]
            if col["name"] == "repair_order_id"
        ] == ["repair_order"]

        response = client_with_examples.delete("/nodes/repair_order/")
        assert response.status_code == 204

        node_with_link = client_with_examples.get("/nodes/repair_order_details/").json()
        assert [
            col["dimension"]
            for col in node_with_link["columns"]
            if col["name"] == "repair_order_id"
        ] == [None]

    def test_create_source_node_without_cols_or_query_service(
        self,
        client_with_examples: TestClient,
    ):
        """
        Trying to create a source node without columns and without
        a query service set up should fail.
        """
        basic_source_comments = {
            "name": "comments",
            "description": "A fact table with comments",
            "columns": [],
            "mode": "published",
            "catalog": "public",
            "schema_": "basic",
            "table": "comments",
        }

        # Trying to create a source node without columns and without
        # a query service set up should fail
        response = client_with_examples.post(
            "/nodes/source/",
            json=basic_source_comments,
        )
        data = response.json()
        assert (
            data["message"] == "No table columns were provided and no query "
            "service is configured for table columns inference!"
        )
        assert response.status_code == 500

    def test_create_source_node_with_query_service(
        self,
        client_with_query_service: TestClient,
    ):
        """
        Creating a source node without columns but with a query service set should
        result in the source node columns being inferred via the query service.
        """
        basic_source_comments = {
            "name": "comments",
            "description": "A fact table with comments",
            "columns": [],
            "mode": "published",
            "catalog": "public",
            "schema_": "basic",
            "table": "comments",
        }

        # Trying to create a source node without columns and without
        # a query service set up should fail
        response = client_with_query_service.post(
            "/nodes/source/",
            json=basic_source_comments,
        )
        data = response.json()
        assert data["name"] == "comments"
        assert data["type"] == "source"
        assert data["display_name"] == "Comments"
        assert data["version"] == "v1.0"
        assert data["status"] == "valid"
        assert data["mode"] == "published"
        assert data["catalog"]["name"] == "public"
        assert data["schema_"] == "basic"
        assert data["table"] == "comments"
        assert data["columns"] == [
            {"name": "id", "type": "int", "attributes": [], "dimension": None},
            {"name": "user_id", "type": "int", "attributes": [], "dimension": None},
            {
                "name": "timestamp",
                "type": "timestamp",
                "attributes": [],
                "dimension": None,
            },
            {"name": "text", "type": "string", "attributes": [], "dimension": None},
        ]
        assert response.status_code == 201

    def test_create_update_source_node(
        self,
        client_with_examples: TestClient,
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
        response = client_with_examples.post(
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
        response = client_with_examples.patch(
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
        response = client_with_examples.patch(
            f"/nodes/{basic_source_comments['name']}/",
            json={"description": "New description", "display_name": "Comments facts"},
        )
        new_data = response.json()
        assert data == new_data

        # Try to update a node with a table that has different columns
        response = client_with_examples.patch(
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
            {"name": "id", "type": "int", "attributes": [], "dimension": None},
            {"name": "user_id", "type": "int", "attributes": [], "dimension": None},
            {
                "name": "timestamp",
                "type": "timestamp",
                "attributes": [],
                "dimension": None,
            },
            {"name": "text_v2", "type": "string", "attributes": [], "dimension": None},
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
        database: Database,  # pylint: disable=unused-argument
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
        assert response.status_code == 500
        assert (
            data["message"]
            == "Node definition contains references to nodes that do not exist"
        )

    def test_create_node_with_type_inference_failure(
        self,
        client_with_examples: TestClient,
    ):
        """
        Attempting to create a published metric where type inference fails should raise
        an appropriate error and fail.
        """
        response = client_with_examples.post(
            "/nodes/metric/",
            json={
                "description": "Average length of employment",
                "query": (
                    "SELECT avg(NOW() - hire_date + 1) as avg_length_of_employment "
                    "FROM foo.bar.hard_hats"
                ),
                "mode": "published",
                "name": "avg_length_of_employment_plus_one",
            },
        )
        data = response.json()
        assert data == {
            "message": (
                "Unable to infer type for some columns on node "
                "`avg_length_of_employment_plus_one`"
            ),
            "errors": [
                {
                    "code": 302,
                    "message": (
                        "Unable to infer type for some columns on node "
                        "`avg_length_of_employment_plus_one`"
                    ),
                    "debug": {"columns": ["avg_length_of_employment"]},
                    "context": "",
                },
            ],
            "warnings": [],
        }

    def test_create_update_transform_node(
        self,
        database: Database,  # pylint: disable=unused-argument
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
        assert data["name"] == "country_agg"
        assert data["display_name"] == "Country Agg"
        assert data["type"] == "transform"
        assert data["description"] == "Distinct users per country"
        assert (
            data["query"]
            == "SELECT country, COUNT(DISTINCT id) AS num_users FROM basic.source.users"
        )
        assert data["columns"] == [
            {"name": "country", "type": "string", "attributes": [], "dimension": None},
            {
                "name": "num_users",
                "type": "bigint",
                "attributes": [],
                "dimension": None,
            },
        ]
        assert data["parents"] == [{"name": "basic.source.users"}]

        # Update the transform node with two minor changes
        response = client.patch(
            "/nodes/country_agg/",
            json={
                "description": "Some new description",
                "display_name": "Country Aggregation by User",
            },
        )
        data = response.json()
        assert data["name"] == "country_agg"
        assert data["display_name"] == "Country Aggregation by User"
        assert data["type"] == "transform"
        assert data["version"] == "v1.1"
        assert data["description"] == "Some new description"
        assert (
            data["query"]
            == "SELECT country, COUNT(DISTINCT id) AS num_users FROM basic.source.users"
        )

        # Try to update with a new query that references a non-existent source
        response = client.patch(
            "/nodes/country_agg/",
            json={
                "query": "SELECT country, COUNT(DISTINCT id) AS num_users FROM comments",
            },
        )
        data = response.json()
        assert (
            data["message"]
            == "Node definition contains references to nodes that do not exist"
        )

        # Try to update with a new query that references an existing source
        response = client.patch(
            "/nodes/country_agg/",
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
            {"name": "country", "type": "string", "attributes": [], "dimension": None},
            {
                "name": "num_users",
                "type": "bigint",
                "attributes": [],
                "dimension": None,
            },
            {
                "name": "num_entries",
                "type": "bigint",
                "attributes": [],
                "dimension": None,
            },
        ]

        # Verify that asking for revisions for a non-existent transform fails
        response = client.get("/nodes/random_transform/revisions/")
        data = response.json()
        assert data["message"] == "A node with name `random_transform` does not exist."

        # Verify that all historical revisions are available for the node
        response = client.get("/nodes/country_agg/revisions/")
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
                    "name": "country",
                    "type": "string",
                    "attributes": [],
                    "dimension": None,
                },
                {
                    "name": "num_users",
                    "type": "bigint",
                    "attributes": [],
                    "dimension": None,
                },
            ],
            "v1.1": [
                {
                    "name": "country",
                    "type": "string",
                    "attributes": [],
                    "dimension": None,
                },
                {
                    "name": "num_users",
                    "type": "bigint",
                    "attributes": [],
                    "dimension": None,
                },
            ],
            "v2.0": [
                {
                    "name": "country",
                    "type": "string",
                    "attributes": [],
                    "dimension": None,
                },
                {
                    "name": "num_users",
                    "type": "bigint",
                    "attributes": [],
                    "dimension": None,
                },
                {
                    "name": "num_entries",
                    "type": "bigint",
                    "attributes": [],
                    "dimension": None,
                },
            ],
        }

    def test_create_dimension_node_fails(
        self,
        database: Database,  # pylint: disable=unused-argument
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
                "name": "countries",
                "primary_key": ["country", "id"],
            },
        )
        assert response.json()["message"] == (
            "Some columns in the primary key country,id were not "
            "found in the list of available columns for the node "
            "countries."
        )

    def test_create_update_dimension_node(
        self,
        database: Database,  # pylint: disable=unused-argument
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
        assert data["name"] == "countries"
        assert data["display_name"] == "Countries"
        assert data["type"] == "dimension"
        assert data["version"] == "v1.0"
        assert data["description"] == "Country dimension"
        assert (
            data["query"] == "SELECT country, COUNT(1) AS user_cnt "
            "FROM basic.source.users GROUP BY country"
        )
        assert data["columns"] == [
            {
                "name": "country",
                "type": "string",
                "attributes": [
                    {"attribute_type": {"namespace": "system", "name": "primary_key"}},
                ],
                "dimension": None,
            },
            {"name": "user_cnt", "type": "bigint", "attributes": [], "dimension": None},
        ]

        # Test updating the dimension node with a new query
        response = client.patch(
            "/nodes/countries/",
            json={"query": "SELECT country FROM basic.source.users GROUP BY country"},
        )
        data = response.json()
        # Should result in a major version update due to the query change
        assert data["version"] == "v2.0"

        # The columns should have been updated
        assert data["columns"] == [
            {"name": "country", "type": "string", "attributes": [], "dimension": None},
        ]

    def test_raise_on_multi_catalog_node(self, client_with_examples: TestClient):
        """
        Test raising when trying to select from multiple catalogs
        """
        response = client_with_examples.post(
            "/nodes/transform/",
            json={
                "query": (
                    "SELECT payment_id, payment_amount, customer_id, account_type "
                    "FROM revenue r LEFT JOIN basic.source.comments b on r.id = b.id"
                ),
                "description": "Multicatalog",
                "mode": "published",
                "name": "multicatalog",
            },
        )
        assert (
            "Cannot create nodes with multi-catalog dependencies"
            in response.json()["message"]
        )

    def test_updating_node_to_invalid_draft(
        self,
        database: Database,  # pylint: disable=unused-argument
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
        assert data["name"] == "countries"
        assert data["display_name"] == "Countries"
        assert data["type"] == "dimension"
        assert data["version"] == "v1.0"
        assert data["description"] == "Country dimension"
        assert (
            data["query"] == "SELECT country, COUNT(1) AS user_cnt "
            "FROM basic.source.users GROUP BY country"
        )
        assert data["columns"] == [
            {
                "name": "country",
                "type": "string",
                "attributes": [
                    {"attribute_type": {"namespace": "system", "name": "primary_key"}},
                ],
                "dimension": None,
            },
            {"name": "user_cnt", "type": "bigint", "attributes": [], "dimension": None},
        ]

        response = client.patch(
            "/nodes/countries/",
            json={"mode": "draft"},
        )
        assert response.status_code == 200

        # Test updating the dimension node with an invalid query
        response = client.patch(
            "/nodes/countries/",
            json={"query": "SELECT country FROM missing_parent GROUP BY country"},
        )
        assert response.status_code == 200

        # Check that node is now a draft with an invalid status
        response = client.get("/nodes/countries")
        assert response.status_code == 200
        data = response.json()
        assert data["mode"] == "draft"
        assert data["status"] == "invalid"

    def test_upsert_materialization_config(  # pylint: disable=too-many-arguments
        self,
        client_with_examples: TestClient,
    ) -> None:
        """
        Test creating & updating materialization config for a node.
        """
        # Setting the materialization config for a source node should fail
        response = client_with_examples.post(
            "/nodes/basic.source.comments/materialization/",
            json={
                "engine_name": "spark",
                "engine_version": "2.4.4",
                "schedule": "0 * * * *",
                "config": {},
            },
        )
        assert response.status_code == 400
        assert (
            response.json()["message"]
            == "Cannot set materialization config for source node `basic.source.comments`!"
        )

        # Setting the materialization config for an engine that doesn't exist should fail
        response = client_with_examples.post(
            "/nodes/basic.transform.country_agg/materialization/",
            json={
                "engine_name": "spark",
                "engine_version": "2.4.4",
                "config": {},
                "schedule": "0 * * * *",
            },
        )
        assert response.status_code == 404
        data = response.json()
        assert data["detail"] == "Engine not found: `spark` version `2.4.4`"

        # Create the engine and check the existing transform node
        client_with_examples.post(
            "/engines/",
            json={
                "name": "spark",
                "version": "2.4.4",
                "dialect": "spark",
            },
        )

        response = client_with_examples.get("/nodes/basic.transform.country_agg/")
        old_node_data = response.json()
        assert old_node_data["version"] == "v1.0"
        assert old_node_data["materialization_configs"] == []

        # Setting the materialization config should succeed
        response = client_with_examples.post(
            "/nodes/basic.transform.country_agg/materialization/",
            json={
                "engine_name": "spark",
                "engine_version": "2.4.4",
                "config": {},
                "schedule": "0 * * * *",
            },
        )
        data = response.json()
        assert (
            data["message"]
            == "Successfully updated materialization config for node `basic.transform.country_agg`"
            " and engine `spark`."
        )

        # Reading the node should yield the materialization config
        response = client_with_examples.get("/nodes/basic.transform.country_agg/")
        data = response.json()
        assert data["version"] == "v1.0"
        assert data["materialization_configs"] == [
            {
                "config": {},
                "schedule": "0 * * * *",
                "engine": {
                    "name": "spark",
                    "uri": None,
                    "version": "2.4.4",
                    "dialect": "spark",
                },
            },
        ]

        # Setting the same config should yield a message indicating so.
        response = client_with_examples.post(
            "/nodes/basic.transform.country_agg/materialization/",
            json={
                "engine_name": "spark",
                "engine_version": "2.4.4",
                "config": {},
                "dialect": "spark",
                "schedule": "0 * * * *",
            },
        )
        assert response.status_code == 204

        data = response.json()
        assert (
            data["message"]
            == "The same materialization config provided already exists for node "
            "`basic.transform.country_agg` so no update was performed."
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
    def database(self, session: Session) -> Database:
        """
        A database fixture.
        """

        database = Database(name="postgres", URI="postgres://")
        session.add(database)
        session.commit()
        return database

    @pytest.fixture
    def source_node(self, session: Session, database: Database) -> Node:
        """
        A source node fixture.
        """

        table = Table(
            database=database,
            table="A",
            columns=[
                Column(name="ds", type=StringType()),
                Column(name="user_id", type=IntegerType()),
            ],
        )
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
            tables=[table],
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

    def test_set_columns_attributes(
        self,
        client_with_examples: TestClient,
    ):
        """
        Validate that setting column attributes on the node works.
        """
        response = client_with_examples.post(
            "/nodes/basic.source.comments/attributes/",
            json=[
                {
                    "attribute_type_namespace": "system",
                    "attribute_type_name": "primary_key",
                    "column_name": "id",
                },
            ],
        )
        data = response.json()
        assert data == [
            {
                "name": "id",
                "type": "int",
                "attributes": [
                    {"attribute_type": {"name": "primary_key", "namespace": "system"}},
                ],
                "dimension": None,
            },
        ]

        # Set columns attributes
        response = client_with_examples.post(
            "/nodes/basic.dimension.users/attributes/",
            json=[
                {
                    "attribute_type_namespace": "system",
                    "attribute_type_name": "primary_key",
                    "column_name": "id",
                },
                {
                    "attribute_type_name": "effective_time",
                    "column_name": "created_at",
                },
            ],
        )
        data = response.json()
        assert data == [
            {
                "name": "id",
                "type": "int",
                "attributes": [
                    {"attribute_type": {"name": "primary_key", "namespace": "system"}},
                ],
                "dimension": None,
            },
            {
                "name": "created_at",
                "type": "timestamp",
                "attributes": [
                    {
                        "attribute_type": {
                            "name": "effective_time",
                            "namespace": "system",
                        },
                    },
                ],
                "dimension": None,
            },
        ]

    def test_set_columns_attributes_failed(self, client_with_examples: TestClient):
        """
        Test setting column attributes with different failure modes.
        """
        response = client_with_examples.post(
            "/nodes/basic.source.comments/attributes/",
            json=[
                {
                    "attribute_type_name": "effective_time",
                    "column_name": "event_timestamp",
                },
            ],
        )
        data = response.json()
        assert response.status_code == 500
        assert (
            data["message"]
            == "Attribute type `system.effective_time` not allowed on node type `source`!"
        )

        response = client_with_examples.get(
            "/nodes/basic.source.comments/",
        )

        response = client_with_examples.post(
            "/nodes/basic.source.comments/attributes/",
            json=[
                {
                    "attribute_type_name": "primary_key",
                    "column_name": "nonexistent_col",
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

        response = client_with_examples.post(
            "/nodes/basic.source.comments/attributes/",
            json=[
                {
                    "attribute_type_name": "nonexistent_attribute",
                    "column_name": "id",
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

        response = client_with_examples.post(
            "/nodes/basic.source.comments/attributes/",
            json=[
                {
                    "attribute_type_name": "primary_key",
                    "column_name": "user_id",
                },
            ],
        )
        assert response.status_code == 201
        data = response.json()
        assert data == [
            {
                "name": "user_id",
                "type": "int",
                "attributes": [
                    {"attribute_type": {"name": "primary_key", "namespace": "system"}},
                ],
                "dimension": {"name": "basic.dimension.users"},
            },
        ]

        response = client_with_examples.post(
            "/nodes/basic.source.comments/attributes/",
            json=[
                {
                    "attribute_type_name": "event_time",
                    "column_name": "event_timestamp",
                },
                {
                    "attribute_type_name": "event_time",
                    "column_name": "post_processing_timestamp",
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

        response = client_with_examples.get("/nodes/basic.source.comments/")
        data = response.json()
        assert data["columns"] == [
            {
                "name": "id",
                "type": "int",
                "attributes": [],
                "dimension": None,
            },
            {
                "name": "user_id",
                "type": "int",
                "attributes": [
                    {"attribute_type": {"namespace": "system", "name": "primary_key"}},
                ],
                "dimension": {"name": "basic.dimension.users"},
            },
            {
                "name": "timestamp",
                "type": "timestamp",
                "attributes": [],
                "dimension": None,
            },
            {"name": "text", "type": "string", "attributes": [], "dimension": None},
            {
                "name": "event_timestamp",
                "type": "timestamp",
                "attributes": [],
                "dimension": None,
            },
            {
                "name": "created_at",
                "type": "timestamp",
                "attributes": [],
                "dimension": None,
            },
            {
                "name": "post_processing_timestamp",
                "type": "timestamp",
                "attributes": [],
                "dimension": None,
            },
        ]


class TestValidateNodes:  # pylint: disable=too-many-public-methods
    """
    Test ``POST /nodes/validate/``.
    """

    def test_validating_a_valid_node(self, client_with_examples: TestClient) -> None:
        """
        Test validating a valid node
        """
        response = client_with_examples.post(
            "/nodes/validate/",
            json={
                "name": "foo",
                "description": "This is my foo transform node!",
                "query": "SELECT payment_id FROM large_revenue_payments_only",
                "type": "transform",
            },
        )
        data = response.json()

        assert response.status_code == 200
        assert len(data) == 5
        assert data["columns"] == [
            {
                "dimension_column": None,
                "dimension_id": None,
                "id": None,
                "name": "payment_id",
                "type": "int",
            },
        ]
        assert data["status"] == "valid"
        assert data["node_revision"]["status"] == "valid"
        assert data["dependencies"][0]["name"] == "large_revenue_payments_only"
        assert data["message"] == "Node `foo` is valid"
        assert data["node_revision"]["id"] is None
        assert data["node_revision"]["mode"] == "published"
        assert data["node_revision"]["name"] == "foo"
        assert (
            data["node_revision"]["query"]
            == "SELECT payment_id FROM large_revenue_payments_only"
        )

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
        assert (
            data["message"]
            == "Node definition contains references to nodes that do not exist"
        )

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

        assert response.status_code == 500
        assert data == {
            "errors": [],
            "message": "('Parse error 1:0:', \"mismatched input 'SUPER' expecting "
            "{'(', 'ADD', 'ALTER', 'ANALYZE', 'CACHE', 'CLEAR', 'COMMENT', "
            "'COMMIT', 'CREATE', 'DELETE', 'DESC', 'DESCRIBE', 'DFS', 'DROP', "
            "'EXPLAIN', 'EXPORT', 'FROM', 'GRANT', 'IMPORT', 'INSERT', 'LIST', "
            "'LOAD', 'LOCK', 'MAP', 'MERGE', 'MSCK', 'REDUCE', 'REFRESH', "
            "'REPAIR', 'REPLACE', 'RESET', 'REVOKE', 'ROLLBACK', 'SELECT', "
            "'SET', 'SHOW', 'START', 'TABLE', 'TRUNCATE', 'UNCACHE', 'UNLOCK', "
            "'UPDATE', 'USE', 'VALUES', 'WITH'}\")",
            "warnings": [],
        }

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

        assert response.status_code == 500
        assert data == {
            "message": "Node definition contains references to nodes that do not exist",
            "errors": [
                {
                    "code": 301,
                    "message": "Node definition contains references to nodes that do not exist",
                    "debug": {"missing_parents": ["node_that_does_not_exist"]},
                    "context": "",
                },
            ],
            "warnings": [],
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

        assert response.status_code == 200
        assert data["message"] == "Node `foo` is invalid"
        assert data["status"] == "invalid"
        assert data["node_revision"]["mode"] == "draft"
        assert data["node_revision"]["status"] == "invalid"
        assert data["columns"] == [
            {
                "id": None,
                "name": "col0",
                "type": "int",
                "dimension_id": None,
                "dimension_column": None,
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

    def test_adding_dimensions_to_node_columns(self, client_with_examples: TestClient):
        """
        Test adding tables to existing nodes
        """
        # Attach the payment_type dimension to the payment_type column on the revenue node
        response = client_with_examples.post(
            "/nodes/revenue/columns/payment_type/?dimension=payment_type",
        )
        data = response.json()
        assert data == {
            "message": (
                "Dimension node payment_type has been successfully "
                "linked to column payment_type on node revenue"
            ),
        }

        # Check that the proper error is raised when the column doesn't exist
        response = client_with_examples.post(
            "/nodes/revenue/columns/non_existent_column/?dimension=payment_type",
        )
        assert response.status_code == 404
        data = response.json()
        assert data["message"] == (
            "Column non_existent_column does not exist on node revenue"
        )

        # Add a dimension including a specific dimension column name
        response = client_with_examples.post(
            "/nodes/revenue/columns/payment_type/"
            "?dimension=payment_type"
            "&dimension_column=payment_type_name",
        )
        assert response.status_code == 422
        data = response.json()
        assert data["message"] == (
            "The column payment_type has type int and is being linked "
            "to the dimension payment_type via the dimension column "
            "payment_type_name, which has type string. These column "
            "types are incompatible and the dimension cannot be linked!"
        )

        response = client_with_examples.post(
            "/nodes/revenue/columns/payment_type/?dimension=basic.dimension.users",
        )
        data = response.json()
        assert data["message"] == (
            "Cannot add dimension to column, because catalogs do not match: default, public"
        )

        # Check that not including the dimension defaults it to the column name
        response = client_with_examples.post("/nodes/revenue/columns/payment_type/")
        assert response.status_code == 201
        data = response.json()
        assert data["message"] == (
            "Dimension node payment_type has been successfully "
            "linked to column payment_type on node revenue"
        )

    def test_node_downstreams(self, client_with_examples: TestClient):
        """
        Test getting downstream nodes of different node types.
        """
        response = client_with_examples.get(
            "/nodes/event_source/downstream/?node_type=metric",
        )
        data = response.json()
        assert {node["name"] for node in data} == {
            "long_events_distinct_countries",
            "device_ids_count",
        }

        response = client_with_examples.get(
            "/nodes/event_source/downstream/?node_type=transform",
        )
        data = response.json()
        assert {node["name"] for node in data} == {"long_events"}

        response = client_with_examples.get(
            "/nodes/event_source/downstream/?node_type=dimension",
        )
        data = response.json()
        assert {node["name"] for node in data} == {"country_dim"}

        response = client_with_examples.get("/nodes/event_source/downstream/")
        data = response.json()
        assert {node["name"] for node in data} == {
            "long_events_distinct_countries",
            "device_ids_count",
            "long_events",
            "country_dim",
        }

        response = client_with_examples.get("/nodes/device_ids_count/downstream/")
        data = response.json()
        assert data == []

        response = client_with_examples.get("/nodes/long_events/downstream/")
        data = response.json()
        assert {node["name"] for node in data} == {"long_events_distinct_countries"}


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
    assert data["similarity"] == 0.7142857142857143

    response = client.get("/nodes/similarity/yet_another_transform/another_transform")
    assert response.status_code == 200
    data = response.json()
    assert data["similarity"] == 0.7142857142857143

    # Check that the proper error is raised when using a source node
    response = client.get("/nodes/similarity/a_transform/source_data")
    assert response.status_code == 409
    data = response.json()
    assert data == {
        "message": "Cannot determine similarity of source nodes",
        "errors": [],
        "warnings": [],
    }


def test_resolving_downstream_status(client_with_examples: TestClient) -> None:
    """
    Test creating and updating a source node
    """
    # Create draft transform and metric nodes with missing parents
    transform1 = {
        "name": "comments_by_migrated_users",
        "description": "Comments by users who have already migrated",
        "query": "SELECT id, user_id FROM comments WHERE text LIKE '%migrated%'",
        "mode": "draft",
    }

    transform2 = {
        "name": "comments_by_users_pending_a_migration",
        "description": "Comments by users who have a migration pending",
        "query": "SELECT id, user_id FROM comments WHERE text LIKE '%migration pending%'",
        "mode": "draft",
    }

    transform3 = {
        "name": "comments_by_users_partially_migrated",
        "description": "Comments by users are partially migrated",
        "query": (
            "SELECT p.id, p.user_id FROM comments_by_users_pending_a_migration p "
            "INNER JOIN comments_by_migrated_users m ON p.user_id = m.user_id"
        ),
        "mode": "draft",
    }

    transform4 = {
        "name": "comments_by_banned_users",
        "description": "Comments by users are partially migrated",
        "query": (
            "SELECT id, user_id FROM comments "
            "INNER JOIN banned_users ON comments.user_id = banned_users.banned_user_id"
        ),
        "mode": "draft",
    }

    transform5 = {
        "name": "comments_by_users_partially_migrated_sample",
        "description": "Sample of comments by users are partially migrated",
        "query": "SELECT id, user_id, foo FROM comments_by_users_partially_migrated",
        "mode": "draft",
    }

    metric1 = {
        "name": "number_of_migrated_users",
        "description": "Number of migrated users",
        "query": "SELECT COUNT(DISTINCT user_id) FROM comments_by_migrated_users",
        "mode": "draft",
    }

    metric2 = {
        "name": "number_of_users_with_pending_migration",
        "description": "Number of users with a migration pending",
        "query": "SELECT COUNT(DISTINCT user_id) FROM comments_by_users_pending_a_migration",
        "mode": "draft",
    }

    metric3 = {
        "name": "number_of_users_partially_migrated",
        "description": "Number of users partially migrated",
        "query": "SELECT COUNT(DISTINCT user_id) FROM comments_by_users_partially_migrated",
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
        response = client_with_examples.post(
            f"/nodes/{node_type.value}/",
            json=node,
        )
        assert response.status_code == 201
        data = response.json()
        assert data["name"] == node["name"]
        assert data["mode"] == node["mode"]
        assert data["status"] == "invalid"

    # Add the missing parent
    missing_parent_node = {
        "name": "comments",
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

    response = client_with_examples.post(
        "/nodes/source/",
        json=missing_parent_node,
    )
    assert response.status_code == 201
    data = response.json()
    assert data["name"] == missing_parent_node["name"]
    assert data["mode"] == missing_parent_node["mode"]

    # Check that downstream nodes have now been switched to a "valid" status
    for node in [transform1, transform2, transform3, metric1, metric2, metric3]:
        response = client_with_examples.get(f"/nodes/{node['name']}/")
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == node["name"]
        assert data["mode"] == node["mode"]  # make sure the mode hasn't been changed
        assert (
            data["status"] == "valid"
        )  # make sure the node's status has been updated to valid

    # Check that nodes still not valid have an invalid status
    for node in [transform4, transform5]:
        response = client_with_examples.get(f"/nodes/{node['name']}/")
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == node["name"]
        assert data["mode"] == node["mode"]  # make sure the mode hasn't been changed
        assert data["status"] == "invalid"
