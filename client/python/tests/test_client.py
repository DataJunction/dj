"""Tests DJ client"""
import pandas
import pytest

from datajunction import DJClient
from datajunction.exceptions import DJClientException, DJNamespaceAlreadyExists
from datajunction.models import (
    AvailabilityState,
    Column,
    ColumnAttribute,
    Engine,
    MaterializationConfig,
    NodeMode,
)


class TestDJClient:
    """
    Tests for DJ client functionality.
    """

    @pytest.fixture
    def client(self, session_with_examples):
        """
        Returns a DJ client instance
        """
        return DJClient(requests_session=session_with_examples)  # type: ignore

    def test_namespaces(self, client):
        """
        Check that `client.namespaces()` works as expected.
        """
        expected = [
            {
                "namespace": "default",
            },
            {
                "namespace": "foo.bar",
            },
        ]
        result = client.namespaces()
        assert result == expected

    def test_nodes_in_namespace(self, client):
        """
        Check that `client.get_nodes_in_namespace()` works as expected.
        """
        assert set(client.namespace("foo.bar").nodes()) == {
            "foo.bar.repair_orders",
            "foo.bar.repair_order_details",
            "foo.bar.repair_type",
            "foo.bar.contractors",
            "foo.bar.municipality_municipality_type",
            "foo.bar.municipality_type",
            "foo.bar.municipality",
            "foo.bar.dispatchers",
            "foo.bar.hard_hats",
            "foo.bar.hard_hat_state",
            "foo.bar.us_states",
            "foo.bar.us_region",
            "foo.bar.repair_order",
            "foo.bar.contractor",
            "foo.bar.hard_hat",
            "foo.bar.local_hard_hats",
            "foo.bar.us_state",
            "foo.bar.dispatcher",
            "foo.bar.municipality_dim",
            "foo.bar.num_repair_orders",
            "foo.bar.avg_repair_price",
            "foo.bar.total_repair_cost",
            "foo.bar.avg_length_of_employment",
            "foo.bar.total_repair_order_discounts",
            "foo.bar.avg_repair_order_discounts",
            "foo.bar.avg_time_to_dispatch",
        }
        assert set(client.namespace("foo.bar").sources()) == {
            "foo.bar.repair_orders",
            "foo.bar.repair_order_details",
            "foo.bar.repair_type",
            "foo.bar.contractors",
            "foo.bar.municipality_municipality_type",
            "foo.bar.municipality_type",
            "foo.bar.municipality",
            "foo.bar.dispatchers",
            "foo.bar.hard_hats",
            "foo.bar.hard_hat_state",
            "foo.bar.us_states",
            "foo.bar.us_region",
        }
        assert set(client.namespace("foo.bar").dimensions()) == {
            "foo.bar.repair_order",
            "foo.bar.contractor",
            "foo.bar.hard_hat",
            "foo.bar.local_hard_hats",
            "foo.bar.us_state",
            "foo.bar.dispatcher",
            "foo.bar.municipality_dim",
        }
        assert set(client.namespace("foo.bar").metrics()) == {
            "foo.bar.num_repair_orders",
            "foo.bar.avg_repair_price",
            "foo.bar.total_repair_cost",
            "foo.bar.avg_length_of_employment",
            "foo.bar.total_repair_order_discounts",
            "foo.bar.avg_repair_order_discounts",
            "foo.bar.avg_time_to_dispatch",
        }
        assert client.namespace("foo.bar").transforms() == []
        assert client.namespace("foo.bar").cubes() == []

    def test_catalogs(self, client):
        """
        Check that `client.catalogs()` works as expected.
        """
        result = client.catalogs()
        assert result == [
            {"engines": [], "name": "draft"},
            {
                "engines": [
                    {
                        "dialect": "spark",
                        "name": "spark",
                        "uri": None,
                        "version": "3.1.1",
                    },
                ],
                "name": "default",
            },
            {
                "engines": [
                    {
                        "dialect": None,
                        "name": "postgres",
                        "uri": None,
                        "version": "15.2",
                    },
                ],
                "name": "public",
            },
        ]

    def test_engines(self, client):
        """
        Check that `client.engines()` works as expected.
        """
        result = client.engines()
        assert result == [
            {"dialect": "spark", "name": "spark", "uri": None, "version": "3.1.1"},
            {"dialect": None, "name": "postgres", "uri": None, "version": "15.2"},
        ]

    def test_all_nodes(self, client):
        """
        Verifies that retrieving nodes with `client.nodes()` or node-type
        specific calls like `client.sources()` work.
        """
        expected_names_only = {
            "default.repair_orders",
            "default.repair_order_details",
            "default.repair_type",
            "default.contractors",
            "default.municipality_municipality_type",
            "default.municipality_type",
            "default.municipality",
            "default.dispatchers",
            "default.hard_hats",
            "default.hard_hat_state",
            "default.us_states",
            "default.us_region",
            "default.repair_order",
            "default.contractor",
            "default.hard_hat",
            "default.local_hard_hats",
            "default.us_state",
            "default.dispatcher",
            "default.municipality_dim",
            "default.num_repair_orders",
            "default.avg_repair_price",
            "default.total_repair_cost",
            "default.avg_length_of_employment",
            "default.total_repair_order_discounts",
            "default.avg_repair_order_discounts",
            "default.avg_time_to_dispatch",
        }
        result_names_only = client.namespace("default").nodes()
        assert set(result_names_only) == expected_names_only

        # sources
        result_names_only = client.namespace("default").sources()
        assert set(result_names_only) == {
            "default.repair_orders",
            "default.repair_order_details",
            "default.repair_type",
            "default.contractors",
            "default.municipality_municipality_type",
            "default.municipality_type",
            "default.municipality",
            "default.dispatchers",
            "default.hard_hats",
            "default.hard_hat_state",
            "default.us_states",
            "default.us_region",
        }

        repair_orders = client.source("default.repair_orders")
        assert repair_orders.name == "default.repair_orders"
        assert repair_orders.catalog == "default"
        assert repair_orders.schema_ == "roads"
        assert repair_orders.table == "repair_orders"
        assert repair_orders.type == "source"

        # dimensions
        result_names_only = client.namespace("default").dimensions()
        assert set(result_names_only) == {
            "default.repair_order",
            "default.contractor",
            "default.hard_hat",
            "default.local_hard_hats",
            "default.us_state",
            "default.dispatcher",
            "default.municipality_dim",
        }
        repair_order_dim = client.dimension("default.repair_order")
        assert repair_order_dim.name == "default.repair_order"
        assert "FROM default.repair_orders" in repair_order_dim.query
        assert repair_order_dim.type == "dimension"
        assert repair_order_dim.primary_key == ["repair_order_id"]

        # transforms
        result = client.namespace("default").transforms()
        assert result == []

        # metrics
        result_names_only = client.namespace("default").metrics()
        assert set(result_names_only) == {
            "default.num_repair_orders",
            "default.avg_repair_price",
            "default.total_repair_cost",
            "default.avg_length_of_employment",
            "default.total_repair_order_discounts",
            "default.avg_repair_order_discounts",
            "default.avg_time_to_dispatch",
        }

        num_repair_orders = client.metric("default.num_repair_orders")
        assert num_repair_orders.name == "default.num_repair_orders"
        assert num_repair_orders.query == (
            "SELECT  count(repair_order_id) default_DOT_num_repair_orders "
            "\n FROM default.repair_orders\n\n"
        )
        assert num_repair_orders.type == "metric"

        # cubes
        result = client.namespace("default").cubes()
        assert result == []
        with pytest.raises(DJClientException) as exc_info:
            client.cube("a_cube")
        assert "Cube `a_cube` does not exist" in str(exc_info)

    def test_deactivating_a_node(self, client):  # pylint: disable=unused-argument
        """
        Verifies that deactivating and reactivating a node works.
        """
        length_metric = client.metric("default.avg_length_of_employment")
        response = length_metric.deactivate()
        assert response == "Successfully deactivated `default.avg_length_of_employment`"
        assert (
            "default.avg_length_of_employment"
            not in client.namespace("default").metrics()
        )
        response = length_metric.activate()
        assert response == "Successfully activated `default.avg_length_of_employment`"
        assert (
            "default.avg_length_of_employment" in client.namespace("default").metrics()
        )

    def test_create_node(self, client):  # pylint: disable=unused-argument
        """
        Verifies that creating a new node works.
        """
        account_type_table = client.new_source(
            name="default.account_type_table",
            description="A source table for account type data",
            display_name="Default: Account Type Table",
            catalog="default",
            schema_="store",
            table="account_type_table",
            columns=[
                Column(name="id", type="int"),
                Column(name="account_type_name", type="string"),
                Column(name="account_type_classification", type="int"),
                Column(name="preferred_payment_method", type="int"),
            ],
        )
        result = account_type_table.save(NodeMode.PUBLISHED)
        assert result["name"] == "default.account_type_table"
        assert "default.account_type_table" in client.namespace("default").sources()

        payment_type_table = client.new_source(
            name="default.payment_type_table",
            description="A source table for different types of payments",
            display_name="Default: Payment Type Table",
            catalog="default",
            schema_="accounting",
            table="payment_type_table",
            columns=[
                Column(name="id", type="int"),
                Column(name="payment_type_name", type="string"),
                Column(name="payment_type_classification", type="string"),
            ],
        )
        result = payment_type_table.save(NodeMode.PUBLISHED)
        assert result["name"] == "default.payment_type_table"
        assert "default.payment_type_table" in client.namespace("default").sources()

        revenue = client.new_source(
            name="default.revenue",
            description="Record of payments",
            display_name="Default: Payment Records",
            catalog="default",
            schema_="accounting",
            table="revenue",
            columns=[
                Column(name="payment_id", type="int"),
                Column(name="payment_amount", type="float"),
                Column(name="payment_type", type="int"),
                Column(name="customer_id", type="int"),
                Column(name="account_type", type="string"),
            ],
        )
        result = revenue.save(NodeMode.PUBLISHED)
        assert result["name"] == "default.revenue"
        assert "default.revenue" in client.namespace("default").sources()

        payment_type_dim = client.new_dimension(
            name="default.payment_type",
            description="Payment type dimension",
            display_name="Default: Payment Type",
            query=(
                "SELECT id, payment_type_name, payment_type_classification "
                "FROM default.payment_type_table"
            ),
            primary_key=["id"],
        )
        payment_type_dim.check()  # Test validating the node
        result = payment_type_dim.save(NodeMode.DRAFT)
        assert result["name"] == "default.payment_type"
        assert "default.payment_type" in client.namespace("default").dimensions()
        payment_type_dim.publish()  # Test changing a draft node to published
        payment_type_dim.sync()
        assert payment_type_dim.mode == NodeMode.PUBLISHED

        account_type_dim = client.new_dimension(
            name="default.account_type",
            description="Account type dimension",
            display_name="Default: Account Type",
            query=(
                "SELECT id, account_type_name, "
                "account_type_classification FROM "
                "default.account_type_table"
            ),
            primary_key=["id"],
        )
        result = account_type_dim.save(NodeMode.PUBLISHED)
        assert result["name"] == "default.account_type"
        assert "default.account_type" in client.namespace("default").dimensions()

        large_revenue_payments_only = client.new_transform(
            name="default.large_revenue_payments_only",
            description="Default: Only large revenue payments",
            query=(
                "SELECT payment_id, payment_amount, customer_id, account_type "
                "FROM default.revenue WHERE payment_amount > 1000000"
            ),
        )
        result = large_revenue_payments_only.save(NodeMode.PUBLISHED)
        assert result["name"] == "default.large_revenue_payments_only"
        assert (
            "default.large_revenue_payments_only"
            in client.namespace("default").transforms()
        )

        result = large_revenue_payments_only.add_materialization(
            MaterializationConfig(
                engine=Engine(name="spark", version="3.1.1"),
                schedule="0 * * * *",
                config={},
            ),
        )
        assert result == {
            "message": "Successfully updated materialization config named `default` for "
            "node `default.large_revenue_payments_only`",
            "urls": [["http://fake.url/job"]],
        }

        large_revenue_payments_and_business_only = client.new_transform(
            name="default.large_revenue_payments_and_business_only",
            description="Only large revenue payments from business accounts",
            query=(
                "SELECT payment_id, payment_amount, customer_id, account_type "
                "FROM default.revenue WHERE "
                "default.large_revenue_payments_and_business_only > 1000000 "
                "AND account_type='BUSINESS'"
            ),
        )
        large_revenue_payments_and_business_only.save(NodeMode.PUBLISHED)
        result = client.transform("default.large_revenue_payments_and_business_only")
        assert result.name == "default.large_revenue_payments_and_business_only"
        assert (
            "default.large_revenue_payments_and_business_only"
            in client.namespace(
                "default",
            ).transforms()
        )

        number_of_account_types = client.new_metric(
            name="default.number_of_account_types",
            description="Total number of account types",
            query="SELECT count(id) FROM default.account_type",
        )
        result = number_of_account_types.save(NodeMode.PUBLISHED)
        assert result["name"] == "default.number_of_account_types"
        assert (
            "default.number_of_account_types" in client.namespace("default").metrics()
        )

    def test_link_dimension(self, client):  # pylint: disable=unused-argument
        """
        Check linking dimensions works
        """
        repair_type = client.source("foo.bar.repair_type")
        result = repair_type.link_dimension(
            "contractor_id",
            "foo.bar.contractor",
            "contractor_id",
        )
        assert result["message"] == (
            "Dimension node foo.bar.contractor has been successfully linked to "
            "column contractor_id on node foo.bar.repair_type"
        )

    def test_sql(self, client):  # pylint: disable=unused-argument
        """
        Check that getting sql via the client works as expected.
        """
        metric = client.metric(node_name="foo.bar.avg_repair_price")
        result = metric.sql(dimensions=[], filters=[])
        assert "SELECT" in result and "FROM" in result

        # Retrieve SQL for a single metric
        result = metric.sql(dimensions=["dimension_that_does_not_exist"], filters=[])
        assert (
            result["message"]
            == "Cannot resolve type of column dimension_that_does_not_exist."
        )

        # Retrieve SQL for multiple metrics using the client object
        result = client.sql(
            metrics=["default.total_repair_cost", "default.avg_repair_price"],
            dimensions=[
                "default.hard_hat.city",
                "default.hard_hat.state",
                "default.dispatcher.company_name",
            ],
            filters=["default.hard_hat.state = 'AZ'"],
            engine_name="spark",
            engine_version="3.1.1",
        )
        assert "SELECT" in result and "FROM" in result

        # Should fail due to dimension not being available
        result = client.sql(
            metrics=["foo.bar.num_repair_orders", "foo.bar.avg_repair_price"],
            dimensions=["default.hard_hat.city"],
            filters=["default.hard_hat.state = 'AZ'"],
            engine_name="spark",
            engine_version="3.1.1",
        )
        assert result["message"] == (
            "The dimension attribute `default.hard_hat.city` is not available on "
            "every metric and thus cannot be included."
        )

    def test_get_metrics(self, client):
        """
        Check that `client.metrics()` works as expected.
        """
        metrics = client.metrics()
        assert metrics == [
            "default.num_repair_orders",
            "default.avg_repair_price",
            "default.total_repair_cost",
            "default.avg_length_of_employment",
            "default.total_repair_order_discounts",
            "default.avg_repair_order_discounts",
            "default.avg_time_to_dispatch",
            "foo.bar.num_repair_orders",
            "foo.bar.avg_repair_price",
            "foo.bar.total_repair_cost",
            "foo.bar.avg_length_of_employment",
            "foo.bar.total_repair_order_discounts",
            "foo.bar.avg_repair_order_discounts",
            "foo.bar.avg_time_to_dispatch",
        ]

    def test_get_dimensions(self, client):
        """
        Check that `metric.dimensions()` works as expected.
        """
        metric = client.metric(node_name="foo.bar.avg_repair_price")
        result = metric.dimensions()
        assert {
            "name": "foo.bar.dispatcher.company_name",
            "type": "string",
            "path": [
                "foo.bar.repair_order_details.repair_order_id",
                "foo.bar.repair_order.dispatcher_id",
            ],
        } in result

    def test_failure_modes(self, client):
        """
        Test some client failure modes when retrieving nodes.
        """
        with pytest.raises(DJClientException) as excinfo:
            client.transform(node_name="default.fruit")
        assert "No node with name default.fruit exists!" in str(excinfo)

        with pytest.raises(DJClientException) as excinfo:
            client.transform(node_name="foo.bar.avg_repair_price")
        assert (
            "A node with name foo.bar.avg_repair_price exists, but it is not a transform node!"
            in str(
                excinfo,
            )
        )

    def test_create_namespace(self, client):
        """
        Verifies that creating a new namespace works.
        """
        namespace = client.new_namespace(namespace="roads.demo")
        assert namespace.namespace == "roads.demo"
        with pytest.raises(DJNamespaceAlreadyExists) as exc_info:
            client.new_namespace(namespace="roads.demo")
        assert "Node namespace `roads.demo` already exists" in str(exc_info.value)

    def test_get_node_revisions(self, client):
        """
        Verifies that retrieving node revisions works
        """
        local_hard_hats = client.dimension("default.local_hard_hats")
        local_hard_hats.display_name = "local hard hats"
        local_hard_hats.description = "Local hard hats dimension"
        local_hard_hats.save()
        local_hard_hats.primary_key = ["hard_hat_id", "last_name"]
        local_hard_hats.save()
        revs = local_hard_hats.revisions()
        assert len(revs) == 3
        assert [rev["version"] for rev in revs] == ["v1.0", "v1.1", "v2.0"]

    def test_update_node_with_query(self, client):
        """
        Verify that updating a node with a query works
        """
        local_hard_hats = client.dimension("default.local_hard_hats")
        local_hard_hats.query = """
        SELECT
        hh.hard_hat_id,
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
        contractor_id,
        hhs.state_id AS state_id
        FROM default.hard_hats hh
        LEFT JOIN default.hard_hat_state hhs
        ON hh.hard_hat_id = hhs.hard_hat_id
        WHERE hh.state_id = 'CA'
        """
        response = local_hard_hats.save()
        assert "WHERE hh.state_id = 'CA'" in response["query"]
        assert response["version"] == "v2.0"

        local_hard_hats.display_name = "local hard hats"
        local_hard_hats.description = "Local hard hats dimension"
        response = local_hard_hats.save()
        assert response["display_name"] == "local hard hats"
        assert response["description"] == "Local hard hats dimension"
        assert response["version"] == "v2.1"

        local_hard_hats.primary_key = ["hard_hat_id", "last_name"]
        response = local_hard_hats.save()

        assert response["version"] == "v3.0"
        assert {
            "name": "hard_hat_id",
            "type": "int",
            "attributes": [
                {"attribute_type": {"namespace": "system", "name": "primary_key"}},
            ],
            "dimension": None,
        } in response["columns"]
        assert {
            "name": "last_name",
            "type": "string",
            "attributes": [
                {"attribute_type": {"namespace": "system", "name": "primary_key"}},
            ],
            "dimension": None,
        } in response["columns"]

    def test_update_source_node(self, client):
        """
        Verify that updating a source node's columns works
        """
        us_states = client.source("default.us_states")
        new_columns = [
            {"name": "state_id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "abbr", "type": "string"},
            {"name": "region", "type": "int"},
        ]
        us_states.columns = new_columns
        response = us_states.save()
        assert response["columns"] == [
            {"attributes": [], "dimension": None, "name": "state_id", "type": "int"},
            {"attributes": [], "dimension": None, "name": "name", "type": "string"},
            {"attributes": [], "dimension": None, "name": "abbr", "type": "string"},
            {"attributes": [], "dimension": None, "name": "region", "type": "int"},
        ]
        assert response["version"] == "v2.0"

    def test_data(self, client):
        """
        Verify that updating a source node's columns works
        """
        metric = client.metric(node_name="default.avg_repair_price")

        # Retrieve data for a single metric
        result = metric.data(dimensions=["default.hard_hat.city"], filters=[])
        expected_df = pandas.DataFrame.from_dict(
            {"default_DOT_avg_repair_price": [1.0, 2.0], "city": ["Foo", "Bar"]},
        )
        pandas.testing.assert_frame_equal(result, expected_df)

        # No data
        with pytest.raises(DJClientException) as exc_info:
            metric.data(dimensions=["default.hard_hat.state"], filters=[])
        assert "No data for query!" in str(exc_info)

        # Error propagation
        with pytest.raises(DJClientException) as exc_info:
            metric.data(dimensions=["default.hard_hat.postal_code"], filters=[])
        assert "Error response from query service" in str(exc_info)

    def test_add_availability(self, client):
        """
        Verify adding an availability state to a node
        """
        dim = client.dimension(node_name="default.contractor")
        response = dim.add_availability(
            AvailabilityState(
                catalog="default",
                schema_="materialized",
                table="contractor",
                valid_through_ts=1688660209,
            ),
        )
        assert response == {"message": "Availability state successfully posted"}

    def test_set_column_attributes(self, client):
        """
        Verify setting column attributes on a node
        """
        dim = client.source(node_name="default.contractors")
        response = dim.set_column_attributes(
            [
                ColumnAttribute(
                    attribute_type_name="dimension",
                    column_name="contact_title",
                ),
            ],
        )
        assert response == [
            {
                "attributes": [
                    {"attribute_type": {"name": "dimension", "namespace": "system"}},
                ],
                "dimension": None,
                "name": "contact_title",
                "type": "string",
            },
        ]
