# pylint: disable=too-many-lines,too-many-statements
"""Tests DJ client"""

from unittest.mock import MagicMock, patch

import pytest
from requests.exceptions import HTTPError

from datajunction import DJBuilder
from datajunction.exceptions import (
    DJClientException,
    DJNamespaceAlreadyExists,
    DJTableAlreadyRegistered,
    DJTagAlreadyExists,
    DJViewAlreadyRegistered,
)
from datajunction.models import (
    AvailabilityState,
    Column,
    ColumnAttribute,
    Materialization,
    MaterializationJobType,
    MaterializationStrategy,
    MetricDirection,
    MetricUnit,
    NodeMode,
)


class TestDJBuilder:  # pylint: disable=too-many-public-methods, protected-access
    """
    Tests for DJ client/builder functionality.
    """

    @pytest.fixture
    def client(self, module__session_with_examples):
        """
        Returns a DJ client instance
        """
        return DJBuilder(requests_session=module__session_with_examples)  # type: ignore

    def test_nodes_in_namespace(self, client):
        """
        Check that `client._get_nodes_in_namespace()` works as expected.
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
            "foo.bar.cube_one",
            "foo.bar.repair_orders_thin",
            "foo.bar.with_custom_metadata",
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
        assert set(client.list_dimensions(namespace="foo.bar")) == {
            "foo.bar.repair_order",
            "foo.bar.contractor",
            "foo.bar.hard_hat",
            "foo.bar.local_hard_hats",
            "foo.bar.us_state",
            "foo.bar.dispatcher",
            "foo.bar.municipality_dim",
        }
        assert set(client.list_metrics(namespace="foo.bar")) == {
            "foo.bar.num_repair_orders",
            "foo.bar.avg_repair_price",
            "foo.bar.total_repair_cost",
            "foo.bar.avg_length_of_employment",
            "foo.bar.total_repair_order_discounts",
            "foo.bar.avg_repair_order_discounts",
            "foo.bar.avg_time_to_dispatch",
        }
        assert set(client.namespace("foo.bar").transforms()) == {
            "foo.bar.with_custom_metadata",
            "foo.bar.repair_orders_thin",
        }
        assert client.namespace("foo.bar").cubes() == ["foo.bar.cube_one"]

    def test_all_nodes(self, client):
        """
        Verifies that retrieving nodes with `client.nodes()` or node-type
        specific calls like `client.sources()` work.
        """
        expected_names_only = {
            "default.repair_orders",
            "default.repair_orders_foo",
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
            "default.cube_two",
            "default.repair_orders_thin",
        }
        result_names_only = client.namespace("default").nodes()
        assert set(result_names_only) == expected_names_only

        # sources
        result_names_only = client.namespace("default").sources()
        assert set(result_names_only) == {
            "default.repair_orders",
            "default.repair_orders_foo",
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
        assert repair_orders.description == "All repair orders"
        assert repair_orders.tags == []
        assert repair_orders.primary_key == []
        assert repair_orders.current_version == "v1.0"
        assert repair_orders.columns[0] == Column(
            name="repair_order_id",
            type="int",
            display_name="Repair Order Id",
            attributes=[],
            dimension=None,
        )

        # dimensions (all)
        all_dimensions = client.list_dimensions()
        assert set(all_dimensions) == {
            "default.repair_order",
            "default.contractor",
            "default.hard_hat",
            "default.local_hard_hats",
            "default.us_state",
            "default.dispatcher",
            "default.municipality_dim",
            "foo.bar.repair_order",
            "foo.bar.contractor",
            "foo.bar.hard_hat",
            "foo.bar.local_hard_hats",
            "foo.bar.us_state",
            "foo.bar.dispatcher",
            "foo.bar.municipality_dim",
        }
        # dimensions (namespace: default)
        result_names_only = client.list_dimensions(namespace="default")
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
        assert repair_order_dim.description == "Repair order dimension"
        assert repair_order_dim.tags == []
        assert repair_order_dim.current_version == "v1.0"
        assert repair_order_dim.columns[0] == Column(
            name="repair_order_id",
            type="int",
            display_name="Repair Order Id",
            attributes=[ColumnAttribute(name="primary_key", namespace="system")],
            dimension=None,
        )

        # transforms
        result = client.namespace("default").transforms()
        assert result == ["default.repair_orders_thin"]
        thin = client.transform("default.repair_orders_thin")
        assert thin.name == "default.repair_orders_thin"
        assert "FROM default.repair_orders" in thin.query
        assert thin.type == "transform"
        assert thin.primary_key == []
        assert thin.description == "3 columns from default.repair_orders"
        assert thin.tags == []
        assert thin.current_version == "v1.0"
        assert thin.columns[0] == Column(
            name="repair_order_id",
            type="int",
            display_name="Repair Order Id",
            attributes=[],
            dimension=None,
        )

        # metrics
        result_names_only = client.list_metrics(namespace="default")
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
            "SELECT count(repair_order_id) FROM default.repair_orders"
        )
        assert num_repair_orders.type == "metric"
        assert num_repair_orders.required_dimensions == []
        assert num_repair_orders.description == "Number of repair orders"
        assert num_repair_orders.tags == []
        assert num_repair_orders.metric_metadata is None
        assert num_repair_orders.current_version == "v1.0"
        assert num_repair_orders.columns[0] == Column(
            name="default_DOT_num_repair_orders",
            type="bigint",
            display_name="Num Repair Orders",
            attributes=[],
            dimension=None,
        )

        # cubes
        result = client.namespace("default").cubes()
        assert result == ["default.cube_two"]
        cube_two = client.cube("default.cube_two")
        assert cube_two.type == "cube"
        assert cube_two.metrics == ["default.num_repair_orders"]
        assert cube_two.dimensions == ["default.municipality_dim.local_region"]
        assert cube_two.filters is None
        assert cube_two.columns[0] == Column(
            name="default.num_repair_orders",
            type="bigint",
            display_name="Num Repair Orders",
            attributes=[],
            dimension=None,
        )

        with pytest.raises(DJClientException) as exc_info:
            client.cube("a_cube")
        assert "Cube `a_cube` does not exist" in str(exc_info)

    def test_deactivating_node(self, client):  # pylint: disable=unused-argument
        """
        Verifies that deactivating and reactivating a node works.
        """
        length_metric = client.metric("default.avg_length_of_employment")
        response = length_metric.delete()
        assert response is None
        assert "default.avg_length_of_employment" not in client.list_metrics(
            namespace="default",
        )
        response = length_metric.restore()
        assert response is None
        assert "default.avg_length_of_employment" in client.list_metrics(
            namespace="default",
        )

    def test_register_table(self, client):  # pylint: disable=unused-argument
        """
        Verifies that registering a table works.
        """
        try:
            client.create_namespace("source")
        except DJNamespaceAlreadyExists:
            pass
        store_comments = client.register_table(
            catalog="default",
            schema="store",
            table="comments",
        )
        assert store_comments.name == "source.default.store.comments"
        assert (
            "source.default.store.comments"
            in client.namespace("source.default.store").sources()
        )
        # and that errors are handled properly
        with patch("starlette.testclient.TestClient.post") as post_mock:
            post_mock.side_effect = HTTPError("409 Client Error: Conflict")
            with pytest.raises(DJTableAlreadyRegistered):
                client.register_table(
                    catalog="default",
                    schema="store",
                    table="comments",
                )
        with patch("starlette.testclient.TestClient.post") as post_mock:
            post_mock.side_effect = Exception("Boom!")
            with pytest.raises(DJClientException):
                client.register_table(
                    catalog="default",
                    schema="store",
                    table="comments",
                )

    def test_register_view(self, client):  # pylint: disable=unused-argument
        """
        Verifies that registering a view works.
        """
        try:
            client.create_namespace("source")
        except DJNamespaceAlreadyExists:
            pass
        store_comments = client.register_view(
            catalog="default",
            schema="store",
            view="comments_view",
            query="SELECT * FROM store.comments",
            replace=True,
        )
        assert store_comments.name == "source.default.store.comments_view"
        assert (
            "source.default.store.comments_view"
            in client.namespace("source.default.store").sources()
        )
        # and that errors are handled properly
        with patch("starlette.testclient.TestClient.post") as post_mock:
            post_mock.side_effect = HTTPError("409 Client Error: Conflict")
            with pytest.raises(DJViewAlreadyRegistered):
                client.register_view(
                    catalog="default",
                    schema="store",
                    view="comments_view",
                    query="SELECT * FROM store.comments",
                )
        with patch("starlette.testclient.TestClient.post") as post_mock:
            post_mock.side_effect = Exception("Boom!")
            with pytest.raises(DJClientException):
                client.register_view(
                    catalog="default",
                    schema="store",
                    view="comments_view",
                    query="SELECT * FROM store.comments",
                    replace=True,
                )

    def test_create_and_update_node(self, client):  # pylint: disable=unused-argument
        """
        Verifies that creating nodes works.
        """
        # create it
        account_type_table = client.create_source(
            name="default.account_type_table",
            description="A source table for account type data",
            display_name="Account Type Table",
            catalog="default",
            schema="store",
            table="account_type_table",
            columns=[
                Column(name="id", type="int"),
                Column(name="account_type_name", type="string"),
                Column(name="account_type_classification", type="int"),
                Column(name="preferred_payment_method", type="int"),
            ],
            mode=NodeMode.DRAFT,
        )
        assert account_type_table.name == "default.account_type_table"
        assert "default.account_type_table" in client.namespace("default").sources()

        # update it
        # ... should fail since update_if_exists is set to False
        with pytest.raises(DJClientException):
            account_type_table = client.create_source(
                name="default.account_type_table",
                description="new description",
                update_if_exists=False,
            )

        # ... should work since update_if_exists is set to True
        account_type_table = client.create_source(
            name="default.account_type_table",
            description="new description",
            update_if_exists=True,
        )
        assert account_type_table.description == "new description"

    def test_saving_a_node(self, client):
        """
        Verifies that saving a node works.
        """
        client._create_node = MagicMock(return_value=MagicMock(status_code=200))
        client._update_node_tags = MagicMock(return_value=MagicMock(status_code=200))

        client.create_tag(
            name="foo",
            description="Foo",
            tag_type="test",
            tag_metadata={"foo": "bar"},
        )
        account_type_table = client.create_source(
            name="default.account_type_table",
            description="A source table for account type data",
            display_name="Account Type Table",
            catalog="default",
            schema="store",
            table="account_type_table",
            columns=[
                Column(name="id", type="int"),
                Column(name="account_type_name", type="string"),
                Column(name="account_type_classification", type="int"),
                Column(name="preferred_payment_method", type="int"),
            ],
            mode=NodeMode.DRAFT,
            tags=["foo"],
            update_if_exists=True,
        )
        assert account_type_table.name == "default.account_type_table"
        assert account_type_table.display_name == "Account Type Table"

        new_node = account_type_table
        new_node.name = "default.account_type_table_new"
        new_node.display_name = "New: Account Type Table"
        new_node.save()
        new_node.refresh()
        assert new_node.name == "default.account_type_table_new"
        assert new_node.display_name == "New: Account Type Table"

    def test_create_nodes(self, client):  # pylint: disable=unused-argument
        """
        Verifies that creating nodes works.
        """
        client.create_tag(
            name="foo",
            description="Foo Bar",
            tag_type="test",
            tag_metadata={"foo": "bar"},
            update_if_exists=True,
        )
        foo_tag = client.tag("foo")
        # source nodes
        account_type_table = client.create_source(
            name="default.account_type_table",
            description="A source table for account type data",
            display_name="Account Type Table",
            catalog="default",
            schema="store",
            table="account_type_table",
            columns=[
                Column(name="id", type="int"),
                Column(name="account_type_name", type="string"),
                Column(name="account_type_classification", type="int"),
                Column(name="preferred_payment_method", type="int"),
            ],
            tags=[foo_tag],
            mode=NodeMode.PUBLISHED,
            update_if_exists=True,
        )
        assert account_type_table.name == "default.account_type_table"
        assert [tag["name"] for tag in account_type_table.tags] == ["foo"]
        assert "default.account_type_table" in client.namespace("default").sources()

        payment_type_table = client.create_source(
            name="default.payment_type_table",
            description="A source table for payment type data",
            display_name="Payment Type Table",
            catalog="default",
            schema="store",
            table="payment_type_table",
            columns=[
                Column(name="id", type="int"),
                Column(name="payment_type_name", type="string"),
                Column(name="payment_type_classification", type="string"),
            ],
            tags=[foo_tag],
            mode=NodeMode.PUBLISHED,
        )
        assert payment_type_table.name == "default.payment_type_table"
        assert [tag["name"] for tag in payment_type_table.tags] == ["foo"]
        assert "default.payment_type_table" in client.namespace("default").sources()

        revenue = client.create_source(
            name="default.revenue",
            description="Record of payments",
            display_name="Payment Records",
            catalog="default",
            schema="accounting",
            table="revenue",
            columns=[
                Column(name="payment_id", type="int"),
                Column(name="payment_amount", type="float"),
                Column(name="payment_type", type="int"),
                Column(name="customer_id", type="int"),
                Column(name="account_type", type="string"),
            ],
            mode=NodeMode.PUBLISHED,
        )
        assert revenue.name == "default.revenue"
        assert "default.revenue" in client.namespace("default").sources()

        # dimension nodes
        payment_type_dim = client.create_dimension(
            name="default.payment_type",
            description="Payment type dimension",
            display_name="Payment Type",
            query=(
                "SELECT id, payment_type_name, payment_type_classification "
                "FROM default.payment_type_table"
            ),
            primary_key=["id"],
            mode=NodeMode.DRAFT,
            tags=[foo_tag],
        )
        payment_type_dim.validate()  # pylint: disable=protected-access
        assert payment_type_dim.name == "default.payment_type"
        assert "default.payment_type" in client.list_dimensions(namespace="default")
        assert [tag["name"] for tag in payment_type_dim.tags] == ["foo"]
        payment_type_dim.publish()  # Test changing a draft node to published
        payment_type_dim.refresh()
        assert payment_type_dim.mode == NodeMode.PUBLISHED

        account_type_dim = client.create_dimension(
            name="default.account_type",
            description="Account type dimension",
            display_name="Account Type",
            query=(
                "SELECT id, account_type_name, "
                "account_type_classification FROM "
                "default.account_type_table"
            ),
            primary_key=["id"],
            mode=NodeMode.PUBLISHED,
        )
        assert account_type_dim.name == "default.account_type"
        assert len(account_type_dim.columns) == 3
        assert "default.account_type" in client.list_dimensions(namespace="default")

        # transform nodes
        large_revenue_payments_only = client.create_transform(
            name="default.large_revenue_payments_only",
            description="Only large revenue payments",
            query=(
                "SELECT payment_id, payment_amount, customer_id, account_type "
                "FROM default.revenue WHERE payment_amount > 1000000"
            ),
            mode=NodeMode.PUBLISHED,
            tags=[foo_tag],
            custom_metadata={"foo": "bar"},
        )
        assert large_revenue_payments_only.name == "default.large_revenue_payments_only"
        assert (
            "default.large_revenue_payments_only"
            in client.namespace("default").transforms()
        )
        assert len(large_revenue_payments_only.columns) == 4
        assert [tag["name"] for tag in large_revenue_payments_only.tags] == ["foo"]
        assert large_revenue_payments_only.custom_metadata == {"foo": "bar"}

        client.transform("default.large_revenue_payments_only")

        result = large_revenue_payments_only.add_materialization(
            Materialization(
                job=MaterializationJobType.SPARK_SQL,
                strategy=MaterializationStrategy.FULL,
                schedule="0 * * * *",
                config={},
            ),
        )
        assert result == {
            "message": "Successfully updated materialization config named `spark_sql__full` for "
            "node `default.large_revenue_payments_only`",
            "urls": [["http://fake.url/job"]],
        }

        result = large_revenue_payments_only.deactivate_materialization(
            materialization_name="spark_sql__full",
        )
        assert result == {
            "message": "The materialization named `spark_sql__full` on node "
            "`default.large_revenue_payments_only` has been successfully deactivated",
        }

        large_revenue_payments_and_business_only = client.create_transform(
            name="default.large_revenue_payments_and_business_only",
            description="Only large revenue payments from business accounts",
            query=(
                "SELECT payment_id, payment_amount, customer_id, account_type "
                "FROM default.revenue WHERE "
                "default.large_revenue_payments_and_business_only > 1000000 "
                "AND account_type='BUSINESS'"
            ),
            mode=NodeMode.PUBLISHED,
        )
        assert (
            large_revenue_payments_and_business_only.name
            == "default.large_revenue_payments_and_business_only"
        )
        assert (
            "default.large_revenue_payments_and_business_only"
            in client.namespace(
                "default",
            ).transforms()
        )

        # metric nodes
        number_of_account_types = client.create_metric(
            name="default.number_of_account_types",
            description="Total number of account types",
            query="SELECT count(id) FROM default.account_type",
            mode=NodeMode.PUBLISHED,
        )
        assert number_of_account_types.name == "default.number_of_account_types"
        assert "default.number_of_account_types" in client.list_metrics(
            namespace="default",
        )
        assert number_of_account_types.required_dimensions is None
        assert number_of_account_types.metric_metadata is None
        assert [tag["name"] for tag in number_of_account_types.tags] == []

        # Test updating metric node
        number_of_account_types = client.create_metric(
            name="default.number_of_account_types",
            query="SELECT count(*) FROM default.account_type",
            required_dimensions=["account_type_name"],
            direction=MetricDirection.HIGHER_IS_BETTER,
            unit=MetricUnit.UNITLESS,
            tags=[foo_tag],
            update_if_exists=True,
        )
        assert number_of_account_types.name == "default.number_of_account_types"
        assert number_of_account_types.required_dimensions == ["account_type_name"]
        assert (
            number_of_account_types.metric_metadata["direction"] == "higher_is_better"
        )
        assert number_of_account_types.metric_metadata["unit"]["name"] == "UNITLESS"

        assert (
            number_of_account_types.query == "SELECT count(*) FROM default.account_type"
        )
        assert number_of_account_types.description == "Total number of account types"
        assert number_of_account_types.mode == "published"
        assert [dim["name"] for dim in number_of_account_types.dimensions()] == [
            "default.account_type.account_type_classification",
            "default.account_type.account_type_name",
            "default.account_type.id",
        ]
        assert [tag["name"] for tag in number_of_account_types.tags] == ["foo"]

        # test setting required dims, direction, and unit at creation time (not update)
        number_of_account_types2 = client.create_metric(
            name="default.number_of_account_types2",
            description="Total number of account types",
            query="SELECT count(id) FROM default.account_type",
            required_dimensions=["account_type_name"],
            direction=MetricDirection.HIGHER_IS_BETTER,
            unit=MetricUnit.UNITLESS,
            tags=[foo_tag],
            mode=NodeMode.PUBLISHED,
        )
        assert number_of_account_types2.name == "default.number_of_account_types2"
        assert number_of_account_types2.required_dimensions == ["account_type_name"]
        assert (
            number_of_account_types2.metric_metadata["direction"] == "higher_is_better"
        )
        assert number_of_account_types2.metric_metadata["unit"]["name"] == "UNITLESS"

        # cube nodes
        cube_one = client.create_cube(
            name="default.cube_one",
            description="Ice ice cube.",
            metrics=["default.number_of_account_types"],
            dimensions=["default.account_type.account_type_name"],
            mode=NodeMode.PUBLISHED,
            tags=[foo_tag],
        )
        assert cube_one.name == "default.cube_one"
        assert cube_one.status == "valid"
        assert cube_one.metrics == ["default.number_of_account_types"]
        assert [tag["name"] for tag in cube_one.tags] == ["foo"]

        # Test updating cube node
        cube_one = client.create_cube(
            name="default.cube_one",
            description="Ice cubes!",
            metrics=[
                "default.number_of_account_types",
                "default.number_of_account_types2",
            ],
            dimensions=["default.account_type.account_type_name"],
            mode=NodeMode.PUBLISHED,
            tags=[],
            update_if_exists=True,
        )
        assert cube_one.name == "default.cube_one"
        assert cube_one.description == "Ice cubes!"
        assert cube_one.metrics == [
            "default.number_of_account_types",
            "default.number_of_account_types2",
        ]
        assert [tag["name"] for tag in cube_one.tags] == []

    def test_link_unlink_dimension(self, client):  # pylint: disable=unused-argument
        """
        Check that linking and unlinking dimensions to a node's column works
        """
        repair_type = client.source("foo.bar.repair_type")
        result = repair_type.link_dimension(
            "contractor_id",
            "foo.bar.contractor",
            "contractor_id",
        )
        assert result["message"] == (
            "The dimension link between foo.bar.repair_type and foo.bar.contractor "
            "has been successfully updated."
        )

        # Unlink the dimension
        result = repair_type.unlink_dimension(
            "contractor_id",
            "foo.bar.contractor",
            "contractor_id",
        )
        assert result["message"] == (
            "Dimension link foo.bar.contractor to node foo.bar.repair_type has been removed."
        )

    def test_link_complex_dimension(self, client):
        """
        Check that linking complex dimensions to a node works as expected
        """

        repair_type = client.source("foo.bar.repair_type")
        result = repair_type.link_complex_dimension(
            dimension_node="foo.bar.contractor",
            join_type="inner",
            join_on="foo.bar.repair_type.contractor_id = foo.bar.contractor.contractor_id",
            role="repair_contractor",
        )
        assert result["message"] == (
            "Dimension node foo.bar.contractor has been "
            "successfully linked to node foo.bar.repair_type."
        )

        # Unlink the dimension
        result = repair_type.remove_complex_dimension_link(
            dimension_node="foo.bar.contractor",
            role="repair_contractor",
        )
        assert result["message"] == (
            "Dimension link foo.bar.contractor (role repair_contractor) to "
            "node foo.bar.repair_type has been removed."
        )

    def test_add_remove_reference_dimension_link(self, client):
        """
        Check that adding a reference dimension link works as expected
        """

        repair_type = client.source("foo.bar.repair_type")
        result = repair_type.add_reference_dimension_link(
            node_column="contractor_id",
            dimension_node="foo.bar.contractor",
            dimension_column="contractor_id",
        )
        assert result["message"] == (
            "foo.bar.repair_type.contractor_id has been successfully linked "
            "to foo.bar.contractor.contractor_id"
        )

        # Unlink the dimension
        result = repair_type.remove_reference_dimension_link(
            node_column="contractor_id",
        )
        assert result["message"] == (
            "The reference dimension link on foo.bar.repair_type.contractor_id has been removed."
        )

    def test_sql(self, client):  # pylint: disable=unused-argument
        """
        Check that getting sql via the client works as expected.
        """

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

        result = client.sql(metrics=["foo.bar.avg_repair_price"])
        assert "SELECT" in result and "FROM" in result

        # Retrieve SQL for a single metric
        result = client.sql(
            metrics=["foo.bar.avg_repair_price"],
            dimensions=["foo.bar.dimension_that_does_not_exist"],
            filters=[],
        )
        assert (
            result["message"]
            == "Please make sure that `dimension_that_does_not_exist` is a dimensional attribute."
            or result["message"]
            == "foo.bar.dimension_that_does_not_exist are not available dimensions on "
            "foo.bar.avg_repair_price"
        )

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
        ) or result["message"] == (
            "default.hard_hat.city are not available dimensions on "
            "foo.bar.num_repair_orders, foo.bar.avg_repair_price"
        )

    def test_get_dimensions(self, client):
        """
        Check that `metric.dimensions()` works as expected.
        """
        metric = client.metric(node_name="foo.bar.avg_repair_price")
        result = metric.dimensions()
        assert {
            "name": "foo.bar.dispatcher.company_name",
            "type": "string",
            "node_name": "foo.bar.dispatcher",
            "node_display_name": "Dispatcher",
            "properties": [],
            "path": [
                "foo.bar.repair_order_details",
                "foo.bar.repair_order",
            ],
            "filter_only": False,
        } in result

    def test_create_namespace(self, client):
        """
        Verifies that creating a new namespace works.
        """
        with pytest.raises(DJClientException) as exc_info:
            client.namespace(namespace="roads.demo")
        assert "Namespace `roads.demo` does not exist" in str(exc_info.value)

        namespace = client.create_namespace(namespace="roads.demo")
        assert namespace.namespace == "roads.demo"

        with pytest.raises(DJNamespaceAlreadyExists) as exc_info:
            client.create_namespace(namespace="roads.demo")
        assert "Node namespace `roads.demo` already exists" in str(exc_info.value)

    def test_create_delete_restore_namespace(self, client):
        """
        Verifies that deleting a new namespace works.
        """
        # create it first
        namespace = client.create_namespace(namespace="roads.demo.foo")
        assert namespace.namespace == "roads.demo.foo"
        with pytest.raises(DJNamespaceAlreadyExists) as exc_info:
            client.create_namespace(namespace="roads.demo.foo")
        assert "Node namespace `roads.demo.foo` already exists" in str(exc_info.value)

        # then delete it
        response = client.delete_namespace(namespace="roads.demo.foo")
        assert response is None
        with pytest.raises(DJClientException) as exc_info:
            client.delete_namespace(namespace="roads.demo.foo")
        assert "Namespace `roads.demo.foo` is already deactivated." in str(
            exc_info.value,
        )

        # and then restore it
        response = client.restore_namespace(namespace="roads.demo.foo")
        assert response is None
        with pytest.raises(DJClientException) as exc_info:
            client.restore_namespace(namespace="roads.demo.foo")
        assert "Node namespace `roads.demo.foo` already exists and is active" in str(
            exc_info.value,
        )

    def test_get_node_revisions(self, client):
        """
        Verifies that retrieving node revisions works
        """
        local_hard_hats = client.dimension("foo.bar.local_hard_hats")
        local_hard_hats.display_name = "local hard hats"
        local_hard_hats.description = "Local hard hats dimension"
        local_hard_hats.save()
        local_hard_hats.primary_key = ["hard_hat_id", "last_name"]
        local_hard_hats.save()
        revs = local_hard_hats.list_revisions()
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
            "display_name": "Hard Hat Id",
            "attributes": [
                {"attribute_type": {"namespace": "system", "name": "primary_key"}},
            ],
            "description": None,
            "dimension": None,
            "partition": None,
        } in response["columns"]
        assert {
            "name": "last_name",
            "type": "string",
            "display_name": "Last Name",
            "attributes": [
                {"attribute_type": {"namespace": "system", "name": "primary_key"}},
            ],
            "description": None,
            "dimension": None,
            "partition": None,
        } in response["columns"]

    def test_update_custom_metadata(self, client):
        """
        Verify that updating a node's custom metadata works.
        """
        transform_node = client.transform("foo.bar.with_custom_metadata")
        assert transform_node.custom_metadata == {"foo": "bar"}

        # update
        transform_node.custom_metadata = {"bar": "baz"}
        transform_node.save()

        # check again
        transform_node_again = client.transform("foo.bar.with_custom_metadata")
        assert transform_node_again.custom_metadata == {"bar": "baz"}

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
            {
                "attributes": [],
                "description": None,
                "dimension": None,
                "display_name": "State Id",
                "name": "state_id",
                "type": "int",
                "partition": None,
            },
            {
                "attributes": [],
                "description": None,
                "dimension": None,
                "display_name": "Name",
                "name": "name",
                "type": "string",
                "partition": None,
            },
            {
                "attributes": [],
                "description": None,
                "dimension": None,
                "display_name": "Abbr",
                "name": "abbr",
                "type": "string",
                "partition": None,
            },
            {
                "attributes": [],
                "description": None,
                "dimension": None,
                "display_name": "Region",
                "name": "region",
                "type": "int",
                "partition": None,
            },
        ]
        assert response["version"] == "v2.0"

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
            "contact_title",
            [
                ColumnAttribute(
                    name="dimension",
                ),
            ],
        )
        assert response == [
            {
                "attributes": [
                    {"attribute_type": {"name": "dimension", "namespace": "system"}},
                ],
                "description": None,
                "dimension": None,
                "display_name": "Contact Title",
                "name": "contact_title",
                "type": "string",
                "partition": None,
            },
        ]

    def test_set_column_display_name(self, client):
        """
        Verify setting a column's display name
        """
        dim = client.source(node_name="default.contractors")
        response = dim.set_column_display_name(
            "contact_title",
            "My Contact's Title",
        )
        assert response["display_name"] == "My Contact's Title"

    def test_set_column_description(self, client):
        """
        Verify setting a column's description
        """
        dim = client.source(node_name="default.contractors")
        response = dim.set_column_description(
            "contact_title",
            "The title of my contact",
        )
        assert response["description"] == "The title of my contact"

    #
    # Tags
    #
    def test_creating_a_tag(self, client):
        """
        Test creating a tag
        """
        client.create_tag(
            name="foo.one",
            description="Foo Bar",
            tag_type="test",
            tag_metadata={"foo": "bar"},
        )
        tag = client.tag("foo.one")
        assert tag.name == "foo.one"
        assert tag.description == "Foo Bar"
        assert tag.tag_type == "test"
        assert tag.tag_metadata == {"foo": "bar"}

    def test_tag_already_exists(self, client):
        """
        Test that the client raises properly when a tag already exists
        """
        client.create_tag(
            name="foo.two",
            description="Foo Bar",
            tag_type="test",
            tag_metadata={"foo": "bar"},
        )
        # update if a tag exists
        client.create_tag(
            name="foo.two",
            description="Foo Bar",
            tag_type="test",
            tag_metadata={"foo": "bar"},
            update_if_exists=True,
        )
        # fail if a tag exists
        with pytest.raises(DJTagAlreadyExists) as exc_info:
            client.create_tag(
                name="foo.two",
                description="Foo Bar",
                tag_type="test",
                tag_metadata={"foo": "bar"},
            )
        assert "Tag `foo.two` already exists" in str(exc_info.value)

    def test_updating_a_tag(self, client):
        """
        Test updating a tag
        """
        client.create_tag(
            name="foo.three",
            description="Foo Bar",
            tag_type="test",
            tag_metadata={"foo": "bar"},
        )
        tag = client.tag("foo.three")
        assert tag.name == "foo.three"
        assert tag.description == "Foo Bar"
        assert tag.tag_type == "test"
        assert tag.tag_metadata == {"foo": "bar"}
        tag.description = "This is an updated description."
        tag.save()
        # refresh the tag
        repulled_tag = client.tag("foo.three")
        repulled_tag.refresh()
        assert repulled_tag.description == "This is an updated description."

    def test_tag_does_not_exist(self, client):
        """
        Test that the client raises properly when a tag does not exist
        """
        with pytest.raises(DJClientException) as exc_info:
            client.tag("does-not-exist")
        assert "Tag `does-not-exist` does not exist" in str(exc_info.value)

    def test_tag_a_node(self, client):
        """
        Test that a node can be tagged properly
        """
        client.create_tag(
            name="foo.four",
            description="Foo Bar",
            tag_type="test",
            tag_metadata={"foo": "bar"},
        )
        tag = client.tag("foo.four")
        node = client.source("default.repair_orders")
        node.tags.append(tag)
        node.save()
        repull_node = client.source("default.repair_orders")
        assert [tag.to_dict() for tag in repull_node.tags] == [tag.to_dict()]

    def test_list_nodes_with_tags(self, client):
        """
        Test that we can list nodes with tags.
        """
        # create some tags
        tag_foo = client.create_tag(
            name="foo.five",
            description="Foo",
            tag_type="test",
            tag_metadata={"foo": "bar"},
        )
        tag_bar = client.create_tag(
            name="bar",
            description="Bar",
            tag_type="test",
            tag_metadata={"foo": "bar"},
        )

        # tag some nodes
        node_one = client.source("default.repair_orders")
        node_one.tags.append(tag_foo)
        node_one.tags.append(tag_bar)
        node_one.save()

        node_two = client.metric("default.num_repair_orders")
        node_two.tags.append(tag_foo)
        node_two.save()

        node_three = client.dimension("default.repair_order")
        node_three.tags.append(tag_foo)
        node_three.tags.append(tag_bar)
        node_three.save()

        # list nodes with tags
        nodes_with_foo = client.list_nodes_with_tags(tag_names=["foo.five"])
        nodes_with_foo_and_bar = client.list_nodes_with_tags(
            tag_names=["bar", "foo.five"],
        )

        # evaluate
        with pytest.raises(DJClientException):
            client.list_nodes_with_tags(tag_names=["does-not-exist"])
        assert (
            client.list_nodes_with_tags(tag_names=["does-not-exist"], skip_missing=True)
            == []
        )
        assert set(nodes_with_foo) == set(
            [
                "default.repair_order",
                "default.repair_orders",
                "default.num_repair_orders",
            ],
        )
        assert set(nodes_with_foo_and_bar) == set(
            ["default.repair_orders", "default.repair_order"],
        )
