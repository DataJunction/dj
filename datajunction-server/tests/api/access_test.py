"""
Tests for the data API.
"""
import pytest
from httpx import AsyncClient

from datajunction_server.api.main import app
from datajunction_server.internal.access.authorization import validate_access
from datajunction_server.models import access


class TestDataAccessControl:  # pylint: disable=too-few-public-methods
    """
    Test the data access control.
    """

    @pytest.mark.asyncio
    async def test_get_metric_data_unauthorized(
        self,
        module__client_with_examples: AsyncClient,
    ) -> None:
        """
        Test retrieving data for a metric
        """

        def validate_access_override():
            def _validate_access(access_control: access.AccessControl):
                access_control.deny_all()

            return _validate_access

        app.dependency_overrides[validate_access] = validate_access_override
        response = await module__client_with_examples.get("/data/basic.num_comments/")
        data = response.json()
        assert "Authorization of User `dj` for this request failed" in data["message"]
        assert "read:node/basic.num_comments" in data["message"]
        assert "read:node/basic.source.comments" in data["message"]
        assert response.status_code == 403
        app.dependency_overrides.clear()

    @pytest.mark.asyncio
    async def test_sql_with_filters_orderby_no_access(
        self,
        module__client_with_examples: AsyncClient,
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
        response = await module__client_with_examples.get(
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
