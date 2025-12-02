"""
Tests for the data API.
"""

from http import HTTPStatus
import pytest
from httpx import AsyncClient

from datajunction_server.internal.access.authorization import AuthorizationService
from datajunction_server.models import access


class DenyAllAuthorizationService(AuthorizationService):
    """
    Custom authorization service that denies all access.
    """

    name = "deny_all"

    def authorize(self, auth_context, requests):
        return [
            access.AccessDecision(request=request, approved=False)
            for request in requests
        ]


class TestDataAccessControl:
    """
    Test the data access control.
    """

    @pytest.mark.asyncio
    async def test_get_metric_data_unauthorized(
        self,
        module__client_with_examples: AsyncClient,
        mocker,
    ) -> None:
        """
        Test retrieving data for a metric
        """

        def get_deny_all_service():
            return DenyAllAuthorizationService()

        mocker.patch(
            "datajunction_server.internal.access.authorization.get_authorization_service",
            get_deny_all_service,
        )
        response = await module__client_with_examples.get("/data/basic.num_comments/")
        data = response.json()
        assert "Access denied to" in data["message"]
        assert "basic.num_comments" in data["message"]
        assert response.status_code == HTTPStatus.FORBIDDEN

    @pytest.mark.asyncio
    async def test_sql_with_filters_orderby_no_access(
        self,
        module__client_with_examples: AsyncClient,
        mocker,
    ):
        """
        Test ``GET /sql/{node_name}/`` with various filters and dimensions using a
        version of the DJ roads database with namespaces.
        """

        def get_deny_all_service():
            return DenyAllAuthorizationService()

        mocker.patch(
            "datajunction_server.internal.access.authorization.get_authorization_service",
            get_deny_all_service,
        )

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
        assert "Access denied to" in data["message"]
        assert "foo.bar" in data["message"]
        assert response.status_code == HTTPStatus.FORBIDDEN
