"""
Tests for access control across APIs.
"""

from http import HTTPStatus
import pytest
from httpx import AsyncClient

from datajunction_server.internal.access.authorization import AuthorizationService
from datajunction_server.models import access
from datajunction_server.models.access import ResourceType


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


class NamespaceOnlyAuthorizationService(AuthorizationService):
    """
    Authorization service that allows namespace access but denies all node access.
    """

    name = "namespace_only"

    def __init__(self, allowed_namespaces: list[str]):
        self.allowed_namespaces = allowed_namespaces

    def authorize(self, auth_context, requests):
        decisions = []
        for request in requests:
            approved = False
            if request.access_object.resource_type == ResourceType.NAMESPACE:
                # Allow access to specified namespaces
                approved = request.access_object.name in self.allowed_namespaces
            # Deny all NODE access
            decisions.append(access.AccessDecision(request=request, approved=approved))
        return decisions


class PartialNodeAuthorizationService(AuthorizationService):
    """
    Authorization service that allows access to specific namespaces and nodes.
    """

    name = "partial_node"

    def __init__(self, allowed_namespaces: list[str], allowed_nodes: list[str]):
        self.allowed_namespaces = allowed_namespaces
        self.allowed_nodes = allowed_nodes

    def authorize(self, auth_context, requests):
        decisions = []
        for request in requests:
            approved = False
            if request.access_object.resource_type == ResourceType.NAMESPACE:
                approved = request.access_object.name in self.allowed_namespaces
            elif request.access_object.resource_type == ResourceType.NODE:
                approved = request.access_object.name in self.allowed_nodes
            decisions.append(access.AccessDecision(request=request, approved=approved))
        return decisions


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
            "datajunction_server.internal.access.authorization.validator.get_authorization_service",
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
            "datajunction_server.internal.access.authorization.validator.get_authorization_service",
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


class TestNamespaceAccessControl:
    """
    Test access control for the ``GET /namespaces/{namespace}/`` endpoint.
    """

    @pytest.mark.asyncio
    async def test_list_nodes_with_no_namespace_access(
        self,
        module__client_with_examples: AsyncClient,
        mocker,
    ):
        """
        User with no namespace READ access should get empty list.
        """

        def get_deny_all_service():
            return DenyAllAuthorizationService()

        mocker.patch(
            "datajunction_server.internal.access.authorization.validator.get_authorization_service",
            get_deny_all_service,
        )

        response = await module__client_with_examples.get("/namespaces/default/")
        assert response.status_code == HTTPStatus.OK
        data = response.json()
        assert data == []

    @pytest.mark.asyncio
    async def test_list_nodes_with_namespace_access_but_no_node_access(
        self,
        module__client_with_examples: AsyncClient,
        mocker,
    ):
        """
        User with namespace READ access but no node READ access should get empty list.
        """

        def get_namespace_only_service():
            return NamespaceOnlyAuthorizationService(allowed_namespaces=["default"])

        mocker.patch(
            "datajunction_server.internal.access.authorization.validator.get_authorization_service",
            get_namespace_only_service,
        )

        response = await module__client_with_examples.get("/namespaces/default/")
        assert response.status_code == HTTPStatus.OK
        data = response.json()
        assert data == []

    @pytest.mark.asyncio
    async def test_list_nodes_with_partial_node_access(
        self,
        module__client_with_examples: AsyncClient,
        mocker,
    ):
        """
        User with namespace access and partial node access should get filtered list.
        """
        allowed_nodes = [
            "default.repair_orders",
            "default.hard_hat",
        ]

        def get_partial_service():
            return PartialNodeAuthorizationService(
                allowed_namespaces=["default"],
                allowed_nodes=allowed_nodes,
            )

        mocker.patch(
            "datajunction_server.internal.access.authorization.validator.get_authorization_service",
            get_partial_service,
        )

        response = await module__client_with_examples.get("/namespaces/default/")
        assert response.status_code == HTTPStatus.OK
        data = response.json()

        # Should only return the allowed nodes
        returned_names = [node["name"] for node in data]
        assert set(returned_names) == set(allowed_nodes)

    @pytest.mark.asyncio
    async def test_list_nodes_with_full_access(
        self,
        module__client_with_examples: AsyncClient,
    ):
        """
        User with full access (PassthroughAuthorizationService) should get all nodes.
        Default test client uses PassthroughAuthorizationService.
        """
        response = await module__client_with_examples.get("/namespaces/default/")
        assert response.status_code == HTTPStatus.OK
        data = response.json()

        # Should return multiple nodes (the roads example has many)
        assert len(data) > 0
        # Verify we get node details
        assert all("name" in node for node in data)
        assert all(node["name"].startswith("default.") for node in data)
