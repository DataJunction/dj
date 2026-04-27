"""
Tests for cube view DDL generation and lifecycle hooks.
"""

import pytest
import pytest_asyncio
from httpx import AsyncClient

from datajunction_server.utils import get_query_service_client


@pytest_asyncio.fixture(scope="module")
async def client_with_cube(module__client_with_roads: AsyncClient):
    """
    Create a simple cube for view DDL tests.
    """
    response = await module__client_with_roads.post(
        "/nodes/cube/",
        json={
            "metrics": [
                "default.num_repair_orders",
                "default.total_repair_cost",
            ],
            "dimensions": [
                "default.hard_hat.country",
                "default.hard_hat.state",
            ],
            "description": "Test cube for view DDL",
            "mode": "published",
            "name": "default.view_test_cube",
        },
    )
    assert response.status_code == 201, response.json()
    return module__client_with_roads


class TestViewDDLEndpoint:
    """Tests for GET /cubes/{name}/view-ddl"""

    @pytest.mark.asyncio
    async def test_view_ddl_returns_correct_names(
        self,
        client_with_cube: AsyncClient,
    ):
        """View DDL endpoint returns correctly formatted view names."""
        response = await client_with_cube.get(
            "/cubes/default.view_test_cube/view-ddl",
        )
        assert response.status_code == 200
        data = response.json()

        # Check view names follow the convention
        assert "default_view_test_cube" in data["versioned_view_name"]
        assert "default_view_test_cube" in data["unversioned_view_name"]

        # Versioned should have version suffix, unversioned should not
        assert "_v1_0" in data["versioned_view_name"]
        assert "_v1_0" not in data["unversioned_view_name"]

    @pytest.mark.asyncio
    async def test_view_ddl_contains_create_or_replace(
        self,
        client_with_cube: AsyncClient,
    ):
        """View DDL should contain CREATE OR REPLACE VIEW statements."""
        response = await client_with_cube.get(
            "/cubes/default.view_test_cube/view-ddl",
        )
        assert response.status_code == 200
        data = response.json()

        assert data["versioned_ddl"].startswith("CREATE OR REPLACE VIEW")
        assert data["unversioned_ddl"].startswith("CREATE OR REPLACE VIEW")

    @pytest.mark.asyncio
    async def test_view_ddl_unversioned_points_at_versioned(
        self,
        client_with_cube: AsyncClient,
    ):
        """Unversioned view DDL should SELECT * FROM the versioned view."""
        response = await client_with_cube.get(
            "/cubes/default.view_test_cube/view-ddl",
        )
        assert response.status_code == 200
        data = response.json()

        assert f"SELECT * FROM {data['versioned_view_name']}" in data["unversioned_ddl"]

    @pytest.mark.asyncio
    async def test_view_ddl_not_materialized(
        self,
        client_with_cube: AsyncClient,
    ):
        """Cube without materialization should have is_materialized=False."""
        response = await client_with_cube.get(
            "/cubes/default.view_test_cube/view-ddl",
        )
        assert response.status_code == 200
        data = response.json()
        assert data["is_materialized"] is False

    @pytest.mark.asyncio
    async def test_view_ddl_versioned_contains_measures_sql(
        self,
        client_with_cube: AsyncClient,
    ):
        """Versioned DDL should contain measures SQL (SELECT with aggregations)."""
        response = await client_with_cube.get(
            "/cubes/default.view_test_cube/view-ddl",
        )
        assert response.status_code == 200
        data = response.json()

        # The measures SQL should contain aggregation-related SQL
        versioned_ddl = data["versioned_ddl"].upper()
        assert "SELECT" in versioned_ddl
        assert "FROM" in versioned_ddl

    @pytest.mark.asyncio
    async def test_view_ddl_no_double_v_in_name(
        self,
        client_with_cube: AsyncClient,
    ):
        """View names should not contain double 'v' (e.g. _vv1_0)."""
        response = await client_with_cube.get(
            "/cubes/default.view_test_cube/view-ddl",
        )
        assert response.status_code == 200
        data = response.json()
        assert "_vv" not in data["versioned_view_name"]

    @pytest.mark.asyncio
    async def test_view_ddl_nonexistent_cube(
        self,
        client_with_cube: AsyncClient,
    ):
        """Should return error for a non-existent cube."""
        response = await client_with_cube.get(
            "/cubes/default.nonexistent_cube/view-ddl",
        )
        assert response.status_code >= 400

    @pytest.mark.asyncio
    async def test_view_ddl_with_dialect(
        self,
        client_with_cube: AsyncClient,
    ):
        """View DDL should accept a dialect parameter."""
        response = await client_with_cube.get(
            "/cubes/default.view_test_cube/view-ddl?dialect=trino",
        )
        assert response.status_code == 200

        response = await client_with_cube.get(
            "/cubes/default.view_test_cube/view-ddl?dialect=spark",
        )
        assert response.status_code == 200


class TestViewCreationOnLifecycle:
    """Tests that view creation is triggered on cube lifecycle events."""

    @pytest.mark.asyncio
    async def test_create_cube_triggers_view_creation(
        self,
        module__client_with_roads: AsyncClient,
    ):
        """Creating a cube should trigger background view creation."""
        qs_client = module__client_with_roads.app.dependency_overrides[
            get_query_service_client
        ]()

        # Track create_view calls
        original_create_view = qs_client.create_view
        create_view_calls = []

        def tracking_create_view(view_name, query_create, request_headers=None):
            create_view_calls.append(view_name)
            return original_create_view(view_name, query_create, request_headers)

        qs_client.create_view = tracking_create_view

        try:
            response = await module__client_with_roads.post(
                "/nodes/cube/",
                json={
                    "metrics": ["default.num_repair_orders"],
                    "dimensions": ["default.hard_hat.country"],
                    "description": "Cube to test view lifecycle",
                    "mode": "published",
                    "name": "default.lifecycle_test_cube",
                },
            )
            assert response.status_code == 201, response.json()

            # Background task should have called create_view for both views
            versioned_calls = [
                c for c in create_view_calls if "lifecycle_test_cube_v" in c
            ]
            unversioned_calls = [
                c
                for c in create_view_calls
                if "lifecycle_test_cube" in c
                and "_v" not in c.split("lifecycle_test_cube")[1]
            ]
            assert len(versioned_calls) >= 1, (
                f"Expected versioned view creation, got calls: {create_view_calls}"
            )
            assert len(unversioned_calls) >= 1, (
                f"Expected unversioned view creation, got calls: {create_view_calls}"
            )
        finally:
            qs_client.create_view = original_create_view

    @pytest.mark.asyncio
    async def test_validate_cube_triggers_view_creation(
        self,
        client_with_cube: AsyncClient,
    ):
        """Revalidating a cube should trigger background view creation."""
        qs_client = client_with_cube.app.dependency_overrides[
            get_query_service_client
        ]()

        original_create_view = qs_client.create_view
        create_view_calls = []

        def tracking_create_view(view_name, query_create, request_headers=None):
            create_view_calls.append(view_name)
            return original_create_view(view_name, query_create, request_headers)

        qs_client.create_view = tracking_create_view

        try:
            response = await client_with_cube.post(
                "/nodes/default.view_test_cube/validate/",
            )
            assert response.status_code == 200, response.json()

            # Should have triggered view creation
            cube_calls = [c for c in create_view_calls if "view_test_cube" in c]
            assert len(cube_calls) >= 2, (
                f"Expected at least 2 create_view calls (versioned + unversioned), "
                f"got: {create_view_calls}"
            )
        finally:
            qs_client.create_view = original_create_view

    @pytest.mark.asyncio
    async def test_update_cube_triggers_view_creation(
        self,
        client_with_cube: AsyncClient,
    ):
        """Updating a cube should trigger background view creation."""
        qs_client = client_with_cube.app.dependency_overrides[
            get_query_service_client
        ]()

        original_create_view = qs_client.create_view
        create_view_calls = []

        def tracking_create_view(view_name, query_create, request_headers=None):
            create_view_calls.append(view_name)
            return original_create_view(view_name, query_create, request_headers)

        qs_client.create_view = tracking_create_view

        try:
            # Minor update (description change)
            response = await client_with_cube.patch(
                "/nodes/default.view_test_cube",
                json={
                    "description": "Updated description for view test",
                },
            )
            assert response.status_code == 200, response.json()

            # Should have triggered view creation
            cube_calls = [c for c in create_view_calls if "view_test_cube" in c]
            assert len(cube_calls) >= 2, (
                f"Expected at least 2 create_view calls (versioned + unversioned), "
                f"got: {create_view_calls}"
            )
        finally:
            qs_client.create_view = original_create_view

    @pytest.mark.asyncio
    async def test_update_non_cube_does_not_trigger_view_creation(
        self,
        module__client_with_roads: AsyncClient,
    ):
        """Updating a non-cube node should NOT trigger view creation."""
        qs_client = module__client_with_roads.app.dependency_overrides[
            get_query_service_client
        ]()

        original_create_view = qs_client.create_view
        create_view_calls = []

        def tracking_create_view(view_name, query_create, request_headers=None):
            create_view_calls.append(view_name)
            return original_create_view(view_name, query_create, request_headers)

        qs_client.create_view = tracking_create_view

        try:
            # Update a metric node (not a cube)
            response = await module__client_with_roads.patch(
                "/nodes/default.num_repair_orders",
                json={
                    "description": "Updated metric description",
                },
            )
            assert response.status_code == 200, response.json()

            # Should NOT have triggered any view creation
            assert len(create_view_calls) == 0, (
                f"Expected no create_view calls for non-cube update, "
                f"got: {create_view_calls}"
            )
        finally:
            qs_client.create_view = original_create_view


class TestViewSettings:
    """Tests for view catalog/schema settings."""

    @pytest.mark.asyncio
    async def test_view_settings_used_in_ddl(
        self,
        client_with_cube: AsyncClient,
    ):
        """View DDL should use the configured view_catalog and view_schema."""
        response = await client_with_cube.get(
            "/cubes/default.view_test_cube/view-ddl",
        )
        assert response.status_code == 200
        data = response.json()

        # Default settings: view_catalog="default", view_schema="dj_views"
        assert data["versioned_view_name"].startswith("default.dj_views.")
        assert data["unversioned_view_name"].startswith("default.dj_views.")
