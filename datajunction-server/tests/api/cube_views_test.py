"""
Tests for cube view DDL generation and lifecycle hooks.
"""

from unittest import mock

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


SPARK_VERSIONED = "default.dj_views.default_view_test_cube_v1_0"
SPARK_UNVERSIONED = "default.dj_views.default_view_test_cube"
TRINO_VERSIONED = "default.dj_views.default_view_test_cube_v1_0__trino"
TRINO_UNVERSIONED = "default.dj_views.default_view_test_cube__trino"


class TestViewDDLEndpoint:
    """Tests for GET /cubes/{name}/view-ddl"""

    @pytest.mark.asyncio
    async def test_view_ddl_response_shape(
        self,
        client_with_cube: AsyncClient,
    ):
        """The endpoint returns versioned + unversioned views for both dialects."""
        response = await client_with_cube.get(
            "/cubes/default.view_test_cube/view-ddl",
        )
        assert response.status_code == 200

        assert response.json() == {
            "spark": {
                "versioned_view_name": SPARK_VERSIONED,
                "unversioned_view_name": SPARK_UNVERSIONED,
                "versioned_ddl": mock.ANY,
                "unversioned_ddl": (
                    f"CREATE OR REPLACE VIEW {SPARK_UNVERSIONED} "
                    f"AS SELECT * FROM {SPARK_VERSIONED}"
                ),
            },
            "trino": {
                "versioned_view_name": TRINO_VERSIONED,
                "unversioned_view_name": TRINO_UNVERSIONED,
                "versioned_ddl": mock.ANY,
                "unversioned_ddl": (
                    f"CREATE OR REPLACE VIEW {TRINO_UNVERSIONED} "
                    f"AS SELECT * FROM {TRINO_VERSIONED}"
                ),
            },
            "is_materialized": False,
        }
        # versioned_ddl bodies are non-deterministic SQL; pin only the prefix.
        data = response.json()
        assert data["spark"]["versioned_ddl"].startswith(
            f"CREATE OR REPLACE VIEW {SPARK_VERSIONED} AS ",
        )
        assert data["trino"]["versioned_ddl"].startswith(
            f"CREATE OR REPLACE VIEW {TRINO_VERSIONED} AS ",
        )

    @pytest.mark.asyncio
    async def test_view_ddl_nonexistent_cube(
        self,
        client_with_cube: AsyncClient,
    ):
        """Non-existent cubes return a client error."""
        response = await client_with_cube.get(
            "/cubes/default.nonexistent_cube/view-ddl",
        )
        assert response.status_code >= 400


class TestViewCreationOnLifecycle:
    """Tests that view creation is triggered on cube lifecycle events."""

    @pytest.mark.asyncio
    async def test_create_cube_triggers_view_creation(
        self,
        module__client_with_roads: AsyncClient,
    ):
        """Creating a cube submits all 4 views (spark+trino, versioned+unversioned)."""
        qs_client = module__client_with_roads.app.dependency_overrides[
            get_query_service_client
        ]()

        original_create_view = qs_client.create_view
        create_view_calls: list[str] = []

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

            assert sorted(
                c for c in create_view_calls if "lifecycle_test_cube" in c
            ) == [
                "default.dj_views.default_lifecycle_test_cube",
                "default.dj_views.default_lifecycle_test_cube__trino",
                "default.dj_views.default_lifecycle_test_cube_v1_0",
                "default.dj_views.default_lifecycle_test_cube_v1_0__trino",
            ]
        finally:
            qs_client.create_view = original_create_view

    @pytest.mark.asyncio
    async def test_validate_cube_triggers_view_creation(
        self,
        client_with_cube: AsyncClient,
    ):
        """Revalidating a cube re-submits all 4 views."""
        qs_client = client_with_cube.app.dependency_overrides[
            get_query_service_client
        ]()

        original_create_view = qs_client.create_view
        create_view_calls: list[str] = []

        def tracking_create_view(view_name, query_create, request_headers=None):
            create_view_calls.append(view_name)
            return original_create_view(view_name, query_create, request_headers)

        qs_client.create_view = tracking_create_view

        try:
            response = await client_with_cube.post(
                "/nodes/default.view_test_cube/validate/",
            )
            assert response.status_code == 200, response.json()

            assert sorted(c for c in create_view_calls if "view_test_cube" in c) == [
                SPARK_UNVERSIONED,
                TRINO_UNVERSIONED,
                SPARK_VERSIONED,
                TRINO_VERSIONED,
            ]
        finally:
            qs_client.create_view = original_create_view

    @pytest.mark.asyncio
    async def test_update_cube_triggers_view_creation(
        self,
        client_with_cube: AsyncClient,
    ):
        """Updating a cube re-submits all 4 views."""
        qs_client = client_with_cube.app.dependency_overrides[
            get_query_service_client
        ]()

        original_create_view = qs_client.create_view
        create_view_calls: list[str] = []

        def tracking_create_view(view_name, query_create, request_headers=None):
            create_view_calls.append(view_name)
            return original_create_view(view_name, query_create, request_headers)

        qs_client.create_view = tracking_create_view

        try:
            response = await client_with_cube.patch(
                "/nodes/default.view_test_cube",
                json={"description": "Updated description for view test"},
            )
            assert response.status_code == 200, response.json()

            # PATCH bumps the cube revision (v1.0 → v1.1).
            assert sorted(c for c in create_view_calls if "view_test_cube" in c) == [
                SPARK_UNVERSIONED,
                TRINO_UNVERSIONED,
                "default.dj_views.default_view_test_cube_v1_1",
                "default.dj_views.default_view_test_cube_v1_1__trino",
            ]
        finally:
            qs_client.create_view = original_create_view

    @pytest.mark.asyncio
    async def test_update_non_cube_does_not_trigger_view_creation(
        self,
        module__client_with_roads: AsyncClient,
    ):
        """Updating a non-cube node does not trigger view creation."""
        qs_client = module__client_with_roads.app.dependency_overrides[
            get_query_service_client
        ]()

        original_create_view = qs_client.create_view
        create_view_calls: list[str] = []

        def tracking_create_view(view_name, query_create, request_headers=None):
            create_view_calls.append(view_name)
            return original_create_view(view_name, query_create, request_headers)

        qs_client.create_view = tracking_create_view

        try:
            response = await module__client_with_roads.patch(
                "/nodes/default.num_repair_orders",
                json={"description": "Updated metric description"},
            )
            assert response.status_code == 200, response.json()

            assert create_view_calls == []
        finally:
            qs_client.create_view = original_create_view
