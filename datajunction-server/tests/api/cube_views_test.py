"""
Tests for cube view DDL generation and lifecycle hooks.
"""

from unittest import mock

import pytest
import pytest_asyncio
from httpx import AsyncClient

from datajunction_server.instrumentation.provider import (
    MetricsProvider,
    get_metrics_provider,
    set_metrics_provider,
)
from datajunction_server.utils import get_query_service_client


class _SpyProvider(MetricsProvider):
    """Records counter/timer calls so tests can assert against them."""

    def __init__(self) -> None:
        self.counters: list[tuple] = []
        self.timers: list[tuple] = []

    def counter(self, name, value=1, tags=None):
        self.counters.append((name, value, tags))

    def gauge(self, name, value, tags=None):  # pragma: no cover
        pass

    def timer(self, name, value_ms, tags=None):
        self.timers.append((name, value_ms, tags))


@pytest.fixture
def spy_metrics():
    original = get_metrics_provider()
    spy = _SpyProvider()
    set_metrics_provider(spy)
    yield spy
    set_metrics_provider(original)


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

    @pytest.mark.asyncio
    async def test_view_ddl_materialized_cube(
        self,
        module__client_with_roads: AsyncClient,
    ):
        """When a cube has an availability state, versioned DDL is SELECT * FROM the materialized table."""
        cube_name = "default.materialized_view_cube"
        response = await module__client_with_roads.post(
            "/nodes/cube/",
            json={
                "metrics": ["default.num_repair_orders"],
                "dimensions": ["default.hard_hat.country"],
                "description": "Materialized cube for view DDL test",
                "mode": "published",
                "name": cube_name,
            },
        )
        assert response.status_code == 201, response.json()

        availability = await module__client_with_roads.post(
            f"/data/{cube_name}/availability/",
            json={
                "catalog": "default",
                "schema_": "roads",
                "table": "materialized_view_cube_table",
                "valid_through_ts": 20260101,
            },
        )
        assert availability.status_code == 200, availability.json()

        response = await module__client_with_roads.get(
            f"/cubes/{cube_name}/view-ddl",
        )
        assert response.status_code == 200

        versioned = "default.dj_views.default_materialized_view_cube_v1_0"
        unversioned = "default.dj_views.default_materialized_view_cube"
        materialized_table = "default.roads.materialized_view_cube_table"
        assert response.json() == {
            "spark": {
                "versioned_view_name": versioned,
                "unversioned_view_name": unversioned,
                "versioned_ddl": (
                    f"CREATE OR REPLACE VIEW {versioned} "
                    f"AS SELECT * FROM {materialized_table}"
                ),
                "unversioned_ddl": (
                    f"CREATE OR REPLACE VIEW {unversioned} AS SELECT * FROM {versioned}"
                ),
            },
            "trino": {
                "versioned_view_name": f"{versioned}__trino",
                "unversioned_view_name": f"{unversioned}__trino",
                "versioned_ddl": mock.ANY,
                "unversioned_ddl": (
                    f"CREATE OR REPLACE VIEW {unversioned}__trino "
                    f"AS SELECT * FROM {versioned}__trino"
                ),
            },
            "is_materialized": True,
        }


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

        original_create_view_async = qs_client.create_view_async
        create_view_calls: list[str] = []

        async def tracking_create_view_async(
            view_name,
            query_create,
            request_headers=None,
        ):
            create_view_calls.append(view_name)
            return await original_create_view_async(
                view_name,
                query_create,
                request_headers,
            )

        qs_client.create_view_async = tracking_create_view_async

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
            qs_client.create_view_async = original_create_view_async

    @pytest.mark.asyncio
    async def test_validate_cube_triggers_view_creation(
        self,
        client_with_cube: AsyncClient,
    ):
        """Revalidating a cube re-submits all 4 views."""
        qs_client = client_with_cube.app.dependency_overrides[
            get_query_service_client
        ]()

        original_create_view_async = qs_client.create_view_async
        create_view_calls: list[str] = []

        async def tracking_create_view_async(
            view_name,
            query_create,
            request_headers=None,
        ):
            create_view_calls.append(view_name)
            return await original_create_view_async(
                view_name,
                query_create,
                request_headers,
            )

        qs_client.create_view_async = tracking_create_view_async

        try:
            response = await client_with_cube.post(
                "/nodes/default.view_test_cube/validate/?sync_views=true",
            )
            assert response.status_code == 200, response.json()

            assert sorted(c for c in create_view_calls if "view_test_cube" in c) == [
                SPARK_UNVERSIONED,
                TRINO_UNVERSIONED,
                SPARK_VERSIONED,
                TRINO_VERSIONED,
            ]
        finally:
            qs_client.create_view_async = original_create_view_async

    @pytest.mark.asyncio
    async def test_validate_non_cube_with_sync_views_does_not_create_views(
        self,
        module__client_with_roads: AsyncClient,
    ):
        """sync_views=true on a non-cube node still skips view creation."""
        qs_client = module__client_with_roads.app.dependency_overrides[
            get_query_service_client
        ]()

        original_create_view_async = qs_client.create_view_async
        create_view_calls: list[str] = []

        async def tracking_create_view_async(
            view_name,
            query_create,
            request_headers=None,
        ):
            create_view_calls.append(view_name)
            return await original_create_view_async(
                view_name,
                query_create,
                request_headers,
            )

        qs_client.create_view_async = tracking_create_view_async

        try:
            response = await module__client_with_roads.post(
                "/nodes/default.num_repair_orders/validate/?sync_views=true",
            )
            assert response.status_code == 200, response.json()

            assert create_view_calls == []
        finally:
            qs_client.create_view_async = original_create_view_async

    @pytest.mark.asyncio
    async def test_validate_cube_without_sync_views_does_not_create_views(
        self,
        client_with_cube: AsyncClient,
    ):
        """Revalidating without sync_views=true does NOT create views."""
        qs_client = client_with_cube.app.dependency_overrides[
            get_query_service_client
        ]()

        original_create_view_async = qs_client.create_view_async
        create_view_calls: list[str] = []

        async def tracking_create_view_async(
            view_name,
            query_create,
            request_headers=None,
        ):
            create_view_calls.append(view_name)
            return await original_create_view_async(
                view_name,
                query_create,
                request_headers,
            )

        qs_client.create_view_async = tracking_create_view_async

        try:
            response = await client_with_cube.post(
                "/nodes/default.view_test_cube/validate/",
            )
            assert response.status_code == 200, response.json()

            cube_calls = [c for c in create_view_calls if "view_test_cube" in c]
            assert len(cube_calls) == 0, (
                f"Expected no create_view calls without sync_views, "
                f"got: {create_view_calls}"
            )
        finally:
            qs_client.create_view_async = original_create_view_async

    @pytest.mark.asyncio
    async def test_update_cube_triggers_view_creation(
        self,
        client_with_cube: AsyncClient,
    ):
        """Updating a cube re-submits all 4 views."""
        qs_client = client_with_cube.app.dependency_overrides[
            get_query_service_client
        ]()

        original_create_view_async = qs_client.create_view_async
        create_view_calls: list[str] = []

        async def tracking_create_view_async(
            view_name,
            query_create,
            request_headers=None,
        ):
            create_view_calls.append(view_name)
            return await original_create_view_async(
                view_name,
                query_create,
                request_headers,
            )

        qs_client.create_view_async = tracking_create_view_async

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
            qs_client.create_view_async = original_create_view_async

    @pytest.mark.asyncio
    async def test_update_non_cube_does_not_trigger_view_creation(
        self,
        module__client_with_roads: AsyncClient,
    ):
        """Updating a non-cube node does not trigger view creation."""
        qs_client = module__client_with_roads.app.dependency_overrides[
            get_query_service_client
        ]()

        original_create_view_async = qs_client.create_view_async
        create_view_calls: list[str] = []

        async def tracking_create_view_async(
            view_name,
            query_create,
            request_headers=None,
        ):
            create_view_calls.append(view_name)
            return await original_create_view_async(
                view_name,
                query_create,
                request_headers,
            )

        qs_client.create_view_async = tracking_create_view_async

        try:
            response = await module__client_with_roads.patch(
                "/nodes/default.num_repair_orders",
                json={"description": "Updated metric description"},
            )
            assert response.status_code == 200, response.json()

            assert create_view_calls == []
        finally:
            qs_client.create_view_async = original_create_view_async

    @pytest.mark.asyncio
    async def test_view_creation_failure_is_swallowed(
        self,
        module__client_with_roads: AsyncClient,
        spy_metrics,
    ):
        """If create_view raises, the cube create still succeeds and the failure metric is emitted."""
        qs_client = module__client_with_roads.app.dependency_overrides[
            get_query_service_client
        ]()

        original_create_view_async = qs_client.create_view_async

        async def failing_create_view_async(
            view_name,
            query_create,
            request_headers=None,
        ):
            raise RuntimeError("query service exploded")

        qs_client.create_view_async = failing_create_view_async

        try:
            response = await module__client_with_roads.post(
                "/nodes/cube/",
                json={
                    "metrics": ["default.num_repair_orders"],
                    "dimensions": ["default.hard_hat.country"],
                    "description": "Cube whose view creation will fail",
                    "mode": "published",
                    "name": "default.failing_view_cube",
                },
            )
            # Cube create succeeds — view creation is fire-and-forget.
            assert response.status_code == 201, response.json()

            failure_counters = [
                c
                for c in spy_metrics.counters
                if c[0] == "dj.views.create" and c[2].get("status") == "failed"
            ]
            assert failure_counters == [
                (
                    "dj.views.create",
                    1,
                    {"cube_name": "default.failing_view_cube", "status": "failed"},
                ),
            ]
        finally:
            qs_client.create_view_async = original_create_view_async
