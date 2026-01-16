"""Tests for /preaggs API endpoints."""

from unittest.mock import MagicMock

import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from datajunction_server.database.preaggregation import PreAggregation
from datajunction_server.models.preaggregation import WorkflowUrl
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.partition import Partition
from datajunction_server.models.materialization import MaterializationStrategy
from datajunction_server.models.partition import Granularity, PartitionType
from datajunction_server.utils import get_query_service_client


@pytest.fixture
def mock_query_service_client(client_with_build_v3):
    """
    Provides a mock query service client for tests that need to mock QS calls.

    Sets up the FastAPI dependency override so the mock is used by the API.
    """
    mock_client = MagicMock()
    # Override the FastAPI dependency
    client_with_build_v3.app.dependency_overrides[get_query_service_client] = (
        lambda: mock_client
    )
    yield mock_client
    # Clean up - remove the override (will be cleared by client fixture anyway)
    if get_query_service_client in client_with_build_v3.app.dependency_overrides:
        del client_with_build_v3.app.dependency_overrides[get_query_service_client]


@pytest.fixture
def mock_qs_for_preaggs(client_with_preaggs):
    """
    Provides a mock query service client for tests using client_with_preaggs fixture.

    Sets up the FastAPI dependency override so the mock is used by the API.
    """
    mock_client = MagicMock()
    client = client_with_preaggs["client"]
    # Override the FastAPI dependency
    client.app.dependency_overrides[get_query_service_client] = lambda: mock_client
    yield mock_client
    # Clean up
    if get_query_service_client in client.app.dependency_overrides:
        del client.app.dependency_overrides[get_query_service_client]


async def _set_temporal_partition_via_session(
    session: AsyncSession,
    node_name: str,
    column_name: str,
    granularity: str = "day",
    format_: str = "yyyyMMdd",
) -> None:
    """
    Set a temporal partition on a column directly via the session.

    This avoids the API's response serialization issue with module-scoped fixtures.
    """
    # Get the node with columns loaded
    result = await session.execute(
        select(Node)
        .options(
            joinedload(Node.current).options(joinedload(NodeRevision.columns)),
        )
        .where(Node.name == node_name),
    )
    node = result.unique().scalar_one()

    # Find the column
    column = next(
        (c for c in node.current.columns if c.name == column_name),
        None,
    )
    if column is None:
        raise ValueError(f"Column {column_name} not found on node {node_name}")

    # Check if partition already exists
    if column.partition is not None:
        # Update existing partition
        column.partition.type_ = PartitionType.TEMPORAL
        column.partition.granularity = Granularity[granularity.upper()]
        column.partition.format = format_
    else:
        # Create new partition
        partition = Partition(
            column=column,
            type_=PartitionType.TEMPORAL,
            granularity=Granularity[granularity.upper()],
            format=format_,
        )
        session.add(partition)

    await session.commit()


async def _plan_preagg(
    client: AsyncClient,
    metrics: list[str],
    dimensions: list[str],
    strategy: str | None = None,
    schedule: str | None = None,
    lookback_window: str | None = None,
) -> dict:
    """Helper to create a preagg via /preaggs/plan endpoint."""
    payload = {"metrics": metrics, "dimensions": dimensions}
    if strategy:
        payload["strategy"] = strategy  # type: ignore
    if schedule:
        payload["schedule"] = schedule  # type: ignore
    if lookback_window:
        payload["lookback_window"] = lookback_window  # type: ignore
    response = await client.post("/preaggs/plan", json=payload)
    assert response.status_code == 201, f"Failed to plan preagg: {response.text}"
    return response.json()["preaggs"][0]


@pytest_asyncio.fixture
async def client_with_preaggs(
    client_with_build_v3: AsyncClient,
):
    """
    Creates pre-aggregations for testing using BUILD_V3 examples.

    Uses /preaggs/plan API to create preaggs, which is more realistic
    and ensures consistency with the actual API behavior.

    NOTE: Gets session from client's dependency override to ensure we use
    the SAME session that the client uses, avoiding event loop binding issues
    with pytest-xdist in Python 3.11.
    """
    client = client_with_build_v3

    # Get session from the client's dependency override - this ensures we use
    # the same session that the API handlers use, avoiding event loop issues
    from datajunction_server.utils import get_session

    session = client.app.dependency_overrides[get_session]()

    # preagg1: Basic preagg with FULL strategy, single grain
    # total_revenue + total_quantity by status
    preagg1_data = await _plan_preagg(
        client,
        metrics=["v3.total_revenue", "v3.total_quantity"],
        dimensions=["v3.order_details.status"],
        strategy="full",
        schedule="0 0 * * *",
    )

    # preagg2: Multi-grain preagg (status + category)
    # total_revenue + avg_unit_price by status and category
    preagg2_data = await _plan_preagg(
        client,
        metrics=["v3.total_revenue", "v3.avg_unit_price"],
        dimensions=["v3.order_details.status", "v3.product.category"],
        strategy="full",
        schedule="0 * * * *",
    )

    # preagg3: Same grain as preagg1 but different metrics (for grain group hash testing)
    # max_unit_price by status
    preagg3_data = await _plan_preagg(
        client,
        metrics=["v3.max_unit_price"],
        dimensions=["v3.order_details.status"],
        strategy="full",
    )

    # preagg4: No strategy set (for testing "requires strategy" validation)
    # total_revenue by category
    preagg4_data = await _plan_preagg(
        client,
        metrics=["v3.total_revenue"],
        dimensions=["v3.product.category"],
    )

    # preagg5-10: Additional preaggs for tests that modify state
    # These use different dimension combinations to avoid grain_group_hash conflicts
    preagg5_data = await _plan_preagg(
        client,
        metrics=["v3.order_count"],
        dimensions=["v3.order_details.status"],
        strategy="full",
        schedule="0 0 * * *",
    )
    preagg6_data = await _plan_preagg(
        client,
        metrics=["v3.min_unit_price"],
        dimensions=["v3.order_details.status"],
        strategy="full",
        schedule="0 0 * * *",
    )
    preagg7_data = await _plan_preagg(
        client,
        metrics=["v3.total_revenue"],
        dimensions=["v3.customer.customer_id"],
    )
    preagg8_data = await _plan_preagg(
        client,
        metrics=["v3.page_view_count"],
        dimensions=["v3.product.category"],
        strategy="full",
        schedule="0 0 * * *",
    )
    preagg9_data = await _plan_preagg(
        client,
        metrics=["v3.session_count"],
        dimensions=["v3.product.category"],
        strategy="full",
        schedule="0 0 * * *",
    )
    preagg10_data = await _plan_preagg(
        client,
        metrics=["v3.visitor_count"],
        dimensions=["v3.product.category"],
    )

    # Fetch actual PreAggregation objects from DB for tests that need them
    preagg1 = await session.get(PreAggregation, preagg1_data["id"])
    preagg2 = await session.get(PreAggregation, preagg2_data["id"])
    preagg3 = await session.get(PreAggregation, preagg3_data["id"])
    preagg4 = await session.get(PreAggregation, preagg4_data["id"])
    preagg5 = await session.get(PreAggregation, preagg5_data["id"])
    preagg6 = await session.get(PreAggregation, preagg6_data["id"])
    preagg7 = await session.get(PreAggregation, preagg7_data["id"])
    preagg8 = await session.get(PreAggregation, preagg8_data["id"])
    preagg9 = await session.get(PreAggregation, preagg9_data["id"])
    preagg10 = await session.get(PreAggregation, preagg10_data["id"])

    yield {
        "client": client,
        "session": session,
        "preagg1": preagg1,
        "preagg2": preagg2,
        "preagg3": preagg3,
        "preagg4": preagg4,
        "preagg5": preagg5,
        "preagg6": preagg6,
        "preagg7": preagg7,
        "preagg8": preagg8,
        "preagg9": preagg9,
        "preagg10": preagg10,
    }


@pytest.mark.xdist_group(name="preaggregations")
class TestListPreaggregations:
    """Tests for GET /preaggs/ endpoint."""

    @pytest.mark.asyncio
    async def test_list_all_preaggs(self, client_with_preaggs):
        """Test listing all pre-aggregations."""
        client = client_with_preaggs["client"]
        response = await client.get("/preaggs/")

        assert response.status_code == 200
        data = response.json()

        assert "items" in data
        assert "total" in data
        assert "limit" in data
        assert "offset" in data
        assert len(data["items"]) >= 3

    @pytest.mark.asyncio
    async def test_list_preaggs_by_node_name(self, client_with_preaggs):
        """Test filtering pre-aggregations by node name."""
        client = client_with_preaggs["client"]
        response = await client.get(
            "/preaggs/",
            params={"node_name": "v3.order_details"},
        )

        assert response.status_code == 200
        data = response.json()

        assert len(data["items"]) >= 3
        for item in data["items"]:
            assert item["node_name"] == "v3.order_details"

    @pytest.mark.asyncio
    async def test_list_preaggs_by_grain(self, client_with_preaggs):
        """Test filtering pre-aggregations by grain columns."""
        client = client_with_preaggs["client"]
        response = await client.get(
            "/preaggs/",
            params={"grain": "v3.order_details.status"},
        )

        assert response.status_code == 200
        data = response.json()

        # Should get preaggs that have status as a grain column
        matching_items = [
            item
            for item in data["items"]
            if "v3.order_details.status" in item["grain_columns"]
        ]
        assert len(matching_items) >= 2

    @pytest.mark.asyncio
    async def test_list_preaggs_by_measures(self, client_with_preaggs):
        """Test filtering pre-aggregations by measures (superset match)."""
        client = client_with_preaggs["client"]
        response = await client.get(
            "/preaggs/",
            params={"measures": "total_revenue"},
        )

        assert response.status_code == 200
        data = response.json()

        # Should get preaggs that have total_revenue measure
        for item in data["items"]:
            measure_names = {m["name"] for m in item["measures"]}
            assert "total_revenue" in measure_names

    @pytest.mark.asyncio
    async def test_list_preaggs_by_multiple_measures(self, client_with_preaggs):
        """Test filtering by multiple measures (all must be present)."""
        client = client_with_preaggs["client"]
        response = await client.get(
            "/preaggs/",
            params={"measures": "total_revenue,total_quantity"},
        )

        assert response.status_code == 200
        data = response.json()

        # Should only get preagg1 (has both measures)
        for item in data["items"]:
            measure_names = {m["name"] for m in item["measures"]}
            assert "total_revenue" in measure_names
            assert "total_quantity" in measure_names

    @pytest.mark.asyncio
    async def test_list_preaggs_by_grain_group_hash(self, client_with_preaggs):
        """Test filtering by grain group hash."""
        client = client_with_preaggs["client"]
        preagg1 = client_with_preaggs["preagg1"]

        response = await client.get(
            "/preaggs/",
            params={"grain_group_hash": preagg1.grain_group_hash},
        )

        assert response.status_code == 200
        data = response.json()

        # Should get preagg1 and preagg3 (same grain group hash)
        assert len(data["items"]) >= 2
        for item in data["items"]:
            assert item["grain_group_hash"] == preagg1.grain_group_hash

    @pytest.mark.asyncio
    async def test_list_preaggs_by_status_pending(self, client_with_preaggs):
        """Test filtering by status='pending' (no availability)."""
        client = client_with_preaggs["client"]
        response = await client.get(
            "/preaggs/",
            params={"status": "pending"},
        )

        assert response.status_code == 200
        data = response.json()

        # All our test pre-aggs are pending (no availability)
        for item in data["items"]:
            assert item["status"] == "pending"

    @pytest.mark.asyncio
    async def test_list_preaggs_invalid_status(self, client_with_preaggs):
        """Test that invalid status returns error."""
        client = client_with_preaggs["client"]
        response = await client.get(
            "/preaggs/",
            params={"status": "invalid"},
        )

        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_list_preaggs_pagination(self, client_with_preaggs):
        """Test pagination parameters."""
        client = client_with_preaggs["client"]
        response = await client.get(
            "/preaggs/",
            params={"limit": 2, "offset": 0},
        )

        assert response.status_code == 200
        data = response.json()

        assert data["limit"] == 2
        assert data["offset"] == 0
        assert len(data["items"]) <= 2

    @pytest.mark.asyncio
    async def test_list_preaggs_node_not_found(self, client_with_preaggs):
        """Test that non-existent node returns error."""
        client = client_with_preaggs["client"]
        response = await client.get(
            "/preaggs/",
            params={"node_name": "nonexistent.node"},
        )

        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_list_preaggs_by_node_version(self, client_with_preaggs):
        """Test filtering pre-aggregations by specific node version."""
        client = client_with_preaggs["client"]

        # First get the current version
        node_response = await client.get("/nodes/v3.order_details/")
        assert node_response.status_code == 200
        current_version = node_response.json()["version"]

        # Filter by version should return our preaggs
        response = await client.get(
            "/preaggs/",
            params={
                "node_name": "v3.order_details",
                "node_version": current_version,
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) >= 1
        for item in data["items"]:
            assert item["node_version"] == current_version

    @pytest.mark.asyncio
    async def test_list_preaggs_invalid_node_version(self, client_with_preaggs):
        """Test that non-existent node version returns error."""
        client = client_with_preaggs["client"]
        response = await client.get(
            "/preaggs/",
            params={
                "node_name": "v3.order_details",
                "node_version": "v99.99.99",
            },
        )

        assert response.status_code == 404
        assert "Version" in response.json()["message"]

    @pytest.mark.asyncio
    async def test_list_preaggs_include_stale(self, client_with_preaggs):
        """
        Test include_stale parameter returns pre-aggs from all node versions.

        This test:
        1. Gets current pre-aggs for a node
        2. Updates the node to create a new version (making existing pre-aggs stale)
        3. Creates new pre-aggs on the new version
        4. Verifies include_stale=false (default) only returns current version pre-aggs
        5. Verifies include_stale=true returns pre-aggs from all versions
        """
        client = client_with_preaggs["client"]

        # Get current version and pre-aggs for v3.order_details
        node_response = await client.get("/nodes/v3.order_details/")
        assert node_response.status_code == 200
        original_version = node_response.json()["version"]

        # Get pre-aggs for current version (without include_stale)
        current_response = await client.get(
            "/preaggs/",
            params={"node_name": "v3.order_details"},
        )
        assert current_response.status_code == 200
        original_preagg_count = current_response.json()["total"]
        assert original_preagg_count >= 1
        original_preagg_ids = {item["id"] for item in current_response.json()["items"]}

        # Update the node to create a new version (this makes existing pre-aggs stale)
        update_response = await client.patch(
            "/nodes/v3.order_details/",
            json={"description": "Updated description to create new version"},
        )
        assert update_response.status_code == 200
        new_version = update_response.json()["version"]
        assert new_version != original_version, "Node version should have changed"

        # Create a new pre-agg on the new version
        plan_response = await client.post(
            "/preaggs/plan",
            json={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.order_id"],  # Different grain
            },
        )
        assert plan_response.status_code == 201
        new_preagg_id = plan_response.json()["preaggs"][0]["id"]

        # Without include_stale (default): should only return new version pre-aggs
        default_response = await client.get(
            "/preaggs/",
            params={"node_name": "v3.order_details"},
        )
        assert default_response.status_code == 200
        default_ids = {item["id"] for item in default_response.json()["items"]}

        # Should include the new pre-agg
        assert new_preagg_id in default_ids
        # Should NOT include original pre-aggs (they're on old version)
        assert len(default_ids & original_preagg_ids) == 0, (
            "Default (no include_stale) should not return stale pre-aggs"
        )

        # With include_stale=true: should return pre-aggs from ALL versions
        stale_response = await client.get(
            "/preaggs/",
            params={"node_name": "v3.order_details", "include_stale": "true"},
        )
        assert stale_response.status_code == 200
        stale_data = stale_response.json()
        stale_ids = {item["id"] for item in stale_data["items"]}

        # Should include the new pre-agg
        assert new_preagg_id in stale_ids
        # Should also include original pre-aggs (stale ones)
        assert original_preagg_ids <= stale_ids, (
            "include_stale=true should return stale pre-aggs"
        )
        # Total should be more than just current version
        assert stale_data["total"] > len(default_ids)

        # Verify versions are different for stale vs current
        versions_in_response = {item["node_version"] for item in stale_data["items"]}
        assert original_version in versions_in_response
        assert new_version in versions_in_response

    @pytest.mark.asyncio
    async def test_list_preaggs_include_stale_false_explicit(self, client_with_preaggs):
        """Test that include_stale=false behaves same as default (no param)."""
        client = client_with_preaggs["client"]

        # Get pre-aggs with default (no include_stale param)
        default_response = await client.get(
            "/preaggs/",
            params={"node_name": "v3.order_details"},
        )
        assert default_response.status_code == 200

        # Get pre-aggs with explicit include_stale=false
        explicit_response = await client.get(
            "/preaggs/",
            params={"node_name": "v3.order_details", "include_stale": "false"},
        )
        assert explicit_response.status_code == 200

        # Should return same results
        default_ids = {item["id"] for item in default_response.json()["items"]}
        explicit_ids = {item["id"] for item in explicit_response.json()["items"]}
        assert default_ids == explicit_ids


@pytest.mark.xdist_group(name="preaggregations")
class TestGetPreaggregationById:
    """Tests for GET /preaggs/{preagg_id} endpoint."""

    @pytest.mark.asyncio
    async def test_get_preagg_by_id(self, client_with_preaggs):
        """Test getting a pre-aggregation by ID."""
        client = client_with_preaggs["client"]
        preagg1 = client_with_preaggs["preagg1"]

        response = await client.get(f"/preaggs/{preagg1.id}")

        assert response.status_code == 200
        data = response.json()

        assert data["id"] == preagg1.id
        assert data["node_revision_id"] == preagg1.node_revision_id
        assert data["grain_columns"] == preagg1.grain_columns
        assert data["measures"] == [
            {
                "aggregation": "SUM",
                "expr_hash": "83632b779d87",
                "expression": "line_total",
                "merge": "SUM",
                "name": "line_total_sum_e1f61696",
                "rule": {
                    "level": None,
                    "type": "full",
                },
                "used_by_metrics": [
                    {
                        "display_name": "Avg Order Value",
                        "name": "v3.avg_order_value",
                    },
                    {
                        "display_name": "Mom Revenue Change",
                        "name": "v3.mom_revenue_change",
                    },
                    {
                        "display_name": "Revenue Per Customer",
                        "name": "v3.revenue_per_customer",
                    },
                    {
                        "display_name": "Revenue Per Page View",
                        "name": "v3.revenue_per_page_view",
                    },
                    {
                        "display_name": "Revenue Per Visitor",
                        "name": "v3.revenue_per_visitor",
                    },
                    {
                        "display_name": "Total Revenue",
                        "name": "v3.total_revenue",
                    },
                    {
                        "display_name": "Trailing 7D Revenue",
                        "name": "v3.trailing_7d_revenue",
                    },
                    {
                        "display_name": "Trailing Wow Revenue Change",
                        "name": "v3.trailing_wow_revenue_change",
                    },
                    {
                        "display_name": "Wow Revenue Change",
                        "name": "v3.wow_revenue_change",
                    },
                ],
            },
            {
                "aggregation": "SUM",
                "expr_hash": "221d2a4bfdae",
                "expression": "quantity",
                "merge": "SUM",
                "name": "quantity_sum_06b64d2e",
                "rule": {
                    "level": None,
                    "type": "full",
                },
                "used_by_metrics": [
                    {
                        "display_name": "Avg Items Per Order",
                        "name": "v3.avg_items_per_order",
                    },
                    {
                        "display_name": "Total Quantity",
                        "name": "v3.total_quantity",
                    },
                ],
            },
        ]
        assert data["sql"] == preagg1.sql
        assert data["grain_group_hash"] == preagg1.grain_group_hash
        assert data["strategy"] == preagg1.strategy.value
        assert data["schedule"] == preagg1.schedule
        assert data["status"] == "pending"

    @pytest.mark.asyncio
    async def test_get_preagg_not_found(self, client_with_preaggs):
        """Test getting non-existent pre-aggregation returns 404."""
        client = client_with_preaggs["client"]

        response = await client.get("/preaggs/99999999")

        assert response.status_code == 404
        assert "99999999" in response.json()["message"]


@pytest.mark.xdist_group(name="preaggregations")
class TestPreaggregationResponseFields:
    """Tests for pre-aggregation response field completeness."""

    @pytest.mark.asyncio
    async def test_response_contains_all_fields(self, client_with_preaggs):
        """Test that response contains all expected fields."""
        client = client_with_preaggs["client"]
        preagg2 = client_with_preaggs["preagg2"]

        response = await client.get(f"/preaggs/{preagg2.id}")

        assert response.status_code == 200
        data = response.json()

        # Check all expected fields are present
        expected_fields = [
            "id",
            "node_revision_id",
            "node_name",
            "node_version",
            "grain_columns",
            "measures",
            "sql",
            "grain_group_hash",
            "strategy",
            "schedule",
            "lookback_window",
            "status",
            "materialized_table_ref",
            "max_partition",
            "created_at",
            "updated_at",
        ]

        for field in expected_fields:
            assert field in data, f"Missing field: {field}"

    @pytest.mark.asyncio
    async def test_response_materialization_config(self, client_with_preaggs):
        """Test materialization config fields are correctly returned."""
        client = client_with_preaggs["client"]
        preagg2 = client_with_preaggs["preagg2"]

        response = await client.get(f"/preaggs/{preagg2.id}")

        assert response.status_code == 200
        data = response.json()

        # preagg2 is created with FULL strategy and hourly schedule
        assert data["strategy"] == "full"
        assert data["schedule"] == "0 * * * *"
        # lookback_window is not set for FULL strategy
        assert data["lookback_window"] is None


class TestPlanPreaggregations:
    """Tests for POST /preaggs/plan endpoint."""

    @pytest.mark.asyncio
    async def test_plan_preaggs_basic(self, client_with_build_v3: AsyncClient):
        """Test basic plan endpoint creates pre-aggs from metrics + dims."""
        response = await client_with_build_v3.post(
            "/preaggs/plan",
            json={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
            },
        )

        assert response.status_code == 201
        data = response.json()

        assert "preaggs" in data
        assert len(data["preaggs"]) >= 1

        # Check first pre-agg has expected structure
        preagg = data["preaggs"][0]
        assert "id" in preagg
        assert "sql" in preagg
        assert "measures" in preagg
        assert "grain_columns" in preagg
        assert preagg["status"] == "pending"

    @pytest.mark.asyncio
    async def test_plan_preaggs_with_strategy(
        self,
        client_with_build_v3: AsyncClient,
    ):
        """Test plan endpoint with materialization strategy."""
        # Use different dimensions than test_plan_preaggs_basic to avoid conflict
        # Use FULL strategy since source node may not have temporal partition columns
        response = await client_with_build_v3.post(
            "/preaggs/plan",
            json={
                "metrics": ["v3.total_quantity"],
                "dimensions": ["v3.product.category"],
                "strategy": "full",
                "schedule": "0 0 * * *",
            },
        )

        assert response.status_code == 201
        data = response.json()

        assert len(data["preaggs"]) >= 1
        preagg = data["preaggs"][0]
        assert preagg["strategy"] == "full"
        assert preagg["schedule"] == "0 0 * * *"

    @pytest.mark.asyncio
    async def test_plan_preaggs_invalid_strategy(
        self,
        client_with_build_v3: AsyncClient,
    ):
        """Test that invalid strategy returns error."""
        response = await client_with_build_v3.post(
            "/preaggs/plan",
            json={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
                "strategy": "view",  # Not valid for pre-aggs
            },
        )

        # DJInvalidInputException returns 422 Unprocessable Entity
        assert response.status_code == 422
        assert "Invalid strategy" in response.json()["message"]

    @pytest.mark.asyncio
    async def test_plan_preaggs_returns_existing(
        self,
        client_with_build_v3: AsyncClient,
    ):
        """Test that calling plan twice returns existing pre-agg."""
        # First call creates
        response1 = await client_with_build_v3.post(
            "/preaggs/plan",
            json={
                "metrics": ["v3.avg_unit_price"],
                "dimensions": ["v3.customer.customer_id"],
            },
        )
        assert response1.status_code == 201
        data1 = response1.json()
        preagg_id_1 = data1["preaggs"][0]["id"]

        # Second call should return same pre-agg
        response2 = await client_with_build_v3.post(
            "/preaggs/plan",
            json={
                "metrics": ["v3.avg_unit_price"],
                "dimensions": ["v3.customer.customer_id"],
            },
        )
        assert response2.status_code == 201
        data2 = response2.json()
        preagg_id_2 = data2["preaggs"][0]["id"]

        assert preagg_id_1 == preagg_id_2


@pytest.mark.xdist_group(name="preaggregations")
class TestUpdatePreaggregationAvailability:
    """Tests for POST /preaggs/{id}/availability/ endpoint."""

    @pytest.mark.asyncio
    async def test_update_availability_creates_new(self, client_with_preaggs):
        """Test updating availability creates new availability state."""
        client = client_with_preaggs["client"]
        preagg1 = client_with_preaggs["preagg1"]

        response = await client.post(
            f"/preaggs/{preagg1.id}/availability/",
            json={
                "catalog": "analytics",
                "schema": "materialized",
                "table": "preagg_test",
                "valid_through_ts": 1704067200,
                "max_temporal_partition": ["2024", "01", "01"],
            },
        )

        assert response.status_code == 200
        data = response.json()

        assert data["status"] == "active"
        assert data["materialized_table_ref"] == "analytics.materialized.preagg_test"
        assert data["max_partition"] == ["2024", "01", "01"]

    @pytest.mark.asyncio
    async def test_update_availability_not_found(self, client_with_preaggs):
        """Test updating non-existent pre-agg returns 404."""
        client = client_with_preaggs["client"]

        response = await client.post(
            "/preaggs/99999999/availability/",
            json={
                "catalog": "analytics",
                "schema": "materialized",
                "table": "preagg_test",
                "valid_through_ts": 1704067200,
            },
        )

        assert response.status_code == 404


@pytest.mark.xdist_group(name="preaggregations")
class TestMaterializePreaggregation:
    """Tests for POST /preaggs/{id}/materialize endpoint."""

    @pytest.mark.asyncio
    async def test_materialize_preagg_requires_strategy(self, client_with_preaggs):
        """Test that materialization requires strategy to be set."""
        client = client_with_preaggs["client"]
        preagg4 = client_with_preaggs["preagg4"]

        # preagg4 has no strategy set
        response = await client.post(f"/preaggs/{preagg4.id}/materialize")

        assert response.status_code == 422
        assert "Strategy must be set" in response.json()["message"]

    @pytest.mark.asyncio
    async def test_materialize_preagg_not_found(self, client_with_preaggs):
        """Test materializing non-existent pre-agg returns 404."""
        client = client_with_preaggs["client"]

        response = await client.post("/preaggs/99999999/materialize")

        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_materialize_preagg_success(
        self,
        client_with_preaggs,
        mock_qs_for_preaggs,
    ):
        """Test successful materialization call to query service."""
        from datajunction_server.database.materialization import MaterializationStrategy

        client = client_with_preaggs["client"]
        preagg1 = client_with_preaggs["preagg1"]

        # preagg1 already has strategy=FULL and schedule set in fixture

        # Mock the materialize_preagg method on the query service client
        mock_result = {
            "urls": ["http://scheduler/job/123.main"],
            "output_tables": ["analytics.materialized.preagg_test"],
        }
        mock_qs_for_preaggs.materialize_preagg.return_value = mock_result

        response = await client.post(f"/preaggs/{preagg1.id}/materialize")

        assert response.status_code == 200
        data = response.json()

        # Response includes the pre-agg info
        assert data["id"] == preagg1.id
        assert data["strategy"] == "full"

        # Verify query service was called
        mock_qs_for_preaggs.materialize_preagg.assert_called_once()
        call_args = mock_qs_for_preaggs.materialize_preagg.call_args
        mat_input = call_args[0][0]  # First positional arg

        # Verify the input structure
        assert mat_input.preagg_id == preagg1.id
        assert "preagg" in mat_input.output_table
        assert mat_input.strategy == MaterializationStrategy.FULL
        # temporal_partitions should be a list (may be empty if no partitions)
        assert isinstance(mat_input.temporal_partitions, list)


@pytest.mark.xdist_group(name="preaggregations")
class TestUpdatePreaggregationConfig:
    """Tests for PATCH /preaggs/{id}/config endpoint."""

    @pytest.mark.asyncio
    async def test_update_config_strategy(self, client_with_preaggs):
        """Test updating pre-agg strategy via config endpoint."""
        client = client_with_preaggs["client"]
        preagg4 = client_with_preaggs["preagg4"]

        # preagg4 starts with no strategy
        response = await client.patch(
            f"/preaggs/{preagg4.id}/config",
            json={"strategy": "full"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == preagg4.id
        assert data["strategy"] == "full"

    @pytest.mark.asyncio
    async def test_update_config_schedule(self, client_with_preaggs):
        """Test updating pre-agg schedule via config endpoint."""
        client = client_with_preaggs["client"]
        preagg4 = client_with_preaggs["preagg4"]

        response = await client.patch(
            f"/preaggs/{preagg4.id}/config",
            json={"schedule": "0 6 * * *"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["schedule"] == "0 6 * * *"

    @pytest.mark.asyncio
    async def test_update_config_lookback_window(self, client_with_preaggs):
        """Test updating pre-agg lookback window via config endpoint."""
        client = client_with_preaggs["client"]
        preagg4 = client_with_preaggs["preagg4"]

        response = await client.patch(
            f"/preaggs/{preagg4.id}/config",
            json={"lookback_window": "7 days"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["lookback_window"] == "7 days"

    @pytest.mark.asyncio
    async def test_update_config_multiple_fields(self, client_with_preaggs):
        """Test updating multiple config fields at once."""
        client = client_with_preaggs["client"]
        preagg4 = client_with_preaggs["preagg4"]

        response = await client.patch(
            f"/preaggs/{preagg4.id}/config",
            json={
                "strategy": "incremental_time",
                "schedule": "0 0 * * *",
                "lookback_window": "3 days",
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["strategy"] == "incremental_time"
        assert data["schedule"] == "0 0 * * *"
        assert data["lookback_window"] == "3 days"

    @pytest.mark.asyncio
    async def test_update_config_not_found(self, client_with_preaggs):
        """Test updating non-existent pre-agg returns 404."""
        client = client_with_preaggs["client"]

        response = await client.patch(
            "/preaggs/99999999/config",
            json={"strategy": "full"},
        )

        assert response.status_code == 404


@pytest.mark.xdist_group(name="preaggregations")
class TestDeletePreaggWorkflow:
    """Tests for DELETE /preaggs/{id}/workflow endpoint."""

    @pytest.mark.asyncio
    async def test_deactivate_workflow_no_workflow_exists(self, client_with_preaggs):
        """Test deactivating when no workflow exists returns appropriate response."""
        client = client_with_preaggs["client"]
        preagg4 = client_with_preaggs["preagg4"]

        # preagg4 has no workflow URL set
        response = await client.delete(f"/preaggs/{preagg4.id}/workflow")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "none"
        assert data["workflow_url"] is None
        assert "No workflow exists" in data["message"]

    @pytest.mark.asyncio
    async def test_deactivate_workflow_success(
        self,
        client_with_preaggs,
        mock_qs_for_preaggs,
    ):
        """Test successfully deactivating a workflow."""
        client = client_with_preaggs["client"]
        session = client_with_preaggs["session"]
        preagg5 = client_with_preaggs["preagg5"]  # Use dedicated preagg

        # Set up workflow URL on preagg5
        preagg = await session.get(PreAggregation, preagg5.id)
        preagg.workflow_urls = [
            WorkflowUrl(label="scheduled", url="http://scheduler/workflow/test-123"),
        ]
        preagg.workflow_status = "active"
        await session.commit()

        # Mock the deactivate method
        mock_qs_for_preaggs.deactivate_preagg_workflow.return_value = {
            "status": "paused",
        }

        response = await client.delete(f"/preaggs/{preagg5.id}/workflow")

        assert response.status_code == 200
        data = response.json()
        # Implementation clears all workflow state after deactivation
        assert data["status"] == "none"
        assert data["workflow_url"] is None
        assert "deactivated" in data["message"].lower()

        # Verify query service was called
        mock_qs_for_preaggs.deactivate_preagg_workflow.assert_called_once()

    @pytest.mark.asyncio
    async def test_deactivate_workflow_not_found(self, client_with_preaggs):
        """Test deactivating workflow for non-existent pre-agg returns 404."""
        client = client_with_preaggs["client"]

        response = await client.delete("/preaggs/99999999/workflow")

        assert response.status_code == 404


@pytest.mark.xdist_group(name="preaggregations")
class TestBulkDeactivateWorkflows:
    """Tests for DELETE /preaggs/workflows endpoint (bulk deactivation)."""

    @pytest.mark.asyncio
    async def test_bulk_deactivate_no_active_workflows(self, client_with_preaggs):
        """Test bulk deactivate when no active workflows exist returns empty result."""
        client = client_with_preaggs["client"]

        # None of the preaggs have active workflows by default
        response = await client.delete(
            "/preaggs/workflows",
            params={"node_name": "v3.order_details"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["deactivated_count"] == 0
        assert data["deactivated"] == []
        assert "No active workflows found" in data["message"]

    @pytest.mark.asyncio
    async def test_bulk_deactivate_node_not_found(self, client_with_preaggs):
        """Test bulk deactivate for non-existent node returns 404."""
        client = client_with_preaggs["client"]

        response = await client.delete(
            "/preaggs/workflows",
            params={"node_name": "nonexistent.node"},
        )

        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_bulk_deactivate_success(
        self,
        client_with_preaggs,
        mock_qs_for_preaggs,
    ):
        """Test successfully bulk deactivating workflows for a node."""
        client = client_with_preaggs["client"]
        session = client_with_preaggs["session"]
        preagg8 = client_with_preaggs["preagg8"]
        preagg9 = client_with_preaggs["preagg9"]

        # Set up active workflows on preagg8 and preagg9 (both use v3.product.category)
        preagg8_obj = await session.get(PreAggregation, preagg8.id)
        preagg8_obj.workflow_urls = [
            WorkflowUrl(label="scheduled", url="http://scheduler/workflow/preagg8"),
        ]
        preagg8_obj.workflow_status = "active"

        preagg9_obj = await session.get(PreAggregation, preagg9.id)
        preagg9_obj.workflow_urls = [
            WorkflowUrl(label="scheduled", url="http://scheduler/workflow/preagg9"),
        ]
        preagg9_obj.workflow_status = "active"
        await session.commit()

        # Mock the deactivate method
        mock_qs_for_preaggs.deactivate_preagg_workflow.return_value = {
            "status": "paused",
        }

        # Bulk deactivate all workflows for v3.page_views_enriched node
        # (preagg8 and preagg9 are based on metrics from v3.page_views_enriched)
        response = await client.delete(
            "/preaggs/workflows",
            params={"node_name": "v3.page_views_enriched"},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["deactivated_count"] == 2
        assert len(data["deactivated"]) == 2

        # Verify query service was called twice
        assert mock_qs_for_preaggs.deactivate_preagg_workflow.call_count == 2

        # Verify the deactivated IDs include our preaggs
        deactivated_ids = {item["id"] for item in data["deactivated"]}
        assert preagg8.id in deactivated_ids
        assert preagg9.id in deactivated_ids

    @pytest.mark.asyncio
    async def test_bulk_deactivate_stale_only(
        self,
        client_with_preaggs,
        mock_qs_for_preaggs,
    ):
        """Test bulk deactivate with stale_only=true only deactivates stale preaggs."""
        client = client_with_preaggs["client"]
        session = client_with_preaggs["session"]
        preagg10 = client_with_preaggs["preagg10"]

        # Set up active workflow on preagg10
        preagg10_obj = await session.get(PreAggregation, preagg10.id)
        preagg10_obj.workflow_urls = [
            WorkflowUrl(label="scheduled", url="http://scheduler/workflow/preagg10"),
        ]
        preagg10_obj.workflow_status = "active"
        await session.commit()

        # With stale_only=true, since all preaggs are on current revision,
        # nothing should be deactivated
        response = await client.delete(
            "/preaggs/workflows",
            params={"node_name": "v3.page_views_enriched", "stale_only": "true"},
        )

        assert response.status_code == 200
        data = response.json()
        # No stale preaggs exist (all are on current revision)
        assert data["deactivated_count"] == 0
        assert "No active workflows found" in data["message"]

        # Verify query service was NOT called
        mock_qs_for_preaggs.deactivate_preagg_workflow.assert_not_called()

    @pytest.mark.asyncio
    async def test_bulk_deactivate_missing_node_name(self, client_with_preaggs):
        """Test bulk deactivate requires node_name parameter."""
        client = client_with_preaggs["client"]

        response = await client.delete("/preaggs/workflows")

        # FastAPI returns 422 for missing required query params
        assert response.status_code == 422


@pytest.mark.xdist_group(name="preaggregations")
class TestRunPreaggBackfill:
    """Tests for POST /preaggs/{id}/backfill endpoint."""

    @pytest.mark.asyncio
    async def test_backfill_no_workflow(self, client_with_preaggs):
        """Test backfill fails when no workflow exists."""
        client = client_with_preaggs["client"]
        preagg4 = client_with_preaggs["preagg4"]

        # preagg4 has no workflow URL
        response = await client.post(
            f"/preaggs/{preagg4.id}/backfill",
            json={
                "start_date": "2024-01-01",
            },
        )

        assert response.status_code == 422
        assert "workflow created first" in response.json()["message"]

    @pytest.mark.asyncio
    async def test_backfill_success(
        self,
        client_with_preaggs,
        mock_qs_for_preaggs,
    ):
        """Test successfully running a backfill."""
        from datajunction_server.database.preaggregation import PreAggregation

        client = client_with_preaggs["client"]
        session = client_with_preaggs["session"]
        preagg6 = client_with_preaggs["preagg6"]  # Use dedicated preagg

        # Set up workflow URL on preagg6
        preagg = await session.get(PreAggregation, preagg6.id)
        preagg.workflow_urls = [
            WorkflowUrl(label="scheduled", url="http://scheduler/workflow/test-123"),
        ]
        preagg.workflow_status = "active"
        await session.commit()

        # Mock the backfill method
        mock_qs_for_preaggs.run_preagg_backfill.return_value = {
            "job_url": "http://scheduler/jobs/backfill-456",
        }

        response = await client.post(
            f"/preaggs/{preagg6.id}/backfill",
            json={
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["job_url"] == "http://scheduler/jobs/backfill-456"
        assert data["start_date"] == "2024-01-01"
        assert data["end_date"] == "2024-01-31"
        assert data["status"] == "running"

        # Verify query service was called with correct input
        mock_qs_for_preaggs.run_preagg_backfill.assert_called_once()
        call_args = mock_qs_for_preaggs.run_preagg_backfill.call_args
        backfill_input = call_args[0][0]
        assert backfill_input.preagg_id == preagg6.id
        assert "preagg" in backfill_input.output_table

    @pytest.mark.asyncio
    async def test_backfill_not_found(self, client_with_preaggs):
        """Test backfill for non-existent pre-agg returns 404."""
        client = client_with_preaggs["client"]

        response = await client.post(
            "/preaggs/99999999/backfill",
            json={"start_date": "2024-01-01"},
        )

        assert response.status_code == 404


@pytest.mark.xdist_group(name="preaggregations")
class TestListPreaggregationsGrainSuperset:
    """Tests for grain superset mode in list_preaggs endpoint."""

    @pytest.mark.asyncio
    async def test_list_preaggs_grain_superset_mode(self, client_with_preaggs):
        """
        Test grain_mode=superset returns pre-aggs at finer grain.

        Fixture has:
        - preagg1: grain = ["v3.order_details.status"]
        - preagg2: grain = ["v3.order_details.status", "v3.product.category"] (finer)
        - preagg3: grain = ["v3.order_details.status"]
        - preagg4: grain = ["v3.product.category"]

        Requesting grain="v3.order_details.status" with mode="superset" should return
        preagg1, preagg2, and preagg3 (all contain status).
        """
        client = client_with_preaggs["client"]

        # Filter by node_name to isolate our test preaggs
        response = await client.get(
            "/preaggs/",
            params={
                "node_name": "v3.order_details",
                "grain": "v3.order_details.status",
                "grain_mode": "superset",
            },
        )

        assert response.status_code == 200
        data = response.json()

        # Should get preagg1, preagg2, preagg3 - all have status
        # preagg2 has finer grain (status + category) but still matches
        matching_ids = {item["id"] for item in data["items"]}
        preagg1 = client_with_preaggs["preagg1"]
        preagg2 = client_with_preaggs["preagg2"]
        preagg3 = client_with_preaggs["preagg3"]
        preagg4 = client_with_preaggs["preagg4"]

        assert preagg1.id in matching_ids, "preagg1 (exact match) should be included"
        assert preagg2.id in matching_ids, "preagg2 (finer grain) should be included"
        assert preagg3.id in matching_ids, "preagg3 (exact match) should be included"
        assert preagg4.id not in matching_ids, (
            "preagg4 (different grain) should NOT be included"
        )

    @pytest.mark.asyncio
    async def test_list_preaggs_exact_vs_superset(self, client_with_preaggs):
        """
        Test that exact mode excludes finer-grained pre-aggs that superset would include.
        """
        client = client_with_preaggs["client"]

        # Exact mode - filter by node_name to isolate our test preaggs
        exact_response = await client.get(
            "/preaggs/",
            params={
                "node_name": "v3.order_details",
                "grain": "v3.order_details.status",
                "grain_mode": "exact",
            },
        )
        assert exact_response.status_code == 200
        exact_ids = {item["id"] for item in exact_response.json()["items"]}

        # Superset mode
        superset_response = await client.get(
            "/preaggs/",
            params={
                "node_name": "v3.order_details",
                "grain": "v3.order_details.status",
                "grain_mode": "superset",
            },
        )
        assert superset_response.status_code == 200
        superset_ids = {item["id"] for item in superset_response.json()["items"]}

        # Superset should include everything exact includes
        assert exact_ids <= superset_ids

        # Superset should also include preagg2 (finer grain)
        preagg2 = client_with_preaggs["preagg2"]
        assert preagg2.id in superset_ids
        assert preagg2.id not in exact_ids


@pytest.mark.xdist_group(name="preaggregations")
class TestAvailabilityMerge:
    """Tests for availability temporal partition merge logic."""

    @pytest.mark.asyncio
    async def test_availability_merge_extends_max_partition(self, client_with_preaggs):
        """Test that posting new max partition extends the range."""
        client = client_with_preaggs["client"]
        preagg3 = client_with_preaggs["preagg3"]

        # First availability post
        response1 = await client.post(
            f"/preaggs/{preagg3.id}/availability/",
            json={
                "catalog": "analytics",
                "schema": "mat",
                "table": "test_merge",
                "valid_through_ts": 1704067200,
                "min_temporal_partition": ["2024", "01", "01"],
                "max_temporal_partition": ["2024", "01", "15"],
            },
        )
        assert response1.status_code == 200
        data1 = response1.json()
        # Response model only exposes max_partition, not min_partition
        assert data1["max_partition"] == ["2024", "01", "15"]

        # Second availability post - extends max
        response2 = await client.post(
            f"/preaggs/{preagg3.id}/availability/",
            json={
                "catalog": "analytics",
                "schema": "mat",
                "table": "test_merge",
                "valid_through_ts": 1704153600,
                "max_temporal_partition": ["2024", "01", "31"],
            },
        )
        assert response2.status_code == 200
        data2 = response2.json()

        # Max should be extended (min preserved internally but not exposed in response)
        assert data2["max_partition"] == ["2024", "01", "31"]

    @pytest.mark.asyncio
    async def test_availability_merge_preserves_max_when_updating_min(
        self,
        client_with_preaggs,
    ):
        """Test that posting earlier min partition preserves max partition."""
        client = client_with_preaggs["client"]
        preagg4 = client_with_preaggs["preagg4"]

        # First availability post
        response1 = await client.post(
            f"/preaggs/{preagg4.id}/availability/",
            json={
                "catalog": "analytics",
                "schema": "mat",
                "table": "test_merge2",
                "valid_through_ts": 1704067200,
                "min_temporal_partition": ["2024", "06", "01"],
                "max_temporal_partition": ["2024", "06", "30"],
            },
        )
        assert response1.status_code == 200
        data1 = response1.json()
        assert data1["max_partition"] == ["2024", "06", "30"]

        # Second post - extends min backwards (max should stay same)
        response2 = await client.post(
            f"/preaggs/{preagg4.id}/availability/",
            json={
                "catalog": "analytics",
                "schema": "mat",
                "table": "test_merge2",
                "valid_through_ts": 1704153600,
                "min_temporal_partition": ["2024", "01", "01"],
            },
        )
        assert response2.status_code == 200
        data2 = response2.json()

        # Max should be preserved from first post
        assert data2["max_partition"] == ["2024", "06", "30"]

    @pytest.mark.asyncio
    async def test_availability_different_table_creates_new(self, client_with_preaggs):
        """Test that posting to different table creates new availability, not merge."""
        client = client_with_preaggs["client"]
        preagg2 = client_with_preaggs["preagg2"]

        # First availability
        response1 = await client.post(
            f"/preaggs/{preagg2.id}/availability/",
            json={
                "catalog": "analytics",
                "schema": "mat",
                "table": "table_v1",
                "valid_through_ts": 1704067200,
                "max_temporal_partition": ["2024", "01", "15"],
            },
        )
        assert response1.status_code == 200
        assert response1.json()["materialized_table_ref"] == "analytics.mat.table_v1"

        # Second availability to different table - should replace
        response2 = await client.post(
            f"/preaggs/{preagg2.id}/availability/",
            json={
                "catalog": "analytics",
                "schema": "mat",
                "table": "table_v2",
                "valid_through_ts": 1704153600,
                "max_temporal_partition": ["2024", "02", "15"],
            },
        )
        assert response2.status_code == 200
        data2 = response2.json()

        # Should be the new table reference
        assert data2["materialized_table_ref"] == "analytics.mat.table_v2"
        assert data2["max_partition"] == ["2024", "02", "15"]


@pytest.mark.xdist_group(name="preaggregations")
class TestIncrementalTimeValidation:
    """Tests for INCREMENTAL_TIME strategy validation requiring temporal partition columns."""

    @pytest.mark.asyncio
    async def test_plan_incremental_no_temporal_columns(
        self,
        client_with_build_v3: AsyncClient,
    ):
        """
        Test that planning with INCREMENTAL_TIME strategy fails
        when source node lacks temporal partition columns.

        The v3.order_details node doesn't have temporal partition columns by default,
        so INCREMENTAL_TIME should be rejected.
        """
        response = await client_with_build_v3.post(
            "/preaggs/plan",
            json={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "strategy": "incremental_time",
            },
        )

        assert response.status_code == 422
        data = response.json()
        assert "INCREMENTAL_TIME" in data["message"]
        assert "temporal partition columns" in data["message"]

    @pytest.mark.asyncio
    async def test_materialize_incremental_no_temporal_columns(
        self,
        client_with_preaggs,
    ):
        """
        Test that materializing with INCREMENTAL_TIME strategy fails
        when source node lacks temporal partition columns.
        """
        client = client_with_preaggs["client"]
        session = client_with_preaggs["session"]
        preagg7 = client_with_preaggs["preagg7"]  # Use dedicated preagg

        # Set strategy to INCREMENTAL_TIME on preagg7
        from datajunction_server.database.preaggregation import PreAggregation

        preagg = await session.get(PreAggregation, preagg7.id)
        preagg.strategy = MaterializationStrategy.INCREMENTAL_TIME
        preagg.schedule = "0 0 * * *"
        await session.commit()

        # Try to materialize - should fail
        response = await client.post(f"/preaggs/{preagg7.id}/materialize")

        assert response.status_code == 422
        data = response.json()
        assert "INCREMENTAL_TIME" in data["message"]
        assert "temporal partition columns" in data["message"]


@pytest.mark.xdist_group(name="preaggregations")
class TestQueryServiceExceptionHandling:
    """Tests for error handling when query service calls fail."""

    @pytest.mark.asyncio
    async def test_materialize_query_service_failure(
        self,
        client_with_preaggs,
        mock_qs_for_preaggs,
    ):
        """Test error handling when materialize_preagg call fails."""
        client = client_with_preaggs["client"]
        preagg1 = client_with_preaggs["preagg1"]

        # Mock query service to raise an exception
        mock_qs_for_preaggs.materialize_preagg.side_effect = Exception(
            "Connection refused",
        )

        response = await client.post(f"/preaggs/{preagg1.id}/materialize")

        assert response.status_code == 500
        data = response.json()
        assert "Failed to create workflow" in data["message"]
        assert "Connection refused" in data["message"]

    @pytest.mark.asyncio
    async def test_deactivate_workflow_query_service_failure(
        self,
        client_with_preaggs,
        mock_qs_for_preaggs,
    ):
        """Test error handling when deactivate_preagg_workflow call fails."""
        from datajunction_server.database.preaggregation import PreAggregation

        client = client_with_preaggs["client"]
        session = client_with_preaggs["session"]
        preagg8 = client_with_preaggs["preagg8"]  # Use dedicated preagg

        # Set up workflow URL
        preagg = await session.get(PreAggregation, preagg8.id)
        preagg.workflow_urls = [
            WorkflowUrl(label="scheduled", url="http://scheduler/workflow/test-123"),
        ]
        preagg.workflow_status = "active"
        await session.commit()

        # Mock query service to raise an exception
        mock_qs_for_preaggs.deactivate_preagg_workflow.side_effect = Exception(
            "Service unavailable",
        )

        response = await client.delete(f"/preaggs/{preagg8.id}/workflow")

        assert response.status_code == 500
        data = response.json()
        assert "Failed to deactivate workflow" in data["message"]
        assert "Service unavailable" in data["message"]

    @pytest.mark.asyncio
    async def test_backfill_query_service_failure(
        self,
        client_with_preaggs,
        mock_qs_for_preaggs,
    ):
        """Test error handling when run_preagg_backfill call fails."""
        from datajunction_server.database.preaggregation import PreAggregation

        client = client_with_preaggs["client"]
        session = client_with_preaggs["session"]
        preagg9 = client_with_preaggs["preagg9"]  # Use dedicated preagg

        # Set up workflow URL
        preagg = await session.get(PreAggregation, preagg9.id)
        preagg.workflow_urls = [
            WorkflowUrl(label="scheduled", url="http://scheduler/workflow/test-123"),
        ]
        preagg.workflow_status = "active"
        await session.commit()

        # Mock query service to raise an exception
        mock_qs_for_preaggs.run_preagg_backfill.side_effect = Exception(
            "Backfill service down",
        )

        response = await client.post(
            f"/preaggs/{preagg9.id}/backfill",
            json={"start_date": "2024-01-01"},
        )

        assert response.status_code == 500
        data = response.json()
        assert "Failed to run backfill" in data["message"]
        assert "Backfill service down" in data["message"]


@pytest.mark.xdist_group(name="preaggregations")
class TestWorkflowUrlExtraction:
    """Tests for workflow URL extraction and fallback logic in materialize."""

    @pytest.mark.asyncio
    async def test_materialize_extracts_main_workflow_url(
        self,
        client_with_preaggs,
        mock_qs_for_preaggs,
    ):
        """Test that .main workflow URL is extracted from response."""
        from datajunction_server.database.preaggregation import PreAggregation

        client = client_with_preaggs["client"]
        session = client_with_preaggs["session"]
        preagg1 = client_with_preaggs["preagg1"]

        # Mock returns multiple URLs - should extract .main
        mock_result = {
            "urls": [
                "http://scheduler/workflow/test.trigger",
                "http://scheduler/workflow/test.main",
                "http://scheduler/workflow/test.cleanup",
            ],
            "output_tables": ["analytics.materialized.preagg_test"],
        }
        mock_qs_for_preaggs.materialize_preagg.return_value = mock_result

        response = await client.post(f"/preaggs/{preagg1.id}/materialize")

        assert response.status_code == 200

        # Check all workflow URLs are stored with appropriate labels
        await session.refresh(preagg1)
        preagg = await session.get(PreAggregation, preagg1.id)
        # Implementation stores ALL URLs with labels based on URL patterns
        assert preagg.workflow_urls == [
            WorkflowUrl(label="workflow", url="http://scheduler/workflow/test.trigger"),
            WorkflowUrl(label="scheduled", url="http://scheduler/workflow/test.main"),
            WorkflowUrl(label="workflow", url="http://scheduler/workflow/test.cleanup"),
        ]
        assert preagg.workflow_status == "active"

    @pytest.mark.asyncio
    async def test_materialize_fallback_to_first_url(
        self,
        client_with_preaggs,
        mock_qs_for_preaggs,
    ):
        """Test fallback to first URL when no .main URL found."""
        from datajunction_server.database.preaggregation import PreAggregation

        client = client_with_preaggs["client"]
        session = client_with_preaggs["session"]
        preagg3 = client_with_preaggs["preagg3"]

        # Mock returns URLs without .main - should fallback to first
        mock_result = {
            "urls": [
                "http://scheduler/workflow/test-fallback",
                "http://scheduler/workflow/test-other",
            ],
            "output_tables": ["analytics.materialized.preagg_test"],
        }
        mock_qs_for_preaggs.materialize_preagg.return_value = mock_result

        response = await client.post(f"/preaggs/{preagg3.id}/materialize")

        assert response.status_code == 200

        # Check all workflow URLs are stored with labels based on URL patterns
        # When no .main URL found, all URLs get 'workflow' label
        await session.refresh(preagg3)
        preagg = await session.get(PreAggregation, preagg3.id)
        assert preagg.workflow_urls == [
            WorkflowUrl(
                label="workflow",
                url="http://scheduler/workflow/test-fallback",
            ),
            WorkflowUrl(
                label="workflow",
                url="http://scheduler/workflow/test-other",
            ),
        ]

    @pytest.mark.asyncio
    async def test_materialize_sets_default_schedule_when_none(
        self,
        client_with_preaggs,
        mock_qs_for_preaggs,
    ):
        """Test that default schedule is set during materialization if not configured."""
        from datajunction_server.database.preaggregation import PreAggregation

        client = client_with_preaggs["client"]
        session = client_with_preaggs["session"]
        preagg10 = client_with_preaggs["preagg10"]  # Use dedicated preagg

        # Set strategy but leave schedule as None
        preagg = await session.get(PreAggregation, preagg10.id)
        preagg.strategy = MaterializationStrategy.FULL
        preagg.schedule = None  # Explicitly None
        await session.commit()

        # Mock query service
        mock_result = {
            "urls": ["http://scheduler/workflow/test.main"],
            "output_tables": ["analytics.materialized.preagg_test"],
        }
        mock_qs_for_preaggs.materialize_preagg.return_value = mock_result

        response = await client.post(f"/preaggs/{preagg10.id}/materialize")

        assert response.status_code == 200

        # Check that schedule was set to default
        await session.refresh(preagg10)
        preagg = await session.get(PreAggregation, preagg10.id)
        assert preagg.schedule is not None
        # Default schedule from API is "0 0 * * *"
        assert preagg.schedule == "0 0 * * *"

    @pytest.mark.asyncio
    async def test_materialize_returns_all_urls(
        self,
        client_with_preaggs,
        mock_qs_for_preaggs,
    ):
        """Test that response includes all workflow URLs from query service."""
        client = client_with_preaggs["client"]
        preagg1 = client_with_preaggs["preagg1"]

        # Mock returns multiple URLs
        all_urls = [
            "http://scheduler/workflow/test.trigger",
            "http://scheduler/workflow/test.main",
            "http://scheduler/workflow/test.cleanup",
        ]
        mock_result = {
            "urls": all_urls,
            "output_tables": ["analytics.materialized.preagg_test"],
        }
        mock_qs_for_preaggs.materialize_preagg.return_value = mock_result

        response = await client.post(f"/preaggs/{preagg1.id}/materialize")

        assert response.status_code == 200
        data = response.json()

        # Response should include all workflow URLs as labeled objects
        assert "workflow_urls" in data
        assert data["workflow_urls"] == [
            {"label": "workflow", "url": "http://scheduler/workflow/test.trigger"},
            {"label": "scheduled", "url": "http://scheduler/workflow/test.main"},
            {"label": "workflow", "url": "http://scheduler/workflow/test.cleanup"},
        ]

    @pytest.mark.asyncio
    async def test_materialize_handles_new_workflow_urls_format(
        self,
        client_with_preaggs,
        mock_qs_for_preaggs,
    ):
        """
        Test that new workflow_urls format is correctly stored.
        """
        client = client_with_preaggs["client"]
        session = client_with_preaggs["session"]
        preagg1 = client_with_preaggs["preagg1"]

        # Mock query service with NEW workflow_urls format
        mock_result = {
            "workflow_urls": [
                {"label": "scheduled", "url": "http://scheduler/scheduled-workflow"},
                {"label": "backfill", "url": "http://scheduler/backfill-workflow"},
            ],
            "status": "active",
        }
        mock_qs_for_preaggs.materialize_preagg.return_value = mock_result

        response = await client.post(f"/preaggs/{preagg1.id}/materialize")
        assert response.status_code == 200

        # Verify workflow_urls are stored correctly with labels from response
        await session.refresh(preagg1)
        preagg = await session.get(PreAggregation, preagg1.id)
        assert preagg.workflow_urls == [
            WorkflowUrl(label="scheduled", url="http://scheduler/scheduled-workflow"),
            WorkflowUrl(label="backfill", url="http://scheduler/backfill-workflow"),
        ]

    @pytest.mark.asyncio
    async def test_materialize_handles_legacy_urls_with_backfill_pattern(
        self,
        client_with_preaggs,
        mock_qs_for_preaggs,
    ):
        """
        Test legacy URLs with .backfill pattern get labeled correctly.
        """
        client = client_with_preaggs["client"]
        session = client_with_preaggs["session"]
        preagg1 = client_with_preaggs["preagg1"]

        # Mock query service with legacy urls format containing .backfill
        mock_result = {
            "urls": [
                "http://scheduler/workflow/test.main",
                "http://scheduler/workflow/test.backfill",  # Should get label "backfill"
            ],
            "output_tables": ["analytics.materialized.preagg_test"],
        }
        mock_qs_for_preaggs.materialize_preagg.return_value = mock_result

        response = await client.post(f"/preaggs/{preagg1.id}/materialize")
        assert response.status_code == 200

        # Verify backfill URL gets correct label
        await session.refresh(preagg1)
        preagg = await session.get(PreAggregation, preagg1.id)
        # Find the backfill URL
        backfill_urls = [wf for wf in preagg.workflow_urls if wf.label == "backfill"]
        assert len(backfill_urls) == 1
        assert ".backfill" in backfill_urls[0].url

    @pytest.mark.asyncio
    async def test_materialize_handles_legacy_urls_with_adhoc_pattern(
        self,
        client_with_preaggs,
        mock_qs_for_preaggs,
    ):
        """
        Test legacy URLs with adhoc pattern get labeled as backfill.
        """
        client = client_with_preaggs["client"]
        session = client_with_preaggs["session"]
        preagg1 = client_with_preaggs["preagg1"]

        # Mock query service with legacy urls containing adhoc
        mock_result = {
            "urls": [
                "http://scheduler/scheduled-workflow",
                "http://scheduler/Adhoc-workflow",  # Should get label "backfill" (case insensitive)
            ],
            "output_tables": ["analytics.materialized.preagg_test"],
        }
        mock_qs_for_preaggs.materialize_preagg.return_value = mock_result

        response = await client.post(f"/preaggs/{preagg1.id}/materialize")
        assert response.status_code == 200

        # Verify adhoc URL gets backfill label
        await session.refresh(preagg1)
        preagg = await session.get(PreAggregation, preagg1.id)
        # Find the backfill URL (adhoc should be labeled as backfill)
        backfill_urls = [wf for wf in preagg.workflow_urls if wf.label == "backfill"]
        assert len(backfill_urls) == 1
        assert "adhoc" in backfill_urls[0].url.lower()


@pytest.mark.xdist_group(name="preaggregations")
class TestIncrementalTimeMaterialization:
    """
    Tests for INCREMENTAL_TIME materialization with temporal partition columns.

    These tests verify that the materialize endpoint correctly handles
    incremental time strategies when the upstream node has temporal partitions.

    Uses the v3 namespace which has dimension links already configured:
    - v3.order_details.order_date -> v3.date.date_id (role: order)
    """

    @pytest.mark.asyncio
    async def test_materialize_incremental_time_with_temporal_partition(
        self,
        client_with_build_v3: AsyncClient,
        session: AsyncSession,
        mock_query_service_client,
    ):
        """
        Test INCREMENTAL_TIME materialization succeeds when node has temporal partitions.

        This test:
        1. Sets up a temporal partition on order_date column
        2. Creates a preagg with INCREMENTAL_TIME strategy via /preaggs/plan
        3. Verifies materialize returns correct temporal partition info
        """
        client = client_with_build_v3

        # Set temporal partition on order_date column via session
        await _set_temporal_partition_via_session(
            session,
            node_name="v3.order_details",
            column_name="order_date",
            granularity="day",
            format_="yyyyMMdd",
        )

        # Create a preagg with INCREMENTAL_TIME strategy via API
        plan_response = await client.post(
            "/preaggs/plan/",
            json={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
                "strategy": "incremental_time",
                "schedule": "0 0 * * *",
                "lookback_window": "3 days",
            },
        )
        assert plan_response.status_code == 201, (
            f"Failed to plan preagg: {plan_response.text}"
        )
        preagg = plan_response.json()["preaggs"][0]
        preagg_id = preagg["id"]

        # Mock query service client's materialize_preagg method
        mock_result = {
            "urls": ["http://scheduler/workflow/incremental.main"],
            "output_tables": ["analytics.preaggs.incremental_test"],
        }
        mock_query_service_client.materialize_preagg.return_value = mock_result

        response = await client.post(f"/preaggs/{preagg_id}/materialize")

        assert response.status_code == 200, f"Materialize failed: {response.text}"
        data = response.json()

        # Verify response structure
        assert data["node_name"] == "v3.order_details"
        assert data["strategy"] == "incremental_time"
        assert data["schedule"] == "0 0 * * *"
        assert data["lookback_window"] == "3 days"
        assert data["workflow_status"] == "active"
        assert data["workflow_urls"] == [
            {"label": "scheduled", "url": "http://scheduler/workflow/incremental.main"},
        ]

    @pytest.mark.asyncio
    async def test_materialize_incremental_passes_temporal_partition_to_query_service(
        self,
        client_with_build_v3: AsyncClient,
        session: AsyncSession,
        mock_query_service_client,
    ):
        """
        Test that temporal partition info is passed to query service during materialization.

        Verifies the PreAggMaterializationInput contains temporal_partitions.
        """
        client = client_with_build_v3

        # Ensure temporal partition is set on order_date column via session
        await _set_temporal_partition_via_session(
            session,
            node_name="v3.order_details",
            column_name="order_date",
            granularity="day",
            format_="yyyyMMdd",
        )

        # Create a preagg with INCREMENTAL_TIME strategy
        plan_response = await client.post(
            "/preaggs/plan/",
            json={
                "metrics": ["v3.order_count"],
                "dimensions": ["v3.order_details.status"],
                "strategy": "incremental_time",
                "schedule": "0 * * * *",
                "lookback_window": "1 day",
            },
        )
        assert plan_response.status_code == 201
        preagg = plan_response.json()["preaggs"][0]
        preagg_id = preagg["id"]

        # Mock query service and capture the call
        mock_result = {
            "urls": ["http://scheduler/workflow/test.main"],
            "output_tables": ["analytics.preaggs.test"],
        }
        mock_query_service_client.materialize_preagg.return_value = mock_result

        response = await client.post(f"/preaggs/{preagg_id}/materialize")
        assert response.status_code == 200

        # Verify the query service was called with temporal partition info
        mock_query_service_client.materialize_preagg.assert_called_once()
        call_args = mock_query_service_client.materialize_preagg.call_args
        mat_input = call_args[0][0]  # First positional argument

        # Check temporal partitions are included in the input
        assert hasattr(mat_input, "temporal_partitions")
        assert len(mat_input.temporal_partitions) > 0

        # Verify the temporal partition has expected properties
        temporal_partition = mat_input.temporal_partitions[0]
        assert temporal_partition.column_name is not None
        assert temporal_partition.format == "yyyyMMdd"
        assert temporal_partition.granularity == "day"

    @pytest.mark.asyncio
    async def test_temporal_partition_via_dimension_link(
        self,
        client_with_build_v3: AsyncClient,
        session: AsyncSession,
        mock_query_service_client,
    ):
        """
        Test temporal partition resolution via dimension link.

        This tests Strategy 2 in preagg_matcher.py where the temporal column
        (order_date) is linked to a dimension (v3.date), and the grain includes
        that dimension's column (v3.date.date_id).

        The code should:
        1. Find that order_date links to v3.date
        2. Search grain_columns for v3.date.*
        3. Map order_date -> date_id as the output column name
        """
        client = client_with_build_v3

        # Set temporal partition on order_date (which links to v3.date) via session
        await _set_temporal_partition_via_session(
            session,
            node_name="v3.order_details",
            column_name="order_date",
            granularity="day",
            format_="yyyyMMdd",
        )

        # Create preagg with grain including v3.date.date_id (linked dimension)
        # This triggers Strategy 2: temporal col -> dimension link -> grain column
        plan_response = await client.post(
            "/preaggs/plan/",
            json={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.date.date_id[order]"],  # Linked via order_date
                "strategy": "incremental_time",
                "schedule": "0 0 * * *",
                "lookback_window": "3 days",
            },
        )
        assert plan_response.status_code == 201, (
            f"Failed to plan preagg: {plan_response.text}"
        )
        preagg = plan_response.json()["preaggs"][0]
        preagg_id = preagg["id"]

        # Mock query service and capture the call
        mock_result = {
            "urls": ["http://scheduler/workflow/dimension_link.main"],
            "output_tables": ["analytics.preaggs.dimension_link_test"],
        }
        mock_query_service_client.materialize_preagg.return_value = mock_result

        response = await client.post(f"/preaggs/{preagg_id}/materialize")
        assert response.status_code == 200, f"Materialize failed: {response.text}"

        # Verify the query service was called
        mock_query_service_client.materialize_preagg.assert_called_once()
        call_args = mock_query_service_client.materialize_preagg.call_args
        mat_input = call_args[0][0]

        # Check temporal partitions
        assert hasattr(mat_input, "temporal_partitions")
        assert len(mat_input.temporal_partitions) > 0

        # The temporal partition should be mapped via dimension link
        # order_date -> v3.date.date_id -> output column "date_id"
        temporal_partition = mat_input.temporal_partitions[0]
        assert temporal_partition.column_name is not None
        assert temporal_partition.format == "yyyyMMdd"
        assert temporal_partition.granularity == "day"
