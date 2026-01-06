"""Tests for /preaggs API endpoints."""

import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.preaggregation import (
    PreAggregation,
    compute_grain_group_hash,
    compute_expression_hash,
)
from datajunction_server.models.decompose import (
    MetricComponent,
    AggregationRule,
    Aggregability,
)
from datajunction_server.models.materialization import MaterializationStrategy


def make_measure(
    name: str,
    expression: str,
    aggregation: str = "SUM",
    merge: str = "SUM",
):
    """Helper to create a measure dict with all required fields (matching MetricComponent)."""
    component = MetricComponent(
        name=name,
        expression=expression,
        aggregation=aggregation,
        merge=merge,
        rule=AggregationRule(type=Aggregability.FULL),
    )
    measure_dict = component.model_dump()
    measure_dict["expr_hash"] = compute_expression_hash(expression)
    return measure_dict


@pytest_asyncio.fixture
async def client_with_preaggs(
    module__client_with_roads: AsyncClient,
    module__session: AsyncSession,
):
    """
    Creates pre-aggregations for testing.
    """
    # First, get the node revision ID for default.repair_orders_fact
    response = await module__client_with_roads.get("/nodes/default.repair_orders_fact/")
    assert response.status_code == 200

    # We need to find the node revision ID through the current node
    from datajunction_server.database import Node

    node = await Node.get_by_name(module__session, "default.repair_orders_fact")
    node_revision_id = node.current.id  # type: ignore

    # Create some pre-aggregations with full measure info
    preagg1 = PreAggregation(
        node_revision_id=node_revision_id,
        grain_columns=["default.date_dim.date_id"],
        measures=[
            make_measure("sum_repair_cost", "repair_cost"),
            make_measure("count_orders", "1", "COUNT", "SUM"),
        ],
        sql="SELECT date_id, SUM(repair_cost), COUNT(1) FROM ... GROUP BY date_id",
        grain_group_hash=compute_grain_group_hash(
            node_revision_id,
            ["default.date_dim.date_id"],
        ),
        strategy=MaterializationStrategy.FULL,
        schedule="0 0 * * *",
    )

    preagg2 = PreAggregation(
        node_revision_id=node_revision_id,
        grain_columns=["default.date_dim.date_id", "default.hard_hat.state"],
        measures=[
            make_measure("sum_repair_cost", "repair_cost"),
            make_measure("avg_repair_cost", "repair_cost", "AVG", "AVG"),
        ],
        sql="SELECT date_id, state, SUM(repair_cost), AVG(repair_cost) FROM ... GROUP BY date_id, state",
        grain_group_hash=compute_grain_group_hash(
            node_revision_id,
            ["default.date_dim.date_id", "default.hard_hat.state"],
        ),
        strategy=MaterializationStrategy.INCREMENTAL_TIME,
        schedule="0 * * * *",
        lookback_window="3 days",
    )

    # Third pre-agg with same grain as preagg1 but different measures
    preagg3 = PreAggregation(
        node_revision_id=node_revision_id,
        grain_columns=["default.date_dim.date_id"],
        measures=[
            make_measure("max_repair_cost", "repair_cost", "MAX", "MAX"),
        ],
        sql="SELECT date_id, MAX(repair_cost) FROM ... GROUP BY date_id",
        grain_group_hash=compute_grain_group_hash(
            node_revision_id,
            ["default.date_dim.date_id"],
        ),
        strategy=MaterializationStrategy.FULL,
    )

    # Fourth pre-agg without strategy (for testing "requires strategy" validation)
    preagg4 = PreAggregation(
        node_revision_id=node_revision_id,
        grain_columns=["default.hard_hat.country"],
        measures=[
            make_measure("sum_repair_cost", "repair_cost"),
        ],
        sql="SELECT country, SUM(repair_cost) FROM ... GROUP BY country",
        grain_group_hash=compute_grain_group_hash(
            node_revision_id,
            ["default.hard_hat.country"],
        ),
        strategy=None,  # No strategy set
        schedule=None,
    )

    module__session.add_all([preagg1, preagg2, preagg3, preagg4])
    await module__session.commit()

    # Store the pre-agg IDs for later use
    await module__session.refresh(preagg1)
    await module__session.refresh(preagg2)
    await module__session.refresh(preagg3)
    await module__session.refresh(preagg4)

    yield {
        "client": module__client_with_roads,
        "session": module__session,
        "preagg1": preagg1,
        "preagg2": preagg2,
        "preagg3": preagg3,
        "preagg4": preagg4,
        "node_revision_id": node_revision_id,
    }


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
            params={"node_name": "default.repair_orders_fact"},
        )

        assert response.status_code == 200
        data = response.json()

        assert len(data["items"]) >= 3
        for item in data["items"]:
            assert item["node_name"] == "default.repair_orders_fact"

    @pytest.mark.asyncio
    async def test_list_preaggs_by_grain(self, client_with_preaggs):
        """Test filtering pre-aggregations by grain columns."""
        client = client_with_preaggs["client"]
        response = await client.get(
            "/preaggs/",
            params={"grain": "default.date_dim.date_id"},
        )

        assert response.status_code == 200
        data = response.json()

        # Should get preagg1 and preagg3 (both have single date_dim grain)
        matching_items = [
            item
            for item in data["items"]
            if sorted(item["grain_columns"]) == ["default.date_dim.date_id"]
        ]
        assert len(matching_items) >= 2

    @pytest.mark.asyncio
    async def test_list_preaggs_by_measures(self, client_with_preaggs):
        """Test filtering pre-aggregations by measures (superset match)."""
        client = client_with_preaggs["client"]
        response = await client.get(
            "/preaggs/",
            params={"measures": "sum_repair_cost"},
        )

        assert response.status_code == 200
        data = response.json()

        # Should get preagg1 and preagg2 (both contain sum_repair_cost)
        for item in data["items"]:
            measure_names = {m["name"] for m in item["measures"]}
            assert "sum_repair_cost" in measure_names

    @pytest.mark.asyncio
    async def test_list_preaggs_by_multiple_measures(self, client_with_preaggs):
        """Test filtering by multiple measures (all must be present)."""
        client = client_with_preaggs["client"]
        response = await client.get(
            "/preaggs/",
            params={"measures": "sum_repair_cost,count_orders"},
        )

        assert response.status_code == 200
        data = response.json()

        # Should only get preagg1 (has both measures)
        for item in data["items"]:
            measure_names = {m["name"] for m in item["measures"]}
            assert "sum_repair_cost" in measure_names
            assert "count_orders" in measure_names

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
        node_response = await client.get("/nodes/default.repair_orders_fact/")
        assert node_response.status_code == 200
        current_version = node_response.json()["version"]

        # Filter by version should return our preaggs
        response = await client.get(
            "/preaggs/",
            params={
                "node_name": "default.repair_orders_fact",
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
                "node_name": "default.repair_orders_fact",
                "node_version": "v99.99.99",
            },
        )

        assert response.status_code == 404
        assert "Version" in response.json()["message"]


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
        assert data["measures"] == [m.model_dump() for m in preagg1.measures]
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

        assert data["strategy"] == "incremental_time"
        assert data["schedule"] == "0 * * * *"
        assert data["lookback_window"] == "3 days"


class TestPlanPreaggregations:
    """Tests for POST /preaggs/plan endpoint."""

    @pytest.mark.asyncio
    async def test_plan_preaggs_basic(self, module__client_with_roads: AsyncClient):
        """Test basic plan endpoint creates pre-aggs from metrics + dims."""
        response = await module__client_with_roads.post(
            "/preaggs/plan",
            json={
                "metrics": ["default.num_repair_orders"],
                "dimensions": ["default.hard_hat.state"],
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
        module__client_with_roads: AsyncClient,
    ):
        """Test plan endpoint with materialization strategy."""
        # Use different dimensions than test_plan_preaggs_basic to avoid conflict
        # Use FULL strategy since source node may not have temporal partition columns
        response = await module__client_with_roads.post(
            "/preaggs/plan",
            json={
                "metrics": ["default.num_repair_orders"],
                "dimensions": ["default.hard_hat.city"],
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
        module__client_with_roads: AsyncClient,
    ):
        """Test that invalid strategy returns error."""
        response = await module__client_with_roads.post(
            "/preaggs/plan",
            json={
                "metrics": ["default.num_repair_orders"],
                "dimensions": ["default.hard_hat.state"],
                "strategy": "view",  # Not valid for pre-aggs
            },
        )

        # DJInvalidInputException returns 422 Unprocessable Entity
        assert response.status_code == 422
        assert "Invalid strategy" in response.json()["message"]

    @pytest.mark.asyncio
    async def test_plan_preaggs_returns_existing(
        self,
        module__client_with_roads: AsyncClient,
    ):
        """Test that calling plan twice returns existing pre-agg."""
        # First call creates
        response1 = await module__client_with_roads.post(
            "/preaggs/plan",
            json={
                "metrics": ["default.avg_repair_price"],
                "dimensions": ["default.dispatcher.company_name"],
            },
        )
        assert response1.status_code == 201
        data1 = response1.json()
        preagg_id_1 = data1["preaggs"][0]["id"]

        # Second call should return same pre-agg
        response2 = await module__client_with_roads.post(
            "/preaggs/plan",
            json={
                "metrics": ["default.avg_repair_price"],
                "dimensions": ["default.dispatcher.company_name"],
            },
        )
        assert response2.status_code == 201
        data2 = response2.json()
        preagg_id_2 = data2["preaggs"][0]["id"]

        assert preagg_id_1 == preagg_id_2


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
        module_mocker,
    ):
        """Test successful materialization call to query service."""
        from datajunction_server.database.materialization import MaterializationStrategy
        from datajunction_server.utils import get_query_service_client

        client = client_with_preaggs["client"]
        preagg1 = client_with_preaggs["preagg1"]

        # preagg1 already has strategy=FULL and schedule set in fixture

        # Mock the materialize_preagg method on the query service client
        # Access the actual client from the app's dependency overrides
        # materialize_preagg returns Dict[str, Any], not MaterializationInfo
        mock_result = {
            "urls": ["http://scheduler/job/123.main"],
            "output_tables": ["analytics.materialized.preagg_test"],
        }
        qs_client = client.app.dependency_overrides[get_query_service_client]()
        mock_materialize = module_mocker.patch.object(
            qs_client,
            "materialize_preagg",
            return_value=mock_result,
        )

        response = await client.post(f"/preaggs/{preagg1.id}/materialize")

        assert response.status_code == 200
        data = response.json()

        # Response includes the pre-agg info
        assert data["id"] == preagg1.id
        assert data["strategy"] == "full"

        # Verify query service was called
        mock_materialize.assert_called_once()
        call_args = mock_materialize.call_args
        mat_input = call_args[0][0]  # First positional arg

        # Verify the input structure
        assert mat_input.preagg_id == preagg1.id
        assert "preagg" in mat_input.output_table
        assert mat_input.strategy == MaterializationStrategy.FULL
        # temporal_partitions should be a list (may be empty if no partitions)
        assert isinstance(mat_input.temporal_partitions, list)


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
        module_mocker,
    ):
        """Test successfully deactivating a workflow."""
        from datajunction_server.database.preaggregation import PreAggregation
        from datajunction_server.utils import get_query_service_client

        client = client_with_preaggs["client"]
        session = client_with_preaggs["session"]
        preagg1 = client_with_preaggs["preagg1"]

        # Set up workflow URL on preagg1
        preagg = await session.get(PreAggregation, preagg1.id)
        preagg.scheduled_workflow_url = "http://scheduler/workflow/test-123"
        preagg.workflow_status = "active"
        await session.commit()

        # Mock the deactivate method
        qs_client = client.app.dependency_overrides[get_query_service_client]()
        mock_deactivate = module_mocker.patch.object(
            qs_client,
            "deactivate_preagg_workflow",
            return_value={"status": "paused"},
        )

        response = await client.delete(f"/preaggs/{preagg1.id}/workflow")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "paused"
        assert data["workflow_url"] == "http://scheduler/workflow/test-123"
        assert "deactivated successfully" in data["message"]

        # Verify query service was called
        mock_deactivate.assert_called_once()

    @pytest.mark.asyncio
    async def test_deactivate_workflow_not_found(self, client_with_preaggs):
        """Test deactivating workflow for non-existent pre-agg returns 404."""
        client = client_with_preaggs["client"]

        response = await client.delete("/preaggs/99999999/workflow")

        assert response.status_code == 404


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
        module_mocker,
    ):
        """Test successfully running a backfill."""
        from datajunction_server.database.preaggregation import PreAggregation
        from datajunction_server.utils import get_query_service_client

        client = client_with_preaggs["client"]
        session = client_with_preaggs["session"]
        preagg1 = client_with_preaggs["preagg1"]

        # Set up workflow URL on preagg1
        preagg = await session.get(PreAggregation, preagg1.id)
        preagg.scheduled_workflow_url = "http://scheduler/workflow/test-123"
        await session.commit()

        # Mock the backfill method
        qs_client = client.app.dependency_overrides[get_query_service_client]()
        mock_backfill = module_mocker.patch.object(
            qs_client,
            "run_preagg_backfill",
            return_value={"job_url": "http://scheduler/jobs/backfill-456"},
        )

        response = await client.post(
            f"/preaggs/{preagg1.id}/backfill",
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
        mock_backfill.assert_called_once()
        call_args = mock_backfill.call_args
        backfill_input = call_args[0][0]
        assert backfill_input.preagg_id == preagg1.id
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


class TestListPreaggregationsGrainSuperset:
    """Tests for grain superset mode in list_preaggs endpoint."""

    @pytest.mark.asyncio
    async def test_list_preaggs_grain_superset_mode(self, client_with_preaggs):
        """
        Test grain_mode=superset returns pre-aggs at finer grain.

        Fixture has:
        - preagg1: grain = ["default.date_dim.date_id"]
        - preagg2: grain = ["default.date_dim.date_id", "default.hard_hat.state"] (finer)
        - preagg3: grain = ["default.date_dim.date_id"]
        - preagg4: grain = ["default.hard_hat.country"]

        Requesting grain="default.date_dim.date_id" with mode="superset" should return
        preagg1, preagg2, and preagg3 (all contain date_dim.date_id).
        """
        client = client_with_preaggs["client"]

        response = await client.get(
            "/preaggs/",
            params={
                "grain": "default.date_dim.date_id",
                "grain_mode": "superset",
            },
        )

        assert response.status_code == 200
        data = response.json()

        # Should get preagg1, preagg2, preagg3 - all have date_dim.date_id
        # preagg2 has finer grain (date_id + state) but still matches
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

        # Exact mode
        exact_response = await client.get(
            "/preaggs/",
            params={
                "grain": "default.date_dim.date_id",
                "grain_mode": "exact",
            },
        )
        assert exact_response.status_code == 200
        exact_ids = {item["id"] for item in exact_response.json()["items"]}

        # Superset mode
        superset_response = await client.get(
            "/preaggs/",
            params={
                "grain": "default.date_dim.date_id",
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


class TestIncrementalTimeValidation:
    """Tests for INCREMENTAL_TIME strategy validation requiring temporal partition columns."""

    @pytest.mark.asyncio
    async def test_plan_incremental_no_temporal_columns(
        self,
        module__client_with_roads: AsyncClient,
    ):
        """
        Test that planning with INCREMENTAL_TIME strategy fails
        when source node lacks temporal partition columns.

        The repair_orders_fact node doesn't have temporal partition columns,
        so INCREMENTAL_TIME should be rejected.
        """
        response = await module__client_with_roads.post(
            "/preaggs/plan",
            json={
                "metrics": ["default.num_repair_orders"],
                "dimensions": ["default.dispatcher.company_name"],
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
        preagg4 = client_with_preaggs["preagg4"]

        # Set strategy to INCREMENTAL_TIME on preagg4
        from datajunction_server.database.preaggregation import PreAggregation

        preagg = await session.get(PreAggregation, preagg4.id)
        preagg.strategy = MaterializationStrategy.INCREMENTAL_TIME
        preagg.schedule = "0 0 * * *"
        await session.commit()

        # Try to materialize - should fail
        response = await client.post(f"/preaggs/{preagg4.id}/materialize")

        assert response.status_code == 422
        data = response.json()
        assert "INCREMENTAL_TIME" in data["message"]
        assert "temporal partition columns" in data["message"]


class TestQueryServiceExceptionHandling:
    """Tests for error handling when query service calls fail."""

    @pytest.mark.asyncio
    async def test_materialize_query_service_failure(
        self,
        client_with_preaggs,
        module_mocker,
    ):
        """Test error handling when materialize_preagg call fails."""
        from datajunction_server.utils import get_query_service_client

        client = client_with_preaggs["client"]
        preagg1 = client_with_preaggs["preagg1"]

        # Mock query service to raise an exception
        qs_client = client.app.dependency_overrides[get_query_service_client]()
        module_mocker.patch.object(
            qs_client,
            "materialize_preagg",
            side_effect=Exception("Connection refused"),
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
        module_mocker,
    ):
        """Test error handling when deactivate_preagg_workflow call fails."""
        from datajunction_server.database.preaggregation import PreAggregation
        from datajunction_server.utils import get_query_service_client

        client = client_with_preaggs["client"]
        session = client_with_preaggs["session"]
        preagg1 = client_with_preaggs["preagg1"]

        # Set up workflow URL
        preagg = await session.get(PreAggregation, preagg1.id)
        preagg.scheduled_workflow_url = "http://scheduler/workflow/test-123"
        preagg.workflow_status = "active"
        await session.commit()

        # Mock query service to raise an exception
        qs_client = client.app.dependency_overrides[get_query_service_client]()
        module_mocker.patch.object(
            qs_client,
            "deactivate_preagg_workflow",
            side_effect=Exception("Service unavailable"),
        )

        response = await client.delete(f"/preaggs/{preagg1.id}/workflow")

        assert response.status_code == 500
        data = response.json()
        assert "Failed to deactivate workflow" in data["message"]
        assert "Service unavailable" in data["message"]

    @pytest.mark.asyncio
    async def test_backfill_query_service_failure(
        self,
        client_with_preaggs,
        module_mocker,
    ):
        """Test error handling when run_preagg_backfill call fails."""
        from datajunction_server.database.preaggregation import PreAggregation
        from datajunction_server.utils import get_query_service_client

        client = client_with_preaggs["client"]
        session = client_with_preaggs["session"]
        preagg1 = client_with_preaggs["preagg1"]

        # Set up workflow URL
        preagg = await session.get(PreAggregation, preagg1.id)
        preagg.scheduled_workflow_url = "http://scheduler/workflow/test-123"
        await session.commit()

        # Mock query service to raise an exception
        qs_client = client.app.dependency_overrides[get_query_service_client]()
        module_mocker.patch.object(
            qs_client,
            "run_preagg_backfill",
            side_effect=Exception("Backfill service down"),
        )

        response = await client.post(
            f"/preaggs/{preagg1.id}/backfill",
            json={"start_date": "2024-01-01"},
        )

        assert response.status_code == 500
        data = response.json()
        assert "Failed to run backfill" in data["message"]
        assert "Backfill service down" in data["message"]


class TestWorkflowUrlExtraction:
    """Tests for workflow URL extraction and fallback logic in materialize."""

    @pytest.mark.asyncio
    async def test_materialize_extracts_main_workflow_url(
        self,
        client_with_preaggs,
        module_mocker,
    ):
        """Test that .main workflow URL is extracted from response."""
        from datajunction_server.utils import get_query_service_client
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
        qs_client = client.app.dependency_overrides[get_query_service_client]()
        module_mocker.patch.object(
            qs_client,
            "materialize_preagg",
            return_value=mock_result,
        )

        response = await client.post(f"/preaggs/{preagg1.id}/materialize")

        assert response.status_code == 200

        # Check the stored workflow URL is the .main one
        await session.refresh(preagg1)
        preagg = await session.get(PreAggregation, preagg1.id)
        assert preagg.scheduled_workflow_url == "http://scheduler/workflow/test.main"
        assert preagg.workflow_status == "active"

    @pytest.mark.asyncio
    async def test_materialize_fallback_to_first_url(
        self,
        client_with_preaggs,
        module_mocker,
    ):
        """Test fallback to first URL when no .main URL found."""
        from datajunction_server.utils import get_query_service_client
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
        qs_client = client.app.dependency_overrides[get_query_service_client]()
        module_mocker.patch.object(
            qs_client,
            "materialize_preagg",
            return_value=mock_result,
        )

        response = await client.post(f"/preaggs/{preagg3.id}/materialize")

        assert response.status_code == 200

        # Check the stored workflow URL is the first one (fallback)
        await session.refresh(preagg3)
        preagg = await session.get(PreAggregation, preagg3.id)
        assert (
            preagg.scheduled_workflow_url == "http://scheduler/workflow/test-fallback"
        )

    @pytest.mark.asyncio
    async def test_materialize_sets_default_schedule_when_none(
        self,
        client_with_preaggs,
        module_mocker,
    ):
        """Test that default schedule is set during materialization if not configured."""
        from datajunction_server.utils import get_query_service_client
        from datajunction_server.database.preaggregation import PreAggregation

        client = client_with_preaggs["client"]
        session = client_with_preaggs["session"]
        preagg4 = client_with_preaggs["preagg4"]

        # Set strategy but leave schedule as None
        preagg = await session.get(PreAggregation, preagg4.id)
        preagg.strategy = MaterializationStrategy.FULL
        preagg.schedule = None  # Explicitly None
        await session.commit()

        # Mock query service
        mock_result = {
            "urls": ["http://scheduler/workflow/test.main"],
            "output_tables": ["analytics.materialized.preagg_test"],
        }
        qs_client = client.app.dependency_overrides[get_query_service_client]()
        module_mocker.patch.object(
            qs_client,
            "materialize_preagg",
            return_value=mock_result,
        )

        response = await client.post(f"/preaggs/{preagg4.id}/materialize")

        assert response.status_code == 200

        # Check that schedule was set to default
        await session.refresh(preagg4)
        preagg = await session.get(PreAggregation, preagg4.id)
        assert preagg.schedule is not None
        # Default schedule from API is "0 0 * * *"
        assert preagg.schedule == "0 0 * * *"

    @pytest.mark.asyncio
    async def test_materialize_returns_all_urls(
        self,
        client_with_preaggs,
        module_mocker,
    ):
        """Test that response includes all workflow URLs from query service."""
        from datajunction_server.utils import get_query_service_client

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
        qs_client = client.app.dependency_overrides[get_query_service_client]()
        module_mocker.patch.object(
            qs_client,
            "materialize_preagg",
            return_value=mock_result,
        )

        response = await client.post(f"/preaggs/{preagg1.id}/materialize")

        assert response.status_code == 200
        data = response.json()

        # Response should include all workflow URLs
        assert "workflow_urls" in data
        assert data["workflow_urls"] == all_urls
