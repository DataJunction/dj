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

    module__session.add_all([preagg1, preagg2, preagg3])
    await module__session.commit()

    # Store the pre-agg IDs for later use
    await module__session.refresh(preagg1)
    await module__session.refresh(preagg2)
    await module__session.refresh(preagg3)

    yield {
        "client": module__client_with_roads,
        "preagg1": preagg1,
        "preagg2": preagg2,
        "preagg3": preagg3,
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
        assert data["measures"] == preagg1.measures
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
        response = await module__client_with_roads.post(
            "/preaggs/plan",
            json={
                "metrics": ["default.num_repair_orders"],
                "dimensions": ["default.hard_hat.city"],
                "strategy": "incremental_time",
                "schedule": "0 0 * * *",
                "lookback_window": "7 days",
            },
        )

        assert response.status_code == 201
        data = response.json()

        assert len(data["preaggs"]) >= 1
        preagg = data["preaggs"][0]
        assert preagg["strategy"] == "incremental_time"
        assert preagg["schedule"] == "0 0 * * *"
        assert preagg["lookback_window"] == "7 days"

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
        preagg1 = client_with_preaggs["preagg1"]

        # preagg1 has no strategy set
        response = await client.post(f"/preaggs/{preagg1.id}/materialize")

        assert response.status_code == 400
        assert "strategy must be configured" in response.json()["message"]

    @pytest.mark.asyncio
    async def test_materialize_preagg_not_found(self, client_with_preaggs):
        """Test materializing non-existent pre-agg returns 404."""
        client = client_with_preaggs["client"]

        response = await client.post("/preaggs/99999999/materialize")

        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_materialize_incremental_requires_schedule(
        self,
        client_with_preaggs,
        session,
    ):
        """Test that incremental materialization requires a schedule."""
        from datajunction_server.database.materialization import MaterializationStrategy
        from datajunction_server.database.preaggregation import PreAggregation

        client = client_with_preaggs["client"]
        preagg1 = client_with_preaggs["preagg1"]

        # Set strategy to INCREMENTAL_TIME but no schedule
        preagg = await session.get(PreAggregation, preagg1.id)
        preagg.strategy = MaterializationStrategy.INCREMENTAL_TIME
        preagg.schedule = None
        await session.commit()

        response = await client.post(f"/preaggs/{preagg1.id}/materialize")

        assert response.status_code == 400
        assert "schedule is required" in response.json()["message"]

    @pytest.mark.asyncio
    async def test_materialize_preagg_success(
        self,
        client_with_preaggs,
        session,
        mocker,
    ):
        """Test successful materialization call to query service."""
        from datajunction_server.database.materialization import MaterializationStrategy
        from datajunction_server.database.preaggregation import PreAggregation
        from datajunction_server.models.materialization import MaterializationInfo

        client = client_with_preaggs["client"]
        preagg1 = client_with_preaggs["preagg1"]

        # Set strategy
        preagg = await session.get(PreAggregation, preagg1.id)
        preagg.strategy = MaterializationStrategy.FULL
        await session.commit()

        # Mock the query service client
        mock_result = MaterializationInfo(
            urls=["http://scheduler/job/123"],
            output_tables=["analytics.materialized.preagg_test"],
        )
        mock_materialize = mocker.patch(
            "datajunction_server.api.preaggregations.get_query_service_client",
        )
        mock_materialize.return_value.materialize_preagg.return_value = mock_result

        response = await client.post(f"/preaggs/{preagg1.id}/materialize")

        assert response.status_code == 200
        data = response.json()

        # Response includes the pre-agg info
        assert data["id"] == preagg1.id
        assert data["strategy"] == "FULL"

        # Verify query service was called
        mock_materialize.return_value.materialize_preagg.assert_called_once()
        call_args = mock_materialize.return_value.materialize_preagg.call_args
        mat_input = call_args[0][0]  # First positional arg

        # Verify the input structure
        assert mat_input.preagg_id == preagg1.id
        assert "preagg" in mat_input.output_table
        assert mat_input.strategy == MaterializationStrategy.FULL
        # temporal_partitions should be a list (may be empty if no partitions)
        assert isinstance(mat_input.temporal_partitions, list)
