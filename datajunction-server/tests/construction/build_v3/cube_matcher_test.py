"""
Tests for cube_matcher module using the BUILD_V3 example data.

These tests create actual cubes via the API and test the cube matching
and SQL generation functions with real database queries rather than mocks.

Uses function-scoped sessions for test isolation.
"""

import time

import pytest

from datajunction_server.construction.build_v3.cube_matcher import (
    build_sql_from_cube,
    build_synthetic_grain_group,
    find_matching_cube,
)
from datajunction_server.construction.build_v3.decomposition import (
    decompose_and_group_metrics,
)
from datajunction_server.construction.build_v3.loaders import load_nodes
from datajunction_server.construction.build_v3.types import BuildContext
from datajunction_server.models.decompose import Aggregability
from datajunction_server.models.dialect import Dialect

from tests.construction.build_v3 import assert_sql_equal


class TestFindMatchingCube:
    """Tests for find_matching_cube"""

    @pytest.mark.asyncio
    async def test_returns_none_when_no_metrics_requested(
        self,
        session,
    ):
        """Should return None when no metrics are requested."""
        result = await find_matching_cube(
            session,
            metrics=[],
            dimensions=["v3.product.category"],
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_no_cubes_exist_for_metric(
        self,
        client_with_build_v3,
        session,
    ):
        """Should return None when no cubes contain the requested metric."""
        result = await find_matching_cube(
            session,
            metrics=["v3.page_view_count"],  # No cube has this metric
            dimensions=["v3.product.category"],
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_for_nonexistent_metric(
        self,
        session,
    ):
        """Should return None for metrics that don't exist."""
        result = await find_matching_cube(
            session,
            metrics=["v3.nonexistent_metric"],
            dimensions=[],
        )
        assert result is None

    # --------------------------------------------
    # Tests for find_matching_cube with cubes that have availability set
    # --------------------------------------------

    @pytest.mark.asyncio
    async def test_finds_cube_with_availability(
        self,
        client_with_build_v3,
        session,
    ):
        """Should find cube that has availability state set."""
        # Create a cube
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.test_cube_with_avail",
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "mode": "published",
                "description": "Test cube with availability",
            },
        )
        assert response.status_code == 201, response.json()

        # Set availability
        valid_through_ts = int(time.time() * 1000)
        response = await client_with_build_v3.post(
            "/data/v3.test_cube_with_avail/availability/",
            json={
                "catalog": "default",
                "schema_": "analytics",
                "table": "test_cube_with_avail",
                "valid_through_ts": valid_through_ts,
            },
        )
        assert response.status_code == 200, response.json()

        # Now find the cube
        result = await find_matching_cube(
            session,
            metrics=["v3.total_revenue"],
            dimensions=["v3.product.category"],
        )

        assert result is not None
        assert result.name == "v3.test_cube_with_avail"
        assert result.availability is not None

    @pytest.mark.asyncio
    async def test_skips_cube_without_availability(
        self,
        client_with_build_v3,
        session,
    ):
        """Should skip cubes that don't have availability set (default behavior)."""
        # Create a cube WITHOUT availability
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.test_cube_no_avail",
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "mode": "published",
                "description": "Test cube without availability",
            },
        )
        assert response.status_code == 201, response.json()

        # Don't set availability - cube should NOT be found with default require_availability=True
        result = await find_matching_cube(
            session,
            metrics=["v3.total_revenue"],
            dimensions=["v3.product.category"],
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_finds_cube_without_availability_when_not_required(
        self,
        client_with_build_v3,
        session,
    ):
        """Should find cubes without availability when require_availability=False."""
        # Create a cube WITHOUT availability
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.test_cube_no_avail_optional",
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "mode": "published",
                "description": "Test cube without availability (optional check)",
            },
        )
        assert response.status_code == 201, response.json()

        # Don't set availability - cube SHOULD be found with require_availability=False
        result = await find_matching_cube(
            session,
            metrics=["v3.total_revenue"],
            dimensions=["v3.product.category"],
            require_availability=False,
        )

        assert result is not None
        assert result.name == "v3.test_cube_no_avail_optional"
        assert result.availability is None

    @pytest.mark.asyncio
    async def test_require_availability_true_is_default(
        self,
        client_with_build_v3,
        session,
    ):
        """Should require availability by default (backward compatibility)."""
        # Create a cube WITHOUT availability
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.test_cube_default_check",
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "mode": "published",
                "description": "Test default require_availability behavior",
            },
        )
        assert response.status_code == 201, response.json()

        # Calling without the parameter should skip cubes without availability
        result = await find_matching_cube(
            session,
            metrics=["v3.total_revenue"],
            dimensions=["v3.product.category"],
            # Not passing require_availability - should default to True
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_empty_dimensions_finds_cube(
        self,
        client_with_build_v3,
        session,
    ):
        """Should find cube even with empty dimensions list (global aggregation)."""
        # Create cube
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.test_cube_empty_dims",
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "mode": "published",
                "description": "Test cube for empty dimensions query",
            },
        )
        assert response.status_code == 201, response.json()

        # Set availability
        valid_through_ts = int(time.time() * 1000)
        response = await client_with_build_v3.post(
            "/data/v3.test_cube_empty_dims/availability/",
            json={
                "catalog": "default",
                "schema_": "analytics",
                "table": "test_cube_empty_dims",
                "valid_through_ts": valid_through_ts,
            },
        )
        assert response.status_code == 200, response.json()

        # Query with empty dimensions - should still find the cube
        result = await find_matching_cube(
            session,
            metrics=["v3.total_revenue"],
            dimensions=[],
        )

        assert result is not None
        assert result.name == "v3.test_cube_empty_dims"

    # --------------------------------------------
    # Tests for dimension coverage in cube matching.
    # --------------------------------------------

    @pytest.mark.asyncio
    async def test_cube_must_cover_all_requested_dimensions(
        self,
        client_with_build_v3,
        session,
    ):
        """Should return None if no cube covers all requested dimensions."""
        # Create cube with only one dimension
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.test_cube_single_dim",
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "mode": "published",
                "description": "Cube with single dimension",
            },
        )
        assert response.status_code == 201, response.json()

        # Set availability
        valid_through_ts = int(time.time() * 1000)
        response = await client_with_build_v3.post(
            "/data/v3.test_cube_single_dim/availability/",
            json={
                "catalog": "default",
                "schema_": "analytics",
                "table": "test_cube_single_dim",
                "valid_through_ts": valid_through_ts,
            },
        )
        assert response.status_code == 200, response.json()

        # Query with dimension not in cube - should NOT find
        result = await find_matching_cube(
            session,
            metrics=["v3.total_revenue"],
            dimensions=[
                "v3.product.category",
                "v3.product.name",  # Not in the cube
            ],
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_cube_with_superset_dimensions_is_found(
        self,
        client_with_build_v3,
        session,
    ):
        """Should find cube that has superset of requested dimensions."""
        # Create cube with multiple dimensions
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.test_cube_multi_dim",
                "metrics": ["v3.total_revenue"],
                "dimensions": [
                    "v3.product.category",
                    "v3.product.subcategory",
                ],
                "mode": "published",
                "description": "Cube with multiple dimensions",
            },
        )
        assert response.status_code == 201, response.json()

        # Set availability
        valid_through_ts = int(time.time() * 1000)
        response = await client_with_build_v3.post(
            "/data/v3.test_cube_multi_dim/availability/",
            json={
                "catalog": "default",
                "schema_": "analytics",
                "table": "test_cube_multi_dim",
                "valid_through_ts": valid_through_ts,
            },
        )
        assert response.status_code == 200, response.json()

        # Query with subset of cube's dimensions - should find (roll-up possible)
        result = await find_matching_cube(
            session,
            metrics=["v3.total_revenue"],
            dimensions=["v3.product.category"],  # Subset of cube dims
        )

        assert result is not None
        assert result.name == "v3.test_cube_multi_dim"

    # --------------------------------------------
    # Tests for metric coverage in cube matching.
    # --------------------------------------------

    @pytest.mark.asyncio
    async def test_cube_must_contain_all_requested_metrics(
        self,
        client_with_build_v3,
        session,
    ):
        """Should return None if no available cube contains all requested metrics."""
        # Create cube with only one metric
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.test_cube_one_metric",
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "mode": "published",
                "description": "Cube with one metric",
            },
        )
        assert response.status_code == 201, response.json()

        # Set availability
        valid_through_ts = int(time.time() * 1000)
        response = await client_with_build_v3.post(
            "/data/v3.test_cube_one_metric/availability/",
            json={
                "catalog": "default",
                "schema_": "analytics",
                "table": "test_cube_one_metric",
                "valid_through_ts": valid_through_ts,
            },
        )
        assert response.status_code == 200, response.json()

        # Query with two metrics - cube only has one
        result = await find_matching_cube(
            session,
            metrics=["v3.total_revenue", "v3.total_quantity"],
            dimensions=["v3.product.category"],
        )

        # Should NOT find since cube doesn't have total_quantity
        assert result is None

    @pytest.mark.asyncio
    async def test_cube_with_multiple_metrics_is_found(
        self,
        client_with_build_v3,
        session,
    ):
        """Should find cube that contains all requested metrics."""
        # Create cube with multiple metrics
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.test_cube_multi_metric",
                "metrics": ["v3.total_revenue", "v3.total_quantity"],
                "dimensions": ["v3.product.category"],
                "mode": "published",
                "description": "Cube with multiple metrics",
            },
        )
        assert response.status_code == 201, response.json()

        # Set availability
        valid_through_ts = int(time.time() * 1000)
        response = await client_with_build_v3.post(
            "/data/v3.test_cube_multi_metric/availability/",
            json={
                "catalog": "default",
                "schema_": "analytics",
                "table": "test_cube_multi_metric",
                "valid_through_ts": valid_through_ts,
            },
        )
        assert response.status_code == 200, response.json()

        # Query with both metrics - should find
        result = await find_matching_cube(
            session,
            metrics=["v3.total_revenue", "v3.total_quantity"],
            dimensions=["v3.product.category"],
        )

        assert result is not None
        assert result.name == "v3.test_cube_multi_metric"

    #  --------------------------------------------
    # Tests for cube selection preference (smallest grain).
    # --------------------------------------------

    @pytest.mark.asyncio
    async def test_prefers_smallest_grain_cube(
        self,
        client_with_build_v3,
        session,
    ):
        """Should prefer cube with smallest grain (fewer dimensions) for less roll-up."""
        valid_through_ts = int(time.time() * 1000)

        # Create small grain cube
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.test_cube_small_grain",
                "metrics": ["v3.order_count"],
                "dimensions": ["v3.product.category"],
                "mode": "published",
                "description": "Small grain cube",
            },
        )
        assert response.status_code == 201, response.json()

        response = await client_with_build_v3.post(
            "/data/v3.test_cube_small_grain/availability/",
            json={
                "catalog": "default",
                "schema_": "analytics",
                "table": "test_cube_small_grain",
                "valid_through_ts": valid_through_ts,
            },
        )
        assert response.status_code == 200, response.json()

        # Create large grain cube
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.test_cube_large_grain",
                "metrics": ["v3.order_count"],
                "dimensions": [
                    "v3.product.category",
                    "v3.product.subcategory",
                    "v3.product.name",
                ],
                "mode": "published",
                "description": "Large grain cube",
            },
        )
        assert response.status_code == 201, response.json()

        response = await client_with_build_v3.post(
            "/data/v3.test_cube_large_grain/availability/",
            json={
                "catalog": "default",
                "schema_": "analytics",
                "table": "test_cube_large_grain",
                "valid_through_ts": valid_through_ts,
            },
        )
        assert response.status_code == 200, response.json()

        # Query should prefer smaller grain
        result = await find_matching_cube(
            session,
            metrics=["v3.order_count"],
            dimensions=["v3.product.category"],
        )

        assert result is not None
        assert result.name == "v3.test_cube_small_grain"

    @pytest.mark.asyncio
    async def test_uses_larger_grain_when_needed(
        self,
        client_with_build_v3,
        session,
    ):
        """Should use larger grain cube when smaller one doesn't cover dimensions."""
        valid_through_ts = int(time.time() * 1000)

        # Create small grain cube
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.test_cube_small",
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "mode": "published",
                "description": "Small grain cube",
            },
        )
        assert response.status_code == 201, response.json()

        response = await client_with_build_v3.post(
            "/data/v3.test_cube_small/availability/",
            json={
                "catalog": "default",
                "schema_": "analytics",
                "table": "test_cube_small",
                "valid_through_ts": valid_through_ts,
            },
        )
        assert response.status_code == 200, response.json()

        # Create large grain cube with additional dimension
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.test_cube_large",
                "metrics": ["v3.total_revenue"],
                "dimensions": [
                    "v3.product.category",
                    "v3.product.subcategory",
                ],
                "mode": "published",
                "description": "Large grain cube",
            },
        )
        assert response.status_code == 201, response.json()

        response = await client_with_build_v3.post(
            "/data/v3.test_cube_large/availability/",
            json={
                "catalog": "default",
                "schema_": "analytics",
                "table": "test_cube_large",
                "valid_through_ts": valid_through_ts,
            },
        )
        assert response.status_code == 200, response.json()

        # Query with dimension only in large cube
        result = await find_matching_cube(
            session,
            metrics=["v3.total_revenue"],
            dimensions=[
                "v3.product.category",
                "v3.product.subcategory",
            ],
        )

        assert result is not None
        assert result.name == "v3.test_cube_large"


class TestBuildSqlFromCube:
    """Tests for build_sql_from_cube function."""

    @pytest.mark.asyncio
    async def test_builds_sql_from_cube_single_metric(
        self,
        client_with_build_v3,
        session,
    ):
        """Should build SQL that queries from cube table for a single metric."""
        # Create a cube with availability
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.test_cube_sql_single",
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "mode": "published",
                "description": "Test cube for SQL generation",
            },
        )
        assert response.status_code == 201, response.json()

        # Set availability
        valid_through_ts = int(time.time() * 1000)
        response = await client_with_build_v3.post(
            "/data/v3.test_cube_sql_single/availability/",
            json={
                "catalog": "default",
                "schema_": "analytics",
                "table": "cube_single",
                "valid_through_ts": valid_through_ts,
            },
        )
        assert response.status_code == 200, response.json()

        # Find the cube
        cube = await find_matching_cube(
            session,
            metrics=["v3.total_revenue"],
            dimensions=["v3.product.category"],
        )
        assert cube is not None

        # Build SQL from the cube
        result = await build_sql_from_cube(
            session=session,
            cube=cube,
            metrics=["v3.total_revenue"],
            dimensions=["v3.product.category"],
            filters=None,
            dialect=Dialect.SPARK,
        )

        # Verify result structure
        assert result is not None
        assert result.sql is not None
        assert len(result.columns) > 0

        # Verify SQL structure using assert_sql_equal
        expected_sql = """
        WITH test_cube_sql_single_0 AS (
          SELECT
            category,
            line_total_sum_e1f61696
          FROM default.analytics.cube_single
        )
        SELECT
          COALESCE(test_cube_sql_single_0.category) AS category,
          SUM(test_cube_sql_single_0.line_total_sum_e1f61696) AS total_revenue
        FROM test_cube_sql_single_0
        GROUP BY  test_cube_sql_single_0.category
        """
        assert_sql_equal(result.sql, expected_sql)

        # Verify columns include the metric and dimension
        column_names = [col.name for col in result.columns]
        assert "category" in column_names
        assert "total_revenue" in column_names

    @pytest.mark.asyncio
    async def test_builds_sql_from_cube_multiple_metrics(
        self,
        client_with_build_v3,
        session,
    ):
        """Should build SQL for multiple metrics from a cube."""
        # Create a cube with multiple metrics
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.test_cube_sql_multi",
                "metrics": ["v3.total_revenue", "v3.total_quantity"],
                "dimensions": ["v3.product.category"],
                "mode": "published",
                "description": "Test cube with multiple metrics",
            },
        )
        assert response.status_code == 201, response.json()

        # Set availability
        valid_through_ts = int(time.time() * 1000)
        response = await client_with_build_v3.post(
            "/data/v3.test_cube_sql_multi/availability/",
            json={
                "catalog": "default",
                "schema_": "analytics",
                "table": "cube_multi",
                "valid_through_ts": valid_through_ts,
            },
        )
        assert response.status_code == 200, response.json()

        # Find the cube
        cube = await find_matching_cube(
            session,
            metrics=["v3.total_revenue", "v3.total_quantity"],
            dimensions=["v3.product.category"],
        )
        assert cube is not None

        # Build SQL from the cube
        result = await build_sql_from_cube(
            session=session,
            cube=cube,
            metrics=["v3.total_revenue", "v3.total_quantity"],
            dimensions=["v3.product.category"],
            filters=None,
            dialect=Dialect.SPARK,
        )

        # Verify result structure
        assert result is not None
        assert result.sql is not None

        # Verify SQL structure using assert_sql_equal
        expected_sql = """
        WITH test_cube_sql_multi_0 AS (
          SELECT
            category,
            line_total_sum_e1f61696,
            quantity_sum_06b64d2e
          FROM default.analytics.cube_multi
        )
        SELECT
          COALESCE(test_cube_sql_multi_0.category) AS category,
          SUM(test_cube_sql_multi_0.line_total_sum_e1f61696) AS total_revenue,
          SUM(test_cube_sql_multi_0.quantity_sum_06b64d2e) AS total_quantity
        FROM test_cube_sql_multi_0
        GROUP BY  test_cube_sql_multi_0.category
        """
        assert_sql_equal(result.sql, expected_sql)

        # Verify columns include both metrics and dimension
        column_names = [col.name for col in result.columns]
        assert "category" in column_names
        assert "total_revenue" in column_names
        assert "total_quantity" in column_names

    @pytest.mark.asyncio
    async def test_builds_sql_from_cube_with_all_v3_order_details_metrics(
        self,
        client_with_build_v3,
        session,
    ):
        """Should build SQL for all v3 order_details metrics including derived metrics.

        This includes:
        - Base metrics: total_revenue, total_quantity, order_count, customer_count,
                       avg_unit_price, max_unit_price, min_unit_price
        - Derived metrics: avg_order_value, avg_items_per_order, revenue_per_customer,
                          price_spread_pct
        """
        # Base metrics from order_details
        base_metrics = [
            "v3.total_revenue",
            "v3.total_quantity",
            "v3.order_count",
            "v3.customer_count",
            "v3.avg_unit_price",
            "v3.max_unit_price",
            "v3.min_unit_price",
        ]
        # Derived metrics that combine base metrics (same fact ratios)
        derived_metrics = [
            "v3.avg_order_value",  # total_revenue / order_count
            "v3.avg_items_per_order",  # total_quantity / order_count
            "v3.revenue_per_customer",  # total_revenue / customer_count
            "v3.price_spread_pct",  # (max_unit_price - min_unit_price) / avg_unit_price * 100
        ]
        all_metrics = base_metrics + derived_metrics

        # Create a cube with all metrics
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.test_cube_all_order_metrics",
                "metrics": all_metrics,
                "dimensions": ["v3.product.category"],
                "mode": "published",
                "description": "Test cube with all order_details metrics including derived",
            },
        )
        assert response.status_code == 201, response.json()

        # Set availability
        valid_through_ts = int(time.time() * 1000)
        response = await client_with_build_v3.post(
            "/data/v3.test_cube_all_order_metrics/availability/",
            json={
                "catalog": "default",
                "schema_": "analytics",
                "table": "cube_all_order_metrics",
                "valid_through_ts": valid_through_ts,
            },
        )
        assert response.status_code == 200, response.json()

        # Find the cube
        cube = await find_matching_cube(
            session,
            metrics=all_metrics,
            dimensions=["v3.product.category"],
        )
        assert cube is not None

        # Build SQL from the cube in Spark
        result = await build_sql_from_cube(
            session=session,
            cube=cube,
            metrics=all_metrics,
            dimensions=["v3.product.category"],
            filters=None,
            dialect=Dialect.SPARK,
        )

        assert result is not None
        assert result.sql is not None

        expected_sql = """
        WITH test_cube_all_order_metrics_0 AS (
          SELECT
            category,
            line_total_sum_e1f61696,
            quantity_sum_06b64d2e,
            order_id_distinct_f93d50ab,
            customer_id_hll_23002251,
            unit_price_count_55cff00f,
            unit_price_sum_55cff00f,
            unit_price_max_55cff00f,
            unit_price_min_55cff00f
          FROM default.analytics.cube_all_order_metrics
        )
        SELECT
          COALESCE(test_cube_all_order_metrics_0.category) AS category,
          SUM(test_cube_all_order_metrics_0.line_total_sum_e1f61696) AS total_revenue,
          SUM(test_cube_all_order_metrics_0.quantity_sum_06b64d2e) AS total_quantity,
          COUNT( DISTINCT test_cube_all_order_metrics_0.order_id_distinct_f93d50ab) AS order_count,
          hll_sketch_estimate(hll_union_agg(test_cube_all_order_metrics_0.customer_id_hll_23002251)) AS customer_count,
          SUM(test_cube_all_order_metrics_0.unit_price_sum_55cff00f) / SUM(test_cube_all_order_metrics_0.unit_price_count_55cff00f) AS avg_unit_price,
          MAX(test_cube_all_order_metrics_0.unit_price_max_55cff00f) AS max_unit_price,
          MIN(test_cube_all_order_metrics_0.unit_price_min_55cff00f) AS min_unit_price,
          SUM(test_cube_all_order_metrics_0.line_total_sum_e1f61696) / NULLIF(COUNT( DISTINCT test_cube_all_order_metrics_0.order_id_distinct_f93d50ab), 0) AS avg_order_value,
          SUM(test_cube_all_order_metrics_0.quantity_sum_06b64d2e) / NULLIF(COUNT( DISTINCT test_cube_all_order_metrics_0.order_id_distinct_f93d50ab), 0) AS avg_items_per_order,
          SUM(test_cube_all_order_metrics_0.line_total_sum_e1f61696) / NULLIF(hll_sketch_estimate(hll_union_agg(test_cube_all_order_metrics_0.customer_id_hll_23002251)), 0) AS revenue_per_customer,
          (MAX(test_cube_all_order_metrics_0.unit_price_max_55cff00f) - MIN(test_cube_all_order_metrics_0.unit_price_min_55cff00f)) / NULLIF(SUM(test_cube_all_order_metrics_0.unit_price_sum_55cff00f) / SUM(test_cube_all_order_metrics_0.unit_price_count_55cff00f), 0) * 100 AS price_spread_pct
        FROM test_cube_all_order_metrics_0
        GROUP BY
          test_cube_all_order_metrics_0.category
        """
        assert_sql_equal(result.sql, expected_sql, normalize_aliases=False)

        # Build SQL from the cube in Druid
        result = await build_sql_from_cube(
            session=session,
            cube=cube,
            metrics=all_metrics,
            dimensions=["v3.product.category"],
            filters=None,
            dialect=Dialect.DRUID,
        )

        assert result is not None
        assert result.sql is not None

        expected_sql = """
        WITH test_cube_all_order_metrics_0 AS (
          SELECT
            category,
            line_total_sum_e1f61696,
            quantity_sum_06b64d2e,
            order_id_distinct_f93d50ab,
            customer_id_hll_23002251,
            unit_price_count_55cff00f,
            unit_price_sum_55cff00f,
            unit_price_max_55cff00f,
            unit_price_min_55cff00f
          FROM default.analytics.cube_all_order_metrics
        )
        SELECT
          COALESCE(test_cube_all_order_metrics_0.category) AS category,
          SUM(test_cube_all_order_metrics_0.line_total_sum_e1f61696) AS total_revenue,
          SUM(test_cube_all_order_metrics_0.quantity_sum_06b64d2e) AS total_quantity,
          COUNT( DISTINCT test_cube_all_order_metrics_0.order_id_distinct_f93d50ab) AS order_count,
          hll_sketch_estimate(ds_hll(test_cube_all_order_metrics_0.customer_id_hll_23002251)) AS customer_count,
          SAFE_DIVIDE(SUM(test_cube_all_order_metrics_0.unit_price_sum_55cff00f), SUM(test_cube_all_order_metrics_0.unit_price_count_55cff00f)) AS avg_unit_price,
          MAX(test_cube_all_order_metrics_0.unit_price_max_55cff00f) AS max_unit_price,
          MIN(test_cube_all_order_metrics_0.unit_price_min_55cff00f) AS min_unit_price,
          SAFE_DIVIDE(SUM(test_cube_all_order_metrics_0.line_total_sum_e1f61696), NULLIF(COUNT( DISTINCT test_cube_all_order_metrics_0.order_id_distinct_f93d50ab), 0)) AS avg_order_value,
          SAFE_DIVIDE(SUM(test_cube_all_order_metrics_0.quantity_sum_06b64d2e), NULLIF(COUNT( DISTINCT test_cube_all_order_metrics_0.order_id_distinct_f93d50ab), 0)) AS avg_items_per_order,
          SAFE_DIVIDE(SUM(test_cube_all_order_metrics_0.line_total_sum_e1f61696), NULLIF(hll_sketch_estimate(ds_hll(test_cube_all_order_metrics_0.customer_id_hll_23002251)), 0)) AS revenue_per_customer,
          SAFE_DIVIDE((MAX(test_cube_all_order_metrics_0.unit_price_max_55cff00f) - MIN(test_cube_all_order_metrics_0.unit_price_min_55cff00f)), NULLIF(SAFE_DIVIDE(SUM(test_cube_all_order_metrics_0.unit_price_sum_55cff00f), SUM(test_cube_all_order_metrics_0.unit_price_count_55cff00f)), 0)) * 100 AS price_spread_pct
        FROM test_cube_all_order_metrics_0
        GROUP BY
          test_cube_all_order_metrics_0.category
        """
        assert_sql_equal(result.sql, expected_sql, normalize_aliases=False)

        # Verify all metrics are in columns
        column_names = [col.name for col in result.columns]
        assert "category" in column_names
        # Base metrics
        assert "total_revenue" in column_names
        assert "total_quantity" in column_names
        assert "order_count" in column_names
        assert "customer_count" in column_names
        assert "avg_unit_price" in column_names
        assert "max_unit_price" in column_names
        assert "min_unit_price" in column_names
        # Derived metrics
        assert "avg_order_value" in column_names
        assert "avg_items_per_order" in column_names
        assert "revenue_per_customer" in column_names
        assert "price_spread_pct" in column_names

    @pytest.mark.asyncio
    async def test_builds_sql_from_cube_with_window_function_metrics(
        self,
        client_with_build_v3,
        session,
    ):
        """Should build SQL for window function metrics (period-over-period).

        Window function metrics have required_dimensions and need special handling.
        This tests:
        - v3.wow_revenue_change (week-over-week, requires date.week)
        - v3.wow_order_growth (week-over-week, requires date.week)
        """
        # Base metrics needed by the derived window metrics
        base_metrics = [
            "v3.total_revenue",
            "v3.order_count",
        ]
        # Window function metrics - require v3.date.week[order] dimension
        window_metrics = [
            "v3.wow_revenue_change",
            "v3.wow_order_growth",
        ]
        all_metrics = base_metrics + window_metrics

        # Create a cube with the required week dimension
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.test_cube_window_metrics",
                "metrics": all_metrics,
                "dimensions": ["v3.date.week[order]", "v3.product.category"],
                "mode": "published",
                "description": "Test cube with window function metrics",
            },
        )
        assert response.status_code == 201, response.json()

        # Set availability
        valid_through_ts = int(time.time() * 1000)
        response = await client_with_build_v3.post(
            "/data/v3.test_cube_window_metrics/availability/",
            json={
                "catalog": "default",
                "schema_": "analytics",
                "table": "cube_window_metrics",
                "valid_through_ts": valid_through_ts,
            },
        )
        assert response.status_code == 200, response.json()

        # Find the cube
        cube = await find_matching_cube(
            session,
            metrics=all_metrics,
            dimensions=["v3.date.week[order]", "v3.product.category"],
        )
        assert cube is not None

        # Build SQL from the cube
        result = await build_sql_from_cube(
            session=session,
            cube=cube,
            metrics=all_metrics,
            dimensions=["v3.date.week[order]", "v3.product.category"],
            filters=None,
            dialect=Dialect.SPARK,
        )

        # Verify result structure
        assert result is not None
        assert result.sql is not None

        # Verify SQL structure using assert_sql_equal
        # Window metrics have LAG() OVER (ORDER BY ...) patterns
        expected_sql = """
        WITH test_cube_window_metrics_0 AS (
          SELECT
            week_order,
            category,
            line_total_sum_e1f61696,
            order_id_distinct_f93d50ab
          FROM default.analytics.cube_window_metrics
        ),
        base_metrics AS (
          SELECT
            COALESCE(test_cube_window_metrics_0.week_order) AS week_order,
            COALESCE(test_cube_window_metrics_0.category) AS category,
            COUNT( DISTINCT test_cube_window_metrics_0.order_id_distinct_f93d50ab) AS order_count,
            SUM(test_cube_window_metrics_0.line_total_sum_e1f61696) AS total_revenue
          FROM test_cube_window_metrics_0
          GROUP BY  test_cube_window_metrics_0.week_order, test_cube_window_metrics_0.category
        )
        SELECT
          base_metrics.week_order AS week_order,
          base_metrics.category AS category,
          SUM(test_cube_window_metrics_0.line_total_sum_e1f61696) AS total_revenue,
          COUNT( DISTINCT test_cube_window_metrics_0.order_id_distinct_f93d50ab) AS order_count,
          (base_metrics.total_revenue - LAG(base_metrics.total_revenue, 1) OVER ( PARTITION BY category
            ORDER BY base_metrics.week_order) ) / NULLIF(LAG(base_metrics.total_revenue, 1) OVER ( PARTITION BY category
            ORDER BY base_metrics.week_order) , 0) * 100 AS wow_revenue_change,
          (CAST(base_metrics.order_count AS DOUBLE) - LAG(CAST(base_metrics.order_count AS DOUBLE), 1) OVER ( PARTITION BY category
            ORDER BY base_metrics.week_order) ) / NULLIF(LAG(CAST(base_metrics.order_count AS DOUBLE), 1) OVER ( PARTITION BY category
            ORDER BY base_metrics.week_order) , 0) * 100 AS wow_order_growth
        FROM base_metrics
        """
        assert_sql_equal(result.sql, expected_sql, normalize_aliases=False)

        # Verify all metrics are in columns
        column_names = [col.name for col in result.columns]
        # Base metrics
        assert "total_revenue" in column_names
        assert "order_count" in column_names
        # Window metrics
        assert "wow_revenue_change" in column_names
        assert "wow_order_growth" in column_names

    @pytest.mark.asyncio
    async def test_builds_sql_from_cube_with_trailing_metrics(
        self,
        client_with_build_v3,
        session,
    ):
        """Should build SQL for trailing window metrics (rolling calculations).

        This tests:
        - v3.trailing_7d_revenue (rolling 7-day sum, requires date.date_id)
        - v3.trailing_wow_revenue_change (trailing WoW %, requires date.date_id)
        """
        # Base metrics needed by the trailing metrics
        base_metrics = [
            "v3.total_revenue",
        ]
        # Trailing metrics - require v3.date.date_id[order] dimension
        trailing_metrics = [
            "v3.trailing_7d_revenue",
            "v3.trailing_wow_revenue_change",
        ]
        all_metrics = base_metrics + trailing_metrics

        # Create a cube with the required date_id dimension
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.test_cube_trailing_metrics",
                "metrics": all_metrics,
                "dimensions": ["v3.date.date_id[order]", "v3.product.category"],
                "mode": "published",
                "description": "Test cube with trailing window metrics",
            },
        )
        assert response.status_code == 201, response.json()

        # Set availability
        valid_through_ts = int(time.time() * 1000)
        response = await client_with_build_v3.post(
            "/data/v3.test_cube_trailing_metrics/availability/",
            json={
                "catalog": "default",
                "schema_": "analytics",
                "table": "cube_trailing_metrics",
                "valid_through_ts": valid_through_ts,
            },
        )
        assert response.status_code == 200, response.json()

        # Find the cube
        cube = await find_matching_cube(
            session,
            metrics=all_metrics,
            dimensions=["v3.date.date_id[order]", "v3.product.category"],
        )
        assert cube is not None

        # Build SQL from the cube
        result = await build_sql_from_cube(
            session=session,
            cube=cube,
            metrics=all_metrics,
            dimensions=["v3.date.date_id[order]", "v3.product.category"],
            filters=None,
            dialect=Dialect.SPARK,
        )

        # Verify result structure
        assert result is not None
        assert result.sql is not None

        # Verify SQL structure using assert_sql_equal
        # Trailing metrics use window functions with ROWS BETWEEN
        expected_sql = """
        WITH test_cube_trailing_metrics_0 AS (
          SELECT
            date_id_order,
            category,
            line_total_sum_e1f61696
          FROM default.analytics.cube_trailing_metrics
        ),
        base_metrics AS (
          SELECT
            COALESCE(test_cube_trailing_metrics_0.date_id_order) AS date_id_order,
            COALESCE(test_cube_trailing_metrics_0.category) AS category,
            SUM(test_cube_trailing_metrics_0.line_total_sum_e1f61696) AS total_revenue
          FROM test_cube_trailing_metrics_0
          GROUP BY
            test_cube_trailing_metrics_0.date_id_order,
            test_cube_trailing_metrics_0.category
        )
        SELECT
          base_metrics.date_id_order AS date_id_order,
          base_metrics.category AS category,
          SUM(test_cube_trailing_metrics_0.line_total_sum_e1f61696) AS total_revenue,
          SUM(base_metrics.total_revenue) OVER ( ORDER BY base_metrics.date_id_order ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)  AS trailing_7d_revenue,
          (SUM(base_metrics.total_revenue) OVER ( ORDER BY base_metrics.date_id_order ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) - SUM(base_metrics.total_revenue) OVER ( ORDER BY base_metrics.date_id_order ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING) ) / NULLIF(SUM(base_metrics.total_revenue) OVER ( ORDER BY base_metrics.date_id_order ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING) , 0) * 100 AS trailing_wow_revenue_change
        FROM base_metrics
        """
        assert_sql_equal(result.sql, expected_sql, normalize_aliases=False)

        # Verify all metrics are in columns
        column_names = [col.name for col in result.columns]
        # Base metrics
        assert "total_revenue" in column_names
        # Trailing metrics
        assert "trailing_7d_revenue" in column_names
        assert "trailing_wow_revenue_change" in column_names

    @pytest.mark.asyncio
    async def test_builds_sql_with_rollup_dimensions(
        self,
        client_with_build_v3,
        session,
    ):
        """Should build SQL that rolls up dimensions when querying subset."""
        # Create a cube with multiple dimensions
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.test_cube_sql_rollup",
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category", "v3.product.subcategory"],
                "mode": "published",
                "description": "Test cube for rollup",
            },
        )
        assert response.status_code == 201, response.json()

        # Set availability
        valid_through_ts = int(time.time() * 1000)
        response = await client_with_build_v3.post(
            "/data/v3.test_cube_sql_rollup/availability/",
            json={
                "catalog": "default",
                "schema_": "analytics",
                "table": "cube_rollup",
                "valid_through_ts": valid_through_ts,
            },
        )
        assert response.status_code == 200, response.json()

        # Find the cube
        cube = await find_matching_cube(
            session,
            metrics=["v3.total_revenue"],
            dimensions=["v3.product.category"],  # Only one dimension - requires rollup
        )
        assert cube is not None

        # Build SQL from the cube (querying with fewer dimensions than cube has)
        result = await build_sql_from_cube(
            session=session,
            cube=cube,
            metrics=["v3.total_revenue"],
            dimensions=["v3.product.category"],
            filters=None,
            dialect=Dialect.SPARK,
        )

        # Verify result structure
        assert result is not None
        assert result.sql is not None

        # Verify SQL has GROUP BY for re-aggregation using assert_sql_equal
        expected_sql = """
        WITH test_cube_sql_rollup_0 AS (
          SELECT
            category,
            line_total_sum_e1f61696
          FROM default.analytics.cube_rollup
        )
        SELECT
          COALESCE(test_cube_sql_rollup_0.category) AS category,
          SUM(test_cube_sql_rollup_0.line_total_sum_e1f61696) AS total_revenue
        FROM test_cube_sql_rollup_0
        GROUP BY  test_cube_sql_rollup_0.category
        """
        assert_sql_equal(result.sql, expected_sql)

        # Should only have the requested dimension in output
        column_names = [col.name for col in result.columns]
        assert "category" in column_names

    @pytest.mark.asyncio
    async def test_builds_sql_respects_dialect(
        self,
        client_with_build_v3,
        session,
    ):
        """Should generate SQL appropriate for the specified dialect."""
        # Create a cube
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.test_cube_dialect",
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "mode": "published",
                "description": "Test cube for dialect",
            },
        )
        assert response.status_code == 201, response.json()

        # Set availability
        valid_through_ts = int(time.time() * 1000)
        response = await client_with_build_v3.post(
            "/data/v3.test_cube_dialect/availability/",
            json={
                "catalog": "default",
                "schema_": "analytics",
                "table": "cube_dialect",
                "valid_through_ts": valid_through_ts,
            },
        )
        assert response.status_code == 200, response.json()

        # Find the cube
        cube = await find_matching_cube(
            session,
            metrics=["v3.total_revenue"],
            dimensions=["v3.product.category"],
        )
        assert cube is not None

        # Build SQL with Spark dialect
        spark_result = await build_sql_from_cube(
            session=session,
            cube=cube,
            metrics=["v3.total_revenue"],
            dimensions=["v3.product.category"],
            filters=None,
            dialect=Dialect.SPARK,
        )

        # Build SQL with Druid dialect
        druid_result = await build_sql_from_cube(
            session=session,
            cube=cube,
            metrics=["v3.total_revenue"],
            dimensions=["v3.product.category"],
            filters=None,
            dialect=Dialect.DRUID,
        )

        # Both should produce valid SQL with expected structure
        expected_sql = """
        WITH test_cube_dialect_0 AS (
          SELECT
            category,
            line_total_sum_e1f61696
          FROM default.analytics.cube_dialect
        )
        SELECT
          COALESCE(test_cube_dialect_0.category) AS category,
          SUM(test_cube_dialect_0.line_total_sum_e1f61696) AS total_revenue
        FROM test_cube_dialect_0
        GROUP BY  test_cube_dialect_0.category
        """
        assert_sql_equal(spark_result.sql, expected_sql)
        assert_sql_equal(druid_result.sql, expected_sql)


class TestBuildSyntheticGrainGroup:
    """Tests for build_synthetic_grain_group function."""

    @pytest.mark.asyncio
    async def test_creates_grain_group_with_correct_structure(
        self,
        client_with_build_v3,
        session,
    ):
        """Should create a GrainGroupSQL with correct structure."""
        # Create a cube with availability
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.test_cube_grain_group",
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "mode": "published",
                "description": "Test cube for grain group",
            },
        )
        assert response.status_code == 201, response.json()

        # Set availability
        valid_through_ts = int(time.time() * 1000)
        response = await client_with_build_v3.post(
            "/data/v3.test_cube_grain_group/availability/",
            json={
                "catalog": "default",
                "schema_": "analytics",
                "table": "cube_grain",
                "valid_through_ts": valid_through_ts,
            },
        )
        assert response.status_code == 200, response.json()

        # Find the cube
        cube = await find_matching_cube(
            session,
            metrics=["v3.total_revenue"],
            dimensions=["v3.product.category"],
        )
        assert cube is not None

        # Create BuildContext and decompose metrics
        ctx = BuildContext(
            session=session,
            metrics=["v3.total_revenue"],
            dimensions=["v3.product.category"],
            filters=[],
            dialect=Dialect.SPARK,
            use_materialized=False,
        )
        await load_nodes(ctx)
        _, decomposed_metrics = await decompose_and_group_metrics(ctx)

        # Build synthetic grain group
        grain_group = build_synthetic_grain_group(
            ctx=ctx,
            decomposed_metrics=decomposed_metrics,
            cube=cube,
        )

        # Verify structure
        assert grain_group is not None
        assert grain_group.parent_name == "v3.test_cube_grain_group"
        assert grain_group.aggregability == Aggregability.FULL

        # Verify grain
        assert "category" in grain_group.grain

        # Verify columns include dimension and metric component
        column_names = [col.name for col in grain_group.columns]
        assert "category" in column_names
        assert "line_total_sum_e1f61696" in column_names

        # Verify component_aliases mapping
        assert len(grain_group.component_aliases) > 0

        # Verify query SQL using assert_sql_equal
        expected_sql = """
        SELECT category, line_total_sum_e1f61696
        FROM default.analytics.cube_grain
        """
        assert_sql_equal(str(grain_group.query), expected_sql)

    @pytest.mark.asyncio
    async def test_grain_group_query_references_cube_table(
        self,
        client_with_build_v3,
        session,
    ):
        """Should generate query that references the cube's availability table."""
        # Create a cube
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.test_cube_table_ref",
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "mode": "published",
                "description": "Test cube for table reference",
            },
        )
        assert response.status_code == 201, response.json()

        # Set availability with specific table name
        valid_through_ts = int(time.time() * 1000)
        response = await client_with_build_v3.post(
            "/data/v3.test_cube_table_ref/availability/",
            json={
                "catalog": "my_catalog",
                "schema_": "my_schema",
                "table": "my_cube_table",
                "valid_through_ts": valid_through_ts,
            },
        )
        assert response.status_code == 200, response.json()

        # Find the cube
        cube = await find_matching_cube(
            session,
            metrics=["v3.total_revenue"],
            dimensions=["v3.product.category"],
        )
        assert cube is not None

        # Create BuildContext and decompose metrics
        ctx = BuildContext(
            session=session,
            metrics=["v3.total_revenue"],
            dimensions=["v3.product.category"],
            filters=[],
            dialect=Dialect.SPARK,
            use_materialized=False,
        )
        await load_nodes(ctx)
        _, decomposed_metrics = await decompose_and_group_metrics(ctx)

        # Build synthetic grain group
        grain_group = build_synthetic_grain_group(
            ctx=ctx,
            decomposed_metrics=decomposed_metrics,
            cube=cube,
        )

        # Verify query references the cube table using assert_sql_equal
        expected_sql = """
        SELECT category, line_total_sum_e1f61696
        FROM my_catalog.my_schema.my_cube_table
        """
        assert_sql_equal(str(grain_group.query), expected_sql)

    @pytest.mark.asyncio
    async def test_grain_group_with_multiple_metrics(
        self,
        client_with_build_v3,
        session,
    ):
        """Should handle multiple metrics correctly in synthetic grain group."""
        # Create a cube with multiple metrics
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.test_cube_multi_metrics_grain",
                "metrics": ["v3.total_revenue", "v3.total_quantity"],
                "dimensions": ["v3.product.category"],
                "mode": "published",
                "description": "Test cube with multiple metrics",
            },
        )
        assert response.status_code == 201, response.json()

        # Set availability
        valid_through_ts = int(time.time() * 1000)
        response = await client_with_build_v3.post(
            "/data/v3.test_cube_multi_metrics_grain/availability/",
            json={
                "catalog": "default",
                "schema_": "analytics",
                "table": "cube_multi_grain",
                "valid_through_ts": valid_through_ts,
            },
        )
        assert response.status_code == 200, response.json()

        # Find the cube
        cube = await find_matching_cube(
            session,
            metrics=["v3.total_revenue", "v3.total_quantity"],
            dimensions=["v3.product.category"],
        )
        assert cube is not None

        # Create BuildContext and decompose metrics
        ctx = BuildContext(
            session=session,
            metrics=["v3.total_revenue", "v3.total_quantity"],
            dimensions=["v3.product.category"],
            filters=[],
            dialect=Dialect.SPARK,
            use_materialized=False,
        )
        await load_nodes(ctx)
        _, decomposed_metrics = await decompose_and_group_metrics(ctx)

        # Build synthetic grain group
        grain_group = build_synthetic_grain_group(
            ctx=ctx,
            decomposed_metrics=decomposed_metrics,
            cube=cube,
        )

        # Verify multiple metrics are in the grain group
        assert len(grain_group.metrics) == 2
        assert "v3.total_revenue" in grain_group.metrics
        assert "v3.total_quantity" in grain_group.metrics

        # Verify component columns for both metrics
        column_names = [col.name for col in grain_group.columns]
        assert "line_total_sum_e1f61696" in column_names
        assert "quantity_sum_06b64d2e" in column_names

        # Verify query SQL using assert_sql_equal
        expected_sql = """
        SELECT category, line_total_sum_e1f61696, quantity_sum_06b64d2e
        FROM default.analytics.cube_multi_grain
        """
        assert_sql_equal(str(grain_group.query), expected_sql)

    @pytest.mark.asyncio
    async def test_grain_group_raises_error_without_availability(
        self,
        client_with_build_v3,
        session,
    ):
        """Should raise ValueError if cube has no availability."""
        # Create a cube WITHOUT availability
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.test_cube_no_avail_grain",
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "mode": "published",
                "description": "Test cube without availability",
            },
        )
        assert response.status_code == 201, response.json()

        # Find the cube without requiring availability
        cube = await find_matching_cube(
            session,
            metrics=["v3.total_revenue"],
            dimensions=["v3.product.category"],
            require_availability=False,
        )
        assert cube is not None
        assert cube.availability is None

        # Create BuildContext
        ctx = BuildContext(
            session=session,
            metrics=["v3.total_revenue"],
            dimensions=["v3.product.category"],
            filters=[],
            dialect=Dialect.SPARK,
            use_materialized=False,
        )
        await load_nodes(ctx)
        _, decomposed_metrics = await decompose_and_group_metrics(ctx)

        # Should raise ValueError
        with pytest.raises(ValueError, match="has no availability"):
            build_synthetic_grain_group(
                ctx=ctx,
                decomposed_metrics=decomposed_metrics,
                cube=cube,
            )


class TestBuildMetricsSqlCubePath:
    """
    Tests for build_metrics_sql that exercise the cube matching path (Layer 1).

    These tests verify that when a matching cube with availability is found,
    build_metrics_sql uses the cube's table as the source instead of computing
    from source tables.
    """

    @pytest.mark.asyncio
    async def test_build_metrics_sql_uses_cube_when_available(
        self,
        client_with_build_v3,
        session,
    ):
        """Should use cube table when matching cube with availability exists."""
        from datajunction_server.construction.build_v3 import build_metrics_sql

        # Create a cube
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.cube_for_metrics_sql",
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "mode": "published",
                "description": "Cube for build_metrics_sql test",
            },
        )
        assert response.status_code == 201, response.json()

        # Set availability
        valid_through_ts = int(time.time() * 1000)
        response = await client_with_build_v3.post(
            "/data/v3.cube_for_metrics_sql/availability/",
            json={
                "catalog": "default",
                "schema_": "analytics",
                "table": "cube_for_metrics_sql",
                "valid_through_ts": valid_through_ts,
            },
        )
        assert response.status_code == 200, response.json()

        # Call build_metrics_sql - should use cube path
        result = await build_metrics_sql(
            session=session,
            metrics=["v3.total_revenue"],
            dimensions=["v3.product.category"],
            filters=None,
            dialect=Dialect.SPARK,
            use_materialized=True,
        )

        # Verify SQL using assert_sql_equal - queries cube table with re-aggregation
        assert_sql_equal(
            result.sql,
            """
            WITH
            cube_for_metrics_sql_0 AS (
                SELECT category, line_total_sum_e1f61696
                FROM default.analytics.cube_for_metrics_sql
            )
            SELECT
                COALESCE(cube_for_metrics_sql_0.category) AS category,
                SUM(cube_for_metrics_sql_0.line_total_sum_e1f61696) AS total_revenue
            FROM cube_for_metrics_sql_0
            GROUP BY cube_for_metrics_sql_0.category
            """,
        )

        # Verify output columns
        assert len(result.columns) == 2
        column_names = [col.name for col in result.columns]
        assert "category" in column_names
        assert "total_revenue" in column_names

    @pytest.mark.asyncio
    async def test_build_metrics_sql_falls_back_when_no_cube(
        self,
        client_with_build_v3,
        session,
    ):
        """Should fall back to source tables when no matching cube exists."""
        from datajunction_server.construction.build_v3 import build_metrics_sql

        # Don't create any cube - just call build_metrics_sql
        result = await build_metrics_sql(
            session=session,
            metrics=["v3.page_view_count"],  # No cube has this metric
            dimensions=["v3.product.category"],
            filters=None,
            dialect=Dialect.SPARK,
            use_materialized=True,
        )

        # Verify SQL uses source tables (page_views), not cube
        assert_sql_equal(
            result.sql,
            """
            WITH
            v3_page_views_enriched AS (
                SELECT view_id, product_id
                FROM default.v3.page_views
            ),
            v3_product AS (
                SELECT product_id, category
                FROM default.v3.products
            ),
            page_views_enriched_0 AS (
                SELECT t2.category, COUNT(t1.view_id) view_id_count_f41e2db4
                FROM v3_page_views_enriched t1
                LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
                GROUP BY t2.category
            )
            SELECT
                COALESCE(page_views_enriched_0.category) AS category,
                SUM(page_views_enriched_0.view_id_count_f41e2db4) AS page_view_count
            FROM page_views_enriched_0
            GROUP BY page_views_enriched_0.category
            """,
        )

        # Verify output columns
        column_names = [col.name for col in result.columns]
        assert "category" in column_names
        assert "page_view_count" in column_names

    @pytest.mark.asyncio
    async def test_build_metrics_sql_skips_cube_when_use_materialized_false(
        self,
        client_with_build_v3,
        session,
    ):
        """Should NOT use cube when use_materialized=False."""
        from datajunction_server.construction.build_v3 import build_metrics_sql

        # Create a cube with availability
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.cube_skip_test",
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "mode": "published",
                "description": "Cube that should be skipped",
            },
        )
        assert response.status_code == 201, response.json()

        valid_through_ts = int(time.time() * 1000)
        response = await client_with_build_v3.post(
            "/data/v3.cube_skip_test/availability/",
            json={
                "catalog": "default",
                "schema_": "analytics",
                "table": "cube_skip_test",
                "valid_through_ts": valid_through_ts,
            },
        )
        assert response.status_code == 200, response.json()

        # Call build_metrics_sql with use_materialized=False
        result = await build_metrics_sql(
            session=session,
            metrics=["v3.total_revenue"],
            dimensions=["v3.product.category"],
            filters=None,
            dialect=Dialect.SPARK,
            use_materialized=False,  # Should skip cube lookup
        )

        # Verify SQL does NOT reference the cube table - uses source tables
        assert_sql_equal(
            result.sql,
            """
            WITH
            v3_order_details AS (
                SELECT oi.product_id, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
                SELECT product_id, category
                FROM default.v3.products
            ),
            order_details_0 AS (
                SELECT t2.category, SUM(t1.line_total) line_total_sum_e1f61696
                FROM v3_order_details t1
                LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
                GROUP BY t2.category
            )
            SELECT
                COALESCE(order_details_0.category) AS category,
                SUM(order_details_0.line_total_sum_e1f61696) AS total_revenue
            FROM order_details_0
            GROUP BY order_details_0.category
            """,
        )

    @pytest.mark.asyncio
    async def test_build_metrics_sql_cube_with_multi_component_metric(
        self,
        client_with_build_v3,
        session,
    ):
        """Should correctly handle multi-component metrics (like AVG) from cube."""
        from datajunction_server.construction.build_v3 import build_metrics_sql

        # Create a cube with AVG metric (decomposes to SUM + COUNT)
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.cube_avg_metric",
                "metrics": ["v3.avg_unit_price"],
                "dimensions": ["v3.product.category"],
                "mode": "published",
                "description": "Cube with AVG metric",
            },
        )
        assert response.status_code == 201, response.json()

        valid_through_ts = int(time.time() * 1000)
        response = await client_with_build_v3.post(
            "/data/v3.cube_avg_metric/availability/",
            json={
                "catalog": "default",
                "schema_": "analytics",
                "table": "cube_avg_metric",
                "valid_through_ts": valid_through_ts,
            },
        )
        assert response.status_code == 200, response.json()

        # Call build_metrics_sql
        result = await build_metrics_sql(
            session=session,
            metrics=["v3.avg_unit_price"],
            dimensions=["v3.product.category"],
            filters=None,
            dialect=Dialect.SPARK,
            use_materialized=True,
        )

        # Verify SQL with combiner expression for AVG (SUM/SUM for re-aggregation)
        # AVG decomposes into unit_price_sum_xxx and unit_price_count_xxx components
        assert_sql_equal(
            result.sql,
            """
            WITH
            cube_avg_metric_0 AS (
                SELECT category, unit_price_count_55cff00f, unit_price_sum_55cff00f
                FROM default.analytics.cube_avg_metric
            )
            SELECT
                COALESCE(cube_avg_metric_0.category) AS category,
                SUM(cube_avg_metric_0.unit_price_sum_55cff00f)
                    / SUM(cube_avg_metric_0.unit_price_count_55cff00f) AS avg_unit_price
            FROM cube_avg_metric_0
            GROUP BY cube_avg_metric_0.category
            """,
            normalize_aliases=True,
        )

        # Verify output columns
        column_names = [col.name for col in result.columns]
        assert "avg_unit_price" in column_names

    @pytest.mark.asyncio
    async def test_build_metrics_sql_cube_with_multiple_metrics(
        self,
        client_with_build_v3,
        session,
    ):
        """Should handle cube with multiple metrics correctly."""
        from datajunction_server.construction.build_v3 import build_metrics_sql

        # Create a cube with multiple metrics
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.cube_multi_metrics",
                "metrics": ["v3.total_revenue", "v3.total_quantity"],
                "dimensions": ["v3.product.category"],
                "mode": "published",
                "description": "Cube with multiple metrics",
            },
        )
        assert response.status_code == 201, response.json()

        valid_through_ts = int(time.time() * 1000)
        response = await client_with_build_v3.post(
            "/data/v3.cube_multi_metrics/availability/",
            json={
                "catalog": "default",
                "schema_": "analytics",
                "table": "cube_multi_metrics",
                "valid_through_ts": valid_through_ts,
            },
        )
        assert response.status_code == 200, response.json()

        # Call build_metrics_sql with both metrics
        result = await build_metrics_sql(
            session=session,
            metrics=["v3.total_revenue", "v3.total_quantity"],
            dimensions=["v3.product.category"],
            filters=None,
            dialect=Dialect.SPARK,
            use_materialized=True,
        )

        # Verify SQL with both metrics from cube
        assert_sql_equal(
            result.sql,
            """
            WITH
            cube_multi_metrics_0 AS (
                SELECT category, line_total_sum_e1f61696, quantity_sum_06b64d2e
                FROM default.analytics.cube_multi_metrics
            )
            SELECT
                COALESCE(cube_multi_metrics_0.category) AS category,
                SUM(cube_multi_metrics_0.line_total_sum_e1f61696) AS total_revenue,
                SUM(cube_multi_metrics_0.quantity_sum_06b64d2e) AS total_quantity
            FROM cube_multi_metrics_0
            GROUP BY cube_multi_metrics_0.category
            """,
        )

        # Verify both metrics in output
        column_names = [col.name for col in result.columns]
        assert "total_revenue" in column_names
        assert "total_quantity" in column_names

    @pytest.mark.asyncio
    async def test_build_metrics_sql_cube_rollup(
        self,
        client_with_build_v3,
        session,
    ):
        """Should roll up cube data when querying with fewer dimensions."""
        from datajunction_server.construction.build_v3 import build_metrics_sql

        # Create a cube with multiple dimensions
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.cube_rollup_test",
                "metrics": ["v3.total_revenue"],
                "dimensions": [
                    "v3.product.category",
                    "v3.product.subcategory",
                ],
                "mode": "published",
                "description": "Cube for rollup test",
            },
        )
        assert response.status_code == 201, response.json()

        valid_through_ts = int(time.time() * 1000)
        response = await client_with_build_v3.post(
            "/data/v3.cube_rollup_test/availability/",
            json={
                "catalog": "default",
                "schema_": "analytics",
                "table": "cube_rollup_test",
                "valid_through_ts": valid_through_ts,
            },
        )
        assert response.status_code == 200, response.json()

        # Query with only one dimension (should roll up)
        result = await build_metrics_sql(
            session=session,
            metrics=["v3.total_revenue"],
            dimensions=["v3.product.category"],  # Subset of cube dims
            filters=None,
            dialect=Dialect.SPARK,
            use_materialized=True,
        )

        # Verify SQL - cube has both dims, but only category requested
        # The cube table is queried with both columns but only category in output
        assert_sql_equal(
            result.sql,
            """
            WITH
            cube_rollup_test_0 AS (
                SELECT category, line_total_sum_e1f61696
                FROM default.analytics.cube_rollup_test
            )
            SELECT
                COALESCE(cube_rollup_test_0.category) AS category,
                SUM(cube_rollup_test_0.line_total_sum_e1f61696) AS total_revenue
            FROM cube_rollup_test_0
            GROUP BY cube_rollup_test_0.category
            """,
        )

        # Only category should be in output (not subcategory)
        column_names = [col.name for col in result.columns]
        assert "category" in column_names
        assert "subcategory" not in column_names
