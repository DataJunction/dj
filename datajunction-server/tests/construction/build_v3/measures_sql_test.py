import pytest
from . import assert_sql_equal, get_first_grain_group
from datajunction_server.construction.build_v3.builder import build_measures_sql


# All base metrics from order_details
ORDER_DETAILS_BASE_METRICS = [
    "v3.total_revenue",
    "v3.total_quantity",
    "v3.order_count",
    "v3.customer_count",
]

# All base metrics from page_views_enriched
PAGE_VIEWS_BASE_METRICS = [
    "v3.page_view_count",
    "v3.product_view_count",
    "v3.session_count",
    "v3.visitor_count",
]

# Derived metrics - same fact ratios (order_details)
SAME_FACT_DERIVED_ORDER = [
    "v3.avg_order_value",  # revenue / orders
    "v3.avg_items_per_order",  # quantity / orders
    "v3.revenue_per_customer",  # revenue / customers
]

# Derived metrics - same fact ratios (page_views)
SAME_FACT_DERIVED_PAGE = [
    "v3.pages_per_session",  # page_views / sessions
]

# Derived metrics - cross-fact ratios
CROSS_FACT_DERIVED = [
    "v3.conversion_rate",  # orders / visitors (order_details page_views)
    "v3.revenue_per_visitor",  # revenue / visitors (order_details page_views)
    "v3.revenue_per_page_view",  # revenue / page_views (order_details page_views)
]

# Derived metrics - period-over-period (window functions, aggregability: NONE)
PERIOD_OVER_PERIOD = [
    "v3.wow_revenue_change",
    "v3.wow_order_growth",
    "v3.mom_revenue_change",
]

# Nested derived metrics - metrics that reference other derived metrics
NESTED_DERIVED_METRICS = [
    "v3.wow_aov_change",  # window function on avg_order_value (derived)
    "v3.aov_growth_index",  # simple derived from avg_order_value
    "v3.efficiency_ratio",  # cross-fact derived from avg_order_value and pages_per_session
]


class TestMeasuresSQLEndpoint:
    """Tests for the /sql/measures/v3/ endpoint."""

    @pytest.mark.asyncio
    async def test_single_metric_single_dimension(self, client_with_build_v3):
        """
        Test the simplest case: one metric, one dimension.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
            },
        )

        assert response.status_code == 200
        data = get_first_grain_group(response.json())

        # Parse and compare SQL structure
        # For single-component metrics, we use the metric name (not hash)
        assert_sql_equal(
            data["sql"],
            """
            WITH v3_order_details AS (
                SELECT o.status, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t1.status, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            GROUP BY t1.status
            """,
        )

        assert "_DOT_" not in data["sql"]
        assert data["columns"] == [
            {
                "name": "status",
                "type": "string",
                "semantic_entity": "v3.order_details.status",
                "semantic_type": "dimension",
            },
            {
                "name": "line_total_sum_e1f61696",
                "type": "double",
                "semantic_entity": "v3.total_revenue:line_total_sum_e1f61696",
                "semantic_type": "metric_component",
            },
        ]

    @pytest.mark.asyncio
    async def test_no_metrics_raises_error(self, client_with_build_v3):
        """Test that empty metrics raises an error."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": [],
                "dimensions": ["v3.order_details.status"],
            },
        )

        # Should return an error (4xx status)
        assert response.status_code >= 400

    @pytest.mark.asyncio
    async def test_nonexistent_metric_raises_error(self, client_with_build_v3):
        """Test that nonexistent metric raises an error."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["nonexistent.metric"],
                "dimensions": ["v3.order_details.status"],
            },
        )

        # Should return an error
        assert response.status_code >= 400
        assert "not found" in response.text.lower()

    @pytest.mark.asyncio
    async def test_metrics_with_no_dimensions(self, client_with_build_v3):
        """
        Test requesting metrics with no dimensions (global aggregation).
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": [
                    "v3.total_revenue",
                    "v3.max_unit_price",
                    "v3.min_unit_price",
                ],
                "dimensions": [],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        assert len(result["grain_groups"]) == 1
        gg = result["grain_groups"][0]
        assert gg["grain"] == []
        assert gg["aggregability"] == "full"

        assert_sql_equal(
            gg["sql"],
            """
            WITH
            v3_order_details AS (
            SELECT  oi.unit_price,
                oi.quantity * oi.unit_price AS line_total
            FROM default.v3.orders o JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )

            SELECT  SUM(t1.line_total) line_total_sum_e1f61696,
                MAX(t1.unit_price) unit_price_max_55cff00f,
                MIN(t1.unit_price) unit_price_min_55cff00f
            FROM v3_order_details t1
            """,
        )

        assert gg["columns"] == [
            {
                "name": "line_total_sum_e1f61696",
                "type": "double",
                "semantic_entity": "v3.total_revenue:line_total_sum_e1f61696",
                "semantic_type": "metric_component",
            },
            {
                "name": "unit_price_max_55cff00f",
                "type": "float",
                "semantic_entity": "v3.max_unit_price:unit_price_max_55cff00f",
                "semantic_type": "metric_component",
            },
            {
                "name": "unit_price_min_55cff00f",
                "type": "float",
                "semantic_entity": "v3.min_unit_price:unit_price_min_55cff00f",
                "semantic_type": "metric_component",
            },
        ]


class TestDimensionJoins:
    """Tests for dimension join functionality (Chunk 2)."""

    @pytest.mark.asyncio
    async def test_mixed_local_and_joined_dimensions(self, client_with_build_v3):
        """
        Test query with both local dimensions and joined dimensions.

        Query: revenue by status (local) and customer name (joined)
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": [
                    "v3.order_details.status",  # Local
                    "v3.customer.name",  # Requires join
                ],
            },
        )

        assert response.status_code == 200
        data = get_first_grain_group(response.json())

        assert_sql_equal(
            data["sql"],
            """
            WITH
            v3_customer AS (
                SELECT customer_id, name
                FROM default.v3.customers
            ),
            v3_order_details AS (
                SELECT o.customer_id, o.status, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t1.status, t2.name, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_customer t2 ON t1.customer_id = t2.customer_id
            GROUP BY t1.status, t2.name
            """,
        )

        assert "_DOT_" not in data["sql"]
        assert data["columns"] == [
            {
                "name": "status",
                "type": "string",
                "semantic_entity": "v3.order_details.status",
                "semantic_type": "dimension",
            },
            {
                "name": "name",
                "type": "string",
                "semantic_entity": "v3.customer.name",
                "semantic_type": "dimension",
            },
            {
                "name": "line_total_sum_e1f61696",
                "type": "double",
                "semantic_entity": "v3.total_revenue:line_total_sum_e1f61696",
                "semantic_type": "metric_component",
            },
        ]

    @pytest.mark.asyncio
    async def test_multiple_metrics_with_dimension_join(self, client_with_build_v3):
        """
        Test multiple metrics with a dimension join.

        Query: revenue and quantity by customer name

        Note: v3.customer.name doesn't specify a role, but the dimension link
        has role="customer". The system finds the path anyway.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue", "v3.total_quantity"],
                "dimensions": ["v3.customer.name"],
            },
        )

        assert response.status_code == 200
        result = response.json()

        # Should have 1 grain group (FULL aggregability)
        assert len(result["grain_groups"]) == 1

        gg = result["grain_groups"][0]

        # Validate aggregability and grain
        assert gg["aggregability"] == "full"
        assert gg["grain"] == ["name"]
        assert sorted(gg["metrics"]) == ["v3.total_quantity", "v3.total_revenue"]

        # Validate columns
        assert gg["columns"] == [
            {
                "name": "name",
                "type": "string",
                "semantic_entity": "v3.customer.name",
                "semantic_type": "dimension",
            },
            {
                "name": "line_total_sum_e1f61696",
                "type": "double",
                "semantic_entity": "v3.total_revenue:line_total_sum_e1f61696",
                "semantic_type": "metric_component",
            },
            {
                "name": "quantity_sum_06b64d2e",
                "type": "bigint",
                "semantic_entity": "v3.total_quantity:quantity_sum_06b64d2e",
                "semantic_type": "metric_component",
            },
        ]

        # Validate SQL
        assert_sql_equal(
            gg["sql"],
            """
            WITH
            v3_customer AS (
                SELECT customer_id, name
                FROM default.v3.customers
            ),
            v3_order_details AS (
                SELECT o.customer_id, oi.quantity, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t2.name, SUM(t1.line_total) line_total_sum_e1f61696, SUM(t1.quantity) quantity_sum_06b64d2e
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_customer t2 ON t1.customer_id = t2.customer_id
            GROUP BY t2.name
            """,
        )

        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue", "v3.total_quantity"],
                "dimensions": ["v3.customer.name"],
                "filters": ["v3.customer.name = 'Abcd'"],
            },
        )
        result = response.json()
        gg = result["grain_groups"][0]

        # Validate SQL
        assert_sql_equal(
            gg["sql"],
            """
            WITH
            v3_customer AS (
                SELECT customer_id, name
                FROM default.v3.customers
                WHERE name = 'Abcd'
            ),
            v3_order_details AS (
                SELECT o.customer_id, oi.quantity, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t2.name, SUM(t1.line_total) line_total_sum_e1f61696, SUM(t1.quantity) quantity_sum_06b64d2e
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_customer t2 ON t1.customer_id = t2.customer_id
            WHERE  t2.name = 'Abcd'
            GROUP BY t2.name
            """,
        )

        # Validate requested_dimensions
        assert result["requested_dimensions"] == ["v3.customer.name"]

    @pytest.mark.asyncio
    async def test_multiple_metrics_multiple_dimensions(self, client_with_build_v3):
        """
        Test multiple metrics with multiple dimensions.

        Query: revenue and quantity by status and customer name
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue", "v3.total_quantity"],
                "dimensions": [
                    "v3.order_details.status",
                    "v3.customer.name",
                ],
            },
        )

        assert response.status_code == 200
        data = get_first_grain_group(response.json())

        assert_sql_equal(
            data["sql"],
            """
            WITH
            v3_customer AS (
                SELECT customer_id, name
                FROM default.v3.customers
            ),
            v3_order_details AS (
                SELECT o.customer_id, o.status, oi.quantity, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t1.status, t2.name, SUM(t1.line_total) line_total_sum_e1f61696, SUM(t1.quantity) quantity_sum_06b64d2e
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_customer t2 ON t1.customer_id = t2.customer_id
            GROUP BY t1.status, t2.name
            """,
        )
        assert data["columns"] == [
            {
                "name": "status",
                "type": "string",
                "semantic_entity": "v3.order_details.status",
                "semantic_type": "dimension",
            },
            {
                "name": "name",
                "type": "string",
                "semantic_entity": "v3.customer.name",
                "semantic_type": "dimension",
            },
            {
                "name": "line_total_sum_e1f61696",
                "type": "double",
                "semantic_entity": "v3.total_revenue:line_total_sum_e1f61696",
                "semantic_type": "metric_component",
            },
            {
                "name": "quantity_sum_06b64d2e",
                "type": "bigint",
                "semantic_entity": "v3.total_quantity:quantity_sum_06b64d2e",
                "semantic_type": "metric_component",
            },
        ]

    @pytest.mark.asyncio
    async def test_complex_multi_dimension_multi_role_query(self, client_with_build_v3):
        """
        Test a complex query with multiple dimensions across different roles:
        - Local dimension: status
        - Customer name (via customer role)
        - Order date month (via order role)
        - Customer registration year (via customer->registration multi-hop)
        - Customer home country (via customer->home multi-hop)

        Uses only FULL aggregability metrics (total_revenue, total_quantity, customer_count)
        to keep the test simple (single grain group).
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": [
                    "v3.total_revenue",
                    "v3.total_quantity",
                    "v3.customer_count",
                ],
                "dimensions": [
                    "v3.order_details.status",
                    "v3.customer.name",
                    "v3.date.month[order]",
                    "v3.date.year[customer->registration]",
                    "v3.location.country[customer->home]",
                ],
            },
        )

        assert response.status_code == 200
        result = response.json()

        # Should have 1 grain group (all FULL aggregability)
        assert len(result["grain_groups"]) == 1

        gg = result["grain_groups"][0]

        # Validate aggregability
        assert gg["aggregability"] == "full"

        # Should have 5 dimensions + 3 metrics = 8 columns
        assert len(gg["columns"]) == 8

        # The SQL should have multi-hop joins:
        # - t2: v3_customer for customer.name (direct)
        # - t3: v3_date for date.month[order] (direct)
        # - t4: v3_customer -> t5: v3_date for date.year[customer->registration] (multi-hop)
        # - t6: v3_customer -> t7: v3_location for location.country[customer->home] (multi-hop)
        assert_sql_equal(
            gg["sql"],
            """
        WITH
        v3_customer AS (
        SELECT  customer_id,
            name,
            registration_date,
            location_id
        FROM default.v3.customers
        ),
        v3_date AS (
        SELECT  date_id,
            month,
            year
        FROM default.v3.dates
        ),
        v3_location AS (
        SELECT  location_id,
            country
        FROM default.v3.locations
        ),
        v3_order_details AS (
        SELECT  o.customer_id,
            o.order_date,
            o.status,
            oi.quantity,
            oi.quantity * oi.unit_price AS line_total
        FROM default.v3.orders o JOIN default.v3.order_items oi ON o.order_id = oi.order_id
        )

        SELECT  t1.status,
            t2.name,
            t3.month month_order,
            t4.year year_registration,
            t5.country country_home,
            SUM(t1.line_total) line_total_sum_e1f61696,
            SUM(t1.quantity) quantity_sum_06b64d2e,
            hll_sketch_agg(t1.customer_id) customer_id_hll_23002251
        FROM v3_order_details t1 LEFT OUTER JOIN v3_customer t2 ON t1.customer_id = t2.customer_id
        LEFT OUTER JOIN v3_date t3 ON t1.order_date = t3.date_id
        LEFT OUTER JOIN v3_date t4 ON t2.registration_date = t4.date_id
        LEFT OUTER JOIN v3_location t5 ON t2.location_id = t5.location_id
        GROUP BY  t1.status, t2.name, t3.month, t4.year, t5.country
        """,
        )

        # Check all dimension semantic entities
        dim_entities = [
            c["semantic_entity"]
            for c in gg["columns"]
            if c["semantic_type"] == "dimension"
        ]
        assert "v3.order_details.status" in dim_entities
        assert "v3.customer.name" in dim_entities
        assert "v3.date.month[order]" in dim_entities
        assert "v3.date.year[customer->registration]" in dim_entities
        assert "v3.location.country[customer->home]" in dim_entities

        # Check all metrics present
        metric_names = [
            c["name"] for c in gg["columns"] if c["semantic_type"] == "metric_component"
        ]
        assert set(metric_names) == {
            "line_total_sum_e1f61696",
            "quantity_sum_06b64d2e",
            "customer_id_hll_23002251",
        }

        # Validate requested_dimensions
        assert result["requested_dimensions"] == [
            "v3.order_details.status",
            "v3.customer.name",
            "v3.date.month[order]",
            "v3.date.year[customer->registration]",
            "v3.location.country[customer->home]",
        ]

    @pytest.mark.asyncio
    async def test_dim_link_with_coalesce_in_join_sql(
        self,
        client_with_build_v3,
    ):
        """A dimension link whose ``join_sql`` uses ``COALESCE`` over two
        fact columns should keep both FK columns in the parent CTE's
        projection so the JOIN clause can reference them at the outer
        level.  Documents whether DJ supports non-column expressions in
        dim-link join_sql.
        """
        resp = await client_with_build_v3.post(
            "/nodes/v3.order_details/link",
            json={
                "dimension_node": "v3.location",
                "join_on": (
                    "COALESCE(v3.order_details.from_location_id, "
                    "v3.order_details.to_location_id) = v3.location.location_id"
                ),
                "role": "either",
            },
        )
        if resp.status_code not in (200, 201):
            pytest.skip(
                f"COALESCE in dim-link join_sql not supported: {resp.json()}",
            )
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.location.country[either]"],
            },
        )
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            get_first_grain_group(response.json())["sql"],
            """
            WITH
            v3_location AS (
              SELECT location_id, country FROM default.v3.locations
            ),
            v3_order_details AS (
              SELECT o.from_location_id,
                o.to_location_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t2.country country_either,
              SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_location t2
              ON COALESCE(t1.from_location_id, t1.to_location_id) = t2.location_id
            GROUP BY t2.country
            """,
        )


class TestMeasuresSQLSkipJoin:
    """
    Tests covering the dimension-join skip optimization for /sql/measures/v3/.

    The optimization peels trailing hops off the join path when the requested
    dimension column is foreign-key-aligned with a column on the previous node.
    """

    @pytest.fixture
    async def setup_loyalty_status_chain(self, client_with_build_v3):
        """
        Adds an extra dimension `v3.loyalty_status` linked from `v3.customer`
        on customer_id (the PK of both). Used to exercise multi-hop FK
        skipping where the requested column flows through every link.

        Defined locally so only the multi-hop tests pay the setup cost; the
        global BUILD_V3 fixture is unaffected.
        """
        response = await client_with_build_v3.post(
            "/nodes/source/",
            json={
                "name": "v3.src_loyalty_status",
                "description": "Per-customer loyalty membership tier and standing",
                "columns": [
                    {"name": "customer_id", "type": "int"},
                    {"name": "tier", "type": "string"},
                    {"name": "standing", "type": "string"},
                ],
                "mode": "published",
                "catalog": "default",
                "schema_": "v3",
                "table": "loyalty_status",
            },
        )
        assert response.status_code in (200, 201, 409)

        response = await client_with_build_v3.post(
            "/nodes/dimension/",
            json={
                "name": "v3.loyalty_status",
                "description": (
                    "Customer loyalty status dimension keyed on customer_id"
                ),
                "query": (
                    "SELECT customer_id, tier, standing FROM v3.src_loyalty_status"
                ),
                "primary_key": ["customer_id"],
                "mode": "published",
            },
        )
        assert response.status_code in (200, 201, 409)

        response = await client_with_build_v3.post(
            "/nodes/v3.customer/link",
            json={
                "dimension_node": "v3.loyalty_status",
                "join_type": "left",
                "join_on": ("v3.customer.customer_id = v3.loyalty_status.customer_id"),
                "join_cardinality": "one_to_one",
            },
        )
        assert response.status_code in (200, 201, 409)

    @pytest.mark.asyncio
    async def test_skip_join_multi_hop_fk_chain(
        self,
        client_with_build_v3,
        setup_loyalty_status_chain,
    ):
        """
        Multi-hop full skip over /sql/measures/v3/.

        Chain: v3.order_details -> v3.customer -> v3.loyalty_status. Both links
        align customer_id (the PK of customer and of loyalty_status), so the
        requested v3.loyalty_status.customer_id resolves directly to the fact's
        customer_id column with no joins emitted.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.loyalty_status.customer_id"],
            },
        )

        assert response.status_code == 200, response.json()
        data = get_first_grain_group(response.json())

        assert_sql_equal(
            data["sql"],
            """
            WITH v3_order_details AS (
                SELECT o.customer_id, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t1.customer_id, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            GROUP BY t1.customer_id
            """,
        )

    # Note: partial multi-hop skipping is exercised by tests in tests/api/sql_test.py
    # (test_metric_with_joinable_dimension_partial_skip_multiple_hops). The roads
    # schema there cleanly forces the multi-hop path, whereas the v3 fixture's
    # multi-role v3.location dim makes the path finder ambiguous.

    @pytest.mark.asyncio
    async def test_skip_join_multi_hop_breaks_on_non_fk_column(
        self,
        client_with_build_v3,
        setup_loyalty_status_chain,
    ):
        """
        Negative case: requested terminal column is NOT FK-aligned with any
        intermediate, so no joins are skipped.

        v3.loyalty_status.tier is a non-key attribute. Walking the chain
        backwards from loyalty_status.tier fails on the very first hop, so
        both joins remain.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.loyalty_status.tier"],
            },
        )

        assert response.status_code == 200, response.json()
        data = get_first_grain_group(response.json())

        assert_sql_equal(
            data["sql"],
            """
            WITH v3_customer AS (
                SELECT customer_id FROM default.v3.customers
            ),
            v3_loyalty_status AS (
                SELECT customer_id, tier FROM default.v3.loyalty_status
            ),
            v3_order_details AS (
                SELECT o.customer_id, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t3.tier, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_customer t2 ON t1.customer_id = t2.customer_id
            LEFT OUTER JOIN v3_loyalty_status t3 ON t2.customer_id = t3.customer_id
            GROUP BY t3.tier
            """,
        )


class TestMeasuresSQLRoles:
    @pytest.mark.asyncio
    async def test_dimensions_with_multiple_roles_same_dimension(
        self,
        client_with_build_v3,
    ):
        """
        Test querying with multiple roles to the same dimension type.

        Uses both from_location and to_location (both link to v3.location with different roles).
        Also includes the order date.

        - total_revenue: SUM - FULL aggregability
        - order_count: COUNT(DISTINCT order_id) - LIMITED aggregability

        With grain group merging, this produces 1 merged grain group.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue", "v3.order_count"],
                "dimensions": [
                    "v3.date.month[order]",  # Order date month
                    "v3.location.country[from]",  # From location country
                    "v3.location.country[to]",  # To location country
                ],
            },
        )

        assert response.status_code == 200
        result = response.json()

        # With merging, should have 1 merged grain group at finest grain
        assert len(result["grain_groups"]) == 1

        gg = result["grain_groups"][0]

        # Merged group has LIMITED aggregability
        assert gg["aggregability"] == "limited"
        assert sorted(gg["grain"]) == [
            "country_from",
            "country_to",
            "month_order",
            "order_id",
        ]
        assert sorted(gg["metrics"]) == ["v3.order_count", "v3.total_revenue"]

        # Validate requested_dimensions
        assert result["requested_dimensions"] == [
            "v3.date.month[order]",
            "v3.location.country[from]",
            "v3.location.country[to]",
        ]

    @pytest.mark.asyncio
    async def test_dimensions_with_different_date_roles(self, client_with_build_v3):
        """
        Test querying order date vs customer registration date (different roles to same dimension).

        Dimension links:
        - v3.order_details -> v3.date with role "order" (direct)
        - v3.order_details -> v3.customer -> v3.date with role "registration" (multi-hop)

        Only total_revenue (FULL), so 1 grain group.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": [
                    "v3.date.year[order]",  # Order year (direct)
                    "v3.date.year[customer->registration]",  # Customer registration year (multi-hop)
                ],
            },
        )

        assert response.status_code == 200
        result = response.json()

        # Should have 1 grain group (FULL aggregability)
        assert len(result["grain_groups"]) == 1

        gg = result["grain_groups"][0]

        # Validate aggregability and grain
        assert gg["aggregability"] == "full"
        assert gg["grain"] == ["year_order", "year_registration"]
        assert gg["metrics"] == ["v3.total_revenue"]

        # Validate columns
        assert gg["columns"] == [
            {
                "name": "year_order",
                "type": "int",
                "semantic_entity": "v3.date.year[order]",
                "semantic_type": "dimension",
            },
            {
                "name": "year_registration",
                "type": "int",
                "semantic_entity": "v3.date.year[customer->registration]",
                "semantic_type": "dimension",
            },
            {
                "name": "line_total_sum_e1f61696",
                "type": "double",
                "semantic_entity": "v3.total_revenue:line_total_sum_e1f61696",
                "semantic_type": "metric_component",
            },
        ]

        # Validate SQL - two separate joins to v3_date for different roles
        assert_sql_equal(
            gg["sql"],
            """
            WITH
            v3_customer AS (
                SELECT customer_id, registration_date
                FROM default.v3.customers
            ),
            v3_date AS (
                SELECT date_id, year
                FROM default.v3.dates
            ),
            v3_order_details AS (
                SELECT o.customer_id, o.order_date, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t2.year year_order, t4.year year_registration, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_date t2 ON t1.order_date = t2.date_id
            LEFT OUTER JOIN v3_customer t3 ON t1.customer_id = t3.customer_id
            LEFT OUTER JOIN v3_date t4 ON t3.registration_date = t4.date_id
            GROUP BY t2.year, t4.year
            """,
        )

        # Validate requested_dimensions
        assert result["requested_dimensions"] == [
            "v3.date.year[order]",
            "v3.date.year[customer->registration]",
        ]

    @pytest.mark.asyncio
    async def test_multi_hop_location_dimension(self, client_with_build_v3):
        """
        Test multi-hop dimension path: order_details -> customer -> location (customer's home).

        Compare with direct location roles (from/to) vs the multi-hop customer home location.
        Only total_revenue (FULL), so 1 grain group.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": [
                    "v3.location.country[from]",  # From location (direct)
                    "v3.location.country[customer->home]",  # Customer's home location (multi-hop)
                ],
            },
        )

        assert response.status_code == 200
        result = response.json()

        # Should have 1 grain group (FULL aggregability)
        assert len(result["grain_groups"]) == 1

        gg = result["grain_groups"][0]

        # Validate aggregability and grain
        assert gg["aggregability"] == "full"
        assert gg["grain"] == ["country_from", "country_home"]
        assert gg["metrics"] == ["v3.total_revenue"]

        # Validate columns
        assert gg["columns"] == [
            {
                "name": "country_from",
                "type": "string",
                "semantic_entity": "v3.location.country[from]",
                "semantic_type": "dimension",
            },
            {
                "name": "country_home",
                "type": "string",
                "semantic_entity": "v3.location.country[customer->home]",
                "semantic_type": "dimension",
            },
            {
                "name": "line_total_sum_e1f61696",
                "type": "double",
                "semantic_entity": "v3.total_revenue:line_total_sum_e1f61696",
                "semantic_type": "metric_component",
            },
        ]

        # Validate SQL
        assert_sql_equal(
            gg["sql"],
            """
            WITH
            v3_customer AS (
                SELECT customer_id, location_id
                FROM default.v3.customers
            ),
            v3_location AS (
                SELECT location_id, country
                FROM default.v3.locations
            ),
            v3_order_details AS (
                SELECT o.customer_id, o.from_location_id, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t2.country country_from, t4.country country_home, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_location t2 ON t1.from_location_id = t2.location_id
            LEFT OUTER JOIN v3_customer t3 ON t1.customer_id = t3.customer_id
            LEFT OUTER JOIN v3_location t4 ON t3.location_id = t4.location_id
            GROUP BY t2.country, t4.country
            """,
        )

        # Validate requested_dimensions
        assert result["requested_dimensions"] == [
            "v3.location.country[from]",
            "v3.location.country[customer->home]",
        ]

    @pytest.mark.asyncio
    async def test_filter_on_multi_hop_dimension_column(self, client_with_build_v3):
        """
        Test that a filter on a dimension column reachable only via a 2-hop
        DimensionLink path (order_details -> customer -> location) is correctly
        pushed into the WHERE clause of the outer query.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.location.country[customer->home]"],
                "filters": ["v3.location.country[customer->home] = 'US'"],
            },
        )
        assert response.status_code == 200
        data = get_first_grain_group(response.json())

        # The filter on the 2-hop dimension column should appear in the WHERE clause,
        # referencing the aliased join table (t3), not the raw dimension node name.
        assert_sql_equal(
            data["sql"],
            """
            WITH
            v3_customer AS (
                SELECT customer_id, location_id
                FROM default.v3.customers
            ),
            v3_location AS (
                SELECT location_id, country
                FROM default.v3.locations
                WHERE country = 'US'
            ),
            v3_order_details AS (
                SELECT o.customer_id, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t3.country country_home, SUM(t1.line_total) line_total_sum_HASH
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_customer t2 ON t1.customer_id = t2.customer_id
            LEFT OUTER JOIN v3_location t3 ON t2.location_id = t3.location_id
            WHERE t3.country = 'US'
            GROUP BY t3.country
            """,
            normalize_aliases=True,
        )

    @pytest.mark.asyncio
    async def test_all_location_roles_in_single_query(self, client_with_build_v3):
        """
        Test querying all three location roles in a single query:
        - from location (direct, role="from")
        - to location (direct, role="to")
        - customer home location (multi-hop, role="customer->home")

        This tests that we can have 3 joins to the same dimension table with different paths.
        Only total_revenue (FULL), so 1 grain group.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": [
                    "v3.location.city[from]",
                    "v3.location.city[to]",
                    "v3.location.city[customer->home]",
                ],
            },
        )

        assert response.status_code == 200
        result = response.json()

        # Should have 1 grain group (FULL aggregability)
        assert len(result["grain_groups"]) == 1

        gg = result["grain_groups"][0]

        # Validate aggregability and grain
        assert gg["aggregability"] == "full"
        assert gg["grain"] == ["city_from", "city_home", "city_to"]
        assert gg["metrics"] == ["v3.total_revenue"]

        # Validate columns - 3 dimensions 1 metric
        assert len(gg["columns"]) == 4
        assert gg["columns"][0]["semantic_entity"] == "v3.location.city[from]"
        assert gg["columns"][0]["name"] == "city_from"
        assert gg["columns"][1]["semantic_entity"] == "v3.location.city[to]"
        assert gg["columns"][1]["name"] == "city_to"
        assert gg["columns"][2]["semantic_entity"] == "v3.location.city[customer->home]"
        assert gg["columns"][2]["name"] == "city_home"
        assert (
            gg["columns"][3]["semantic_entity"]
            == "v3.total_revenue:line_total_sum_e1f61696"
        )
        assert gg["columns"][3]["name"] == "line_total_sum_e1f61696"

        # Validate SQL
        assert_sql_equal(
            gg["sql"],
            """
            WITH
            v3_customer AS (
                SELECT customer_id, location_id
                FROM default.v3.customers
            ),
            v3_location AS (
                SELECT location_id, city
                FROM default.v3.locations
            ),
            v3_order_details AS (
                SELECT o.customer_id, o.from_location_id, o.to_location_id,
                       oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t2.city city_from, t3.city city_to, t5.city city_home,
                   SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_location t2 ON t1.from_location_id = t2.location_id
            LEFT OUTER JOIN v3_location t3 ON t1.to_location_id = t3.location_id
            LEFT OUTER JOIN v3_customer t4 ON t1.customer_id = t4.customer_id
            LEFT OUTER JOIN v3_location t5 ON t4.location_id = t5.location_id
            GROUP BY t2.city, t3.city, t5.city
            """,
        )

        # Validate requested_dimensions
        assert result["requested_dimensions"] == [
            "v3.location.city[from]",
            "v3.location.city[to]",
            "v3.location.city[customer->home]",
        ]


class TestMeasuresSQLMultipleMetrics:
    @pytest.mark.asyncio
    async def test_two_metrics_same_parent(self, client_with_build_v3):
        """
        Test requesting two metrics from the same parent node.

        Query: total_revenue and total_quantity by status
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue", "v3.total_quantity"],
                "dimensions": ["v3.order_details.status"],
            },
        )

        assert response.status_code == 200
        data = get_first_grain_group(response.json())

        # Parse and compare SQL structure
        # Both metrics are single-component, so they use metric names
        assert_sql_equal(
            data["sql"],
            """
            WITH v3_order_details AS (
                SELECT o.status, oi.quantity, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t1.status,
                   SUM(t1.line_total) line_total_sum_e1f61696,
                   SUM(t1.quantity) quantity_sum_06b64d2e
            FROM v3_order_details t1
            GROUP BY t1.status
            """,
        )

        assert "_DOT_" not in data["sql"]
        assert data["columns"] == [
            {
                "name": "status",
                "type": "string",
                "semantic_entity": "v3.order_details.status",
                "semantic_type": "dimension",
            },
            {
                "name": "line_total_sum_e1f61696",
                "type": "double",
                "semantic_entity": "v3.total_revenue:line_total_sum_e1f61696",
                "semantic_type": "metric_component",
            },
            {
                "name": "quantity_sum_06b64d2e",
                "type": "bigint",
                "semantic_entity": "v3.total_quantity:quantity_sum_06b64d2e",
                "semantic_type": "metric_component",
            },
        ]

    @pytest.mark.asyncio
    async def test_three_metrics_same_parent(self, client_with_build_v3):
        """
        Test requesting three metrics from the same parent node.

        Query: revenue, quantity, and order_count by status

        - total_revenue: SUM - FULL aggregability
        - total_quantity: SUM - FULL aggregability
        - order_count: COUNT(DISTINCT order_id) - LIMITED aggregability

        With grain group merging, this produces 1 merged grain group at the finest grain.
        All metrics from the same parent are merged into one CTE with raw values.
        Aggregations are applied in the final metrics SQL layer.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue", "v3.total_quantity", "v3.order_count"],
                "dimensions": ["v3.order_details.status"],
            },
        )

        assert response.status_code == 200
        result = response.json()

        # With merging, should have 1 merged grain group at finest grain (LIMITED)
        assert len(result["grain_groups"]) == 1

        gg = result["grain_groups"][0]

        # Merged group has LIMITED aggregability (worst case)
        assert gg["aggregability"] == "limited"
        assert gg["grain"] == ["order_id", "status"]
        assert sorted(gg["metrics"]) == [
            "v3.order_count",
            "v3.total_quantity",
            "v3.total_revenue",
        ]

        # Columns: dimension grain column 3 raw metric columns
        assert len(gg["columns"]) == 4
        assert gg["columns"][0] == {
            "name": "status",
            "type": "string",
            "semantic_entity": "v3.order_details.status",
            "semantic_type": "dimension",
        }
        assert gg["columns"][1] == {
            "name": "order_id",
            "type": "int",
            "semantic_entity": "v3.order_details.order_id",
            "semantic_type": "dimension",
        }

        # Raw metric columns (for later aggregation)
        assert_sql_equal(
            gg["sql"],
            """
            WITH v3_order_details AS (
                SELECT o.order_id, o.status, oi.quantity, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t1.status, t1.order_id, SUM(t1.line_total) line_total_sum_e1f61696, SUM(t1.quantity) quantity_sum_06b64d2e
            FROM v3_order_details t1
            GROUP BY t1.status, t1.order_id
            """,
        )

        # Validate requested_dimensions
        assert result["requested_dimensions"] == ["v3.order_details.status"]

        # Validate components are included for materialization planning
        assert "components" in gg
        assert len(gg["components"]) == 3

        # Sort by name for deterministic comparison
        components = sorted(gg["components"], key=lambda c: c["name"])

        # order_count component (LIMITED - grain column)
        assert components[0] == {
            "name": "line_total_sum_e1f61696",
            "expression": "line_total",
            "aggregation": "SUM",
            "merge": "SUM",
            "aggregability": "full",
        }

        # total_revenue component
        assert components[1] == {
            "name": "order_id",
            "expression": "order_id",
            "aggregation": None,
            "merge": None,
            "aggregability": "limited",
        }

        # total_quantity component
        assert components[2] == {
            "name": "quantity_sum_06b64d2e",
            "expression": "quantity",
            "aggregation": "SUM",
            "merge": "SUM",
            "aggregability": "full",
        }

    @pytest.mark.asyncio
    async def test_page_views_full_metrics(self, client_with_build_v3):
        """
        Test FULL aggregability metrics from page_views_enriched.

        Only tests page_view_count and product_view_count (FULL).
        Uses page_type as dimension (available on the transform).
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.page_view_count", "v3.product_view_count"],
                "dimensions": ["v3.page_views_enriched.page_type"],
            },
        )

        assert response.status_code == 200
        result = response.json()

        # Should have 1 grain group (FULL aggregability)
        assert len(result["grain_groups"]) == 1

        gg = result["grain_groups"][0]

        # Validate aggregability and grain
        assert gg["aggregability"] == "full"
        assert gg["grain"] == ["page_type"]
        assert sorted(gg["metrics"]) == ["v3.page_view_count", "v3.product_view_count"]

        # Validate columns
        assert gg["columns"] == [
            {
                "name": "page_type",
                "type": "string",
                "semantic_entity": "v3.page_views_enriched.page_type",
                "semantic_type": "dimension",
            },
            {
                "name": "view_id_count_f41e2db4",
                "type": "bigint",
                "semantic_entity": "v3.page_view_count:view_id_count_f41e2db4",
                "semantic_type": "metric_component",
            },
            {
                "name": "is_product_view_sum_eb3a4b41",
                "type": "bigint",
                "semantic_entity": "v3.product_view_count:is_product_view_sum_eb3a4b41",
                "semantic_type": "metric_component",
            },
        ]

        # Validate SQL
        assert_sql_equal(
            gg["sql"],
            """
            WITH v3_page_views_enriched AS (
              SELECT
                view_id,
                page_type,
                CASE WHEN page_type = 'product' THEN 1 ELSE 0 END AS is_product_view
              FROM default.v3.page_views
            )
            SELECT
              t1.page_type,
              COUNT(t1.view_id) view_id_count_f41e2db4,
              SUM(t1.is_product_view) is_product_view_sum_eb3a4b41
            FROM v3_page_views_enriched t1
            GROUP BY t1.page_type
            """,
        )

        # Validate requested_dimensions
        assert result["requested_dimensions"] == ["v3.page_views_enriched.page_type"]

    @pytest.mark.asyncio
    async def test_order_details_metrics_with_three_dimensions(
        self,
        client_with_build_v3,
    ):
        """
        Test order_details base metrics with three dimensions:
        - status (local)
        - customer name (joined via customer)
        - product category (joined via product)

        Metrics have different aggregabilities:
        - total_revenue: SUM - FULL
        - total_quantity: SUM - FULL
        - customer_count: APPROX_COUNT_DISTINCT - FULL
        - order_count: COUNT(DISTINCT order_id) - LIMITED

        With grain group merging, this produces 1 merged grain group at finest grain.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ORDER_DETAILS_BASE_METRICS,
                "dimensions": [
                    "v3.order_details.status",
                    "v3.customer.name",
                    "v3.product.category",
                ],
            },
        )

        assert response.status_code == 200
        result = response.json()

        # With merging, should have 1 merged grain group at finest grain (LIMITED)
        assert len(result["grain_groups"]) == 1

        gg = result["grain_groups"][0]

        # Merged group has LIMITED aggregability (worst case)
        assert gg["aggregability"] == "limited"
        assert sorted(gg["grain"]) == ["category", "name", "order_id", "status"]
        assert sorted(gg["metrics"]) == [
            "v3.customer_count",
            "v3.order_count",
            "v3.total_quantity",
            "v3.total_revenue",
        ]
        assert "_DOT_" not in gg["sql"]

        # Validate requested_dimensions
        assert result["requested_dimensions"] == [
            "v3.order_details.status",
            "v3.customer.name",
            "v3.product.category",
        ]


class TestMeasuresSQLCrossFact:
    @pytest.mark.asyncio
    async def test_cross_fact_metrics_two_parents(self, client_with_build_v3):
        """
        Test metrics from different parent nodes return separate grain groups.

        Query: total_revenue (from order_details) and page_view_count (from page_views)

        This produces two grain groups, one for each parent node.

        Both facts link to v3.product (without roles), so we can use that as a
        shared dimension.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue", "v3.page_view_count"],
                "dimensions": ["v3.product.category"],
            },
        )

        assert response.status_code == 200
        result = response.json()

        # Should have exactly two grain groups (one per parent fact)
        assert len(result["grain_groups"]) == 2

        # Find grain groups by metric for predictable assertions
        gg_by_metric = {gg["metrics"][0]: gg for gg in result["grain_groups"]}

        # Validate grain group for total_revenue (from order_details)
        gg_revenue = gg_by_metric["v3.total_revenue"]
        assert gg_revenue["aggregability"] == "full"
        assert gg_revenue["grain"] == ["category"]
        assert gg_revenue["metrics"] == ["v3.total_revenue"]
        assert gg_revenue["columns"] == [
            {
                "name": "category",
                "type": "string",
                "semantic_entity": "v3.product.category",
                "semantic_type": "dimension",
            },
            {
                "name": "line_total_sum_e1f61696",
                "type": "double",
                "semantic_entity": "v3.total_revenue:line_total_sum_e1f61696",
                "semantic_type": "metric_component",
            },
        ]
        assert_sql_equal(
            gg_revenue["sql"],
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
            )
            SELECT t2.category, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            GROUP BY t2.category
            """,
        )

        # Validate grain group for page_view_count (from page_views_enriched)
        gg_pageviews = gg_by_metric["v3.page_view_count"]
        assert gg_pageviews["aggregability"] == "full"
        assert gg_pageviews["grain"] == ["category"]
        assert gg_pageviews["metrics"] == ["v3.page_view_count"]
        assert gg_pageviews["columns"] == [
            {
                "name": "category",
                "type": "string",
                "semantic_entity": "v3.product.category",
                "semantic_type": "dimension",
            },
            {
                "name": "view_id_count_f41e2db4",
                "type": "bigint",
                "semantic_entity": "v3.page_view_count:view_id_count_f41e2db4",
                "semantic_type": "metric_component",
            },
        ]
        assert_sql_equal(
            gg_pageviews["sql"],
            """
            WITH
            v3_page_views_enriched AS (
                SELECT view_id, product_id
                FROM default.v3.page_views
            ),
            v3_product AS (
                SELECT product_id, category
                FROM default.v3.products
            )
            SELECT t2.category, COUNT(t1.view_id) view_id_count_f41e2db4
            FROM v3_page_views_enriched t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            GROUP BY t2.category
            """,
        )

        # Validate requested_dimensions
        assert result["requested_dimensions"] == ["v3.product.category"]
        assert result["dialect"] == "spark"

    @pytest.mark.asyncio
    async def test_cross_fact_metrics_different_aggregabilities(
        self,
        client_with_build_v3,
    ):
        """
        Test cross-fact metrics with different aggregabilities.

        - total_revenue (from order_details): FULL aggregability
        - session_count (from page_views_enriched): LIMITED aggregability (COUNT DISTINCT)

        Both facts link to v3.product (no role), so v3.product.category is a
        valid shared dimension. The LIMITED metric adds session_id to its grain.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue", "v3.session_count"],
                "dimensions": ["v3.product.category"],
            },
        )

        assert response.status_code == 200
        result = response.json()

        # Should have exactly two grain groups (different aggregability different facts)
        assert len(result["grain_groups"]) == 2

        # Find grain groups by metric for predictable assertions
        gg_by_metric = {gg["metrics"][0]: gg for gg in result["grain_groups"]}

        # Validate grain group for total_revenue (FULL aggregability from order_details)
        gg_revenue = gg_by_metric["v3.total_revenue"]
        assert gg_revenue["aggregability"] == "full"
        assert gg_revenue["grain"] == ["category"]
        assert gg_revenue["metrics"] == ["v3.total_revenue"]
        assert gg_revenue["columns"] == [
            {
                "name": "category",
                "type": "string",
                "semantic_entity": "v3.product.category",
                "semantic_type": "dimension",
            },
            {
                "name": "line_total_sum_e1f61696",
                "type": "double",
                "semantic_entity": "v3.total_revenue:line_total_sum_e1f61696",
                "semantic_type": "metric_component",
            },
        ]
        assert_sql_equal(
            gg_revenue["sql"],
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
            )
            SELECT t2.category, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            GROUP BY t2.category
            """,
        )

        # Validate grain group for session_count (LIMITED aggregability from page_views)
        # COUNT DISTINCT requires session_id in GROUP BY for re-aggregation
        gg_sessions = gg_by_metric["v3.session_count"]
        assert gg_sessions["aggregability"] == "limited"
        assert gg_sessions["grain"] == ["category", "session_id"]
        assert gg_sessions["metrics"] == ["v3.session_count"]
        assert gg_sessions["columns"] == [
            {
                "name": "category",
                "type": "string",
                "semantic_entity": "v3.product.category",
                "semantic_type": "dimension",
            },
            {
                "name": "session_id",
                "type": "string",
                "semantic_entity": "v3.page_views_enriched.session_id",
                "semantic_type": "dimension",
            },
        ]
        assert_sql_equal(
            gg_sessions["sql"],
            """
            WITH
            v3_page_views_enriched AS (
                SELECT session_id, product_id
                FROM default.v3.page_views
            ),
            v3_product AS (
                SELECT product_id, category
                FROM default.v3.products
            )
            SELECT t2.category, t1.session_id
            FROM v3_page_views_enriched t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            GROUP BY t2.category, t1.session_id
            """,
        )

        # Validate requested_dimensions
        assert result["requested_dimensions"] == ["v3.product.category"]
        assert result["dialect"] == "spark"

    @pytest.mark.asyncio
    async def test_cross_fact_derived_metric(self, client_with_build_v3):
        """
        Test a derived metric that combines metrics from different facts.

        v3.conversion_rate = v3.order_count / v3.visitor_count
        - order_count (COUNT DISTINCT order_id) comes from order_details
        - visitor_count (COUNT DISTINCT customer_id) comes from page_views_enriched

        Both base metrics have LIMITED aggregability, so their level columns
        (order_id and customer_id) are added to the grain for re-aggregation.

        Using v3.product.category as dimension since both facts link to product.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.conversion_rate"],
                "dimensions": ["v3.product.category"],
            },
        )

        assert response.status_code == 200
        result = response.json()

        # Should have exactly two grain groups (one per base metric's parent fact)
        assert len(result["grain_groups"]) == 2

        # Find grain groups by metric for predictable assertions
        gg_by_metric = {gg["metrics"][0]: gg for gg in result["grain_groups"]}

        # Validate grain group for order_count (LIMITED aggregability from order_details)
        # COUNT DISTINCT order_id requires order_id in GROUP BY
        gg_orders = gg_by_metric["v3.order_count"]
        assert gg_orders["aggregability"] == "limited"
        assert gg_orders["grain"] == ["category", "order_id"]
        assert gg_orders["metrics"] == ["v3.order_count"]
        assert gg_orders["columns"] == [
            {
                "name": "category",
                "type": "string",
                "semantic_entity": "v3.product.category",
                "semantic_type": "dimension",
            },
            {
                "name": "order_id",
                "type": "int",
                "semantic_entity": "v3.order_details.order_id",
                "semantic_type": "dimension",
            },
        ]
        # Joins order_details -> product for category dimension
        assert_sql_equal(
            gg_orders["sql"],
            """
            WITH
            v3_order_details AS (
                SELECT o.order_id, oi.product_id
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
                SELECT product_id, category
                FROM default.v3.products
            )
            SELECT t2.category, t1.order_id
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            GROUP BY t2.category, t1.order_id
            """,
        )

        # Validate grain group for visitor_count (LIMITED aggregability from page_views)
        # COUNT DISTINCT customer_id requires customer_id in GROUP BY
        gg_visitors = gg_by_metric["v3.visitor_count"]
        assert gg_visitors["aggregability"] == "limited"
        assert gg_visitors["grain"] == ["category", "customer_id"]
        assert gg_visitors["metrics"] == ["v3.visitor_count"]
        assert gg_visitors["columns"] == [
            {
                "name": "category",
                "type": "string",
                "semantic_entity": "v3.product.category",
                "semantic_type": "dimension",
            },
            {
                "name": "customer_id",
                "type": "int",
                "semantic_entity": "v3.page_views_enriched.customer_id",
                "semantic_type": "dimension",
            },
        ]
        # Joins page_views_enriched -> product for category dimension
        assert_sql_equal(
            gg_visitors["sql"],
            """
            WITH
            v3_page_views_enriched AS (
                SELECT customer_id, product_id
                FROM default.v3.page_views
            ),
            v3_product AS (
                SELECT product_id, category
                FROM default.v3.products
            )
            SELECT t2.category, t1.customer_id
            FROM v3_page_views_enriched t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            GROUP BY t2.category, t1.customer_id
            """,
        )

        # Validate requested_dimensions
        assert result["requested_dimensions"] == ["v3.product.category"]
        assert result["dialect"] == "spark"

    @pytest.mark.asyncio
    async def test_cross_fact_multiple_derived_metrics(self, client_with_build_v3):
        """
        Test multiple cross-fact derived metrics in the same request.

        v3.conversion_rate = order_count / visitor_count
        v3.revenue_per_visitor = total_revenue / visitor_count
        v3.revenue_per_page_view = total_revenue / page_view_count

        These decompose into base metrics from two facts:
        - From order_details: total_revenue (FULL), order_count (LIMITED)
        - From page_views_enriched: visitor_count (LIMITED), page_view_count (FULL)

        With grain group merging, each parent produces ONE merged grain group
        with raw values at finest grain. Aggregations are applied in metrics SQL.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": [
                    "v3.conversion_rate",
                    "v3.revenue_per_visitor",
                    "v3.revenue_per_page_view",
                ],
                "dimensions": ["v3.product.category"],
            },
        )

        assert response.status_code == 200
        result = response.json()

        # With merging, should have 2 grain groups (one per parent):
        # 1. order_details (merged: total_revenue order_count) at LIMITED grain
        # 2. page_views_enriched (merged: visitor_count page_view_count) at LIMITED grain
        assert len(result["grain_groups"]) == 2

        # Find grain groups by parent name
        gg_order_details = next(
            gg
            for gg in result["grain_groups"]
            if "v3.order_count" in gg["metrics"] or "v3.total_revenue" in gg["metrics"]
        )
        gg_page_views = next(
            gg
            for gg in result["grain_groups"]
            if "v3.visitor_count" in gg["metrics"]
            or "v3.page_view_count" in gg["metrics"]
        )

        # Validate merged grain group for order_details
        assert gg_order_details["aggregability"] == "limited"
        assert gg_order_details["grain"] == ["category", "order_id"]
        assert sorted(gg_order_details["metrics"]) == [
            "v3.order_count",
            "v3.total_revenue",
        ]
        assert_sql_equal(
            gg_order_details["sql"],
            """
            WITH
            v3_order_details AS (
                SELECT o.order_id, oi.product_id, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
                SELECT product_id, category
                FROM default.v3.products
            )
            SELECT t2.category, t1.order_id, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            GROUP BY t2.category, t1.order_id
            """,
        )

        # Validate merged grain group for page_views_enriched
        assert gg_page_views["aggregability"] == "limited"
        assert gg_page_views["grain"] == ["category", "customer_id"]
        assert sorted(gg_page_views["metrics"]) == [
            "v3.page_view_count",
            "v3.visitor_count",
        ]
        assert_sql_equal(
            gg_page_views["sql"],
            """
            WITH
            v3_page_views_enriched AS (
                SELECT view_id, customer_id, product_id
                FROM default.v3.page_views
            ),
            v3_product AS (
                SELECT product_id, category
                FROM default.v3.products
            )
            SELECT t2.category, t1.customer_id, COUNT(t1.view_id) view_id_count_f41e2db4
            FROM v3_page_views_enriched t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            GROUP BY t2.category, t1.customer_id
            """,
        )

        # Validate requested_dimensions
        assert result["requested_dimensions"] == ["v3.product.category"]
        assert result["dialect"] == "spark"


class TestMeasuresSQLComponents:
    @pytest.mark.asyncio
    async def test_multi_component_metric(self, client_with_build_v3):
        """
        Test a metric that decomposes into multiple components.

        AVG(unit_price) decomposes into:
        - COUNT(unit_price) - FULL aggregability
        - SUM(unit_price) - FULL aggregability

        Both components have FULL aggregability, so there's only 1 grain group.
        The measures SQL should output both components with hash-suffixed names,
        and semantic_type should be "metric_component" (not "metric").
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.avg_unit_price"],
                "dimensions": ["v3.order_details.status"],
            },
        )

        assert response.status_code == 200
        result = response.json()

        # Should have 1 grain group (both COUNT and SUM are FULL aggregability)
        assert len(result["grain_groups"]) == 1

        gg = result["grain_groups"][0]

        # Validate aggregability
        assert gg["aggregability"] == "full"

        # Validate grain (just the dimension)
        assert gg["grain"] == ["status"]

        # Validate metrics covered
        assert gg["metrics"] == ["v3.avg_unit_price"]

        # Validate columns: 1 dimension 2 metric components = 3 columns
        assert len(gg["columns"]) == 3
        assert gg["columns"][0] == {
            "name": "status",
            "type": "string",
            "semantic_entity": "v3.order_details.status",
            "semantic_type": "dimension",
        }
        # Components have hash suffixes and semantic_type "metric_component"
        assert gg["columns"][1]["semantic_type"] == "metric_component"
        assert gg["columns"][2]["semantic_type"] == "metric_component"
        assert gg["columns"][1]["name"] == "unit_price_count_55cff00f"
        assert gg["columns"][2]["name"] == "unit_price_sum_55cff00f"

        # semantic_entity should include component info
        assert "v3.avg_unit_price:" in gg["columns"][1]["semantic_entity"]
        assert "v3.avg_unit_price:" in gg["columns"][2]["semantic_entity"]

        # Validate SQL
        assert_sql_equal(
            gg["sql"],
            """
            WITH
            v3_order_details AS (
                SELECT o.status, oi.unit_price
                FROM default.v3.orders o JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t1.status, COUNT(t1.unit_price) unit_price_count_HASH, SUM(t1.unit_price) unit_price_sum_HASH
            FROM v3_order_details t1
            GROUP BY t1.status
            """,
            normalize_aliases=True,
        )

        # Validate requested_dimensions
        assert result["requested_dimensions"] == ["v3.order_details.status"]
        assert result["dialect"] == "spark"

    @pytest.mark.asyncio
    async def test_mixed_single_and_multi_component_metrics(self, client_with_build_v3):
        """
        Test mixing single-component metrics with multi-component metrics.

        - total_revenue: single component (SUM) → semantic_type: "metric"
        - avg_unit_price: multi-component (COUNT SUM) → semantic_type: "metric_component"

        Both are FULL aggregability, so there's only 1 grain group.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue", "v3.avg_unit_price"],
                "dimensions": ["v3.order_details.status"],
            },
        )

        assert response.status_code == 200
        result = response.json()

        # Should have 1 grain group (all FULL aggregability)
        assert len(result["grain_groups"]) == 1

        gg = result["grain_groups"][0]

        # Validate aggregability
        assert gg["aggregability"] == "full"

        # Validate grain
        assert gg["grain"] == ["status"]

        # Validate metrics covered (sorted)
        assert sorted(gg["metrics"]) == ["v3.avg_unit_price", "v3.total_revenue"]

        # Validate columns: 1 dimension 1 single-component metric 2 multi-component metrics = 4 columns
        assert len(gg["columns"]) == 4
        assert gg["columns"][0] == {
            "name": "status",
            "type": "string",
            "semantic_entity": "v3.order_details.status",
            "semantic_type": "dimension",
        }
        # Single-component metric has clean name and type "metric"
        assert gg["columns"][1] == {
            "name": "line_total_sum_e1f61696",
            "type": "double",
            "semantic_entity": "v3.total_revenue:line_total_sum_e1f61696",
            "semantic_type": "metric_component",
        }
        # Multi-component metrics have type "metric_component"
        assert gg["columns"][2]["name"] == "unit_price_count_55cff00f"
        assert gg["columns"][3]["name"] == "unit_price_sum_55cff00f"
        assert (
            gg["columns"][2]["semantic_entity"]
            == "v3.avg_unit_price:unit_price_count_55cff00f"
        )
        assert (
            gg["columns"][3]["semantic_entity"]
            == "v3.avg_unit_price:unit_price_sum_55cff00f"
        )

        assert gg["columns"][2]["semantic_type"] == "metric_component"
        assert gg["columns"][3]["semantic_type"] == "metric_component"

        # Validate SQL
        assert_sql_equal(
            gg["sql"],
            """
            WITH
            v3_order_details AS (
                SELECT o.status, oi.unit_price, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t1.status, SUM(t1.line_total) line_total_sum_e1f61696, COUNT(t1.unit_price) unit_price_count_55cff00f, SUM(t1.unit_price) unit_price_sum_55cff00f
            FROM v3_order_details t1
            GROUP BY t1.status
            """,
            normalize_aliases=False,
        )

        # Validate requested_dimensions
        assert result["requested_dimensions"] == ["v3.order_details.status"]
        assert result["dialect"] == "spark"

    @pytest.mark.asyncio
    async def test_multiple_metrics_with_same_component(self, client_with_build_v3):
        """
        Test metrics that share components.

        - avg_unit_price: decomposes into COUNT(unit_price) SUM(unit_price)
        - total_unit_price: is just SUM(unit_price) (single component)

        Component deduplication is active: SUM(unit_price) appears only ONCE.
        Both metrics share the same SUM component (unit_price_sum_55cff00f).
        The shared component gets the hash suffix from avg_unit_price (multi-component).

        Both are FULL aggregability, so there's only 1 grain group.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.avg_unit_price", "v3.total_unit_price"],
                "dimensions": ["v3.order_details.status"],
            },
        )

        assert response.status_code == 200
        result = response.json()

        # Should have 1 grain group (all FULL aggregability)
        assert len(result["grain_groups"]) == 1

        gg = result["grain_groups"][0]

        # Validate aggregability
        assert gg["aggregability"] == "full"

        # Validate grain
        assert gg["grain"] == ["status"]

        # Validate metrics covered (sorted)
        assert sorted(gg["metrics"]) == ["v3.avg_unit_price", "v3.total_unit_price"]

        # Validate columns: 1 dimension 2 metric columns (WITH sharing) = 3 columns
        # SUM(unit_price) is deduplicated - appears once for both avg_unit_price and total_unit_price
        assert len(gg["columns"]) == 3
        assert gg["columns"][0] == {
            "name": "status",
            "type": "string",
            "semantic_entity": "v3.order_details.status",
            "semantic_type": "dimension",
        }

        # Both components come from avg_unit_price (first encountered, multi-component)
        assert gg["columns"][1] == {
            "name": "unit_price_count_55cff00f",
            "type": "bigint",
            "semantic_entity": "v3.avg_unit_price:unit_price_count_55cff00f",
            "semantic_type": "metric_component",
        }
        # SUM component is shared - deduplicated to one occurrence
        assert gg["columns"][2] == {
            "name": "unit_price_sum_55cff00f",
            "type": "double",
            "semantic_entity": "v3.avg_unit_price:unit_price_sum_55cff00f",
            "semantic_type": "metric_component",
        }

        # Validate SQL - SUM appears only ONCE (component deduplication is working)
        assert_sql_equal(
            gg["sql"],
            """
            WITH
            v3_order_details AS (
                SELECT o.status, oi.unit_price
                FROM default.v3.orders o JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t1.status, COUNT(t1.unit_price) unit_price_count_55cff00f, SUM(t1.unit_price) unit_price_sum_55cff00f
            FROM v3_order_details t1
            GROUP BY t1.status
            """,
            normalize_aliases=False,
        )

        # Validate requested_dimensions
        assert result["requested_dimensions"] == ["v3.order_details.status"]
        assert result["dialect"] == "spark"


class TestMetricTypesMeasuresSQL:
    @pytest.mark.asyncio
    async def test_approx_count_distinct_full_aggregability(self, client_with_build_v3):
        """
        Test FULL aggregability metric: APPROX_COUNT_DISTINCT(customer_id).

        APPROX_COUNT_DISTINCT uses HyperLogLog sketches which are fully aggregatable.
        The measures SQL outputs the sketch aggregation directly.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.customer_count"],
                "dimensions": ["v3.order_details.status"],
            },
        )

        assert response.status_code == 200
        result = response.json()

        # Should have 1 grain group (FULL aggregability)
        assert len(result["grain_groups"]) == 1

        gg = result["grain_groups"][0]

        # Validate aggregability
        assert gg["aggregability"] == "full"

        # Validate grain
        assert gg["grain"] == ["status"]

        # Validate metrics
        assert gg["metrics"] == ["v3.customer_count"]

        # Validate columns
        # Note: type is "binary" because measures SQL stores the HLL sketch,
        # which is then converted to bigint via hll_sketch_estimate in metrics SQL
        assert gg["columns"] == [
            {
                "name": "status",
                "type": "string",
                "semantic_entity": "v3.order_details.status",
                "semantic_type": "dimension",
            },
            {
                "name": "customer_id_hll_23002251",
                "type": "binary",
                "semantic_entity": "v3.customer_count:customer_id_hll_23002251",
                "semantic_type": "metric_component",
            },
        ]

        # Validate SQL - uses hll_sketch_agg for APPROX_COUNT_DISTINCT
        assert_sql_equal(
            gg["sql"],
            """
            WITH
            v3_order_details AS (
                SELECT o.customer_id, o.status
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t1.status, hll_sketch_agg(t1.customer_id) customer_id_hll_23002251
            FROM v3_order_details t1
            GROUP BY t1.status
            """,
        )

        # Validate requested_dimensions
        assert result["requested_dimensions"] == ["v3.order_details.status"]

    @pytest.mark.asyncio
    async def test_count_distinct_with_if_expression(self, client_with_build_v3):
        """
        Regression test: COUNT(DISTINCT IF(cond, col, NULL)) should produce a clean
        SQL identifier as the grain column name, not the raw IF expression string.

        v3.product_session_count = COUNT(DISTINCT IF(is_product_view = 1, session_id, NULL))

        The grain column level is the full IF expression. Previously, this expression
        was passed directly as a column name to make_column_ref(), producing a quoted
        expression like t1."if(is_product_view = 1, session_id, NULL)" in the SQL and
        the raw expression string as the column `name` in the response metadata.

        After the fix, the expression should be:
        - Selected with a clean alias in the SQL
        - Referenced by that alias in GROUP BY
        - Reported with the clean alias as `name` in column metadata
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.product_session_count"],
                "dimensions": ["v3.product.category"],
            },
        )

        assert response.status_code == 200
        result = response.json()

        assert len(result["grain_groups"]) == 1
        gg = result["grain_groups"][0]

        assert gg["aggregability"] == "limited"
        # The grain alias is component.name from decompose.py:
        # amenable_col_names([is_product_view, session_id]) + "_distinct" + "_" + short_hash
        assert sorted(gg["grain"]) == [
            "category",
            "is_product_view_session_id_distinct_ee91aa40",
        ]
        assert gg["metrics"] == ["v3.product_session_count"]

        assert gg["columns"] == [
            {
                "name": "category",
                "type": "string",
                "semantic_entity": "v3.product.category",
                "semantic_type": "dimension",
            },
            {
                "name": "is_product_view_session_id_distinct_ee91aa40",
                "type": "string",
                "semantic_entity": "v3.page_views_enriched.is_product_view_session_id_distinct_ee91aa40",
                "semantic_type": "dimension",
            },
        ]

        assert_sql_equal(
            gg["sql"],
            """
            WITH
            v3_page_views_enriched AS (
                SELECT session_id, product_id,
                    CASE WHEN page_type = 'product' THEN 1 ELSE 0 END AS is_product_view
                FROM default.v3.page_views
            ),
            v3_product AS (
                SELECT product_id, category
                FROM default.v3.products
            )
            SELECT t2.category,
                IF(t1.is_product_view = 1, t1.session_id, NULL) is_product_view_session_id_distinct_ee91aa40
            FROM v3_page_views_enriched t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            GROUP BY t2.category, is_product_view_session_id_distinct_ee91aa40
            """,
        )


class TestGrainColumnDimensionDedup:
    @pytest.mark.asyncio
    async def test_count_distinct_grain_col_not_duplicated_when_also_a_dimension(
        self,
        client_with_build_v3,
    ):
        """
        Regression test: when a COUNT(DISTINCT col) metric's grain column is also
        requested as an explicit dimension, it must appear only once in the SELECT.

        v3.order_count = COUNT(DISTINCT order_id), so order_id is its LIMITED grain
        column. If order_id is also requested as a dimension, the old code added it
        to the projection twice — once via the dimension loop and once via the grain
        column loop — producing a duplicate column in the SQL.

        The measures layer passes through raw values for re-aggregation (not COUNT DISTINCT
        itself), so the fix is that order_id appears exactly once in SELECT / columns.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.order_count"],
                "dimensions": [
                    "v3.order_details.status",
                    "v3.order_details.order_id",
                ],
            },
        )

        assert response.status_code == 200
        gg = response.json()["grain_groups"][0]

        # order_id must appear exactly once in the SELECT (not duplicated as both
        # dimension column and grain column)
        assert_sql_equal(
            gg["sql"],
            """
            WITH v3_order_details AS (
                SELECT o.order_id, o.status
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t1.status, t1.order_id
            FROM v3_order_details t1
            GROUP BY t1.status, t1.order_id
            """,
            normalize_aliases=False,
        )

        # Columns list must not contain order_id twice — it should appear once,
        # as a dimension (the grain column entry is suppressed since it's covered).
        col_names = [c["name"] for c in gg["columns"]]
        assert col_names.count("order_id") == 1, (
            f"order_id appeared {col_names.count('order_id')} times: {col_names}"
        )
        assert gg["columns"] == [
            {
                "name": "status",
                "type": "string",
                "semantic_entity": "v3.order_details.status",
                "semantic_type": "dimension",
            },
            {
                "name": "order_id",
                "type": "int",
                "semantic_entity": "v3.order_details.order_id",
                "semantic_type": "dimension",
            },
        ]


class TestMeasuresSQLDerived:
    @pytest.mark.asyncio
    async def test_period_over_period_measures(self, client_with_build_v3):
        """
        Test period-over-period metrics through measures SQL.

        v3.wow_revenue_change is a derived metric with LAG() window function.
        Measures SQL outputs the base metric (total_revenue) at the requested grain.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.wow_revenue_change"],
                "dimensions": ["v3.date.week[order]"],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        # Should have one grain group for the base metric
        assert len(result["grain_groups"]) == 1

        gg = result["grain_groups"][0]
        assert gg["aggregability"] == "full"
        assert gg["grain"] == ["week_order"]
        assert gg["metrics"] == ["v3.total_revenue"]

        assert_sql_equal(
            gg["sql"],
            """
            WITH
            v3_date AS (
            SELECT  date_id,
                week
            FROM default.v3.dates
            ),
            v3_order_details AS (
            SELECT  o.order_date,
                oi.quantity * oi.unit_price AS line_total
            FROM default.v3.orders o JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )

            SELECT  t2.week week_order,
                SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1 LEFT OUTER JOIN v3_date t2 ON t1.order_date = t2.date_id
            GROUP BY  t2.week
            """,
        )

        assert gg["columns"] == [
            {
                "name": "week_order",
                "type": "int",
                "semantic_entity": "v3.date.week[order]",
                "semantic_type": "dimension",
            },
            {
                "name": "line_total_sum_e1f61696",
                "type": "double",
                "semantic_entity": "v3.total_revenue:line_total_sum_e1f61696",
                "semantic_type": "metric_component",
            },
        ]

    @pytest.mark.asyncio
    async def test_all_additional_metrics_combined(self, client_with_build_v3):
        """
        Test MIN, MAX, conditional, and standard SUM metrics with multiple dimensions.

        Metrics from order_details:
        - v3.max_unit_price: MAX aggregation
        - v3.min_unit_price: MIN aggregation
        - v3.completed_order_revenue: SUM with CASE WHEN
        - v3.total_revenue: SUM (standard)

        Dimensions:
        - v3.order_details.status (local)
        - v3.product.category (joined)
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": [
                    "v3.max_unit_price",
                    "v3.min_unit_price",
                    "v3.completed_order_revenue",
                    "v3.total_revenue",
                    "v3.price_spread",
                    "v3.price_spread_pct",
                ],
                "dimensions": [
                    "v3.order_details.status",
                    "v3.product.category",
                ],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        # All FULL aggregability, should be one grain group
        assert len(result["grain_groups"]) == 1

        gg = result["grain_groups"][0]
        assert gg["aggregability"] == "full"
        assert set(gg["grain"]) == {"category", "status"}
        assert set(gg["metrics"]) == {
            "v3.max_unit_price",
            "v3.min_unit_price",
            "v3.completed_order_revenue",
            "v3.total_revenue",
            "v3.price_spread",
            "v3.avg_unit_price",
        }

        assert_sql_equal(
            gg["sql"],
            """
            WITH
            v3_order_details AS (
            SELECT  o.status,
                oi.product_id,
                oi.unit_price,
                oi.quantity * oi.unit_price AS line_total
            FROM default.v3.orders o JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
            SELECT  product_id,
                category
            FROM default.v3.products
            )

            SELECT  t1.status,
                t2.category,
                MAX(t1.unit_price) unit_price_max_55cff00f,
                MIN(t1.unit_price) unit_price_min_55cff00f,
                SUM(CASE WHEN t1.status = 'completed' THEN t1.line_total ELSE 0 END) status_line_total_sum_43004dae,
                SUM(t1.line_total) line_total_sum_e1f61696,
                COUNT(t1.unit_price) unit_price_count_55cff00f,
                SUM(t1.unit_price) unit_price_sum_55cff00f
            FROM v3_order_details t1 LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            GROUP BY  t1.status, t2.category
            """,
        )

        assert gg["columns"] == [
            {
                "name": "status",
                "type": "string",
                "semantic_entity": "v3.order_details.status",
                "semantic_type": "dimension",
            },
            {
                "name": "category",
                "type": "string",
                "semantic_entity": "v3.product.category",
                "semantic_type": "dimension",
            },
            {
                "name": "unit_price_max_55cff00f",
                "type": "float",
                "semantic_entity": "v3.max_unit_price:unit_price_max_55cff00f",
                "semantic_type": "metric_component",
            },
            {
                "name": "unit_price_min_55cff00f",
                "type": "float",
                "semantic_entity": "v3.min_unit_price:unit_price_min_55cff00f",
                "semantic_type": "metric_component",
            },
            {
                "name": "status_line_total_sum_43004dae",
                "type": "double",
                "semantic_entity": "v3.completed_order_revenue:status_line_total_sum_43004dae",
                "semantic_type": "metric_component",
            },
            {
                "name": "line_total_sum_e1f61696",
                "type": "double",
                "semantic_entity": "v3.total_revenue:line_total_sum_e1f61696",
                "semantic_type": "metric_component",
            },
            {
                "name": "unit_price_count_55cff00f",
                "semantic_entity": "v3.avg_unit_price:unit_price_count_55cff00f",
                "semantic_type": "metric_component",
                "type": "bigint",
            },
            {
                "name": "unit_price_sum_55cff00f",
                "semantic_entity": "v3.avg_unit_price:unit_price_sum_55cff00f",
                "semantic_type": "metric_component",
                "type": "double",
            },
        ]


class TestMeasuresSQLFilters:
    @pytest.mark.asyncio
    async def test_simple_filter_on_local_column(self, client_with_build_v3):
        """Test a simple filter on a local (fact) column."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
                "filters": ["v3.order_details.status = 'completed'"],
            },
        )

        assert response.status_code == 200, response.json()
        data = get_first_grain_group(response.json())
        assert_sql_equal(
            data["sql"],
            """
            WITH v3_order_details AS (
                SELECT o.status, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
                WHERE o.status = 'completed'
            )
            SELECT t1.status, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            GROUP BY t1.status
            """,
        )

    @pytest.mark.asyncio
    async def test_filter_on_dimension_column(self, client_with_build_v3):
        """Test a filter on a joined dimension column that is also in GROUP BY."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "filters": ["v3.product.category = 'Electronics'"],
            },
        )

        assert response.status_code == 200, response.json()
        data = get_first_grain_group(response.json())
        assert_sql_equal(
            data["sql"],
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
                WHERE category = 'Electronics'
            )
            SELECT t2.category, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            WHERE t2.category = 'Electronics'
            GROUP BY t2.category
            """,
        )

    @pytest.mark.asyncio
    async def test_multiple_filters_combined_with_and(self, client_with_build_v3):
        """Test multiple filters are combined with AND."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status", "v3.product.category"],
                "filters": [
                    "v3.order_details.status = 'completed'",
                    "v3.product.category = 'Electronics'",
                ],
            },
        )

        assert response.status_code == 200, response.json()
        data = get_first_grain_group(response.json())
        assert_sql_equal(
            data["sql"],
            """
            WITH
            v3_order_details AS (
                SELECT o.status, oi.product_id, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
                WHERE o.status = 'completed'
            ),
            v3_product AS (
                SELECT product_id, category
                FROM default.v3.products
                WHERE category = 'Electronics'
            )
            SELECT t1.status, t2.category, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            WHERE t2.category = 'Electronics'
            GROUP BY t1.status, t2.category
            """,
        )

    @pytest.mark.asyncio
    async def test_filter_with_comparison_operators(self, client_with_build_v3):
        """Test filters with comparison operators on a role-qualified dimension."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.date.year[order]"],
                "filters": ["v3.date.year[order] >= 2024"],
            },
        )

        assert response.status_code == 200, response.json()
        data = get_first_grain_group(response.json())
        assert_sql_equal(
            data["sql"],
            """
            WITH
            v3_date AS (
                SELECT date_id, year
                FROM default.v3.dates
                WHERE year >= 2024
            ),
            v3_order_details AS (
                SELECT o.order_date, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t2.year year_order, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_date t2 ON t1.order_date = t2.date_id
            WHERE t2.year >= 2024
            GROUP BY t2.year
            """,
        )

    @pytest.mark.asyncio
    async def test_filter_with_in_operator(self, client_with_build_v3):
        """Test filter with IN operator on a local column."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
                "filters": ["v3.order_details.status IN ('completed', 'pending')"],
            },
        )

        assert response.status_code == 200, response.json()
        data = get_first_grain_group(response.json())
        assert_sql_equal(
            data["sql"],
            """
            WITH v3_order_details AS (
                SELECT o.status, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
                WHERE o.status IN ('completed', 'pending')
            )
            SELECT t1.status, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            GROUP BY t1.status
            """,
        )


class TestBaseMetricCaching:
    """Tests for base metric caching when processing derived metrics."""

    @pytest.mark.asyncio
    async def test_base_metric_plus_derived_that_uses_it(self, client_with_build_v3):
        """
        Test requesting both a derived metric AND its base metric.

        When we request ["v3.avg_order_value", "v3.order_count"], the flow is:
        1. Process v3.avg_order_value (derived FIRST) - decomposes base metrics:
           - v3.total_revenue is decomposed and cached
           - v3.order_count is decomposed and cached
        2. Process v3.order_count (base SECOND) - ALREADY in cache from step 1
           -> This hits the caching path in group_metrics_by_parent (lines 485-493)

        Order matters! Derived must be first to test the cache hit path.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                # Derived first, then base - order matters for cache testing
                "metrics": ["v3.avg_order_value", "v3.order_count"],
                "dimensions": ["v3.date.month[order]"],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        # Should have one grain group (both metrics from same parent)
        assert len(result["grain_groups"]) == 1

        gg = result["grain_groups"][0]
        # Both base metrics (order_count and total_revenue) should be in the grain group
        assert "v3.order_count" in gg["metrics"]
        assert "v3.total_revenue" in gg["metrics"]

    @pytest.mark.asyncio
    async def test_shared_base_metric_across_derived(self, client_with_build_v3):
        """
        Test requesting two derived metrics that share a base metric.

        v3.avg_order_value = total_revenue / order_count
        v3.avg_items_per_order = total_quantity / order_count

        Both use v3.order_count, so it should only be decomposed once.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.avg_order_value", "v3.avg_items_per_order"],
                "dimensions": ["v3.date.month[order]"],
            },
        )

        assert response.status_code == 200, response.json()
        result = response.json()

        # Should have one grain group
        assert len(result["grain_groups"]) == 1

        gg = result["grain_groups"][0]
        # Should have all three base metrics
        assert "v3.total_revenue" in gg["metrics"]
        assert "v3.total_quantity" in gg["metrics"]
        assert "v3.order_count" in gg["metrics"]


class TestTemporalFilters:
    """Tests for include_temporal_filters and lookback_window functionality."""

    @pytest.fixture
    async def setup_temporal_partition(self, client_with_build_v3):
        """Set up cube-based temporal partition for testing."""
        # Step 1: Add temporal partition to v3.date dimension
        response = await client_with_build_v3.post(
            "/nodes/v3.date/columns/date_id/partition",
            json={
                "type_": "temporal",
                "format": "yyyyMMdd",
                "granularity": "day",
            },
        )
        assert response.status_code in (200, 201, 422, 409)

        # Step 2: Create dimension link between order_details and date
        response = await client_with_build_v3.post(
            "/nodes/v3.order_details/link",
            json={
                "dimension_node": "v3.date",
                "join_type": "left",
                "join_on": "v3.order_details.order_date = v3.date.date_id",
            },
        )
        assert response.status_code in (200, 201, 400, 409, 422)

        # Step 3: Create a cube with metrics and date dimension
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.test_temporal_cube",
                "display_name": "Test Temporal Cube",
                "description": "Cube for temporal filtering tests",
                "mode": "published",
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.date.date_id"],
            },
        )
        assert response.status_code in (200, 201, 409)

        # Step 4: Set partition on the cube's date column
        response = await client_with_build_v3.post(
            "/nodes/v3.test_temporal_cube/columns/v3.date.date_id/partition",
            json={
                "type_": "temporal",
                "format": "yyyyMMdd",
                "granularity": "day",
            },
        )
        assert response.status_code in (200, 201, 422, 409)

    @pytest.mark.asyncio
    async def test_temporal_filter_exact_partition(
        self,
        session,
        client_with_build_v3,
        setup_temporal_partition,
    ):
        """
        Test that include_temporal_filters=True adds exact partition filter.

        Uses cube with v3.date.date_id dimension configured as a temporal partition.
        The filter should be pushed down to parent node through dimension link:
        date_id = CAST(DATE_FORMAT(DJ_LOGICAL_TIMESTAMP(), 'yyyyMMdd') AS INT)
        """
        result = await build_measures_sql(
            session=session,
            metrics=["v3.total_revenue"],
            dimensions=["v3.date.date_id"],
            include_temporal_filters=True,
        )

        # Should have DJ_LOGICAL_TIMESTAMP in the SQL for exact partition match.
        # Pushed into the parent CTE's WHERE so it applies before any internal
        # join, not after — protecting any OUTER JOIN inside parent body.
        assert_sql_equal(
            result.grain_groups[0].sql,
            """
            WITH v3_order_details AS (
                SELECT o.order_date, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
                WHERE order_date = CAST(DATE_FORMAT(CAST(DJ_LOGICAL_TIMESTAMP() AS TIMESTAMP), 'yyyyMMdd') AS INT)
            )
            SELECT t1.order_date date_id, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            GROUP BY t1.order_date
            """,
        )

    @pytest.mark.asyncio
    async def test_temporal_filter_with_lookback(
        self,
        session,
        client_with_build_v3,
        setup_temporal_partition,
    ):
        """
        Test that lookback_window generates BETWEEN filter.

        The filter should be pushed down through dimension link:
        date_id BETWEEN CAST(DATE_FORMAT(DJ_LOGICAL_TIMESTAMP() - INTERVAL '3' DAY, 'yyyyMMdd') AS INT)
                    AND CAST(DATE_FORMAT(DJ_LOGICAL_TIMESTAMP(), 'yyyyMMdd') AS INT)
        """
        result = await build_measures_sql(
            session=session,
            metrics=["v3.total_revenue"],
            dimensions=["v3.date.date_id"],
            include_temporal_filters=True,
            lookback_window="3 DAY",
        )

        # Should have BETWEEN for lookback window — pushed into parent CTE.
        assert_sql_equal(
            result.grain_groups[0].sql,
            """
            WITH v3_order_details AS (
                SELECT o.order_date, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
                WHERE order_date BETWEEN CAST(DATE_FORMAT(CAST(DJ_LOGICAL_TIMESTAMP() AS TIMESTAMP) - INTERVAL '3' DAY, 'yyyyMMdd') AS INT)
                                     AND CAST(DATE_FORMAT(CAST(DJ_LOGICAL_TIMESTAMP() AS TIMESTAMP), 'yyyyMMdd') AS INT)
            )
            SELECT t1.order_date date_id, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            GROUP BY t1.order_date
            """,
        )

    @pytest.mark.asyncio
    async def test_no_temporal_filter_when_disabled(
        self,
        session,
        client_with_build_v3,
        setup_temporal_partition,
    ):
        """
        Test that temporal filters are NOT added when include_temporal_filters=False.

        Even though a cube with temporal partitions exists, filters should not be applied
        when include_temporal_filters=False.
        """
        result = await build_measures_sql(
            session=session,
            metrics=["v3.total_revenue"],
            dimensions=["v3.date.date_id"],
            include_temporal_filters=False,  # Default, but explicit
        )

        # No temporal filter should be present
        assert_sql_equal(
            result.grain_groups[0].sql,
            """
            WITH v3_order_details AS (
                SELECT o.order_date, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t1.order_date date_id, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            GROUP BY t1.order_date
            """,
        )

    @pytest.mark.asyncio
    async def test_temporal_filter_with_user_filter(
        self,
        session,
        client_with_build_v3,
        setup_temporal_partition,
    ):
        """
        Test that temporal filter is combined with user filters.

        The temporal filter should be combined with the user filter using AND.
        """
        result = await build_measures_sql(
            session=session,
            metrics=["v3.total_revenue"],
            dimensions=["v3.date.date_id"],
            filters=["v3.date.date_id > 20200101"],
            include_temporal_filters=True,
        )

        # Both user filter and temporal filter are pushed into the parent CTE's
        # WHERE — combined via AND.  Temporal goes in first (via the
        # injected_filters path), then the user filter is appended via the
        # generic pushdown path; the user-filter form uses the FK alias
        # (`o.order_date`) while the temporal form uses the bare column.
        assert_sql_equal(
            result.grain_groups[0].sql,
            """
            WITH v3_order_details AS (
                SELECT o.order_date, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
                WHERE order_date = CAST(DATE_FORMAT(CAST(DJ_LOGICAL_TIMESTAMP() AS TIMESTAMP), 'yyyyMMdd') AS INT)
                  AND o.order_date > 20200101
            )
            SELECT t1.order_date date_id, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            GROUP BY t1.order_date
            """,
        )

    @pytest.mark.asyncio
    async def test_temporal_filter_falls_back_to_parent_cte(
        self,
        session,
        client_with_build_v3,
        setup_temporal_partition,
    ):
        """
        When the parent node's primary FROM table is a source node (not a
        transform), find_upstream_temporal_source_node returns None.  The
        filter then falls back to the parent node's own CTE — never to the
        outer grain-group WHERE, since that would silently turn an OUTER JOIN
        inside the parent body into an INNER JOIN.

        v3.order_details reads directly from default.v3.orders (a source), so
        no upstream pushdown — the temporal predicate lands in the parent CTE.
        """
        result = await build_measures_sql(
            session=session,
            metrics=["v3.total_revenue"],
            dimensions=["v3.date.date_id"],
            include_temporal_filters=True,
        )

        assert_sql_equal(
            result.grain_groups[0].sql,
            """
            WITH v3_order_details AS (
                SELECT o.order_date, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
                WHERE order_date = CAST(DATE_FORMAT(CAST(DJ_LOGICAL_TIMESTAMP() AS TIMESTAMP), 'yyyyMMdd') AS INT)
            )
            SELECT t1.order_date date_id, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            GROUP BY t1.order_date
            """,
        )


class TestCubeBasedTemporalFiltering:
    """Tests for cube-based temporal filtering via REST API."""

    @pytest.mark.asyncio
    async def test_temporal_filters_applied_when_cube_exists(
        self,
        client_with_build_v3,
    ):
        """
        Test that temporal filters are applied when metrics+dimensions resolve to a cube.

        Steps:
        1. Set up temporal partition on date dimension
        2. Create a cube with multiple metrics and the date dimension
        3. Set partition on the cube's date column
        4. Request measures SQL with various metric+dimension combos from that cube
        5. Verify temporal filters are applied
        """
        # Step 1: Add temporal partition to v3.date dimension
        response = await client_with_build_v3.post(
            "/nodes/v3.date/columns/date_id/partition",
            json={
                "type_": "temporal",
                "format": "yyyyMMdd",
                "granularity": "day",
            },
        )
        assert response.status_code in (200, 201, 422)

        # Create dimension link between order_details and date
        response = await client_with_build_v3.post(
            "/nodes/v3.order_details/link",
            json={
                "dimension_node": "v3.date",
                "join_type": "left",
                "join_on": "v3.order_details.order_date = v3.date.date_id",
            },
        )
        assert response.status_code in (200, 201, 400, 409, 422)

        # Step 2: Create a cube with multiple metrics and date dimension
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.test_orders_by_date_cube",
                "display_name": "Test Orders by Date Cube",
                "description": "Test cube for temporal filtering with multiple metrics",
                "mode": "published",
                "metrics": ["v3.total_revenue", "v3.total_quantity", "v3.order_count"],
                "dimensions": ["v3.date.date_id"],  # Only include date dimension
            },
        )
        assert response.status_code in (200, 201)

        # Step 3: Set partition on the cube's date column
        response = await client_with_build_v3.post(
            "/nodes/v3.test_orders_by_date_cube/columns/v3.date.date_id/partition",
            json={
                "type_": "temporal",
                "format": "yyyyMMdd",
                "granularity": "day",
            },
        )
        assert response.status_code in (200, 201, 422)

        # Step 4c: Test with multiple metrics from the cube
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue", "v3.total_quantity"],
                "dimensions": ["v3.date.date_id"],
                "include_temporal_filters": True,
            },
        )

        assert response.status_code == 200
        data = response.json()

        # Temporal filter pushed into the parent CTE's WHERE.
        assert_sql_equal(
            data["grain_groups"][0]["sql"],
            """
            WITH v3_order_details AS (
                SELECT o.order_date, oi.quantity, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
                WHERE order_date = CAST(DATE_FORMAT(CAST(DJ_LOGICAL_TIMESTAMP() AS TIMESTAMP), 'yyyyMMdd') AS INT)
            )
            SELECT t1.order_date date_id,
                   SUM(t1.line_total) line_total_sum_e1f61696,
                   SUM(t1.quantity) quantity_sum_06b64d2e
            FROM v3_order_details t1
            GROUP BY t1.order_date
            """,
        )

        # Step 4d: Test with all three metrics
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue", "v3.total_quantity", "v3.order_count"],
                "dimensions": ["v3.date.date_id"],
                "include_temporal_filters": True,
            },
        )

        assert response.status_code == 200
        data = response.json()

        # Temporal filter pushed into the parent CTE's WHERE.
        assert_sql_equal(
            data["grain_groups"][0]["sql"],
            """
            WITH v3_order_details AS (
              SELECT
                o.order_id,
                o.order_date,
                oi.quantity,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
              WHERE order_date = CAST(DATE_FORMAT(CAST(DJ_LOGICAL_TIMESTAMP() AS TIMESTAMP), 'yyyyMMdd') AS INT)
            )
            SELECT
              t1.order_date date_id,
              t1.order_id,
              SUM(t1.line_total) line_total_sum_e1f61696,
              SUM(t1.quantity) quantity_sum_06b64d2e
            FROM v3_order_details t1
            GROUP BY  t1.order_date, t1.order_id
            """,
        )

        # Step 4e: Test with lookback window
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.date.date_id"],
                "include_temporal_filters": True,
                "lookback_window": "7 DAY",
            },
        )

        assert response.status_code == 200
        data = response.json()

        # BETWEEN filter for lookback — pushed into parent CTE.
        assert_sql_equal(
            data["grain_groups"][0]["sql"],
            """
            WITH v3_order_details AS (
                SELECT o.order_date, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
                WHERE order_date BETWEEN CAST(DATE_FORMAT(CAST(DJ_LOGICAL_TIMESTAMP() AS TIMESTAMP) - INTERVAL '7' DAY, 'yyyyMMdd') AS INT)
                                     AND CAST(DATE_FORMAT(CAST(DJ_LOGICAL_TIMESTAMP() AS TIMESTAMP), 'yyyyMMdd') AS INT)
            )
            SELECT t1.order_date date_id,
                   SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            GROUP BY t1.order_date
            """,
        )

    @pytest.mark.asyncio
    async def test_no_temporal_filters_when_no_cube_match(
        self,
        client_with_build_v3,
    ):
        """
        Test that temporal filters are NOT applied when metrics+dimensions don't resolve to a cube.

        Even if include_temporal_filters=True, filters should not be applied if there's no matching cube.
        """
        # Request measures SQL with include_temporal_filters=True but no matching cube
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
                "include_temporal_filters": True,  # Request temporal filters
            },
        )

        assert response.status_code == 200
        data = response.json()

        # Should NOT have temporal filter since no cube matches
        sql = data["grain_groups"][0]["sql"]
        assert "DJ_LOGICAL_TIMESTAMP()" not in sql


class TestTemporalFilterPushdown:
    """
    Tests for temporal filter pushdown into the date-spine CTE.

    Three-node rolling window pattern:
      v3.date_spine          — pure date spine (drives the rolling join)
      v3.orders_7d_window    — fact filtered to 7-day window via DJ_LOGICAL_TIMESTAMP()
      v3.rolling_7d_orders   — rolling join of spine × windowed fact, links to v3.date

    With include_temporal_filters=True the temporal filter for v3.date.date_id
    is pushed into the v3_date_spine CTE so the rolling join only processes
    the target date(s), not all dates.
    """

    @pytest.fixture
    async def setup_rolling_window(self, client_with_build_v3):
        # Date spine
        response = await client_with_build_v3.post(
            "/nodes/transform/",
            json={
                "name": "v3.date_spine",
                "display_name": "Date Spine",
                "description": "Calendar date spine for rolling window joins",
                "mode": "published",
                "query": "SELECT date_id FROM v3.src_dates",
                "columns": [
                    {
                        "name": "date_id",
                        "type": "int",
                        "attributes": [{"attribute_type": {"name": "primary_key"}}],
                    },
                ],
            },
        )
        assert response.status_code in (200, 201), response.text

        # Windowed fact
        response = await client_with_build_v3.post(
            "/nodes/transform/",
            json={
                "name": "v3.orders_7d_window",
                "display_name": "Orders 7D Window",
                "description": "Orders filtered to 7-day window ending at logical timestamp",
                "mode": "published",
                "query": """
                    SELECT order_id, order_date
                    FROM v3.src_orders
                    WHERE order_date >= date_sub(
                        CAST(DATE_FORMAT(CAST(DJ_LOGICAL_TIMESTAMP() AS TIMESTAMP), 'yyyyMMdd') AS INT), 6
                    )
                    AND order_date <= CAST(DATE_FORMAT(CAST(DJ_LOGICAL_TIMESTAMP() AS TIMESTAMP), 'yyyyMMdd') AS INT)
                """,
                "columns": [
                    {"name": "order_id", "type": "int"},
                    {"name": "order_date", "type": "int"},
                ],
            },
        )
        assert response.status_code in (200, 201), response.text

        # Rolling join
        response = await client_with_build_v3.post(
            "/nodes/transform/",
            json={
                "name": "v3.rolling_7d_orders",
                "display_name": "Rolling 7D Orders",
                "description": "Order count over rolling 7-day window anchored on date spine",
                "mode": "published",
                "query": """
                    SELECT ds.date_id, COUNT(o.order_id) AS order_cnt_7d
                    FROM v3.date_spine ds
                    LEFT OUTER JOIN v3.orders_7d_window o
                      ON o.order_date >= date_sub(ds.date_id, 6)
                     AND o.order_date <= ds.date_id
                    GROUP BY ds.date_id
                """,
                "columns": [
                    {
                        "name": "date_id",
                        "type": "int",
                        "attributes": [{"attribute_type": {"name": "primary_key"}}],
                    },
                    {"name": "order_cnt_7d", "type": "bigint"},
                ],
            },
        )
        assert response.status_code in (200, 201), response.text
        response = await client_with_build_v3.post(
            "/nodes/v3.rolling_7d_orders/link",
            json={
                "dimension_node": "v3.date",
                "join_type": "left",
                "join_on": "v3.rolling_7d_orders.date_id = v3.date.date_id",
            },
        )
        assert response.status_code in (200, 201), response.text

        response = await client_with_build_v3.post(
            "/nodes/metric/",
            json={
                "name": "v3.rolling_7d_count",
                "display_name": "Rolling 7D Order Count",
                "mode": "published",
                "query": "SELECT SUM(order_cnt_7d) FROM v3.rolling_7d_orders",
            },
        )
        assert response.status_code in (200, 201), response.text

        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.rolling_7d_cube",
                "display_name": "Rolling 7D Cube",
                "mode": "published",
                "metrics": ["v3.rolling_7d_count"],
                "dimensions": ["v3.date.date_id"],
            },
        )
        assert response.status_code in (200, 201), response.text

        response = await client_with_build_v3.post(
            "/nodes/v3.rolling_7d_cube/columns/v3.date.date_id/partition",
            json={"type_": "temporal", "format": "yyyyMMdd", "granularity": "day"},
        )
        assert response.status_code in (200, 201), response.text

    @pytest.mark.asyncio
    async def test_temporal_filter_pushdown_full_sql(
        self,
        client_with_build_v3,
        setup_rolling_window,
    ):
        """
        The temporal filter for v3.date.date_id should be pushed into the
        v3_date_spine CTE rather than appearing only on the outer query.
        This limits the rolling join to only the target date(s).
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.rolling_7d_count"],
                "dimensions": ["v3.date.date_id"],
                "include_temporal_filters": True,
            },
        )
        assert response.status_code == 200
        sql = response.json()["grain_groups"][0]["sql"]
        assert_sql_equal(
            sql,
            """
            WITH v3_date_spine AS (
              SELECT
                date_id
              FROM default.v3.dates
              WHERE
                date_id = CAST(DATE_FORMAT(CAST(DJ_LOGICAL_TIMESTAMP() AS TIMESTAMP), 'yyyyMMdd') AS INT)
            ),
            v3_orders_7d_window AS (
              SELECT
                order_id,
                order_date
              FROM default.v3.orders
              WHERE
                order_date >= date_sub(CAST(DATE_FORMAT(CAST(DJ_LOGICAL_TIMESTAMP() AS TIMESTAMP), 'yyyyMMdd') AS INT), 6)
                AND order_date <= CAST(DATE_FORMAT(CAST(DJ_LOGICAL_TIMESTAMP() AS TIMESTAMP), 'yyyyMMdd') AS INT)
            ),
            v3_rolling_7d_orders AS (
              SELECT
                ds.date_id,
                COUNT(o.order_id) AS order_cnt_7d
              FROM v3_date_spine ds
              LEFT OUTER JOIN v3_orders_7d_window o ON o.order_date >= date_sub(ds.date_id, 6) AND o.order_date <= ds.date_id
              GROUP BY ds.date_id
            )
            SELECT  t1.date_id,
                SUM(t1.order_cnt_7d) order_cnt_7d_sum_37699d3d
            FROM v3_rolling_7d_orders t1
            GROUP BY t1.date_id
            """,
        )

    @pytest.mark.asyncio
    async def test_no_upstream_pushdown_falls_back_to_parent_cte(
        self,
        session,
        client_with_build_v3,
    ):
        """
        When the primary FROM of the parent node is a non-source transform that
        does NOT expose the FK column (date_id), upstream pushdown finds no
        match. The filter then falls back to the parent's own CTE — never to
        the outer grain-group WHERE, which would be unsafe in the presence of
        OUTER JOINs inside the parent body.

        v3.orders_by_date is a non-source transform but its columns don't
        include date_id directly via the underlying source — so
        find_upstream_temporal_source_node returns None and the filter lands
        on the parent CTE.
        """
        # Transform that reads from v3.order_details (non-source, no date_id column)
        # but exposes date_id by aliasing order_date
        response = await client_with_build_v3.post(
            "/nodes/transform/",
            json={
                "name": "v3.orders_by_date",
                "display_name": "Orders By Date",
                "mode": "published",
                "query": "SELECT order_date AS date_id, COUNT(order_id) AS order_cnt FROM v3.order_details GROUP BY order_date",
                "columns": [
                    {
                        "name": "date_id",
                        "type": "int",
                        "attributes": [{"attribute_type": {"name": "primary_key"}}],
                    },
                    {"name": "order_cnt", "type": "bigint"},
                ],
            },
        )
        assert response.status_code in (200, 201), response.text
        response = await client_with_build_v3.post(
            "/nodes/v3.orders_by_date/link",
            json={
                "dimension_node": "v3.date",
                "join_type": "left",
                "join_on": "v3.orders_by_date.date_id = v3.date.date_id",
            },
        )
        assert response.status_code in (200, 201), response.text
        response = await client_with_build_v3.post(
            "/nodes/metric/",
            json={
                "name": "v3.orders_by_date_count",
                "mode": "published",
                "query": "SELECT SUM(order_cnt) FROM v3.orders_by_date",
            },
        )
        assert response.status_code in (200, 201), response.text
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.orders_by_date_cube",
                "mode": "published",
                "metrics": ["v3.orders_by_date_count"],
                "dimensions": ["v3.date.date_id"],
            },
        )
        assert response.status_code in (200, 201), response.text
        response = await client_with_build_v3.post(
            "/nodes/v3.orders_by_date_cube/columns/v3.date.date_id/partition",
            json={"type_": "temporal", "format": "yyyyMMdd", "granularity": "day"},
        )
        assert response.status_code in (200, 201), response.text

        result = await build_measures_sql(
            session=session,
            metrics=["v3.orders_by_date_count"],
            dimensions=["v3.date.date_id"],
            include_temporal_filters=True,
        )
        sql = result.grain_groups[0].sql

        # Filter lands in the parent CTE (v3_orders_by_date), not the outer
        # WHERE — outer WHERE would be unsafe if the parent body had any
        # OUTER JOIN.
        assert_sql_equal(
            sql,
            """
            WITH v3_order_details AS (
              SELECT
                o.order_id,
                oi.line_number,
                o.customer_id,
                o.order_date,
                o.from_location_id,
                o.to_location_id,
                o.status,
                oi.product_id,
                oi.quantity,
                oi.unit_price,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_orders_by_date AS (
              SELECT
                order_date AS date_id,
                COUNT(order_id) AS order_cnt
              FROM v3_order_details
              WHERE date_id = CAST(DATE_FORMAT(CAST(DJ_LOGICAL_TIMESTAMP() AS TIMESTAMP), 'yyyyMMdd') AS INT)
              GROUP BY  order_date
            )
            SELECT
              t1.date_id,
              SUM(t1.order_cnt) order_cnt_sum_e668c538
            FROM v3_orders_by_date t1
            GROUP BY  t1.date_id
            """,
        )

    @pytest.mark.asyncio
    async def test_parent_query_no_from_falls_back_to_parent_cte(
        self,
        session,
        client_with_build_v3,
    ):
        """
        When the parent node's query has no FROM clause,
        find_upstream_temporal_source_node returns None at the from_ check
        and the filter falls back to the parent's own CTE — not the outer
        grain-group WHERE.
        """
        response = await client_with_build_v3.post(
            "/nodes/transform/",
            json={
                "name": "v3.constant_date",
                "display_name": "Constant Date",
                "mode": "published",
                "query": "SELECT CAST(DATE_FORMAT(CAST(DJ_LOGICAL_TIMESTAMP() AS TIMESTAMP), 'yyyyMMdd') AS INT) AS date_id, 1 AS cnt",
                "columns": [
                    {
                        "name": "date_id",
                        "type": "int",
                        "attributes": [{"attribute_type": {"name": "primary_key"}}],
                    },
                    {"name": "cnt", "type": "bigint"},
                ],
            },
        )
        assert response.status_code in (200, 201), response.text
        response = await client_with_build_v3.post(
            "/nodes/v3.constant_date/link",
            json={
                "dimension_node": "v3.date",
                "join_type": "left",
                "join_on": "v3.constant_date.date_id = v3.date.date_id",
            },
        )
        assert response.status_code in (200, 201), response.text
        response = await client_with_build_v3.post(
            "/nodes/metric/",
            json={
                "name": "v3.constant_date_count",
                "mode": "published",
                "query": "SELECT SUM(cnt) FROM v3.constant_date",
            },
        )
        assert response.status_code in (200, 201), response.text
        response = await client_with_build_v3.post(
            "/nodes/cube/",
            json={
                "name": "v3.constant_date_cube",
                "mode": "published",
                "metrics": ["v3.constant_date_count"],
                "dimensions": ["v3.date.date_id"],
            },
        )
        assert response.status_code in (200, 201), response.text
        response = await client_with_build_v3.post(
            "/nodes/v3.constant_date_cube/columns/v3.date.date_id/partition",
            json={"type_": "temporal", "format": "yyyyMMdd", "granularity": "day"},
        )
        assert response.status_code in (200, 201), response.text

        result = await build_measures_sql(
            session=session,
            metrics=["v3.constant_date_count"],
            dimensions=["v3.date.date_id"],
            include_temporal_filters=True,
        )
        sql = result.grain_groups[0].sql
        # Filter lands in the parent CTE (no FROM means no inner-side hazard;
        # AND-into-WHERE on the parent's own SELECT is safe).
        assert_sql_equal(
            sql,
            """
            WITH v3_constant_date AS (
              SELECT
                CAST(DATE_FORMAT(CAST(DJ_LOGICAL_TIMESTAMP() AS TIMESTAMP), 'yyyyMMdd') AS INT) AS date_id,
                1 AS cnt
              WHERE date_id = CAST(DATE_FORMAT(CAST(DJ_LOGICAL_TIMESTAMP() AS TIMESTAMP), 'yyyyMMdd') AS INT)
            )
            SELECT
              t1.date_id,
              SUM(t1.cnt) cnt_sum_293e7033
            FROM v3_constant_date t1
            GROUP BY  t1.date_id
            """,
        )


class TestNonDecomposableMetrics:
    """Tests for metrics that cannot be decomposed (Aggregability.NONE)."""

    @pytest.mark.asyncio
    async def test_non_decomposable_metric_max_by(
        self,
        session,
        client_with_build_v3,
    ):
        """
        Test that non-decomposable metrics like MAX_BY are handled.

        MAX_BY cannot be pre-aggregated because it needs access to the full
        dataset to determine which row has the maximum value. Since it has
        Aggregability.NONE, the query outputs raw rows at native grain
        (PK columns) rather than aggregated values.
        """
        result = await build_measures_sql(
            session=session,
            metrics=["v3.top_product_by_revenue"],
            dimensions=["v3.order_details.status"],
        )

        # Non-decomposable metrics should have Aggregability.NONE
        assert len(result.grain_groups) == 1
        gg = result.grain_groups[0]
        assert gg.aggregability.value == "none"

        # The grain should be the native grain (PK columns) since we can't aggregate
        # For order_details, native grain is order_id + line_number
        assert set(gg.grain) == {"order_id", "line_number"}

        # SQL should output raw values at native grain, not aggregated
        assert_sql_equal(
            gg.sql,
            """
            WITH v3_order_details AS (
              SELECT
                o.order_id,
                oi.line_number,
                o.status,
                oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT
              t1.status,
              t1.order_id,
              t1.line_number,
              t1.product_id,
              t1.line_total
            FROM v3_order_details t1
            """,
        )


class TestCombinedMeasuresSQLEndpoint:
    """Tests for the /sql/measures/v3/combined endpoint."""

    @pytest.mark.asyncio
    async def test_combined_single_grain_group(self, client_with_build_v3):
        """
        Test combined endpoint with metrics from a single parent node.
        When there's only one grain group, no JOIN is needed.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/combined",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
            },
        )

        assert response.status_code == 200
        data = response.json()

        # Single grain group means no JOIN needed
        assert data["grain_groups_combined"] == 1
        assert "status" in data["grain"]

        # Verify SQL structure - single grain group, no JOIN
        assert_sql_equal(
            data["sql"],
            """
            WITH v3_order_details AS (
                SELECT o.status, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t1.status, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            GROUP BY t1.status
            """,
        )

        # Verify columns include dimensions and measures
        column_names = [col["name"] for col in data["columns"]]
        assert "status" in column_names
        assert "line_total_sum_e1f61696" in column_names

    @pytest.mark.asyncio
    async def test_combined_cross_fact_metrics(self, client_with_build_v3):
        """
        Test combined endpoint with metrics from different parent nodes.
        Should produce FULL OUTER JOIN with COALESCE.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/combined",
            params={
                "metrics": ["v3.total_revenue", "v3.page_view_count"],
                "dimensions": ["v3.date.date_id"],
            },
        )

        assert response.status_code == 200
        data = response.json()

        # Cross-fact metrics mean multiple grain groups
        assert data["grain_groups_combined"] >= 2
        assert "date_id" in data["grain"]

        # Verify SQL structure - FULL OUTER JOIN with COALESCE
        assert_sql_equal(
            data["sql"],
            """
            WITH
            v3_order_details AS (
            SELECT  o.order_date,
                oi.quantity * oi.unit_price AS line_total
            FROM default.v3.orders o JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_page_views_enriched AS (
            SELECT  view_id,
                page_date
            FROM default.v3.page_views
            ),
            gg1 AS (
            SELECT  t1.order_date date_id,
                SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            GROUP BY  t1.order_date
            ),
            gg2 AS (
            SELECT  t1.page_date date_id,
                COUNT(t1.view_id) view_id_count_f41e2db4
            FROM v3_page_views_enriched t1
            GROUP BY  t1.page_date
            )
            SELECT  COALESCE(gg1.date_id, gg2.date_id) date_id,
                gg1.line_total_sum_e1f61696,
                gg2.view_id_count_f41e2db4
            FROM gg1 FULL OUTER JOIN gg2 ON gg1.date_id = gg2.date_id
            """,
        )

        # Verify columns include measures from both sources
        column_names = [col["name"] for col in data["columns"]]
        assert "date_id" in column_names
        assert "line_total_sum_e1f61696" in column_names
        assert "view_id_count_f41e2db4" in column_names

    @pytest.mark.asyncio
    async def test_combined_endpoint_returns_correct_metadata(
        self,
        client_with_build_v3,
    ):
        """
        Test that column metadata is correctly populated.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/combined",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
            },
        )

        assert response.status_code == 200
        data = response.json()

        # Verify SQL matches expected structure
        assert_sql_equal(
            data["sql"],
            """
            WITH v3_order_details AS (
                SELECT o.status, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t1.status, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            GROUP BY t1.status
            """,
        )

        # Check semantic types in columns
        columns = {col["name"]: col for col in data["columns"]}

        # Status should be a dimension
        assert columns["status"]["semantic_type"] == "dimension"

        # Revenue should be a metric component (semantic type varies based on single-component)
        assert columns["line_total_sum_e1f61696"]["semantic_type"] == "metric_component"

    @pytest.mark.asyncio
    async def test_combined_empty_metrics_returns_error(self, client_with_build_v3):
        """
        Test that empty metrics list returns an error.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/combined",
            params={
                "metrics": [],
                "dimensions": ["v3.order_details.status"],
            },
        )

        # Should return error for empty metrics
        assert response.status_code in (400, 422)

    @pytest.mark.asyncio
    async def test_combined_source_tables_default(self, client_with_build_v3):
        """
        Test that source=source_tables (default) returns source info.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/combined",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["grain_groups_combined"] == 1
        assert data["use_preagg_tables"] is False
        assert data["columns"] == [
            {
                "name": "status",
                "semantic_entity": "v3.order_details.status",
                "semantic_type": "dimension",
                "type": "string",
            },
            {
                "name": "line_total_sum_e1f61696",
                "semantic_entity": "v3.total_revenue:line_total_sum_e1f61696",
                "semantic_type": "metric_component",
                "type": "double",
            },
        ]
        assert data["grain"] == ["status"]
        assert data["source_tables"] == ["v3.order_details"]
        assert_sql_equal(
            data["sql"],
            """
            WITH
            v3_order_details AS (
            SELECT  o.status,
                oi.quantity * oi.unit_price AS line_total
            FROM default.v3.orders o JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )

            SELECT  t1.status,
                SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            GROUP BY  t1.status
            """,
        )

    @pytest.mark.asyncio
    async def test_combined_source_preagg_tables(self, client_with_build_v3):
        """
        Test that source=preagg_tables generates SQL reading from pre-agg tables.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/combined",
            params={
                "use_preagg_tables": "true",
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
            },
        )

        assert response.status_code == 200
        data = response.json()

        # Should indicate preagg_tables source
        assert data["use_preagg_tables"] is True

        # Source tables should be pre-agg table references
        assert len(data["source_tables"]) >= 1
        assert data["source_tables"] == [
            "default.dj_preaggs.v3_order_details_preagg_d344b4e3",
        ]

        # Extract the preagg table name for SQL comparison
        # The SQL should read from the pre-agg table with re-aggregation
        assert_sql_equal(
            data["sql"],
            """
            SELECT status, SUM(line_total_sum_e1f61696) line_total_sum_e1f61696
            FROM default.dj_preaggs.v3_order_details_preagg_d344b4e3
            GROUP BY status
            """,
        )

    @pytest.mark.asyncio
    async def test_combined_preagg_uses_configured_catalog_schema(
        self,
        client_with_build_v3,
    ):
        """
        Test that preagg_tables source uses configured catalog and schema.
        """
        from datajunction_server.config import Settings

        settings = Settings()

        response = await client_with_build_v3.get(
            "/sql/measures/v3/combined",
            params={
                "use_preagg_tables": "true",
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
            },
        )

        assert response.status_code == 200
        data = response.json()

        expected_table = (
            f"{settings.preagg_catalog}.{settings.preagg_schema}"
            f".v3_order_details_preagg_d344b4e3"
        )

        # Source tables should include the configured catalog.schema prefix
        assert len(data["source_tables"]) >= 1
        assert data["source_tables"] == [expected_table]

        # Verify the SQL also references this table
        assert_sql_equal(
            data["sql"],
            f"""
            SELECT status, SUM(line_total_sum_e1f61696) line_total_sum_e1f61696
            FROM {expected_table}
            GROUP BY status
            """,
        )


class TestMeasuresSQLNestedDerived:
    """
    Test measures SQL for nested derived metrics.

    Nested derived metrics are metrics that reference other derived metrics.
    For measures SQL, we need to decompose down to the base components.
    """

    @pytest.mark.asyncio
    async def test_nested_derived_metric_decomposes_to_base_components(
        self,
        client_with_build_v3,
    ):
        """
        Test that a nested derived metric decomposes to its base components.

        v3.aov_growth_index references v3.avg_order_value which references
        v3.total_revenue and v3.order_count.

        The measures SQL should contain the base components (line_total_sum, order_id_count).
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.aov_growth_index"],
                "dimensions": ["v3.order_details.status"],
            },
        )

        assert response.status_code == 200, response.json()
        data = response.json()

        # Should have grain groups with base components
        assert "grain_groups" in data
        assert len(data["grain_groups"]) >= 1

        # Verify the SQL structure using assert_sql_equal
        # Uses merged grain group approach: order_id as grain column for COUNT DISTINCT
        gg = data["grain_groups"][0]
        assert_sql_equal(
            gg["sql"],
            """
            WITH
            v3_order_details AS (
                SELECT o.order_id, o.status, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT
                t1.status,
                t1.order_id,
                SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            GROUP BY t1.status, t1.order_id
            """,
        )

    @pytest.mark.asyncio
    async def test_nested_derived_window_metric_decomposes_to_base_components(
        self,
        client_with_build_v3,
    ):
        """
        Test that a window function nested derived metric decomposes correctly.

        v3.wow_aov_change uses LAG() on v3.avg_order_value, which itself
        references v3.total_revenue and v3.order_count.

        The measures SQL should contain the base components.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.wow_aov_change"],
                "dimensions": ["v3.product.category"],
            },
        )

        assert response.status_code == 200, response.json()
        data = response.json()

        # Should have grain groups with base components
        assert "grain_groups" in data
        assert len(data["grain_groups"]) >= 1

        # Verify the SQL structure for the first grain group
        # Uses merged grain group approach with week dimension for window function
        gg = data["grain_groups"][0]
        assert_sql_equal(
            gg["sql"],
            """
            WITH
            v3_date AS (
                SELECT date_id, week
                FROM default.v3.dates
            ),
            v3_order_details AS (
                SELECT o.order_id, o.order_date, oi.product_id, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
                SELECT product_id, category
                FROM default.v3.products
            )
            SELECT
                t2.category,
                t3.week,
                t1.order_id,
                SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            LEFT OUTER JOIN v3_date t3 ON t1.order_date = t3.date_id
            GROUP BY t2.category, t3.week, t1.order_id
            """,
        )

    @pytest.mark.asyncio
    async def test_nested_derived_cross_fact_decomposes_to_base_components(
        self,
        client_with_build_v3,
    ):
        """
        Test cross-fact nested derived metric decomposition.

        v3.efficiency_ratio = v3.avg_order_value / v3.pages_per_session

        Both intermediate metrics come from different facts, so we should
        get grain groups from both order_details and page_views.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.efficiency_ratio"],
                "dimensions": ["v3.product.category"],
            },
        )

        assert response.status_code == 200, response.json()
        data = response.json()

        # Should have two grain groups - one from each fact
        assert "grain_groups" in data
        assert len(data["grain_groups"]) == 2

        # Find the grain groups by their parent
        order_gg = None
        page_gg = None
        for gg in data["grain_groups"]:
            if "order_details" in gg["sql"].lower():
                order_gg = gg
            if "page_views" in gg["sql"].lower():
                page_gg = gg

        assert order_gg is not None, "Should have grain group from order_details"
        assert page_gg is not None, "Should have grain group from page_views"

        # Verify order_details grain group has components for total_revenue/order_count
        # Uses merged grain group approach with order_id as grain column
        assert_sql_equal(
            order_gg["sql"],
            """
            WITH
            v3_order_details AS (
                SELECT o.order_id, oi.product_id, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
                SELECT product_id, category
                FROM default.v3.products
            )
            SELECT
                t2.category,
                t1.order_id,
                SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            GROUP BY t2.category, t1.order_id
            """,
        )

        # Verify page_views grain group has components for page_view_count/session_count
        assert_sql_equal(
            page_gg["sql"],
            """
            WITH
            v3_page_views_enriched AS (
            SELECT  view_id,
                session_id,
                product_id
            FROM default.v3.page_views
            ),
            v3_product AS (
            SELECT  product_id,
                category
            FROM default.v3.products
            )
            SELECT  t2.category,
                t1.session_id,
                COUNT(t1.view_id) view_id_count_f41e2db4
            FROM v3_page_views_enriched t1 LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            GROUP BY  t2.category, t1.session_id
            """,
        )


class TestCubeMaterializeEndpoint:
    """Tests for the POST /cubes/{name}/materialize endpoint."""

    @pytest.mark.asyncio
    async def test_cube_materialize_nonexistent_cube_returns_error(
        self,
        client_with_build_v3,
    ):
        """
        Test that materialize endpoint returns error for nonexistent cube.
        """
        response = await client_with_build_v3.post(
            "/cubes/nonexistent.cube/materialize",
            json={
                "schedule": "0 0 * * *",
            },
        )
        # Should get 404 or 422 because cube not found
        assert response.status_code in (404, 422)


class TestFilterOnlyDimensions:
    """Tests for filter-only dimensions (dimensions in WHERE but not in GROUP BY)."""

    @pytest.mark.asyncio
    async def test_filter_on_local_column(
        self,
        client_with_build_v3,
    ):
        """
        Test filtering on a local column (no external dimension join needed).
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
                "filters": ["v3.order_details.status = 'completed'"],
            },
        )

        assert response.status_code == 200, response.json()
        data = get_first_grain_group(response.json())

        # Filter should be applied in the SQL
        assert_sql_equal(
            data["sql"],
            """
            WITH v3_order_details AS (
                SELECT o.status, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
                WHERE o.status = 'completed'
            )
            SELECT t1.status, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            GROUP BY t1.status
            """,
        )

        # Output columns should include status (it's in GROUP BY) and the metric component
        assert data["columns"] == [
            {
                "name": "status",
                "type": "string",
                "semantic_entity": "v3.order_details.status",
                "semantic_type": "dimension",
            },
            {
                "name": "line_total_sum_e1f61696",
                "type": "double",
                "semantic_entity": "v3.total_revenue:line_total_sum_e1f61696",
                "semantic_type": "metric_component",
            },
        ]

        # Verify grain includes status
        assert data["grain"] == ["status"]

    @pytest.mark.asyncio
    async def test_filter_only_dimension_excluded_from_output(
        self,
        client_with_build_v3,
    ):
        """
        Test that a dimension used only in a filter is not included in output.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
                "filters": ["v3.product.category = 'Electronics'"],
            },
        )

        assert response.status_code == 200, response.json()
        data = get_first_grain_group(response.json())

        # Filter dimension should be JOINed but not in output
        assert_sql_equal(
            data["sql"],
            """
            WITH
            v3_order_details AS (
                SELECT o.status, oi.product_id, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
                SELECT product_id, category
                FROM default.v3.products
                WHERE category = 'Electronics'
            )
            SELECT t1.status, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            WHERE t2.category = 'Electronics'
            GROUP BY t1.status
            """,
        )

        # Output columns should only include status and the metric component
        # category should NOT be in output
        assert data["columns"] == [
            {
                "name": "status",
                "type": "string",
                "semantic_entity": "v3.order_details.status",
                "semantic_type": "dimension",
            },
            {
                "name": "line_total_sum_e1f61696",
                "type": "double",
                "semantic_entity": "v3.total_revenue:line_total_sum_e1f61696",
                "semantic_type": "metric_component",
            },
        ]

        # Grain should only include status, not category
        assert data["grain"] == ["status"]


class TestDeepNestedStructAccess:
    """
    Tests that deep (3+) nested struct column paths survive end-to-end through both
    the v2 and v3 SQL generation pipelines.

    v3: embeds the transform SQL verbatim into a CTE — the raw dotted path must pass
    through unchanged.  v2: compiles the AST (set_struct_ref=True is fired) and
    struct_column_name() must return the full intermediate path, not just the leaf.
    """

    @pytest.mark.asyncio
    async def test_v3_measures_sql_preserves_deep_struct_path(
        self,
        client_with_build_v3,
    ):
        """
        v3 measures SQL embeds the transform query verbatim in a CTE. A 3-level deep
        struct path (e.g. src.device_health.network.rtt_ms) must appear unchanged in
        that CTE — no intermediate rewriting should strip part of the path.
        """
        client = client_with_build_v3

        # Ensure the struct_v3 namespace exists before creating nodes
        await client.post("/namespaces/struct_v3/")

        # Source with a 3-level nested struct: device_health.network.rtt_ms
        src_resp = await client.post(
            "/nodes/source",
            json={
                "name": "struct_v3.probe_source",
                "display_name": "Probe Source",
                "description": "Source with nested struct column",
                "catalog": "default",
                "schema_": "v3",
                "table": "probes",
                "columns": [
                    {"name": "probe_id", "type": "bigint"},
                    {
                        "name": "device_health",
                        "type": "struct<network struct<rtt_ms bigint, loss_pct double>>",
                    },
                ],
                "mode": "published",
            },
        )
        assert src_resp.status_code == 200, src_resp.json()

        # Transform drilling into the 3-level deep struct path
        transform_resp = await client.post(
            "/nodes/transform",
            json={
                "name": "struct_v3.probe_transform",
                "display_name": "Probe Transform",
                "description": "Accesses deep struct field",
                "query": (
                    "SELECT probe_id, "
                    "device_health.network.rtt_ms AS rtt_ms "
                    "FROM struct_v3.probe_source"
                ),
                "mode": "published",
            },
        )
        assert transform_resp.status_code == 201, transform_resp.json()

        # Transform drilling into the 3-level deep struct path with an alias on the source node
        transform_resp = await client.post(
            "/nodes/transform",
            json={
                "name": "struct_v3.probe_transform_w_alias",
                "display_name": "Probe Transform",
                "description": "Accesses deep struct field",
                "query": (
                    "SELECT probe_id, "
                    "ps.device_health.network.rtt_ms AS rtt_ms "
                    "FROM struct_v3.probe_source ps"
                ),
                "mode": "published",
            },
        )
        assert transform_resp.status_code == 201, transform_resp.json()

        # Metric aggregating the deep struct field
        metric_resp = await client.post(
            "/nodes/metric",
            json={
                "name": "struct_v3.total_rtt",
                "display_name": "Total RTT",
                "description": "Sum of round-trip time from nested struct",
                "query": "SELECT SUM(rtt_ms) FROM struct_v3.probe_transform",
                "mode": "published",
            },
        )
        assert metric_resp.status_code == 201, metric_resp.json()

        # Metric aggregating the deep struct field with an alias on the transform node
        metric_resp = await client.post(
            "/nodes/metric",
            json={
                "name": "struct_v3.total_rtt_with_alias",
                "display_name": "Total RTT with Alias",
                "description": "Sum of round-trip time from nested struct",
                "query": "SELECT SUM(rtt_ms) FROM struct_v3.probe_transform_w_alias",
                "mode": "published",
            },
        )
        assert metric_resp.status_code == 201, metric_resp.json()

        sql_resp = await client.get(
            "/sql/measures/v3/",
            params={"metrics": ["struct_v3.total_rtt"]},
        )
        assert sql_resp.status_code == 200, sql_resp.json()

        sql = sql_resp.json()["grain_groups"][0]["sql"]

        sql_resp = await client.get(
            "/sql/measures/v3/",
            params={"metrics": ["struct_v3.total_rtt_with_alias"]},
        )
        assert sql_resp.status_code == 200, sql_resp.json()

        sql_with_alias = sql_resp.json()["grain_groups"][0]["sql"]

        # The CTE for the transform must embed the original query verbatim, preserving
        # the full struct path device_health.network.rtt_ms.
        assert_sql_equal(
            sql,
            """
            WITH struct_v3_probe_transform AS (
                SELECT device_health.network.rtt_ms AS rtt_ms
                FROM default.v3.probes
            )
            SELECT SUM(t1.rtt_ms) rtt_ms_sum_HASH
            FROM struct_v3_probe_transform t1
            """,
            normalize_aliases=True,
        )

        assert_sql_equal(
            sql_with_alias,
            """
            WITH struct_v3_probe_transform_w_alias AS (
                SELECT ps.device_health.network.rtt_ms AS rtt_ms
                FROM default.v3.probes ps
            )
            SELECT SUM(t1.rtt_ms) rtt_ms_sum_HASH
            FROM struct_v3_probe_transform_w_alias t1
            """,
            normalize_aliases=True,
        )


class TestIntermediateDimLinkUpstreamExpansion:
    """
    Regression tests for intermediate DimensionLink nodes whose upstream SQL
    dependencies were not being loaded into ctx.nodes.

    When a dimension node is only reachable via a DimensionLink (not via
    NodeRelationship from the starting metrics/dimensions), its own upstream
    dependencies would be absent from ctx.nodes. This caused raw DJ node names
    (e.g. "v3.spine_calendar_dim") to appear in generated CTE bodies instead of
    being replaced with their CTE aliases (e.g. "v3_spine_calendar_dim").
    """

    @pytest.fixture
    async def setup_spine_nodes(self, client_with_build_v3):
        """
        Set up a 2-hop DimensionLink chain where the intermediate hop node
        references a non-source node in its SQL (via CROSS JOIN).

        Chain:
          v3.spine_test_count (metric)
            -> v3.src_spine_fact (source/parent)
            -[dim link 1]-> v3.spine_fact_expanded (intermediate dim, cross-joins v3.spine_calendar_dim)
            -[dim link 2]-> v3.spine_output_dim (final dim, requested by user)

          v3.spine_calendar_dim (non-source dimension, referenced in v3.spine_fact_expanded SQL)
            -> v3.src_spine_calendar (source)
        """
        # Source: fact table
        resp = await client_with_build_v3.post(
            "/nodes/source/",
            json={
                "name": "v3.src_spine_fact",
                "description": "Fact table for spine test",
                "columns": [
                    {"name": "id", "type": "int"},
                    {"name": "value", "type": "float"},
                ],
                "mode": "published",
                "catalog": "default",
                "schema_": "v3",
                "table": "spine_fact",
            },
        )
        assert resp.status_code in (200, 201, 409)

        # Source: calendar table (upstream of spine_calendar_dim)
        resp = await client_with_build_v3.post(
            "/nodes/source/",
            json={
                "name": "v3.src_spine_calendar",
                "description": "Calendar source for spine test",
                "columns": [
                    {"name": "day_num", "type": "int"},
                    {"name": "label", "type": "string"},
                ],
                "mode": "published",
                "catalog": "default",
                "schema_": "v3",
                "table": "spine_calendar",
            },
        )
        assert resp.status_code in (200, 201, 409)

        # Non-source dimension: calendar spine - this is what the intermediate node cross-joins
        resp = await client_with_build_v3.post(
            "/nodes/dimension/",
            json={
                "name": "v3.spine_calendar_dim",
                "description": "Calendar dimension for spine expansion",
                "query": "SELECT day_num, label FROM v3.src_spine_calendar",
                "mode": "published",
                "primary_key": ["day_num"],
            },
        )
        assert resp.status_code in (200, 201, 409)

        # Intermediate dimension: cross-joins spine_calendar_dim
        # This node is only reachable via DimensionLink (not NodeRelationship),
        # so its upstream dependencies (spine_calendar_dim) won't be found by
        # find_upstream_node_names() in the first pass.
        resp = await client_with_build_v3.post(
            "/nodes/dimension/",
            json={
                "name": "v3.spine_fact_expanded",
                "description": "Fact expanded with calendar spine (intermediate DimensionLink hop)",
                "query": """
                    SELECT f.id, c.day_num
                    FROM v3.src_spine_fact f
                    CROSS JOIN v3.spine_calendar_dim c
                """,
                "mode": "published",
                "primary_key": ["id"],
            },
        )
        assert resp.status_code in (200, 201, 409)

        # Final dimension: requested by the user
        resp = await client_with_build_v3.post(
            "/nodes/dimension/",
            json={
                "name": "v3.spine_output_dim",
                "description": "Output dimension for spine test",
                "query": "SELECT id, id * 100 AS id_scaled FROM v3.src_spine_fact",
                "mode": "published",
                "primary_key": ["id"],
            },
        )
        assert resp.status_code in (200, 201, 409)

        # Metric: simple count on the fact source
        resp = await client_with_build_v3.post(
            "/nodes/metric/",
            json={
                "name": "v3.spine_test_count",
                "description": "Count metric for spine test",
                "query": "SELECT COUNT(id) FROM v3.src_spine_fact",
                "mode": "published",
            },
        )
        assert resp.status_code in (200, 201, 409)

        # DimensionLink 1: fact source -> intermediate dim (1st hop)
        resp = await client_with_build_v3.post(
            "/nodes/v3.src_spine_fact/link",
            json={
                "dimension_node": "v3.spine_fact_expanded",
                "join_type": "left",
                "join_on": "v3.src_spine_fact.id = v3.spine_fact_expanded.id",
            },
        )
        assert resp.status_code in (200, 201, 409)

        # DimensionLink 2: intermediate dim -> final dim (2nd hop)
        resp = await client_with_build_v3.post(
            "/nodes/v3.spine_fact_expanded/link",
            json={
                "dimension_node": "v3.spine_output_dim",
                "join_type": "left",
                "join_on": "v3.spine_fact_expanded.id = v3.spine_output_dim.id",
            },
        )
        assert resp.status_code in (200, 201, 409)

    @pytest.mark.asyncio
    async def test_intermediate_dim_upstream_node_expanded_in_cte(
        self,
        client_with_build_v3,
        setup_spine_nodes,
    ):
        """
        The intermediate dimension node (v3.spine_fact_expanded) is only added to
        ctx.nodes via preload_join_paths(), not via find_upstream_node_names(). Its
        SQL references v3.spine_calendar_dim (a non-source node) which must also be
        loaded so it gets a CTE and its name is rewritten correctly.

        Without the fix in load_nodes(), v3.spine_calendar_dim would appear as the
        raw dotted DJ node name in the CTE body instead of being replaced with the
        CTE alias v3_spine_calendar_dim.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.spine_test_count"],
                "dimensions": ["v3.spine_output_dim.id_scaled"],
            },
        )
        assert response.status_code == 200

        sql = response.json()["grain_groups"][0]["sql"]

        # v3.spine_calendar_dim is referenced in v3.spine_fact_expanded's SQL via
        # CROSS JOIN. Since spine_fact_expanded is only added to ctx.nodes via
        # preload_join_paths() (not by the initial find_upstream_node_names traversal),
        # its upstream dependency spine_calendar_dim must be loaded in a second pass
        # so that its name gets replaced with the CTE alias in the CTE body.
        assert_sql_equal(
            sql,
            """
            WITH
            v3_spine_calendar_dim AS (
                SELECT day_num, label
                FROM default.v3.spine_calendar
            ),
            v3_spine_output_dim AS (
                SELECT id, id * 100 AS id_scaled
                FROM default.v3.spine_fact
            ),
            v3_spine_fact_expanded AS (
                SELECT f.id
                FROM default.v3.spine_fact f
                CROSS JOIN v3_spine_calendar_dim c
            )
            SELECT t3.id_scaled, COUNT(t1.id) id_count_HASH
            FROM default.v3.spine_fact t1
            LEFT OUTER JOIN v3_spine_fact_expanded t2 ON t1.id = t2.id
            LEFT OUTER JOIN v3_spine_output_dim t3 ON t2.id = t3.id
            GROUP BY t3.id_scaled
            """,
            normalize_aliases=True,
        )


class TestDimReferencingDimViaAlias:
    """
    Integration test for the bug where needed_columns_by_node incorrectly pruned
    CTE projections when a dimension node references another dimension node via
    an aliased JOIN in its SQL body.

    Scenario:
        td.dim_a joins td.dim_b AS b_alias and selects b_alias.p and b_alias.q AS y.
        td.fact links to both td.dim_a and td.dim_b.
        When querying dimensions td.dim_a.x, td.dim_a.y, td.dim_b.p,
        the td_dim_b CTE must project both p and q — even though only p is
        explicitly requested, q is needed by td.dim_a's SQL body.
    """

    @pytest.mark.asyncio
    async def test_dim_b_retains_referenced_column_from_dim_a(
        self,
        client_with_service_setup,
    ):
        """
        Verify that when dim_a references dim_b.q via an alias, the td_dim_b CTE
        retains q in its projection even though only td.dim_b.p was requested.
        """
        # Create namespace
        resp = await client_with_service_setup.post("/namespaces/td/")
        assert resp.status_code in (200, 201), resp.text

        # Source: id, val
        resp = await client_with_service_setup.post(
            "/nodes/source/",
            json={
                "name": "td.src_fact",
                "catalog": "default",
                "schema_": "td",
                "table": "src_fact",
                "columns": [
                    {"name": "id", "type": "int"},
                    {"name": "val", "type": "float"},
                ],
            },
        )
        assert resp.status_code in (200, 201), resp.text

        # Source: id, x
        resp = await client_with_service_setup.post(
            "/nodes/source/",
            json={
                "name": "td.src_a",
                "catalog": "default",
                "schema_": "td",
                "table": "src_a",
                "columns": [
                    {"name": "id", "type": "int"},
                    {"name": "x", "type": "string"},
                ],
            },
        )
        assert resp.status_code in (200, 201), resp.text

        # Source: id, p, q, r
        resp = await client_with_service_setup.post(
            "/nodes/source/",
            json={
                "name": "td.src_b",
                "catalog": "default",
                "schema_": "td",
                "table": "src_b",
                "columns": [
                    {"name": "id", "type": "int"},
                    {"name": "p", "type": "string"},
                    {"name": "q", "type": "string"},
                    {"name": "r", "type": "string"},
                ],
            },
        )
        assert resp.status_code in (200, 201), resp.text

        # dim_b: id, p, q, r — exposes all columns from src_b
        resp = await client_with_service_setup.post(
            "/nodes/dimension/",
            json={
                "name": "td.dim_b",
                "mode": "published",
                "primary_key": ["id"],
                "query": "SELECT id, p, q, r FROM td.src_b",
            },
        )
        assert resp.status_code in (200, 201), resp.text

        # dim_a: joins td.dim_b AS b_alias and selects b_alias.p and b_alias.q AS y
        # This means dim_a's SQL body references two columns from dim_b: p and q
        resp = await client_with_service_setup.post(
            "/nodes/dimension/",
            json={
                "name": "td.dim_a",
                "mode": "published",
                "primary_key": ["id"],
                "query": (
                    "SELECT a.id, a.x, b_alias.p, b_alias.q AS y"
                    " FROM td.src_a a"
                    " CROSS JOIN td.dim_b AS b_alias"
                ),
            },
        )
        assert resp.status_code in (200, 201), resp.text

        # fact transform
        resp = await client_with_service_setup.post(
            "/nodes/transform/",
            json={
                "name": "td.fact",
                "mode": "published",
                "query": "SELECT id, val FROM td.src_fact",
            },
        )
        assert resp.status_code in (200, 201), resp.text

        # metric
        resp = await client_with_service_setup.post(
            "/nodes/metric/",
            json={
                "name": "td.total_val",
                "mode": "published",
                "query": "SELECT SUM(val) FROM td.fact",
            },
        )
        assert resp.status_code in (200, 201), resp.text

        # Link fact -> dim_a and fact -> dim_b
        resp = await client_with_service_setup.post(
            "/nodes/td.fact/link",
            json={
                "dimension_node": "td.dim_a",
                "join_type": "left",
                "join_on": "td.fact.id = td.dim_a.id",
            },
        )
        assert resp.status_code in (200, 201), resp.text

        resp = await client_with_service_setup.post(
            "/nodes/td.fact/link",
            json={
                "dimension_node": "td.dim_b",
                "join_type": "left",
                "join_on": "td.fact.id = td.dim_b.id",
            },
        )
        assert resp.status_code in (200, 201), resp.text

        # Query: request dimensions from both dim_a and dim_b
        resp = await client_with_service_setup.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["td.total_val"],
                "dimensions": [
                    "td.dim_a.x",
                    "td.dim_a.y",
                    "td.dim_b.p",
                ],
            },
        )
        assert resp.status_code == 200, resp.text

        sql = resp.json()["grain_groups"][0]["sql"]

        # The td_dim_b CTE must project id, p, AND q.
        # q is not directly requested but is needed by td_dim_a's SQL body via b_alias.q.
        # If q is missing, td_dim_a's CTE body would be broken.
        assert_sql_equal(
            sql,
            """
            WITH td_dim_b AS (
                SELECT id, p, q
                FROM default.td.src_b
            ),
            td_fact AS (
                SELECT id, val
                FROM default.td.src_fact
            ),
            td_dim_a AS (
                SELECT a.id, a.x, b_alias.q AS y
                FROM default.td.src_a a
                CROSS JOIN td_dim_b AS b_alias
            )
            SELECT
                t2.x,
                t2.y,
                t3.p,
                SUM(t1.val) val_sum_HASH
            FROM td_fact t1
            LEFT OUTER JOIN td_dim_a t2 ON t1.id = t2.id
            LEFT OUTER JOIN td_dim_b t3 ON t1.id = t3.id
            GROUP BY t2.x, t2.y, t3.p
            """,
            normalize_aliases=True,
        )


class TestMetricExprReferencesDimColumn:
    """
    A metric expression may reference a column from a joined dimension node
    using fully-qualified ``<dim_node>.<column>`` notation. The generated SQL
    must:

    1. Preserve that column in the dim's CTE projection (otherwise
       filter_cte_projection drops it as unused).
    2. Rewrite the namespace from the dim node name to the dim's joined table
       alias (otherwise the renderer emits the literal node name, which is
       not a valid table reference).
    """

    @pytest.mark.asyncio
    async def test_dim_column_in_metric_expression(self, client_with_service_setup):
        resp = await client_with_service_setup.post("/namespaces/mx/")
        assert resp.status_code in (200, 201), resp.text

        resp = await client_with_service_setup.post(
            "/nodes/source/",
            json={
                "name": "mx.src_fact",
                "catalog": "default",
                "schema_": "mx",
                "table": "src_fact",
                "columns": [
                    {"name": "id", "type": "int"},
                    {"name": "customer_id", "type": "int"},
                    {"name": "amount", "type": "double"},
                ],
            },
        )
        assert resp.status_code in (200, 201), resp.text

        resp = await client_with_service_setup.post(
            "/nodes/source/",
            json={
                "name": "mx.src_customer",
                "catalog": "default",
                "schema_": "mx",
                "table": "src_customer",
                "columns": [
                    {"name": "customer_id", "type": "int"},
                    {"name": "tier", "type": "string"},
                    {"name": "threshold", "type": "double"},
                ],
            },
        )
        assert resp.status_code in (200, 201), resp.text

        resp = await client_with_service_setup.post(
            "/nodes/dimension/",
            json={
                "name": "mx.customer",
                "mode": "published",
                "primary_key": ["customer_id"],
                "query": ("SELECT customer_id, tier, threshold FROM mx.src_customer"),
            },
        )
        assert resp.status_code in (200, 201), resp.text

        resp = await client_with_service_setup.post(
            "/nodes/transform/",
            json={
                "name": "mx.fact",
                "mode": "published",
                "query": ("SELECT id, customer_id, amount FROM mx.src_fact"),
            },
        )
        assert resp.status_code in (200, 201), resp.text

        # Metric expression directly references mx.customer.threshold — a
        # column on the joined dim node.
        resp = await client_with_service_setup.post(
            "/nodes/metric/",
            json={
                "name": "mx.qualifying_amount",
                "mode": "published",
                "query": (
                    "SELECT SUM(CASE WHEN amount >= mx.customer.threshold "
                    "THEN amount ELSE 0 END) FROM mx.fact"
                ),
            },
        )
        assert resp.status_code in (200, 201), resp.text

        resp = await client_with_service_setup.post(
            "/nodes/mx.fact/link",
            json={
                "dimension_node": "mx.customer",
                "join_type": "left",
                "join_on": "mx.fact.customer_id = mx.customer.customer_id",
            },
        )
        assert resp.status_code in (200, 201), resp.text

        resp = await client_with_service_setup.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["mx.qualifying_amount"],
                "dimensions": ["mx.customer.tier"],
            },
        )
        assert resp.status_code == 200, resp.text

        sql = resp.json()["grain_groups"][0]["sql"]

        # mx_customer CTE must project ``threshold`` (referenced by the
        # metric expression) in addition to ``tier`` (the requested dim) and
        # ``customer_id`` (the join key). The metric expression's
        # ``mx.customer.threshold`` must be rewritten to ``t2.threshold``.
        assert_sql_equal(
            sql,
            """
            WITH mx_customer AS (
                SELECT customer_id, tier, threshold
                FROM default.mx.src_customer
            ),
            mx_fact AS (
                SELECT customer_id, amount
                FROM default.mx.src_fact
            )
            SELECT
                t2.tier,
                SUM(CASE WHEN t1.amount >= t2.threshold
                    THEN t1.amount ELSE 0 END)
                    amount_mx_DOT_customer_DOT_threshold_amount_sum_HASH
            FROM mx_fact t1
            LEFT OUTER JOIN mx_customer t2 ON t1.customer_id = t2.customer_id
            GROUP BY t2.tier
            """,
            normalize_aliases=True,
        )

        # /sql/metrics/v3 wraps the measures grain group and re-aggregates
        # to produce the final metric. The same dim-namespace fix must apply:
        # the inner grain-group CTE must reference ``t2.threshold`` (not
        # ``mx.customer.threshold``) and ``mx_customer`` must still project
        # ``threshold``.
        resp = await client_with_service_setup.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["mx.qualifying_amount"],
                "dimensions": ["mx.customer.tier"],
            },
        )
        assert resp.status_code == 200, resp.text
        metrics_sql = resp.json()["sql"]

        assert_sql_equal(
            metrics_sql,
            """
            WITH
            mx_customer AS (
                SELECT customer_id, tier, threshold
                FROM default.mx.src_customer
            ),
            mx_fact AS (
                SELECT customer_id, amount
                FROM default.mx.src_fact
            ),
            fact_0 AS (
                SELECT
                    t2.tier,
                    SUM(CASE WHEN t1.amount >= t2.threshold
                        THEN t1.amount ELSE 0 END)
                        amount_mx_DOT_customer_DOT_threshold_amount_sum_HASH
                FROM mx_fact t1
                LEFT OUTER JOIN mx_customer t2 ON t1.customer_id = t2.customer_id
                GROUP BY t2.tier
            )
            SELECT
                fact_0.tier AS tier,
                SUM(fact_0.amount_mx_DOT_customer_DOT_threshold_amount_sum_HASH)
                    AS qualifying_amount
            FROM fact_0
            GROUP BY fact_0.tier
            """,
            normalize_aliases=True,
        )


class TestSparkJoinHints:
    """
    Tests for Spark SQL join hints emitted via spark_hints on DimensionLink.

    When spark_hints is set on a dimension link, the generated SELECT should
    include a /*+ HINT(alias) */ comment immediately after SELECT.
    """

    @pytest.fixture
    async def setup_spark_hint_nodes(self, client_with_build_v3):
        """
        Set up a minimal fact + dimension graph for spark hint tests.

        Graph:
          sh.total_orders (metric)
            -> sh.orders (source/parent)
            -[dim link, spark_hints=broadcast]-> sh.customer (dimension)
            -[dim link, spark_hints=merge]-> sh.product (dimension)
        """
        # Create namespace
        await client_with_build_v3.post("/namespaces/sh/", json={})

        # Source: orders
        resp = await client_with_build_v3.post(
            "/nodes/source/",
            json={
                "name": "sh.orders",
                "description": "Orders fact table",
                "columns": [
                    {"name": "order_id", "type": "int"},
                    {"name": "customer_id", "type": "int"},
                    {"name": "product_id", "type": "int"},
                ],
                "mode": "published",
                "catalog": "default",
                "schema_": "sh",
                "table": "orders",
            },
        )
        assert resp.status_code in (200, 201, 409)

        # Source: customers
        resp = await client_with_build_v3.post(
            "/nodes/source/",
            json={
                "name": "sh.src_customer",
                "description": "Customers source",
                "columns": [
                    {"name": "customer_id", "type": "int"},
                    {"name": "country", "type": "string"},
                ],
                "mode": "published",
                "catalog": "default",
                "schema_": "sh",
                "table": "customers",
            },
        )
        assert resp.status_code in (200, 201, 409)

        # Source: products
        resp = await client_with_build_v3.post(
            "/nodes/source/",
            json={
                "name": "sh.src_product",
                "description": "Products source",
                "columns": [
                    {"name": "product_id", "type": "int"},
                    {"name": "category", "type": "string"},
                ],
                "mode": "published",
                "catalog": "default",
                "schema_": "sh",
                "table": "products",
            },
        )
        assert resp.status_code in (200, 201, 409)

        # Dimension: customer
        resp = await client_with_build_v3.post(
            "/nodes/dimension/",
            json={
                "name": "sh.customer",
                "description": "Customer dimension",
                "query": "SELECT customer_id, country FROM sh.src_customer",
                "mode": "published",
                "primary_key": ["customer_id"],
            },
        )
        assert resp.status_code in (200, 201, 409)

        # Dimension: product
        resp = await client_with_build_v3.post(
            "/nodes/dimension/",
            json={
                "name": "sh.product",
                "description": "Product dimension",
                "query": "SELECT product_id, category FROM sh.src_product",
                "mode": "published",
                "primary_key": ["product_id"],
            },
        )
        assert resp.status_code in (200, 201, 409)

        # Metric
        resp = await client_with_build_v3.post(
            "/nodes/metric/",
            json={
                "name": "sh.total_orders",
                "description": "Total orders",
                "query": "SELECT COUNT(order_id) FROM sh.orders",
                "mode": "published",
            },
        )
        assert resp.status_code in (200, 201, 409)

        # Dimension link: orders -> customer (broadcast hint)
        resp = await client_with_build_v3.post(
            "/nodes/sh.orders/link",
            json={
                "dimension_node": "sh.customer",
                "join_type": "left",
                "join_on": "sh.orders.customer_id = sh.customer.customer_id",
                "spark_hints": "broadcast",
            },
        )
        assert resp.status_code in (200, 201, 409)

        # Dimension link: orders -> product (merge hint)
        resp = await client_with_build_v3.post(
            "/nodes/sh.orders/link",
            json={
                "dimension_node": "sh.product",
                "join_type": "left",
                "join_on": "sh.orders.product_id = sh.product.product_id",
                "spark_hints": "merge",
            },
        )
        assert resp.status_code in (200, 201, 409)

    @pytest.mark.asyncio
    async def test_single_broadcast_hint(
        self,
        client_with_build_v3,
        setup_spark_hint_nodes,
    ):
        """
        When a dimension link has spark_hints=broadcast, the generated SQL should
        include /*+ BROADCAST(alias) */ immediately after SELECT.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["sh.total_orders"],
                "dimensions": ["sh.customer.country"],
            },
        )
        assert response.status_code == 200
        sql = response.json()["grain_groups"][0]["sql"]
        assert_sql_equal(
            sql,
            """
            WITH
            sh_customer AS (
                SELECT customer_id, country FROM default.sh.customers
            )
            SELECT /*+ BROADCAST(t2) */
                t2.country,
                COUNT(t1.order_id) order_id_count_HASH
            FROM default.sh.orders t1
            LEFT OUTER JOIN sh_customer t2 ON t1.customer_id = t2.customer_id
            GROUP BY t2.country
            """,
            normalize_aliases=True,
        )

    @pytest.mark.asyncio
    async def test_multiple_hints(self, client_with_build_v3, setup_spark_hint_nodes):
        """
        When two dimension links have different spark_hints, both hints should
        appear in the /*+ ... */ comment.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["sh.total_orders"],
                "dimensions": ["sh.customer.country", "sh.product.category"],
            },
        )
        assert response.status_code == 200
        sql = response.json()["grain_groups"][0]["sql"]
        assert_sql_equal(
            sql,
            """
            WITH
            sh_customer AS (
                SELECT customer_id, country FROM default.sh.customers
            ),
            sh_product AS (
                SELECT product_id, category FROM default.sh.products
            )
            SELECT /*+ BROADCAST(t2), MERGE(t3) */
                t2.country,
                t3.category,
                COUNT(t1.order_id) order_id_count_HASH
            FROM default.sh.orders t1
            LEFT OUTER JOIN sh_customer t2 ON t1.customer_id = t2.customer_id
            LEFT OUTER JOIN sh_product t3 ON t1.product_id = t3.product_id
            GROUP BY t2.country, t3.category
            """,
            normalize_aliases=True,
        )

    @pytest.mark.asyncio
    async def test_no_hints_when_not_configured(self, client_with_build_v3):
        """
        When no dimension links have spark_hints, the generated SQL should
        not contain any hint comment.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
            },
        )
        assert response.status_code == 200
        sql = response.json()["grain_groups"][0]["sql"]
        assert_sql_equal(
            sql,
            """
            WITH v3_order_details AS (
                SELECT o.status, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t1.status, SUM(t1.line_total) line_total_sum_HASH
            FROM v3_order_details t1
            GROUP BY t1.status
            """,
            normalize_aliases=True,
        )


class TestOuterJoinFilterSafety:
    """Filters must not silently turn OUTER JOINs into INNER JOINs.

    The bug: when a parent transform's body contains an OUTER JOIN and a
    filter resolves to the non-preserved side, applying the filter in WHERE
    drops the NULL fill-in rows the OUTER JOIN was meant to preserve.

    The fix: route such filters through the inner-side relation as a wrapping
    subquery — both for filters pushed into the parent CTE itself, and for
    parent-alias atoms that would otherwise land on the outer query's WHERE
    after a build-time RIGHT/FULL OUTER JOIN to a dim.
    """

    @pytest.fixture
    async def setup_right_outer_join_parent(self, client_with_build_v3):
        """A transform whose body has ``orders RIGHT OUTER JOIN dates``.

        ``orders o`` is the non-preserved (left) side; ``dates d`` is the
        preserved (right) side.  Filters on ``o.*`` columns post-join would
        eliminate dates rows that have no matching order — the failure mode
        we want to verify the builder avoids.
        """
        response = await client_with_build_v3.post(
            "/nodes/transform/",
            json={
                "name": "v3.dates_with_orders",
                "display_name": "Dates with Orders",
                "description": "All dates, joined to orders that fall on them.",
                "mode": "published",
                "query": """
                    SELECT
                        d.date_id,
                        o.order_id,
                        o.customer_id,
                        o.status,
                        o.order_date
                    FROM v3.src_orders o
                    RIGHT OUTER JOIN v3.src_dates d ON o.order_date = d.date_id
                """,
                "columns": [
                    {
                        "name": "date_id",
                        "type": "int",
                        "attributes": [{"attribute_type": {"name": "primary_key"}}],
                    },
                    {"name": "order_id", "type": "int"},
                    {"name": "customer_id", "type": "int"},
                    {"name": "status", "type": "string"},
                    {"name": "order_date", "type": "int"},
                ],
            },
        )
        assert response.status_code in (200, 201), response.text

        response = await client_with_build_v3.post(
            "/nodes/metric/",
            json={
                "name": "v3.dates_with_orders_count",
                "display_name": "Order Count via Dates",
                "mode": "published",
                "query": "SELECT COUNT(order_id) FROM v3.dates_with_orders",
            },
        )
        assert response.status_code in (200, 201), response.text

    @pytest.mark.asyncio
    async def test_local_filter_on_non_preserved_side_wraps_inside_parent_cte(
        self,
        client_with_build_v3,
        setup_right_outer_join_parent,
    ):
        """A filter on ``status`` (a column from the non-preserved ``o`` side)
        must be applied INSIDE the orders subquery — before the RIGHT OUTER
        JOIN — not in the CTE body's WHERE.  Verifies the inner-side wrapping
        path inside ``_inject_filter_into_where``.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.dates_with_orders_count"],
                "dimensions": ["v3.dates_with_orders.date_id"],
                "filters": ["v3.dates_with_orders.status = 'completed'"],
            },
        )
        assert response.status_code == 200, response.json()
        sql = get_first_grain_group(response.json())["sql"]
        assert_sql_equal(
            sql,
            """
            WITH v3_dates_with_orders AS (
                SELECT
                    d.date_id,
                    o.order_id,
                    o.status
                FROM (SELECT * FROM default.v3.orders o WHERE o.status = 'completed') o
                RIGHT OUTER JOIN default.v3.dates d ON o.order_date = d.date_id
            )
            SELECT
                t1.date_id,
                COUNT(t1.order_id) order_id_count_HASH
            FROM v3_dates_with_orders t1
            GROUP BY t1.date_id
            """,
            normalize_aliases=True,
        )

    @pytest.mark.asyncio
    async def test_filter_on_preserved_side_lands_in_cte_where(
        self,
        client_with_build_v3,
        setup_right_outer_join_parent,
    ):
        """A filter on ``date_id`` (the preserved ``d`` side) is safe as a
        plain CTE WHERE — pushdown into the dates subquery is unnecessary.
        Verifies the classifier doesn't over-trigger on preserved-side filters.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.dates_with_orders_count"],
                "dimensions": ["v3.dates_with_orders.date_id"],
                "filters": ["v3.dates_with_orders.date_id >= 20240101"],
            },
        )
        assert response.status_code == 200, response.json()
        sql = get_first_grain_group(response.json())["sql"]
        assert_sql_equal(
            sql,
            """
            WITH v3_dates_with_orders AS (
                SELECT
                    d.date_id,
                    o.order_id
                FROM default.v3.orders o
                RIGHT OUTER JOIN default.v3.dates d ON o.order_date = d.date_id
                WHERE d.date_id >= 20240101
            )
            SELECT
                t1.date_id,
                COUNT(t1.order_id) order_id_count_HASH
            FROM v3_dates_with_orders t1
            GROUP BY t1.date_id
            """,
            normalize_aliases=True,
        )


class TestUpstreamFilterOnlyPushdown:
    """A filter on a dim that has no direct link from the parent node, but
    DOES have a link from an upstream node, gets pushed down as a filter on
    the parent's local FK column — matching v2's filter-only behavior."""

    @pytest.mark.asyncio
    async def test_filter_on_dim_linked_only_on_upstream(
        self,
        module__client_with_build_v3,
    ):
        """v3.order_details has a link to v3.product on product_id. A wrapper
        transform that selects FROM v3.order_details has no links of its own.
        Filtering on v3.product.product_id should resolve to the wrapper's
        product_id column via upstream link traversal."""
        client = module__client_with_build_v3

        await client.post(
            "/nodes/transform/",
            json={
                "name": "v3.order_details_wrapper_filter_only",
                "description": "wrapper without dim links",
                "query": (
                    "SELECT order_id, line_number, product_id, quantity, "
                    "unit_price, quantity * unit_price AS line_total "
                    "FROM v3.order_details"
                ),
                "mode": "published",
                "primary_key": ["order_id", "line_number"],
            },
        )
        await client.post(
            "/nodes/metric/",
            json={
                "name": "v3.wrapper_filter_only_total_revenue",
                "description": "Total revenue from wrapper",
                "query": (
                    "SELECT SUM(line_total) FROM v3.order_details_wrapper_filter_only"
                ),
                "mode": "published",
            },
        )

        response = await client.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.wrapper_filter_only_total_revenue"],
                "dimensions": [
                    "v3.order_details_wrapper_filter_only.order_id",
                ],
                "filters": ["v3.product.product_id = 7"],
            },
        )

        assert response.status_code == 200, response.json()
        data = get_first_grain_group(response.json())

        assert_sql_equal(
            data["sql"],
            """
            WITH
            v3_order_details AS (
                SELECT
                    o.order_id,
                    oi.line_number,
                    o.customer_id,
                    o.order_date,
                    o.from_location_id,
                    o.to_location_id,
                    o.status,
                    oi.product_id,
                    oi.quantity,
                    oi.unit_price,
                    oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
                WHERE oi.product_id = 7
            ),
            v3_order_details_wrapper_filter_only AS (
                SELECT
                    order_id,
                    product_id,
                    quantity * unit_price AS line_total
                FROM v3_order_details
                WHERE product_id = 7
            )
            SELECT
                t1.order_id,
                SUM(t1.line_total) line_total_sum_HASH
            FROM v3_order_details_wrapper_filter_only t1
            GROUP BY t1.order_id
            """,
            normalize_aliases=True,
        )

    @pytest.mark.asyncio
    async def test_pushdown_into_upstream_source_when_fk_col_not_on_parent(
        self,
        module__client_with_build_v3,
    ):
        """A filter-only dim whose FK column lives on an upstream SOURCE
        and is NOT projected up to the parent transform must be pushed
        into the child transform's CTE, qualified by the source's alias.
        Exercises ``_resolve_pushdown_target`` for the SOURCE branch and
        ``_rewrite_filter_col_refs`` with a qualifier.
        """
        client = module__client_with_build_v3

        await client.post(
            "/nodes/source/",
            json={
                "name": "v3.src_audit_dates",
                "description": "audit dates",
                "columns": [{"name": "dateint", "type": "int"}],
                "mode": "published",
                "catalog": "default",
                "schema_": "v3",
                "table": "audit_dates",
            },
        )
        await client.post(
            "/nodes/dimension/",
            json={
                "name": "v3.audit_date_dim",
                "description": "Audit date dim",
                "query": "SELECT dateint FROM v3.src_audit_dates",
                "mode": "published",
                "primary_key": ["dateint"],
            },
        )
        await client.post(
            "/nodes/source/",
            json={
                "name": "v3.src_audit_log",
                "description": "Account audit events",
                "columns": [
                    {"name": "audit_id", "type": "int"},
                    {"name": "audit_date", "type": "int"},
                    {"name": "account_id", "type": "int"},
                    {"name": "event_type", "type": "string"},
                ],
                "mode": "published",
                "catalog": "default",
                "schema_": "v3",
                "table": "audit_log",
            },
        )
        await client.post(
            "/nodes/v3.src_audit_log/link",
            json={
                "dimension_node": "v3.audit_date_dim",
                "join_type": "inner",
                "join_on": ("v3.src_audit_log.audit_date = v3.audit_date_dim.dateint"),
            },
        )
        await client.post(
            "/nodes/transform/",
            json={
                "name": "v3.account_events",
                "description": "Event counts per account",
                "query": (
                    "SELECT account_id, event_type, COUNT(*) AS event_count "
                    "FROM v3.src_audit_log a "
                    "GROUP BY account_id, event_type"
                ),
                "mode": "published",
                "primary_key": ["account_id", "event_type"],
            },
        )
        await client.post(
            "/nodes/metric/",
            json={
                "name": "v3.total_event_count",
                "description": "Total events",
                "query": "SELECT SUM(event_count) FROM v3.account_events",
                "mode": "published",
            },
        )

        # Two separate filter strings on the same filter-only dim — each
        # registers an independent pushdown entry on the same target CTE,
        # exercising the AND-combine branch in build_grain_group_sql.
        # Also include an unrelated filter on a parent column so the
        # pushdown loop sees filter strings that DON'T reference the
        # target dim (covers branch fall-through in the dim-match scan).
        response = await client.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_event_count"],
                "dimensions": ["v3.account_events.event_type"],
                "filters": [
                    "v3.audit_date_dim.dateint >= 20260101",
                    "v3.audit_date_dim.dateint <= 20260131",
                    "v3.account_events.event_type = 'login'",
                ],
            },
        )

        assert response.status_code == 200, response.json()
        sql = get_first_grain_group(response.json())["sql"]
        assert_sql_equal(
            sql,
            """
            WITH v3_account_events AS (
                SELECT
                    account_id,
                    event_type,
                    COUNT(*) AS event_count
                FROM default.v3.audit_log a
                WHERE
                    a.audit_date >= 20260101
                    AND a.audit_date <= 20260131
                    AND event_type = 'login'
                GROUP BY account_id, event_type
            )
            SELECT
                t1.event_type,
                SUM(t1.event_count) event_count_sum_HASH
            FROM v3_account_events t1
            GROUP BY t1.event_type
            """,
            normalize_aliases=True,
        )

    @pytest.mark.asyncio
    async def test_filter_only_via_parent_column_annotation(
        self,
        module__client_with_build_v3,
    ):
        """A column-level ``dimension``/``dimension_column`` annotation on
        the parent's projection is the cheapest resolution path — no BFS,
        no pushdown. Filter lands on the parent's local column.
        Exercises the layer-1 annotation match in
        ``_resolve_filter_only_dim``.
        """
        client = module__client_with_build_v3

        await client.post(
            "/nodes/source/",
            json={
                "name": "v3.src_status_codes",
                "description": "status code dim source",
                "columns": [
                    {"name": "status_code", "type": "string"},
                    {"name": "status_label", "type": "string"},
                ],
                "mode": "published",
                "catalog": "default",
                "schema_": "v3",
                "table": "status_codes",
            },
        )
        await client.post(
            "/nodes/dimension/",
            json={
                "name": "v3.status_dim",
                "description": "status dim",
                "query": ("SELECT status_code, status_label FROM v3.src_status_codes"),
                "mode": "published",
                "primary_key": ["status_code"],
            },
        )
        await client.post(
            "/nodes/source/",
            json={
                "name": "v3.src_status_log",
                "description": "status log source",
                "columns": [
                    {"name": "log_id", "type": "int"},
                    {"name": "raw_status", "type": "string"},
                ],
                "mode": "published",
                "catalog": "default",
                "schema_": "v3",
                "table": "status_log",
            },
        )
        await client.post(
            "/nodes/transform/",
            json={
                "name": "v3.status_events",
                "description": "events by status",
                "query": (
                    "SELECT log_id, raw_status AS event_status FROM v3.src_status_log"
                ),
                "mode": "published",
                "primary_key": ["log_id"],
            },
        )
        # Annotate event_status on the parent with
        # dimension=v3.status_dim, dimension_column=status_code.
        await client.post(
            "/nodes/v3.status_events/columns/event_status/",
            params={
                "dimension": "v3.status_dim",
                "dimension_column": "status_code",
            },
        )
        await client.post(
            "/nodes/metric/",
            json={
                "name": "v3.event_count_by_status",
                "description": "count of status events",
                "query": "SELECT COUNT(log_id) FROM v3.status_events",
                "mode": "published",
            },
        )

        response = await client.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.event_count_by_status"],
                "dimensions": ["v3.status_events.log_id"],
                "filters": ["v3.status_dim.status_code = 'OPEN'"],
            },
        )

        assert response.status_code == 200, response.json()
        sql = get_first_grain_group(response.json())["sql"]
        assert_sql_equal(
            sql,
            """
            WITH v3_status_events AS (
                SELECT log_id, raw_status AS event_status
                FROM default.v3.status_log
                WHERE raw_status = 'OPEN'
            )
            SELECT
                t1.log_id,
                COUNT(t1.log_id) log_id_count_HASH
            FROM v3_status_events t1
            GROUP BY t1.log_id
            """,
            normalize_aliases=True,
        )

    @pytest.mark.asyncio
    async def test_pushdown_into_upstream_transform_unaliased(
        self,
        module__client_with_build_v3,
    ):
        """A filter-only dim whose link lives on a TRANSFORM upstream (not
        a SOURCE) and whose FK column is NOT projected onto the parent
        gets pushed into the linked transform's own CTE — unaliased,
        because we know the FK column name on that CTE directly.
        Exercises ``_resolve_pushdown_target``'s TRANSFORM branch.
        """
        client = module__client_with_build_v3

        await client.post(
            "/nodes/source/",
            json={
                "name": "v3.src_session_events",
                "description": "raw session events",
                "columns": [
                    {"name": "session_id", "type": "int"},
                    {"name": "event_date", "type": "int"},
                    {"name": "duration_sec", "type": "int"},
                ],
                "mode": "published",
                "catalog": "default",
                "schema_": "v3",
                "table": "session_events",
            },
        )
        await client.post(
            "/nodes/source/",
            json={
                "name": "v3.src_event_date",
                "description": "event date dim source",
                "columns": [{"name": "dateint", "type": "int"}],
                "mode": "published",
                "catalog": "default",
                "schema_": "v3",
                "table": "event_dates",
            },
        )
        await client.post(
            "/nodes/dimension/",
            json={
                "name": "v3.event_date_dim",
                "description": "event date dim",
                "query": "SELECT dateint FROM v3.src_event_date",
                "mode": "published",
                "primary_key": ["dateint"],
            },
        )
        # Transform projecting event_date — the link will live HERE
        # (transform, not source).
        await client.post(
            "/nodes/transform/",
            json={
                "name": "v3.session_durations",
                "description": "per-session duration",
                "query": (
                    "SELECT session_id, event_date, "
                    "SUM(duration_sec) AS total_duration "
                    "FROM v3.src_session_events "
                    "GROUP BY session_id, event_date"
                ),
                "mode": "published",
                "primary_key": ["session_id", "event_date"],
            },
        )
        await client.post(
            "/nodes/v3.session_durations/link",
            json={
                "dimension_node": "v3.event_date_dim",
                "join_type": "inner",
                "join_on": (
                    "v3.session_durations.event_date = v3.event_date_dim.dateint"
                ),
            },
        )
        # Downstream wrapper that aggregates away event_date — the FK col
        # is no longer on the parent's projection.
        await client.post(
            "/nodes/transform/",
            json={
                "name": "v3.session_totals",
                "description": "totals across all dates",
                "query": (
                    "SELECT session_id, SUM(total_duration) AS lifetime_duration "
                    "FROM v3.session_durations "
                    "GROUP BY session_id"
                ),
                "mode": "published",
                "primary_key": ["session_id"],
            },
        )
        await client.post(
            "/nodes/metric/",
            json={
                "name": "v3.total_lifetime_duration",
                "description": "Sum lifetime duration",
                "query": ("SELECT SUM(lifetime_duration) FROM v3.session_totals"),
                "mode": "published",
            },
        )

        response = await client.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_lifetime_duration"],
                "dimensions": ["v3.session_totals.session_id"],
                "filters": ["v3.event_date_dim.dateint = 20260101"],
            },
        )

        assert response.status_code == 200, response.json()
        sql = get_first_grain_group(response.json())["sql"]
        assert_sql_equal(
            sql,
            """
            WITH v3_session_durations AS (
                SELECT
                    session_id,
                    event_date,
                    SUM(duration_sec) AS total_duration
                FROM default.v3.session_events
                WHERE event_date = 20260101
                GROUP BY session_id, event_date
            ),
            v3_session_totals AS (
                SELECT
                    session_id,
                    SUM(total_duration) AS lifetime_duration
                FROM v3_session_durations
                GROUP BY session_id
            )
            SELECT
                t1.session_id,
                SUM(t1.lifetime_duration) lifetime_duration_sum_HASH
            FROM v3_session_totals t1
            GROUP BY t1.session_id
            """,
            normalize_aliases=True,
        )

    @pytest.mark.asyncio
    async def test_layer_2_filter_only_interacts_with_wrapper_cte_absorption(
        self,
        module__client_with_build_v3,
    ):
        """Layer 2 (upstream FK passthrough) resolves a filter-only dim
        to the parent's local column. When that local column lives on the
        non-preserved side of a downstream RIGHT OUTER JOIN at the
        grain-group level, the wrapper-CTE absorber (#2) wraps the parent
        into ``<parent>_filtered`` so the filter applies before the
        RIGHT OUTER reaches the preserved side. Verifies that #1 (the
        per-CTE absorber) and #2 (the wrapper-CTE absorber) compose
        cleanly for filter-only refs.
        """
        client = module__client_with_build_v3

        await client.post(
            "/nodes/source/",
            json={
                "name": "v3.src_allocations_absorb",
                "description": "Customer marketing allocations",
                "columns": [
                    {"name": "customer_id", "type": "int"},
                    {"name": "campaign", "type": "string"},
                    {"name": "allocated_date", "type": "int"},
                ],
                "mode": "published",
                "catalog": "default",
                "schema_": "v3",
                "table": "allocations_absorb",
            },
        )
        await client.post(
            "/nodes/dimension/",
            json={
                "name": "v3.allocation_absorb",
                "description": "Allocations dimension",
                "query": (
                    "SELECT customer_id, campaign, allocated_date "
                    "FROM v3.src_allocations_absorb"
                ),
                "mode": "published",
                "primary_key": ["customer_id", "allocated_date"],
            },
        )

        # Wrapper transform — no direct dim link to v3.product. A filter
        # on v3.product must resolve via Layer 2 upstream FK passthrough.
        await client.post(
            "/nodes/transform/",
            json={
                "name": "v3.order_details_absorb_wrapper",
                "description": "wrapper for absorber interaction test",
                "query": (
                    "SELECT order_id, line_number, customer_id, order_date, "
                    "product_id, quantity, unit_price, "
                    "quantity * unit_price AS line_total "
                    "FROM v3.order_details"
                ),
                "mode": "published",
                "primary_key": ["order_id", "line_number"],
            },
        )
        # Wrapper RIGHT-OUTER-joins v3.allocation_absorb — wrapper becomes
        # the non-preserved side at the grain-group level. Filtering on
        # wrapper.product_id in the outer WHERE would defeat the RIGHT
        # OUTER unless wrapper-CTE absorption fires.
        await client.post(
            "/nodes/v3.order_details_absorb_wrapper/link/",
            json={
                "dimension_node": "v3.allocation_absorb",
                "join_type": "right",
                "join_on": (
                    "v3.order_details_absorb_wrapper.customer_id = "
                    "v3.allocation_absorb.customer_id AND "
                    "v3.order_details_absorb_wrapper.order_date = "
                    "v3.allocation_absorb.allocated_date"
                ),
            },
        )
        await client.post(
            "/nodes/metric/",
            json={
                "name": "v3.absorb_wrapper_total_revenue",
                "description": "Total revenue from absorb wrapper",
                "query": (
                    "SELECT SUM(line_total) FROM v3.order_details_absorb_wrapper"
                ),
                "mode": "published",
            },
        )

        response = await client.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.absorb_wrapper_total_revenue"],
                "dimensions": ["v3.allocation_absorb.campaign"],
                # Filter-only on a dim whose link lives only on an upstream
                # of the wrapper (v3.order_details has the link to v3.product).
                "filters": ["v3.product.product_id = 7"],
            },
        )

        assert response.status_code == 200, response.json()
        sql = get_first_grain_group(response.json())["sql"]
        # Surprising-but-correct: wrapper-CTE absorption (#2) does NOT
        # fire here. The filter never reaches outer WHERE — per-CTE
        # pushdown lands it in the wrapper CTE and propagates to
        # v3_order_details. With the wrapper already pre-filtered, the
        # RIGHT OUTER JOIN to v3.allocation_absorb operates on the
        # narrowed wrapper rows but still preserves all allocation rows
        # (RIGHT OUTER semantics). Allocations without a matching
        # filtered wrapper row appear with NULL metric.
        assert_sql_equal(
            sql,
            """
            WITH
            v3_allocation_absorb AS (
                SELECT customer_id, campaign, allocated_date
                FROM default.v3.allocations_absorb
            ),
            v3_order_details AS (
                SELECT
                    o.order_id,
                    oi.line_number,
                    o.customer_id,
                    o.order_date,
                    o.from_location_id,
                    o.to_location_id,
                    o.status,
                    oi.product_id,
                    oi.quantity,
                    oi.unit_price,
                    oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
                WHERE oi.product_id = 7
            ),
            v3_order_details_absorb_wrapper AS (
                SELECT
                    customer_id,
                    order_date,
                    product_id,
                    quantity * unit_price AS line_total
                FROM v3_order_details
                WHERE product_id = 7
            )
            SELECT
                t2.campaign,
                SUM(t1.line_total) line_total_sum_HASH
            FROM v3_order_details_absorb_wrapper t1
            RIGHT OUTER JOIN v3_allocation_absorb t2
                ON t1.customer_id = t2.customer_id
                AND t1.order_date = t2.allocated_date
            GROUP BY t2.campaign
            """,
            normalize_aliases=True,
        )

    @pytest.mark.asyncio
    async def test_unreachable_dim_raises_clear_error(
        self,
        module__client_with_build_v3,
    ):
        """A filter referencing a dim with no link anywhere in the
        parent's upstream lineage raises a ``Cannot find join path`` error
        rather than silently dropping the filter. Exercises the raise
        in ``resolve_dimensions``.
        """
        client = module__client_with_build_v3
        # Case A: filter-only dim, fully unreachable → enters the
        # filter-only branch but pushdown registers nothing → raises.
        response = await client.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
                "filters": ["v3.totally_unlinked_dim.some_col = 1"],
            },
        )
        assert response.status_code == 422
        assert response.json() == {
            "message": (
                "Cannot find join path from v3.order_details to "
                "dimension v3.totally_unlinked_dim. Please create a "
                "dimension link between these nodes."
            ),
            "errors": [],
            "warnings": [],
        }

        # Case B: GROUP BY on an unlinked dim → skips the filter-only
        # branch entirely (dim not in filter_dimensions) → raises.
        # Exercises the False-branch on `if dim in ctx.filter_dimensions`.
        response = await client.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": [
                    "v3.order_details.status",
                    "v3.totally_unlinked_dim.some_col",
                ],
            },
        )
        assert response.status_code == 422
        assert response.json() == {
            "message": (
                "Cannot find join path from v3.order_details to "
                "dimension v3.totally_unlinked_dim. Please create a "
                "dimension link between these nodes."
            ),
            "errors": [],
            "warnings": [],
        }


class TestWrapperCTEAbsorption:
    """When a LEFT/INNER-joined dim's filter would silently defeat a
    downstream RIGHT/FULL OUTER JOIN, the LEFT/INNER join + filter are
    absorbed into a ``<parent>_filtered`` CTE so the filter applies
    *before* the OUTER JOIN reaches the preserved side.

    Also covers the COALESCE projection/GROUP BY for full-skipped FKs
    whose dim is fully skipped but a co-joined sibling dim carries the
    same FK alignment.
    """

    @pytest.fixture
    async def setup_right_outer_dim(self, client_with_build_v3):
        """Add a ``v3.allocation`` dim RIGHT-OUTER-linked to
        ``v3.order_details``.  The RIGHT-OUTER link is the trigger for
        wrapper-CTE absorption when any LEFT-joined dim has a filter.
        """
        r = await client_with_build_v3.post(
            "/nodes/source/",
            json={
                "name": "v3.src_allocations",
                "description": "Customer marketing allocations",
                "columns": [
                    {"name": "customer_id", "type": "int"},
                    {"name": "campaign", "type": "string"},
                    {"name": "allocated_date", "type": "int"},
                ],
                "mode": "published",
                "catalog": "default",
                "schema_": "v3",
                "table": "allocations",
            },
        )
        assert r.status_code in (200, 201), r.text

        r = await client_with_build_v3.post(
            "/nodes/dimension/",
            json={
                "name": "v3.allocation",
                "description": "Allocations dimension",
                "query": (
                    "SELECT customer_id, campaign, allocated_date "
                    "FROM v3.src_allocations"
                ),
                "mode": "published",
                "primary_key": ["customer_id", "allocated_date"],
            },
        )
        assert r.status_code in (200, 201), r.text

        r = await client_with_build_v3.post(
            "/nodes/v3.order_details/link/",
            json={
                "dimension_node": "v3.allocation",
                "join_type": "right",
                "join_on": (
                    "v3.order_details.customer_id = v3.allocation.customer_id "
                    "AND v3.order_details.order_date = v3.allocation.allocated_date"
                ),
            },
        )
        assert r.status_code in (200, 201), r.text

    @pytest.mark.asyncio
    async def test_left_filter_before_right_join_absorbed_into_filtered_cte(
        self,
        client_with_build_v3,
        setup_right_outer_dim,
    ):
        """LEFT-joined dim with filter + RIGHT-joined dim → absorbed
        into ``v3_order_details_filtered`` CTE; outer WHERE empty."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": [
                    "v3.product.category",
                    "v3.allocation.campaign",
                ],
                "filters": ["v3.product.category = 'Electronics'"],
            },
        )
        assert response.status_code == 200, response.json()
        sql = get_first_grain_group(response.json())["sql"]
        assert_sql_equal(
            sql,
            """
            WITH
            v3_allocation AS (
                SELECT customer_id, campaign, allocated_date
                FROM default.v3.allocations
            ),
            v3_order_details AS (
                SELECT
                    o.customer_id,
                    o.order_date,
                    oi.product_id,
                    oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
                SELECT product_id, category
                FROM default.v3.products
                WHERE category = 'Electronics'
            ),
            v3_order_details_filtered AS (
                SELECT *, t2.category
                FROM v3_order_details t1
                LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
                WHERE t2.category = 'Electronics'
            )
            SELECT t1.category, t3.campaign,
                SUM(t1.line_total) line_total_sum_HASH
            FROM v3_order_details_filtered t1
            RIGHT OUTER JOIN v3_allocation t3
                ON t1.customer_id = t3.customer_id
                AND t1.order_date = t3.allocated_date
            GROUP BY t1.category, t3.campaign
            """,
            normalize_aliases=True,
        )

    @pytest.mark.asyncio
    async def test_no_right_join_no_absorption(
        self,
        client_with_build_v3,
    ):
        """No RIGHT/FULL OUTER → no wrapper CTE built."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "filters": ["v3.product.category = 'Electronics'"],
            },
        )
        assert response.status_code == 200, response.json()
        sql = get_first_grain_group(response.json())["sql"]
        assert_sql_equal(
            sql,
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
                WHERE category = 'Electronics'
            )
            SELECT t2.category, SUM(t1.line_total) line_total_sum_HASH
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            WHERE t2.category = 'Electronics'
            GROUP BY t2.category
            """,
            normalize_aliases=True,
        )

    @pytest.mark.asyncio
    async def test_unfiltered_left_join_not_absorbed(
        self,
        client_with_build_v3,
        setup_right_outer_dim,
    ):
        """LEFT-joined dim without a filter is left alone."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": [
                    "v3.product.category",
                    "v3.allocation.campaign",
                ],
            },
        )
        assert response.status_code == 200, response.json()
        sql = get_first_grain_group(response.json())["sql"]
        assert_sql_equal(
            sql,
            """
            WITH
            v3_allocation AS (
                SELECT customer_id, campaign, allocated_date
                FROM default.v3.allocations
            ),
            v3_order_details AS (
                SELECT
                    o.customer_id,
                    o.order_date,
                    oi.product_id,
                    oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
                SELECT product_id, category
                FROM default.v3.products
            )
            SELECT t2.category, t3.campaign, SUM(t1.line_total) line_total_sum_HASH
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            RIGHT OUTER JOIN v3_allocation t3
                ON t1.customer_id = t3.customer_id
                AND t1.order_date = t3.allocated_date
            GROUP BY t2.category, t3.campaign
            """,
            normalize_aliases=True,
        )

    @pytest.mark.asyncio
    async def test_coalesce_for_full_skipped_fk_with_co_joined_sibling(
        self,
        client_with_build_v3,
    ):
        """When ``v3.customer.customer_id`` is full-skipped to
        ``t1.customer_id`` and ``v3.customer`` is also joined for
        another column (``name``), the projection becomes
        ``COALESCE(t1.customer_id, t_customer.customer_id)`` and the
        GROUP BY mirrors it.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": [
                    "v3.customer.customer_id",
                    "v3.customer.name",
                ],
            },
        )
        assert response.status_code == 200, response.json()
        sql = get_first_grain_group(response.json())["sql"]
        assert_sql_equal(
            sql,
            """
            WITH
            v3_customer AS (
                SELECT customer_id, name FROM default.v3.customers
            ),
            v3_order_details AS (
                SELECT o.customer_id, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT
                COALESCE(t1.customer_id, t2.customer_id) AS customer_id,
                t2.name,
                SUM(t1.line_total) line_total_sum_HASH
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_customer t2 ON t1.customer_id = t2.customer_id
            GROUP BY COALESCE(t1.customer_id, t2.customer_id), t2.name
            """,
            normalize_aliases=True,
        )

    @pytest.fixture
    async def setup_full_outer_dim(self, client_with_build_v3):
        """Same as ``setup_right_outer_dim`` but FULL OUTER instead of
        RIGHT OUTER — exercises the FULL-OUTER-trigger branch of the
        absorber."""
        r = await client_with_build_v3.post(
            "/nodes/source/",
            json={
                "name": "v3.src_allocations_full",
                "columns": [
                    {"name": "customer_id", "type": "int"},
                    {"name": "campaign", "type": "string"},
                    {"name": "allocated_date", "type": "int"},
                ],
                "mode": "published",
                "catalog": "default",
                "schema_": "v3",
                "table": "allocations",
            },
        )
        assert r.status_code in (200, 201), r.text
        r = await client_with_build_v3.post(
            "/nodes/dimension/",
            json={
                "name": "v3.allocation_full",
                "query": (
                    "SELECT customer_id, campaign, allocated_date "
                    "FROM v3.src_allocations_full"
                ),
                "mode": "published",
                "primary_key": ["customer_id", "allocated_date"],
            },
        )
        assert r.status_code in (200, 201), r.text
        r = await client_with_build_v3.post(
            "/nodes/v3.order_details/link/",
            json={
                "dimension_node": "v3.allocation_full",
                "join_type": "full",
                "join_on": (
                    "v3.order_details.customer_id = v3.allocation_full.customer_id "
                    "AND v3.order_details.order_date = v3.allocation_full.allocated_date"
                ),
            },
        )
        assert r.status_code in (200, 201), r.text

    @pytest.mark.asyncio
    async def test_full_outer_also_triggers_absorption(
        self,
        client_with_build_v3,
        setup_full_outer_dim,
    ):
        """A FULL OUTER JOIN preserves both sides — same defeat risk
        as RIGHT OUTER, so absorption fires here too."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": [
                    "v3.product.category",
                    "v3.allocation_full.campaign",
                ],
                "filters": ["v3.product.category = 'Electronics'"],
            },
        )
        assert response.status_code == 200, response.json()
        sql = get_first_grain_group(response.json())["sql"]
        assert "v3_order_details_filtered" in sql
        assert "FULL OUTER JOIN v3_allocation_full" in sql

    @pytest.mark.asyncio
    async def test_compound_atom_not_absorbed(
        self,
        client_with_build_v3,
        setup_right_outer_dim,
    ):
        """A compound atom referencing both the parent and an absorbed
        dim alias stays in the outer WHERE — the absorber only picks
        single-alias atoms."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": [
                    "v3.product.category",
                    "v3.allocation.campaign",
                ],
                "filters": [
                    "v3.order_details.order_id > v3.product.product_id",
                ],
            },
        )
        assert response.status_code == 200, response.json()
        sql = get_first_grain_group(response.json())["sql"]
        # No wrapper CTE built — compound atom isn't a single-alias
        # filter the absorber knows how to handle.
        assert "v3_order_details_filtered" not in sql

    @pytest.mark.asyncio
    async def test_post_right_left_filter_also_absorbed(
        self,
        client_with_build_v3,
        setup_right_outer_dim,
    ):
        """A filter on a LEFT-joined dim that's positioned *after* the
        RIGHT/FULL OUTER also gets absorbed into the wrapper CTE.  The
        absorber consolidates every filtered fact-side LEFT/INNER join
        regardless of where it appears relative to the RIGHT JOIN, so
        the wrapper applies the filter before the OUTER JOIN reaches
        the preserved side — preventing the post-RIGHT LEFT JOIN from
        being defeated by the outer WHERE.
        """
        # Make v3.product a post-RIGHT LEFT-joined dim by listing it
        # AFTER the RIGHT-joined allocation dim.  DJ joins dims in
        # request order, so this ordering produces:
        #   member_download t1
        #   RIGHT JOIN allocation t_a
        #   LEFT JOIN product t_p           <- positioned AFTER the RIGHT
        # With the broadened absorber, the filter on v3.product.category
        # is still absorbed into v3_order_details_filtered.
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": [
                    "v3.allocation.campaign",
                    "v3.product.category",
                ],
                "filters": ["v3.product.category = 'Electronics'"],
            },
        )
        assert response.status_code == 200, response.json()
        sql = get_first_grain_group(response.json())["sql"]
        # The filtered product LEFT JOIN should be absorbed even though
        # it sits after the RIGHT JOIN to allocation in request order.
        assert "v3_order_details_filtered" in sql
        # No outer WHERE on the absorbed dim alias — it's now inside
        # the wrapper.  References to the dim's column should resolve
        # through the wrapper's alias (t1), not a separate dim alias.
        assert "WHERE t2.category" not in sql

    @pytest.mark.asyncio
    async def test_pre_and_post_right_filters_both_absorbed(
        self,
        client_with_build_v3,
        setup_right_outer_dim,
    ):
        """Filters on two LEFT-joined dims — one before and one after
        the RIGHT JOIN — both get absorbed into a single wrapper CTE.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": [
                    "v3.product.category",  # pre-RIGHT LEFT
                    "v3.allocation.campaign",  # RIGHT
                    "v3.customer.name",  # post-RIGHT LEFT
                ],
                "filters": [
                    "v3.product.category = 'Electronics'",
                    "v3.customer.name = 'Alice'",
                ],
            },
        )
        assert response.status_code == 200, response.json()
        sql = get_first_grain_group(response.json())["sql"]
        # Both filters are absorbed into one wrapper CTE.
        assert "v3_order_details_filtered" in sql
        # Both filter predicates should be applied inside the wrapper.
        wrapper_section = sql.split("v3_order_details_filtered AS (")[1].split(
            ")",
            1,
        )[0]
        assert "category = 'Electronics'" in wrapper_section
        assert "name = 'Alice'" in wrapper_section

    @pytest.mark.asyncio
    async def test_preserved_side_filter_remains_in_outer_where(
        self,
        client_with_build_v3,
        setup_right_outer_dim,
    ):
        """A filter on the RIGHT/FULL OUTER preserved side stays in the
        outer WHERE (not absorbed) — filtering a preserved side just
        narrows it without defeating anything.  Together with a
        LEFT-joined-dim filter (absorbed), this verifies the absorber
        leaves preserved-side single-dim atoms alone instead of
        silently dropping them.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": [
                    "v3.product.category",
                    "v3.allocation.campaign",
                ],
                "filters": [
                    "v3.product.category = 'Electronics'",
                    "v3.allocation.campaign = 'spring'",
                ],
            },
        )
        assert response.status_code == 200, response.json()
        sql = get_first_grain_group(response.json())["sql"]
        # Product filter absorbed.
        assert "v3_order_details_filtered" in sql
        # Allocation (preserved RIGHT side) filter STAYS in outer WHERE
        # — it wasn't silently dropped.
        outer_section = sql.split("v3_order_details_filtered AS (")[1]
        outer_section = outer_section.split(")")[-1]  # after the last )
        assert "campaign" in outer_section

    @pytest.mark.asyncio
    async def test_compound_atom_alongside_absorbed_dim(
        self,
        client_with_build_v3,
        setup_right_outer_dim,
    ):
        """A compound (cross-namespace) atom plus a single-dim atom on
        a LEFT-joined dim: the single-dim atom is absorbed into the
        wrapper CTE, and the compound atom rides through ``other_atoms``
        — exercising the iteration over ``other_atoms`` in both the
        column-collection and alias-rewrite loops.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": [
                    "v3.product.category",
                    "v3.allocation.campaign",
                ],
                "filters": [
                    "v3.product.category = 'Electronics'",
                    # Cross-namespace compound: absorbed dim (product) vs
                    # RIGHT-preserved dim (allocation).  Single non-AND
                    # atom, so it lands in ``other_atoms`` and exercises
                    # both the column-collection and alias-rewrite loops
                    # over ``other_atoms``.
                    "v3.product.category != v3.allocation.campaign",
                ],
            },
        )
        assert response.status_code == 200, response.json()
        sql = get_first_grain_group(response.json())["sql"]
        assert_sql_equal(
            sql,
            """
            WITH
            v3_allocation AS (
                SELECT customer_id, campaign, allocated_date
                FROM default.v3.allocations
            ),
            v3_order_details AS (
                SELECT
                    o.customer_id,
                    o.order_date,
                    oi.product_id,
                    oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
                SELECT product_id, category
                FROM default.v3.products
                WHERE category = 'Electronics'
            ),
            v3_order_details_filtered AS (
                SELECT *, t2.category
                FROM v3_order_details t1
                LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
                WHERE t2.category = 'Electronics'
            )
            SELECT t1.category, t3.campaign,
                SUM(t1.line_total) line_total_sum_HASH
            FROM v3_order_details_filtered t1
            RIGHT OUTER JOIN v3_allocation t3
                ON t1.customer_id = t3.customer_id
                AND t1.order_date = t3.allocated_date
            WHERE t1.category != t3.campaign
            GROUP BY t1.category, t3.campaign
            """,
            normalize_aliases=True,
        )


class TestMeasuresMaterializedParentFilter:
    """Filters on a materialized parent's local columns are routed
    through ``inject_filter_into_select`` instead of being dropped.

    With materialization, no parent CTE is built, so the safety
    backstop in ``_apply_outer_where_atoms`` must keep the filter
    visible at the outer level.
    """

    @pytest.mark.asyncio
    async def test_local_filter_on_materialized_parent_kept_in_outer(
        self,
        client_with_build_v3,
    ):
        """Add availability to ``v3.order_details`` so it's materialized,
        then request a metric on it with a local-column filter.  The
        filter must survive into the outer SQL (via the safety
        backstop), even though no parent CTE exists to push it into.
        """
        response = await client_with_build_v3.post(
            "/data/v3.order_details/availability/",
            json={
                "catalog": "analytics",
                "schema_": "warehouse",
                "table": "order_details_materialized_pa",
                "valid_through_ts": 9999999999,
            },
        )
        assert response.status_code == 200, response.json()

        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
                "filters": ["v3.order_details.status = 'completed'"],
            },
        )
        assert response.status_code == 200, response.json()
        sql = get_first_grain_group(response.json())["sql"]
        assert_sql_equal(
            sql,
            """
            SELECT t1.status, SUM(t1.line_total) line_total_sum_HASH
            FROM analytics.warehouse.order_details_materialized_pa t1
            WHERE t1.status = 'completed'
            GROUP BY t1.status
            """,
            normalize_aliases=True,
        )


class TestMultiHopInnerDefeatsLeft:
    """Multi-hop dim chains can produce ``LEFT JOIN dim1 INNER JOIN dim2 ON
    dim1.X = dim2.Y`` shapes when ``dim1`` is reached via a LEFT link from
    the fact and ``dim1`` has its own INNER link to ``dim2``.

    The downstream INNER's predicate references ``dim1`` (a non-preserved
    alias).  For fact rows where ``dim1`` doesn't match, ``dim1.X`` is NULL
    → the INNER predicate is NULL → the row is dropped.  This silently
    converts the upstream LEFT JOIN into an effective INNER JOIN.

    This is a real hazard in multi-hop dim graphs (common pattern: a
    fact LEFT-links to a "main" dim, that dim INNER-links to a lookup
    dim, and a user requests a column from the lookup).  The fix
    requires the absorber or classifier to recognize that an INNER's
    cross-dim predicate undoes upstream preservation.
    """

    @pytest.fixture
    async def setup_multi_hop_chain(self, client_with_build_v3):
        """Build a chain: ``v3.order_details`` LEFT-linked to ``v3.product``,
        ``v3.product`` INNER-linked to a new ``v3.product_lookup`` dim.
        Requesting a column from ``v3.product_lookup`` forces the chain.
        """
        # New lookup dim source + dim
        r = await client_with_build_v3.post(
            "/nodes/source/",
            json={
                "name": "v3.src_product_lookup",
                "columns": [
                    {"name": "category", "type": "string"},
                    {"name": "category_group", "type": "string"},
                ],
                "mode": "published",
                "catalog": "default",
                "schema_": "v3",
                "table": "product_lookup",
            },
        )
        assert r.status_code in (200, 201), r.text

        r = await client_with_build_v3.post(
            "/nodes/dimension/",
            json={
                "name": "v3.product_lookup",
                "description": "Product category lookup",
                "query": "SELECT category, category_group FROM v3.src_product_lookup",
                "mode": "published",
                "primary_key": ["category"],
            },
        )
        assert r.status_code in (200, 201), r.text

        # v3.product INNER-links to v3.product_lookup on its `category`
        # column.  The link's join_on is cross-dim:
        #   v3.product.category = v3.product_lookup.category
        # so when chained downstream of the LEFT link from order_details,
        # the INNER's predicate references the LEFT-joined dim's column.
        r = await client_with_build_v3.post(
            "/nodes/v3.product/link/",
            json={
                "dimension_node": "v3.product_lookup",
                "join_type": "inner",
                "join_on": ("v3.product.category = v3.product_lookup.category"),
            },
        )
        assert r.status_code in (200, 201), r.text

    @pytest.mark.asyncio
    async def test_inner_link_downstream_of_left_defeats_left(
        self,
        client_with_build_v3,
        setup_multi_hop_chain,
    ):
        """Demonstrates the bug: chain ``order_details LEFT product INNER
        product_lookup`` generates SQL where the INNER's predicate
        references the LEFT-joined ``product`` alias, silently dropping
        fact rows that have no matching product.

        Asserts the current (buggy) shape via ``assert_sql_equal``.
        Marked ``xfail`` until the classifier propagates non-preserved
        through cross-dim INNER predicates (or the absorber broadens
        scope to consolidate multi-hop chains).
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": [
                    "v3.product_lookup.category_group",
                ],
            },
        )
        assert response.status_code == 200, response.json()
        sql = get_first_grain_group(response.json())["sql"]
        # Current behavior (the documented bug): the chain emits
        # ``... LEFT OUTER JOIN v3_product t2 ... INNER JOIN v3_product_lookup t3
        # ON t2.category = t3.category``.  The INNER's predicate
        # references the LEFT-joined ``t2`` alias — for fact rows where
        # ``t2`` doesn't match, ``t2.category`` is NULL and the INNER
        # silently drops them, defeating the upstream LEFT.
        #
        # A correctness fix would either flip the downstream INNER to
        # LEFT or wrap the LEFT+INNER pair so the NULL fact rows from
        # the LEFT JOIN survive.  Until then this asserts the buggy
        # shape so the test fails the day the fix lands and we can
        # update the expected output.
        assert_sql_equal(
            sql,
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
            v3_product_lookup AS (
                SELECT category, category_group
                FROM default.v3.product_lookup
            )
            SELECT t3.category_group, SUM(t1.line_total) line_total_sum_HASH
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            INNER JOIN v3_product_lookup t3 ON t2.category = t3.category
            GROUP BY t3.category_group
            """,
            normalize_aliases=True,
        )
