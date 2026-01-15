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
            name
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
            t5.year year_registration,
            t7.country country_home,
            SUM(t1.line_total) line_total_sum_e1f61696,
            SUM(t1.quantity) quantity_sum_06b64d2e,
            hll_sketch_agg(t1.customer_id) customer_id_hll_23002251
        FROM v3_order_details t1
        LEFT OUTER JOIN v3_customer t2 ON t1.customer_id = t2.customer_id
        LEFT OUTER JOIN v3_date t3 ON t1.order_date = t3.date_id
        LEFT OUTER JOIN v3_customer t4 ON t1.customer_id = t4.customer_id
        LEFT OUTER JOIN v3_date t5 ON t4.registration_date = t5.date_id
        LEFT OUTER JOIN v3_customer t6 ON t1.customer_id = t6.customer_id
        LEFT OUTER JOIN v3_location t7 ON t6.location_id = t7.location_id
        GROUP BY  t1.status, t2.name, t3.month, t5.year, t7.country
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
                SELECT customer_id
                FROM v3.src_customers
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
                SELECT customer_id
                FROM v3.src_customers
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
                SELECT customer_id
                FROM v3.src_customers
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
    async def test_simple_filter_on_local_column(self, client_with_build_v3):
        """Test a simple filter on a local (fact) column."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
                "filters": ["status = 'completed'"],
            },
        )

        assert response.status_code == 200, response.json()
        data = response.json()
        sql = data["grain_groups"][0]["sql"]

        # Should have WHERE clause with the filter
        assert "WHERE" in sql
        assert "status" in sql
        assert "'completed'" in sql

    async def test_filter_on_dimension_column(self, client_with_build_v3):
        """Test a filter on a joined dimension column."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "filters": ["v3.product.category = 'Electronics'"],
            },
        )

        assert response.status_code == 200, response.json()
        data = response.json()
        sql = data["grain_groups"][0]["sql"]

        # Should have WHERE clause referencing the dimension column
        assert "WHERE" in sql
        assert "category" in sql
        assert "'Electronics'" in sql

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
        data = response.json()
        sql = data["grain_groups"][0]["sql"]

        # Should have WHERE clause with both filters combined with AND
        assert "WHERE" in sql
        assert "AND" in sql
        assert "'completed'" in sql
        assert "'Electronics'" in sql

    async def test_filter_with_comparison_operators(self, client_with_build_v3):
        """Test filters with various comparison operators."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.date.year[order]"],
                "filters": ["v3.date.year[order] >= 2024"],
            },
        )

        assert response.status_code == 200, response.json()
        data = response.json()
        sql = data["grain_groups"][0]["sql"]

        # Should have filter with >= operator
        assert "WHERE" in sql
        assert ">=" in sql
        assert "2024" in sql

    async def test_filter_with_in_operator(self, client_with_build_v3):
        """Test filter with IN operator."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
                "filters": ["status IN ('completed', 'pending')"],
            },
        )

        assert response.status_code == 200, response.json()
        data = response.json()
        sql = data["grain_groups"][0]["sql"]

        # Should have filter with IN operator
        assert "WHERE" in sql
        assert "IN" in sql
        assert "'completed'" in sql
        assert "'pending'" in sql


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
        """Set up temporal partition on order_date column."""
        # Ensure the temporal partition is configured (may already exist in template)
        response = await client_with_build_v3.post(
            "/nodes/v3.order_details/columns/order_date/partition",
            json={
                "type_": "temporal",
                "granularity": "day",
                "format": "yyyyMMdd",
            },
        )
        assert response.status_code in (200, 201, 409)  # 409 = already exists

    @pytest.mark.asyncio
    async def test_temporal_filter_exact_partition(
        self,
        session,
        client_with_build_v3,
        setup_temporal_partition,
    ):
        """
        Test that include_temporal_filters=True adds exact partition filter.

        Uses v3.order_details which has order_date configured as a temporal partition.
        The filter should be: order_date = CAST(DATE_FORMAT(DJ_LOGICAL_TIMESTAMP(), 'yyyyMMdd') AS INT)
        """
        result = await build_measures_sql(
            session=session,
            metrics=["v3.total_revenue"],
            dimensions=["v3.order_details.status"],
            include_temporal_filters=True,
        )

        # Should have DJ_LOGICAL_TIMESTAMP in the SQL for exact partition match
        assert_sql_equal(
            result.grain_groups[0].sql,
            """
            WITH v3_order_details AS (
                SELECT o.order_date, o.status, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t1.status, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            WHERE t1.order_date = CAST(DATE_FORMAT(CAST(DJ_LOGICAL_TIMESTAMP() AS TIMESTAMP), 'yyyyMMdd') AS INT)
            GROUP BY t1.status
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

        The filter should be:
        order_date BETWEEN CAST(DATE_FORMAT(DJ_LOGICAL_TIMESTAMP() - INTERVAL '3' DAY, 'yyyyMMdd') AS INT)
                    AND CAST(DATE_FORMAT(DJ_LOGICAL_TIMESTAMP(), 'yyyyMMdd') AS INT)
        """
        result = await build_measures_sql(
            session=session,
            metrics=["v3.total_revenue"],
            dimensions=["v3.order_details.status"],
            include_temporal_filters=True,
            lookback_window="3 DAY",
        )

        # Should have BETWEEN for lookback window
        assert_sql_equal(
            result.grain_groups[0].sql,
            """
            WITH v3_order_details AS (
                SELECT o.order_date, o.status, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t1.status, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            WHERE t1.order_date BETWEEN CAST(DATE_FORMAT(CAST(DJ_LOGICAL_TIMESTAMP() AS TIMESTAMP) - INTERVAL '3' DAY, 'yyyyMMdd') AS INT)
                                    AND CAST(DATE_FORMAT(CAST(DJ_LOGICAL_TIMESTAMP() AS TIMESTAMP), 'yyyyMMdd') AS INT)
            GROUP BY t1.status
            """,
        )

    @pytest.mark.asyncio
    async def test_no_temporal_filter_when_disabled(
        self,
        session,
        client_with_build_v3,
    ):
        """
        Test that temporal filters are NOT added when include_temporal_filters=False.
        """
        result = await build_measures_sql(
            session=session,
            metrics=["v3.total_revenue"],
            dimensions=["v3.order_details.status"],
            include_temporal_filters=False,  # Default, but explicit
        )

        # No temporal filter should be present
        assert_sql_equal(
            result.grain_groups[0].sql,
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

    @pytest.mark.asyncio
    async def test_temporal_filter_with_user_filter(
        self,
        session,
        client_with_build_v3,
        setup_temporal_partition,
    ):
        """
        Test that temporal filter is combined with user filters.

        The filter should be:
        (order_date = CAST(DATE_FORMAT(DJ_LOGICAL_TIMESTAMP(), 'yyyyMMdd') AS INT)) AND (status = 'active')
        """
        result = await build_measures_sql(
            session=session,
            metrics=["v3.total_revenue"],
            dimensions=["v3.order_details.status"],
            filters=["status = 'active'"],
            include_temporal_filters=True,
        )

        # Should have AND between temporal and user filters
        assert_sql_equal(
            result.grain_groups[0].sql,
            """
            WITH v3_order_details AS (
                SELECT o.order_date, o.status, oi.quantity * oi.unit_price AS line_total
                FROM default.v3.orders o
                JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t1.status, SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            WHERE t1.status = 'active' AND t1.order_date = CAST(DATE_FORMAT(CAST(DJ_LOGICAL_TIMESTAMP() AS TIMESTAMP), 'yyyyMMdd') AS INT)
            GROUP BY t1.status
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
                "dimensions": ["v3.date_dim.date_id"],
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
            SELECT  oi.quantity * oi.unit_price AS line_total
            FROM default.v3.orders o JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_page_views_enriched AS (
            SELECT  view_id
            FROM default.v3.page_views
            ),
            gg1 AS (
            SELECT  t1.date_id,
                SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            GROUP BY  t1.date_id
            ),
            gg2 AS (
            SELECT  t1.date_id,
                COUNT(t1.view_id) view_id_count_f41e2db4
            FROM v3_page_views_enriched t1
            GROUP BY  t1.date_id
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
            "default.dj_preaggs.v3_order_details_preagg_b18e32ec",
        ]

        # Extract the preagg table name for SQL comparison
        # The SQL should read from the pre-agg table with re-aggregation
        assert_sql_equal(
            data["sql"],
            """
            SELECT status, SUM(line_total_sum_e1f61696) line_total_sum_e1f61696
            FROM default.dj_preaggs.v3_order_details_preagg_b18e32ec
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
        # The default settings are:
        # preagg_catalog = "default"
        # preagg_schema = "dj_preaggs"

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

        # Source tables should include the default catalog.schema prefix
        assert len(data["source_tables"]) >= 1
        assert data["source_tables"] == [
            "default.dj_preaggs.v3_order_details_preagg_b18e32ec",
        ]

        # Verify the SQL also references this table
        assert_sql_equal(
            data["sql"],
            """
            SELECT status, SUM(line_total_sum_e1f61696) line_total_sum_e1f61696
            FROM default.dj_preaggs.v3_order_details_preagg_b18e32ec
            GROUP BY status
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
