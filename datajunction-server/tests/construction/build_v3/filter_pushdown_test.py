"""
Tests for filter pushdown into CTEs.

Verifies that user-supplied dimension filters are pushed down into the
appropriate CTE WHERE clauses, not just applied on the outer query.
"""

import pytest

from tests.construction.build_v3 import assert_sql_equal, get_first_grain_group


class TestFilterPushdownToParentCTE:
    """Filters on dimensions linked via FK on the parent node push into the parent CTE."""

    @pytest.mark.asyncio
    async def test_date_filter_pushed_to_parent_cte(
        self,
        client_with_build_v3,
    ):
        """
        Filter on v3.date.date_id[order] should push order_date filter into
        the v3_order_details CTE (parent node) via the FK dimension link.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "filters": ["v3.date.date_id[order] >= 20240101"],
            },
        )
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            get_first_grain_group(response.json())["sql"],
            """
            WITH
            v3_order_details AS (
              SELECT o.order_date,
                oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
              WHERE o.order_date >= 20240101
            ),
            v3_product AS (
              SELECT product_id, category
              FROM default.v3.products
            )
            SELECT t2.category,
              SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            GROUP BY t2.category
            """,
        )


class TestFilterPushdownToDimensionCTE:
    """Filters on dimension columns push into the dimension node's CTE."""

    @pytest.mark.asyncio
    async def test_product_filter_pushed_to_dimension_cte(
        self,
        client_with_build_v3,
    ):
        """
        Filter on v3.product.category should push into the v3_product CTE.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "filters": ["v3.product.category = 'Electronics'"],
            },
        )
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            get_first_grain_group(response.json())["sql"],
            """
            WITH
            v3_order_details AS (
              SELECT oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
              SELECT product_id, category
              FROM default.v3.products
              WHERE category = 'Electronics'
            )
            SELECT t2.category,
              SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            WHERE t2.category = 'Electronics'
            GROUP BY t2.category
            """,
        )


class TestFilterPushdownMultiple:
    """Multiple filters push into their respective CTEs."""

    @pytest.mark.asyncio
    async def test_multiple_filters_pushed_to_different_ctes(
        self,
        client_with_build_v3,
    ):
        """
        Date filter → parent CTE (via FK), product filter → product dim CTE.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "filters": [
                    "v3.date.date_id[order] >= 20240101",
                    "v3.product.category = 'Electronics'",
                ],
            },
        )
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            get_first_grain_group(response.json())["sql"],
            """
            WITH
            v3_order_details AS (
              SELECT o.order_date,
                oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
              WHERE o.order_date >= 20240101
            ),
            v3_product AS (
              SELECT product_id, category
              FROM default.v3.products
              WHERE category = 'Electronics'
            )
            SELECT t2.category,
              SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            WHERE t2.category = 'Electronics'
            GROUP BY t2.category
            """,
        )

    @pytest.mark.asyncio
    async def test_two_filters_on_same_parent_cte_compose_as_and(
        self,
        client_with_build_v3,
    ):
        """Two user filters on distinct columns of the same parent compose
        as AND in both the CTE's WHERE and the outer WHERE — no duplicates,
        no dropped predicate.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "filters": [
                    "v3.date.date_id[order] >= 20240101",
                    "v3.order_details.status = 'completed'",
                ],
            },
        )
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            get_first_grain_group(response.json())["sql"],
            """
            WITH
            v3_order_details AS (
              SELECT o.order_date,
                o.status,
                oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
              WHERE o.order_date >= 20240101 AND o.status = 'completed'
            ),
            v3_product AS (
              SELECT product_id, category
              FROM default.v3.products
            )
            SELECT t2.category,
              SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            GROUP BY t2.category
            """,
        )


class TestFilterPushdownMultiRef:
    """A single filter predicate can reference multiple dim refs (OR-combined)."""

    @pytest.mark.asyncio
    async def test_or_predicate_both_refs_in_same_cte_pushed_down(
        self,
        client_with_build_v3,
    ):
        """OR-combined refs that both resolve to the same CTE push down as
        one predicate.  Both refs are fully qualified — bare column refs in
        filters are ambiguous across multi-parent builds and aren't a
        supported input shape.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "filters": [
                    "v3.date.date_id[order] >= 20240101 "
                    "OR v3.order_details.status = 'completed'",
                ],
            },
        )
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            get_first_grain_group(response.json())["sql"],
            """
            WITH
            v3_order_details AS (
              SELECT o.order_date,
                o.status,
                oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
              WHERE o.order_date >= 20240101 OR o.status = 'completed'
            ),
            v3_product AS (
              SELECT product_id, category
              FROM default.v3.products
            )
            SELECT t2.category,
              SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            GROUP BY t2.category
            """,
        )

    @pytest.mark.asyncio
    async def test_or_predicate_crossing_ctes_stays_on_outer_query(
        self,
        client_with_build_v3,
    ):
        """An OR that mixes columns from different CTEs must not push into
        either CTE — a partial rewrite would reference an unresolved name
        inside a CTE.  The whole predicate stays on the outer WHERE.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "filters": [
                    "v3.product.category = 'Electronics' "
                    "OR v3.date.date_id[order] >= 20240101",
                ],
            },
        )
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            get_first_grain_group(response.json())["sql"],
            """
            WITH
            v3_order_details AS (
              SELECT o.order_date,
                oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
              SELECT product_id, category
              FROM default.v3.products
            )
            SELECT t2.category,
              SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            WHERE t2.category = 'Electronics' OR t1.order_date >= 20240101
            GROUP BY t2.category
            """,
        )


class TestFilterPushdownEdgeCases:
    """Edge cases for filter pushdown."""

    @pytest.mark.asyncio
    async def test_no_filters_no_pushdown(
        self,
        client_with_build_v3,
    ):
        """No filters → CTEs should not have injected WHERE clauses."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
            },
        )
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            get_first_grain_group(response.json())["sql"],
            """
            WITH
            v3_order_details AS (
              SELECT oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
              SELECT product_id, category
              FROM default.v3.products
            )
            SELECT t2.category,
              SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            GROUP BY t2.category
            """,
        )

    @pytest.mark.asyncio
    async def test_filter_on_local_dimension(
        self,
        client_with_build_v3,
    ):
        """Filter on a local dimension (column directly on parent) pushes to parent CTE."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
                "filters": ["v3.order_details.status = 'completed'"],
            },
        )
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            get_first_grain_group(response.json())["sql"],
            """
            WITH
            v3_order_details AS (
              SELECT o.status,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
              WHERE o.status = 'completed'
            )
            SELECT t1.status,
              SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            GROUP BY t1.status
            """,
        )


class TestFilterPushdownOperators:
    """Operator coverage: each predicate shape should push into the right CTE."""

    @pytest.mark.asyncio
    async def test_in_list_filter_pushed_to_parent_cte(
        self,
        client_with_build_v3,
    ):
        """IN (...) on a parent FK column pushes into the parent CTE."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "filters": ["v3.date.date_id[order] IN (20240101, 20240102)"],
            },
        )
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            get_first_grain_group(response.json())["sql"],
            """
            WITH
            v3_order_details AS (
              SELECT o.order_date,
                oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
              WHERE o.order_date IN (20240101, 20240102)
            ),
            v3_product AS (
              SELECT product_id, category
              FROM default.v3.products
            )
            SELECT t2.category,
              SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            GROUP BY t2.category
            """,
        )

    @pytest.mark.asyncio
    async def test_not_in_list_filter_pushed_to_dim_cte(
        self,
        client_with_build_v3,
    ):
        """NOT IN (...) on a dim column pushes into the dim CTE."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "filters": [
                    "v3.product.category NOT IN ('Electronics', 'Clothing')",
                ],
            },
        )
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            get_first_grain_group(response.json())["sql"],
            """
            WITH
            v3_order_details AS (
              SELECT oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
              SELECT product_id, category
              FROM default.v3.products
              WHERE category NOT IN ('Electronics', 'Clothing')
            )
            SELECT t2.category,
              SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            WHERE t2.category NOT IN ('Electronics', 'Clothing')
            GROUP BY t2.category
            """,
        )

    @pytest.mark.asyncio
    async def test_is_null_filter_pushed_to_dim_cte(
        self,
        client_with_build_v3,
    ):
        """IS NULL pushes into the dim CTE."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "filters": ["v3.product.subcategory IS NULL"],
            },
        )
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            get_first_grain_group(response.json())["sql"],
            """
            WITH
            v3_order_details AS (
              SELECT oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
              SELECT product_id, category, subcategory
              FROM default.v3.products
              WHERE subcategory IS NULL
            )
            SELECT t2.category,
              SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            WHERE t2.subcategory IS NULL
            GROUP BY t2.category
            """,
        )

    @pytest.mark.asyncio
    async def test_is_not_null_filter_pushed_to_dim_cte(
        self,
        client_with_build_v3,
    ):
        """IS NOT NULL pushes into the dim CTE."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "filters": ["v3.product.subcategory IS NOT NULL"],
            },
        )
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            get_first_grain_group(response.json())["sql"],
            """
            WITH
            v3_order_details AS (
              SELECT oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
              SELECT product_id, category, subcategory
              FROM default.v3.products
              WHERE subcategory IS NOT NULL
            )
            SELECT t2.category,
              SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            WHERE t2.subcategory IS NOT NULL
            GROUP BY t2.category
            """,
        )

    @pytest.mark.asyncio
    async def test_between_filter_pushed_to_parent_cte(
        self,
        client_with_build_v3,
    ):
        """BETWEEN pushes into the parent CTE via the FK column."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "filters": [
                    "v3.date.date_id[order] BETWEEN 20240101 AND 20240131",
                ],
            },
        )
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            get_first_grain_group(response.json())["sql"],
            """
            WITH
            v3_order_details AS (
              SELECT o.order_date,
                oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
              WHERE o.order_date BETWEEN 20240101 AND 20240131
            ),
            v3_product AS (
              SELECT product_id, category
              FROM default.v3.products
            )
            SELECT t2.category,
              SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            GROUP BY t2.category
            """,
        )

    @pytest.mark.asyncio
    async def test_like_filter_pushed_to_dim_cte(
        self,
        client_with_build_v3,
    ):
        """LIKE pushes into the dim CTE."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "filters": ["v3.product.category LIKE 'Elect%'"],
            },
        )
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            get_first_grain_group(response.json())["sql"],
            """
            WITH
            v3_order_details AS (
              SELECT oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
              SELECT product_id, category
              FROM default.v3.products
              WHERE category LIKE 'Elect%'
            )
            SELECT t2.category,
              SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            WHERE t2.category LIKE 'Elect%'
            GROUP BY t2.category
            """,
        )

    @pytest.mark.asyncio
    async def test_not_equal_filter_pushed_to_dim_cte(
        self,
        client_with_build_v3,
    ):
        """<> pushes into the dim CTE."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "filters": ["v3.product.category <> 'Electronics'"],
            },
        )
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            get_first_grain_group(response.json())["sql"],
            """
            WITH
            v3_order_details AS (
              SELECT oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
              SELECT product_id, category
              FROM default.v3.products
              WHERE category <> 'Electronics'
            )
            SELECT t2.category,
              SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            WHERE t2.category <> 'Electronics'
            GROUP BY t2.category
            """,
        )

    @pytest.mark.asyncio
    async def test_not_wrapped_predicate_pushed_to_dim_cte(
        self,
        client_with_build_v3,
    ):
        """NOT (...) wrapping a single-CTE predicate pushes that CTE."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "filters": ["NOT (v3.product.category = 'Electronics')"],
            },
        )
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            get_first_grain_group(response.json())["sql"],
            """
            WITH
            v3_order_details AS (
              SELECT oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
              SELECT product_id, category
              FROM default.v3.products
              WHERE NOT (category = 'Electronics')
            )
            SELECT t2.category,
              SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            WHERE NOT (t2.category = 'Electronics')
            GROUP BY t2.category
            """,
        )

    @pytest.mark.asyncio
    async def test_function_wrapped_filter_pushed_to_dim_cte(
        self,
        client_with_build_v3,
    ):
        """A predicate that wraps the column in a function still pushes."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "filters": ["LOWER(v3.product.category) = 'electronics'"],
            },
        )
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            get_first_grain_group(response.json())["sql"],
            """
            WITH
            v3_order_details AS (
              SELECT oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            ),
            v3_product AS (
              SELECT product_id, category
              FROM default.v3.products
              WHERE LOWER(category) = 'electronics'
            )
            SELECT t2.category,
              SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            WHERE LOWER(t2.category) = 'electronics'
            GROUP BY t2.category
            """,
        )


class TestFilterPushdownMultiRole:
    """Multi-role dim filter pushdown — each role's filter targets a distinct CTE / join."""

    @pytest.mark.asyncio
    async def test_two_roles_of_same_dim_each_filter_pushed_independently(
        self,
        client_with_build_v3,
    ):
        """Filter on date_id[order] pushes via the order FK; an
        independent filter on a different role's PK pushes via that
        role's FK.  Each role resolves to its own column on the parent.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "filters": [
                    "v3.date.date_id[order] >= 20240101",
                    "v3.location.location_id[from] = 5",
                ],
            },
        )
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            get_first_grain_group(response.json())["sql"],
            """
            WITH
            v3_order_details AS (
              SELECT o.order_date,
                o.from_location_id,
                oi.product_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
              WHERE o.order_date >= 20240101 AND o.from_location_id = 5
            ),
            v3_product AS (
              SELECT product_id, category
              FROM default.v3.products
            )
            SELECT t2.category,
              SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
            GROUP BY t2.category
            """,
        )


class TestFilterPushdownMultiHop:
    """Filter on a dim two hops from the parent (parent → customer → location)."""

    @pytest.mark.asyncio
    async def test_filter_on_multi_hop_dim_pushes_into_terminal_dim_cte(
        self,
        client_with_build_v3,
    ):
        """A non-PK filter on a multi-hop dim pushes into that dim's CTE."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.location.country[customer->home]"],
                "filters": [
                    "v3.location.country[customer->home] = 'US'",
                ],
            },
        )
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            get_first_grain_group(response.json())["sql"],
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
              SELECT o.customer_id,
                oi.quantity * oi.unit_price AS line_total
              FROM default.v3.orders o
              JOIN default.v3.order_items oi ON o.order_id = oi.order_id
            )
            SELECT t3.country country_home,
              SUM(t1.line_total) line_total_sum_e1f61696
            FROM v3_order_details t1
            LEFT OUTER JOIN v3_customer t2 ON t1.customer_id = t2.customer_id
            LEFT OUTER JOIN v3_location t3 ON t2.location_id = t3.location_id
            WHERE t3.country = 'US'
            GROUP BY t3.country
            """,
        )


class TestDimCteFilterOnNonFkColumn:
    """Regression: when the parent's FK column shares a name with a
    non-PK column on the dim, a filter on the dim's PK column must
    resolve to the dim's own column inside the dim CTE — not to the
    parent's FK column name, which would silently collide with the
    dim's same-named column and filter against the wrong data.
    """

    @pytest.mark.asyncio
    async def test_filter_on_non_fk_dim_column_resolves_correctly(
        self,
        client_with_build_v3,
    ):
        client = client_with_build_v3
        resp = await client.post(
            "/nodes/source/",
            json={
                "name": "v3.src_nfk_events",
                "columns": [
                    {"name": "account_id", "type": "int"},
                    {"name": "is_fraud", "type": "int"},
                    {"name": "amount", "type": "double"},
                ],
                "mode": "published",
                "catalog": "default",
                "schema_": "v3",
                "table": "nfk_events",
            },
        )
        assert resp.status_code in (200, 201), resp.json()
        resp = await client.post(
            "/nodes/transform/",
            json={
                "name": "v3.nfk_events_xform",
                "query": "SELECT account_id, is_fraud, amount FROM v3.src_nfk_events",
                "mode": "published",
                "primary_key": ["account_id"],
            },
        )
        assert resp.status_code == 201, resp.json()
        # Dim has is_fraud_key (int PK) and is_fraud (string label) —
        # the dim's non-PK column shares a name with the parent's FK.
        resp = await client.post(
            "/nodes/dimension/",
            json={
                "name": "v3.nfk_is_fraud_dim",
                "query": (
                    "SELECT t.is_fraud_key, t.is_fraud "
                    "FROM (SELECT 0 AS is_fraud_key, 'false' AS is_fraud "
                    "UNION ALL SELECT 1, 'true') t"
                ),
                "mode": "published",
                "primary_key": ["is_fraud_key"],
            },
        )
        assert resp.status_code == 201, resp.json()
        resp = await client.post(
            "/nodes/v3.nfk_events_xform/link/",
            json={
                "dimension_node": "v3.nfk_is_fraud_dim",
                "join_type": "left",
                "join_on": (
                    "v3.nfk_events_xform.is_fraud = v3.nfk_is_fraud_dim.is_fraud_key"
                ),
            },
        )
        assert resp.status_code in (200, 201), resp.json()
        resp = await client.post(
            "/nodes/metric/",
            json={
                "name": "v3.nfk_total_amount",
                "query": "SELECT SUM(amount) FROM v3.nfk_events_xform",
                "mode": "published",
            },
        )
        assert resp.status_code == 201, resp.json()

        response = await client.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.nfk_total_amount"],
                "dimensions": ["v3.nfk_is_fraud_dim.is_fraud"],
                "filters": ["v3.nfk_is_fraud_dim.is_fraud_key IN (0)"],
            },
        )
        assert response.status_code == 200, response.json()
        assert_sql_equal(
            get_first_grain_group(response.json())["sql"],
            """
            WITH v3_nfk_events_xform AS (
              SELECT is_fraud, amount
              FROM default.v3.nfk_events
              WHERE is_fraud IN (0)
            ),
            v3_nfk_is_fraud_dim AS (
              SELECT t.is_fraud_key, t.is_fraud
              FROM (
                SELECT 0 AS is_fraud_key, 'false' AS is_fraud
                UNION ALL
                SELECT 1, 'true'
              ) t
              WHERE t.is_fraud_key IN (0)
            )
            SELECT t2.is_fraud,
                   SUM(t1.amount) amount_sum_HASH
            FROM v3_nfk_events_xform t1
            LEFT OUTER JOIN v3_nfk_is_fraud_dim t2
              ON t1.is_fraud = t2.is_fraud_key
            GROUP BY t2.is_fraud
            """,
            normalize_aliases=True,
        )
