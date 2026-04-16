"""
Tests for filter pushdown into CTEs.

Verifies that user-supplied dimension filters are pushed down into the
appropriate CTE WHERE clauses, not just applied on the outer query.
"""

import pytest

from tests.construction.build_v3 import get_first_grain_group


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
        data = get_first_grain_group(response.json())
        sql = data["sql"]

        # The filter should be pushed into the v3_order_details CTE as
        # order_date >= 20240101 (order_date is the FK to v3.date.date_id)
        assert "order_date >= 20240101" in sql


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
        data = get_first_grain_group(response.json())
        sql = data["sql"]

        # The filter should appear in the v3_product CTE WHERE clause
        # AND in the outer WHERE (redundant but safe)
        assert sql.count("'Electronics'") >= 2, (
            f"Filter should appear in both CTE and outer WHERE, got:\n{sql}"
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
        data = get_first_grain_group(response.json())
        sql = data["sql"]

        # Date filter pushed to parent CTE as FK column
        assert "order_date >= 20240101" in sql
        # Product filter pushed to dimension CTE
        assert sql.count("'Electronics'") >= 2


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
        data = get_first_grain_group(response.json())
        sql = data["sql"]

        # The v3_order_details CTE should not have a WHERE clause
        # (only the original query's WHERE, if any)
        cte_start = sql.lower().find("v3_order_details as")
        assert cte_start >= 0, "v3_order_details CTE should exist"
        # Find closing paren for this CTE
        paren_depth = 0
        cte_body_start = sql.find("(", cte_start)
        for i in range(cte_body_start, len(sql)):
            if sql[i] == "(":
                paren_depth += 1
            elif sql[i] == ")":
                paren_depth -= 1
                if paren_depth == 0:
                    cte_body = sql[cte_body_start : i + 1]
                    break
        else:
            cte_body = ""
        # No WHERE with date filter values
        assert "20240101" not in cte_body

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
        data = get_first_grain_group(response.json())
        sql = data["sql"]

        # status is a local column on v3.order_details — should be pushed
        # into the v3_order_details CTE
        assert sql.count("'completed'") >= 2, (
            f"Local dim filter should appear in CTE and outer WHERE:\n{sql}"
        )
