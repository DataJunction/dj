"""
Tests for build_v2 column scoping in nested subqueries.

This tests a specific bug where column references in nested subqueries
incorrectly get qualified with table aliases from outer scopes.

The bug occurs when:
1. A transform has nested subqueries (inner SELECT inside FROM)
2. The outer query has a LEFT JOIN with a table alias
3. Columns in the inner subquery get incorrectly qualified with the outer table alias

For example, this transform:
    SELECT
        videos.video_id,  -- outer scope, references 'videos' from LEFT JOIN
        ...
    FROM (
        SELECT video_id, ...  -- inner scope, should be unqualified
        FROM source_table
    )
    LEFT JOIN valid_video_ids AS videos ON ...

Should NOT have the inner `video_id` become `videos.video_id`.
"""

import pytest
from httpx import AsyncClient

from datajunction_server.sql.parsing.backends.antlr4 import parse


class TestNestedSubqueryColumnScoping:
    """Tests for column scoping in nested subqueries."""

    @pytest.mark.asyncio
    async def test_nested_subquery_with_left_join_column_scoping(
        self,
        module__client_with_build_v3: AsyncClient,
    ):
        """
        Test that columns in nested subqueries are not incorrectly qualified
        with table aliases from outer scopes.

        This reproduces a bug where:
        - A transform has a nested subquery structure
        - The outer query has a LEFT JOIN with an alias (e.g., 'videos')
        - Columns in the inner subquery incorrectly get qualified with 'videos.'
        """
        client = module__client_with_build_v3

        # Create a source node for valid IDs (will be LEFT JOINed)
        valid_ids_response = await client.post(
            "/nodes/source/",
            json={
                "name": "v3.valid_product_ids",
                "description": "Valid product IDs for validation",
                "columns": [
                    {"name": "product_id", "type": "int"},
                    {"name": "is_active", "type": "boolean"},
                ],
                "mode": "published",
                "catalog": "default",
                "schema_": "v3",
                "table": "valid_product_ids",
            },
        )
        assert valid_ids_response.status_code in (200, 201, 409), (
            valid_ids_response.json()
        )

        # Create a transform with nested subquery and LEFT JOIN
        # The key pattern is:
        # - Inner subquery selects product_id (from source)
        # - Outer query LEFT JOINs to valid_product_ids AS products
        # - Outer query references products.product_id
        transform_response = await client.post(
            "/nodes/transform/",
            json={
                "name": "v3.nested_subquery_transform",
                "description": "Transform with nested subquery and LEFT JOIN",
                "query": """
                    SELECT
                        inner_data.order_id,
                        inner_data.product_id,
                        inner_data.quantity,
                        inner_data.computed_value,
                        products.product_id AS valid_product_id,
                        products.is_active,
                        products.product_id IS NOT NULL AS is_valid_product
                    FROM (
                        SELECT
                            order_id,
                            product_id,
                            quantity,
                            quantity * 10 AS computed_value
                        FROM v3.src_order_items
                    ) AS inner_data
                    LEFT JOIN v3.valid_product_ids AS products
                        ON inner_data.product_id = products.product_id
                """,
                "mode": "published",
            },
        )
        assert transform_response.status_code in (200, 201), transform_response.json()

        # Create a metric on the transform
        metric_response = await client.post(
            "/nodes/metric/",
            json={
                "name": "v3.nested_transform_quantity",
                "description": "Total quantity from nested transform",
                "query": "SELECT SUM(quantity) FROM v3.nested_subquery_transform",
                "mode": "published",
            },
        )
        assert metric_response.status_code in (200, 201), metric_response.json()

        # Get SQL for the metric - this triggers the build_v2 flow
        sql_response = await client.get(
            "/sql/v3.nested_transform_quantity",
        )
        assert sql_response.status_code == 200, sql_response.json()

        sql = sql_response.json()["sql"]

        # The SQL should be parseable without errors
        # If the bug exists, the SQL will have invalid column references
        # like 'products.product_id' in the inner subquery where 'products' isn't in scope
        try:
            parsed = parse(sql)
            # If we get here, the SQL is at least syntactically valid
            assert parsed is not None
        except Exception as e:
            pytest.fail(f"Generated SQL is not parseable: {e}\n\nSQL:\n{sql}")

        # Additional check: the inner subquery should NOT reference 'products'
        # Look for the pattern that indicates the bug:
        # The inner SELECT (inside FROM) should not have 'products.' prefix
        #
        # We'll check this by looking for 'products.product_id' or 'products.quantity'
        # appearing before the LEFT JOIN clause
        sql_upper = sql.upper()
        left_join_pos = sql_upper.find("LEFT JOIN")

        if left_join_pos > 0:
            # Get the SQL before the LEFT JOIN
            before_join = sql[:left_join_pos]

            # Check if 'products.' appears in the inner subquery context
            # We need to be careful here - 'products' is a valid reference AFTER the JOIN
            # The bug would cause 'products.' to appear INSIDE the inner SELECT
            #
            # Look for the inner SELECT pattern
            inner_select_start = before_join.upper().rfind("SELECT")
            if inner_select_start > 0:
                inner_select_section = before_join[inner_select_start:]
                # This should not contain 'products.' since products isn't defined yet
                assert "products." not in inner_select_section.lower(), (
                    f"Bug detected: 'products.' reference found in inner subquery "
                    f"where 'products' alias is not in scope.\n\n"
                    f"Inner SELECT section:\n{inner_select_section}\n\n"
                    f"Full SQL:\n{sql}"
                )

    @pytest.mark.asyncio
    async def test_deeply_nested_subquery_column_scoping(
        self,
        module__client_with_build_v3: AsyncClient,
    ):
        """
        Test that columns in deeply nested subqueries (multiple levels)
        are not incorrectly qualified with table aliases from outer scopes.
        """
        client = module__client_with_build_v3

        # Create a transform with multiple levels of nesting
        transform_response = await client.post(
            "/nodes/transform/",
            json={
                "name": "v3.deeply_nested_transform",
                "description": "Transform with deeply nested subqueries",
                "query": """
                    SELECT
                        level1.order_id,
                        level1.product_id,
                        level1.total_quantity,
                        products.product_id AS validated_product_id
                    FROM (
                        SELECT
                            level2.order_id,
                            level2.product_id,
                            SUM(level2.quantity) AS total_quantity
                        FROM (
                            SELECT
                                order_id,
                                product_id,
                                quantity
                            FROM v3.src_order_items
                        ) AS level2
                        GROUP BY level2.order_id, level2.product_id
                    ) AS level1
                    LEFT JOIN v3.valid_product_ids AS products
                        ON level1.product_id = products.product_id
                """,
                "mode": "published",
            },
        )
        assert transform_response.status_code in (200, 201), transform_response.json()

        # Create a metric on the deeply nested transform
        metric_response = await client.post(
            "/nodes/metric/",
            json={
                "name": "v3.deeply_nested_quantity",
                "description": "Total quantity from deeply nested transform",
                "query": "SELECT SUM(total_quantity) FROM v3.deeply_nested_transform",
                "mode": "published",
            },
        )
        assert metric_response.status_code in (200, 201), metric_response.json()

        # Get SQL for the metric
        sql_response = await client.get(
            "/sql/v3.deeply_nested_quantity",
        )
        assert sql_response.status_code == 200, sql_response.json()

        sql = sql_response.json()["sql"]

        # The SQL should be parseable without errors
        try:
            parsed = parse(sql)
            assert parsed is not None
        except Exception as e:
            pytest.fail(f"Generated SQL is not parseable: {e}\n\nSQL:\n{sql}")

    @pytest.mark.asyncio
    async def test_multiple_left_joins_column_scoping(
        self,
        module__client_with_build_v3: AsyncClient,
    ):
        """
        Test that columns are correctly scoped when there are multiple LEFT JOINs
        with different aliases.
        """
        client = module__client_with_build_v3

        # Create additional source node for customers
        # (v3.src_customers already exists in BUILD_V3)

        # Create a transform with multiple LEFT JOINs
        transform_response = await client.post(
            "/nodes/transform/",
            json={
                "name": "v3.multi_join_transform",
                "description": "Transform with multiple LEFT JOINs",
                "query": """
                    SELECT
                        inner_data.order_id,
                        inner_data.customer_id,
                        inner_data.product_id,
                        customers.name AS customer_name,
                        products.is_active AS product_is_active
                    FROM (
                        SELECT
                            o.order_id,
                            o.customer_id,
                            oi.product_id
                        FROM v3.src_orders o
                        JOIN v3.src_order_items oi ON o.order_id = oi.order_id
                    ) AS inner_data
                    LEFT JOIN v3.src_customers AS customers
                        ON inner_data.customer_id = customers.customer_id
                    LEFT JOIN v3.valid_product_ids AS products
                        ON inner_data.product_id = products.product_id
                """,
                "mode": "published",
            },
        )
        assert transform_response.status_code in (200, 201), transform_response.json()

        # Create a metric
        metric_response = await client.post(
            "/nodes/metric/",
            json={
                "name": "v3.multi_join_count",
                "description": "Count from multi-join transform",
                "query": "SELECT COUNT(*) FROM v3.multi_join_transform",
                "mode": "published",
            },
        )
        assert metric_response.status_code in (200, 201), metric_response.json()

        # Get SQL
        sql_response = await client.get(
            "/sql/v3.multi_join_count",
        )
        assert sql_response.status_code == 200, sql_response.json()

        sql = sql_response.json()["sql"]

        # Verify the SQL is parseable
        try:
            parsed = parse(sql)
            assert parsed is not None
        except Exception as e:
            pytest.fail(f"Generated SQL is not parseable: {e}\n\nSQL:\n{sql}")

        # Check that table aliases from outer JOINs don't appear in inner subquery
        sql_lower = sql.lower()

        # Find the innermost subquery (before the first LEFT JOIN)
        first_left_join = sql_lower.find("left join")
        if first_left_join > 0:
            before_joins = sql[:first_left_join]
            # Look for the inner SELECT
            inner_selects = before_joins.lower().split("select")
            if len(inner_selects) > 1:
                # Check the innermost SELECT doesn't reference outer aliases
                innermost = inner_selects[-1]
                assert "customers." not in innermost, (
                    f"Bug: 'customers.' found in inner subquery\n{sql}"
                )
                assert "products." not in innermost, (
                    f"Bug: 'products.' found in inner subquery\n{sql}"
                )
