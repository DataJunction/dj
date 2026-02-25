"""
Tests for BFS-based join path finding in BuildV3.

These tests cover:
- Multi-hop dimension graphs (depth >= 5)
- Cycle prevention in dimension link graphs
- Multiple paths to same dimension with different roles
- Batched query performance with large graphs
"""

import pytest
from datajunction_server.construction.build_v3.loaders import (
    find_join_paths_batch,
)
from . import assert_sql_equal


@pytest.mark.asyncio
class TestBFSJoinPathFinding:
    """Tests for BFS join path finding with complex dimension graphs."""

    async def test_existing_multi_path_to_date(self, client_with_build_v3):
        """
        Test requesting date dimension that can be reached through multiple paths.

        In BUILD_V3:
        - v3.order_details has order_date FK to v3.date
        - v3.order_details -> v3.customer -> v3.date (through registration_date)

        When we request v3.date.month, the system should detect this and potentially
        use the direct path (or both if requesting with different roles).
        """
        # Request a metric with just v3.date.month (no role specified)
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.date.month"],
            },
        )

        assert response.status_code == 200, response.json()
        data = response.json()
        sql = data["grain_groups"][0]["sql"]

        print(f"\n=== SQL with v3.date.month (no role) ===\n{sql}\n")

        # This documents current behavior: when no role is specified,
        # the system picks ONE of the available paths (might be the longer one!)
        #
        # ISSUE: The system should prefer the shortest path, or use skip-join
        # optimization when the dimension is a local FK column.
        #
        # For now, just verify the query works and document what we get
        if "v3_customer" in sql.lower():
            print(
                "⚠️  WARNING: Using longer path through customer instead of direct path!",
            )
        else:
            print("✅ Using direct path (or skip-join)")

        # Verify basic structure - should have a month column
        columns = data["grain_groups"][0]["columns"]
        column_names = [col["name"] for col in columns]
        month_cols = [n for n in column_names if "month" in n.lower()]
        assert len(month_cols) >= 1, (
            f"Should have at least one month column, got: {column_names}"
        )

    async def test_multi_hop_path_depth_5(self, session, client_with_build_v3):
        """
        Test BFS finds a 5-hop path through multiple intermediate dimensions.

        Graph structure:
        fact -> dim1 -> dim2 -> dim3 -> dim4 -> dim5

        This tests that BFS correctly explores deep paths without hitting
        artificial depth limits.
        """
        # Create a chain of 5 dimension nodes
        for i in range(1, 6):
            response = await client_with_build_v3.post(
                "/nodes/dimension/",
                json={
                    "name": f"v3.chain_dim_{i}",
                    "description": f"Dimension {i} in chain",
                    "query": f"SELECT id_{i} FROM v3.src_chain_{i}",
                    "mode": "published",
                    "primary_key": [f"id_{i}"],
                },
            )
            assert response.status_code == 201, response.json()

        # Create a transform that will be our starting point
        response = await client_with_build_v3.post(
            "/nodes/transform/",
            json={
                "name": "v3.chain_fact",
                "description": "Fact table at start of chain",
                "query": "SELECT fact_id, dim1_id FROM v3.src_facts",
                "mode": "published",
            },
        )
        assert response.status_code == 201, response.json()

        # Create dimension links: fact -> dim1 -> dim2 -> dim3 -> dim4 -> dim5
        # Link 1: fact -> dim1
        response = await client_with_build_v3.post(
            "/nodes/v3.chain_fact/link/",
            json={
                "dimension_node": "v3.chain_dim_1",
                "join_sql": "v3.chain_fact.dim1_id = v3.chain_dim_1.id_1",
                "join_type": "left",
            },
        )
        assert response.status_code == 201, response.json()

        # Link 2: dim1 -> dim2
        response = await client_with_build_v3.post(
            "/nodes/v3.chain_dim_1/link/",
            json={
                "dimension_node": "v3.chain_dim_2",
                "join_sql": "v3.chain_dim_1.id_1 = v3.chain_dim_2.id_2",
                "join_type": "left",
            },
        )
        assert response.status_code == 201, response.json()

        # Link 3: dim2 -> dim3
        response = await client_with_build_v3.post(
            "/nodes/v3.chain_dim_2/link/",
            json={
                "dimension_node": "v3.chain_dim_3",
                "join_sql": "v3.chain_dim_2.id_2 = v3.chain_dim_3.id_3",
                "join_type": "left",
            },
        )
        assert response.status_code == 201, response.json()

        # Link 4: dim3 -> dim4
        response = await client_with_build_v3.post(
            "/nodes/v3.chain_dim_3/link/",
            json={
                "dimension_node": "v3.chain_dim_4",
                "join_sql": "v3.chain_dim_3.id_3 = v3.chain_dim_4.id_4",
                "join_type": "left",
            },
        )
        assert response.status_code == 201, response.json()

        # Link 5: dim4 -> dim5
        response = await client_with_build_v3.post(
            "/nodes/v3.chain_dim_4/link/",
            json={
                "dimension_node": "v3.chain_dim_5",
                "join_sql": "v3.chain_dim_4.id_4 = v3.chain_dim_5.id_5",
                "join_type": "left",
            },
        )
        assert response.status_code == 201, response.json()

        # Now test that we can find the path from fact to dim5
        # Get the revision ID for chain_fact
        from sqlalchemy import select
        from datajunction_server.database.node import Node

        result = await session.execute(
            select(Node).where(Node.name == "v3.chain_fact"),
        )
        fact_node = result.scalar_one()
        source_rev_id = fact_node.current.id

        # Use BFS to find path
        paths = await find_join_paths_batch(
            session,
            source_revision_ids={source_rev_id},
            target_dimension_names={"v3.chain_dim_5"},
        )

        # Should find exactly one path: fact -> dim1 -> dim2 -> dim3 -> dim4 -> dim5
        matching_paths = [
            (key, path) for key, path in paths.items() if key[1] == "v3.chain_dim_5"
        ]
        assert len(matching_paths) == 1, (
            f"Expected exactly 1 path to chain_dim_5, found {len(matching_paths)}"
        )

        key, path = matching_paths[0]
        assert len(path) == 5, f"Expected 5-hop path, got {len(path)} hops"

    async def test_cycle_prevention(self, session, client_with_build_v3):
        """
        Test that BFS correctly handles cycles in the dimension graph.

        Graph structure with cycle:
        fact -> dim_a -> dim_b -> dim_c -> dim_a (cycle!)
                     \\-> dim_target

        BFS should still find the path to dim_target without infinite loops.
        """
        # Create dimensions that form a cycle
        for dim_name in ["dim_a", "dim_b", "dim_c", "dim_target"]:
            response = await client_with_build_v3.post(
                "/nodes/dimension/",
                json={
                    "name": f"v3.cycle_{dim_name}",
                    "description": f"Dimension {dim_name} for cycle test",
                    "query": f"SELECT id FROM v3.src_{dim_name}",
                    "mode": "published",
                    "primary_key": ["id"],
                },
            )
            assert response.status_code == 201, response.json()

        # Create fact
        response = await client_with_build_v3.post(
            "/nodes/transform/",
            json={
                "name": "v3.cycle_fact",
                "description": "Fact with cyclic dimension references",
                "query": "SELECT fact_id, dim_a_id FROM v3.src_cycle_facts",
                "mode": "published",
            },
        )
        assert response.status_code == 201, response.json()

        # Create links forming a cycle: fact -> a -> b -> c -> a (cycle)
        response = await client_with_build_v3.post(
            "/nodes/v3.cycle_fact/link/",
            json={
                "dimension_node": "v3.cycle_dim_a",
                "join_sql": "v3.cycle_fact.dim_a_id = v3.cycle_dim_a.id",
                "join_type": "left",
            },
        )
        assert response.status_code == 201, response.json()

        response = await client_with_build_v3.post(
            "/nodes/v3.cycle_dim_a/link/",
            json={
                "dimension_node": "v3.cycle_dim_b",
                "join_sql": "v3.cycle_dim_a.id = v3.cycle_dim_b.id",
                "join_type": "left",
            },
        )
        assert response.status_code == 201, response.json()

        response = await client_with_build_v3.post(
            "/nodes/v3.cycle_dim_b/link/",
            json={
                "dimension_node": "v3.cycle_dim_c",
                "join_sql": "v3.cycle_dim_b.id = v3.cycle_dim_c.id",
                "join_type": "left",
            },
        )
        assert response.status_code == 201, response.json()

        # Create the cycle: c -> a
        response = await client_with_build_v3.post(
            "/nodes/v3.cycle_dim_c/link/",
            json={
                "dimension_node": "v3.cycle_dim_a",
                "join_sql": "v3.cycle_dim_c.id = v3.cycle_dim_a.id",
                "join_type": "left",
                "role": "back_to_a",  # Use a role to distinguish this link
            },
        )
        assert response.status_code == 201, response.json()

        # Also create a path to target: a -> target
        response = await client_with_build_v3.post(
            "/nodes/v3.cycle_dim_a/link/",
            json={
                "dimension_node": "v3.cycle_dim_target",
                "join_sql": "v3.cycle_dim_a.id = v3.cycle_dim_target.id",
                "join_type": "left",
            },
        )
        assert response.status_code == 201, response.json()

        # Test BFS finds path to target without infinite loop
        from sqlalchemy import select
        from datajunction_server.database.node import Node

        result = await session.execute(
            select(Node).where(Node.name == "v3.cycle_fact"),
        )
        fact_node = result.scalar_one()
        source_rev_id = fact_node.current.id

        # This should complete without hanging (cycle prevention works)
        paths = await find_join_paths_batch(
            session,
            source_revision_ids={source_rev_id},
            target_dimension_names={"v3.cycle_dim_target"},
            max_depth=10,  # Even with cycles, should terminate quickly
        )

        # Should find path: fact -> dim_a -> dim_target
        matching_paths = [
            (key, path)
            for key, path in paths.items()
            if key[1] == "v3.cycle_dim_target"
        ]
        assert len(matching_paths) >= 1, "Should find at least one path to target"

        # Verify we found a reasonable path (not infinitely long)
        key, path = matching_paths[0]
        assert len(path) <= 5, (
            f"Path should be short despite cycle, got {len(path)} hops"
        )

    async def test_multiple_roles_same_dimension(self, session, client_with_build_v3):
        """
        Test that BFS correctly finds multiple paths to the same dimension with different roles.

        This is a critical real-world scenario where a fact might link to the same
        dimension multiple times with different semantic meanings.

        Example:
        order -> date (order_date role="order")
        order -> date (ship_date role="ship")
        order -> date (delivery_date role="delivery")
        """
        # Create a date dimension
        response = await client_with_build_v3.post(
            "/nodes/dimension/",
            json={
                "name": "v3.multi_role_date",
                "description": "Date dimension with multiple roles",
                "query": "SELECT date_id, year, month FROM v3.src_dates",
                "mode": "published",
                "primary_key": ["date_id"],
            },
        )
        assert response.status_code == 201, response.json()

        # Create an order fact
        response = await client_with_build_v3.post(
            "/nodes/transform/",
            json={
                "name": "v3.multi_role_orders",
                "description": "Orders with multiple date references",
                "query": """
                    SELECT
                        order_id,
                        order_date_id,
                        ship_date_id,
                        delivery_date_id
                    FROM v3.src_orders
                """,
                "mode": "published",
            },
        )
        assert response.status_code == 201, response.json()

        # Create three links to the same date dimension with different roles
        for role, col in [
            ("order", "order_date_id"),
            ("ship", "ship_date_id"),
            ("delivery", "delivery_date_id"),
        ]:
            response = await client_with_build_v3.post(
                "/nodes/v3.multi_role_orders/link/",
                json={
                    "dimension_node": "v3.multi_role_date",
                    "join_sql": f"v3.multi_role_orders.{col} = v3.multi_role_date.date_id",
                    "join_type": "left",
                    "role": role,
                },
            )
            assert response.status_code == 201, response.json()

        # Test BFS finds all three paths
        from sqlalchemy import select
        from datajunction_server.database.node import Node

        result = await session.execute(
            select(Node).where(Node.name == "v3.multi_role_orders"),
        )
        orders_node = result.scalar_one()
        source_rev_id = orders_node.current.id

        paths = await find_join_paths_batch(
            session,
            source_revision_ids={source_rev_id},
            target_dimension_names={"v3.multi_role_date"},
        )

        # Should find exactly 3 paths, one for each role
        matching_paths = [
            (key, path) for key, path in paths.items() if key[1] == "v3.multi_role_date"
        ]
        assert len(matching_paths) == 3, (
            f"Expected 3 paths (one per role), found {len(matching_paths)}"
        )

        # Verify roles are correct
        found_roles = {key[2] for key, _ in matching_paths}  # key[2] is role_path
        expected_roles = {"order", "ship", "delivery"}
        assert found_roles == expected_roles, (
            f"Found roles {found_roles}, expected {expected_roles}"
        )

    async def test_same_dimension_different_role_paths(
        self,
        client_with_build_v3,
    ):
        """
        Test requesting the same dimension column with different role paths.

        Critical scenario:
        - v3.time.month[sale] - time through sale (1-hop)
        - v3.time.month[customer->birth] - time through customer birth (2-hop)

        Both request v3.time.month but via different semantic paths.
        The system should find both paths, create separate joins, and map output correctly.
        """
        # Create a time dimension (separate from existing v3.date to avoid conflicts)
        response = await client_with_build_v3.post(
            "/nodes/source/",
            json={
                "name": "v3.src_time",
                "description": "Time source",
                "catalog": "default",
                "schema_": "v3",
                "table": "time_dim",
                "columns": [
                    {"name": "time_id", "type": "int"},
                    {"name": "month", "type": "int"},
                    {"name": "year", "type": "int"},
                ],
            },
        )
        assert response.status_code in (200, 201), response.json()

        response = await client_with_build_v3.post(
            "/nodes/dimension/",
            json={
                "name": "v3.time",
                "description": "Time dimension",
                "query": "SELECT time_id, month, year FROM v3.src_time",
                "mode": "published",
                "primary_key": ["time_id"],
            },
        )
        assert response.status_code == 201, response.json()

        # Create customer dimension
        response = await client_with_build_v3.post(
            "/nodes/source/",
            json={
                "name": "v3.src_customers_time",
                "description": "Customers source",
                "catalog": "default",
                "schema_": "v3",
                "table": "customers_time",
                "columns": [
                    {"name": "customer_id", "type": "int"},
                    {"name": "name", "type": "string"},
                    {"name": "birth_time_id", "type": "int"},
                ],
            },
        )
        assert response.status_code in (200, 201), response.json()

        response = await client_with_build_v3.post(
            "/nodes/dimension/",
            json={
                "name": "v3.customer_time",
                "description": "Customer dimension",
                "query": "SELECT customer_id, name, birth_time_id FROM v3.src_customers_time",
                "mode": "published",
                "primary_key": ["customer_id"],
            },
        )
        assert response.status_code == 201, response.json()

        # Create sales fact
        response = await client_with_build_v3.post(
            "/nodes/source/",
            json={
                "name": "v3.src_sales_time",
                "description": "Sales source",
                "catalog": "default",
                "schema_": "v3",
                "table": "sales_time",
                "columns": [
                    {"name": "sale_id", "type": "int"},
                    {"name": "customer_id", "type": "int"},
                    {"name": "sale_time_id", "type": "int"},
                    {"name": "amount", "type": "double"},
                ],
            },
        )
        assert response.status_code in (200, 201), response.json()

        response = await client_with_build_v3.post(
            "/nodes/transform/",
            json={
                "name": "v3.sales_time",
                "description": "Sales fact",
                "query": "SELECT sale_id, customer_id, sale_time_id, amount FROM v3.src_sales_time",
                "mode": "published",
            },
        )
        assert response.status_code == 201, response.json()

        # Create dimension links using correct API format
        # Path 1: sales -> time (role="sale")
        response = await client_with_build_v3.post(
            "/nodes/v3.sales_time/link",
            json={
                "dimension_node": "v3.time",
                "join_on": "v3.sales_time.sale_time_id = v3.time.time_id",
                "join_type": "left",
                "join_cardinality": "many_to_one",
                "role": "sale",
            },
        )
        assert response.status_code == 201, response.json()

        # Path 2: sales -> customer (role="customer")
        response = await client_with_build_v3.post(
            "/nodes/v3.sales_time/link",
            json={
                "dimension_node": "v3.customer_time",
                "join_on": "v3.sales_time.customer_id = v3.customer_time.customer_id",
                "join_type": "left",
                "join_cardinality": "many_to_one",
                "role": "customer",
            },
        )
        assert response.status_code == 201, response.json()

        # Path 3: customer -> time (role="birth")
        response = await client_with_build_v3.post(
            "/nodes/v3.customer_time/link",
            json={
                "dimension_node": "v3.time",
                "join_on": "v3.customer_time.birth_time_id = v3.time.time_id",
                "join_type": "left",
                "join_cardinality": "many_to_one",
                "role": "birth",
            },
        )
        assert response.status_code == 201, response.json()

        # Create metric
        response = await client_with_build_v3.post(
            "/nodes/metric/",
            json={
                "name": "v3.total_sales_time",
                "description": "Total sales",
                "query": "SELECT SUM(amount) FROM v3.sales_time",
                "mode": "published",
            },
        )
        assert response.status_code == 201, response.json()

        # Now test: request metric with BOTH role paths to v3.time
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_sales_time"],
                "dimensions": [
                    "v3.time.month[sale]",  # Short path: sales->time (1-hop)
                    "v3.time.month[customer->birth]",  # Long path: sales->customer->time (2-hop)
                ],
            },
        )

        assert response.status_code == 200, response.json()
        data = response.json()

        # Verify we have grain groups
        assert len(data["grain_groups"]) >= 1

        sql = data["grain_groups"][0]["sql"]

        # Verify the SQL structure using assert_sql_equal
        # Should have:
        # 1. Two separate joins to v3_time (different aliases for each role path)
        # 2. Join to customer for the long path
        # 3. Two month columns in SELECT (month_sale and month_birth)
        assert_sql_equal(
            sql,
            """
            WITH
            v3_customer_time AS (
                SELECT customer_id
                FROM v3.src_customers_time
            ),
            v3_sales_time AS (
                SELECT customer_id, sale_time_id, amount
                FROM default.v3.sales_time
            ),
            v3_time AS (
                SELECT time_id, month
                FROM default.v3.time_dim
            )
            SELECT
                t2.month month_sale,
                t4.month month_birth,
                SUM(t1.amount) amount_sum_50949daa
            FROM v3_sales_time t1
            LEFT OUTER JOIN v3_time t2 ON t1.sale_time_id = t2.time_id
            LEFT OUTER JOIN v3_customer_time t3 ON t1.customer_id = t3.customer_id
            LEFT OUTER JOIN v3_time t4 ON t3.birth_time_id = t4.time_id
            GROUP BY t2.month, t4.month
            """,
        )

        # Verify columns include both month references
        columns = data["grain_groups"][0]["columns"]
        column_names = [col["name"] for col in columns]

        # Should have two month columns (one for each role path)
        month_columns = [name for name in column_names if "month" in name.lower()]
        assert len(month_columns) == 2, (
            f"Expected exactly 2 month columns (one per role path), "
            f"found {len(month_columns)}: {column_names}"
        )

    async def test_multi_hop_role_path(self, session, client_with_build_v3):
        """
        Test BFS correctly builds multi-hop role paths.

        Example: order -> customer (role="customer") -> country (role="home")
        Should produce role_path = "customer->home"
        """
        # Create dimensions
        for dim_name in ["multi_customer", "multi_country"]:
            response = await client_with_build_v3.post(
                "/nodes/dimension/",
                json={
                    "name": f"v3.{dim_name}",
                    "description": f"Dimension {dim_name}",
                    "query": f"SELECT id FROM v3.src_{dim_name}",
                    "mode": "published",
                    "primary_key": ["id"],
                },
            )
            assert response.status_code == 201, response.json()

        # Create fact
        response = await client_with_build_v3.post(
            "/nodes/transform/",
            json={
                "name": "v3.multi_role_orders",
                "description": "Orders for multi-hop role test",
                "query": "SELECT order_id, customer_id FROM v3.src_orders",
                "mode": "published",
            },
        )
        assert response.status_code == 201, response.json()

        # Link: order -> customer (role="customer")
        response = await client_with_build_v3.post(
            "/nodes/v3.multi_role_orders/link/",
            json={
                "dimension_node": "v3.multi_customer",
                "join_sql": "v3.multi_role_orders.customer_id = v3.multi_customer.id",
                "join_type": "left",
                "role": "customer",
            },
        )
        assert response.status_code == 201, response.json()

        # Link: customer -> country (role="home")
        response = await client_with_build_v3.post(
            "/nodes/v3.multi_customer/link/",
            json={
                "dimension_node": "v3.multi_country",
                "join_sql": "v3.multi_customer.id = v3.multi_country.id",
                "join_type": "left",
                "role": "home",
            },
        )
        assert response.status_code == 201, response.json()

        # Test BFS builds correct role path
        from sqlalchemy import select
        from datajunction_server.database.node import Node

        result = await session.execute(
            select(Node).where(Node.name == "v3.multi_role_orders"),
        )
        orders_node = result.scalar_one()
        source_rev_id = orders_node.current.id

        paths = await find_join_paths_batch(
            session,
            source_revision_ids={source_rev_id},
            target_dimension_names={"v3.multi_country"},
        )

        # Find the path to country
        matching_paths = [
            (key, path) for key, path in paths.items() if key[1] == "v3.multi_country"
        ]
        assert len(matching_paths) == 1, f"Expected 1 path, found {len(matching_paths)}"

        key, path = matching_paths[0]
        source_rev, target_name, role_path = key

        # Verify role path is "customer->home"
        assert role_path == "customer->home", (
            f"Expected role_path='customer->home', got '{role_path}'"
        )
        assert len(path) == 2, f"Expected 2-hop path, got {len(path)}"

    async def test_batch_query_performance(self, session, client_with_build_v3):
        """
        Test that BFS uses batched queries (one query per depth level).

        This is a regression test to ensure we don't revert to N+1 queries.
        We can't easily measure query count in tests, but we can verify that
        large graphs are handled efficiently.
        """
        # Create a "star" schema with one fact linking to many dimensions
        num_dimensions = 20

        # Create dimensions
        for i in range(num_dimensions):
            response = await client_with_build_v3.post(
                "/nodes/dimension/",
                json={
                    "name": f"v3.batch_dim_{i}",
                    "description": f"Dimension {i} for batch test",
                    "query": f"SELECT id FROM v3.src_dim_{i}",
                    "mode": "published",
                    "primary_key": ["id"],
                },
            )
            assert response.status_code == 201, response.json()

        # Create fact
        response = await client_with_build_v3.post(
            "/nodes/transform/",
            json={
                "name": "v3.batch_fact",
                "description": "Fact with many dimension links",
                "query": "SELECT fact_id FROM v3.src_batch_facts",
                "mode": "published",
            },
        )
        assert response.status_code == 201, response.json()

        # Create links to all dimensions
        for i in range(num_dimensions):
            response = await client_with_build_v3.post(
                "/nodes/v3.batch_fact/link/",
                json={
                    "dimension_node": f"v3.batch_dim_{i}",
                    "join_sql": f"v3.batch_fact.fact_id = v3.batch_dim_{i}.id",
                    "join_type": "left",
                },
            )
            assert response.status_code == 201, response.json()

        # Test BFS finds all paths efficiently
        from sqlalchemy import select
        from datajunction_server.database.node import Node

        result = await session.execute(
            select(Node).where(Node.name == "v3.batch_fact"),
        )
        fact_node = result.scalar_one()
        source_rev_id = fact_node.current.id

        # Request all dimensions at once
        target_dims = {f"v3.batch_dim_{i}" for i in range(num_dimensions)}

        paths = await find_join_paths_batch(
            session,
            source_revision_ids={source_rev_id},
            target_dimension_names=target_dims,
        )

        # Should find all dimensions
        found_targets = {key[1] for key in paths.keys()}
        assert len(found_targets) == num_dimensions, (
            f"Expected to find {num_dimensions} dimensions, found {len(found_targets)}"
        )

        # All should be 1-hop paths (direct links)
        for key, path in paths.items():
            assert len(path) == 1, f"Expected 1-hop path for {key[1]}, got {len(path)}"
