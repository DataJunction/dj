"""
Tests for derived metrics functionality.
Derived metrics are metrics that reference other metrics.
"""

from typing import Any, Callable, Coroutine, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

import datajunction_server.sql.parsing.ast as ast_module
from datajunction_server.database.column import Column as DBColumn
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.user import User
from datajunction_server.errors import (
    DJError,
    DJErrorException,
    DJException,
    ErrorCode,
)
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.parsing.ast import Column, CompileContext, Query, Table
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.sql.parsing.types import IntegerType, StringType


class TestDerivedMetricHelpers:
    """Test helper functions for derived metrics."""

    def test_is_derived_metric_property_false_for_regular_metric(self):
        """Test that is_derived_metric returns False for a regular metric."""
        # Create mock parent (transform node)
        parent = Node(name="default.transform", type=NodeType.TRANSFORM)

        # Create metric with transform parent
        metric_node = Node(name="default.metric", type=NodeType.METRIC)
        metric_rev = NodeRevision(
            node=metric_node,
            name="default.metric",
            type=NodeType.METRIC,
            version="1",
            query="SELECT SUM(amount) FROM default.transform",
            parents=[parent],
        )

        assert metric_rev.is_derived_metric is False
        assert metric_rev.metric_parents == []
        assert metric_rev.non_metric_parents == [parent]

    def test_is_derived_metric_property_true_for_derived_metric(self):
        """Test that is_derived_metric returns True when parent is a metric."""
        # Create base metric
        base_metric = Node(name="default.revenue", type=NodeType.METRIC)

        # Create derived metric with metric parent (no FROM clause)
        derived_node = Node(name="default.wow_revenue", type=NodeType.METRIC)
        derived_rev = NodeRevision(
            node=derived_node,
            name="default.wow_revenue",
            type=NodeType.METRIC,
            version="1",
            query="SELECT LAG(default.revenue, 1) OVER (ORDER BY week)",
            parents=[base_metric],
        )

        assert derived_rev.is_derived_metric is True
        assert derived_rev.metric_parents == [base_metric]
        assert derived_rev.non_metric_parents == []

    def test_is_derived_metric_with_mixed_parents(self):
        """Test is_derived_metric with both metric and non-metric parents."""
        # Create transform and metric parents
        transform = Node(name="default.transform", type=NodeType.TRANSFORM)
        base_metric = Node(name="default.revenue", type=NodeType.METRIC)

        # Create metric with mixed parents
        metric_node = Node(name="default.mixed", type=NodeType.METRIC)
        metric_rev = NodeRevision(
            node=metric_node,
            name="default.mixed",
            type=NodeType.METRIC,
            version="1",
            query="SELECT SUM(amount) + default.revenue FROM default.transform",
            parents=[transform, base_metric],
        )

        assert metric_rev.is_derived_metric is True
        assert metric_rev.metric_parents == [base_metric]
        assert metric_rev.non_metric_parents == [transform]

    def test_is_derived_metric_false_for_non_metric_nodes(self):
        """Test that is_derived_metric is False for non-metric node types."""
        transform_node = Node(name="default.transform", type=NodeType.TRANSFORM)
        transform_rev = NodeRevision(
            node=transform_node,
            name="default.transform",
            type=NodeType.TRANSFORM,
            version="1",
            query="SELECT * FROM source",
            parents=[],
        )

        assert transform_rev.is_derived_metric is False


class TestExtractDependenciesForDerivedMetrics:
    """
    Tests to verify that extract_dependencies correctly finds metric references
    in derived metric queries (which have no FROM clause).
    """

    @pytest.mark.asyncio
    async def test_extract_dependencies_finds_table_refs_in_base_metric(self):
        """
        Base metrics have a FROM clause, so extract_dependencies should find Table refs.
        """
        # Base metric query with FROM clause
        query = parse("SELECT SUM(amount) FROM default.orders")

        # Find all Table nodes
        tables = list(query.find_all(Table))

        assert len(tables) == 1
        assert tables[0].identifier(quotes=False) == "default.orders"

    @pytest.mark.asyncio
    async def test_extract_dependencies_finds_column_refs_in_derived_metric(self):
        """
        Derived metrics have no FROM clause - metric references are Column nodes.
        extract_dependencies should find these Column references.
        """
        # Derived metric query - NO FROM clause, metrics are column references
        query = parse("SELECT default.metric_a / default.metric_b")

        # Mock session and context
        mock_session = AsyncMock()
        mock_context = CompileContext(session=mock_session, exception=DJException())

        # Mock get_dj_node to return mock nodes
        mock_metric_a = MagicMock()
        mock_metric_a.name = "default.metric_a"
        mock_metric_b = MagicMock()
        mock_metric_b.name = "default.metric_b"

        # Patch get_dj_node to return our mock nodes
        original_get_dj_node = ast_module.get_dj_node

        async def mock_get_dj_node(session, name, types):
            if name == "default.metric_a":
                return mock_metric_a
            elif name == "default.metric_b":
                return mock_metric_b
            return None

        ast_module.get_dj_node = mock_get_dj_node

        try:
            # Call extract_dependencies
            deps, danglers = await query.extract_dependencies(mock_context)

            # Should find both metric references
            dep_names = [d.name for d in deps.keys()]
            assert "default.metric_a" in dep_names
            assert "default.metric_b" in dep_names
            assert len(danglers) == 0
        finally:
            # Restore original function
            ast_module.get_dj_node = original_get_dj_node

    @pytest.mark.asyncio
    async def test_extract_dependencies_handles_unknown_refs_in_derived_metric(self):
        """
        For derived metrics with unknown references, they should appear in danglers.
        """
        # Derived metric query with unknown references
        query = parse("SELECT unknown.metric_x / unknown.metric_y")

        # Mock session and context
        mock_session = AsyncMock()
        mock_context = CompileContext(session=mock_session, exception=DJException())

        # Patch get_dj_node to raise DJErrorException (node not found)
        original_get_dj_node = ast_module.get_dj_node

        async def mock_get_dj_node(session, name, types):
            # Simulate node not found - raises exception like the real function
            raise DJErrorException(
                DJError(code=ErrorCode.UNKNOWN_NODE, message=f"Node {name} not found"),
            )

        ast_module.get_dj_node = mock_get_dj_node

        try:
            # Call extract_dependencies
            deps, danglers = await query.extract_dependencies(mock_context)

            # Should have no deps but unknown refs in danglers
            assert len(deps) == 0
            assert "unknown.metric_x" in danglers
            assert "unknown.metric_y" in danglers
        finally:
            # Restore original function
            ast_module.get_dj_node = original_get_dj_node

    @pytest.mark.asyncio
    async def test_get_parent_query_finds_query_for_column(self):
        """
        Test that _get_parent_query correctly finds the parent Query node.
        """
        # Derived metric query - no FROM
        query = parse("SELECT default.metric_a / default.metric_b")

        # Verify no FROM clause
        assert query.select.from_ is None

        # Find columns and check parent traversal
        columns = list(query.find_all(Column))
        assert len(columns) >= 2  # At least metric_a and metric_b

        for col in columns:
            if col.namespace:
                # Check _get_parent_query works
                parent_query = col._get_parent_query()
                assert parent_query is not None, (
                    f"Column {col} should have parent Query"
                )
                assert isinstance(parent_query, Query)
                assert parent_query.select.from_ is None

    @pytest.mark.asyncio
    async def test_column_compile_resolves_metric_reference_type(self):
        """
        When a Column references a metric (has namespace, no table source),
        Column.compile should resolve the metric and get its output type.
        """
        # Derived metric query
        query = parse("SELECT default.metric_a + default.metric_b")

        # Mock session and context
        mock_session = AsyncMock()
        mock_context = CompileContext(session=mock_session, exception=DJException())

        # Mock metric nodes with columns that have types
        mock_col_a = MagicMock()
        mock_col_a.type = IntegerType()
        mock_metric_a = MagicMock()
        mock_metric_a.columns = [mock_col_a]

        mock_col_b = MagicMock()
        mock_col_b.type = IntegerType()
        mock_metric_b = MagicMock()
        mock_metric_b.columns = [mock_col_b]

        # Patch get_dj_node
        original_get_dj_node = ast_module.get_dj_node

        async def mock_get_dj_node(session, name, types):
            if name == "default.metric_a":
                return mock_metric_a
            elif name == "default.metric_b":
                return mock_metric_b
            return None

        ast_module.get_dj_node = mock_get_dj_node

        try:
            # Compile the query
            await query.compile(mock_context)

            # Find the columns in the query
            columns = list(query.find_all(Column))

            # Each column should have been compiled and have a type
            for col in columns:
                if col.namespace:
                    assert col.is_compiled()
                    assert col._type is not None
                    assert isinstance(col._type, IntegerType)
        finally:
            ast_module.get_dj_node = original_get_dj_node


@pytest.mark.asyncio
class TestColumnCompileWithRealNodes:
    """
    Tests for Column.compile metric and dimension resolution using real database objects.

    These tests create actual Node/NodeRevision objects in the database and test
    the Column.compile code path that resolves metric references and dimension
    attributes in derived metric queries.
    """

    @pytest.fixture
    async def column_compile_test_graph(
        self,
        session: AsyncSession,
        current_user: User,
    ):
        """
        Creates a test graph with metrics, dimensions, and source nodes for testing
        Column.compile's metric and dimension reference resolution.

        Graph structure:
        - coltest.source (SOURCE) with columns: id, amount, customer_id
        - coltest.customer (DIMENSION) with columns: id, name, email
        - coltest.revenue (METRIC) -> parent: coltest.source
        - coltest.orders (METRIC) -> parent: coltest.source
        """
        # Create source node
        source = Node(
            name="coltest.source",
            type=NodeType.SOURCE,
            current_version="1",
            created_by_id=current_user.id,
        )
        source_rev = NodeRevision(
            node=source,
            name=source.name,
            type=source.type,
            version="1",
            display_name="Source",
            created_by_id=current_user.id,
            columns=[
                DBColumn(name="id", type=IntegerType(), order=0),
                DBColumn(name="amount", type=IntegerType(), order=1),
                DBColumn(name="customer_id", type=IntegerType(), order=2),
            ],
        )
        source.current = source_rev

        # Create dimension node
        customer_dim = Node(
            name="coltest.customer",
            type=NodeType.DIMENSION,
            current_version="1",
            created_by_id=current_user.id,
        )
        customer_dim_rev = NodeRevision(
            node=customer_dim,
            name=customer_dim.name,
            type=customer_dim.type,
            version="1",
            display_name="Customer",
            created_by_id=current_user.id,
            columns=[
                DBColumn(name="id", type=IntegerType(), order=0),
                DBColumn(name="name", type=StringType(), order=1),
                DBColumn(name="email", type=StringType(), order=2),
            ],
        )
        customer_dim.current = customer_dim_rev

        session.add_all([source, source_rev, customer_dim, customer_dim_rev])
        await session.flush()

        # Create base metric: revenue
        revenue_metric = Node(
            name="coltest.revenue",
            type=NodeType.METRIC,
            current_version="1",
            created_by_id=current_user.id,
        )
        revenue_metric_rev = NodeRevision(
            node=revenue_metric,
            name=revenue_metric.name,
            type=revenue_metric.type,
            version="1",
            display_name="Revenue",
            query="SELECT SUM(amount) FROM coltest.source",
            parents=[source],
            created_by_id=current_user.id,
            columns=[DBColumn(name="coltest_DOT_revenue", type=IntegerType(), order=0)],
        )
        revenue_metric.current = revenue_metric_rev

        # Create base metric: orders
        orders_metric = Node(
            name="coltest.orders",
            type=NodeType.METRIC,
            current_version="1",
            created_by_id=current_user.id,
        )
        orders_metric_rev = NodeRevision(
            node=orders_metric,
            name=orders_metric.name,
            type=orders_metric.type,
            version="1",
            display_name="Orders",
            query="SELECT COUNT(*) FROM coltest.source",
            parents=[source],
            created_by_id=current_user.id,
            columns=[DBColumn(name="coltest_DOT_orders", type=IntegerType(), order=0)],
        )
        orders_metric.current = orders_metric_rev

        session.add_all(
            [
                revenue_metric,
                revenue_metric_rev,
                orders_metric,
                orders_metric_rev,
            ],
        )
        await session.flush()

        return {
            "source": source,
            "customer_dim": customer_dim,
            "revenue_metric": revenue_metric,
            "orders_metric": orders_metric,
        }

    @pytest.mark.asyncio
    async def test_column_compile_resolves_metric_reference(
        self,
        session: AsyncSession,
        column_compile_test_graph,
    ):
        """
        Test that Column.compile resolves a metric reference (coltest.revenue)
        and gets its output type.
        """
        # Parse a derived metric query that references base metrics
        query = parse("SELECT coltest.revenue + coltest.orders")
        ctx = CompileContext(session=session, exception=DJException())

        await query.compile(ctx)

        # Find columns with namespace (metric references)
        columns = [c for c in query.find_all(Column) if c.namespace]
        assert len(columns) == 2

        # Both should be compiled with IntegerType
        for col in columns:
            assert col.is_compiled(), f"Column {col} should be compiled"
            assert isinstance(col._type, IntegerType), (
                f"Column {col} should have IntegerType"
            )

    @pytest.mark.asyncio
    async def test_column_compile_resolves_dimension_attribute(
        self,
        session: AsyncSession,
        column_compile_test_graph,
    ):
        """
        Test that Column.compile resolves a dimension attribute reference
        (coltest.customer.name) and gets the column's type.
        """
        # Parse a query referencing a dimension attribute (no FROM clause)
        query = parse("SELECT coltest.customer.name")
        ctx = CompileContext(session=session, exception=DJException())

        await query.compile(ctx)

        # Find the column with namespace
        columns = [c for c in query.find_all(Column) if c.namespace]
        assert len(columns) == 1

        col = columns[0]
        assert col.is_compiled()
        assert isinstance(col._type, StringType)

    @pytest.mark.asyncio
    async def test_column_compile_metric_ratio_expression(
        self,
        session: AsyncSession,
        column_compile_test_graph,
    ):
        """
        Test that Column.compile handles a derived metric expression with division.
        """
        # Parse a ratio metric query
        query = parse("SELECT coltest.revenue / coltest.orders")
        ctx = CompileContext(session=session, exception=DJException())

        await query.compile(ctx)

        # Both metric references should be resolved
        columns = [c for c in query.find_all(Column) if c.namespace]
        assert len(columns) == 2
        for col in columns:
            assert col.is_compiled()

    @pytest.mark.asyncio
    async def test_column_compile_nonexistent_metric_errors(
        self,
        session: AsyncSession,
        column_compile_test_graph,
    ):
        """
        Test that Column.compile adds an error when referencing a non-existent metric.
        """
        # Parse a query referencing a metric that doesn't exist
        query = parse("SELECT coltest.nonexistent_metric")
        ctx = CompileContext(session=session, exception=DJException())

        await query.compile(ctx)

        # Should have an error
        assert len(ctx.exception.errors) > 0
        error_messages = [e.message for e in ctx.exception.errors]
        assert any("does not exist" in msg for msg in error_messages)

    @pytest.mark.asyncio
    async def test_column_compile_nonexistent_dimension_column_errors(
        self,
        session: AsyncSession,
        column_compile_test_graph,
    ):
        """
        Test that Column.compile adds an error when referencing a non-existent
        column on an existing dimension.
        """
        # Parse a query referencing a column that doesn't exist on the dimension
        query = parse("SELECT coltest.customer.nonexistent_column")
        ctx = CompileContext(session=session, exception=DJException())

        await query.compile(ctx)

        # Should have an error
        assert len(ctx.exception.errors) > 0
        error_messages = [e.message for e in ctx.exception.errors]
        assert any("does not exist" in msg for msg in error_messages)


@pytest.mark.integration
class TestDerivedMetricsIntegration:
    """
    Integration tests for derived metrics using the canonical DERIVED_METRICS example set.

    This test class uses a self-contained schema with:
    - orders_source (fact) -> dimensions: date, customer
    - events_source (fact) -> dimensions: date, customer
    - inventory_source (fact) -> dimensions: warehouse (NO overlap with orders/events)

    Patterns tested:
    1. Same-parent ratio: revenue_per_order (revenue / orders) - both from orders_source
    2. Cross-fact ratio with shared dims: revenue_per_page_view (revenue / page_views)
    3. Period-over-period: wow_revenue_change, mom_revenue_change (LAG on base metric)
    4. Failure case: cross-fact with NO shared dimensions (should fail validation)
    """

    @pytest.mark.asyncio
    async def test_create_same_parent_ratio_metric(
        self,
        client_example_loader: Callable[
            [Optional[List[str]]],
            Coroutine[Any, Any, AsyncClient],
        ],
    ):
        """Test creating a derived metric that is a ratio of two base metrics from same parent."""
        client = await client_example_loader(["DERIVED_METRICS"])

        # Verify the derived metric was created
        response = await client.get("/nodes/default.dm_revenue_per_order")
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "default.dm_revenue_per_order"
        assert data["type"] == "metric"

        # Verify it references the base metrics as parents
        parents = [p["name"] for p in data["parents"]]
        assert "default.dm_revenue" in parents
        assert "default.dm_orders" in parents

    @pytest.mark.asyncio
    async def test_create_cross_fact_ratio_metric_with_shared_dimensions(
        self,
        client_example_loader: Callable[
            [Optional[List[str]]],
            Coroutine[Any, Any, AsyncClient],
        ],
    ):
        """Test creating a derived metric from metrics in different fact tables with shared dims."""
        client = await client_example_loader(["DERIVED_METRICS"])

        # Verify the cross-fact metric was created
        response = await client.get("/nodes/default.dm_revenue_per_page_view")
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "default.dm_revenue_per_page_view"
        assert data["type"] == "metric"

        # Verify it references metrics from different parents
        parents = [p["name"] for p in data["parents"]]
        assert "default.dm_revenue" in parents
        assert "default.dm_page_views" in parents

    @pytest.mark.asyncio
    async def test_create_period_over_period_wow_metric(
        self,
        client_example_loader: Callable[
            [Optional[List[str]]],
            Coroutine[Any, Any, AsyncClient],
        ],
    ):
        """Test creating a week-over-week derived metric using LAG window function."""
        client = await client_example_loader(["DERIVED_METRICS"])

        # Verify the WoW metric was created
        response = await client.get("/nodes/default.dm_wow_revenue_change")
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "default.dm_wow_revenue_change"
        assert data["type"] == "metric"

        # Verify it has the base metric as parent
        parents = [p["name"] for p in data["parents"]]
        assert "default.dm_revenue" in parents

    @pytest.mark.asyncio
    async def test_create_period_over_period_mom_metric(
        self,
        client_example_loader: Callable[
            [Optional[List[str]]],
            Coroutine[Any, Any, AsyncClient],
        ],
    ):
        """Test creating a month-over-month derived metric using LAG window function."""
        client = await client_example_loader(["DERIVED_METRICS"])

        # Verify the MoM metric was created
        response = await client.get("/nodes/default.dm_mom_revenue_change")
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "default.dm_mom_revenue_change"
        assert data["type"] == "metric"

        # Verify it has the base metric as parent
        parents = [p["name"] for p in data["parents"]]
        assert "default.dm_revenue" in parents

    @pytest.mark.asyncio
    async def test_derived_metric_upstream_nodes(
        self,
        client_example_loader: Callable[
            [Optional[List[str]]],
            Coroutine[Any, Any, AsyncClient],
        ],
    ):
        """Test that derived metrics have correct upstream lineage."""
        client = await client_example_loader(["DERIVED_METRICS"])

        # Get upstream nodes for the derived metric
        response = await client.get("/nodes/default.dm_revenue_per_order/upstream/")
        assert response.status_code == 200
        upstream = response.json()
        upstream_names = [n["name"] for n in upstream]

        # Should include both base metrics
        assert "default.dm_revenue" in upstream_names
        assert "default.dm_orders" in upstream_names
        # Should also include the ultimate parent (orders_source)
        assert "default.orders_source" in upstream_names

    @pytest.mark.asyncio
    async def test_derived_metric_downstream_from_base(
        self,
        client_example_loader: Callable[
            [Optional[List[str]]],
            Coroutine[Any, Any, AsyncClient],
        ],
    ):
        """Test that base metrics show derived metrics as downstream."""
        client = await client_example_loader(["DERIVED_METRICS"])

        # Get downstream nodes for a base metric
        response = await client.get("/nodes/default.dm_revenue/downstream/")
        assert response.status_code == 200
        downstream = response.json()
        downstream_names = [n["name"] for n in downstream]

        # Should include derived metrics that reference it
        assert "default.dm_revenue_per_order" in downstream_names
        assert "default.dm_revenue_per_page_view" in downstream_names
        assert "default.dm_wow_revenue_change" in downstream_names
        assert "default.dm_mom_revenue_change" in downstream_names

    @pytest.mark.asyncio
    async def test_same_parent_derived_metric_dimensions(
        self,
        client_example_loader: Callable[
            [Optional[List[str]]],
            Coroutine[Any, Any, AsyncClient],
        ],
    ):
        """Test that derived metrics from same parent have same dimensions as base metrics."""
        client = await client_example_loader(["DERIVED_METRICS"])

        # Get dimensions for base metric (revenue from orders_source)
        response = await client.get("/nodes/default.dm_revenue/dimensions/")
        assert response.status_code == 200
        base_dims = {d["name"] for d in response.json()}

        # Get dimensions for derived metric (ratio of two metrics from same parent)
        response = await client.get("/nodes/default.dm_revenue_per_order/dimensions/")
        assert response.status_code == 200
        derived_dims = {d["name"] for d in response.json()}

        # Derived metric from same parent should have same dimensions
        # (intersection of identical sets = the same set)
        assert derived_dims == base_dims

    @pytest.mark.asyncio
    async def test_cross_fact_derived_metric_dimensions_intersection(
        self,
        client_example_loader: Callable[
            [Optional[List[str]]],
            Coroutine[Any, Any, AsyncClient],
        ],
    ):
        """Test that cross-fact derived metrics have intersection of parent dimensions."""
        client = await client_example_loader(["DERIVED_METRICS"])

        # Get dimensions for orders-based metric (revenue)
        response = await client.get("/nodes/default.dm_revenue/dimensions/")
        assert response.status_code == 200
        orders_dims = {d["name"] for d in response.json()}

        # Get dimensions for events-based metric (page_views)
        response = await client.get("/nodes/default.dm_page_views/dimensions/")
        assert response.status_code == 200
        events_dims = {d["name"] for d in response.json()}

        # Get dimensions for cross-fact derived metric (revenue_per_page_view)
        response = await client.get(
            "/nodes/default.dm_revenue_per_page_view/dimensions/",
        )
        assert response.status_code == 200
        derived_dims = {d["name"] for d in response.json()}

        # Cross-fact derived metric should have intersection of dimensions
        # Both orders and events share date and customer dimensions
        expected_intersection = orders_dims & events_dims
        assert derived_dims == expected_intersection

        # Verify the shared dimensions are present
        assert len(derived_dims) > 0, "Should have at least some shared dimensions"

    @pytest.mark.asyncio
    async def test_cross_fact_no_shared_dimensions_fails(
        self,
        client_example_loader: Callable[
            [Optional[List[str]]],
            Coroutine[Any, Any, AsyncClient],
        ],
    ):
        """
        Test that creating a derived metric from two fact tables with NO shared
        dimensions fails validation.

        orders_source dims: date, customer
        inventory_source dims: warehouse (no overlap!)
        """
        client = await client_example_loader(["DERIVED_METRICS"])

        # Try to create a derived metric combining revenue (orders) and total_inventory (inventory)
        # These have NO shared dimensions - orders has date/customer, inventory has warehouse
        response = await client.post(
            "/nodes/metric/",
            json={
                "name": "default.invalid_cross_fact_metric",
                "description": "This should fail - no shared dimensions",
                "query": "SELECT default.dm_revenue / NULLIF(default.dm_total_inventory, 0)",
                "mode": "published",
            },
        )

        # This should fail because there are no shared dimensions
        # between orders_source (date, customer) and inventory_source (warehouse)
        assert response.status_code in (400, 422), (
            f"Expected failure for cross-fact metric with no shared dims, "
            f"got {response.status_code}: {response.json()}"
        )

    @pytest.mark.asyncio
    async def test_nested_derived_metric_succeeds(
        self,
        client_example_loader: Callable[
            [Optional[List[str]]],
            Coroutine[Any, Any, AsyncClient],
        ],
    ):
        """
        Test that creating a derived metric that references another derived metric succeeds.
        Multi-level derived metrics are now supported - derived metrics can reference other
        derived metrics, and SQL generation will recursively expand them to base metrics.
        """
        client = await client_example_loader(["DERIVED_METRICS"])

        # Create a derived metric that references revenue_per_order (which is derived)
        response = await client.post(
            "/nodes/metric/",
            json={
                "name": "default.nested_derived_metric",
                "description": "Nested derived metric - references another derived metric",
                "query": "SELECT default.dm_revenue_per_order * 2",
                "mode": "published",
            },
        )

        # This should succeed - nested derived metrics are now supported
        assert response.status_code in (200, 201), (
            f"Expected success for nested derived metric, "
            f"got {response.status_code}: {response.json()}"
        )
