"""Tests for build_v3 helper functions."""

from unittest.mock import MagicMock

from datajunction_server.construction.build_v3.cte import (
    filter_cte_projection,
    flatten_inner_ctes,
    get_column_full_name,
    get_table_references_from_ast,
    rewrite_table_references,
    topological_sort_nodes,
)
from datajunction_server.construction.build_v3.decomposition import (
    analyze_grain_groups,
    build_component_expression,
    get_native_grain,
)
from datajunction_server.construction.build_v3.dimensions import (
    parse_dimension_ref,
)
from datajunction_server.construction.build_v3.filters import (
    combine_filters,
    parse_filter,
    resolve_filter_references,
)
from datajunction_server.construction.build_v3.utils import (
    amenable_name,
    get_cte_name,
    get_column_type,
    get_short_name,
    make_column_ref,
    make_name,
)
from datajunction_server.construction.build_v3.utils import (
    extract_columns_from_expression,
)
from datajunction_server.construction.build_v3.materialization import (
    get_materialized_table_parts,
    get_physical_table_name,
    get_table_reference_parts_with_materialization,
    has_available_materialization,
)
from datajunction_server.construction.build_v3.types import (
    MetricGroup,
    DecomposedMetricInfo,
)
from datajunction_server.models.decompose import MetricComponent, Aggregability
from datajunction_server.models.decompose import AggregationRule
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.sql.parsing import ast
from datajunction_server.models.node import NodeType


class TestDimensionRefParsing:
    """Tests for dimension reference parsing."""

    def test_simple_dimension_ref(self):
        """Test parsing a simple dimension reference."""
        ref = parse_dimension_ref("v3.customer.name")
        assert ref.node_name == "v3.customer"
        assert ref.column_name == "name"
        assert ref.role is None

    def test_dimension_ref_with_role(self):
        """Test parsing a dimension reference with role."""
        ref = parse_dimension_ref("v3.date.month[order]")
        assert ref.node_name == "v3.date"
        assert ref.column_name == "month"
        assert ref.role == "order"

    def test_dimension_ref_with_multi_hop_role(self):
        """Test parsing a dimension reference with multi-hop role."""
        ref = parse_dimension_ref("v3.date.month[customer->registration]")
        assert ref.node_name == "v3.date"
        assert ref.column_name == "month"
        assert ref.role == "customer->registration"

    def test_dimension_ref_with_deep_role_path(self):
        """Test parsing dimension reference with deep role path."""
        ref = parse_dimension_ref("v3.location.country[customer->home]")
        assert ref.node_name == "v3.location"
        assert ref.column_name == "country"
        assert ref.role == "customer->home"

    def test_dimension_ref_just_column_name(self):
        """Test parsing a bare column name (no node prefix)."""
        ref = parse_dimension_ref("status")
        assert ref.node_name == ""
        assert ref.column_name == "status"
        assert ref.role is None


class TestMakeColumnRef:
    """Tests for make_column_ref function."""

    def test_column_with_table_alias(self):
        """Test creating a column reference with table alias."""
        col = make_column_ref("status", "t1")
        assert str(col) == "t1.status"

    def test_column_without_table_alias(self):
        """Test creating a column reference without table alias."""
        col = make_column_ref("status")
        assert str(col) == "status"


class TestNameHelpers:
    """Tests for name manipulation helper functions."""

    def test_get_short_name(self):
        """Test getting short name from fully qualified name."""
        assert get_short_name("v3.order_details") == "order_details"
        assert get_short_name("catalog.schema.table") == "table"
        assert get_short_name("simple") == "simple"

    def test_amenable_name(self):
        """Test converting node name to SQL-safe name."""
        assert amenable_name("v3.order_details") == "v3_order_details"
        assert amenable_name("my.node.name") == "my_node_name"

    def test_get_cte_name(self):
        """Test generating CTE name from node name."""
        assert get_cte_name("v3.order_details") == "v3_order_details"

    def test_make_name_simple(self):
        """Test make_name with simple name."""
        name = make_name("table")
        assert str(name) == "table"

    def test_make_name_dotted(self):
        """Test make_name with dotted name."""
        name = make_name("catalog.schema.table")
        assert str(name) == "catalog.schema.table"


class TestColumnTypeHelpers:
    """Tests for column type helper functions."""

    def test_get_column_type_found(self):
        """Test getting column type when column exists."""
        mock_col = MagicMock()
        mock_col.name = "status"
        mock_col.type = "varchar"

        mock_rev = MagicMock()
        mock_rev.columns = [mock_col]

        mock_node = MagicMock()
        mock_node.current = mock_rev

        result = get_column_type(mock_node, "status")
        assert result == "varchar"

    def test_get_column_type_not_found(self):
        """Test getting column type when column doesn't exist."""
        mock_col = MagicMock()
        mock_col.name = "other"
        mock_col.type = "varchar"

        mock_rev = MagicMock()
        mock_rev.columns = [mock_col]

        mock_node = MagicMock()
        mock_node.current = mock_rev

        result = get_column_type(mock_node, "missing")
        assert result == "string"

    def test_get_column_type_no_current(self):
        """Test getting column type when node has no current revision."""
        mock_node = MagicMock()
        mock_node.current = None

        result = get_column_type(mock_node, "status")
        assert result == "string"


class TestNativeGrain:
    """Tests for get_native_grain function."""

    def test_native_grain_with_pk(self):
        """Test getting native grain with primary key columns."""
        mock_col1 = MagicMock()
        mock_col1.name = "order_id"
        mock_col1.has_primary_key_attribute = True

        mock_col2 = MagicMock()
        mock_col2.name = "status"
        mock_col2.has_primary_key_attribute = False

        mock_rev = MagicMock()
        mock_rev.columns = [mock_col1, mock_col2]

        mock_node = MagicMock()
        mock_node.current = mock_rev

        result = get_native_grain(mock_node)
        assert result == ["order_id"]

    def test_native_grain_no_pk(self):
        """Test getting native grain with no primary key columns."""
        mock_col = MagicMock()
        mock_col.name = "status"
        mock_col.has_primary_key_attribute = False

        mock_rev = MagicMock()
        mock_rev.columns = [mock_col]

        mock_node = MagicMock()
        mock_node.current = mock_rev

        result = get_native_grain(mock_node)
        assert result == []

    def test_native_grain_no_current(self):
        """Test getting native grain when node has no current revision."""
        mock_node = MagicMock()
        mock_node.current = None

        result = get_native_grain(mock_node)
        assert result == []


class TestPhysicalTableHelpers:
    """Tests for physical table name helpers."""

    def test_get_physical_table_name_source(self):
        """Test getting physical table name for source node."""
        mock_catalog = MagicMock()
        mock_catalog.name = "default"

        mock_rev = MagicMock()
        mock_rev.catalog = mock_catalog
        mock_rev.schema_ = "v3"
        mock_rev.table = "orders"

        mock_node = MagicMock()
        mock_node.type = NodeType.SOURCE
        mock_node.current = mock_rev

        result = get_physical_table_name(mock_node)
        assert result == "default.v3.orders"

    def test_get_physical_table_name_transform(self):
        """Test getting physical table name for transform node."""
        mock_node = MagicMock()
        mock_node.type = NodeType.TRANSFORM
        mock_node.current = MagicMock()

        result = get_physical_table_name(mock_node)
        assert result is None

    def test_get_physical_table_name_no_current(self):
        """Test getting physical table name when node has no current revision."""
        mock_node = MagicMock()
        mock_node.current = None

        result = get_physical_table_name(mock_node)
        assert result is None


class TestTableReferenceHelpers:
    """Tests for table reference helpers."""

    def test_get_table_reference_source(self):
        """Test getting table reference for source node."""
        mock_catalog = MagicMock()
        mock_catalog.name = "default"

        mock_rev = MagicMock()
        mock_rev.catalog = mock_catalog
        mock_rev.schema_ = "v3"
        mock_rev.table = "orders"

        mock_node = MagicMock()
        mock_node.type = NodeType.SOURCE
        mock_node.name = "v3.src_orders"
        mock_node.current = mock_rev

        table_parts, is_physical = get_table_reference_parts_with_materialization(
            MagicMock(),
            mock_node,
        )
        assert table_parts == ["default", "v3", "orders"]
        assert is_physical is True

    def test_get_table_reference_transform(self):
        """Test getting table reference for transform node (returns CTE name)."""
        mock_node = MagicMock()
        mock_node.type = NodeType.TRANSFORM
        mock_node.name = "v3.order_details"
        mock_node.current = MagicMock()
        mock_node.current.availability = None  # No materialization

        mock_ctx = MagicMock()
        mock_ctx.use_materialized = True

        table_parts, is_physical = get_table_reference_parts_with_materialization(
            mock_ctx,
            mock_node,
        )
        assert table_parts == ["v3_order_details"]  # List with CTE name
        assert is_physical is False  # It's a CTE, not a physical table

    def test_get_table_reference_transform_materialized(self):
        """Test getting table reference for materialized transform node."""
        # Set up availability (materialization)
        mock_availability = MagicMock()
        mock_availability.catalog = "mat_catalog"
        mock_availability.schema_ = "mat_schema"
        mock_availability.table = "mat_table"
        mock_availability.is_available.return_value = True

        mock_node = MagicMock()
        mock_node.type = NodeType.TRANSFORM
        mock_node.name = "v3.order_details"
        mock_node.current = MagicMock()
        mock_node.current.availability = mock_availability

        mock_ctx = MagicMock()
        mock_ctx.use_materialized = True

        table_parts, is_physical = get_table_reference_parts_with_materialization(
            mock_ctx,
            mock_node,
        )
        assert table_parts == [
            "mat_catalog",
            "mat_schema",
            "mat_table",
        ]  # Physical table parts
        assert is_physical is True  # Using materialized table


class TestMaterializationHelpers:
    """Tests for materialization helper functions."""

    def test_has_available_materialization_true(self):
        """Test checking for available materialization when present."""
        mock_availability = MagicMock()
        mock_availability.catalog = "mat_catalog"
        mock_availability.schema_ = "mat_schema"
        mock_availability.table = "mat_table"
        mock_availability.is_available.return_value = (
            True  # Mock the is_available() call
        )

        mock_rev = MagicMock()
        mock_rev.availability = mock_availability

        mock_node = MagicMock()
        mock_node.current = mock_rev

        result = has_available_materialization(mock_node)
        assert result is True

    def test_has_available_materialization_false(self):
        """Test checking for available materialization when absent."""
        mock_rev = MagicMock()
        mock_rev.availability = None

        mock_node = MagicMock()
        mock_node.current = mock_rev

        result = has_available_materialization(mock_node)
        assert result is False

    def test_get_materialized_table_parts(self):
        """Test getting materialized table parts."""
        mock_availability = MagicMock()
        mock_availability.catalog = "mat_catalog"
        mock_availability.schema_ = "mat_schema"
        mock_availability.table = "mat_table"

        mock_rev = MagicMock()
        mock_rev.availability = mock_availability

        mock_node = MagicMock()
        mock_node.current = mock_rev

        result = get_materialized_table_parts(mock_node)
        assert result == ["mat_catalog", "mat_schema", "mat_table"]

    def test_get_materialized_table_parts_no_availability(self):
        """Test getting materialized table parts when not available."""
        mock_rev = MagicMock()
        mock_rev.availability = None

        mock_node = MagicMock()
        mock_node.current = mock_rev

        result = get_materialized_table_parts(mock_node)
        assert result is None


class TestASTHelpers:
    """Tests for AST manipulation helper functions."""

    def test_get_column_full_name(self):
        """Test extracting full column name from AST."""
        query = parse("SELECT t.column_name FROM table t")
        cols = list(query.find_all(ast.Column))
        assert len(cols) == 1
        result = get_column_full_name(cols[0])
        assert result == "t.column_name"

    def test_get_column_full_name_namespaced(self):
        """Test extracting full column name with namespace."""
        query = parse("SELECT v3.date.month FROM table")
        cols = list(query.find_all(ast.Column))
        assert len(cols) == 1
        result = get_column_full_name(cols[0])
        assert result == "v3.date.month"

    def test_extract_columns_from_expression(self):
        """Test extracting column names from expression."""
        query = parse("SELECT SUM(a + b * c) FROM table")
        expr = query.select.projection[0]
        cols = extract_columns_from_expression(expr)
        assert cols == {"a", "b", "c"}

    def test_get_table_references_from_ast(self):
        """Test extracting table references from query AST."""
        query = parse("SELECT * FROM a JOIN b ON a.id = b.id")
        refs = get_table_references_from_ast(query)
        assert "a" in refs
        assert "b" in refs


class TestFilterHelpers:
    """Tests for filter parsing and resolution helpers."""

    def test_parse_filter_simple(self):
        """Test parsing a simple filter expression."""
        result = parse_filter("status = 'active'")
        assert isinstance(result, ast.BinaryOp)
        assert str(result) == "status = 'active'"

    def test_parse_filter_comparison(self):
        """Test parsing a comparison filter."""
        result = parse_filter("amount >= 100")
        assert isinstance(result, ast.BinaryOp)

    def test_parse_filter_in_clause(self):
        """Test parsing an IN clause filter."""
        result = parse_filter("status IN ('active', 'pending')")
        assert str(result) == "status IN ('active', 'pending')"

    def test_combine_filters_empty(self):
        """Test combining empty filter list."""
        result = combine_filters([])
        assert result is None

    def test_combine_filters_single(self):
        """Test combining single filter."""
        f1 = parse_filter("status = 'active'")
        result = combine_filters([f1])
        assert str(result) == "status = 'active'"

    def test_combine_filters_multiple(self):
        """Test combining multiple filters with AND."""
        f1 = parse_filter("status = 'active'")
        f2 = parse_filter("amount >= 100")
        result = combine_filters([f1, f2])
        assert "AND" in str(result).upper()

    def test_resolve_filter_references(self):
        """Test resolving dimension references in filters."""
        filter_ast = parse_filter("v3_product_category = 'Electronics'")
        aliases = {"v3_product_category": "category"}
        result = resolve_filter_references(filter_ast, aliases, "t1")
        # The column should be resolved to use the alias
        assert result is not None


class TestBuildComponentExpression:
    """Tests for build_component_expression function."""

    def test_build_simple_sum(self):
        """Test building a simple SUM aggregation."""
        component = MetricComponent(
            name="revenue_sum",
            expression="line_total",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        )
        result = build_component_expression(component)
        assert isinstance(result, ast.Function)
        assert str(result).upper().startswith("SUM")

    def test_build_count(self):
        """Test building a COUNT aggregation."""
        component = MetricComponent(
            name="order_count",
            expression="order_id",
            aggregation="COUNT",
            rule=AggregationRule(type=Aggregability.FULL),
        )
        result = build_component_expression(component)
        assert isinstance(result, ast.Function)
        assert "COUNT" in str(result).upper()

    def test_build_no_aggregation(self):
        """Test building expression without aggregation."""
        component = MetricComponent(
            name="raw_value",
            expression="line_total",
            aggregation=None,
            rule=AggregationRule(type=Aggregability.NONE),
        )
        result = build_component_expression(component)
        assert isinstance(result, ast.Column)

    def test_build_template_aggregation(self):
        """Test building expression with template aggregation like SUM(POWER({}, 2))."""
        component = MetricComponent(
            name="sum_squared",
            expression="value",
            aggregation="SUM(POWER({}, 2))",
            rule=AggregationRule(type=Aggregability.FULL),
        )
        result = build_component_expression(component)
        # Should expand the template
        result_str = str(result).upper()
        assert "SUM" in result_str
        assert "POWER" in result_str


class TestTopologicalSortNodes:
    """Tests for topological_sort_nodes function."""

    def test_empty_node_names(self):
        """Test with empty node names set."""
        ctx = MagicMock()
        ctx.nodes = {}
        result = topological_sort_nodes(ctx, set())
        assert result == []

    def test_single_transform_node(self):
        """Test sorting a single transform node."""
        # Create a mock transform node
        transform_node = MagicMock()
        transform_node.name = "v3.my_transform"
        transform_node.type = NodeType.TRANSFORM
        transform_node.current = MagicMock()
        transform_node.current.query = "SELECT * FROM v3.source"

        # Mock the parsed query AST
        query_ast = parse("SELECT * FROM v3_source")

        ctx = MagicMock()
        ctx.nodes = {"v3.my_transform": transform_node}
        ctx.get_parsed_query.return_value = query_ast

        result = topological_sort_nodes(ctx, {"v3.my_transform"})
        assert len(result) == 1
        assert result[0].name == "v3.my_transform"

    def test_source_node_handling(self):
        """Test that SOURCE nodes have no dependencies."""
        source_node = MagicMock()
        source_node.name = "v3.source_table"
        source_node.type = NodeType.SOURCE

        ctx = MagicMock()
        ctx.nodes = {"v3.source_table": source_node}

        result = topological_sort_nodes(ctx, {"v3.source_table"})
        assert len(result) == 1
        assert result[0].name == "v3.source_table"

    def test_metric_node_skipped(self):
        """Test that METRIC nodes are skipped (handled separately)."""
        metric_node = MagicMock()
        metric_node.name = "v3.my_metric"
        metric_node.type = NodeType.METRIC

        ctx = MagicMock()
        ctx.nodes = {"v3.my_metric": metric_node}

        result = topological_sort_nodes(ctx, {"v3.my_metric"})
        # Metric nodes are skipped, so no result
        assert len(result) == 0

    def test_node_without_current(self):
        """Test handling a node without current revision."""
        node = MagicMock()
        node.name = "v3.broken"
        node.type = NodeType.TRANSFORM
        node.current = None

        ctx = MagicMock()
        ctx.nodes = {"v3.broken": node}

        result = topological_sort_nodes(ctx, {"v3.broken"})
        assert len(result) == 1  # Still included with empty deps

    def test_node_without_query(self):
        """Test handling a node without a query."""
        node = MagicMock()
        node.name = "v3.no_query"
        node.type = NodeType.TRANSFORM
        node.current = MagicMock()
        node.current.query = None

        ctx = MagicMock()
        ctx.nodes = {"v3.no_query": node}

        result = topological_sort_nodes(ctx, {"v3.no_query"})
        assert len(result) == 1  # Still included with empty deps

    def test_table_reference_extraction(self):
        """Test that table references are extracted correctly from SQL."""
        # Test various SQL patterns to see what table names are extracted
        # v3.node_a parses as namespace.table, str() returns "v3.node_a"
        test_cases = [
            ("SELECT * FROM v3_node_a", {"v3_node_a"}),
            ("SELECT * FROM v3.node_a", {"v3.node_a"}),
            ("SELECT * FROM v3.b JOIN v3.c ON 1=1", {"v3.b", "v3.c"}),
        ]
        for sql, expected in test_cases:
            query_ast = parse(sql)
            refs = get_table_references_from_ast(query_ast)
            assert refs == expected, f"SQL: {sql}, expected {expected}, got {refs}"

    def test_dependency_ordering(self):
        """Test that dependencies come before dependents."""
        # Create two transform nodes where B depends on A
        # Table references use namespace.name format (v3.node_a)
        node_a = MagicMock()
        node_a.name = "v3.node_a"
        node_a.type = NodeType.TRANSFORM
        node_a.current = MagicMock()
        node_a.current.query = "SELECT * FROM external_source"

        node_b = MagicMock()
        node_b.name = "v3.node_b"
        node_b.type = NodeType.TRANSFORM
        node_b.current = MagicMock()
        node_b.current.query = "SELECT * FROM v3.node_a"

        # Parse queries - v3.node_a parses as namespace.table
        query_a = parse("SELECT * FROM external_source")
        query_b = parse("SELECT * FROM v3.node_a")

        ctx = MagicMock()
        ctx.nodes = {"v3.node_a": node_a, "v3.node_b": node_b}

        def get_parsed_query(node):
            if node.name == "v3.node_a":
                return query_a
            return query_b

        ctx.get_parsed_query.side_effect = get_parsed_query

        result = topological_sort_nodes(ctx, {"v3.node_a", "v3.node_b"})

        # Node A should come before Node B since B depends on A
        assert len(result) == 2
        result_names = [n.name for n in result]
        assert result_names.index("v3.node_a") < result_names.index("v3.node_b")

    def test_diamond_dependency(self):
        """Test diamond-shaped dependency: D depends on B,C which both depend on A."""
        # A -> B -> D
        # A -> C -> D
        # Table references use namespace.name format (v3.a, v3.b, etc.)
        node_a = MagicMock()
        node_a.name = "v3.a"
        node_a.type = NodeType.TRANSFORM
        node_a.current = MagicMock()
        node_a.current.query = "SELECT 1"

        node_b = MagicMock()
        node_b.name = "v3.b"
        node_b.type = NodeType.TRANSFORM
        node_b.current = MagicMock()
        node_b.current.query = "SELECT * FROM v3.a"

        node_c = MagicMock()
        node_c.name = "v3.c"
        node_c.type = NodeType.TRANSFORM
        node_c.current = MagicMock()
        node_c.current.query = "SELECT * FROM v3.a"

        node_d = MagicMock()
        node_d.name = "v3.d"
        node_d.type = NodeType.TRANSFORM
        node_d.current = MagicMock()
        node_d.current.query = "SELECT * FROM v3.b JOIN v3.c"

        ctx = MagicMock()
        ctx.nodes = {
            "v3.a": node_a,
            "v3.b": node_b,
            "v3.c": node_c,
            "v3.d": node_d,
        }

        def get_parsed_query(node):
            queries = {
                "v3.a": parse("SELECT 1"),
                "v3.b": parse("SELECT * FROM v3.a"),
                "v3.c": parse("SELECT * FROM v3.a"),
                "v3.d": parse("SELECT * FROM v3.b JOIN v3.c ON 1=1"),
            }
            return queries[node.name]

        ctx.get_parsed_query.side_effect = get_parsed_query

        result = topological_sort_nodes(ctx, {"v3.a", "v3.b", "v3.c", "v3.d"})

        result_names = [n.name for n in result]
        assert len(result) == 4, f"Expected 4 nodes, got {result_names}"
        # A must come first (no dependencies)
        assert result_names[0] == "v3.a"
        # B and C must come before D (D depends on both)
        assert result_names.index("v3.b") < result_names.index("v3.d")
        assert result_names.index("v3.c") < result_names.index("v3.d")
        assert result_names.index("v3.c") < result_names.index("v3.d")

    def test_node_not_in_context(self):
        """Test handling when a node name is not found in ctx.nodes."""
        ctx = MagicMock()
        ctx.nodes = {}  # Empty - no nodes

        result = topological_sort_nodes(ctx, {"v3.missing_node"})
        assert result == []

    def test_parse_exception_handling(self):
        """Test that parse exceptions are handled gracefully."""
        node = MagicMock()
        node.name = "v3.bad_query"
        node.type = NodeType.TRANSFORM
        node.current = MagicMock()
        node.current.query = "INVALID SQL SYNTAX"

        ctx = MagicMock()
        ctx.nodes = {"v3.bad_query": node}
        # Simulate parse failure
        ctx.get_parsed_query.side_effect = Exception("Parse error")

        result = topological_sort_nodes(ctx, {"v3.bad_query"})
        # Should still return the node with empty dependencies
        assert len(result) == 1
        assert result[0].name == "v3.bad_query"

    def test_dependency_on_skipped_metric_node(self):
        """Test that dependencies on METRIC nodes (which are skipped) are handled.

        This tests the case where a transform references a metric node by name.
        The metric is skipped (not added to dependencies/dependents), so the
        'if dep in dependents' check returns False for that dependency.

        When a transform depends on a skipped node, it has an unresolvable
        dependency and won't appear in the sorted output (per the algorithm).
        """
        # Create a metric node that will be skipped
        # Use underscore format since that's what appears in SQL table references
        metric_node = MagicMock()
        metric_node.name = "v3_my_metric"
        metric_node.type = NodeType.METRIC

        # Create a transform that references the metric node name
        transform_node = MagicMock()
        transform_node.name = "v3_transform"
        transform_node.type = NodeType.TRANSFORM
        transform_node.current = MagicMock()
        transform_node.current.query = "SELECT * FROM v3_my_metric"

        # The parsed query will return v3_my_metric as a table reference
        query_ast = parse("SELECT * FROM v3_my_metric")

        ctx = MagicMock()
        ctx.nodes = {"v3_my_metric": metric_node, "v3_transform": transform_node}
        ctx.get_parsed_query.return_value = query_ast

        # Both nodes are in node_names, but metric will be skipped
        result = topological_sort_nodes(ctx, {"v3_my_metric", "v3_transform"})

        # The transform depends on the skipped metric, so it has an unresolvable
        # dependency and won't be returned (in_degree never reaches 0).
        # This tests that the 'if dep in dependents' check handles missing deps.
        assert len(result) == 0


class TestRewriteTableReferences:
    """Tests for rewrite_table_references function."""

    def test_rewrite_inner_cte_reference(self):
        """Test rewriting an inner CTE reference with prefixed name and alias."""
        query_ast = parse("SELECT * FROM base_cte")

        ctx = MagicMock()
        ctx.nodes = {}  # No node references

        cte_names = {}
        inner_cte_renames = {"base_cte": "prefix_base_cte"}

        result = rewrite_table_references(query_ast, ctx, cte_names, inner_cte_renames)

        # Should rename to prefixed name with alias to original
        table = list(result.find_all(ast.Table))[0]
        assert str(table.name) == "prefix_base_cte"
        assert str(table.alias) == "base_cte"

    def test_rewrite_inner_cte_preserves_existing_alias(self):
        """Test that existing aliases are preserved when rewriting inner CTEs."""
        query_ast = parse("SELECT * FROM base_cte AS b")

        ctx = MagicMock()
        ctx.nodes = {}

        cte_names = {}
        inner_cte_renames = {"base_cte": "prefix_base_cte"}

        result = rewrite_table_references(query_ast, ctx, cte_names, inner_cte_renames)

        # Should rename but keep existing alias
        table = list(result.find_all(ast.Table))[0]
        assert str(table.name) == "prefix_base_cte"
        assert str(table.alias) == "b"  # Original alias preserved

    def test_rewrite_source_node_to_physical_table(self):
        """Test rewriting a source node reference to physical table name."""
        query_ast = parse("SELECT * FROM v3_source")

        # Create a source node mock
        source_node = MagicMock()
        source_node.name = "v3.source"
        source_node.type = NodeType.SOURCE
        source_node.current = MagicMock()
        source_node.current.catalog = MagicMock()
        source_node.current.catalog.name = "catalog"
        source_node.current.schema_ = "schema"
        source_node.current.table = "source_table"

        ctx = MagicMock()
        ctx.nodes = {"v3_source": source_node}
        ctx.use_materialized = True

        cte_names = {}

        result = rewrite_table_references(query_ast, ctx, cte_names)

        # Should be rewritten to physical table name
        table = list(result.find_all(ast.Table))[0]
        assert "catalog" in str(table.name) or "source" in str(table.name).lower()

    def test_rewrite_transform_node_to_cte(self):
        """Test rewriting a transform node reference to CTE name."""
        query_ast = parse("SELECT * FROM v3_transform")

        # Create a transform node mock
        transform_node = MagicMock()
        transform_node.name = "v3.transform"
        transform_node.type = NodeType.TRANSFORM
        transform_node.current = MagicMock()
        transform_node.current.availability = None  # Not materialized

        ctx = MagicMock()
        ctx.nodes = {"v3_transform": transform_node}
        ctx.use_materialized = True

        cte_names = {"v3_transform": "v3_transform_cte"}

        result = rewrite_table_references(query_ast, ctx, cte_names)

        # Should be rewritten to CTE name
        table = list(result.find_all(ast.Table))[0]
        assert str(table.name) == "v3_transform_cte"

    def test_rewrite_materialized_node_to_physical_table(self):
        """Test rewriting a materialized node to its physical table name."""
        query_ast = parse("SELECT * FROM v3_materialized")

        # Create a materialized transform node mock
        mat_node = MagicMock()
        mat_node.name = "v3.materialized"
        mat_node.type = NodeType.TRANSFORM
        mat_node.current = MagicMock()

        # Mock availability state
        availability = MagicMock()
        availability.is_available.return_value = True
        availability.catalog = "mat_catalog"
        availability.schema_ = "mat_schema"
        availability.table = "mat_table"
        mat_node.current.availability = availability

        ctx = MagicMock()
        ctx.nodes = {"v3_materialized": mat_node}
        ctx.use_materialized = True

        cte_names = {}

        result = rewrite_table_references(query_ast, ctx, cte_names)

        # Should be rewritten to materialized physical table
        table = list(result.find_all(ast.Table))[0]
        table_str = str(table.name)
        assert "mat" in table_str.lower()

    def test_rewrite_unknown_table_unchanged(self):
        """Test that unknown table references are left unchanged."""
        query_ast = parse("SELECT * FROM unknown_table")

        ctx = MagicMock()
        ctx.nodes = {}  # No matching nodes

        cte_names = {}

        result = rewrite_table_references(query_ast, ctx, cte_names)

        # Should be unchanged
        table = list(result.find_all(ast.Table))[0]
        assert str(table.name) == "unknown_table"

    def test_rewrite_multiple_tables(self):
        """Test rewriting multiple table references in a single query."""
        query_ast = parse("SELECT * FROM v3_a JOIN v3_b ON v3_a.id = v3_b.id")

        node_a = MagicMock()
        node_a.name = "v3.a"
        node_a.type = NodeType.TRANSFORM
        node_a.current = MagicMock()
        node_a.current.availability = None

        node_b = MagicMock()
        node_b.name = "v3.b"
        node_b.type = NodeType.TRANSFORM
        node_b.current = MagicMock()
        node_b.current.availability = None

        ctx = MagicMock()
        ctx.nodes = {"v3_a": node_a, "v3_b": node_b}
        ctx.use_materialized = True

        cte_names = {"v3_a": "cte_a", "v3_b": "cte_b"}

        result = rewrite_table_references(query_ast, ctx, cte_names)

        # Both should be rewritten
        tables = list(result.find_all(ast.Table))
        table_names = [str(t.name) for t in tables]
        assert "cte_a" in table_names
        assert "cte_b" in table_names

    def test_rewrite_with_no_inner_cte_renames(self):
        """Test that None inner_cte_renames is handled correctly."""
        query_ast = parse("SELECT * FROM v3_transform")

        transform_node = MagicMock()
        transform_node.name = "v3.transform"
        transform_node.type = NodeType.TRANSFORM
        transform_node.current = MagicMock()
        transform_node.current.availability = None

        ctx = MagicMock()
        ctx.nodes = {"v3_transform": transform_node}
        ctx.use_materialized = True

        cte_names = {"v3_transform": "v3_cte"}

        # Pass None for inner_cte_renames
        result = rewrite_table_references(query_ast, ctx, cte_names, None)

        table = list(result.find_all(ast.Table))[0]
        assert str(table.name) == "v3_cte"


class TestFilterCteProjection:
    """Tests for filter_cte_projection function."""

    def test_filter_aliased_columns(self):
        """Test filtering columns that have aliases."""
        query_ast = parse("SELECT a AS col_a, b AS col_b, c AS col_c FROM t")

        result = filter_cte_projection(query_ast, {"col_a", "col_c"})

        # Should only keep col_a and col_c
        projection = result.select.projection
        assert len(projection) == 2
        aliases = [
            str(p.alias.name) for p in projection if hasattr(p, "alias") and p.alias
        ]
        assert "col_a" in aliases
        assert "col_c" in aliases
        assert "col_b" not in aliases

    def test_filter_unaliased_columns(self):
        """Test filtering columns without aliases."""
        query_ast = parse("SELECT col_a, col_b, col_c FROM t")

        result = filter_cte_projection(query_ast, {"col_a", "col_c"})

        # Should only keep col_a and col_c
        projection = result.select.projection
        assert len(projection) == 2
        names = [str(p.name.name) for p in projection if isinstance(p, ast.Column)]
        assert "col_a" in names
        assert "col_c" in names

    def test_filter_mixed_columns(self):
        """Test filtering mix of aliased and unaliased columns."""
        query_ast = parse("SELECT a AS alias_a, plain_b, c AS alias_c FROM t")

        result = filter_cte_projection(query_ast, {"alias_a", "plain_b"})

        projection = result.select.projection
        assert len(projection) == 2

    def test_filter_keeps_all_when_all_needed(self):
        """Test that all columns are kept when all are in the filter set."""
        query_ast = parse("SELECT a, b, c FROM t")

        result = filter_cte_projection(query_ast, {"a", "b", "c"})

        projection = result.select.projection
        assert len(projection) == 3

    def test_filter_empty_set_keeps_original(self):
        """Test that empty filter set keeps original projection."""
        query_ast = parse("SELECT a, b, c FROM t")
        original_len = len(query_ast.select.projection)

        result = filter_cte_projection(query_ast, set())

        # When everything is filtered, should keep original
        assert len(result.select.projection) == original_len

    def test_filter_column_with_table_prefix(self):
        """Test filtering columns with table prefixes."""
        query_ast = parse("SELECT t.a AS col_a, t.b AS col_b FROM t")

        result = filter_cte_projection(query_ast, {"col_a"})

        projection = result.select.projection
        assert len(projection) == 1

    def test_filter_expressions_kept(self):
        """Test that complex expressions (non-Column, non-Alias) are kept."""
        # Use a function call as expression - this hits the else branch
        query_ast = parse("SELECT COUNT(*) AS cnt, a AS col_a FROM t")

        result = filter_cte_projection(query_ast, {"col_a"})

        # col_a should be kept, COUNT(*) may or may not be kept depending on implementation
        projection = result.select.projection
        # At minimum col_a should be there
        assert any(
            hasattr(p, "alias") and p.alias and str(p.alias.name) == "col_a"
            for p in projection
        )

    def test_filter_star_expression(self):
        """Test handling of SELECT * (no specific columns)."""
        query_ast = parse("SELECT * FROM t")

        # SELECT * creates a Star node, not Column nodes
        result = filter_cte_projection(query_ast, {"a", "b"})

        # Star expressions should be kept (they fall into the else branch)
        assert len(result.select.projection) >= 1

    def test_filter_none_column_name(self):
        """Test handling when column name extraction returns None."""
        query_ast = parse("SELECT a AS col_a, b FROM t")

        # Filter for columns that exist
        result = filter_cte_projection(query_ast, {"col_a", "b"})

        projection = result.select.projection
        assert len(projection) == 2


class TestFlattenInnerCtes:
    """Tests for flatten_inner_ctes function."""

    def test_no_ctes_returns_empty(self):
        """Test that a query with no CTEs returns empty results."""
        query_ast = parse("SELECT * FROM t")

        extracted, renames = flatten_inner_ctes(query_ast, "outer_cte")

        assert extracted == []
        assert renames == {}

    def test_single_inner_cte(self):
        """Test extracting a single inner CTE."""
        query_ast = parse("WITH temp AS (SELECT 1 AS x) SELECT * FROM temp")

        extracted, renames = flatten_inner_ctes(query_ast, "v3_transform")

        # Should have one extracted CTE
        assert len(extracted) == 1
        assert extracted[0][0] == "v3_transform__temp"

        # Should have rename mapping
        assert "temp" in renames
        assert renames["temp"] == "v3_transform__temp"

        # Original query should have CTEs cleared
        assert query_ast.ctes == []

    def test_multiple_inner_ctes(self):
        """Test extracting multiple inner CTEs."""
        query_ast = parse("""
            WITH
                cte_a AS (SELECT 1 AS a),
                cte_b AS (SELECT 2 AS b)
            SELECT * FROM cte_a JOIN cte_b ON 1=1
        """)

        extracted, renames = flatten_inner_ctes(query_ast, "outer")

        # Should have two extracted CTEs
        assert len(extracted) == 2

        # Check names
        cte_names = [name for name, _ in extracted]
        assert "outer__cte_a" in cte_names
        assert "outer__cte_b" in cte_names

        # Check renames
        assert renames["cte_a"] == "outer__cte_a"
        assert renames["cte_b"] == "outer__cte_b"

    def test_nested_ctes_recursive_flattening(self):
        """Test that nested CTEs are recursively flattened."""
        # This is a query with a CTE that itself has CTEs
        # Note: Standard SQL doesn't allow this, but our AST might represent it
        query_ast = parse("""
            WITH outer_temp AS (
                SELECT * FROM source
            )
            SELECT * FROM outer_temp
        """)

        # Manually add nested CTE to test recursive behavior
        if query_ast.ctes:
            inner_cte = query_ast.ctes[0]
            # Create a nested CTE structure
            nested_cte = ast.Query(
                select=ast.Select(projection=[ast.Column(ast.Name("nested_col"))]),
            )
            nested_cte.alias = ast.Name("nested")
            inner_cte.ctes = [nested_cte]

        extracted, renames = flatten_inner_ctes(query_ast, "prefix")

        # Should include both the outer and nested CTEs
        assert len(extracted) >= 1

    def test_cte_without_alias_skipped(self):
        """Test that CTEs without aliases are handled gracefully."""
        query_ast = parse("WITH temp AS (SELECT 1) SELECT * FROM temp")

        # Manually remove alias to test edge case
        if query_ast.ctes:
            query_ast.ctes[0].alias = None

        extracted, renames = flatten_inner_ctes(query_ast, "outer")

        # Should handle gracefully (skip CTEs without aliases)
        # The original CTE will be skipped
        assert len(extracted) == 0
        assert len(renames) == 0

    def test_preserves_cte_query_content(self):
        """Test that extracted CTE query content is preserved."""
        query_ast = parse("""
            WITH data AS (
                SELECT id, name, value
                FROM source_table
                WHERE value > 10
            )
            SELECT * FROM data
        """)

        extracted, renames = flatten_inner_ctes(query_ast, "transform")

        # Check that the CTE content is preserved
        assert len(extracted) == 1
        cte_name, cte_query = extracted[0]
        assert cte_name == "transform__data"

        # The query should have a SELECT
        assert cte_query.select is not None

    def test_prefix_format(self):
        """Test that the prefix format uses double underscore separator."""
        query_ast = parse("WITH my_cte AS (SELECT 1) SELECT * FROM my_cte")

        extracted, renames = flatten_inner_ctes(query_ast, "v3_order_details")

        # Should use double underscore as separator
        assert extracted[0][0] == "v3_order_details__my_cte"
        assert "__" in extracted[0][0]


class TestAnalyzeGrainGroups:
    """Tests for analyze_grain_groups function."""

    def _make_component(
        self,
        name: str,
        aggregability: Aggregability,
        level: list[str] | None = None,
    ) -> MetricComponent:
        """Helper to create a MetricComponent with specified aggregability."""
        return MetricComponent(
            name=name,
            expression="col",
            aggregation="SUM",
            merge="SUM",
            rule=AggregationRule(type=aggregability, level=level),
        )

    def _make_metric_node(self, name: str) -> MagicMock:
        """Helper to create a mock metric node."""
        node = MagicMock()
        node.name = name
        node.type = NodeType.METRIC
        return node

    def _make_parent_node(
        self,
        name: str,
        pk_columns: list[str] | None = None,
    ) -> MagicMock:
        """Helper to create a mock parent node with optional PK columns."""
        node = MagicMock()
        node.name = name
        node.type = NodeType.TRANSFORM
        node.current = MagicMock()

        # Set up columns with primary key attribute
        columns = []
        for col_name in pk_columns or []:
            col = MagicMock()
            col.name = col_name
            col.has_primary_key_attribute = True
            columns.append(col)
        node.current.columns = columns

        return node

    def _make_decomposed_metric(
        self,
        metric_node: MagicMock,
        components: list[MetricComponent],
        aggregability: Aggregability,
    ) -> DecomposedMetricInfo:
        """Helper to create a DecomposedMetricInfo."""
        return DecomposedMetricInfo(
            metric_node=metric_node,
            components=components,
            aggregability=aggregability,
            combiner="",
            derived_ast=parse("SELECT 1"),
        )

    def test_empty_metric_group(self):
        """Test with a metric group that has no metrics."""
        parent_node = self._make_parent_node("v3.parent")
        metric_group = MetricGroup(parent_node=parent_node, decomposed_metrics=[])

        result = analyze_grain_groups(metric_group, ["dim1", "dim2"])

        assert result == []

    def test_single_metric_full_aggregability(self):
        """Test single metric with FULL aggregability."""
        parent_node = self._make_parent_node("v3.parent")
        metric_node = self._make_metric_node("v3.metric1")
        component = self._make_component("comp1", Aggregability.FULL)
        decomposed = self._make_decomposed_metric(
            metric_node,
            [component],
            Aggregability.FULL,
        )

        metric_group = MetricGroup(
            parent_node=parent_node,
            decomposed_metrics=[decomposed],
        )

        result = analyze_grain_groups(metric_group, ["dim1"])

        assert len(result) == 1
        assert result[0].aggregability == Aggregability.FULL
        assert result[0].grain_columns == []
        assert len(result[0].components) == 1

    def test_single_metric_limited_aggregability(self):
        """Test single metric with LIMITED aggregability and level columns."""
        parent_node = self._make_parent_node("v3.parent")
        metric_node = self._make_metric_node("v3.metric1")
        component = self._make_component(
            "comp1",
            Aggregability.LIMITED,
            level=["user_id"],
        )
        decomposed = self._make_decomposed_metric(
            metric_node,
            [component],
            Aggregability.LIMITED,
        )

        metric_group = MetricGroup(
            parent_node=parent_node,
            decomposed_metrics=[decomposed],
        )

        result = analyze_grain_groups(metric_group, ["dim1"])

        assert len(result) == 1
        assert result[0].aggregability == Aggregability.LIMITED
        assert result[0].grain_columns == ["user_id"]
        assert len(result[0].components) == 1

    def test_single_metric_none_aggregability(self):
        """Test single metric with NONE aggregability uses native grain."""
        parent_node = self._make_parent_node("v3.parent", pk_columns=["id", "date"])
        metric_node = self._make_metric_node("v3.metric1")
        component = self._make_component("comp1", Aggregability.NONE)
        decomposed = self._make_decomposed_metric(
            metric_node,
            [component],
            Aggregability.NONE,
        )

        metric_group = MetricGroup(
            parent_node=parent_node,
            decomposed_metrics=[decomposed],
        )

        result = analyze_grain_groups(metric_group, ["dim1"])

        assert len(result) == 1
        assert result[0].aggregability == Aggregability.NONE
        # Native grain is sorted PK columns
        assert result[0].grain_columns == ["date", "id"]
        assert len(result[0].components) == 1

    def test_multiple_metrics_same_aggregability(self):
        """Test multiple metrics with same aggregability grouped together."""
        parent_node = self._make_parent_node("v3.parent")
        metric_node1 = self._make_metric_node("v3.metric1")
        metric_node2 = self._make_metric_node("v3.metric2")
        comp1 = self._make_component("comp1", Aggregability.FULL)
        comp2 = self._make_component("comp2", Aggregability.FULL)
        decomposed1 = self._make_decomposed_metric(
            metric_node1,
            [comp1],
            Aggregability.FULL,
        )
        decomposed2 = self._make_decomposed_metric(
            metric_node2,
            [comp2],
            Aggregability.FULL,
        )

        metric_group = MetricGroup(
            parent_node=parent_node,
            decomposed_metrics=[decomposed1, decomposed2],
        )

        result = analyze_grain_groups(metric_group, ["dim1"])

        # Should be grouped into single grain group
        assert len(result) == 1
        assert result[0].aggregability == Aggregability.FULL
        assert len(result[0].components) == 2

    def test_multiple_metrics_different_aggregability(self):
        """Test multiple metrics with different aggregabilities create separate groups."""
        parent_node = self._make_parent_node("v3.parent", pk_columns=["id"])
        metric_node1 = self._make_metric_node("v3.metric1")
        metric_node2 = self._make_metric_node("v3.metric2")
        comp1 = self._make_component("comp1", Aggregability.FULL)
        comp2 = self._make_component("comp2", Aggregability.NONE)
        decomposed1 = self._make_decomposed_metric(
            metric_node1,
            [comp1],
            Aggregability.FULL,
        )
        decomposed2 = self._make_decomposed_metric(
            metric_node2,
            [comp2],
            Aggregability.NONE,
        )

        metric_group = MetricGroup(
            parent_node=parent_node,
            decomposed_metrics=[decomposed1, decomposed2],
        )

        result = analyze_grain_groups(metric_group, ["dim1"])

        # Should create two grain groups
        assert len(result) == 2
        # FULL should come first (sorted)
        assert result[0].aggregability == Aggregability.FULL
        assert result[1].aggregability == Aggregability.NONE

    def test_limited_with_different_levels(self):
        """Test LIMITED aggregability with different level columns creates separate groups."""
        parent_node = self._make_parent_node("v3.parent")
        metric_node1 = self._make_metric_node("v3.metric1")
        metric_node2 = self._make_metric_node("v3.metric2")
        comp1 = self._make_component("comp1", Aggregability.LIMITED, level=["user_id"])
        comp2 = self._make_component("comp2", Aggregability.LIMITED, level=["order_id"])
        decomposed1 = self._make_decomposed_metric(
            metric_node1,
            [comp1],
            Aggregability.LIMITED,
        )
        decomposed2 = self._make_decomposed_metric(
            metric_node2,
            [comp2],
            Aggregability.LIMITED,
        )

        metric_group = MetricGroup(
            parent_node=parent_node,
            decomposed_metrics=[decomposed1, decomposed2],
        )

        result = analyze_grain_groups(metric_group, ["dim1"])

        # Should create two grain groups with different level columns
        assert len(result) == 2
        assert all(g.aggregability == Aggregability.LIMITED for g in result)
        # Sorted by grain columns
        assert result[0].grain_columns == ["order_id"]
        assert result[1].grain_columns == ["user_id"]

    def test_limited_with_same_levels_grouped(self):
        """Test LIMITED with same level columns are grouped together."""
        parent_node = self._make_parent_node("v3.parent")
        metric_node1 = self._make_metric_node("v3.metric1")
        metric_node2 = self._make_metric_node("v3.metric2")
        comp1 = self._make_component("comp1", Aggregability.LIMITED, level=["user_id"])
        comp2 = self._make_component("comp2", Aggregability.LIMITED, level=["user_id"])
        decomposed1 = self._make_decomposed_metric(
            metric_node1,
            [comp1],
            Aggregability.LIMITED,
        )
        decomposed2 = self._make_decomposed_metric(
            metric_node2,
            [comp2],
            Aggregability.LIMITED,
        )

        metric_group = MetricGroup(
            parent_node=parent_node,
            decomposed_metrics=[decomposed1, decomposed2],
        )

        result = analyze_grain_groups(metric_group, ["dim1"])

        # Should be grouped into single grain group
        assert len(result) == 1
        assert result[0].aggregability == Aggregability.LIMITED
        assert result[0].grain_columns == ["user_id"]
        assert len(result[0].components) == 2

    def test_sorting_full_before_limited_before_none(self):
        """Test that grain groups are sorted: FULL, then LIMITED, then NONE."""
        parent_node = self._make_parent_node("v3.parent", pk_columns=["id"])
        metric_node1 = self._make_metric_node("v3.metric1")
        metric_node2 = self._make_metric_node("v3.metric2")
        metric_node3 = self._make_metric_node("v3.metric3")

        # Create in reverse order: NONE, LIMITED, FULL
        comp_none = self._make_component("comp_none", Aggregability.NONE)
        comp_limited = self._make_component(
            "comp_limited",
            Aggregability.LIMITED,
            level=["user_id"],
        )
        comp_full = self._make_component("comp_full", Aggregability.FULL)

        decomposed1 = self._make_decomposed_metric(
            metric_node1,
            [comp_none],
            Aggregability.NONE,
        )
        decomposed2 = self._make_decomposed_metric(
            metric_node2,
            [comp_limited],
            Aggregability.LIMITED,
        )
        decomposed3 = self._make_decomposed_metric(
            metric_node3,
            [comp_full],
            Aggregability.FULL,
        )

        metric_group = MetricGroup(
            parent_node=parent_node,
            decomposed_metrics=[decomposed1, decomposed2, decomposed3],
        )

        result = analyze_grain_groups(metric_group, ["dim1"])

        assert len(result) == 3
        # Should be sorted: FULL first, then LIMITED, then NONE
        assert result[0].aggregability == Aggregability.FULL
        assert result[1].aggregability == Aggregability.LIMITED
        assert result[2].aggregability == Aggregability.NONE

    def test_multiple_components_per_metric(self):
        """Test metric with multiple components (e.g., AVG decomposed to SUM and COUNT)."""
        parent_node = self._make_parent_node("v3.parent")
        metric_node = self._make_metric_node("v3.avg_metric")

        # AVG decomposes to SUM and COUNT, both FULL
        comp_sum = self._make_component("sum_val", Aggregability.FULL)
        comp_count = self._make_component("count_val", Aggregability.FULL)

        decomposed = self._make_decomposed_metric(
            metric_node,
            [comp_sum, comp_count],
            Aggregability.FULL,
        )

        metric_group = MetricGroup(
            parent_node=parent_node,
            decomposed_metrics=[decomposed],
        )

        result = analyze_grain_groups(metric_group, ["dim1"])

        assert len(result) == 1
        assert result[0].aggregability == Aggregability.FULL
        # Both components in the same group
        assert len(result[0].components) == 2

    def test_limited_with_none_level_treated_as_empty(self):
        """Test LIMITED with level=None treated as empty level list."""
        parent_node = self._make_parent_node("v3.parent")
        metric_node = self._make_metric_node("v3.metric1")
        component = self._make_component("comp1", Aggregability.LIMITED, level=None)
        decomposed = self._make_decomposed_metric(
            metric_node,
            [component],
            Aggregability.LIMITED,
        )

        metric_group = MetricGroup(
            parent_node=parent_node,
            decomposed_metrics=[decomposed],
        )

        result = analyze_grain_groups(metric_group, ["dim1"])

        assert len(result) == 1
        assert result[0].aggregability == Aggregability.LIMITED
        assert result[0].grain_columns == []

    def test_parent_node_preserved_in_grain_group(self):
        """Test that parent_node is correctly set in resulting GrainGroups."""
        parent_node = self._make_parent_node("v3.my_parent")
        metric_node = self._make_metric_node("v3.metric1")
        component = self._make_component("comp1", Aggregability.FULL)
        decomposed = self._make_decomposed_metric(
            metric_node,
            [component],
            Aggregability.FULL,
        )

        metric_group = MetricGroup(
            parent_node=parent_node,
            decomposed_metrics=[decomposed],
        )

        result = analyze_grain_groups(metric_group, ["dim1"])

        assert result[0].parent_node == parent_node
        assert result[0].parent_node.name == "v3.my_parent"
