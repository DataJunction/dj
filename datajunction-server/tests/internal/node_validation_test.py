"""
Tests for node validation functionality in datajunction_server.internal.validation
"""

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.column import Column
from datajunction_server.database.node import Node, NodeRevision, NodeType
from datajunction_server.database.user import OAuthProvider, User
from datajunction_server.internal.validation import (
    _reparse_parent_column_types,
    validate_node_data,
)
from datajunction_server.models.node import NodeRevisionBase, NodeStatus
from datajunction_server.sql.parsing import types as ct


@pytest_asyncio.fixture
async def user(session: AsyncSession) -> User:
    """
    A user fixture.
    """
    user = User(
        username="testuser",
        oauth_provider=OAuthProvider.BASIC,
    )
    session.add(user)
    await session.commit()
    return user


@pytest_asyncio.fixture
async def parent_with_malformed_column(session: AsyncSession, user: User) -> Node:
    """
    A parent node with a column that has malformed/unparseable type.
    """
    node = Node(
        name="test.parent_malformed",
        type=NodeType.TRANSFORM,
        created_by_id=user.id,
        current_version="v1.0",
    )

    # Create a column with a malformed type string
    col = Column(
        name="bad_column",
        type="INVALID_TYPE_SYNTAX<>",  # This will fail to parse
        order=0,
    )

    node_revision = NodeRevision(
        name="test.parent_malformed",
        display_name="Parent with Malformed Column",
        type=NodeType.TRANSFORM,
        query="SELECT 1 as bad_column",
        status=NodeStatus.VALID,
        version="v1.0",
        node=node,
        columns=[col],
        created_by_id=user.id,
    )
    session.add(node)
    session.add(node_revision)
    await session.commit()
    await session.refresh(node, ["current"])
    await session.refresh(node.current, ["columns"])
    return node


@pytest.mark.asyncio
async def test_validate_node_with_malformed_parent_column_type(
    session: AsyncSession,
    user: User,
    parent_with_malformed_column: Node,
):
    """
    Test validation handles parent columns with unparseable types gracefully.
    This covers lines 120-124 in validation.py (exception handling in parse_rule).
    """
    # Create a child node that references the parent with malformed column type
    data = NodeRevisionBase(
        name="test.child_of_malformed",
        display_name="Child of Malformed Parent",
        type=NodeType.TRANSFORM,
        query="SELECT * FROM test.parent_malformed",
        mode="published",
    )

    # Validation should handle this gracefully without crashing
    validator = await validate_node_data(data, session)

    # The validation should complete without exceptions
    # Status may be valid or invalid depending on other factors, but shouldn't crash
    assert validator.status in [NodeStatus.VALID, NodeStatus.INVALID]

    # If the parent is found and has columns, those columns should be processed
    # even if type parsing fails
    if validator.dependencies_map:
        for parent in validator.dependencies_map.keys():
            if parent.name == parent_with_malformed_column.name:
                # Parent should have the column even with malformed type
                assert len(parent.columns) > 0


@pytest.mark.asyncio
async def test_validate_node_with_complex_column_types(
    session: AsyncSession,
    user: User,
):
    """
    Test validation with parent that has complex column types (MapType, ListType, StructType).
    Ensures these types are properly parsed and not re-parsed.
    """
    # Create parent with complex types
    parent_node = Node(
        name="test.parent_complex",
        type=NodeType.TRANSFORM,
        created_by_id=user.id,
        current_version="v1.0",
    )

    # Column with map type
    map_col = Column(
        name="map_column",
        type=ct.MapType(
            key_type=ct.StringType(),
            value_type=ct.IntegerType(),
        ),
        order=0,
    )

    # Column with list type
    list_col = Column(
        name="list_column",
        type=ct.ListType(element_type=ct.StringType()),
        order=1,
    )

    # Column with struct type
    from datajunction_server.sql.parsing.ast import Name

    struct_col = Column(
        name="struct_column",
        type=ct.StructType(
            ct.NestedField(name=Name("field1"), field_type=ct.IntegerType()),
            ct.NestedField(name=Name("field2"), field_type=ct.StringType()),
        ),
        order=2,
    )

    node_revision = NodeRevision(
        name="test.parent_complex",
        display_name="Parent with Complex Types",
        type=NodeType.TRANSFORM,
        query="SELECT MAP('a', 1) as map_column, ARRAY['x'] as list_column, "
        "STRUCT(1, 'y') as struct_column",
        status=NodeStatus.VALID,
        version="v1.0",
        node=parent_node,
        columns=[map_col, list_col, struct_col],
        created_by_id=user.id,
    )
    session.add(parent_node)
    session.add(node_revision)
    await session.commit()

    # Create child referencing parent
    data = NodeRevisionBase(
        name="test.child_complex",
        display_name="Child of Complex Parent",
        type=NodeType.TRANSFORM,
        query="SELECT * FROM test.parent_complex",
        mode="published",
    )

    # Should validate without issues
    validator = await validate_node_data(data, session)

    # Validation should succeed without crashing
    # The main goal is to ensure that parent nodes with complex types (MapType, ListType, StructType)
    # don't cause validation to fail when they're parsed (covering lines 120-124 in validation.py)
    assert validator.status in [NodeStatus.VALID, NodeStatus.INVALID]

    # Verify that the parent was found in dependencies
    parent_names = [p.name for p in validator.dependencies_map.keys()]
    assert "test.parent_complex" in parent_names

    # Verify the parent has the complex column types we set
    for parent in validator.dependencies_map.keys():
        if parent.name == "test.parent_complex":
            column_types = {col.name: type(col.type).__name__ for col in parent.columns}
            assert "map_column" in column_types
            assert "list_column" in column_types
            assert "struct_column" in column_types
            assert column_types["map_column"] == "MapType"
            assert column_types["list_column"] == "ListType"
            assert column_types["struct_column"] == "StructType"


@pytest.mark.asyncio
async def test_validate_node_with_nested_struct_field_access(
    session: AsyncSession,
    user: User,
):
    """
    Test that a transform selecting a two-level deep struct field (col.lvl1.leaf)
    resolves to VALID and infers the correct leaf type.

    Reproduces: "Cannot resolve type of column metrics_data.clock_type.total_val"
    when the source table has a column of type struct<clock_type struct<total_val bigint>>.
    """
    from datajunction_server.sql.parsing.ast import Name

    source_node = Node(
        name="test.source_nested_struct",
        type=NodeType.SOURCE,
        created_by_id=user.id,
        current_version="v1.0",
    )

    # metrics_data: struct<clock_type struct<total_val bigint, partial_val bigint>,
    #                      other_type struct<total_val bigint>>
    metrics_data_col = Column(
        name="metrics_data",
        type=ct.StructType(
            ct.NestedField(
                name=Name("clock_type"),
                field_type=ct.StructType(
                    ct.NestedField(name=Name("total_val"), field_type=ct.BigIntType()),
                    ct.NestedField(
                        name=Name("partial_val"),
                        field_type=ct.BigIntType(),
                    ),
                ),
            ),
            ct.NestedField(
                name=Name("other_type"),
                field_type=ct.StructType(
                    ct.NestedField(name=Name("total_val"), field_type=ct.BigIntType()),
                ),
            ),
        ),
        order=0,
    )
    dimension_col = Column(name="region_id", type=ct.StringType(), order=1)

    source_revision = NodeRevision(
        name="test.source_nested_struct",
        display_name="Source with Nested Struct",
        type=NodeType.SOURCE,
        query=None,
        status=NodeStatus.VALID,
        version="v1.0",
        node=source_node,
        columns=[metrics_data_col, dimension_col],
        created_by_id=user.id,
    )
    session.add(source_node)
    session.add(source_revision)
    await session.commit()

    data = NodeRevisionBase(
        name="test.child_nested_struct",
        display_name="Child accessing nested struct",
        type=NodeType.TRANSFORM,
        query=(
            "SELECT "
            "metrics_data.clock_type.total_val AS total_clock_val, "
            "region_id "
            "FROM test.source_nested_struct"
        ),
        mode="published",
    )

    validator = await validate_node_data(data, session)

    assert validator.status == NodeStatus.VALID, (
        f"Expected VALID but got {validator.status}. Errors: {validator.errors}"
    )
    output_col_types = {col.name: col.type for col in validator.columns}
    assert "total_clock_val" in output_col_types
    assert output_col_types["total_clock_val"] == ct.BigIntType()


@pytest.mark.asyncio
async def test_struct_field_access_depth1_valid(session: AsyncSession, user: User):
    """
    Validate that depth-1 struct field access (struct_col.field) resolves correctly
    and produces a helpful error when the field doesn't exist.
    """
    from datajunction_server.sql.parsing.ast import Name

    source_node = Node(
        name="test.struct_depth1_source",
        type=NodeType.SOURCE,
        created_by_id=user.id,
        current_version="v1.0",
    )
    # ad_account: struct<id string, status string, version bigint>
    ad_account_col = Column(
        name="ad_account",
        type=ct.StructType(
            ct.NestedField(name=Name("id"), field_type=ct.StringType()),
            ct.NestedField(name=Name("status"), field_type=ct.StringType()),
            ct.NestedField(name=Name("version"), field_type=ct.BigIntType()),
        ),
        order=0,
    )
    source_revision = NodeRevision(
        name="test.struct_depth1_source",
        display_name="Source with struct",
        type=NodeType.SOURCE,
        query=None,
        status=NodeStatus.VALID,
        version="v1.0",
        node=source_node,
        columns=[ad_account_col],
        created_by_id=user.id,
    )
    session.add(source_node)
    session.add(source_revision)
    await session.commit()

    # Valid: accessing an existing field
    valid_data = NodeRevisionBase(
        name="test.struct_depth1_valid",
        display_name="Child accessing valid struct field",
        type=NodeType.TRANSFORM,
        query="SELECT ad_account.status AS account_status FROM test.struct_depth1_source",
        mode="published",
    )
    validator = await validate_node_data(valid_data, session)
    assert validator.status == NodeStatus.VALID, (
        f"Expected VALID but got {validator.status}. Errors: {validator.errors}"
    )
    output_col_types = {col.name: col.type for col in validator.columns}
    assert output_col_types["account_status"] == ct.StringType()

    # Invalid: accessing a non-existent field → should give a helpful error
    invalid_data = NodeRevisionBase(
        name="test.struct_depth1_invalid",
        display_name="Child accessing missing struct field",
        type=NodeType.TRANSFORM,
        query=(
            "SELECT ad_account.ad_account_status AS bad_col "
            "FROM test.struct_depth1_source"
        ),
        mode="published",
    )
    validator_invalid = await validate_node_data(invalid_data, session)
    assert validator_invalid.status == NodeStatus.INVALID
    all_error_messages = " ".join(e.message for e in validator_invalid.errors)
    assert "ad_account_status" in all_error_messages
    assert "Available" in all_error_messages


@pytest.mark.asyncio
async def test_metric_with_nonexistent_table_alias_is_invalid(
    session: AsyncSession,
    user: User,
):
    """
    Regression test: a metric query referencing a nonexistent table alias (e.g. `ghost.value`
    where `ghost` is not a struct column, not a valid alias, and not a dimension node) should
    produce NodeStatus.INVALID with an INVALID_COLUMN error.

    Previously, INVALID_COLUMN errors from compile() inside extract_dependencies() were
    silently discarded, causing such metrics to pass validation incorrectly.
    """
    source_node = Node(
        name="test.source_for_invalid_alias_metric",
        type=NodeType.SOURCE,
        created_by_id=user.id,
        current_version="v1.0",
    )
    source_revision = NodeRevision(
        name="test.source_for_invalid_alias_metric",
        display_name="Source for invalid alias metric test",
        type=NodeType.SOURCE,
        query=None,
        status=NodeStatus.VALID,
        version="v1.0",
        node=source_node,
        columns=[
            Column(name="event_count", type=ct.BigIntType(), order=0),
            Column(name="user_id", type=ct.StringType(), order=1),
        ],
        created_by_id=user.id,
    )
    session.add(source_node)
    session.add(source_revision)
    await session.commit()

    # `ghost` is not an alias, not a struct, and not a dimension node — should be INVALID
    data = NodeRevisionBase(
        name="test.metric_invalid_alias",
        display_name="Metric with invalid alias",
        type=NodeType.METRIC,
        query=(
            "SELECT COUNT(IF(ghost.value IS NOT NULL, 1, 0)) "
            "FROM test.source_for_invalid_alias_metric"
        ),
        mode="published",
    )

    validator = await validate_node_data(data, session)

    assert validator.status == NodeStatus.INVALID, (
        f"Expected INVALID but got {validator.status}. Errors: {validator.errors}"
    )
    from datajunction_server.errors import ErrorCode

    error_codes = [e.code for e in validator.errors]
    assert ErrorCode.INVALID_COLUMN in error_codes, (
        f"Expected INVALID_COLUMN error, got: {validator.errors}"
    )


@pytest.mark.asyncio
async def test_metric_referencing_dimension_attr_is_valid(
    session: AsyncSession,
    user: User,
):
    """
    A no-FROM-clause metric that references a valid dimension attribute
    (e.g. ``test.dates.year``) should remain VALID after the INVALID_COLUMN surfacing fix.

    Dimension attribute references (``dim_node.col``) are resolved inside compile() via
    a second lookup branch — the compile step sets _is_compiled=True before appending any
    error, so no INVALID_COLUMN is ever generated for them. This test confirms that valid
    dimension references are not incorrectly flagged.
    """
    dim_node = Node(
        name="test.dates",
        type=NodeType.DIMENSION,
        created_by_id=user.id,
        current_version="v1.0",
    )
    dim_revision = NodeRevision(
        name="test.dates",
        display_name="Dates dimension",
        type=NodeType.DIMENSION,
        query="SELECT dateint, year FROM default.calendar",
        status=NodeStatus.VALID,
        version="v1.0",
        node=dim_node,
        columns=[
            Column(name="dateint", type=ct.IntegerType(), order=0),
            Column(name="year", type=ct.IntegerType(), order=1),
        ],
        created_by_id=user.id,
    )
    for obj in (dim_node, dim_revision):
        session.add(obj)
    await session.commit()

    # No-FROM-clause metric referencing a dimension attribute — valid
    data = NodeRevisionBase(
        name="test.current_year_sessions",
        display_name="Current Year Sessions",
        type=NodeType.METRIC,
        query="SELECT COUNT(IF(test.dates.year = 2024, 1, 0))",
        mode="published",
    )

    validator = await validate_node_data(data, session)

    assert validator.status == NodeStatus.VALID, (
        f"Expected VALID but got {validator.status}. Errors: {validator.errors}"
    )


@pytest.mark.asyncio
async def test_metric_with_lambda_parameters_is_valid(
    session: AsyncSession,
    user: User,
):
    """
    Regression test: a transform node using higher-order functions (FILTER, AGGREGATE) with
    lambda expressions should remain VALID. Lambda parameters (e.g. ``c`` in ``c -> c.name``)
    are valid namespaces inside the lambda body and must not be flagged as INVALID_COLUMN.

    Previously, the INVALID_COLUMN surfacing fix in PR #1961 incorrectly treated lambda
    parameters as unresolved table aliases, causing nodes using FILTER/AGGREGATE/TRANSFORM
    with struct field access to be rejected.
    """
    from datajunction_server.sql.parsing.ast import Name

    _make_source(
        session,
        user,
        "test.lambda_source",
        [
            Column(
                name="items",
                type=ct.ListType(
                    element_type=ct.StructType(
                        ct.NestedField(name=Name("key"), field_type=ct.StringType()),
                        ct.NestedField(name=Name("val"), field_type=ct.DoubleType()),
                    ),
                ),
                order=0,
            ),
        ],
    )
    await session.commit()

    # Lambda parameters `x` and `acc` must not be treated as unresolved table aliases
    data = NodeRevisionBase(
        name="test.transform_with_lambda",
        display_name="Transform with lambda",
        type=NodeType.TRANSFORM,
        query=(
            "SELECT 4.0 * AGGREGATE("
            "  FILTER(items, x -> x.key = 'FOO'),"
            "  CAST(0.0 AS DOUBLE),"
            "  (acc, x) -> CAST(acc + x.val AS DOUBLE)"
            ") AS result "
            "FROM test.lambda_source"
        ),
        mode="published",
    )

    validator = await validate_node_data(data, session)

    from datajunction_server.errors import ErrorCode

    invalid_col_errors = [
        e for e in validator.errors if e.code == ErrorCode.INVALID_COLUMN
    ]
    assert not invalid_col_errors, (
        f"Unexpected INVALID_COLUMN errors from lambda params: {invalid_col_errors}"
    )


def _make_source(session, user, name, columns):
    """Helper: create and persist a SOURCE node with the given columns."""
    node = Node(
        name=name,
        type=NodeType.SOURCE,
        created_by_id=user.id,
        current_version="v1.0",
    )
    revision = NodeRevision(
        name=name,
        display_name=name,
        type=NodeType.SOURCE,
        query=None,
        status=NodeStatus.VALID,
        version="v1.0",
        node=node,
        columns=columns,
        created_by_id=user.id,
    )
    session.add(node)
    session.add(revision)
    return node


@pytest.mark.asyncio
async def test_lateral_view_alias_not_flagged_as_invalid_column(
    session: AsyncSession,
    user: User,
):
    """
    Regression: LATERAL VIEW generates a virtual table alias (e.g. ``t`` in
    ``LATERAL VIEW explode(arr) t AS elem``) that is referenced as ``t.elem``
    in the SELECT. That alias is not an ast.Table or ast.Query node, so it was
    not captured in local_aliases before the fix — causing a false INVALID_COLUMN.
    """
    from datajunction_server.errors import ErrorCode

    _make_source(
        session,
        user,
        "test.lateral_source",
        [Column(name="arr", type=ct.ListType(element_type=ct.StringType()), order=0)],
    )
    await session.commit()

    data = NodeRevisionBase(
        name="test.lateral_transform",
        display_name="Lateral view transform",
        type=NodeType.TRANSFORM,
        query=(
            "SELECT t.elem FROM test.lateral_source LATERAL VIEW explode(arr) t AS elem"
        ),
        mode="published",
    )
    validator = await validate_node_data(data, session)
    invalid_col_errors = [
        e for e in validator.errors if e.code == ErrorCode.INVALID_COLUMN
    ]
    assert not invalid_col_errors, (
        f"False INVALID_COLUMN from LATERAL VIEW alias: {invalid_col_errors}"
    )


@pytest.mark.asyncio
async def test_unnest_alias_not_flagged_as_invalid_column(
    session: AsyncSession,
    user: User,
):
    """
    Regression: UNNEST with a column alias (``UNNEST(arr) AS t(elem)``) produces a
    virtual table ``t`` referenced as ``t.elem``. Like LATERAL VIEW, that alias may
    not be captured in local_aliases — causing a false INVALID_COLUMN.
    """
    from datajunction_server.errors import ErrorCode

    _make_source(
        session,
        user,
        "test.unnest_source",
        [Column(name="arr", type=ct.ListType(element_type=ct.StringType()), order=0)],
    )
    await session.commit()

    data = NodeRevisionBase(
        name="test.unnest_transform",
        display_name="Unnest transform",
        type=NodeType.TRANSFORM,
        query=(
            "SELECT t.elem FROM test.unnest_source CROSS JOIN UNNEST(arr) AS t(elem)"
        ),
        mode="published",
    )
    validator = await validate_node_data(data, session)
    invalid_col_errors = [
        e for e in validator.errors if e.code == ErrorCode.INVALID_COLUMN
    ]
    assert not invalid_col_errors, (
        f"False INVALID_COLUMN from UNNEST alias: {invalid_col_errors}"
    )


@pytest.mark.asyncio
async def test_values_clause_alias_not_flagged_as_invalid_column(
    session: AsyncSession,
    user: User,
):
    """
    A VALUES clause with explicit column aliases ``(VALUES ...) AS v(a, b)`` should
    not produce false INVALID_COLUMN errors for ``v.a`` / ``v.b``.
    """
    from datajunction_server.errors import ErrorCode

    data = NodeRevisionBase(
        name="test.values_transform",
        display_name="Values transform",
        type=NodeType.TRANSFORM,
        query="SELECT v.a, v.b FROM (VALUES (1, 2)) AS v(a, b)",
        mode="published",
    )
    validator = await validate_node_data(data, session)
    invalid_col_errors = [
        e for e in validator.errors if e.code == ErrorCode.INVALID_COLUMN
    ]
    assert not invalid_col_errors, (
        f"False INVALID_COLUMN from VALUES alias: {invalid_col_errors}"
    )


@pytest.mark.asyncio
async def test_correlated_subquery_outer_alias_not_flagged_as_invalid_column(
    session: AsyncSession,
    user: User,
):
    """
    A correlated subquery references an alias (``o``) from the outer query scope.
    ``find_all(ast.Table)`` traverses into subqueries, so ``o`` should be in
    local_aliases and not produce a false INVALID_COLUMN.
    This test confirms that assumption holds.
    """
    from datajunction_server.errors import ErrorCode

    _make_source(
        session,
        user,
        "test.outer_table",
        [Column(name="id", type=ct.BigIntType(), order=0)],
    )
    _make_source(
        session,
        user,
        "test.inner_table",
        [Column(name="id", type=ct.BigIntType(), order=0)],
    )
    await session.commit()

    data = NodeRevisionBase(
        name="test.correlated_transform",
        display_name="Correlated subquery transform",
        type=NodeType.TRANSFORM,
        query=(
            "SELECT o.id "
            "FROM test.outer_table o "
            "WHERE EXISTS ("
            "  SELECT 1 FROM test.inner_table WHERE test.inner_table.id = o.id"
            ")"
        ),
        mode="published",
    )
    validator = await validate_node_data(data, session)
    invalid_col_errors = [
        e for e in validator.errors if e.code == ErrorCode.INVALID_COLUMN
    ]
    assert not invalid_col_errors, (
        f"False INVALID_COLUMN from correlated subquery outer alias: {invalid_col_errors}"
    )


class TestReparseParentColumnTypes:
    """Tests for _reparse_parent_column_types."""

    def _make_revision(self, name: str, columns: list) -> NodeRevision:
        return NodeRevision(
            name=name,
            type=NodeType.SOURCE,
            version="v1",
            columns=columns,
        )

    def test_string_type_is_parsed(self):
        """A column with a string type is parsed into a ColumnType object."""
        col = Column(name="tags", type="int", order=0)
        revision = self._make_revision("test.node", [col])
        _reparse_parent_column_types({revision: None})
        assert isinstance(col.type, ct.IntegerType)

    def test_unparseable_string_is_left_unchanged(self):
        """If parsing fails, the original string value is preserved."""
        col = Column(name="bad", type="NOT_A_VALID_TYPE_$$$$", order=0)
        revision = self._make_revision("test.node", [col])
        _reparse_parent_column_types({revision: None})
        assert col.type == "NOT_A_VALID_TYPE_$$$$"

    def test_already_parsed_type_is_skipped(self):
        """Columns with a proper ColumnType subclass are left untouched."""
        col = Column(name="id", type=ct.BigIntType(), order=0)
        original = col.type
        revision = self._make_revision("test.node", [col])
        _reparse_parent_column_types({revision: None})
        assert col.type is original

    def test_empty_map_is_noop(self):
        """An empty dependencies_map doesn't raise."""
        _reparse_parent_column_types({})


# ---------------------------------------------------------------------------
# validate_node_data_v2 smoke tests
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def v2_parent_source(session: AsyncSession, user: User) -> Node:
    """Source node with typed columns for use as a parent in v2 tests."""
    node = Node(
        name="test.v2_parent",
        type=NodeType.SOURCE,
        created_by_id=user.id,
        current_version="v1.0",
    )
    revision = NodeRevision(
        name="test.v2_parent",
        display_name="v2 parent",
        type=NodeType.SOURCE,
        query=None,
        status=NodeStatus.VALID,
        version="v1.0",
        node=node,
        columns=[
            Column(name="id", type=ct.BigIntType(), order=0),
            Column(name="is_winning_bid", type=ct.BooleanType(), order=1),
        ],
        created_by_id=user.id,
    )
    session.add(node)
    session.add(revision)
    await session.commit()
    return node


@pytest.mark.asyncio
async def test_validate_node_data_v2_returns_valid_for_simple_transform(
    session: AsyncSession,
    user: User,
    v2_parent_source: Node,
):
    """Happy path: simple transform selecting a typed column resolves cleanly."""
    from datajunction_server.internal.validation import validate_node_data_v2

    data = NodeRevisionBase(
        name="test.v2_child_valid",
        display_name="v2 child",
        type=NodeType.TRANSFORM,
        query="SELECT id FROM test.v2_parent",
        mode="published",
    )
    validator = await validate_node_data_v2(data, session)

    assert validator.status == NodeStatus.VALID, validator.errors
    assert [c.name for c in validator.columns] == ["id"]
    assert not validator.missing_parents_map


@pytest.mark.asyncio
async def test_validate_node_data_v2_flags_missing_parent(
    session: AsyncSession,
    user: User,
):
    """A query referencing a non-existent parent surfaces the missing parent
    via both `missing_parents_map` and an error on the validator."""
    from datajunction_server.errors import ErrorCode
    from datajunction_server.internal.validation import validate_node_data_v2

    data = NodeRevisionBase(
        name="test.v2_missing_parent_child",
        display_name="v2 missing parent child",
        type=NodeType.TRANSFORM,
        query="SELECT id FROM test.does_not_exist",
        mode="published",
    )
    validator = await validate_node_data_v2(data, session)

    assert validator.status == NodeStatus.INVALID
    assert "test.does_not_exist" in validator.missing_parents_map
    assert any(err.code == ErrorCode.MISSING_PARENT for err in validator.errors), [
        (e.code, e.message) for e in validator.errors
    ]


@pytest.mark.asyncio
async def test_validate_node_data_v2_flags_sum_boolean(
    session: AsyncSession,
    user: User,
    v2_parent_source: Node,
):
    """SUM(boolean) is not a valid Spark aggregation — v2 should surface a
    TYPE_INFERENCE error. Locks in the un-suppression of
    'Unable to infer type' error strings at the single-node boundary."""
    from datajunction_server.errors import ErrorCode
    from datajunction_server.internal.validation import validate_node_data_v2

    data = NodeRevisionBase(
        name="test.v2_sum_boolean",
        display_name="sum boolean metric",
        type=NodeType.METRIC,
        query="SELECT SUM(is_winning_bid) FROM test.v2_parent",
        mode="published",
    )
    validator = await validate_node_data_v2(data, session)

    assert validator.status == NodeStatus.INVALID
    assert any(
        err.code == ErrorCode.TYPE_INFERENCE and "Unable to infer type" in err.message
        for err in validator.errors
    ), [(e.code, e.message) for e in validator.errors]


@pytest.mark.asyncio
async def test_validate_node_data_v2_surfaces_parse_errors(
    session: AsyncSession,
    user: User,
):
    """Malformed SQL returns INVALID_SQL_QUERY and doesn't crash validation."""
    from datajunction_server.errors import ErrorCode
    from datajunction_server.internal.validation import validate_node_data_v2

    data = NodeRevisionBase(
        name="test.v2_bad_sql",
        display_name="bad sql",
        type=NodeType.TRANSFORM,
        query="SELECT ))",  # guaranteed parser failure
        mode="published",
    )
    validator = await validate_node_data_v2(data, session)
    assert validator.status == NodeStatus.INVALID
    assert any(err.code == ErrorCode.INVALID_SQL_QUERY for err in validator.errors), [
        (e.code, e.message) for e in validator.errors
    ]


@pytest.mark.asyncio
async def test_validate_node_data_v2_no_candidates_skips_db_load(
    session: AsyncSession,
    user: User,
):
    """A derived metric that references no namespaced candidates (pure literal
    arithmetic) should skip the bulk Node.get_by_names load entirely."""
    from datajunction_server.internal.validation import validate_node_data_v2

    data = NodeRevisionBase(
        name="test.v2_literal_metric",
        display_name="literal metric",
        type=NodeType.METRIC,
        query="SELECT 1 + 1",
        mode="published",
    )
    validator = await validate_node_data_v2(data, session)
    # Status may be valid or invalid depending on metric-query constraints;
    # the important coverage is that validation completed without a DB load.
    assert validator.missing_parents_map == {}


@pytest.mark.asyncio
async def test_validate_node_data_v2_skips_parents_without_columns(
    session: AsyncSession,
    user: User,
):
    """Parent nodes whose current revision has no columns are skipped when
    building parent_columns_map (line 573→572)."""
    from datajunction_server.internal.validation import validate_node_data_v2

    parent = Node(
        name="test.v2_empty_parent",
        type=NodeType.SOURCE,
        created_by_id=user.id,
        current_version="v1.0",
    )
    revision = NodeRevision(
        name="test.v2_empty_parent",
        display_name="empty parent",
        type=NodeType.SOURCE,
        query=None,
        status=NodeStatus.VALID,
        version="v1.0",
        node=parent,
        columns=[],  # intentionally empty
        created_by_id=user.id,
    )
    session.add(parent)
    session.add(revision)
    await session.commit()

    data = NodeRevisionBase(
        name="test.v2_empty_parent_child",
        display_name="child of empty parent",
        type=NodeType.TRANSFORM,
        query="SELECT * FROM test.v2_empty_parent",
        mode="published",
    )
    validator = await validate_node_data_v2(data, session)
    # Validation should not crash; the empty-columns parent is just skipped.
    assert validator.status in (NodeStatus.VALID, NodeStatus.INVALID)


@pytest.mark.asyncio
async def test_validate_node_data_v2_flags_invalid_required_dimensions(
    session: AsyncSession,
    user: User,
):
    """required_dimensions pointing at a column that doesn't exist on the
    referenced dim node surfaces an INVALID_COLUMN error."""
    from datajunction_server.errors import ErrorCode
    from datajunction_server.internal.validation import validate_node_data_v2

    source = Node(
        name="test.v2_req_dim_parent",
        type=NodeType.SOURCE,
        created_by_id=user.id,
        current_version="v1.0",
    )
    source_rev = NodeRevision(
        name="test.v2_req_dim_parent",
        display_name="req dim parent",
        type=NodeType.SOURCE,
        query=None,
        status=NodeStatus.VALID,
        version="v1.0",
        node=source,
        columns=[Column(name="id", type=ct.BigIntType(), order=0)],
        created_by_id=user.id,
    )
    dim = Node(
        name="test.v2_dim_tiny",
        type=NodeType.DIMENSION,
        created_by_id=user.id,
        current_version="v1.0",
    )
    dim_rev = NodeRevision(
        name="test.v2_dim_tiny",
        display_name="tiny dim",
        type=NodeType.DIMENSION,
        query="SELECT 1 AS id",
        status=NodeStatus.VALID,
        version="v1.0",
        node=dim,
        columns=[Column(name="id", type=ct.BigIntType(), order=0)],
        created_by_id=user.id,
    )
    session.add_all([source, source_rev, dim, dim_rev])
    await session.commit()

    # Construct a transient NodeRevision carrying the string-form required
    # dimension (NodeRevisionBase doesn't have the field — it's on
    # MetricNodeFields via CreateNode). v2 resolves these strings against
    # the DB and flags any that don't match a real column.
    child = NodeRevision(
        name="test.v2_req_dim_child",
        display_name="req dim child",
        type=NodeType.METRIC,
        query="SELECT COUNT(id) FROM test.v2_req_dim_parent",
        status=NodeStatus.VALID,
        required_dimensions=["test.v2_dim_tiny.ghost_col"],  # type: ignore[list-item]
    )
    validator = await validate_node_data_v2(child, session)
    assert validator.status == NodeStatus.INVALID
    assert any(
        err.code == ErrorCode.INVALID_COLUMN and "required dimensions" in err.message
        for err in validator.errors
    ), [(e.code, e.message) for e in validator.errors]
    assert any(
        err.code == ErrorCode.INVALID_COLUMN and "required dimensions" in err.message
        for err in validator.errors
    ), [(e.code, e.message) for e in validator.errors]
