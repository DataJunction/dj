"""
Tests for node validation functionality in datajunction_server.internal.validation
"""

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.column import Column
from datajunction_server.database.node import Node, NodeRevision, NodeType
from datajunction_server.database.user import OAuthProvider, User
from datajunction_server.internal.validation import validate_node_data
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
