"""Tests for self-join dimension link validation."""

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from datajunction_server.database.column import Column
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.internal.nodes import validate_complex_dimension_link
from datajunction_server.models.dimensionlink import JoinLinkInput, JoinType
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.parsing import types as ct


@pytest.mark.asyncio
async def test_self_join_requires_role(session: AsyncSession, current_user):
    """
    Test that self-join dimension links require a role.

    When a dimension links to itself (e.g., employee → manager),
    a role must be specified to distinguish the relationship.
    """
    # Create a simple employee dimension
    node = Node(
        name="test.employee",
        type=NodeType.DIMENSION,
        current_version="v1",
        created_by_id=current_user.id,
    )
    revision = NodeRevision(
        node=node,
        name="test.employee",
        type=NodeType.DIMENSION,
        version="v1",
        created_by_id=current_user.id,
        query="SELECT employee_id, employee_name, manager_employee_id FROM employees",
        columns=[
            Column(name="employee_id", type=ct.IntegerType()),
            Column(name="employee_name", type=ct.StringType()),
            Column(name="manager_employee_id", type=ct.IntegerType()),
        ],
    )
    node.current = revision
    session.add(node)
    session.add(revision)
    await session.commit()

    # Reload with relationships eagerly loaded
    result = await session.execute(
        select(Node)
        .where(Node.name == node.name)
        .options(selectinload(Node.current).selectinload(NodeRevision.catalog)),
    )
    node = result.scalar_one()

    # Try to create self-join without role - should fail
    link_input = JoinLinkInput(
        dimension_node="test.employee",
        join_type=JoinType.LEFT,
        join_on="test.employee.manager_employee_id = test.employee.employee_id",
        role=None,  # No role specified!
    )

    with pytest.raises(DJInvalidInputException) as exc_info:
        await validate_complex_dimension_link(
            session=session,
            node=node,
            dimension_node=node,  # Self-join: same node
            link_input=link_input,
        )

    error_message = str(exc_info.value).lower()
    assert "require a role" in error_message or "self-join" in error_message


@pytest.mark.asyncio
async def test_self_join_with_role_succeeds(session: AsyncSession, current_user):
    """
    Test that self-join dimension links work correctly when a role is specified.

    This validates the employee → manager relationship pattern.
    """
    # Create an employee dimension
    node = Node(
        name="test.employee_with_manager",
        type=NodeType.DIMENSION,
        current_version="v1",
        created_by_id=current_user.id,
    )
    revision = NodeRevision(
        node=node,
        name="test.employee_with_manager",
        type=NodeType.DIMENSION,
        version="v1",
        created_by_id=current_user.id,
        query="SELECT employee_id, employee_name, manager_employee_id FROM employees",
        columns=[
            Column(name="employee_id", type=ct.IntegerType()),
            Column(name="employee_name", type=ct.StringType()),
            Column(name="manager_employee_id", type=ct.IntegerType()),
        ],
    )
    node.current = revision
    session.add(node)
    session.add(revision)
    await session.commit()

    # Reload with relationships eagerly loaded
    result = await session.execute(
        select(Node)
        .where(Node.name == node.name)
        .options(selectinload(Node.current).selectinload(NodeRevision.catalog)),
    )
    node = result.scalar_one()

    # Create self-join WITH role - should succeed
    link_input = JoinLinkInput(
        dimension_node="test.employee_with_manager",
        join_type=JoinType.LEFT,
        join_on="test.employee_with_manager.manager_employee_id = test.employee_with_manager.employee_id",
        role="manager",  # Role specified!
    )

    # This should not raise an exception
    result = await validate_complex_dimension_link(
        session=session,
        node=node,
        dimension_node=node,  # Self-join
        link_input=link_input,
    )

    # Verify result is a Join AST node
    assert result is not None

    # The join should use "origin" for the left table and "manager" (role) for the right
    assert (
        str(result)
        == "LEFT JOIN test.employee_with_manager AS manager ON origin.manager_employee_id = manager.employee_id"
    )


@pytest.mark.asyncio
async def test_self_join_location_hierarchy(session: AsyncSession, current_user):
    """
    Test self-join for location hierarchy (location → parent location).

    This is a common pattern for geographic or organizational hierarchies.
    """
    # Create a location dimension with parent relationship
    node = Node(
        name="test.location",
        type=NodeType.DIMENSION,
        current_version="v1",
        created_by_id=current_user.id,
    )
    revision = NodeRevision(
        node=node,
        name="test.location",
        type=NodeType.DIMENSION,
        version="v1",
        created_by_id=current_user.id,
        query="SELECT location_id, location_name, parent_location_id FROM locations",
        columns=[
            Column(name="location_id", type=ct.IntegerType()),
            Column(name="location_name", type=ct.StringType()),
            Column(name="parent_location_id", type=ct.IntegerType()),
        ],
    )
    node.current = revision
    session.add(node)
    session.add(revision)
    await session.commit()

    # Reload with relationships eagerly loaded
    result = await session.execute(
        select(Node)
        .where(Node.name == node.name)
        .options(selectinload(Node.current).selectinload(NodeRevision.catalog)),
    )
    node = result.scalar_one()

    # Create self-join for parent hierarchy
    link_input = JoinLinkInput(
        dimension_node="test.location",
        join_type=JoinType.LEFT,
        join_on="test.location.parent_location_id = test.location.location_id",
        role="parent",
    )

    result = await validate_complex_dimension_link(
        session=session,
        node=node,
        dimension_node=node,
        link_input=link_input,
    )

    assert str(result) == (
        "LEFT JOIN test.location AS parent ON origin.parent_location_id = parent.location_id"
    )


@pytest.mark.asyncio
async def test_regular_dimension_link_still_works(session: AsyncSession, current_user):
    """
    Test that regular (non-self-join) dimension links still work correctly.

    This ensures our self-join fix doesn't break normal dimension links.
    """
    # Create an order dimension
    order_node = Node(
        name="test.order",
        type=NodeType.DIMENSION,
        current_version="v1",
        created_by_id=current_user.id,
    )
    order_revision = NodeRevision(
        node=order_node,
        name="test.order",
        type=NodeType.DIMENSION,
        version="v1",
        created_by_id=current_user.id,
        query="SELECT order_id, customer_id, order_date FROM orders",
        columns=[
            Column(name="order_id", type=ct.IntegerType()),
            Column(name="customer_id", type=ct.IntegerType()),
            Column(name="order_date", type=ct.DateType()),
        ],
    )
    order_node.current = order_revision

    # Create a customer dimension
    customer_node = Node(
        name="test.customer",
        type=NodeType.DIMENSION,
        current_version="v1",
        created_by_id=current_user.id,
    )
    customer_revision = NodeRevision(
        node=customer_node,
        name="test.customer",
        type=NodeType.DIMENSION,
        version="v1",
        created_by_id=current_user.id,
        query="SELECT customer_id, customer_name FROM customers",
        columns=[
            Column(name="customer_id", type=ct.IntegerType()),
            Column(name="customer_name", type=ct.StringType()),
        ],
    )
    customer_node.current = customer_revision

    session.add_all([order_node, order_revision, customer_node, customer_revision])
    await session.commit()

    # Reload with relationships eagerly loaded
    result = await session.execute(
        select(Node)
        .where(Node.name == order_node.name)
        .options(selectinload(Node.current).selectinload(NodeRevision.catalog)),
    )
    order_node = result.scalar_one()

    result = await session.execute(
        select(Node)
        .where(Node.name == customer_node.name)
        .options(selectinload(Node.current).selectinload(NodeRevision.catalog)),
    )
    customer_node = result.scalar_one()

    # Create a regular dimension link (NOT a self-join)
    link_input = JoinLinkInput(
        dimension_node="test.customer",
        join_type=JoinType.LEFT,
        join_on="test.order.customer_id = test.customer.customer_id",
        role=None,  # Role is optional for non-self-joins
    )

    # This should work fine - it's not a self-join
    result = await validate_complex_dimension_link(
        session=session,
        node=order_node,
        dimension_node=customer_node,  # Different nodes
        link_input=link_input,
    )

    assert str(result) == (
        "LEFT JOIN test.customer ON test.order.customer_id = test.customer.customer_id"
    )


@pytest.mark.asyncio
async def test_self_join_with_inner_join(session: AsyncSession, current_user):
    """
    Test self-join with INNER join type (not just LEFT).
    """
    node = Node(
        name="test.org_unit",
        type=NodeType.DIMENSION,
        current_version="v1",
        created_by_id=current_user.id,
    )
    revision = NodeRevision(
        node=node,
        name="test.org_unit",
        type=NodeType.DIMENSION,
        version="v1",
        created_by_id=current_user.id,
        query="SELECT unit_id, unit_name, parent_unit_id FROM org_units",
        columns=[
            Column(name="unit_id", type=ct.IntegerType()),
            Column(name="unit_name", type=ct.StringType()),
            Column(name="parent_unit_id", type=ct.IntegerType()),
        ],
    )
    node.current = revision
    session.add(node)
    session.add(revision)
    await session.commit()

    # Reload with relationships eagerly loaded
    result = await session.execute(
        select(Node)
        .where(Node.name == node.name)
        .options(selectinload(Node.current).selectinload(NodeRevision.catalog)),
    )
    node = result.scalar_one()

    # Self-join with INNER join type
    link_input = JoinLinkInput(
        dimension_node="test.org_unit",
        join_type=JoinType.INNER,  # INNER instead of LEFT
        join_on="test.org_unit.parent_unit_id = test.org_unit.unit_id",
        role="parent_org",
    )

    result = await validate_complex_dimension_link(
        session=session,
        node=node,
        dimension_node=node,
        link_input=link_input,
    )

    assert str(result) == (
        "INNER JOIN test.org_unit AS parent_org ON origin.parent_unit_id = parent_org.unit_id"
    )
