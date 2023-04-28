"""
Tests for ``dj.models.node``.
"""

# pylint: disable=use-implicit-booleaness-not-comparison

import pytest
from sqlmodel import Session

from dj.models.node import Node, NodeRevision, NodeType


def test_node_relationship(session: Session) -> None:
    """
    Test the n:n self-referential relationships.
    """
    node_a = Node(name="A", current_version="1")
    node_a_rev = NodeRevision(name="A", version="1", node=node_a)

    node_b = Node(name="B", current_version="1")
    node_a_rev = NodeRevision(name="B", version="1", node=node_b)

    node_c = Node(name="C", current_version="1")
    node_c_rev = NodeRevision(
        name="C",
        version="1",
        node=node_c,
        parents=[node_a, node_b],
    )

    session.add(node_c_rev)

    assert node_a.children == [node_c_rev]
    assert node_b.children == [node_c_rev]
    assert node_c.children == []

    assert node_a_rev.parents == []
    assert node_a_rev.parents == []
    assert node_c_rev.parents == [node_a, node_b]


def test_extra_validation() -> None:
    """
    Test ``extra_validation``.
    """
    node = Node(name="A", type=NodeType.SOURCE, current_version="1")
    node_revision = NodeRevision(
        name=node.name,
        type=node.type,
        node=node,
        version="1",
        query="SELECT * FROM B",
    )
    with pytest.raises(Exception) as excinfo:
        node_revision.extra_validation()
    assert str(excinfo.value) == "Node A of type source should not have a query"

    node = Node(name="A", type=NodeType.METRIC, current_version="1")
    node_revision = NodeRevision(
        name=node.name,
        type=node.type,
        node=node,
        version="1",
    )
    with pytest.raises(Exception) as excinfo:
        node_revision.extra_validation()
    assert str(excinfo.value) == "Node A of type metric needs a query"

    node = Node(name="A", type=NodeType.METRIC, current_version="1")
    node_revision = NodeRevision(
        name=node.name,
        type=node.type,
        node=node,
        version="1",
        query="SELECT count(repair_order_id) + "
        "repair_order_id AS num_repair_orders "
        "FROM repair_orders",
    )
    with pytest.raises(Exception) as excinfo:
        node_revision.extra_validation()
    assert str(excinfo.value) == (
        "Node A of type metric has an invalid query, "
        "should have a single aggregation"
    )

    node = Node(name="A", type=NodeType.TRANSFORM, current_version="1")
    node_revision = NodeRevision(
        name=node.name,
        type=node.type,
        node=node,
        version="1",
        query="SELECT * FROM B",
    )
    node_revision.extra_validation()

    node = Node(name="A", type=NodeType.TRANSFORM, current_version="1")
    node_revision = NodeRevision(
        name=node.name,
        type=node.type,
        node=node,
        version="1",
    )
    with pytest.raises(Exception) as excinfo:
        node_revision.extra_validation()
    assert str(excinfo.value) == "Node A of type transform needs a query"

    node = Node(name="A", type=NodeType.CUBE, current_version="1")
    node_revision = NodeRevision(name=node.name, type=node.type, node=node, version="1")
    with pytest.raises(Exception) as excinfo:
        node_revision.extra_validation()
    assert str(excinfo.value) == "Node A of type cube node needs cube elements"
