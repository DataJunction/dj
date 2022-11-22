"""
Tests for ``dj.models.node``.
"""

# pylint: disable=use-implicit-booleaness-not-comparison

import pytest
from sqlmodel import Session

from dj.models.node import Node, NodeType


def test_node_relationship(session: Session) -> None:
    """
    Test the n:n self-referential relationships.
    """
    node_a = Node(name="A")
    node_b = Node(name="B")
    node_c = Node(name="C", parents=[node_a, node_b])

    session.add(node_c)

    assert node_a.children == [node_c]
    assert node_b.children == [node_c]
    assert node_c.children == []

    assert node_a.parents == []
    assert node_b.parents == []
    assert node_c.parents == [node_a, node_b]


def test_extra_validation() -> None:
    """
    Test ``extra_validation``.
    """
    node = Node(name="A", type=NodeType.SOURCE)
    with pytest.raises(Exception) as excinfo:
        node.extra_validation()
    assert str(excinfo.value) == "Node A of type source needs at least one table"

    node = Node(name="A", type=NodeType.SOURCE, query="SELECT * FROM B")
    with pytest.raises(Exception) as excinfo:
        node.extra_validation()
    assert str(excinfo.value) == "Node A of type source should not have a query"

    node = Node(name="A", type=NodeType.METRIC)
    with pytest.raises(Exception) as excinfo:
        node.extra_validation()
    assert str(excinfo.value) == "Node A of type metric needs a query"

    node = Node(name="A", type=NodeType.METRIC, query="SELECT 42")
    with pytest.raises(Exception) as excinfo:
        node.extra_validation()
    assert str(excinfo.value) == (
        "Node A of type metric has an invalid query, "
        "should have a single aggregation"
    )

    node = Node(name="A", type=NodeType.TRANSFORM, query="SELECT * FROM B")
    node.extra_validation()

    node = Node(name="A", type=NodeType.TRANSFORM)
    with pytest.raises(Exception) as excinfo:
        node.extra_validation()
    assert str(excinfo.value) == "Node A of type transform needs a query"
