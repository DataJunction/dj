"""
Tests for ``datajunction.models``.
"""

# pylint: disable=use-implicit-booleaness-not-comparison

from sqlmodel import Session

from datajunction.models import Node


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
