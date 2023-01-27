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
    node_a_ref = Node(name="A", current_version=1)
    node_a = NodeRevision(name="A", version=1, reference_node=node_a_ref)

    node_b_ref = Node(name="B", current_version=1)
    node_b = NodeRevision(name="B", version=1, reference_node=node_b_ref)

    node_c_ref = Node(name="C", current_version=1)
    node_c = NodeRevision(
        name="C",
        version=1,
        reference_node=node_c_ref,
        parents=[node_a_ref, node_b_ref],
    )

    session.add(node_c)

    assert node_a_ref.children == [node_c]
    assert node_b_ref.children == [node_c]
    assert node_c_ref.children == []

    assert node_a.parents == []
    assert node_b.parents == []
    assert node_c.parents == [node_a_ref, node_b_ref]


def test_extra_validation() -> None:
    """
    Test ``extra_validation``.
    """
    ref_node = Node(name="A", type=NodeType.SOURCE, current_version=1)
    node = NodeRevision(
        name=ref_node.name,
        type=ref_node.type,
        reference_node=ref_node,
        version=1,
    )
    with pytest.raises(Exception) as excinfo:
        node.extra_validation()
    assert str(excinfo.value) == "Node A of type source needs at least one table"

    ref_node = Node(name="A", type=NodeType.SOURCE, current_version=1)
    node = NodeRevision(
        name=ref_node.name,
        type=ref_node.type,
        reference_node=ref_node,
        version=1,
        query="SELECT * FROM B",
    )
    with pytest.raises(Exception) as excinfo:
        node.extra_validation()
    assert str(excinfo.value) == "Node A of type source should not have a query"

    ref_node = Node(name="A", type=NodeType.METRIC, current_version=1)
    node = NodeRevision(
        name=ref_node.name,
        type=ref_node.type,
        reference_node=ref_node,
        version=1,
    )
    with pytest.raises(Exception) as excinfo:
        node.extra_validation()
    assert str(excinfo.value) == "Node A of type metric needs a query"

    ref_node = Node(name="A", type=NodeType.METRIC, current_version=1)
    node = NodeRevision(
        name=ref_node.name,
        type=ref_node.type,
        reference_node=ref_node,
        version=1,
        query="SELECT 42",
    )
    with pytest.raises(Exception) as excinfo:
        node.extra_validation()
    assert str(excinfo.value) == (
        "Node A of type metric has an invalid query, "
        "should have a single aggregation"
    )

    ref_node = Node(name="A", type=NodeType.TRANSFORM, current_version=1)
    node = NodeRevision(
        name=ref_node.name,
        type=ref_node.type,
        reference_node=ref_node,
        version=1,
        query="SELECT * FROM B",
    )
    node.extra_validation()

    ref_node = Node(name="A", type=NodeType.TRANSFORM, current_version=1)
    node = NodeRevision(
        name=ref_node.name,
        type=ref_node.type,
        reference_node=ref_node,
        version=1,
    )
    with pytest.raises(Exception) as excinfo:
        node.extra_validation()
    assert str(excinfo.value) == "Node A of type transform needs a query"
