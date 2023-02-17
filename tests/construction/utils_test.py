"""
Tests for building nodes and extracting dependencies
"""
from typing import Optional

# pylint: disable=too-many-lines
import pytest
from sqlmodel import Session

from dj.construction.exceptions import CompoundBuildException
from dj.construction.utils import get_dj_node, make_name
from dj.errors import DJException
from dj.models.node import NodeType
from dj.sql.parsing.ast import Name, Namespace


@pytest.mark.parametrize(
    "namespace,name,expected_make_name",
    [
        (Namespace([Name("a"), Name("b"), Name("c")]), "d", "a.b.c.d"),
        (Namespace([Name("a"), Name("b")]), "node-name", "a.b.node-name"),
        (Namespace([]), "node-[name]", "node-[name]"),
        (None, "node-[name]", "node-[name]"),
        (Namespace([Name("a"), Name("b"), Name("c")]), None, "a.b.c"),
        (
            Namespace([Name("a"), Name("b"), Name("c")]),
            "node&(name)",
            "a.b.c.node&(name)",
        ),
        (Namespace([Name("a"), Name("b"), Name("c")]), "+d", "a.b.c.+d"),
        (Namespace([Name("a"), Name("b"), Name("c")]), "-d", "a.b.c.-d"),
        (Namespace([Name("a"), Name("b"), Name("c")]), "~~d", "a.b.c.~~d"),
    ],
)
def test_make_name(namespace: Optional[Namespace], name: str, expected_make_name: str):
    """
    Test making names from a namespace and a name
    """
    assert make_name(namespace, name) == expected_make_name


def test_get_dj_node_raise_unknown_node_exception(session: Session):
    """
    Test raising an unknown node exception when calling get_dj_node
    """
    CompoundBuildException().reset()
    with pytest.raises(DJException) as exc_info:
        get_dj_node(session, "foobar")

    assert "No node" in str(exc_info.value)

    with pytest.raises(DJException) as exc_info:
        get_dj_node(session, "foobar", kinds={NodeType.METRIC, NodeType.DIMENSION})

    assert "NodeType.DIMENSION" in str(exc_info.value)
    assert "NodeType.METRIC" in str(exc_info.value)
    assert "NodeType.SOURCE" not in str(exc_info.value)
    assert "NodeType.TRANSFORM" not in str(exc_info.value)

    with pytest.raises(DJException) as exc_info:
        # test that the event_type raises because it's a dimension and not a transform
        get_dj_node(session, "event_type", kinds={NodeType.TRANSFORM})

    assert (
        "Cannot get DJ node event_type\nThe following error happened:\n- No node `event_type` exists of kind NodeType.TRANSFORM. (error code: 203)"  # pylint: disable=C0301
        in str(exc_info.value)
    )

    CompoundBuildException().reset()

    CompoundBuildException().set_raise(False)
    # test that the event_type raises because it's a dimension and not a transform
    get_dj_node(session, "event_type", kinds={NodeType.TRANSFORM})

    assert (
        "No node `event_type` exists of kind NodeType.TRANSFORM. (error code: 203)"
        in str(CompoundBuildException())
    )

    CompoundBuildException().reset()

