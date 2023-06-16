"""
Tests for building nodes and extracting dependencies
"""

# pylint: disable=too-many-lines
import pytest
from sqlmodel import Session

from datajunction_server.construction.utils import get_dj_node
from datajunction_server.errors import DJErrorException
from datajunction_server.models.node import NodeType


def test_get_dj_node_raise_unknown_node_exception(session: Session):
    """
    Test raising an unknown node exception when calling get_dj_node
    """
    with pytest.raises(DJErrorException) as exc_info:
        get_dj_node(session, "foobar")

    assert "No node" in str(exc_info.value)

    with pytest.raises(DJErrorException) as exc_info:
        get_dj_node(session, "foobar", kinds={NodeType.METRIC, NodeType.DIMENSION})

    assert "NodeType.DIMENSION" in str(exc_info.value)
    assert "NodeType.METRIC" in str(exc_info.value)
    assert "NodeType.SOURCE" not in str(exc_info.value)
    assert "NodeType.TRANSFORM" not in str(exc_info.value)

    with pytest.raises(DJErrorException) as exc_info:
        # test that the event_type raises because it's a dimension and not a transform
        get_dj_node(session, "event_type", kinds={NodeType.TRANSFORM})

    assert (
        "No node `event_type` exists of kind NodeType.TRANSFORM"  # pylint: disable=C0301
        in str(exc_info.value)
    )

    # test that the event_type raises because it's a dimension and not a transform
    with pytest.raises(DJErrorException) as exc_info:
        get_dj_node(session, "event_type", kinds={NodeType.TRANSFORM})

    assert "No node `event_type` exists of kind NodeType.TRANSFORM" in str(
        exc_info.value,
    )
