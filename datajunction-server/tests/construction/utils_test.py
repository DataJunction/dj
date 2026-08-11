"""
Tests for building nodes and extracting dependencies
"""

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.construction.utils import (
    dimension_link_default_literal,
    get_dj_node,
)
from datajunction_server.errors import DJErrorException
from datajunction_server.models.node_type import NodeType


@pytest.mark.asyncio
async def test_get_dj_node_raise_unknown_node_exception(session: AsyncSession):
    """
    Test raising an unknown node exception when calling get_dj_node
    """
    with pytest.raises(DJErrorException) as exc_info:
        await get_dj_node(session, "foobar")

    assert "No node" in str(exc_info.value)

    with pytest.raises(DJErrorException) as exc_info:
        await get_dj_node(
            session,
            "foobar",
            kinds={NodeType.METRIC, NodeType.DIMENSION},
        )

    assert "dimension" in str(exc_info.value)
    assert "metric" in str(exc_info.value)
    assert "source" not in str(exc_info.value)
    assert "transform" not in str(exc_info.value)

    with pytest.raises(DJErrorException) as exc_info:
        # test that the event_type raises because it's a dimension and not a transform
        await get_dj_node(session, "event_type", kinds={NodeType.TRANSFORM})

    assert "No node `event_type` exists of kind transform" in str(exc_info.value)

    # test that the event_type raises because it's a dimension and not a transform
    with pytest.raises(DJErrorException) as exc_info:
        await get_dj_node(session, "event_type", kinds={NodeType.TRANSFORM})

    assert "No node `event_type` exists of kind transform" in str(
        exc_info.value,
    )


def test_dimension_link_default_literal_uses_column_type():
    """
    Dimension link defaults should render according to target column type when known.
    """
    assert str(dimension_link_default_literal("0", "int")) == "0"
    assert str(dimension_link_default_literal(0, "string")) == "'0'"
    assert str(dimension_link_default_literal("false", "boolean")) == "False"
    assert str(dimension_link_default_literal("Bob's Team", "string")) == (
        "'Bob''s Team'"
    )


def test_dimension_link_default_literal_validation_edges():
    """
    Dimension link defaults should reject invalid typed literals.
    """
    with pytest.raises(ValueError, match="Boolean values cannot be used"):
        dimension_link_default_literal(True, "int")

    with pytest.raises(ValueError, match="Invalid boolean default value"):
        dimension_link_default_literal("maybe", "boolean")


def test_dimension_link_default_literal_fallbacks():
    """
    Dimension link defaults should render fallback literals from Python values.
    """
    assert str(dimension_link_default_literal(None)) == "NULL"
    assert str(dimension_link_default_literal(True)) == "True"
    assert str(dimension_link_default_literal(0, "boolean")) == "False"
