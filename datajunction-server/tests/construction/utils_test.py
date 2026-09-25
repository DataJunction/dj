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


@pytest.mark.parametrize(
    "default_value, column_type, expected",
    [
        # The column type decides how the value renders
        ("Unknown", "string", "'Unknown'"),
        (0, "bigint", "0"),
        (1.5, "double", "1.5"),
        (True, "boolean", "True"),
        # A string-authored value renders in the column's type
        ("0", "int", "0"),
        ("12.50", "decimal(10,2)", "12.50"),
        ("true", "boolean", "True"),
        ("FALSE", "bool", "False"),
        # A numeric value on a string column stays quoted
        (0, "string", "'0'"),
        (False, "varchar", "'False'"),
        # A value the column type can't hold falls back to a quoted string
        ("Unknown", "int", "'Unknown'"),
        ("Unknown", "boolean", "'Unknown'"),
        (True, "int", "'True'"),
        (3, "boolean", "'3'"),
        ("nan", "int", "'nan'"),
        ("inf", "float", "'inf'"),
        # Without a usable column type, the value's own type decides
        (0, None, "0"),
        (True, None, "True"),
        ("Unknown", None, "'Unknown'"),
        (0, "timestamp", "0"),
        ("1970-01-01", "date", "'1970-01-01'"),
        # Embedded quotes are escaped
        ("O'Brien", "string", "'O''Brien'"),
    ],
)
def test_dimension_link_default_literal(default_value, column_type, expected):
    """
    A dimension link's default value renders to match its target column.
    """
    literal = dimension_link_default_literal(default_value, column_type)
    assert str(literal) == expected
