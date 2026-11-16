"""
Tests for building nodes and extracting dependencies
"""

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from types import SimpleNamespace

from datajunction_server.construction.build_v3.measures import (
    build_dimension_col_expr,
)
from datajunction_server.construction.utils import (
    column_type_category,
    dimension_link_default_for_column,
    dimension_link_default_literal,
    dimension_link_typed_default_literal,
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


@pytest.mark.parametrize(
    "column_type, expected",
    [
        ("tinyint", "integer"),
        ("smallint", "integer"),
        ("int", "integer"),
        ("INTEGER", "integer"),
        ("bigint", "integer"),
        ("long", "integer"),
        ("float", "decimal"),
        ("double", "decimal"),
        ("real", "decimal"),
        ("decimal(10,2)", "decimal"),
        ("numeric", "decimal"),
        ("boolean", "boolean"),
        ("varchar(20)", "string"),
        ("char(2)", "string"),
        ("string", "string"),
        ("date", "date"),
        ("timestamp", "timestamp"),
        ("timestamp(3)", "timestamp"),
        ("timestamp with time zone", "timestamp"),
        ("timestamp(3) with time zone", "timestamp"),
        ("timestamp without time zone", "timestamp"),
        ("timestamp_ntz", "timestamp"),
        ("binary", None),
        ("array<int>", None),
        ("", None),
        (None, None),
    ],
)
def test_column_type_category(column_type, expected):
    """A column type maps to a default_values category, ignoring parameters."""
    assert column_type_category(column_type) == expected


@pytest.mark.parametrize(
    "column_type, expected",
    [
        ("varchar(20)", "'Unknown'"),
        ("bigint", "-1"),
        ("decimal(10,2)", "0.5"),
        ("boolean", "False"),
        ("date", "DATE '1970-01-01'"),
        ("timestamp", "TIMESTAMP '1970-01-01 00:00:00'"),
        ("timestamp with time zone", "TIMESTAMP '1970-01-01 00:00:00'"),
        ("binary", None),
        (None, None),
    ],
)
def test_dimension_link_typed_default_literal(column_type, expected):
    """The entry for the column's type category renders as a typed literal."""
    default_values = {
        "string": "Unknown",
        "integer": -1,
        "decimal": 0.5,
        "boolean": False,
        "date": "1970-01-01",
        "timestamp": "1970-01-01T00:00:00",
    }
    literal = dimension_link_typed_default_literal(default_values, column_type)
    assert (str(literal) if literal is not None else None) == expected


def test_dimension_link_typed_default_literal_no_matching_entry():
    """A category with no entry yields no literal."""
    assert dimension_link_typed_default_literal({"string": "x"}, "int") is None


def test_dimension_link_default_for_column():
    """default_value applies to every column; default_values only to a match."""
    assert str(dimension_link_default_for_column("N/A", None, None)) == "'N/A'"
    assert str(dimension_link_default_for_column(0, None, "int")) == "0"
    assert str(dimension_link_default_for_column(None, {"integer": 7}, "int")) == "7"
    assert dimension_link_default_for_column(None, {"integer": 7}, "string") is None
    assert dimension_link_default_for_column(None, {"integer": 7}, None) is None
    assert dimension_link_default_for_column(None, None, "int") is None
    assert dimension_link_default_for_column(None, {}, "int") is None


def test_build_dimension_col_expr_unknown_column_type():
    """Without a known column type, default_values adds no COALESCE."""
    link = SimpleNamespace(
        role=None,
        default_value=None,
        default_values={"string": "Unknown", "integer": -1},
    )
    resolved_dim = SimpleNamespace(
        is_local=False,
        column_name="plan_id",
        node_name="shared.plan",
        join_path=SimpleNamespace(links=[link], target_node_name="shared.plan"),
    )
    expr = build_dimension_col_expr(
        resolved_dim,  # type: ignore
        "t1",
        {("shared.plan", ""): "t2"},
        "plan_id",
        ctx=None,
    )
    assert str(expr) == "t2.plan_id"
