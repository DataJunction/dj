"""
Tests for ``dj.typing``.
"""

import pytest

from dj.typing import ColumnType, ColumnTypeError


def test_columntype_bad_primitive():
    """tests that a nonexistent primitive raises"""
    with pytest.raises(ColumnTypeError) as exc:
        ColumnType("array[rand]")
    assert "RAND is not an acceptable type" in str(exc)


def test_columntype_bad_complex():
    """tests that an unknown complex type raises"""
    with pytest.raises(ColumnTypeError) as exc:
        ColumnType("list[string]")
    assert "LIST is not a known complex type." in str(exc)


def test_columntype_not_complex():
    """tests that a primitive is not complex"""
    with pytest.raises(ColumnTypeError) as exc:
        _ = ColumnType.int["str"]
    assert "The type INT is not a complex type" in str(exc)


def test_columntype_array_wrong_number_generic():
    """tests that complex require specific number of args"""
    with pytest.raises(ColumnTypeError) as exc:
        ColumnType("array[str, str]")
    assert "ColumnTypeError('STR, STR is not an acceptable type for ARRAY')" in str(exc)


def test_columntype_map_bad_key():
    """tests that map require specific key types"""
    with pytest.raises(ColumnTypeError) as exc:
        ColumnType("map[null, str]")
    assert "`NULL` is not an acceptable MAP key type." in str(exc)


def test_columntype_map_wrong_number_generic():
    """tests that map require specific number of args"""
    with pytest.raises(ColumnTypeError) as exc:
        ColumnType("map[str]")
    assert "MAP expects 2 inner types but got 1" in str(exc)


def test_invalid_complex_types():
    """Tests invalid complex types."""
    with pytest.raises(ColumnTypeError) as exc:
        ColumnType("array[map]")
    assert "Missing type definition for complex type!" in str(exc)

    with pytest.raises(ColumnTypeError) as exc:
        ColumnType("map[str]]")
    assert "Unbalanced parentheses" in str(exc)


def test_validate_columntype_returns_primitive():
    """tests that direct primitive validation returns"""
    assert ColumnType._validate_type("INT") == "INT"  # pylint: disable=W0212


def test_nested_types():
    """Test various nested types."""
    assert (
        ColumnType._validate_type("ARRAY[STR]") == "ARRAY[STR]"  # pylint: disable=W0212
    )
    assert (
        ColumnType._validate_type("ARRAY[MAP[STR, STR]]")  # pylint: disable=W0212
        == "ARRAY[MAP[STR, STR]]"
    )
    assert (
        ColumnType._validate_type("ARRAY[ROW[STR, STR]]")  # pylint: disable=W0212
        == "ARRAY[ROW[STR, STR]]"
    )
    assert (
        ColumnType._validate_type(  # pylint: disable=W0212
            "ROW[ARRAY[STR], MAP[STR, STR], STR, INT, MAP[STR, MAP[FLOAT, STR]]]",
        )
        == "ROW[ARRAY[STR], MAP[STR, STR], STR, INT, MAP[STR, MAP[FLOAT, STR]]]"
    )
    assert (
        ColumnType._validate_type(  # pylint: disable=W0212
            "ROW[MAP[STR, ARRAY[STR]], STR name]",
        )
        == "ROW[MAP[STR, ARRAY[STR]], STR]"
    )

    assert (
        ColumnType._validate_type("MAP[STR, MAP[STR, STR]]")  # pylint: disable=W0212
        == "MAP[STR, MAP[STR, STR]]"
    )


def test_serialize_not_fully_defined_complex():
    """tests that getting a string from a complex type
    without specified inner types raises
    """
    with pytest.raises(ColumnTypeError) as exc:
        _ = ColumnType.Array.value
    assert "cannot be serialized as it is a complex type not fully defined" in str(
        exc,
    )  # pylint: disable=W0212
