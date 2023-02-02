"""
Tests for ``dj.typing``.
"""

import pytest

from dj.typing import ColumnType, ColumnTypeError


def test_columntype_bad_primitive():
    """tests that a nonexistent primitive raises"""
    with pytest.raises(ColumnTypeError) as exc:
        ColumnType("array[string]")
    assert "STRING is not an acceptable type" in str(exc)


def test_columntype_bad_complex():
    """tests that an unknown complex type raises"""
    with pytest.raises(ColumnTypeError) as exc:
        ColumnType("list[string]")
    assert "LIST is not a KNOWN complex type" in str(exc)


def test_columntype_not_complex():
    """tests that a primitive is not complex"""
    with pytest.raises(ColumnTypeError) as exc:
        _ = ColumnType.int["str"]
    assert "The type INT is not a complex type" in str(exc)


def test_columntype_wrong_number_generic():
    """tests that complex require specific number of args"""
    with pytest.raises(ColumnTypeError) as exc:
        ColumnType("Map[string]")
    assert "MAP expects 2 inner type(s) but got 1" in str(exc)


def test_validate_columntype_returns_primitive():
    """tests that direct primitive validation returns"""
    assert ColumnType._validate_type("INT") == "INT"  # pylint: disable=W0212

def test_serialize_not_fully_defined_complex():
    """tests that getting a string from a complex type
       without specified inner types raises
    """
    with pytest.raises(ColumnTypeError) as exc:
        _ = ColumnType.Array.value
    assert "cannot be serialized as it is a complex type not fully defined" in str(exc)  # pylint: disable=W0212