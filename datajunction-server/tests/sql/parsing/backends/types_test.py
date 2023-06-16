"""
Tests for types
"""
import datajunction_server.sql.parsing.types as ct
from datajunction_server.sql.parsing import ast


def test_types_compatible():
    """
    Checks whether type compatibility checks work
    """
    assert ct.IntegerType().is_compatible(ct.IntegerType())
    assert ct.IntegerType().is_compatible(ct.BigIntType())
    assert ct.BigIntType().is_compatible(ct.IntegerType())
    assert ct.TinyIntType().is_compatible(ct.BigIntType())
    assert ct.BigIntType().is_compatible(ct.BigIntType())
    assert ct.BigIntType().is_compatible(ct.IntegerType())
    assert ct.FloatType().is_compatible(ct.DoubleType())
    assert ct.StringType().is_compatible(ct.VarcharType())
    assert ct.DateType().is_compatible(ct.TimeType())

    assert not ct.StringType().is_compatible(ct.IntegerType())
    assert not ct.StringType().is_compatible(ct.BooleanType())
    assert not ct.StringType().is_compatible(ct.BinaryType())
    assert not ct.StringType().is_compatible(ct.BigIntType())
    assert not ct.StringType().is_compatible(ct.DateType())


def test_varchar_in_ast():
    """
    Test that varchar types support length as a parameter.
    """
    cast_expr = ast.Cast(
        data_type=ast.ColumnType("varchar(10)"),
        expression=ast.Column(ast.Name("abc")),
    )
    assert str(cast_expr) == "CAST(abc AS VARCHAR(10))"
