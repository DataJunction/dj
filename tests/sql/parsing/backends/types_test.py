"""
Tests for types
"""
import dj.sql.parsing.types as ct


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
