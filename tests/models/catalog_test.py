"""
Tests for ``dj.models.catalog``.
"""

from dj.models.catalog import Catalog


def test_catalog_str_repr():
    """
    Test that a catalog instance renders appropriately to a string
    """
    spark = Catalog(name="spark")
    assert str(spark) == "spark"
    trino = Catalog(name="trino")
    assert str(trino) == "trino"
    druid = Catalog(name="druid")
    assert str(druid) == "druid"
