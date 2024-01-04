"""
Tests for ``datajunction_server.models.catalog``.
"""

from datajunction_server.database.catalog import Catalog


def test_catalog_str_and_hash():
    """
    Test that a catalog instance works properly with str and hash
    """
    spark = Catalog(id=1, name="spark")
    assert str(spark) == "spark"
    assert hash(spark) == 1
    trino = Catalog(id=2, name="trino")
    assert str(trino) == "trino"
    assert hash(trino) == 2
    druid = Catalog(id=3, name="druid")
    assert str(druid) == "druid"
    assert hash(druid) == 3
