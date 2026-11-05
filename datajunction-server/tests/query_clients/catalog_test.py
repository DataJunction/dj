"""Tests for physical catalog resolution in direct query clients."""

from datajunction_server.models.dialect import Dialect
from datajunction_server.query_clients.catalog import (
    catalog_from_engine_uri,
    replace_catalog_alias,
)


def test_unsupported_engine_uri_has_no_physical_catalog():
    assert (
        catalog_from_engine_uri("postgresql://host/database", Dialect.POSTGRES) is None
    )


def test_matching_catalog_alias_keeps_sql_exactly_as_submitted():
    sql = "SELECT * FROM warehouse.sales.orders"
    assert (
        replace_catalog_alias(sql, "warehouse", "warehouse", Dialect.SNOWFLAKE) == sql
    )
