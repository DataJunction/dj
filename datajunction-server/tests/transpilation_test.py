"""Tests the transpilation plugins."""

from unittest import mock

import pytest

from datajunction_server.models.dialect import Dialect, DialectRegistry
from datajunction_server.models.engine import Dialect
from datajunction_server.models.metric import TranslatedSQL
from datajunction_server.models.sql import GeneratedSQL, NodeNameVersion
from datajunction_server.transpilation import (
    SQLGlotTranspilationPlugin,
    SQLTranspilationPlugin,
    transpile_sql,
)


def test_get_transpilation_plugin(regular_settings) -> None:
    """
    Test ``get_transpilation_plugin``
    """
    plugin = DialectRegistry.get_plugin(Dialect.SPARK.value)
    assert plugin == SQLGlotTranspilationPlugin
    assert plugin.package_name == "sqlglot"

    plugin = DialectRegistry.get_plugin(Dialect.TRINO.value)
    assert plugin == SQLGlotTranspilationPlugin

    plugin = DialectRegistry.get_plugin(Dialect.DRUID.value)
    assert plugin == SQLGlotTranspilationPlugin


def test_get_transpilation_plugin_unknown() -> None:
    """
    Test ``get_transpilation_plugin`` with an unknown plugin
    """
    plugin = DialectRegistry.get_plugin("random")
    assert not plugin


def test_translated_sql() -> None:
    """
    Verify that the TranslatedSQL object will call the configured transpilation plugin
    """
    translated_sql = TranslatedSQL.create(
        sql="1",
        columns=[],
        dialect=Dialect.SPARK,
    )
    assert translated_sql.sql == "1"
    generated_sql = GeneratedSQL.create(
        node=NodeNameVersion(name="a", version="v1.0"),
        sql="1",
        columns=[],
        dialect=Dialect.SPARK,
    )
    assert generated_sql.sql == "1"


def test_druid_sql(regular_settings) -> None:
    """
    Verify that the TranslatedSQL object will call the configured transpilation plugin
    """
    translated_sql = TranslatedSQL.create(
        sql="SELECT 1",
        columns=[],
        dialect=Dialect.DRUID,
    )
    assert translated_sql.sql == "SELECT\n  1"
    generated_sql = GeneratedSQL.create(
        node=NodeNameVersion(name="a", version="v1.0"),
        sql="SELECT 1",
        columns=[],
        dialect=Dialect.DRUID,
    )
    assert generated_sql.sql == "SELECT\n  1"


def test_sqlglot_transpile_success():
    with mock.patch(
        "datajunction_server.transpilation.settings.transpilation_plugins",
        return_value=["sqlglot"],
    ):
        plugin = SQLGlotTranspilationPlugin()
        DialectRegistry.register("custom123", SQLGlotTranspilationPlugin)
        result = plugin.transpile_sql(
            "SELECT * FROM bar",
            input_dialect=Dialect.TRINO,
            output_dialect=Dialect.SPARK,
        )
        assert result == "SELECT\n  *\nFROM bar"

        result = plugin.transpile_sql(
            "SELECT * FROM bar",
            input_dialect=Dialect.TRINO,
            output_dialect=Dialect("custom123"),
        )
        assert result == "SELECT * FROM bar"


def test_sqlglot_transpile_uses_schema_to_annotate_types():
    """Schema types are attached before dialect-specific SQL generation."""
    plugin = SQLGlotTranspilationPlugin()
    result = plugin.transpile_sql(
        "SELECT t.payload['name'] FROM catalog.schema.events AS t",
        input_dialect=Dialect.SPARK,
        output_dialect=Dialect.BIGQUERY,
        schema={
            "catalog": {
                "schema": {
                    "events": {"payload": "struct<name string>"},
                },
            },
        },
    )

    assert result == (
        "SELECT\n  `t`.`payload`.name AS `name`\n"
        "FROM `catalog`.`schema`.`events` AS `t`"
    )


# TODO: Remove this skip after upgrading to a SQLGlot release containing the
# map EXPLODE type-annotation fix.
@pytest.mark.skip(reason="Requires the unreleased SQLGlot map EXPLODE fix")
def test_sqlglot_transpile_map_explode_with_schema():
    """A typed Spark map EXPLODE becomes a two-column Trino UNNEST."""
    plugin = SQLGlotTranspilationPlugin()
    result = plugin.transpile_sql(
        """SELECT
  CAST(test_id AS BIGINT) AS test_id,
  key,
  value
FROM (
  SELECT
    test_id,
    explode(map_column) AS (key, value)
  FROM some_table T
)""",
        input_dialect=Dialect.SPARK,
        output_dialect=Dialect.TRINO,
        schema={
            "some_table": {
                "test_id": "INT",
                "map_column": "MAP<INT, VARCHAR>",
            },
        },
    )

    assert (
        result
        == '''SELECT
  TRY_CAST("_0"."test_id" AS BIGINT) AS "test_id",
  "_0"."key" AS "key",
  "_0"."value" AS "value"
FROM (
  SELECT
    "t"."test_id" AS "test_id",
    _u_2."key" AS "key",
    _u_2."value" AS "value"
  FROM "some_table" AS "t"
  CROSS JOIN UNNEST("t"."map_column") AS _u_2("key", "value")
) AS "_0"'''
    )


def test_default_transpile_success():
    with mock.patch(
        "datajunction_server.transpilation.settings.transpilation_plugins",
        return_value=["default"],
    ):
        plugin = SQLTranspilationPlugin()
        DialectRegistry.register("custom123", SQLTranspilationPlugin)
        result = plugin.transpile_sql(
            "SELECT * FROM bar",
            input_dialect=Dialect.TRINO,
            output_dialect=Dialect.SPARK,
        )
        assert result == "SELECT * FROM bar"

        result = plugin.transpile_sql(
            "SELECT * FROM bar",
            input_dialect=Dialect.TRINO,
            output_dialect=Dialect("custom123"),
        )
        assert result == "SELECT * FROM bar"


def test_transpile_sql():
    with mock.patch(
        "datajunction_server.transpilation.settings.transpilation_plugins",
        return_value=["default"],
    ):
        assert transpile_sql("1", dialect=Dialect.SPARK) == "1"
        assert transpile_sql("SELECT 1", dialect=None) == "SELECT 1"


def test_translated_sql_no_dialect() -> None:
    """
    Verify that the TranslatedSQL object will call the configured transpilation plugin
    """
    translated_sql = TranslatedSQL.create(
        sql="1",
        columns=[],
        dialect=None,
    )
    assert translated_sql.sql == "1"
    assert TranslatedSQL.validate_dialect(translated_sql.dialect) is None
