"""Tests the transpilation plugins."""

from unittest import mock

from datajunction_server.models.engine import Dialect
from datajunction_server.models.metric import TranslatedSQL
from datajunction_server.models.sql import GeneratedSQL, NodeNameVersion
from datajunction_server.transpilation import (
    SQLGlotTranspilationPlugin,
    SQLTranspilationPlugin,
    transpile_sql,
)
from datajunction_server.models.dialect import Dialect, DialectRegistry


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
