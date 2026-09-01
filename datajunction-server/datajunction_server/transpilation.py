"""SQL transpilation plugins manager."""

import logging
from typing import Any

import sqlglot
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.qualify import qualify

from datajunction_server.models.dialect import DialectRegistry, dialect_plugin
from datajunction_server.models.engine import Dialect
from datajunction_server.utils import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)


@dialect_plugin(Dialect.SPARK.value)
class SQLTranspilationPlugin:
    """
    SQL transpilation plugin base class. To add support for a new SQL transpilation library,
    implement this class with a custom `transpile_sql` method that works for the library. The
    base implementation here is a no-op that returns the query unchanged.

    ``package_name`` identifies the plugin and is matched against
    ``settings.transpilation_plugins`` to decide whether the plugin is registered
    (see ``register_dialect_plugin``).
    """

    package_name: str | None = "default"

    def transpile_sql(
        self,
        query: str,
        *,
        input_dialect: Dialect | None = None,
        output_dialect: Dialect | None = None,
    ) -> str:
        """Transpile a given SQL query using the specific library."""
        return query


@dialect_plugin(Dialect.BIGQUERY.value)
@dialect_plugin(Dialect.CLICKHOUSE.value)
@dialect_plugin(Dialect.DRUID.value)
@dialect_plugin(Dialect.DUCKDB.value)
@dialect_plugin(Dialect.POSTGRES.value)
@dialect_plugin(Dialect.REDSHIFT.value)
@dialect_plugin(Dialect.SNOWFLAKE.value)
@dialect_plugin(Dialect.SPARK.value)
@dialect_plugin(Dialect.SQLITE.value)
@dialect_plugin(Dialect.TRINO.value)
class SQLGlotTranspilationPlugin(SQLTranspilationPlugin):
    """
    Implement sqlglot as a transpilation option
    """

    package_name: str = "sqlglot"

    def transpile_sql(
        self,
        query: str,
        *,
        input_dialect: Dialect | None = None,
        output_dialect: Dialect | None = None,
        schema: dict[str, Any] | None = None,
    ) -> str:
        """
        Transpile a given SQL query using the specific library.
        """
        # Check to make sure that the output dialect is supported by sqlglot before transpiling
        if (
            input_dialect
            and output_dialect
            and output_dialect.name in dir(sqlglot.dialects.Dialects)
        ):
            input_name = str(input_dialect.name.lower())
            expression = sqlglot.parse_one(query, read=input_name)
            if schema:
                expression = qualify(expression, schema=schema, dialect=input_name)
            annotate_types(expression, schema=schema, dialect=input_name)
            return expression.sql(
                dialect=str(output_dialect.name.lower()),
                pretty=True,
            )
        return query


def transpile_sql(
    sql: str,
    dialect: Dialect | None = None,
    schema: dict[str, Any] | None = None,
) -> str:
    """
    Transpile SQL to a target dialect.

    This function performs two steps:
    1. Translate canonical function names to dialect-specific names
    2. Use SQLGlot (if available) for general SQL transpilation

    Args:
        sql: The SQL string to transpile
        dialect: The target SQL dialect (if None, returns SQL unchanged)
        schema: Optional SQLGlot schema mapping used to annotate AST types

    Returns:
        The transpiled SQL string
    """
    if dialect:
        # Use SQLGlot for general transpilation
        if plugin_class := DialectRegistry.get_plugin(  # pragma: no cover
            dialect.name.lower(),
        ):
            plugin = plugin_class()
            kwargs: dict[str, Any] = {
                "input_dialect": Dialect.SPARK,
                "output_dialect": dialect,
            }
            # Schema annotation is a SQLGlot capability. Keeping it out of calls
            # to other plugins preserves compatibility with their existing API.
            if schema is not None and isinstance(plugin, SQLGlotTranspilationPlugin):
                kwargs["schema"] = schema
            return plugin.transpile_sql(
                sql,
                **kwargs,
            )
    return sql
