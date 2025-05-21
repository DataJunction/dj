"""SQL transpilation plugins manager."""

import importlib
from typing import Optional
import logging

from datajunction_server.models.engine import Dialect
from datajunction_server.models.dialect import DialectRegistry, dialect_plugin
from datajunction_server.utils import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)


@dialect_plugin(Dialect.SPARK.value)
class SQLTranspilationPlugin:
    """
    SQL transpilation plugin base class. To add support for a new SQL transpilation library,
    implement this class with a custom `transpile_sql` method that works for the library. The
    base implementation here will handle checking that the package exists before loading and
    using it, or skipping transpilation if the package cannot be loaded.
    """

    package_name: Optional[str] = "default"

    def __init__(self):
        """
        Load the package
        """
        self.check_package()

    def check_package(self):
        """
        Check that the selected SQL transpilation package is installed and loads it.
        """
        if self.package_name:  # pragma: no cover
            try:
                importlib.import_module(self.package_name)
            except ImportError:
                logger.warning(
                    "The SQL transpilation package is not installed: %s",
                    self.package_name,
                )

    def transpile_sql(
        self,
        query: str,
        *,
        input_dialect: Optional[Dialect] = None,
        output_dialect: Optional[Dialect] = None,
    ) -> str:
        """Transpile a given SQL query using the specific library."""
        return query


@dialect_plugin(Dialect.SPARK.value)
@dialect_plugin(Dialect.TRINO.value)
@dialect_plugin(Dialect.DRUID.value)
class SQLGlotTranspilationPlugin(SQLTranspilationPlugin):
    """
    Implement sqlglot as a transpilation option
    """

    package_name: str = "sqlglot"

    def transpile_sql(
        self,
        query: str,
        *,
        input_dialect: Optional[Dialect] = None,
        output_dialect: Optional[Dialect] = None,
    ) -> str:
        """
        Transpile a given SQL query using the specific library.
        """
        import sqlglot

        # Check to make sure that the output dialect is supported by sqlglot before transpiling
        if (
            input_dialect
            and output_dialect
            and output_dialect.name in dir(sqlglot.dialects.Dialects)
        ):
            value = sqlglot.transpile(
                query,
                read=str(input_dialect.name.lower()),  # type: ignore
                write=str(output_dialect.name.lower()),  # type: ignore
                pretty=True,
            )[0]
            return value
        return query


def transpile_sql(
    sql: str,
    dialect: Dialect | None = None,
) -> str:
    if dialect:
        if plugin_class := DialectRegistry.get_plugin(  # pragma: no cover
            dialect.name.lower(),
        ):
            plugin = plugin_class()
            return plugin.transpile_sql(
                sql,
                input_dialect=Dialect.SPARK,
                output_dialect=dialect,
            )
    return sql
