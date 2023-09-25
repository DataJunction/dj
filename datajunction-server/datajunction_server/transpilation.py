"""SQL transpilation plugins manager."""
import importlib
from abc import ABC, abstractmethod
from typing import Optional

from datajunction_server.errors import DJException, DJPluginNotFoundException
from datajunction_server.models.engine import Dialect


class SQLTranspilationPlugin(ABC):
    """
    SQL transpilation plugin base class. To add support for a new SQL transpilation library,
    implement this class with a custom `transpile_sql` method that works for the library. The
    base implementation here will handle checking that the package exists before loading and
    using it, or skipping transpilation if the package cannot be loaded.
    """

    package_name: Optional[str] = None

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
            except ImportError as import_err:
                raise DJException(
                    message=f"Not installed: {self.package_name}",
                ) from import_err

    @abstractmethod
    def transpile_sql(
        self,
        query: str,
        *,
        input_dialect: Optional[Dialect] = None,
        output_dialect: Optional[Dialect] = None,
    ) -> str:
        """Transpile a given SQL query using the specific library."""


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
        import sqlglot  # pylint: disable=import-outside-toplevel,import-error

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


def get_transpilation_plugin(package_name: str) -> SQLTranspilationPlugin:
    """
    Retrieves the configured SQL transpilation plugin
    """
    transpilation_plugins = [
        clazz
        for clazz in SQLTranspilationPlugin.__subclasses__()
        if clazz.package_name == package_name
    ]
    if not transpilation_plugins:
        raise DJPluginNotFoundException(
            message=f"No SQL transpilation plugin found for package `{package_name}`!",
        )
    return transpilation_plugins[0]()  # type: ignore
