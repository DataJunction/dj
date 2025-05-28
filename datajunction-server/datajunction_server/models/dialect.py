import logging

from pydantic import BaseModel
from datajunction_server.enum import StrEnum
from datajunction_server.utils import get_settings
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datajunction_server.transpilation import SQLTranspilationPlugin


logger = logging.getLogger(__name__)
settings = get_settings()


class Dialect(StrEnum):
    """
    SQL dialect
    """

    SPARK = "spark"
    TRINO = "trino"
    DRUID = "druid"

    @classmethod
    def _missing_(cls, value: object) -> "Dialect":
        """
        Create a Dialect enum member from a string, but only if it corresponds to a
        registered transpilation plugin in the plugin registry. If it has a corresponding
        plugin, a dynamic enum member is created for the dialect. If not, a ValueError is
        raised to prevent the use of unsupported dialects.
        """
        if isinstance(value, str):
            key = value.lower()
            if key in DialectRegistry._registry:
                new_member = str.__new__(cls, key)
                new_member._name_ = key.upper()
                new_member._value_ = key
                return new_member
            raise ValueError(
                f"Dialect '{value}' does not have a registered SQL transpilation plugin that "
                "supports it. Please configure a plugin on the server to use this dialect.",
            )
        raise TypeError(f"{value!r} is not a valid string for {cls.__name__}")


class DialectInfo(BaseModel):
    """
    Information about a SQL dialect and its associated plugin class.
    """

    name: str
    plugin_class: str


class DialectRegistry:
    """
    Registry for SQL dialect plugins.
    """

    _registry: dict[str, type["SQLTranspilationPlugin"]] = {}

    @classmethod
    def register(cls, dialect_name: str, plugin: type["SQLTranspilationPlugin"]):
        """
        Registers a new dialect plugin.
        """
        logger.info(
            "Registering plugin %s for dialect %s",
            plugin.__name__,
            dialect_name,
        )
        cls._registry[dialect_name.lower()] = plugin

    @classmethod
    def get_plugin(cls, dialect_name: str) -> type["SQLTranspilationPlugin"] | None:
        """
        Retrieves the plugin class for a given dialect name.
        """
        return cls._registry.get(dialect_name.lower())

    @classmethod
    def list(cls) -> list[str]:
        """
        A list of supported dialects.
        """
        return list(cls._registry.keys())


def register_dialect_plugin(name: str, plugin_cls: type["SQLTranspilationPlugin"]):
    """
    Registers a plugin for the given dialect. Skips registration if the plugin is not
    configured in the settings.
    """
    if plugin_cls.package_name not in settings.transpilation_plugins:
        logger.warning(
            "Skipping plugin registration for '%s' (%s) (not in configured transpilation plugins: %s)",
            name,
            plugin_cls.package_name,
            settings.transpilation_plugins,
        )
        return

    DialectRegistry.register(name, plugin_cls)


def dialect_plugin(name: str):
    """
    Decorator to register a dialect plugin. This decorator should be used on a class that
    inherits from SQLTranspilationPlugin.

    Example usage:

    @dialect_plugin("my_dialect")
    class MyDialectPlugin(SQLTranspilationPlugin):
        def transpile_sql(self, query: str, input_dialect: str = None, output_dialect: str = None):
            # Custom transpilation logic here
            return query
    """

    def wrapper(cls):
        register_dialect_plugin(name, cls)
        return cls

    return wrapper
