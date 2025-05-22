from unittest import mock
import pytest
from datajunction_server.models.dialect import (
    Dialect,
    DialectRegistry,
    register_dialect_plugin,
)
from datajunction_server.models.dialect import dialect_plugin
from datajunction_server.transpilation import (
    SQLTranspilationPlugin,
)
from datajunction_server.models.dialect import Dialect, DialectRegistry, dialect_plugin
from datajunction_server.models.metric import TranslatedSQL


@dialect_plugin("custom123")
class TestPlugin(SQLTranspilationPlugin):
    """
    Custom transpilation plugin for testing.
    """

    package_name = "custom_transpilation_plugin"

    def transpile_sql(
        self,
        query: str,
        *,
        input_dialect: Dialect | None = None,
        output_dialect: Dialect | None = None,
    ) -> str:
        return query + "\n\n-- transpiled"


def test_dialect_register_and_get_plugin():
    """
    Test the DialectRegistry register method.
    """
    DialectRegistry._registry.clear()
    DialectRegistry.register("dialect_a", TestPlugin)
    DialectRegistry.register("dialect_b", TestPlugin)
    plugin_a = DialectRegistry.get_plugin("dialect_a")
    plugin_b = DialectRegistry.get_plugin("dialect_b")
    assert plugin_a is TestPlugin
    assert plugin_b is TestPlugin
    listed = DialectRegistry.list()
    assert set(listed) == {"dialect_a", "dialect_b"}


def test_custom_sql() -> None:
    """
    Verify that the custom dialect plugin can be used to transpile SQL.
    """
    DialectRegistry.register("dialect_a", TestPlugin)

    translated_sql = TranslatedSQL.create(
        sql="SELECT 1",
        columns=[],
        dialect=Dialect("dialect_a"),
    )
    assert translated_sql.sql == "SELECT 1\n\n-- transpiled"


def test_dialect_enum():
    """
    Test the Dialect enum for valid and invalid values.
    """
    assert Dialect.SPARK == "spark"
    assert Dialect.TRINO == "trino"
    assert Dialect.DRUID == "druid"

    with pytest.raises(ValueError):
        Dialect("unknown_dialect")

    with pytest.raises(TypeError):
        Dialect(123)

    # Test dynamic creation of a dialect if registered
    DialectRegistry.register("custom", TestPlugin)
    assert Dialect("custom") == "custom"

    with pytest.raises(ValueError):
        Dialect("unregistered_dialect")


def test_dialect_registry_register_and_get_plugin():
    """
    Test registering and retrieving plugins in the DialectRegistry.
    """

    class MockPlugin:
        package_name = "mock_plugin"

    DialectRegistry.register("mock_dialect", MockPlugin)
    assert DialectRegistry.get_plugin("mock_dialect") == MockPlugin
    assert DialectRegistry.get_plugin("nonexistent_dialect") is None


def test_dialect_registry_list():
    """
    Test listing all registered dialects in the DialectRegistry.
    """

    class MockPlugin:
        pass

    DialectRegistry._registry.clear()  # Clear the registry for a clean test
    DialectRegistry.register("dialect1", MockPlugin)
    DialectRegistry.register("dialect2", MockPlugin)

    assert sorted(DialectRegistry.list()) == ["dialect1", "dialect2"]


def test_register_dialect_plugin(caplog):
    """
    Test the register_dialect_plugin function.
    """

    class MockPlugin:
        package_name = "mock_plugin"

    # Simulate the settings where the plugin is not included
    register_dialect_plugin("test_dialect", MockPlugin)
    assert DialectRegistry.get_plugin("test_dialect") is None
    caplog.set_level("WARNING")
    assert caplog.messages == [
        "Skipping plugin registration for 'test_dialect' (mock_plugin) "
        "(not in configured transpilation plugins: ['default', 'sqlglot'])",
    ]

    # Simulate adding the plugin to the settings
    mock_settings = mock.MagicMock()
    mock_settings.transpilation_plugins = ["default", "mock_plugin"]
    with mock.patch("datajunction_server.models.dialect.settings", mock_settings):
        register_dialect_plugin("test_dialect", MockPlugin)
        assert DialectRegistry.get_plugin("test_dialect") is MockPlugin
