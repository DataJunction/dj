"""Tests the transpilation plugins."""
import pytest
from pytest_mock import MockerFixture

from datajunction_server.errors import DJPluginNotFoundException
from datajunction_server.models.engine import Dialect
from datajunction_server.models.metric import TranslatedSQL
from datajunction_server.models.sql import GeneratedSQL, NodeNameVersion
from datajunction_server.transpilation import get_transpilation_plugin


class MockSettings:  # pylint: disable=too-few-public-methods
    """
    Mock settings object
    """

    sql_transpilation_library = "sqlglot"


def test_get_transpilation_plugin() -> None:
    """
    Test ``get_transpilation_plugin``
    """
    # Will raise an error with for an unknown transpilation library
    with pytest.raises(DJPluginNotFoundException) as excinfo:
        get_transpilation_plugin("somepackage")
    assert "No SQL transpilation plugin found for package `somepackage`!" in str(
        excinfo,
    )

    package_name = "sqlglot"
    known = get_transpilation_plugin(package_name)
    assert known.package_name == package_name
    assert known.transpile_sql("1") == "1"
    assert (
        known.transpile_sql(
            "1",
            input_dialect=Dialect.SPARK,
            output_dialect=Dialect.TRINO,
        )
        == "1"
    )
    assert known.transpile_sql("1", output_dialect=Dialect.TRINO) == "1"


def test_translated_sql(mocker: MockerFixture) -> None:
    """
    Verify that the TranslatedSQL object will call the configured transpilation plugin
    """
    mock_settings = mocker.MagicMock()
    mock_settings.return_value = MockSettings()
    mocker.patch("datajunction_server.models.metric.get_settings", mock_settings)
    translated_sql = TranslatedSQL(
        sql="1",
        columns=[],
        dialect=Dialect.SPARK,
    )
    assert translated_sql.sql == "1"
    generated_sql = GeneratedSQL(
        node=NodeNameVersion(name="a", version="v1.0"),
        sql="1",
        columns=[],
        dialect=Dialect.SPARK,
    )
    assert generated_sql.sql == "1"


def test_druid_sql(mocker: MockerFixture) -> None:
    """
    Verify that the TranslatedSQL object will call the configured transpilation plugin
    """
    mock_settings = mocker.MagicMock()
    mock_settings.return_value = MockSettings()
    mocker.patch("datajunction_server.models.metric.get_settings", mock_settings)
    translated_sql = TranslatedSQL(
        sql="SELECT 1",
        columns=[],
        dialect=Dialect.DRUID,
    )
    assert translated_sql.sql == "SELECT 1"
    generated_sql = GeneratedSQL(
        node=NodeNameVersion(name="a", version="v1.0"),
        sql="SELECT 1",
        columns=[],
        dialect=Dialect.DRUID,
    )
    assert generated_sql.sql == "SELECT 1"
