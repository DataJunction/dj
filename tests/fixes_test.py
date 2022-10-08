"""
Tests for ``dj.fixes``.
"""
# pylint: disable=invalid-name

import pytest
from pytest_mock import MockerFixture

from dj.fixes import patch_druid_get_columns


def test_patch_druid_get_columns(mocker: MockerFixture) -> None:
    """
    Test ``patch_druid_get_columns``.
    """
    pytest.importorskip("pydruid")

    DruidDialect = mocker.patch("dj.fixes.DruidDialect")
    connection = mocker.MagicMock()

    mocker.patch("dj.fixes.PYDRUID_INSTALLED", new=False)
    patch_druid_get_columns()
    DruidDialect.assert_not_called()

    mocker.patch("dj.fixes.PYDRUID_INSTALLED", new=True)
    patch_druid_get_columns()

    DruidDialect.get_columns(None, connection, "table_name", "schema")
    assert (
        str(connection.execute.mock_calls[0].args[0])
        == """
SELECT COLUMN_NAME,
       DATA_TYPE,
       IS_NULLABLE,
       COLUMN_DEFAULT
  FROM INFORMATION_SCHEMA.COLUMNS
 WHERE TABLE_NAME = 'table_name'
 AND TABLE_SCHEMA = 'schema'"""
    )

    connection.execute.reset_mock()
    DruidDialect.get_columns(None, connection, "table_name")
    assert (
        str(connection.execute.mock_calls[0].args[0])
        == """
SELECT COLUMN_NAME,
       DATA_TYPE,
       IS_NULLABLE,
       COLUMN_DEFAULT
  FROM INFORMATION_SCHEMA.COLUMNS
 WHERE TABLE_NAME = 'table_name'
"""
    )
