"""
Tests for ``datajunction.utils``.
"""

import logging
from pathlib import Path

import pytest
from pytest_mock import MockerFixture
from yarl import URL

from datajunction.typing import ColumnType
from datajunction.utils import (
    get_issue_url,
    get_more_specific_type,
    get_name_from_path,
    get_session,
    get_settings,
    setup_logging,
)


def test_setup_logging() -> None:
    """
    Test ``setup_logging``.
    """
    setup_logging("debug")
    assert logging.root.level == logging.DEBUG

    with pytest.raises(ValueError) as excinfo:
        setup_logging("invalid")
    assert str(excinfo.value) == "Invalid log level: invalid"


def test_get_session(mocker: MockerFixture) -> None:
    """
    Test ``get_session``.
    """
    mocker.patch("datajunction.utils.get_engine")
    Session = mocker.patch("datajunction.utils.Session")  # pylint: disable=invalid-name

    session = next(get_session())

    assert session == Session().__enter__.return_value


def test_get_settings(mocker: MockerFixture) -> None:
    """
    Test ``get_settings``.
    """
    mocker.patch("datajunction.utils.load_dotenv")
    Settings = mocker.patch(  # pylint: disable=invalid-name
        "datajunction.utils.Settings",
    )

    # should be already cached, since it's called by the Celery app
    get_settings()
    Settings.assert_not_called()


def test_get_name_from_path() -> None:
    """
    Test ``get_name_from_path``.
    """
    with pytest.raises(Exception) as excinfo:
        get_name_from_path(Path("/path/to/repository"), Path("/path/to/repository"))
    assert str(excinfo.value) == "Invalid path: /path/to/repository"

    with pytest.raises(Exception) as excinfo:
        get_name_from_path(
            Path("/path/to/repository"),
            Path("/path/to/repository/nodes"),
        )
    assert str(excinfo.value) == "Invalid path: /path/to/repository/nodes"

    with pytest.raises(Exception) as excinfo:
        get_name_from_path(
            Path("/path/to/repository"),
            Path("/path/to/repository/invalid/test.yaml"),
        )
    assert str(excinfo.value) == "Invalid path: /path/to/repository/invalid/test.yaml"

    assert (
        get_name_from_path(
            Path("/path/to/repository"),
            Path("/path/to/repository/nodes/test.yaml"),
        )
        == "test"
    )

    assert (
        get_name_from_path(
            Path("/path/to/repository"),
            Path("/path/to/repository/nodes/core/test.yaml"),
        )
        == "core.test"
    )

    assert (
        get_name_from_path(
            Path("/path/to/repository"),
            Path("/path/to/repository/nodes/dev.nodes/test.yaml"),
        )
        == "dev%2Enodes.test"
    )

    assert (
        get_name_from_path(
            Path("/path/to/repository"),
            Path("/path/to/repository/nodes/5%_nodes/test.yaml"),
        )
        == "5%25_nodes.test"
    )


def test_get_more_specific_type() -> None:
    """
    Test ``get_more_specific_type``.
    """
    assert (
        get_more_specific_type(ColumnType.STR, ColumnType.DATETIME)
        == ColumnType.DATETIME
    )
    assert get_more_specific_type(ColumnType.STR, ColumnType.INT) == ColumnType.INT
    assert get_more_specific_type(None, ColumnType.INT) == ColumnType.INT


def test_get_issue_url() -> None:
    """
    Test ``get_issue_url``.
    """
    assert get_issue_url() == URL(
        "https://github.com/DataJunction/datajunction/issues/new",
    )
    assert get_issue_url(
        baseurl=URL("https://example.org/"),
        title="Title with spaces",
        body="This is the body",
        labels=["help", "troubleshoot"],
    ) == URL(
        "https://example.org/?title=Title+with+spaces&"
        "body=This+is+the+body&labels=help,troubleshoot",
    )
