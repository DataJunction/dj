"""
Tests for ``dj.utils``.
"""

import logging
from pathlib import Path

import pytest
from pytest_mock import MockerFixture
from sqlalchemy.engine.url import make_url
from yarl import URL

from dj.config import Settings
from dj.typing import ColumnType
from dj.utils import (
    flatten,
    get_engine,
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
    mocker.patch("dj.utils.get_engine")
    Session = mocker.patch("dj.utils.Session")  # pylint: disable=invalid-name

    session = next(get_session())

    assert session == Session().__enter__.return_value


def test_get_settings(mocker: MockerFixture) -> None:
    """
    Test ``get_settings``.
    """
    mocker.patch("dj.utils.load_dotenv")
    Settings = mocker.patch(  # pylint: disable=invalid-name, redefined-outer-name
        "dj.utils.Settings",
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
        "https://github.com/DataJunction/dj/issues/new",
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


def test_get_engine(mocker: MockerFixture, settings: Settings) -> None:
    """
    Test ``get_engine``.
    """
    mocker.patch("dj.utils.get_settings", return_value=settings)
    engine = get_engine()
    assert engine.url == make_url("sqlite://")


def test_flatten():
    """
    Test ``flatten``
    """
    assert list(
        flatten([1, {1, 2, 3}, range(5), (8, (18, [4, iter(range(9))], [10]))]),
    ) == [1, 1, 2, 3, range(0, 5), 8, 18, 4, 0, 1, 2, 3, 4, 5, 6, 7, 8, 10]
