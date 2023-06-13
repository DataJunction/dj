"""
Tests for ``djqs.utils``.
"""

import logging

import pytest
from pytest_mock import MockerFixture
from sqlalchemy.engine.url import make_url

from djqs.config import Settings
from djqs.utils import get_metadata_engine, get_session, get_settings, setup_logging


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
    mocker.patch("djqs.utils.get_metadata_engine")
    Session = mocker.patch("djqs.utils.Session")  # pylint: disable=invalid-name

    session = next(get_session())

    assert session == Session().__enter__.return_value


def test_get_settings(mocker: MockerFixture) -> None:
    """
    Test ``get_settings``.
    """
    mocker.patch("djqs.utils.load_dotenv")
    Settings = mocker.patch(  # pylint: disable=invalid-name, redefined-outer-name
        "djqs.utils.Settings",
    )

    # should be already cached, since it's called by the Celery app
    get_settings()
    Settings.assert_not_called()


def test_get_metadata_engine(mocker: MockerFixture, settings: Settings) -> None:
    """
    Test ``get_metadata_engine``.
    """
    mocker.patch("djqs.utils.get_settings", return_value=settings)
    engine = get_metadata_engine()
    assert engine.url == make_url("sqlite://")
