"""
Tests for ``djqs.utils``.
"""

import logging

import pytest
from pytest_mock import MockerFixture

from djqs.utils import get_settings, setup_logging


def test_setup_logging() -> None:
    """
    Test ``setup_logging``.
    """
    setup_logging("debug")
    assert logging.root.level == logging.DEBUG

    with pytest.raises(ValueError) as excinfo:
        setup_logging("invalid")
    assert str(excinfo.value) == "Invalid log level: invalid"


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
