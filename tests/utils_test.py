"""
Tests for ``datajunction.utils``.
"""

import logging

import pytest
from pytest_mock import MockerFixture

from datajunction.utils import get_session, setup_logging


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

    assert session == Session.return_value.__enter__.return_value
