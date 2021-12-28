"""
Tests for ``datajunction.utils``.
"""

import logging
from pathlib import Path

import pytest
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from datajunction.utils import find_directory, get_session, setup_logging


def test_setup_logging() -> None:
    """
    Test ``setup_logging``.
    """
    setup_logging("debug")
    assert logging.root.level == logging.DEBUG

    with pytest.raises(ValueError) as excinfo:
        setup_logging("invalid")
    assert str(excinfo.value) == "Invalid log level: invalid"


def test_find_directory(fs: FakeFilesystem) -> None:  # pylint: disable=invalid-name
    """
    Test ``find_directory``.
    """
    fs.create_dir("/path/to/repository/nodes/core")
    fs.create_file("/path/to/repository/.env")

    path = find_directory(Path("/path/to/repository/nodes/core"))
    assert path == Path("/path/to/repository")

    with pytest.raises(SystemExit) as excinfo:
        find_directory(Path("/path/to"))
    assert str(excinfo.value) == "No configuration found!"


def test_get_session(mocker: MockerFixture) -> None:
    """
    Test ``get_session``.
    """
    mocker.patch("datajunction.utils.get_engine")
    Session = mocker.patch("datajunction.utils.Session")  # pylint: disable=invalid-name

    session = next(get_session())

    assert session == Session.return_value.__enter__.return_value
