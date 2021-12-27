"""
Tests for ``datajunction.utils``.
"""

import logging
from pathlib import Path

import pytest
from pyfakefs.fake_filesystem import FakeFilesystem

from datajunction.models import Config
from datajunction.utils import find_directory, load_config, setup_logging


def test_setup_logging() -> None:
    """
    Test ``setup_logging``.
    """
    setup_logging("debug")
    assert logging.root.level == logging.DEBUG

    with pytest.raises(ValueError) as excinfo:
        setup_logging("invalid")
    assert str(excinfo.value) == "Invalid log level: invalid"


def test_find_directory(fs: FakeFilesystem) -> None:
    """
    Test ``find_directory``.
    """
    fs.create_dir("/path/to/repository/nodes/core")
    fs.create_file("/path/to/repository/dj.yaml")

    path = find_directory(Path("/path/to/repository/nodes/core"))
    assert path == Path("/path/to/repository")

    with pytest.raises(SystemExit) as excinfo:
        find_directory(Path("/path/to"))
    assert str(excinfo.value) == "No configuration found!"


def test_load_config(repository: Path, config: Config) -> None:
    """
    Test ``load_config``.
    """
    config = load_config(repository)
    assert config.dict() == {"index": "sqlite:///dj.db"}

    with pytest.raises(SystemExit) as excinfo:
        load_config(Path("/path/to"))
    assert str(excinfo.value) == "No configuration found!"
