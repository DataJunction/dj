"""
Tests for ``datajunction.cli.add_database``.
"""
# pylint: disable=invalid-name

from pathlib import Path

import pytest
import yaml
from pyfakefs.fake_filesystem import FakeFilesystem

from datajunction.cli import add_database
from datajunction.errors import DJException


@pytest.mark.asyncio
async def test_add_database_minimum_requirements(fs: FakeFilesystem) -> None:
    """
    Test adding a database with the minimum required arguments
    """
    test_repo = "/foo"
    fs.create_dir(test_repo)
    await add_database.run(
        Path(test_repo),
        database="testdb",
        uri="testdb://test",
    )
    with open(
        Path(test_repo) / Path("databases/testdb.yaml"),
        "r",
        encoding="utf-8",
    ) as f:
        assert yaml.safe_load(f) == {
            "URI": "testdb://test",
            "async_": False,
            "cost": 1.0,  # default
            "description": "",  # default
            "read-only": True,  # default
        }


@pytest.mark.asyncio
async def test_add_database(fs: FakeFilesystem) -> None:
    """
    Test adding a database using required and optional arguments
    """
    test_repo = "/foo"
    fs.create_dir(test_repo)
    await add_database.run(
        Path(test_repo),
        database="testdb",
        uri="testdb://test",
        description="This is a description",
        read_only=True,
        cost=11.0,
    )
    with open(
        Path(test_repo) / Path("databases/testdb.yaml"),
        "r",
        encoding="utf-8",
    ) as f:
        assert yaml.safe_load(f) == {
            "URI": "testdb://test",
            "async_": False,
            "cost": 11.0,
            "description": "This is a description",
            "read-only": True,
        }


@pytest.mark.asyncio
async def test_add_database_raise_config_already_exists(fs: FakeFilesystem) -> None:
    """
    Test raising an exception when adding a database that already exists
    """
    test_repo = "/foo"
    fs.create_dir(test_repo)
    await add_database.run(
        Path(test_repo),
        database="testdb",
        uri="testdb://test",
        description="This is a description",
        read_only=True,
        cost=11.0,
    )
    with pytest.raises(DJException) as exc_info:
        await add_database.run(
            Path(test_repo),
            database="testdb",
            uri="testdb://test",
            description="This is a description",
            read_only=True,
            cost=11.0,
        )

    assert "Database configuration already exists" in str(exc_info.value)
