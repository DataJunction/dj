

from pathlib import Path
import tempfile

import pytest
import yaml

from datajunction.cli import add_database
from datajunction.errors import DJException

@pytest.mark.asyncio
async def test_add_database_minimum_requirements() -> None:
    """
    Test adding a database with the minimum required arguments
    """
    with tempfile.TemporaryDirectory() as tmpdirname:
        await add_database.run(
            Path(tmpdirname),
            database="testdb",
            uri="testdb://test",
        )
        with open(Path(tmpdirname) / Path("databases/testdb.yaml"), "r") as f:
            assert yaml.safe_load(f) == {
                "URI": "testdb://test",
                "async_": False,
                "cost": 1.0,  # default
                "description": "",  # default
                "read-only": True  # default
            }

@pytest.mark.asyncio
async def test_add_database() -> None:
    """
    Test adding a database using required and optional arguments
    """
    with tempfile.TemporaryDirectory() as tmpdirname:
        await add_database.run(
            Path(tmpdirname),
            database="testdb",
            uri="testdb://test",
            description="This is a description",
            read_only=True,
            cost=11.0,
        )
        with open(Path(tmpdirname) / Path("databases/testdb.yaml"), "r") as f:
            assert yaml.safe_load(f) == {
                "URI": "testdb://test",
                "async_": False,
                "cost": 11.0,
                "description": "This is a description",
                "read-only": True
            }

@pytest.mark.asyncio
async def test_add_database_raise_config_already_exists() -> None:
    """
    Test raising an exception when adding a database that already exists
    """
    with tempfile.TemporaryDirectory() as tmpdirname:
        await add_database.run(
            Path(tmpdirname),
            database="testdb",
            uri="testdb://test",
            description="This is a description",
            read_only=True,
            cost=11.0
        )
        with pytest.raises(DJException) as exc_info:
            await add_database.run(
                Path(tmpdirname),
                database="testdb",
                uri="testdb://test",
                description="This is a description",
                read_only=True,
                cost=11.0
            )
        
        assert "Database configuration already exists" in str(exc_info.value)