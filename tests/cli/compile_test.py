"""
Tests for ``datajunction.cli.compile``.
"""
# pylint: disable=redefined-outer-name, invalid-name

from datetime import datetime, timezone
from operator import itemgetter
from pathlib import Path

import pytest
import sqlalchemy
from freezegun import freeze_time
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture
from sqlmodel import Session

from datajunction.cli.compile import (
    get_columns,
    get_more_specific_type,
    index_databases,
    index_nodes,
    load_data,
    run,
)
from datajunction.models import Column, Database


def test_get_more_specific_type() -> None:
    """
    Test ``get_more_specific_type``.
    """
    assert get_more_specific_type("str", "datetime") == "datetime"
    assert get_more_specific_type("str", "int") == "int"
    assert get_more_specific_type(None, "int") == "int"


def test_load_data(fs: FakeFilesystem) -> None:
    """
    Test ``load_data``.
    """
    fs.create_file(
        "/path/to/repository/example.yaml",
        contents="foo: bar",
    )
    assert load_data(Path("/path/to/repository/example.yaml")) == {"foo": "bar"}


@pytest.mark.asyncio
async def test_index_databases(repository: Path, session: Session) -> None:
    """
    Test ``index_databases``.
    """
    with freeze_time("2021-01-01T00:00:00Z"):
        Path("/path/to/repository/databases/druid.yaml").touch()
        Path("/path/to/repository/databases/postgres.yaml").touch()
        Path("/path/to/repository/databases/gsheets.yaml").touch()

    with freeze_time("2021-01-02T00:00:00Z"):
        databases = await index_databases(repository, session)

    configs = [database.dict(exclude={"id": True}) for database in databases]
    assert sorted(configs, key=itemgetter("name")) == [
        {
            "async_": False,
            "created_at": datetime(2021, 1, 2, 0, 0, tzinfo=timezone.utc),
            "updated_at": datetime(2021, 1, 2, 0, 0, tzinfo=timezone.utc),
            "name": "druid",
            "description": "An Apache Druid database",
            "URI": "druid://localhost:8082/druid/v2/sql/",
            "read_only": True,
        },
        {
            "async_": False,
            "created_at": datetime(2021, 1, 2, 0, 0, tzinfo=timezone.utc),
            "updated_at": datetime(2021, 1, 2, 0, 0, tzinfo=timezone.utc),
            "name": "gsheets",
            "description": "A Google Sheets connector",
            "URI": "gsheets://",
            "read_only": True,
        },
        {
            "async_": False,
            "created_at": datetime(2021, 1, 2, 0, 0, tzinfo=timezone.utc),
            "updated_at": datetime(2021, 1, 2, 0, 0, tzinfo=timezone.utc),
            "name": "postgres",
            "description": "A Postgres database",
            "URI": "postgresql://username:FoolishPassword@localhost:5433/examples",
            "read_only": False,
        },
    ]

    # update the Druid database and reindex
    with freeze_time("2021-01-03T00:00:00Z"):
        Path("/path/to/repository/databases/druid.yaml").touch()
        databases = await index_databases(repository, session)
    databases = sorted(databases, key=lambda database: database.name)

    assert [(database.name, database.updated_at) for database in databases] == [
        ("druid", datetime(2021, 1, 3, 0, 0, tzinfo=timezone.utc)),
        ("gsheets", datetime(2021, 1, 2, 0, 0, tzinfo=timezone.utc)),
        ("postgres", datetime(2021, 1, 2, 0, 0, tzinfo=timezone.utc)),
    ]

    # test that a missing timezone is treated as UTC
    databases[0].updated_at = databases[0].updated_at.replace(tzinfo=None)
    with freeze_time("2021-01-03T00:00:00Z"):
        databases = await index_databases(repository, session)
    databases = sorted(databases, key=lambda database: database.name)

    assert [(database.name, database.updated_at) for database in databases] == [
        ("druid", datetime(2021, 1, 3, 0, 0, tzinfo=timezone.utc)),
        ("gsheets", datetime(2021, 1, 2, 0, 0, tzinfo=timezone.utc)),
        ("postgres", datetime(2021, 1, 2, 0, 0, tzinfo=timezone.utc)),
    ]


def test_get_columns(mocker: MockerFixture) -> None:
    """
    Test ``get_columns``.
    """
    mocker.patch("datajunction.cli.compile.create_engine")
    inspect = mocker.patch("datajunction.cli.compile.inspect")
    inspect.return_value.get_columns.side_effect = [
        [
            {"name": "ds", "type": sqlalchemy.sql.sqltypes.String()},
            {"name": "cnt", "type": sqlalchemy.sql.sqltypes.Integer()},
        ],
        [
            {"name": "ds", "type": sqlalchemy.sql.sqltypes.DateTime()},
            {"name": "cnt", "type": sqlalchemy.sql.sqltypes.Float()},
        ],
    ]

    representations = [mocker.MagicMock(), mocker.MagicMock()]
    assert get_columns(representations) == [
        Column(id=None, name="ds", type="datetime"),
        Column(id=None, name="cnt", type="int"),
    ]


@pytest.mark.asyncio
async def test_index_nodes(
    mocker: MockerFixture,
    repository: Path,
    session: Session,
) -> None:
    """
    Test ``index_nodes``.
    """
    mocker.patch(
        "datajunction.cli.compile.get_columns",
        side_effect=[
            [
                Column(id=None, name="ds", type="datetime"),
                Column(id=None, name="cnt", type="int"),
            ],
            [
                Column(id=None, name="ds", type="datetime"),
                Column(id=None, name="cnt", type="int"),
            ],
            [
                Column(id=None, name="ds", type="datetime"),
                Column(id=None, name="cnt", type="int"),
            ],
            [
                Column(id=None, name="ds", type="datetime"),
                Column(id=None, name="cnt", type="int"),
            ],
        ],
    )

    session.add(Database(name="druid", URI="druid://localhost:8082/druid/v2/sql/"))
    session.add(
        Database(
            name="postgres",
            URI="postgresql://username:FoolishPassword@localhost:5433/examples",
        ),
    )
    session.add(Database(name="gsheets", URI="gsheets://"))

    with freeze_time("2021-01-01T00:00:00Z"):
        Path("/path/to/repository/nodes/core/comments.yaml").touch()
        Path("/path/to/repository/nodes/core/users.yaml").touch()

    with freeze_time("2021-01-02T00:00:00Z"):
        nodes = await index_nodes(repository, session)

    configs = [node.dict(exclude={"id": True}) for node in nodes]
    assert sorted(configs, key=itemgetter("name")) == [
        {
            "name": "core.comments",
            "description": "A fact table with comments",
            "created_at": datetime(2021, 1, 2, 0, 0, tzinfo=timezone.utc),
            "updated_at": datetime(2021, 1, 2, 0, 0, tzinfo=timezone.utc),
            "expression": None,
        },
        {
            "name": "core.users",
            "description": "A user dimension table",
            "created_at": datetime(2021, 1, 2, 0, 0, tzinfo=timezone.utc),
            "updated_at": datetime(2021, 1, 2, 0, 0, tzinfo=timezone.utc),
            "expression": None,
        },
    ]

    # update one of the nodes and reindex
    with freeze_time("2021-01-03T00:00:00Z"):
        Path("/path/to/repository/nodes/core/users.yaml").touch()
        nodes = await index_nodes(repository, session)
    nodes = sorted(nodes, key=lambda node: node.name)

    assert [(node.name, node.updated_at) for node in nodes] == [
        ("core.comments", datetime(2021, 1, 2, 0, 0, tzinfo=timezone.utc)),
        ("core.users", datetime(2021, 1, 3, 0, 0, tzinfo=timezone.utc)),
    ]

    # test that a missing timezone is treated as UTC
    nodes[0].updated_at = nodes[0].updated_at.replace(tzinfo=None)
    with freeze_time("2021-01-03T00:00:00Z"):
        nodes = await index_nodes(repository, session)
    nodes = sorted(nodes, key=lambda node: node.name)

    assert [(node.name, node.updated_at) for node in nodes] == [
        ("core.comments", datetime(2021, 1, 2, 0, 0, tzinfo=timezone.utc)),
        ("core.users", datetime(2021, 1, 3, 0, 0, tzinfo=timezone.utc)),
    ]


@pytest.mark.asyncio
async def test_run(mocker: MockerFixture, repository: Path) -> None:
    """
    Test the ``run`` command.
    """
    mocker.patch("datajunction.cli.compile.create_db_and_tables")
    get_session = mocker.patch("datajunction.cli.compile.get_session")
    session = get_session.return_value.__next__.return_value

    index_databases = mocker.patch("datajunction.cli.compile.index_databases")
    index_nodes = mocker.patch("datajunction.cli.compile.index_nodes")

    await run(repository)

    index_databases.assert_called_with(repository, session)
    index_nodes.assert_called_with(repository, session)

    session.commit.assert_called()
