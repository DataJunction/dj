"""
Tests for ``datajunction.cli.compile``.
"""

from datetime import datetime, timezone
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
from datajunction.models import Column, Database, Node, Representation


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
        "/path/to/repository/dj.yaml",
        contents="foo: bar",
    )
    assert load_data(Path("/path/to/repository/dj.yaml")) == {"foo": "bar"}


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

    assert databases == [
        Database(
            id=1,
            name="druid",
            URI="druid://localhost:8082/druid/v2/sql/",
            updated_at=datetime(2021, 1, 2, 0, 0, tzinfo=timezone.utc),
            created_at=datetime(2021, 1, 2, 0, 0, tzinfo=timezone.utc),
            description="An Apache Druid database",
            read_only=True,
        ),
        Database(
            id=2,
            name="postgres",
            URI="postgresql://username:FoolishPassword@localhost:5433/examples",
            updated_at=datetime(2021, 1, 2, 0, 0, tzinfo=timezone.utc),
            created_at=datetime(2021, 1, 2, 0, 0, tzinfo=timezone.utc),
            description="A Postgres database",
            read_only=False,
        ),
        Database(
            id=3,
            name="gsheets",
            URI="gsheets://",
            updated_at=datetime(2021, 1, 2, 0, 0, tzinfo=timezone.utc),
            created_at=datetime(2021, 1, 2, 0, 0, tzinfo=timezone.utc),
            description="A Google Sheets connector",
            read_only=True,
        ),
    ]

    # update the Druid database and reindex
    with freeze_time("2021-01-03T00:00:00Z"):
        Path("/path/to/repository/databases/druid.yaml").touch()
        databases = await index_databases(repository, session)

    assert [(database.name, database.updated_at) for database in databases] == [
        ("druid", datetime(2021, 1, 3, 0, 0, tzinfo=timezone.utc)),
        ("postgres", datetime(2021, 1, 2, 0, 0, tzinfo=timezone.utc)),
        ("gsheets", datetime(2021, 1, 2, 0, 0, tzinfo=timezone.utc)),
    ]

    # test that a missing timezone is treated as UTC
    databases[0].updated_at = databases[0].updated_at.replace(tzinfo=None)
    with freeze_time("2021-01-03T00:00:00Z"):
        databases = await index_databases(repository, session)

    assert [(database.name, database.updated_at) for database in databases] == [
        ("druid", datetime(2021, 1, 3, 0, 0, tzinfo=timezone.utc)),
        ("postgres", datetime(2021, 1, 2, 0, 0, tzinfo=timezone.utc)),
        ("gsheets", datetime(2021, 1, 2, 0, 0, tzinfo=timezone.utc)),
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
    mocker: MockerFixture, repository: Path, session: Session
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
        )
    )
    session.add(Database(name="gsheets", URI="gsheets://"))

    with freeze_time("2021-01-01T00:00:00Z"):
        Path("/path/to/repository/nodes/core/comments.yaml").touch()
        Path("/path/to/repository/nodes/core/users.yaml").touch()

    with freeze_time("2021-01-02T00:00:00Z"):
        nodes = await index_nodes(repository, session)

    assert nodes == [
        Node(
            id=1,
            name="core.comments",
            description="A fact table with comments",
            created_at=datetime(2021, 1, 2, tzinfo=timezone.utc),
            updated_at=datetime(2021, 1, 2, tzinfo=timezone.utc),
            expression=None,
            columns=[
                Column(
                    id=1,
                    name="ds",
                    type="datetime",
                    node_id=2,
                ),
                Column(
                    id=2,
                    name="cnt",
                    type="int",
                    node_id=2,
                ),
            ],
            representations=[
                Representation(
                    id=1,
                    catalog=None,
                    schema_=None,
                    table="https://docs.google.com/spreadsheets/d/1SkEZOipqjXQnxHLMr2kZ7Tbn7OiHSgO99gOCS5jTQJs/edit#gid=1811447072",
                    cost=100.0,
                    node_id=1,
                    database_id=3,
                ),
                Representation(
                    id=2,
                    catalog=None,
                    schema_="public",
                    table="comments",
                    cost=10.0,
                    node_id=1,
                    database_id=2,
                ),
                Representation(
                    id=3,
                    catalog=None,
                    schema_="druid",
                    table="comments",
                    cost=1.0,
                    node_id=1,
                    database_id=1,
                ),
            ],
        ),
        Node(
            id=2,
            name="core.users",
            description="A user dimension table",
            created_at=datetime(2021, 1, 2, tzinfo=timezone.utc),
            updated_at=datetime(2021, 1, 2, tzinfo=timezone.utc),
            expression=None,
            columns=[
                Column(
                    id=1,
                    name="ds",
                    type="datetime",
                    node_id=2,
                ),
                Column(
                    id=2,
                    name="cnt",
                    type="int",
                    node_id=2,
                ),
            ],
            representations=[
                Representation(
                    id=4,
                    catalog=None,
                    schema_=None,
                    table="https://docs.google.com/spreadsheets/d/1SkEZOipqjXQnxHLMr2kZ7Tbn7OiHSgO99gOCS5jTQJs/edit#gid=0",
                    cost=100.0,
                    node_id=2,
                    database_id=3,
                ),
                Representation(
                    id=5,
                    catalog=None,
                    schema_="public",
                    table="dim_users",
                    cost=10.0,
                    node_id=2,
                    database_id=2,
                ),
            ],
        ),
    ]

    # update one of the nodes and reindex
    with freeze_time("2021-01-03T00:00:00Z"):
        Path("/path/to/repository/nodes/core/users.yaml").touch()
        nodes = await index_nodes(repository, session)

    assert [(node.name, node.updated_at) for node in nodes] == [
        ("core.comments", datetime(2021, 1, 2, 0, 0, tzinfo=timezone.utc)),
        ("core.users", datetime(2021, 1, 3, 0, 0, tzinfo=timezone.utc)),
    ]

    # test that a missing timezone is treated as UTC
    nodes[0].updated_at = nodes[0].updated_at.replace(tzinfo=None)
    with freeze_time("2021-01-03T00:00:00Z"):
        nodes = await index_nodes(repository, session)

    assert [(node.name, node.updated_at) for node in nodes] == [
        ("core.comments", datetime(2021, 1, 2, 0, 0, tzinfo=timezone.utc)),
        ("core.users", datetime(2021, 1, 3, 0, 0, tzinfo=timezone.utc)),
    ]


@pytest.mark.asyncio
async def test_run(mocker: MockerFixture, repository: Path) -> None:
    """
    Test the ``run`` command.
    """
    mocker.patch("datajunction.cli.compile.create_engine")
    mocker.patch("datajunction.cli.compile.SQLModel")

    Session = mocker.patch("datajunction.cli.compile.Session")
    session = Session.return_value.__enter__.return_value

    index_databases = mocker.patch("datajunction.cli.compile.index_databases")
    index_nodes = mocker.patch("datajunction.cli.compile.index_nodes")

    await run(repository)

    index_databases.assert_called_with(repository, session)
    index_nodes.assert_called_with(repository, session)

    session.commit.assert_called()
