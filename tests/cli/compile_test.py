"""
Tests for ``datajunction.cli.compile``.
"""
# pylint: disable=redefined-outer-name, invalid-name

from datetime import datetime, timezone
from operator import itemgetter
from pathlib import Path
from unittest import mock

import pytest
import sqlalchemy
import yaml
from freezegun import freeze_time
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture
from sqlmodel import Session

from datajunction.cli.compile import (
    add_dimensions_to_columns,
    add_node,
    get_columns_from_tables,
    get_table_columns,
    index_databases,
    index_nodes,
    load_data,
    run,
    update_node_config,
    yaml_file_changed,
)
from datajunction.constants import DEFAULT_DIMENSION_COLUMN
from datajunction.models.column import Column
from datajunction.models.database import Database
from datajunction.models.node import Node, NodeType
from datajunction.models.query import Query  # pylint: disable=unused-import
from datajunction.models.table import Table
from datajunction.sql.parse import get_dependencies
from datajunction.typing import ColumnType


@pytest.mark.asyncio
async def test_load_data(fs: FakeFilesystem) -> None:
    """
    Test ``load_data``.
    """
    repository = Path("/path/to/repository")
    path = repository / "nodes/example.yaml"
    fs.create_file(path, contents="foo: bar")

    config = await load_data(repository, path)
    assert config == {"foo": "bar", "name": "example", "path": path}


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
            "cost": 1.0,
            "created_at": datetime(2021, 1, 2, 0, 0),
            "updated_at": datetime(2021, 1, 2, 0, 0),
            "name": "druid",
            "description": "An Apache Druid database",
            "URI": "druid://host.docker.internal:8082/druid/v2/sql/",
            "read_only": True,
        },
        {
            "async_": False,
            "cost": 100.0,
            "created_at": datetime(2021, 1, 2, 0, 0),
            "updated_at": datetime(2021, 1, 2, 0, 0),
            "name": "gsheets",
            "description": "A Google Sheets connector",
            "URI": "gsheets://",
            "read_only": True,
        },
        {
            "async_": False,
            "cost": 10.0,
            "created_at": datetime(2021, 1, 2, 0, 0),
            "updated_at": datetime(2021, 1, 2, 0, 0),
            "name": "postgres",
            "description": "A Postgres database",
            "URI": "postgresql://username:FoolishPassword@host.docker.internal:5433/examples",
            "read_only": False,
        },
    ]

    # update the Druid database and reindex
    with freeze_time("2021-01-03T00:00:00Z"):
        Path("/path/to/repository/databases/druid.yaml").touch()
        databases = await index_databases(repository, session)
    databases = sorted(databases, key=lambda database: database.name)

    assert [(database.name, database.updated_at) for database in databases] == [
        ("druid", datetime(2021, 1, 3, 0, 0)),
        ("gsheets", datetime(2021, 1, 2, 0, 0)),
        ("postgres", datetime(2021, 1, 2, 0, 0)),
    ]

    # test that a missing timezone is treated as UTC
    databases[0].updated_at = databases[0].updated_at.replace(tzinfo=None)
    with freeze_time("2021-01-03T00:00:00Z"):
        databases = await index_databases(repository, session)
    databases = sorted(databases, key=lambda database: database.name)

    assert [(database.name, database.updated_at) for database in databases] == [
        ("druid", datetime(2021, 1, 3, 0, 0)),
        ("gsheets", datetime(2021, 1, 2, 0, 0)),
        ("postgres", datetime(2021, 1, 2, 0, 0)),
    ]


@pytest.mark.asyncio
async def test_index_databases_force(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test ``index_databases`` with the ``--force`` option.
    """
    _logger = mocker.patch("datajunction.cli.compile._logger")
    session = mocker.MagicMock()
    session.exec().one_or_none().updated_at = datetime(
        2021,
        1,
        3,
        0,
        0,
        tzinfo=timezone.utc,
    )

    repository = Path("/path/to/another/repository")
    with freeze_time("2021-01-01T00:00:00Z"):
        fs.create_file(repository / "databases/druid.yaml", contents="{}\n")

    await index_databases(repository, session, force=False)

    _logger.info.assert_has_calls(
        [
            mock.call("Processing database %s", "druid"),
            mock.call("Database %s is up-to-date, skipping", "druid"),
        ],
    )

    await index_databases(repository, session, force=True)

    _logger.info.assert_has_calls(
        [
            mock.call("Processing database %s", "druid"),
            mock.call(
                "Loading database from config %s",
                Path("/path/to/another/repository/databases/druid.yaml"),
            ),
            mock.call("Updating database %s", "druid"),
        ],
    )


def test_get_table_columns(mocker: MockerFixture) -> None:
    """
    Test ``get_table_columns``.
    """
    mocker.patch("datajunction.cli.compile.create_engine")
    inspect = mocker.patch("datajunction.cli.compile.inspect")
    inspect().get_columns.return_value = [
        {"name": "ds", "type": sqlalchemy.sql.sqltypes.DateTime()},
        {"name": "cnt", "type": sqlalchemy.sql.sqltypes.Float()},
    ]

    assert get_table_columns("sqlite://", "schema", "table") == [
        Column(id=None, name="ds", type=ColumnType.DATETIME),
        Column(id=None, name="cnt", type=ColumnType.FLOAT),
    ]


def test_get_table_columns_error(mocker: MockerFixture) -> None:
    """
    Test ``get_table_columns`` raising an exception.
    """
    mocker.patch("datajunction.cli.compile.create_engine")
    inspect = mocker.patch("datajunction.cli.compile.inspect")
    inspect().get_columns.side_effect = Exception(
        "An unexpected error occurred",
    )

    assert get_table_columns("sqlite://", "schema", "table") == []


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
        "datajunction.cli.compile.get_table_columns",
        return_value=[],
    )
    mocker.patch("datajunction.cli.compile.update_node_config")

    session.add(
        Database(name="druid", URI="druid://host.docker.internal:8082/druid/v2/sql/"),
    )
    session.add(
        Database(
            name="postgres",
            URI="postgresql://username:FoolishPassword@host.docker.internal:5433/examples",
        ),
    )
    session.add(Database(name="gsheets", URI="gsheets://"))
    session.flush()

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
            "type": NodeType.SOURCE,
            "created_at": datetime(2021, 1, 2, 0, 0),
            "updated_at": datetime(2021, 1, 2, 0, 0),
            "expression": None,
        },
        {
            "name": "core.num_comments",
            "description": "Number of comments",
            "type": NodeType.METRIC,
            "created_at": datetime(2021, 1, 2, 0, 0),
            "updated_at": datetime(2021, 1, 2, 0, 0),
            "expression": "SELECT COUNT(*) FROM core.comments",
        },
        {
            "name": "core.users",
            "description": "A user dimension table",
            "type": NodeType.DIMENSION,
            "created_at": datetime(2021, 1, 2, 0, 0),
            "updated_at": datetime(2021, 1, 2, 0, 0),
            "expression": None,
        },
    ]

    # update one of the nodes and reindex
    with freeze_time("2021-01-03T00:00:00Z"):
        Path("/path/to/repository/nodes/core/users.yaml").touch()
        nodes = await index_nodes(repository, session)
    nodes = sorted(nodes, key=lambda node: node.name)

    assert [(node.name, node.updated_at) for node in nodes] == [
        ("core.comments", datetime(2021, 1, 2, 0, 0)),
        ("core.num_comments", datetime(2021, 1, 3, 0, 0)),
        ("core.users", datetime(2021, 1, 3, 0, 0)),
    ]

    # test that a missing timezone is treated as UTC
    nodes[0].updated_at = nodes[0].updated_at.replace(tzinfo=None)
    with freeze_time("2021-01-03T00:00:00Z"):
        nodes = await index_nodes(repository, session)
    nodes = sorted(nodes, key=lambda node: node.name)

    assert [(node.name, node.updated_at) for node in nodes] == [
        ("core.comments", datetime(2021, 1, 2, 0, 0)),
        ("core.num_comments", datetime(2021, 1, 3, 0, 0)),
        ("core.users", datetime(2021, 1, 3, 0, 0)),
    ]


@pytest.mark.asyncio
async def test_add_node_force(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test ``add_node`` with the ``--force`` option.
    """
    _logger = mocker.patch("datajunction.cli.compile._logger")
    session = mocker.MagicMock()
    session.exec().one_or_none().updated_at = datetime(
        2021,
        1,
        3,
        0,
        0,
        tzinfo=timezone.utc,
    )
    databases = mocker.MagicMock()
    mocker.patch("datajunction.cli.compile.update_node_config")

    with freeze_time("2021-01-01T00:00:00Z"):
        fs.create_file("/path/to/repository/nodes/test.yaml")

    data = {
        "name": "test",
        "path": Path("/path/to/repository/nodes/test.yaml"),
        "description": "",
        "type": "transform",
    }

    await add_node(session, databases, data, [], force=False)  # type: ignore

    _logger.info.assert_has_calls(
        [
            mock.call("Processing node %s", "test"),
            mock.call("Node %s is up-do-date, skipping", "test"),
        ],
    )

    _logger.info.reset_mock()
    await add_node(session, databases, data, [], force=True)  # type: ignore

    _logger.info.assert_has_calls(
        [
            mock.call("Processing node %s", "test"),
            mock.call("Updating node %s", "test"),
        ],
    )


@pytest.mark.asyncio
async def test_run(mocker: MockerFixture, repository: Path) -> None:
    """
    Test the ``run`` command.
    """
    mocker.patch("datajunction.cli.compile.create_db_and_tables")
    get_session = mocker.patch("datajunction.cli.compile.get_session")
    session = get_session().__next__()
    session.get.return_value = False

    index_databases = mocker.patch("datajunction.cli.compile.index_databases")
    index_nodes = mocker.patch("datajunction.cli.compile.index_nodes")

    await run(repository)

    index_databases.assert_called_with(repository, session, False)
    index_nodes.assert_called_with(repository, session, False)

    session.commit.assert_called()
    assert session.add.call_count == 2


@pytest.mark.asyncio
async def test_run_reload(mocker: MockerFixture, repository: Path) -> None:
    """
    Test the ``run`` command with ``--reload``.
    """
    mocker.patch("datajunction.cli.compile.create_db_and_tables")
    get_session = mocker.patch("datajunction.cli.compile.get_session")
    session = get_session().__next__.return_value
    awatch = mocker.patch("datajunction.cli.compile.awatch")
    mocker.patch("datajunction.cli.compile.index_databases")
    mocker.patch("datajunction.cli.compile.index_nodes")
    awatch().__aiter__.return_value = ["event"]

    await run(repository, reload=True)

    assert session.commit.call_count == 2


def test_yaml_file_changed() -> None:
    """
    Test ``yaml_file_changed``.
    """
    assert yaml_file_changed(None, "/path/to/config.yaml") is True
    assert yaml_file_changed(None, "/path/to/config.yml") is True
    assert yaml_file_changed(None, "/path/to/dj.db") is False


def test_get_dependencies() -> None:
    """
    Test ``get_dependencies``.
    """
    assert get_dependencies("SELECT 1") == set()
    assert get_dependencies("SELECT COUNT(*) FROM core.comments") == {"core.comments"}
    assert (
        get_dependencies(
            "SELECT COUNT(*) FROM core.comments cc JOIN core.events ce ON cc.id = ce.id",
        )
        == {"core.comments", "core.events"}
    )
    assert get_dependencies("SELECT 1 FROM a UNION SELECT 2 FROM b") == {"a", "b"}


@pytest.mark.asyncio
async def test_update_node_config(mocker: MockerFixture, fs: FakeFilesystem) -> None:
    """
    Test ``update_node_config``.
    """
    _logger = mocker.patch("datajunction.cli.compile._logger")

    database = Database(name="test", URI="sqlite://")

    table_a = Table(
        database=database,
        table="A",
        columns=[
            Column(name="ds", type=ColumnType.STR),
            Column(name="user_id", type=ColumnType.INT),
        ],
    )

    table_b = Table(
        database=database,
        table="B",
        columns=[Column(name="ds", type=ColumnType.DATETIME)],
    )

    node = Node(
        name="C",
        tables=[table_a, table_b],
        columns=[
            Column(name="ds", type=ColumnType.DATETIME),
            Column(name="user_id", type=ColumnType.INT),
        ],
        type=NodeType.SOURCE,
    )
    path = Path("/path/to/repository/configs/nodes/C.yaml")
    fs.create_file(
        path,
        contents=yaml.safe_dump({}),
    )

    await update_node_config(node, path)

    with open(path, encoding="utf-8") as input_:
        assert yaml.safe_load(input_) == {
            "columns": {
                "ds": {"type": "DATETIME"},
                "user_id": {"type": "INT"},
            },
            "tables": {
                "test": [
                    {"catalog": None, "cost": 1.0, "schema": None, "table": "A"},
                    {"catalog": None, "cost": 1.0, "schema": None, "table": "B"},
                ],
            },
            "type": "source",
        }
    _logger.info.assert_called_with(
        "Updating node %s config with column information",
        "C",
    )

    await update_node_config(node, path)
    _logger.info.assert_called_with(
        "Node %s is up-do-date, skipping",
        "C",
    )


@pytest.mark.asyncio
async def test_update_node_config_user_attributes(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test ``update_node_config`` when the user has added attributes to a column.
    """
    _logger = mocker.patch("datajunction.cli.compile._logger")

    database = Database(name="test", URI="sqlite://")

    table_a = Table(
        database=database,
        table="A",
        columns=[
            Column(name="ds", type=ColumnType.STR),
            Column(name="user_id", type=ColumnType.INT),
        ],
    )

    table_b = Table(
        database=database,
        table="B",
        columns=[Column(name="ds", type=ColumnType.DATETIME)],
    )

    node = Node(
        name="C",
        tables=[table_a, table_b],
        columns=[
            Column(name="ds", type=ColumnType.DATETIME),
            Column(name="user_id", type=ColumnType.INT),
        ],
        type=NodeType.SOURCE,
    )
    path = Path("/path/to/repository/configs/nodes/C.yaml")
    fs.create_file(
        path,
        contents=yaml.safe_dump(
            {
                "columns": {
                    "old_column": {"type": "INT"},
                    "ds": {"user_attribute": "Hello, world!"},
                },
            },
        ),
    )

    await update_node_config(node, path)

    with open(path, encoding="utf-8") as input_:
        assert yaml.safe_load(input_) == {
            "columns": {
                "ds": {"type": "DATETIME", "user_attribute": "Hello, world!"},
                "user_id": {"type": "INT"},
            },
            "tables": {
                "test": [
                    {"catalog": None, "cost": 1.0, "schema": None, "table": "A"},
                    {"catalog": None, "cost": 1.0, "schema": None, "table": "B"},
                ],
            },
            "type": "source",
        }
    _logger.info.assert_called_with(
        "Updating node %s config with column information",
        "C",
    )

    await update_node_config(node, path)
    _logger.info.assert_called_with(
        "Node %s is up-do-date, skipping",
        "C",
    )


def test_get_columns_from_tables() -> None:
    """
    Test ``get_columns_from_tables``.
    """
    table_a = Table(
        table="A",
        columns=[
            Column(name="ds", type=ColumnType.STR),
            Column(name="user_id", type=ColumnType.INT),
        ],
    )

    table_b = Table(
        table="B",
        columns=[Column(name="ds", type=ColumnType.DATETIME)],
    )

    assert get_columns_from_tables([table_a, table_b]) == [
        Column(name="ds", type=ColumnType.DATETIME),
        Column(name="user_id", type=ColumnType.INT),
    ]


def test_add_dimensions_to_columns(mocker: MockerFixture, repository: Path) -> None:
    """
    Test ``add_dimensions_to_columns``.
    """
    session = mocker.MagicMock()
    dimension = Node(
        name="users",
        type=NodeType.DIMENSION,
        columns=[
            Column(name="id", type=ColumnType.INT),
            Column(name="uuid", type=ColumnType.INT),
            Column(name="city", type=ColumnType.STR),
        ],
    )
    session.exec().one_or_none.return_value = dimension

    with open(repository / "nodes/core/comments.yaml", encoding="utf-8") as input_:
        data = yaml.safe_load(input_)

    columns = [
        Column(name="id", type=ColumnType.INT),
        Column(name="user_id", type=ColumnType.INT),
    ]

    add_dimensions_to_columns(session, data, columns)

    assert columns[1].dimension == dimension
    assert columns[1].dimension_column == DEFAULT_DIMENSION_COLUMN

    # custom column
    data["columns"]["user_id"]["dimension"] = "users.uuid"
    session.exec().one_or_none.return_value = None
    session.exec().one.return_value = dimension

    add_dimensions_to_columns(session, data, columns)

    assert columns[1].dimension == dimension
    assert columns[1].dimension_column == "uuid"

    # invalid column
    data["columns"]["user_id"]["dimension"] = "invalid"
    session.exec().one_or_none.return_value = None

    with pytest.raises(Exception) as excinfo:
        add_dimensions_to_columns(session, data, columns)
    assert str(excinfo.value) == "Invalid dimension: invalid"
