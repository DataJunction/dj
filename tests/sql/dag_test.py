"""
Tests for ``dj.sql.dag``.
"""

from collections import defaultdict
from typing import Dict, Set

import pytest
from pytest_mock import MockerFixture

from dj.models.column import Column
from dj.models.database import Database
from dj.models.node import Node, NodeRevision, NodeType
from dj.models.table import Table
from dj.sql.dag import (
    get_cheapest_online_database,
    get_computable_databases,
    get_database_for_nodes,
    get_dimensions,
    get_referenced_columns_from_sql,
)
from dj.typing import ColumnType


def test_get_computable_databases() -> None:
    """
    Test ``get_computable_databases``.
    """
    database_1 = Database(id=1, name="shared", URI="sqlite://", cost=1.0)
    database_2 = Database(id=2, name="not shared", URI="sqlite://", cost=2.0)
    database_3 = Database(id=3, name="fast", URI="sqlite://", cost=0.1)

    parent_a_ref = Node(name="A", current_version="1")
    parent_a = NodeRevision(
        node=parent_a_ref,
        version="1",
        tables=[
            Table(database=database_1, table="A"),
            Table(database=database_2, table="A"),
        ],
    )
    parent_a_ref.current = parent_a

    parent_b_ref = Node(name="B", current_version="1")
    parent_b = NodeRevision(
        node=parent_b_ref,
        version="1",
        tables=[Table(database=database_1, table="B")],
    )
    parent_b_ref.current = parent_b

    child_ref = Node(name="C", current_version="1")
    child = NodeRevision(
        node=child_ref,
        version="1",
        tables=[Table(database=database_3, table="C")],
        parents=[parent_a_ref, parent_b_ref],
    )
    child_ref.current = child

    assert {database.name for database in get_computable_databases(child)} == {
        "fast",
        "shared",
    }
    assert {database.name for database in get_computable_databases(parent_a)} == {
        "shared",
        "not shared",
    }
    assert {database.name for database in get_computable_databases(parent_b)} == {
        "shared",
    }


def test_get_computable_databases_heterogeneous_columns() -> None:
    """
    Test ``get_computable_databases`` when columns are heterogeneous.
    """
    database_1 = Database(id=1, name="one", URI="sqlite://", cost=1.0)
    database_2 = Database(id=2, name="two", URI="sqlite://", cost=2.0)

    parent_ref = Node(name="core.A", current_version="1")
    parent = NodeRevision(
        node=parent_ref,
        version="1",
        tables=[
            Table(
                database=database_1,
                table="A",
                columns=[
                    Column(name="ds", type=ColumnType.STR),
                    Column(name="user_id", type=ColumnType.INT),
                ],
            ),
            Table(
                database=database_2,
                table="A",
                columns=[
                    Column(name="ds", type=ColumnType.STR),
                ],
            ),
        ],
        columns=[
            Column(name="ds", type=ColumnType.STR),
            Column(name="user_id", type=ColumnType.INT),
        ],
    )
    parent_ref.current = parent

    child_1_ref = Node(name="core.B", current_version="1")
    child_1 = NodeRevision(
        node=child_1_ref,
        version="1",
        query="SELECT COUNT(core.A.user_id) FROM core.A",
        parents=[parent_ref],
    )
    child_1_ref.current = child_1

    assert {database.name for database in get_computable_databases(child_1)} == {
        "one",
    }
    child_2_ref = Node(name="core.C", current_version="1")
    child_2 = NodeRevision(
        node=child_2_ref,
        version="1",
        query="SELECT COUNT(user_id) FROM core.A",
        parents=[parent_ref],
    )
    child_2_ref.current = child_2

    assert {database.name for database in get_computable_databases(child_2)} == {
        "one",
    }


def test_get_referenced_columns_from_sql() -> None:
    """
    Test ``get_referenced_columns_from_sql``.
    """
    database = Database(id=1, name="one", URI="sqlite://", cost=1.0)
    parent_1_ref = Node(name="core.A", current_version="1")
    parent_1 = NodeRevision(
        node=parent_1_ref,
        version="1",
        tables=[
            Table(
                database=database,
                table="A",
                columns=[
                    Column(name="ds", type=ColumnType.STR),
                    Column(name="user_id", type=ColumnType.INT),
                ],
            ),
        ],
        columns=[
            Column(name="ds", type=ColumnType.STR),
            Column(name="user_id", type=ColumnType.INT),
        ],
    )
    parent_1_ref.current = parent_1
    parent_2_ref = Node(name="core.B", current_version="1")
    parent_2 = NodeRevision(
        node=parent_2_ref,
        version="1",
        tables=[
            Table(
                database=database,
                table="B",
                columns=[
                    Column(name="ds", type=ColumnType.STR),
                    Column(name="event_id", type=ColumnType.INT),
                ],
            ),
        ],
        columns=[
            Column(name="ds", type=ColumnType.STR),
            Column(name="event_id", type=ColumnType.INT),
        ],
    )
    parent_2_ref.current = parent_2

    assert get_referenced_columns_from_sql(
        "SELECT core.A.ds FROM core.A",
        [parent_1_ref],
    ) == {
        "core.A": {"ds"},
    }
    assert get_referenced_columns_from_sql("SELECT ds FROM core.A", [parent_1_ref]) == {
        "core.A": {"ds"},
    }
    assert get_referenced_columns_from_sql(
        "SELECT ds FROM core.A WHERE user_id > 0",
        [parent_1_ref],
    ) == {"core.A": {"ds", "user_id"}}
    assert get_referenced_columns_from_sql(
        (
            "SELECT core.A.ds, core.A.user_id, core.B.event_id "
            "FROM core.A JOIN core.B ON core.A.ds = core.B.ds"
        ),
        [parent_1_ref, parent_2_ref],
    ) == {"core.A": {"ds", "user_id"}, "core.B": {"ds", "event_id"}}
    assert get_referenced_columns_from_sql(
        (
            "SELECT user_id, event_id "
            "FROM core.A JOIN core.B ON core.A.ds = core.B.ds"
        ),
        [parent_1_ref, parent_2_ref],
    ) == {"core.A": {"ds", "user_id"}, "core.B": {"ds", "event_id"}}
    with pytest.raises(Exception) as excinfo:
        get_referenced_columns_from_sql(
            (
                "SELECT ds, user_id, event_id "
                "FROM core.A JOIN core.B ON core.A.ds = core.B.ds"
            ),
            [parent_1_ref, parent_2_ref],
        )
    assert str(excinfo.value) == "Column ds is ambiguous"
    with pytest.raises(Exception) as excinfo:
        get_referenced_columns_from_sql("SELECT invalid FROM core.A", [parent_1_ref])
    assert str(excinfo.value) == "Column invalid not found in any parent"


def test_get_dimensions() -> None:
    """
    Test ``get_dimensions``.
    """
    database = Database(id=1, name="one", URI="sqlite://")

    dimension_ref = Node(name="B", type=NodeType.DIMENSION, current_version="1")
    dimension = NodeRevision(
        node=dimension_ref,
        version="1",
        tables=[
            Table(
                database=database,
                table="B",
                columns=[
                    Column(name="id", type=ColumnType.INT),
                    Column(name="attribute", type=ColumnType.STR),
                ],
            ),
        ],
        columns=[
            Column(name="id", type=ColumnType.INT),
            Column(name="attribute", type=ColumnType.STR),
        ],
    )
    dimension_ref.current = dimension

    parent_ref = Node(name="A", current_version="1")
    parent = NodeRevision(
        node=parent_ref,
        version="1",
        tables=[
            Table(
                database=database,
                table="A",
                columns=[
                    Column(name="ds", type=ColumnType.STR),
                    Column(name="b_id", type=ColumnType.INT, dimension=dimension_ref),
                ],
            ),
        ],
        columns=[
            Column(name="ds", type=ColumnType.STR),
            Column(name="b_id", type=ColumnType.INT, dimension=dimension_ref),
        ],
    )
    parent_ref.current = parent

    child_ref = Node(name="C", current_version="1")
    child = NodeRevision(
        node=child_ref,
        version="1",
        query="SELECT COUNT(*) FROM A",
        parents=[parent_ref],
    )
    child_ref.current = child

    assert get_dimensions(child_ref) == ["A.b_id", "A.ds", "B.attribute", "B.id"]


@pytest.mark.asyncio
async def test_get_database_for_nodes(mocker: MockerFixture) -> None:
    """
    Test ``get_database_for_nodes``.
    """
    database_1 = Database(id=1, name="fast", URI="sqlite://", cost=1.0)
    database_2 = Database(id=2, name="slow", URI="sqlite://", cost=10.0)

    get_session = mocker.patch("dj.sql.build.get_session")
    session = next(get_session())
    session.exec().all.return_value = [database_1, database_2]

    parent_ref = Node(name="parent", current_version="1")
    parent = NodeRevision(
        node=parent_ref,
        version="1",
        tables=[
            Table(
                database=database_2,
                table="comments",
                columns=[
                    Column(name="user_id", type=ColumnType.INT),
                    Column(name="comment", type=ColumnType.STR),
                ],
            ),
        ],
    )
    parent_ref.current = parent

    referenced_columns: Dict[str, Set[str]] = defaultdict(set)
    assert (
        await get_database_for_nodes(session, [parent_ref], referenced_columns)
        == database_2
    )

    # without parents, return the cheapest DB
    assert await get_database_for_nodes(session, [], referenced_columns) == database_1

    # with no active database
    create_engine = mocker.patch("dj.models.database.create_engine")
    create_engine.side_effect = Exception("foo")
    database_1 = Database(id=1, name="fast", URI="sqlite://", cost=1.0)
    database_2 = Database(id=2, name="slow", URI="sqlite://", cost=10.0)

    get_session = mocker.patch("dj.sql.build.get_session")
    session = next(get_session())
    session.exec().all.return_value = [database_1, database_2]
    with pytest.raises(Exception) as excinfo:
        await get_database_for_nodes(
            session,
            [],
            referenced_columns,
            check_database_online=True,
        )
    assert str(excinfo.value) == "No active database was found"

    result = await get_database_for_nodes(
        session,
        [],
        referenced_columns,
        database_name="fast",
        check_database_online=True,
    )
    assert result == database_1
    result = await get_database_for_nodes(
        session,
        [],
        referenced_columns,
        database_name="slow",
        check_database_online=True,
    )
    assert result == database_2


@pytest.mark.asyncio
async def test_get_cheapest_online_database(mocker: MockerFixture) -> None:
    """
    Test ``get_cheapest_online_database``.
    """
    database_1 = Database(id=1, name="fast", URI="sqlite://", cost=1.0)
    database_2 = Database(id=2, name="slow", URI="sqlite://", cost=10.0)

    slow_ping = mocker.MagicMock()
    slow_ping.done.side_effect = [False, True]
    slow_ping.result.return_value = True

    fast_ping = mocker.MagicMock()
    fast_ping.done.side_effect = [True, True]
    fast_ping.result.return_value = True

    asyncio = mocker.patch("dj.sql.dag.asyncio")
    asyncio.wait = mocker.AsyncMock(
        side_effect=[([fast_ping], [slow_ping]), ([slow_ping], [])],
    )
    asyncio.create_task.side_effect = [slow_ping, fast_ping]

    assert await get_cheapest_online_database({database_1, database_2}) == database_1
    assert (
        await get_cheapest_online_database(
            {database_1, database_2},
            check_database_online=False,
        )
        == database_1
    )


@pytest.mark.asyncio
async def test_get_cheapest_online_database_offline(mocker: MockerFixture) -> None:
    """
    Test ``get_cheapest_online_database`` when the fastest DB is offline.
    """
    database_1 = Database(id=1, name="fast", URI="sqlite://", cost=1.0)
    database_2 = Database(id=2, name="slow", URI="sqlite://", cost=10.0)

    slow_ping = mocker.MagicMock()
    slow_ping.done.side_effect = [False, True]
    slow_ping.result.return_value = False

    fast_ping = mocker.MagicMock()
    fast_ping.done.side_effect = [True, True]
    fast_ping.result.return_value = True

    asyncio = mocker.patch("dj.sql.dag.asyncio")
    asyncio.wait = mocker.AsyncMock(
        side_effect=[([fast_ping], [slow_ping]), ([slow_ping], [])],
    )
    asyncio.create_task.side_effect = [slow_ping, fast_ping]

    assert await get_cheapest_online_database({database_1, database_2}) == database_2
    assert (
        await get_cheapest_online_database(
            {database_1, database_2},
            check_database_online=False,
        )
        == database_1
    )


@pytest.mark.asyncio
async def test_get_cheapest_online_database_timeout(mocker: MockerFixture) -> None:
    """
    Test ``get_cheapest_online_database`` when pinging the fastest DB times out.
    """
    database_1 = Database(id=1, name="fast", URI="sqlite://", cost=1.0)
    database_2 = Database(id=2, name="slow", URI="sqlite://", cost=10.0)

    slow_ping = mocker.MagicMock()
    slow_ping.done.side_effect = [False, False]
    slow_ping.result.return_value = True

    fast_ping = mocker.MagicMock()
    fast_ping.done.side_effect = [True, True]
    fast_ping.result.return_value = True

    asyncio = mocker.patch("dj.sql.dag.asyncio")
    asyncio.wait = mocker.AsyncMock(
        side_effect=[([fast_ping], [slow_ping]), ([], [slow_ping])],
    )
    asyncio.create_task.side_effect = [slow_ping, fast_ping]

    assert await get_cheapest_online_database({database_1, database_2}) == database_2
    assert (
        await get_cheapest_online_database(
            {database_1, database_2},
            check_database_online=False,
        )
        == database_1
    )
