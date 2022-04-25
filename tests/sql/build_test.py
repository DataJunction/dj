"""
Tests for ``datajunction.sql.build``.
"""
# pylint: disable=invalid-name, too-many-lines, line-too-long

import datetime

import pytest
from pytest_mock import MockerFixture
from sqlalchemy.engine import create_engine
from sqlmodel import Session

from datajunction.models.column import Column
from datajunction.models.database import Database
from datajunction.models.node import Node, NodeType
from datajunction.models.table import Table
from datajunction.sql.build import (
    find_on_clause,
    get_dimensions_from_filters,
    get_filter,
    get_join_columns,
    get_query_for_node,
    get_query_for_sql,
)
from datajunction.typing import ColumnType


def test_get_query_for_node(mocker: MockerFixture) -> None:
    """
    Test ``get_query_for_node``.
    """
    database = Database(id=1, name="slow", URI="sqlite://", cost=1.0)

    parent = Node(name="A")

    child = Node(
        name="B",
        tables=[
            Table(
                database=database,
                table="B",
                columns=[Column(name="cnt", type=ColumnType.INT)],
            ),
        ],
        type=NodeType.METRIC,
        expression="SELECT COUNT(*) AS cnt FROM A",
        parents=[parent],
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE B (cnt INTEGER)")
    mocker.patch("datajunction.sql.transpile.create_engine", return_value=engine)
    session = mocker.MagicMock()

    create_query = get_query_for_node(session, child, [], [])
    assert create_query.database_id == 1
    assert create_query.submitted_query == 'SELECT "B".cnt \nFROM "B"'


def test_get_query_for_node_with_groupbys(mocker: MockerFixture) -> None:
    """
    Test ``get_query_for_node`` with group bys.
    """
    database = Database(id=1, name="slow", URI="sqlite://", cost=1.0)

    parent = Node(
        name="A",
        tables=[
            Table(
                database=database,
                table="A",
                columns=[
                    Column(name="user_id", type=ColumnType.INT),
                    Column(name="comment", type=ColumnType.STR),
                ],
            ),
        ],
        columns=[
            Column(name="user_id", type=ColumnType.INT),
            Column(name="comment", type=ColumnType.STR),
        ],
    )

    child = Node(
        name="B",
        type=NodeType.METRIC,
        expression="SELECT COUNT(*) AS cnt FROM A",
        parents=[parent],
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE A (user_id INTEGER, comment TEXT)")
    mocker.patch("datajunction.sql.transpile.create_engine", return_value=engine)
    session = mocker.MagicMock()

    create_query = get_query_for_node(session, child, ["A.user_id"], [])
    space = " "
    assert create_query.database_id == 1
    assert (
        create_query.submitted_query
        == f"""SELECT count('*') AS cnt, "A".user_id{space}
FROM (SELECT "A".user_id AS user_id, "A".comment AS comment{space}
FROM "A") AS "A" GROUP BY "A".user_id"""
    )


def test_get_query_for_node_specify_database(mocker: MockerFixture) -> None:
    """
    Test ``get_query_for_node`` when a database is specified.
    """
    database = Database(id=1, name="slow", URI="sqlite://", cost=1.0)

    parent = Node(name="A")

    child = Node(
        name="B",
        tables=[
            Table(
                database=database,
                table="B",
                columns=[Column(name="cnt", type=ColumnType.INT)],
            ),
        ],
        type=NodeType.METRIC,
        expression="SELECT COUNT(*) AS cnt FROM A",
        parents=[parent],
        columns=[Column(name="cnt", type=ColumnType.INT)],
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE B (cnt INTEGER)")
    mocker.patch("datajunction.sql.transpile.create_engine", return_value=engine)
    session = mocker.MagicMock()
    session.exec().one.return_value = database

    create_query = get_query_for_node(session, child, [], [], 1)
    assert create_query.database_id == 1
    assert create_query.submitted_query == 'SELECT "B".cnt \nFROM "B"'

    with pytest.raises(Exception) as excinfo:
        get_query_for_node(session, child, [], [], 2)
    assert str(excinfo.value) == "Database ID 2 is not valid"


def test_get_query_for_node_no_databases(mocker: MockerFixture) -> None:
    """
    Test ``get_query_for_node``.
    """
    database = Database(id=1, name="slow", URI="sqlite://", cost=1.0)

    parent = Node(name="A")

    child = Node(
        name="B",
        tables=[
            Table(
                database=database,
                table="B",
                columns=[Column(name="one", type=ColumnType.STR)],
            ),
        ],
        type=NodeType.METRIC,
        expression="SELECT COUNT(*) AS cnt FROM A",
        parents=[parent],
        columns=[Column(name="one", type=ColumnType.STR)],
    )

    mocker.patch("datajunction.sql.dag.get_computable_databases", return_value=set())
    session = mocker.MagicMock()

    with pytest.raises(Exception) as excinfo:
        get_query_for_node(session, child, [], [])
    assert str(excinfo.value) == "No valid database was found"


def test_get_query_for_node_with_dimensions(mocker: MockerFixture) -> None:
    """
    Test ``get_query_for_node`` when filtering/grouping by a dimension.
    """
    database = Database(id=1, name="one", URI="sqlite://")

    dimension = Node(
        name="core.users",
        type=NodeType.DIMENSION,
        tables=[
            Table(
                database=database,
                table="dim_users",
                columns=[
                    Column(name="id", type=ColumnType.INT),
                    Column(name="age", type=ColumnType.INT),
                    Column(name="gender", type=ColumnType.STR),
                ],
            ),
        ],
        columns=[
            Column(name="id", type=ColumnType.INT),
            Column(name="age", type=ColumnType.INT),
            Column(name="gender", type=ColumnType.STR),
        ],
    )

    parent = Node(
        name="core.comments",
        tables=[
            Table(
                database=database,
                table="comments",
                columns=[
                    Column(name="ds", type=ColumnType.STR),
                    Column(name="user_id", type=ColumnType.INT),
                    Column(name="text", type=ColumnType.STR),
                ],
            ),
        ],
        columns=[
            Column(name="ds", type=ColumnType.STR),
            Column(name="user_id", type=ColumnType.INT, dimension=dimension),
            Column(name="text", type=ColumnType.STR),
        ],
    )

    child = Node(
        name="core.num_comments",
        type=NodeType.METRIC,
        expression="SELECT COUNT(*) FROM core.comments",
        parents=[parent],
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE dim_users (id INTEGER, age INTEGER, gender TEXT)")
    connection.execute("CREATE TABLE comments (ds TEXT, user_id INTEGER, text TEXT)")
    mocker.patch("datajunction.sql.transpile.create_engine", return_value=engine)
    session = mocker.MagicMock()
    session.exec().one.return_value = dimension

    create_query = get_query_for_node(
        session,
        child,
        ["core.users.gender"],
        ["core.users.age>25"],
    )
    space = " "
    assert create_query.database_id == 1
    assert (
        create_query.submitted_query
        == f"""SELECT count('*') AS count_1, "core.users".gender{space}
FROM (SELECT comments.ds AS ds, comments.user_id AS user_id, comments.text AS text{space}
FROM comments) AS "core.comments" JOIN (SELECT dim_users.id AS id, dim_users.age AS age, dim_users.gender AS gender{space}
FROM dim_users) AS "core.users" ON "core.comments".user_id = "core.users".id{space}
WHERE "core.users".age > 25 GROUP BY "core.users".gender"""
    )

    with pytest.raises(Exception) as excinfo:
        get_query_for_node(session, child, ["aaaa"], [])
    assert str(excinfo.value) == "Invalid dimension: aaaa"

    with pytest.raises(Exception) as excinfo:
        get_query_for_node(session, child, ["aaaa", "bbbb"], [])
    assert str(excinfo.value) == "Invalid dimensions: aaaa, bbbb"


def test_get_query_for_node_with_multiple_dimensions(mocker: MockerFixture) -> None:
    """
    Test ``get_query_for_node`` when filtering/grouping by a dimension.
    """
    database = Database(id=1, name="one", URI="sqlite://")

    dimension_1 = Node(
        name="core.users",
        type=NodeType.DIMENSION,
        tables=[
            Table(
                database=database,
                table="dim_users",
                columns=[
                    Column(name="id", type=ColumnType.INT),
                    Column(name="age", type=ColumnType.INT),
                    Column(name="gender", type=ColumnType.STR),
                ],
            ),
        ],
        columns=[
            Column(name="id", type=ColumnType.INT),
            Column(name="age", type=ColumnType.INT),
            Column(name="gender", type=ColumnType.STR),
        ],
    )

    dimension_2 = Node(
        name="core.bands",
        type=NodeType.DIMENSION,
        tables=[
            Table(
                database=database,
                table="dim_bands",
                columns=[
                    Column(name="uuid", type=ColumnType.INT),
                    Column(name="name", type=ColumnType.STR),
                    Column(name="genre", type=ColumnType.STR),
                ],
            ),
        ],
        columns=[
            Column(name="uuid", type=ColumnType.INT),
            Column(name="name", type=ColumnType.STR),
            Column(name="genre", type=ColumnType.STR),
        ],
    )

    parent = Node(
        name="core.comments",
        tables=[
            Table(
                database=database,
                table="comments",
                columns=[
                    Column(name="ds", type=ColumnType.STR),
                    Column(name="user_id", type=ColumnType.INT),
                    Column(name="band_id", type=ColumnType.INT),
                    Column(name="text", type=ColumnType.STR),
                ],
            ),
        ],
        columns=[
            Column(name="ds", type=ColumnType.STR),
            Column(name="user_id", type=ColumnType.INT, dimension=dimension_1),
            Column(
                name="band_id",
                type=ColumnType.INT,
                dimension=dimension_2,
                dimension_column="uuid",
            ),
            Column(name="text", type=ColumnType.STR),
        ],
    )

    child = Node(
        name="core.num_comments",
        type=NodeType.METRIC,
        expression="SELECT COUNT(*) FROM core.comments",
        parents=[parent],
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE dim_users (id INTEGER, age INTEGER, gender TEXT)")
    connection.execute("CREATE TABLE dim_bands (uuid INTEGER, name TEXT, genre TEXT)")
    connection.execute(
        "CREATE TABLE comments (ds TEXT, user_id INTEGER, band_id INTEGER, text TEXT)",
    )
    mocker.patch("datajunction.sql.transpile.create_engine", return_value=engine)
    session = mocker.MagicMock()
    session.exec().one.side_effect = [dimension_1, dimension_2]

    create_query = get_query_for_node(
        session,
        child,
        ["core.users.gender"],
        ["core.bands.genre='rock'"],
    )
    space = " "
    assert create_query.database_id == 1
    assert (
        create_query.submitted_query
        == f"""SELECT count('*') AS count_1, "core.users".gender{space}
FROM (SELECT comments.ds AS ds, comments.user_id AS user_id, comments.band_id AS band_id, comments.text AS text{space}
FROM comments) AS "core.comments" JOIN (SELECT dim_users.id AS id, dim_users.age AS age, dim_users.gender AS gender{space}
FROM dim_users) AS "core.users" ON "core.comments".user_id = "core.users".id, (SELECT comments.ds AS ds, comments.user_id AS user_id, comments.band_id AS band_id, comments.text AS text{space}
FROM comments) AS "core.comments" JOIN (SELECT dim_bands.uuid AS uuid, dim_bands.name AS name, dim_bands.genre AS genre{space}
FROM dim_bands) AS "core.bands" ON "core.comments".band_id = "core.bands".uuid{space}
WHERE "core.bands".genre = 'rock' GROUP BY "core.users".gender"""
    )


def test_get_filter(mocker: MockerFixture) -> None:
    """
    Test ``get_filter``.
    """
    greater_than = mocker.MagicMock()
    less_than = mocker.MagicMock()
    equals = mocker.MagicMock()
    mocker.patch(
        "datajunction.sql.build.COMPARISONS",
        new={
            ">": greater_than,
            "<": less_than,
            "=": equals,
        },
    )
    column_a = mocker.MagicMock()
    column_date = mocker.MagicMock()
    column_date.type.python_type = datetime.date
    column_dt = mocker.MagicMock()
    column_dt.type.python_type = datetime.datetime
    columns = {"a": column_a, "day": column_date, "dt": column_dt}

    # basic
    get_filter(columns, "a>0")
    greater_than.assert_called_with(column_a, 0)

    # date
    get_filter(columns, "day=2020-01-01")
    equals.assert_called_with(column_date, "2020-01-01 00:00:00")
    get_filter(columns, "day<20200202")
    less_than.assert_called_with(column_date, "2020-02-02 00:00:00")
    get_filter(columns, "day=3/3/2020")
    equals.assert_called_with(column_date, "2020-03-03 00:00:00")

    # datetime
    get_filter(columns, "dt=2012-01-19 17:21:00")
    equals.assert_called_with(column_dt, "2012-01-19 17:21:00")
    with pytest.raises(Exception) as excinfo:
        get_filter(columns, "dt>foo/bar-baz")
    assert str(excinfo.value) == "Invalid date or datetime value: foo/bar-baz"

    # exceptions
    with pytest.raises(Exception) as excinfo:
        get_filter(columns, "invalid")
    assert (
        str(excinfo.value)
        == """The filter "invalid" is invalid
The following error happened:
- The filter "invalid" is not a valid filter. Filters should consist of a dimension name, follow by a valid operator (<=|<|>=|>|!=|=), followed by a value. If the value is a string or date/time it should be enclosed in single quotes. (error code: 100)"""
    )

    with pytest.raises(Exception) as excinfo:
        get_filter(columns, "b>0")
    assert str(excinfo.value) == "Invalid column name: b"

    with pytest.raises(Exception) as excinfo:
        get_filter(columns, "a>open('/etc/passwd').read()")
    assert str(excinfo.value) == "Invalid value: open('/etc/passwd').read()"


def test_get_query_for_sql(mocker: MockerFixture, session: Session) -> None:
    """
    Test ``get_query_for_sql``.
    """
    get_session = mocker.patch("datajunction.sql.build.get_session")
    get_session().__next__.return_value = session

    database = Database(id=1, name="slow", URI="sqlite://", cost=1.0)

    A = Node(
        name="A",
        tables=[
            Table(
                database=database,
                table="A",
                columns=[
                    Column(name="one", type=ColumnType.STR),
                    Column(name="two", type=ColumnType.STR),
                ],
            ),
        ],
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE A (one TEXT, two TEXT)")
    mocker.patch("datajunction.sql.transpile.create_engine", return_value=engine)

    B = Node(
        name="B",
        type=NodeType.METRIC,
        expression="SELECT COUNT(*) AS cnt FROM A",
        parents=[A],
    )
    session.add(B)
    session.commit()

    sql = "SELECT B FROM metrics"
    create_query = get_query_for_sql(sql)

    assert create_query.database_id == 1

    space = " "
    assert (
        create_query.submitted_query
        == f'''SELECT count('*') AS "B"{space}
FROM (SELECT "A".one AS one, "A".two AS two{space}
FROM "A") AS "A"'''
    )


def test_get_query_for_sql_no_metrics(mocker: MockerFixture, session: Session) -> None:
    """
    Test ``get_query_for_sql`` when no metrics are requested.
    """
    get_session = mocker.patch("datajunction.sql.build.get_session")
    get_session().__next__.return_value = session

    database = Database(id=1, name="db", URI="sqlite://")

    dimension = Node(
        name="core.users",
        type=NodeType.DIMENSION,
        tables=[
            Table(
                database=database,
                table="dim_users",
                columns=[
                    Column(name="id", type=ColumnType.INT),
                    Column(name="age", type=ColumnType.INT),
                    Column(name="gender", type=ColumnType.STR),
                ],
            ),
        ],
        columns=[
            Column(name="id", type=ColumnType.INT),
            Column(name="age", type=ColumnType.INT),
            Column(name="gender", type=ColumnType.STR),
        ],
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE dim_users (id INTEGER, age INTEGER, gender TEXT)")
    mocker.patch("datajunction.sql.transpile.create_engine", return_value=engine)

    session.add(dimension)
    session.commit()

    sql = 'SELECT "core.users.gender", "core.users.age" FROM metrics'
    create_query = get_query_for_sql(sql)

    assert create_query.database_id == 1

    space = " "
    assert (
        create_query.submitted_query
        == f'''SELECT "core.users".gender, "core.users".age{space}
FROM (SELECT dim_users.id AS id, dim_users.age AS age, dim_users.gender AS gender{space}
FROM dim_users) AS "core.users"'''
    )

    other_dimension = Node(
        name="core.other_dim",
        type=NodeType.DIMENSION,
        columns=[
            Column(name="full_name", type=ColumnType.STR),
        ],
    )
    session.add(other_dimension)
    session.commit()

    sql = 'SELECT "core.users.gender", "core.other_dim.full_name" FROM metrics'
    with pytest.raises(Exception) as excinfo:
        get_query_for_sql(sql)
    assert (
        str(excinfo.value)
        == "Cannot query from multiple dimensions when no metric is specified"
    )


def test_get_query_for_sql_no_tables(mocker: MockerFixture, session: Session) -> None:
    """
    Test ``get_query_for_sql`` when no tables are involved.
    """
    get_session = mocker.patch("datajunction.sql.build.get_session")
    get_session().__next__.return_value = session

    database = Database(id=1, name="memory", URI="sqlite://")
    session.add(database)
    session.commit()

    sql = "SELECT 1"
    create_query = get_query_for_sql(sql)

    assert create_query.database_id == 1
    assert create_query.submitted_query == "SELECT 1"


def test_get_query_for_sql_having(mocker: MockerFixture, session: Session) -> None:
    """
    Test ``get_query_for_sql``.
    """
    get_session = mocker.patch("datajunction.sql.build.get_session")
    get_session().__next__.return_value = session

    database = Database(id=1, name="slow", URI="sqlite://", cost=1.0)

    A = Node(
        name="A",
        tables=[
            Table(
                database=database,
                table="A",
                columns=[
                    Column(name="one", type=ColumnType.STR),
                    Column(name="two", type=ColumnType.STR),
                ],
            ),
        ],
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE A (one TEXT, two TEXT)")
    mocker.patch("datajunction.sql.transpile.create_engine", return_value=engine)

    B = Node(
        name="B",
        type=NodeType.METRIC,
        expression="SELECT COUNT(*) AS cnt FROM A",
        parents=[A],
    )
    session.add(B)
    session.commit()

    sql = "SELECT B FROM metrics HAVING B > 10"
    create_query = get_query_for_sql(sql)

    assert create_query.database_id == 1

    space = " "
    assert (
        create_query.submitted_query
        == f"""SELECT count('*') AS "B"{space}
FROM (SELECT "A".one AS one, "A".two AS two{space}
FROM "A") AS "A"{space}
HAVING count('*') > 10"""
    )

    sql = "SELECT B FROM metrics HAVING C > 10"
    with pytest.raises(Exception) as excinfo:
        get_query_for_sql(sql)
    assert str(excinfo.value) == "Invalid dimension: C"


def test_get_query_for_sql_with_dimensions(
    mocker: MockerFixture,
    session: Session,
) -> None:
    """
    Test ``get_query_for_sql`` with dimensions in the query.
    """
    get_session = mocker.patch("datajunction.sql.build.get_session")
    get_session().__next__.return_value = session

    database = Database(id=1, name="slow", URI="sqlite://", cost=1.0)

    dimension = Node(
        name="core.users",
        type=NodeType.DIMENSION,
        tables=[
            Table(
                database=database,
                table="dim_users",
                columns=[
                    Column(name="id", type=ColumnType.INT),
                    Column(name="age", type=ColumnType.INT),
                    Column(name="gender", type=ColumnType.STR),
                ],
            ),
        ],
        columns=[
            Column(name="id", type=ColumnType.INT),
            Column(name="age", type=ColumnType.INT),
            Column(name="gender", type=ColumnType.STR),
        ],
    )

    parent = Node(
        name="core.comments",
        tables=[
            Table(
                database=database,
                table="comments",
                columns=[
                    Column(name="ds", type=ColumnType.STR),
                    Column(name="user_id", type=ColumnType.INT),
                    Column(name="text", type=ColumnType.STR),
                ],
            ),
        ],
        columns=[
            Column(name="ds", type=ColumnType.STR),
            Column(name="user_id", type=ColumnType.INT, dimension=dimension),
            Column(name="text", type=ColumnType.STR),
        ],
    )

    child = Node(
        name="core.num_comments",
        type=NodeType.METRIC,
        expression="SELECT COUNT(*) FROM core.comments",
        parents=[parent],
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE dim_users (id INTEGER, age INTEGER, gender TEXT)")
    connection.execute("CREATE TABLE comments (ds TEXT, user_id INTEGER, text TEXT)")
    mocker.patch("datajunction.sql.transpile.create_engine", return_value=engine)

    session.add(child)
    session.add(dimension)
    session.commit()

    sql = """
SELECT "core.users.gender", "core.num_comments"
FROM metrics
WHERE "core.users.age" > 25
GROUP BY "core.users.gender"
    """
    create_query = get_query_for_sql(sql)

    assert create_query.database_id == 1

    space = " "
    assert (
        create_query.submitted_query
        == f"""SELECT "core.users".gender, count('*') AS "core.num_comments"{space}
FROM (SELECT comments.ds AS ds, comments.user_id AS user_id, comments.text AS text{space}
FROM comments) AS "core.comments" JOIN (SELECT dim_users.id AS id, dim_users.age AS age, dim_users.gender AS gender{space}
FROM dim_users) AS "core.users" ON "core.comments".user_id = "core.users".id{space}
WHERE "core.users".age > 25 GROUP BY "core.users".gender"""
    )

    sql = """
SELECT "core.users.invalid", "core.num_comments"
FROM metrics
WHERE "core.users.age" > 25
GROUP BY "core.users.invalid"
    """
    with pytest.raises(Exception) as excinfo:
        get_query_for_sql(sql)
    assert str(excinfo.value) == "Invalid dimension: core.users.invalid"


def test_get_query_for_sql_with_dimensions_order_by(
    mocker: MockerFixture,
    session: Session,
) -> None:
    """
    Test ``get_query_for_sql`` with dimensions in the query and ``ORDER BY``.
    """
    get_session = mocker.patch("datajunction.sql.build.get_session")
    get_session().__next__.return_value = session

    database = Database(id=1, name="slow", URI="sqlite://", cost=1.0)

    dimension = Node(
        name="core.users",
        type=NodeType.DIMENSION,
        tables=[
            Table(
                database=database,
                table="dim_users",
                columns=[
                    Column(name="id", type=ColumnType.INT),
                    Column(name="age", type=ColumnType.INT),
                    Column(name="gender", type=ColumnType.STR),
                ],
            ),
        ],
        columns=[
            Column(name="id", type=ColumnType.INT),
            Column(name="age", type=ColumnType.INT),
            Column(name="gender", type=ColumnType.STR),
        ],
    )

    parent = Node(
        name="core.comments",
        tables=[
            Table(
                database=database,
                table="comments",
                columns=[
                    Column(name="ds", type=ColumnType.STR),
                    Column(name="user_id", type=ColumnType.INT),
                    Column(name="text", type=ColumnType.STR),
                ],
            ),
        ],
        columns=[
            Column(name="ds", type=ColumnType.STR),
            Column(name="user_id", type=ColumnType.INT, dimension=dimension),
            Column(name="text", type=ColumnType.STR),
        ],
    )

    child = Node(
        name="core.num_comments",
        type=NodeType.METRIC,
        expression="SELECT COUNT(*) FROM core.comments",
        parents=[parent],
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE dim_users (id INTEGER, age INTEGER, gender TEXT)")
    connection.execute("CREATE TABLE comments (ds TEXT, user_id INTEGER, text TEXT)")
    mocker.patch("datajunction.sql.transpile.create_engine", return_value=engine)

    session.add(child)
    session.add(dimension)
    session.commit()

    sql = """
SELECT "core.users.gender" AS "core.users.gender",
       "core.num_comments" AS "core.num_comments"
FROM main.metrics
GROUP BY "core.users.gender"
ORDER BY "core.num_comments" DESC
LIMIT 100;
    """
    create_query = get_query_for_sql(sql)

    space = " "

    assert create_query.database_id == 1
    assert (
        create_query.submitted_query
        == f"""SELECT "core.users".gender AS "core.users.gender", count('*') AS "core.num_comments"{space}
FROM (SELECT comments.ds AS ds, comments.user_id AS user_id, comments.text AS text{space}
FROM comments) AS "core.comments" JOIN (SELECT dim_users.id AS id, dim_users.age AS age, dim_users.gender AS gender{space}
FROM dim_users) AS "core.users" ON "core.comments".user_id = "core.users".id GROUP BY "core.users".gender ORDER BY count('*') DESC
 LIMIT 100 OFFSET 0"""
    )

    sql = """
SELECT "core.users.gender" AS "core.users.gender",
       "core.num_comments" AS "core.num_comments"
FROM main.metrics
GROUP BY "core.users.gender"
ORDER BY "core.num_comments" ASC
LIMIT 100;
    """
    create_query = get_query_for_sql(sql)

    assert (
        create_query.submitted_query
        == f"""SELECT "core.users".gender AS "core.users.gender", count('*') AS "core.num_comments"{space}
FROM (SELECT comments.ds AS ds, comments.user_id AS user_id, comments.text AS text{space}
FROM comments) AS "core.comments" JOIN (SELECT dim_users.id AS id, dim_users.age AS age, dim_users.gender AS gender{space}
FROM dim_users) AS "core.users" ON "core.comments".user_id = "core.users".id GROUP BY "core.users".gender ORDER BY count('*')
 LIMIT 100 OFFSET 0"""
    )
    sql = """
SELECT "core.users.gender" AS "core.users.gender",
       "core.num_comments" AS "core.num_comments"
FROM main.metrics
GROUP BY "core.users.gender"
ORDER BY "core.num_comments" ASC
LIMIT 100;
    """
    create_query = get_query_for_sql(sql)

    assert (
        create_query.submitted_query
        == f"""SELECT "core.users".gender AS "core.users.gender", count('*') AS "core.num_comments"{space}
FROM (SELECT comments.ds AS ds, comments.user_id AS user_id, comments.text AS text{space}
FROM comments) AS "core.comments" JOIN (SELECT dim_users.id AS id, dim_users.age AS age, dim_users.gender AS gender{space}
FROM dim_users) AS "core.users" ON "core.comments".user_id = "core.users".id GROUP BY "core.users".gender ORDER BY count('*')
 LIMIT 100 OFFSET 0"""
    )

    sql = """
SELECT "core.users.gender" AS "core.users.gender",
       "core.num_comments" AS "core.num_comments"
FROM main.metrics
GROUP BY "core.users.gender"
ORDER BY "core.users.gender" ASC
LIMIT 100;
    """
    create_query = get_query_for_sql(sql)

    assert (
        create_query.submitted_query
        == f"""SELECT "core.users".gender AS "core.users.gender", count('*') AS "core.num_comments"{space}
FROM (SELECT comments.ds AS ds, comments.user_id AS user_id, comments.text AS text{space}
FROM comments) AS "core.comments" JOIN (SELECT dim_users.id AS id, dim_users.age AS age, dim_users.gender AS gender{space}
FROM dim_users) AS "core.users" ON "core.comments".user_id = "core.users".id GROUP BY "core.users".gender ORDER BY "core.users".gender
 LIMIT 100 OFFSET 0"""
    )

    sql = """
SELECT "core.users.gender" AS "core.users.gender",
       "core.num_comments" AS "core.num_comments"
FROM main.metrics
GROUP BY "core.users.gender"
ORDER BY invalid ASC
LIMIT 100;
    """
    with pytest.raises(Exception) as excinfo:
        get_query_for_sql(sql)
    assert str(excinfo.value) == "Invalid identifier: invalid"


def test_get_query_for_sql_compound_names(
    mocker: MockerFixture,
    session: Session,
) -> None:
    """
    Test ``get_query_for_sql`` with nodes with compound names.
    """
    get_session = mocker.patch("datajunction.sql.build.get_session")
    get_session().__next__.return_value = session

    database = Database(id=1, name="slow", URI="sqlite://", cost=1.0)

    A = Node(
        name="core.A",
        tables=[
            Table(
                database=database,
                table="A",
                columns=[
                    Column(name="one", type=ColumnType.STR),
                    Column(name="two", type=ColumnType.STR),
                ],
            ),
        ],
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE A (one TEXT, two TEXT)")
    mocker.patch("datajunction.sql.transpile.create_engine", return_value=engine)

    B = Node(
        name="core.B",
        type=NodeType.METRIC,
        expression="SELECT COUNT(*) AS cnt FROM core.A",
        parents=[A],
    )
    session.add(B)
    session.commit()

    sql = "SELECT core.B FROM metrics"
    create_query = get_query_for_sql(sql)

    assert create_query.database_id == 1

    space = " "
    assert (
        create_query.submitted_query
        == f'''SELECT count('*') AS "core.B"{space}
FROM (SELECT "A".one AS one, "A".two AS two{space}
FROM "A") AS "core.A"'''
    )


def test_get_query_for_sql_multiple_databases(
    mocker: MockerFixture,
    session: Session,
) -> None:
    """
    Test ``get_query_for_sql`` when the parents are in multiple databases.
    """
    get_session = mocker.patch("datajunction.sql.build.get_session")
    get_session().__next__.return_value = session

    database_1 = Database(id=1, name="slow", URI="sqlite://", cost=10.0)
    database_2 = Database(id=2, name="fast", URI="sqlite://", cost=1.0)

    A = Node(
        name="A",
        tables=[
            Table(
                database=database_1,
                table="A",
                columns=[
                    Column(name="one", type=ColumnType.STR),
                    Column(name="two", type=ColumnType.STR),
                ],
            ),
            Table(
                database=database_2,
                table="A",
                columns=[
                    Column(name="one", type=ColumnType.STR),
                ],
            ),
        ],
        columns=[
            Column(name="one", type=ColumnType.STR),
            Column(name="two", type=ColumnType.STR),
        ],
    )

    engine = create_engine(database_1.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE A (one TEXT, two TEXT)")
    mocker.patch("datajunction.sql.transpile.create_engine", return_value=engine)

    B = Node(
        name="B",
        type=NodeType.METRIC,
        expression="SELECT COUNT(*) AS cnt FROM A",
        parents=[A],
    )
    session.add(B)
    session.commit()

    sql = "SELECT B FROM metrics"
    create_query = get_query_for_sql(sql)

    assert create_query.database_id == 2  # fast

    B.expression = "SELECT COUNT(two) AS cnt FROM A"
    session.add(B)
    session.commit()

    sql = "SELECT B FROM metrics"
    create_query = get_query_for_sql(sql)

    assert create_query.database_id == 1  # slow


def test_get_query_for_sql_multiple_metrics(
    mocker: MockerFixture,
    session: Session,
) -> None:
    """
    Test ``get_query_for_sql`` with multiple metrics.
    """
    get_session = mocker.patch("datajunction.sql.build.get_session")
    get_session().__next__.return_value = session

    database = Database(id=1, name="slow", URI="sqlite://", cost=1.0)

    A = Node(
        name="A",
        tables=[
            Table(
                database=database,
                table="A",
                columns=[
                    Column(name="one", type=ColumnType.STR),
                    Column(name="two", type=ColumnType.STR),
                ],
            ),
        ],
        columns=[
            Column(name="one", type=ColumnType.STR),
            Column(name="two", type=ColumnType.STR),
        ],
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE A (one TEXT, two TEXT)")
    mocker.patch("datajunction.sql.transpile.create_engine", return_value=engine)

    B = Node(
        name="B",
        type=NodeType.METRIC,
        expression="SELECT COUNT(*) AS cnt FROM A",
        parents=[A],
    )
    session.add(B)
    C = Node(
        name="C",
        type=NodeType.METRIC,
        expression="SELECT MAX(one) AS max_one FROM A",
        parents=[A],
    )
    session.add(C)
    session.commit()

    sql = "SELECT B, C FROM metrics"
    create_query = get_query_for_sql(sql)

    assert create_query.database_id == 1

    space = " "
    assert (
        create_query.submitted_query
        == f'''SELECT count('*') AS "B", max("A".one) AS "C"{space}
FROM (SELECT "A".one AS one, "A".two AS two{space}
FROM "A") AS "A"'''
    )


def test_get_query_for_sql_non_identifiers(
    mocker: MockerFixture,
    session: Session,
) -> None:
    """
    Test ``get_query_for_sql`` with metrics and non-identifiers in the ``SELECT``.
    """
    get_session = mocker.patch("datajunction.sql.build.get_session")
    get_session().__next__.return_value = session

    database = Database(id=1, name="slow", URI="sqlite://", cost=1.0)

    A = Node(
        name="A",
        tables=[
            Table(
                database=database,
                table="A",
                columns=[
                    Column(name="one", type=ColumnType.STR),
                    Column(name="two", type=ColumnType.STR),
                ],
            ),
        ],
        columns=[
            Column(name="one", type=ColumnType.STR),
            Column(name="two", type=ColumnType.STR),
        ],
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE A (one TEXT, two TEXT)")
    mocker.patch("datajunction.sql.transpile.create_engine", return_value=engine)

    B = Node(
        name="B",
        type=NodeType.METRIC,
        expression="SELECT COUNT(*) AS cnt FROM A",
        parents=[A],
    )
    session.add(B)
    C = Node(
        name="C",
        type=NodeType.METRIC,
        expression="SELECT MAX(one) AS max_one FROM A",
        parents=[A],
    )
    session.add(C)
    session.commit()

    sql = "SELECT B, C, 'test' FROM metrics"
    create_query = get_query_for_sql(sql)

    assert create_query.database_id == 1

    space = " "
    assert (
        create_query.submitted_query
        == f'''SELECT count('*') AS "B", max("A".one) AS "C", test{space}
FROM (SELECT "A".one AS one, "A".two AS two{space}
FROM "A") AS "A"'''
    )


def test_get_query_for_sql_different_parents(
    mocker: MockerFixture,
    session: Session,
) -> None:
    """
    Test ``get_query_for_sql`` with metrics with different parents.
    """
    get_session = mocker.patch("datajunction.sql.build.get_session")
    get_session().__next__.return_value = session

    database = Database(id=1, name="slow", URI="sqlite://", cost=1.0)

    A = Node(
        name="A",
        tables=[
            Table(
                database=database,
                table="A",
                columns=[
                    Column(name="one", type=ColumnType.STR),
                    Column(name="two", type=ColumnType.STR),
                ],
            ),
        ],
    )
    B = Node(
        name="B",
        tables=[
            Table(
                database=database,
                table="B",
                columns=[
                    Column(name="one", type=ColumnType.STR),
                    Column(name="two", type=ColumnType.STR),
                ],
            ),
        ],
    )
    C = Node(
        name="C",
        type=NodeType.METRIC,
        expression="SELECT COUNT(*) AS cnt FROM A",
        parents=[A],
    )
    session.add(C)
    D = Node(
        name="D",
        type=NodeType.METRIC,
        expression="SELECT MAX(one) AS max_one FROM A",
        parents=[B],
    )
    session.add(D)
    session.commit()

    sql = "SELECT C, D FROM metrics"
    with pytest.raises(Exception) as excinfo:
        get_query_for_sql(sql)
    assert str(excinfo.value) == "Metrics C and D have non-shared parents"


def test_get_query_for_sql_not_metric(mocker: MockerFixture, session: Session) -> None:
    """
    Test ``get_query_for_sql`` when the projection is not a metric node.
    """
    get_session = mocker.patch("datajunction.sql.build.get_session")
    get_session().__next__.return_value = session

    database = Database(id=1, name="slow", URI="sqlite://", cost=1.0)

    A = Node(
        name="A",
        tables=[
            Table(
                database=database,
                table="A",
                columns=[
                    Column(name="one", type=ColumnType.STR),
                    Column(name="two", type=ColumnType.STR),
                ],
            ),
        ],
    )

    B = Node(
        name="B",
        expression="SELECT one FROM A",
        parents=[A],
    )
    session.add(B)
    session.commit()

    sql = "SELECT B FROM metrics"
    with pytest.raises(Exception) as excinfo:
        get_query_for_sql(sql)
    assert str(excinfo.value) == "Invalid dimension: B"


def test_get_query_for_sql_no_databases(
    mocker: MockerFixture,
    session: Session,
) -> None:
    """
    Test ``get_query_for_sql`` when no common databases are found.
    """
    get_session = mocker.patch("datajunction.sql.build.get_session")
    get_session().__next__.return_value = session

    A = Node(
        name="A",
        tables=[],
    )

    B = Node(
        name="B",
        type=NodeType.METRIC,
        expression="SELECT COUNT(*) AS cnt FROM A",
        parents=[A],
    )
    session.add(B)
    session.commit()

    sql = "SELECT B FROM metrics"
    with pytest.raises(Exception) as excinfo:
        get_query_for_sql(sql)
    assert str(excinfo.value) == "No valid database was found"


def test_get_query_for_sql_alias(mocker: MockerFixture, session: Session) -> None:
    """
    Test ``get_query_for_sql`` with aliases.
    """
    get_session = mocker.patch("datajunction.sql.build.get_session")
    get_session().__next__.return_value = session

    database = Database(id=1, name="slow", URI="sqlite://", cost=1.0)

    A = Node(
        name="A",
        tables=[
            Table(
                database=database,
                table="A",
                columns=[
                    Column(name="one", type=ColumnType.STR),
                    Column(name="two", type=ColumnType.STR),
                ],
            ),
        ],
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE A (one TEXT, two TEXT)")
    mocker.patch("datajunction.sql.transpile.create_engine", return_value=engine)

    B = Node(
        name="B",
        type=NodeType.METRIC,
        expression="SELECT COUNT(*) AS cnt FROM A",
        parents=[A],
    )
    session.add(B)
    session.commit()

    sql = "SELECT B AS my_metric FROM metrics"
    create_query = get_query_for_sql(sql)

    assert create_query.database_id == 1

    space = " "
    assert (
        create_query.submitted_query
        == f'''SELECT count('*') AS my_metric{space}
FROM (SELECT "A".one AS one, "A".two AS two{space}
FROM "A") AS "A"'''
    )


def test_get_query_for_sql_where_groupby(
    mocker: MockerFixture,
    session: Session,
) -> None:
    """
    Test ``get_query_for_sql`` with a where and a group by.
    """
    get_session = mocker.patch("datajunction.sql.build.get_session")
    get_session().__next__.return_value = session

    database = Database(id=1, name="slow", URI="sqlite://", cost=1.0)

    comments = Node(
        name="core.comments",
        tables=[
            Table(
                database=database,
                table="comments",
                columns=[
                    Column(name="user_id", type=ColumnType.INT),
                    Column(name="comment", type=ColumnType.STR),
                ],
            ),
        ],
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE comments (user_id INT, comment TEXT)")
    mocker.patch("datajunction.sql.transpile.create_engine", return_value=engine)

    num_comments = Node(
        name="core.num_comments",
        type=NodeType.METRIC,
        expression="SELECT COUNT(*) FROM core.comments",
        parents=[comments],
    )
    session.add(num_comments)
    session.commit()

    sql = """
SELECT "core.num_comments", "core.comments.user_id" FROM metrics
WHERE "core.comments.user_id" > 1
GROUP BY "core.comments.user_id"
    """
    create_query = get_query_for_sql(sql)

    assert create_query.database_id == 1

    space = " "
    assert (
        create_query.submitted_query
        == f"""SELECT count('*') AS "core.num_comments", "core.comments".user_id{space}
FROM (SELECT comments.user_id AS user_id, comments.comment AS comment{space}
FROM comments) AS "core.comments"{space}
WHERE "core.comments".user_id > 1 GROUP BY "core.comments".user_id"""
    )


def test_get_query_for_sql_date_trunc(
    mocker: MockerFixture,
    session: Session,
) -> None:
    """
    Test ``get_query_for_sql`` with a call to ``DATE_TRUNC``.
    """
    get_session = mocker.patch("datajunction.sql.build.get_session")
    get_session().__next__.return_value = session

    database = Database(id=1, name="db", URI="sqlite://")

    comments = Node(
        name="core.comments",
        tables=[
            Table(
                database=database,
                table="comments",
                columns=[
                    Column(name="user_id", type=ColumnType.INT),
                    Column(name="timestamp", type=ColumnType.DATETIME),
                ],
            ),
        ],
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE comments (user_id INT, timestamp DATETIME)")
    mocker.patch("datajunction.sql.transpile.create_engine", return_value=engine)

    num_comments = Node(
        name="core.num_comments",
        type=NodeType.METRIC,
        expression="SELECT COUNT(*) FROM core.comments",
        parents=[comments],
    )
    session.add(num_comments)
    session.commit()

    sql = """
SELECT
    DATE_TRUNC('day', "core.comments.timestamp") AS "__timestamp",
    "core.num_comments"
FROM metrics
GROUP BY
    DATE_TRUNC('day', "core.comments.timestamp")
    """
    create_query = get_query_for_sql(sql)

    assert create_query.database_id == 1

    space = " "
    assert (
        create_query.submitted_query
        == f"""SELECT datetime("core.comments".timestamp, 'start of day') AS __timestamp, count('*') AS "core.num_comments"{space}
FROM (SELECT comments.user_id AS user_id, comments.timestamp AS timestamp{space}
FROM comments) AS "core.comments" GROUP BY datetime("core.comments".timestamp, 'start of day')"""
    )


def test_get_query_for_sql_invalid_column(
    mocker: MockerFixture,
    session: Session,
) -> None:
    """
    Test ``get_query_for_sql`` with an invalid column.
    """
    get_session = mocker.patch("datajunction.sql.build.get_session")
    get_session().__next__.return_value = session

    database = Database(id=1, name="slow", URI="sqlite://", cost=1.0)

    comments = Node(
        name="core.comments",
        tables=[
            Table(
                database=database,
                table="comments",
                columns=[
                    Column(name="user_id", type=ColumnType.INT),
                    Column(name="comment", type=ColumnType.STR),
                ],
            ),
        ],
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE comments (user_id INT, comment TEXT)")
    mocker.patch("datajunction.sql.transpile.create_engine", return_value=engine)

    num_comments = Node(
        name="core.num_comments",
        type=NodeType.METRIC,
        expression="SELECT COUNT(*) FROM core.comments",
        parents=[comments],
    )
    session.add(num_comments)
    session.commit()

    sql = """
SELECT "core.num_comments" FROM metrics
WHERE "core.some_other_parent.user_id" > 1
    """
    with pytest.raises(Exception) as excinfo:
        get_query_for_sql(sql)
    assert str(excinfo.value) == "Invalid dimension: core.some_other_parent.user_id"


def test_get_dimensions_from_filters() -> None:
    """
    Test ``get_dimensions_from_filters``.
    """
    assert get_dimensions_from_filters(["a>1", "b=10"]) == {"a", "b"}

    with pytest.raises(Exception) as excinfo:
        get_dimensions_from_filters(["aaaa"])
    assert (
        str(excinfo.value)
        == """The filter "aaaa" is invalid
The following error happened:
- The filter "aaaa" is not a valid filter. Filters should consist of a dimension name, follow by a valid operator (<=|<|>=|>|!=|=), followed by a value. If the value is a string or date/time it should be enclosed in single quotes. (error code: 100)"""
    )


def test_find_on_clause(mocker: MockerFixture) -> None:
    """
    Test ``find_on_clause``.
    """
    database = Database(id=1, name="one", URI="sqlite://")

    dimension = Node(
        name="core.users",
        type=NodeType.DIMENSION,
        tables=[
            Table(
                database=database,
                table="dim_users",
                columns=[
                    Column(name="id", type=ColumnType.INT),
                    Column(name="age", type=ColumnType.INT),
                    Column(name="gender", type=ColumnType.STR),
                ],
            ),
        ],
        columns=[
            Column(name="id", type=ColumnType.INT),
            Column(name="age", type=ColumnType.INT),
            Column(name="gender", type=ColumnType.STR),
        ],
    )

    parent = Node(
        name="core.comments",
        tables=[
            Table(
                database=database,
                table="comments",
                columns=[
                    Column(name="ds", type=ColumnType.STR),
                    Column(name="user_id", type=ColumnType.INT, dimension=dimension),
                    Column(name="text", type=ColumnType.STR),
                ],
            ),
        ],
        columns=[
            Column(name="ds", type=ColumnType.STR),
            Column(name="user_id", type=ColumnType.INT, dimension=dimension),
            Column(name="text", type=ColumnType.STR),
        ],
    )

    child = Node(name="core.num_comments", parents=[parent])

    node_select = mocker.MagicMock()
    subquery = mocker.MagicMock()
    find_on_clause(child, node_select, dimension, subquery)

    assert node_select.columns.__getitem__.called_with("user_id")
    assert subquery.columns.__getitem__.called_with("id")


def test_find_on_clause_parent_no_columns(mocker: MockerFixture) -> None:
    """
    Test ``find_on_clause`` when a parent has no columns.

    I think we expect all nodes to have at least one column, so this test is just for
    completeness.
    """
    database = Database(id=1, name="one", URI="sqlite://")

    dimension = Node(
        name="core.users",
        type=NodeType.DIMENSION,
        tables=[
            Table(
                database=database,
                table="dim_users",
                columns=[
                    Column(name="id", type=ColumnType.INT),
                    Column(name="age", type=ColumnType.INT),
                    Column(name="gender", type=ColumnType.STR),
                ],
            ),
        ],
        columns=[
            Column(name="id", type=ColumnType.INT),
            Column(name="age", type=ColumnType.INT),
            Column(name="gender", type=ColumnType.STR),
        ],
    )

    parent_1 = Node(
        name="core.comments",
        tables=[
            Table(
                database=database,
                table="comments",
                columns=[
                    Column(name="ds", type=ColumnType.STR),
                    Column(name="user_id", type=ColumnType.INT, dimension=dimension),
                    Column(name="text", type=ColumnType.STR),
                ],
            ),
        ],
        columns=[
            Column(name="ds", type=ColumnType.STR),
            Column(name="user_id", type=ColumnType.INT, dimension=dimension),
            Column(name="text", type=ColumnType.STR),
        ],
    )

    parent_2 = Node(
        name="a_weird_node",
        tables=[
            Table(
                database=database,
                table="empty",
                columns=[],
            ),
        ],
        columns=[],
    )

    child = Node(name="core.num_comments", parents=[parent_2, parent_1])

    node_select = mocker.MagicMock()
    subquery = mocker.MagicMock()
    find_on_clause(child, node_select, dimension, subquery)

    assert node_select.columns.__getitem__.called_with("user_id")


def test_find_on_clause_parent_invalid_reference(mocker: MockerFixture) -> None:
    """
    Test ``find_on_clause`` when a parent has no columns.

    The compiler should check that the dimension is valid, but the table could change.
    """
    database = Database(id=1, name="one", URI="sqlite://")

    dimension = Node(
        name="core.users",
        type=NodeType.DIMENSION,
        tables=[
            Table(
                database=database,
                table="dim_users",
                columns=[
                    Column(name="id", type=ColumnType.INT),
                    Column(name="age", type=ColumnType.INT),
                    Column(name="gender", type=ColumnType.STR),
                ],
            ),
        ],
        columns=[
            Column(name="id", type=ColumnType.INT),
            Column(name="age", type=ColumnType.INT),
            Column(name="gender", type=ColumnType.STR),
        ],
    )

    parent = Node(
        name="core.comments",
        tables=[
            Table(
                database=database,
                table="comments",
                columns=[
                    Column(name="ds", type=ColumnType.STR),
                    Column(name="user_id", type=ColumnType.INT),
                    Column(name="text", type=ColumnType.STR),
                ],
            ),
        ],
        columns=[
            Column(name="ds", type=ColumnType.STR),
            Column(name="user_id", type=ColumnType.INT),
            Column(name="text", type=ColumnType.STR),
        ],
    )

    child = Node(name="core.num_comments", parents=[parent])

    node_select = mocker.MagicMock()
    subquery = mocker.MagicMock()

    with pytest.raises(Exception) as excinfo:
        find_on_clause(child, node_select, dimension, subquery)
    assert (
        str(excinfo.value)
        == "Node core.num_comments has no columns with dimension core.users"
    )


def test_get_join_columns() -> None:
    """
    Test ``get_join_columns``.
    """
    database = Database(id=1, name="one", URI="sqlite://")

    dimension = Node(
        name="core.users",
        type=NodeType.DIMENSION,
        tables=[
            Table(
                database=database,
                table="dim_users",
                columns=[
                    Column(name="id", type=ColumnType.INT),
                    Column(name="age", type=ColumnType.INT),
                    Column(name="gender", type=ColumnType.STR),
                ],
            ),
        ],
        columns=[
            Column(name="id", type=ColumnType.INT),
            Column(name="age", type=ColumnType.INT),
            Column(name="gender", type=ColumnType.STR),
        ],
    )

    orphan = Node(name="orphan")

    with pytest.raises(Exception) as excinfo:
        get_join_columns(orphan, dimension)
    assert str(excinfo.value) == "Node orphan has no columns with dimension core.users"

    parent_without_columns = Node(name="parent_without_columns")
    broken = Node(name="broken", parents=[parent_without_columns])

    with pytest.raises(Exception) as excinfo:
        get_join_columns(broken, dimension)
    assert str(excinfo.value) == "Node broken has no columns with dimension core.users"

    parent = Node(
        name="parent",
        tables=[
            Table(
                database=database,
                table="comments",
                columns=[
                    Column(name="ds", type=ColumnType.STR),
                    Column(name="user_id", type=ColumnType.INT),
                    Column(name="text", type=ColumnType.STR),
                ],
            ),
        ],
        columns=[
            Column(name="ds", type=ColumnType.STR),
            Column(name="user_id", type=ColumnType.INT, dimension=dimension),
            Column(name="text", type=ColumnType.STR),
        ],
    )

    child = Node(name="child", parents=[parent_without_columns, parent])

    parent_name, column_name, dimension_column = get_join_columns(child, dimension)
    assert parent_name == "parent"
    assert column_name == "user_id"
    assert dimension_column == "id"
