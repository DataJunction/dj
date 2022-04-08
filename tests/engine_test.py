"""
Tests for ``datajunction.engine``.
"""
# pylint: disable=invalid-name

import pytest
from pytest_mock import MockerFixture
from sqlalchemy.engine import create_engine
from sqlmodel import Session

from datajunction.engine import (
    ColumnMetadata,
    Description,
    TypeEnum,
    get_columns_from_description,
    get_filter,
    get_query_for_node,
    get_query_for_sql,
    run_query,
)
from datajunction.models.database import Column, Database, Table
from datajunction.models.node import Node
from datajunction.models.query import Query
from datajunction.typing import ColumnType


def test_get_columns_from_description(mocker: MockerFixture) -> None:
    """
    Test ``get_columns_from_description``.
    """
    dialect = mocker.MagicMock()
    dialect.dbapi.STRING = "STRING"
    dialect.dbapi.BINARY = "BINARY"
    dialect.dbapi.NUMBER = "NUMBER"
    dialect.dbapi.DATETIME = "DATETIME"

    description: Description = [
        ("a", "STRING", "", "", "", "", False),
        ("b", "BINARY", "", "", "", "", False),
        ("c", "NUMBER", "", "", "", "", False),
        ("d", "DATETIME", "", "", "", "", False),
        ("e", "INVALID", "", "", "", "", False),
    ]

    assert get_columns_from_description(description, dialect) == [
        ColumnMetadata(name="a", type=TypeEnum.STRING),
        ColumnMetadata(name="b", type=TypeEnum.BINARY),
        ColumnMetadata(name="c", type=TypeEnum.NUMBER),
        ColumnMetadata(name="d", type=TypeEnum.DATETIME),
        ColumnMetadata(name="e", type=TypeEnum.UNKNOWN),
    ]


def test_run_query() -> None:
    """
    Test ``run_query``.
    """
    database = Database(name="test", URI="sqlite://")
    query = Query(
        database=database,
        submitted_query="SELECT 1",
        executed_query="SELECT 1",
    )
    sql, columns, stream = run_query(query)[0]
    assert sql == "SELECT 1"
    assert columns == [ColumnMetadata(name="1", type=TypeEnum.STRING)]
    assert list(stream) == [(1,)]


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
        expression="SELECT COUNT(*) AS cnt FROM A",
        parents=[parent],
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE B (cnt INTEGER)")
    mocker.patch("datajunction.sql.transpile.create_engine", return_value=engine)

    create_query = get_query_for_node(child, [], [])
    assert create_query.database_id == 1
    assert create_query.submitted_query == 'SELECT "B".cnt \nFROM "B"'


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
        expression="SELECT COUNT(*) AS cnt FROM A",
        parents=[parent],
    )

    engine = create_engine(database.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE B (cnt INTEGER)")
    mocker.patch("datajunction.sql.transpile.create_engine", return_value=engine)

    create_query = get_query_for_node(child, [], [], 1)
    assert create_query.database_id == 1
    assert create_query.submitted_query == 'SELECT "B".cnt \nFROM "B"'

    with pytest.raises(Exception) as excinfo:
        get_query_for_node(child, [], [], 2)
    assert str(excinfo.value) == "Unable to compute B on database 2"


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
        expression="SELECT COUNT(*) AS cnt FROM A",
        parents=[parent],
    )

    mocker.patch("datajunction.engine.get_computable_databases", return_value=[])

    with pytest.raises(Exception) as excinfo:
        get_query_for_node(child, [], [])
    assert str(excinfo.value) == "Unable to compute B (no common database)"


def test_get_filter(mocker: MockerFixture) -> None:
    """
    Test ``get_filter``.
    """
    greater_than = mocker.MagicMock()
    mocker.patch("datajunction.engine.COMPARISONS", new={">": greater_than})
    column_a = mocker.MagicMock()
    columns = {"a": column_a}

    get_filter(columns, "a>0")
    greater_than.assert_called_with(column_a, 0)

    with pytest.raises(Exception) as excinfo:
        get_filter(columns, "invalid")
    assert str(excinfo.value) == "Invalid filter: invalid"

    with pytest.raises(Exception) as excinfo:
        get_filter(columns, "b>0")
    assert str(excinfo.value) == "Invalid column name: b"

    with pytest.raises(Exception) as excinfo:
        get_filter(columns, "a>=0")
    assert str(excinfo.value) == "Invalid operation: >= (valid: >)"

    with pytest.raises(Exception) as excinfo:
        get_filter(columns, "a>open('/etc/passwd').read()")
    assert str(excinfo.value) == "Invalid value: open('/etc/passwd').read()"


def test_get_query_for_sql(mocker: MockerFixture, session: Session) -> None:
    """
    Test ``get_query_for_sql``.
    """
    get_session = mocker.patch("datajunction.engine.get_session")
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


def test_get_query_for_sql_compound_names(
    mocker: MockerFixture,
    session: Session,
) -> None:
    """
    Test ``get_query_for_sql`` with nodes with compound names.
    """
    get_session = mocker.patch("datajunction.engine.get_session")
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
    get_session = mocker.patch("datajunction.engine.get_session")
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
    )

    engine = create_engine(database_1.URI)
    connection = engine.connect()
    connection.execute("CREATE TABLE A (one TEXT, two TEXT)")
    mocker.patch("datajunction.sql.transpile.create_engine", return_value=engine)

    B = Node(
        name="B",
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
    get_session = mocker.patch("datajunction.engine.get_session")
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
        expression="SELECT COUNT(*) AS cnt FROM A",
        parents=[A],
    )
    session.add(B)
    C = Node(
        name="C",
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
    get_session = mocker.patch("datajunction.engine.get_session")
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
        expression="SELECT COUNT(*) AS cnt FROM A",
        parents=[A],
    )
    session.add(B)
    C = Node(
        name="C",
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
    get_session = mocker.patch("datajunction.engine.get_session")
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
        expression="SELECT COUNT(*) AS cnt FROM A",
        parents=[A],
    )
    session.add(C)
    D = Node(
        name="D",
        expression="SELECT MAX(one) AS max_one FROM A",
        parents=[B],
    )
    session.add(D)
    session.commit()

    sql = "SELECT C, D FROM metrics"
    with pytest.raises(Exception) as excinfo:
        get_query_for_sql(sql)
    assert str(excinfo.value) == "All metrics should have the same parents"


def test_get_query_for_sql_not_metric(mocker: MockerFixture, session: Session) -> None:
    """
    Test ``get_query_for_sql`` when the projection is not a metric node.
    """
    get_session = mocker.patch("datajunction.engine.get_session")
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
    assert str(excinfo.value) == "Not a valid metric: B"


def test_get_query_for_sql_no_databases(
    mocker: MockerFixture,
    session: Session,
) -> None:
    """
    Test ``get_query_for_sql`` when no common databases are found.
    """
    get_session = mocker.patch("datajunction.engine.get_session")
    get_session().__next__.return_value = session

    A = Node(
        name="A",
        tables=[],
    )

    B = Node(
        name="B",
        expression="SELECT COUNT(*) AS cnt FROM A",
        parents=[A],
    )
    session.add(B)
    session.commit()

    sql = "SELECT B FROM metrics"
    with pytest.raises(Exception) as excinfo:
        get_query_for_sql(sql)
    assert str(excinfo.value) == "Unable to run SQL (no common database)"


def test_get_query_for_sql_alias(mocker: MockerFixture, session: Session) -> None:
    """
    Test ``get_query_for_sql`` with aliases.
    """
    get_session = mocker.patch("datajunction.engine.get_session")
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
    get_session = mocker.patch("datajunction.engine.get_session")
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
        expression="SELECT COUNT(*) FROM core.comments",
        parents=[comments],
    )
    session.add(num_comments)
    session.commit()

    sql = """
SELECT "core.num_comments" FROM metrics
WHERE "core.comments.user_id" > 1
GROUP BY "core.comments.user_id"
    """
    create_query = get_query_for_sql(sql)

    assert create_query.database_id == 1

    space = " "
    assert (
        create_query.submitted_query
        == f"""SELECT count('*') AS "core.num_comments"{space}
FROM (SELECT comments.user_id AS user_id, comments.comment AS comment{space}
FROM comments) AS "core.comments"{space}
WHERE "core.comments".user_id > 1 GROUP BY "core.comments".user_id"""
    )


def test_get_query_for_sql_invalid_column(
    mocker: MockerFixture,
    session: Session,
) -> None:
    """
    Test ``get_query_for_sql`` with an invalid column.
    """
    get_session = mocker.patch("datajunction.engine.get_session")
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
    assert str(excinfo.value) == "Invalid identifier: core.some_other_parent"
