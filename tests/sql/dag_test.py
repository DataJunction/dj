"""
Tests for ``datajunction.sql.dag``.
"""

from datajunction.models.database import Database, Table
from datajunction.models.node import Node
from datajunction.models.query import Query  # pylint: disable=unused-import
from datajunction.sql.dag import get_computable_databases


def test_get_computable_databases() -> None:
    """
    Test ``get_computable_databases``.
    """
    database_1 = Database(id=1, name="shared", URI="sqlite://", cost=1.0)
    database_2 = Database(id=2, name="not shared", URI="sqlite://", cost=2.0)
    database_3 = Database(id=3, name="fast", URI="sqlite://", cost=0.1)

    parent_a = Node(
        name="A",
        tables=[
            Table(database=database_1, table="A"),
            Table(database=database_2, table="A"),
        ],
    )
    parent_b = Node(
        name="B",
        tables=[Table(database=database_1, table="B")],
    )
    child = Node(
        name="C",
        tables=[Table(database=database_3, table="C")],
        parents=[parent_a, parent_b],
    )

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
