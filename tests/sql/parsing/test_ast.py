"""
testing ast Nodes and their methods
"""
import pytest
from dj.sql.parsing.ast import (
    Alias,
    From,
    Query,
    Select,
    Table,
    Wildcard,
    Number,
    String,
    Boolean,
    Column,
)


def test_findall_trivial(trivial_query):
    """
    test find_all on a trivial query
    """
    assert [Table(name="a", quote_style=None)] == list(trivial_query.find_all(Table))


def test_filter_trivial(trivial_query):
    """
    test filtering nodes of a trivial query
    """
    assert [Table(name="a", quote_style=None)] == list(
        trivial_query.filter(lambda node: isinstance(node, Table)),
    )


def test_flatten_trivial(trivial_query):
    """
    test flattening on a trivial query
    """
    assert [
        Query(
            ctes=[],
            select=Select(
                distinct=False,
                from_=From(table=Table(name="a", quote_style=None), joins=[]),
                group_by=[],
                having=None,
                projection=[Wildcard()],
                where=None,
                limit=None,
            ),
            subquery=False,
        ),
        Select(
            distinct=False,
            from_=From(table=Table(name="a", quote_style=None), joins=[]),
            group_by=[],
            having=None,
            projection=[Wildcard()],
            where=None,
            limit=None,
        ),
        From(table=Table(name="a", quote_style=None), joins=[]),
        Table(name="a", quote_style=None),
        Wildcard(),
    ] == list(trivial_query.flatten())


def test_trivial_apply(trivial_query):
    """
    test the apply method for nodes on a trivial query
    """
    flat = []
    trivial_query.apply(lambda node: flat.append(node))  # pylint: disable=W0108
    assert flat == list(trivial_query.flatten())


def test_named_alias_or_name():
    """
    test a named node for returning it's alias name when a child of an alias
    """
    named = Table("a", None)
    alias = Alias(
        "alias",
        None,
        named,
    )
    named.add_parents(alias)
    assert named.alias_or_name() == "alias"


@pytest.mark.parametrize("value1, value2", list(zip(range(5), range(5, 10))))
def test_number_hash(value1, value2):
    assert hash(Number(value1)) == hash(Number(value1))
    assert hash(Number(value1)) != hash(Number(value2))
    assert hash(Number(value1)) != hash(String(str((value1))))


@pytest.mark.parametrize("value1, value2", [(True, False), (False, True)])
def test_boolean_hash(value1, value2):
    assert hash(Boolean(value1)) == hash(Boolean(value1))
    assert hash(Boolean(value1)) != hash(Boolean(value2))
    assert hash(Boolean(value1)) != hash(String(str((value1))))


def test_wildcard_table_reference():
    wildcard = Wildcard()
    wildcard.add_table(Table("a", None))
    assert wildcard.table == Table("a", None)
