import pytest

from dj.sql.parsing.ast import Query, Select, From, Table, Wildcard, Alias


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
        trivial_query.filter(lambda node: isinstance(node, Table))
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
    trivial_query.apply(lambda node: flat.append(node))
    assert flat == list(trivial_query.flatten())


def test_named_alias_or_name():
    named = Table("a", None)
    alias = Alias(
        "alias",
        None,
        named,
    )
    named.add_parents(alias)
    assert named.alias_or_name() == "alias"
