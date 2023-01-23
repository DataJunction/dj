"""
Tests for compiling nodes
"""

# pylint: disable=too-many-lines
import pytest
from sqlmodel import Session

from dj.construction.compile import compile_node, compile_query
from dj.construction.exceptions import CompoundBuildException
from dj.construction.extract import (
    extract_dependencies_from_node,
    extract_dependencies_from_query,
)
from dj.construction.utils import make_name
from dj.errors import DJException
from dj.models import Column, Node
from dj.sql.parsing import ast
from dj.sql.parsing.backends.sqloxide import parse
from dj.typing import ColumnType


def test_get_table_node_is_none(construction_session: Session):
    """
    Test a nonexistent table node with compound exception ignore
    """
    query = parse(
        "select x from purchases",
        "hive",
    )
    CompoundBuildException().reset()
    CompoundBuildException().set_raise(False)
    compile_query(construction_session, query)
    assert "No node `purchases`" in str(CompoundBuildException().errors)
    CompoundBuildException().reset()


@pytest.mark.parametrize(
    "namespace,name,expected_make_name",
    [
        (ast.Namespace([ast.Name("a"), ast.Name("b"), ast.Name("c")]), "d", "a.b.c.d"),
        (ast.Namespace([ast.Name("a"), ast.Name("b")]), "node-name", "a.b.node-name"),
        (ast.Namespace([]), "node-[name]", "node-[name]"),
        (None, "node-[name]", "node-[name]"),
        (ast.Namespace([ast.Name("a"), ast.Name("b"), ast.Name("c")]), None, "a.b.c"),
        (
            ast.Namespace([ast.Name("a"), ast.Name("b"), ast.Name("c")]),
            "node&(name)",
            "a.b.c.node&(name)",
        ),
        (
            ast.Namespace([ast.Name("a"), ast.Name("b"), ast.Name("c")]),
            "+d",
            "a.b.c.+d",
        ),
        (
            ast.Namespace([ast.Name("a"), ast.Name("b"), ast.Name("c")]),
            "-d",
            "a.b.c.-d",
        ),
        (
            ast.Namespace([ast.Name("a"), ast.Name("b"), ast.Name("c")]),
            "~~d",
            "a.b.c.~~d",
        ),
    ],
)
def test_make_name(
    namespace: ast.Optional[ast.Namespace],
    name: str,
    expected_make_name: str,
):
    """
    Test making names from a namespace and a name
    """
    assert make_name(namespace, name) == expected_make_name


def test_raise_on_dangling_refs_in_extract_dependencies(construction_session: Session):
    """
    Test getting dependencies from a query that has dangling references
    """
    query = parse("select a, b, c from does_not_exist")
    with pytest.raises(DJException) as exc_info:
        extract_dependencies_from_query(session=construction_session, query=query)

    assert "Cannot extract dependencies from query" in str(exc_info.value)


def test_catching_dangling_refs_in_extract_dependencies(construction_session: Session):
    """
    Test getting dependencies from a query that has dangling references when set not to raise
    """
    query = parse("select a, b, c from does_not_exist")
    _, _, danglers = extract_dependencies_from_query(
        session=construction_session,
        query=query,
        raise_=False,
    )
    assert "does_not_exist" in danglers


def test_raising_on_extract_from_node_with_no_query(construction_session: Session):
    """
    Test getting dependencies from a query that has dangling references when set not to raise
    """
    node_foo = Node(
        name="foo",
        columns=[
            Column(name="ds", type=ColumnType.DATETIME),
            Column(name="user_id", type=ColumnType.INT),
            Column(name="foo", type=ColumnType.FLOAT),
        ],
    )
    with pytest.raises(DJException) as exc_info:
        extract_dependencies_from_node(
            session=construction_session,
            node=node_foo,
            raise_=False,
        )

    assert "Node has no query to extract from." in str(exc_info.value)


def test_raise_on_unnamed_subquery_in_implicit_join(construction_session: Session):
    """
    Test raising on an unnamed subquery in an implicit join
    """
    query = parse(
        "SELECT country FROM basic.transform.country_agg, "
        "(SELECT country FROM basic.transform.country_agg)",
    )
    with pytest.raises(DJException) as exc_info:
        compile_query(
            session=construction_session,
            query=query,
        )
    assert "You may only use an unnamed subquery alone for" in str(exc_info.value)


def test_raise_on_ambiguous_column(construction_session: Session):
    """
    Test raising on ambiguous column
    """
    query = parse(
        "SELECT country FROM basic.transform.country_agg a "
        "LEFT JOIN basic.dimension.countries b on a.country = b.country",
    )
    with pytest.raises(DJException) as exc_info:
        compile_query(
            session=construction_session,
            query=query,
        )
    assert "`country` appears in multiple references and so must be namespaced" in str(
        exc_info.value,
    )


def test_compile_node(construction_session: Session):
    """
    Test compiling a node
    """
    node_a = Node(name="A", query="SELECT country FROM basic.transform.country_agg")
    compile_node(session=construction_session, node=node_a)


def test_raise_on_compile_node_with_no_query(construction_session: Session):
    """
    Test raising when compiling a node that has no query
    """
    node_a = Node(name="A")

    with pytest.raises(DJException) as exc_info:
        compile_node(session=construction_session, node=node_a)

    assert "Cannot compile node `A` with no query" in str(exc_info.value)


def test_raise_on_unjoinable_automatic_dimension_groupby(construction_session: Session):
    """
    Test raising where a dimension node is automatically detected but unjoinable
    """
    node_a = Node(
        name="A",
        query=(
            "SELECT country FROM basic.transform.country_agg "
            "GROUP BY basic.dimension.countries.country"
        ),
    )

    with pytest.raises(DJException) as exc_info:
        compile_node(session=construction_session, node=node_a)

    assert "Dimension `basic.dimension.countries` is not joinable" in str(
        exc_info.value,
    )


def test_raise_on_having_without_a_groupby(construction_session: Session):
    """
    Test raising when using a having without a groupby
    """
    node_a = Node(
        name="A",
        query=(
            "SELECT country FROM basic.transform.country_agg " "HAVING country='US'"
        ),
    )

    with pytest.raises(DJException) as exc_info:
        compile_node(session=construction_session, node=node_a)

    assert "HAVING without a GROUP BY is not allowed" in str(exc_info.value)


def test_having(construction_session: Session):
    """
    Test using having
    """
    node_a = Node(
        name="A",
        query=(
            "SELECT order_date, status FROM dbt.source.jaffle_shop.orders "
            "GROUP BY dbt.dimension.customers.id "
            "HAVING dbt.dimension.customers.id=1"
        ),
    )
    compile_node(session=construction_session, node=node_a)
