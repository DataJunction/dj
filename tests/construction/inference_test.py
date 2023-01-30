"""test inferring types"""

# pylint: disable=W0621,C0325
import pytest
from sqlalchemy import select
from sqlmodel import Session

from dj.construction.inference import get_type_of_expression
from dj.models import Node
from dj.sql.parsing import ast
from dj.sql.parsing.ast import BinaryOpKind
from dj.sql.parsing.backends.exceptions import DJParseException
from dj.sql.parsing.backends.sqloxide import parse
from dj.typing import ColumnType


def test_infer_column_with_table(construction_session: Session):
    """
    Test getting the type of a column that has a table
    """
    node = next(
        construction_session.exec(
            select(Node).filter(Node.name == "dbt.source.jaffle_shop.orders"),
        ),
    )[0]
    table = ast.Table(ast.Name("orders"), _dj_node=node)
    assert (
        get_type_of_expression(ast.Column(ast.Name("id"), _table=table))
        == ColumnType.INT
    )
    assert (
        get_type_of_expression(ast.Column(ast.Name("user_id"), _table=table))
        == ColumnType.INT
    )
    assert (
        get_type_of_expression(ast.Column(ast.Name("order_date"), _table=table))
        == ColumnType.DATE
    )
    assert (
        get_type_of_expression(ast.Column(ast.Name("status"), _table=table))
        == ColumnType.STR
    )


def test_infer_values():
    """
    Test inferring types from values directly
    """
    assert get_type_of_expression(ast.String("foo")) == ColumnType.STR
    assert get_type_of_expression(ast.Number(value=1.1)) == ColumnType.FLOAT


def test_raise_on_invalid_infer_binary_op():
    """
    Test raising when trying to infer types from an invalid binary op
    """
    with pytest.raises(DJParseException) as exc_info:
        get_type_of_expression(
            ast.BinaryOp(
                op=BinaryOpKind.Modulo,
                left=ast.String(value="foo"),
                right=ast.String(value="bar"),
            ),
        )

    assert (
        "Incompatible types in binary operation 'foo' % 'bar'. "
        "Got left STR, right STR."
    ) in str(exc_info.value)


def test_infer_column_with_an_aliased_table(construction_session: Session):
    """
    Test getting the type of a column that has an aliased table
    """
    node = next(
        construction_session.exec(
            select(Node).filter(Node.name == "dbt.source.jaffle_shop.orders"),
        ),
    )[0]
    table = ast.Table(ast.Name("orders"), _dj_node=node)
    alias = ast.Alias(
        ast.Name("foo"),
        ast.Namespace([ast.Name("a"), ast.Name("b"), ast.Name("c")]),
        child=table,
    )
    col = ast.Column(ast.Name("status"), _table=alias)
    assert get_type_of_expression(col) == ColumnType.STR


def test_raising_when_table_has_no_dj_node():
    """
    Test raising when getting the type of a column that has a table with no DJ node
    """
    table = ast.Table(ast.Name("orders"))
    col = ast.Column(ast.Name("status"), _table=table)

    with pytest.raises(DJParseException) as exc_info:
        get_type_of_expression(col)

    assert (
        "Cannot resolve type of column orders.status. "
        "column's table does not have a DJ Node."
    ) in str(exc_info.value)


def test_raising_when_expression_parent_not_a_table():
    """
    Test raising when getting the type of a column thats parent is not a table
    """
    query = parse("select 1")
    col = ast.Column(
        ast.Name("status"),
        _table=query.select,
    )  # intentionally adding a non-table AST node

    with pytest.raises(DJParseException) as exc_info:
        get_type_of_expression(col)

    assert (
        "DJ does not currently traverse subqueries for type information. Consider extraction first."
    ) in str(exc_info.value)


def test_raising_when_select_has_multiple_expressions_in_projection():
    """
    Test raising when a select has more than one in projection
    """
    select = parse("select 1, 2").select

    with pytest.raises(DJParseException) as exc_info:
        get_type_of_expression(select)

    assert ("single expression in its projection") in str(exc_info.value)


def test_raising_when_between_different_types():
    """
    Test raising when a between has multiple types
    """
    select = parse("select 1 between 'hello' and TRUE").select

    with pytest.raises(DJParseException) as exc_info:
        get_type_of_expression(select)

    assert ("BETWEEN expects all elements to have the same type") in str(exc_info.value)


def test_raising_when_unop_bad_type():
    """
    Test raising when a unop gets a bad type
    """
    select = parse("select not 'hello'").select

    with pytest.raises(DJParseException) as exc_info:
        get_type_of_expression(select)

    assert ("Incompatible type in unary operation") in str(exc_info.value)


def test_raising_when_expression_has_no_parent():
    """
    Test raising when getting the type of a column that has no parent
    """
    col = ast.Column(ast.Name("status"), _table=None)

    with pytest.raises(DJParseException) as exc_info:
        get_type_of_expression(col)

    assert "Cannot resolve type of column status." in str(exc_info.value)


def test_infer_types_complicated(construction_session: Session):
    """
    Test inferring complicated types
    """
    query = parse(
        """
      SELECT id+1-2/3*5%6&10|8^5,
      DATE_TRUNC('day', '2014-03-10'),
      NOW(),
      Coalesce(NULL, 5),
      Coalesce(NULL),
      NULL,
      MAX(id) OVER
        (PARTITION BY first_name ORDER BY last_name)
        AS running_total,
      MAX(id) OVER
        (PARTITION BY first_name ORDER BY last_name)
        AS running_total,
      MIN(id) OVER
        (PARTITION BY first_name ORDER BY last_name)
        AS running_total,
      AVG(id) OVER
        (PARTITION BY first_name ORDER BY last_name)
        AS running_total,
      COUNT(id) OVER
        (PARTITION BY first_name ORDER BY last_name)
        AS running_total,
      SUM(id) OVER
        (PARTITION BY first_name ORDER BY last_name)
        AS running_total,
      NOT TRUE,
      10,
      id>5,
      id<5,
      id>=5,
      id<=5,
      id BETWEEN 4 AND 5,
      id IN (5, 5),
      id NOT IN (3, 4),
      id NOT IN (SELECT -5),
      first_name LIKE 'Ca%',
      id is null,
      (id=5)=TRUE,
      'hello world',
      first_name as fn,
      last_name<>'yoyo' and last_name='yoyo' or last_name='yoyo',
      last_name,
      bizarre,
      (select 5.0)
      FROM (
      SELECT id,
         first_name,
         last_name<>'yoyo' and last_name='yoyo' or last_name='yoyo' as bizarre,
         last_name
      FROM dbt.source.jaffle_shop.customers
        )
    """,
    )
    query.compile(construction_session)
    types = [
        ColumnType.INT,
        ColumnType.DATETIME,
        ColumnType.DATETIME,
        ColumnType.INT,
        ColumnType.NULL,
        ColumnType.NULL,
        ColumnType.INT,
        ColumnType.INT,
        ColumnType.INT,
        ColumnType.INT,
        ColumnType.INT,
        ColumnType.INT,
        ColumnType.BOOL,
        ColumnType.INT,
        ColumnType.BOOL,
        ColumnType.BOOL,
        ColumnType.BOOL,
        ColumnType.BOOL,
        ColumnType.BOOL,
        ColumnType.BOOL,
        ColumnType.BOOL,
        ColumnType.BOOL,
        ColumnType.BOOL,
        ColumnType.BOOL,
        ColumnType.BOOL,
        ColumnType.STR,
        ColumnType.STR,
        ColumnType.BOOL,
        ColumnType.STR,
        ColumnType.BOOL,
        ColumnType.FLOAT,
    ]
    assert types == [exp.type for exp in query.select.projection]
