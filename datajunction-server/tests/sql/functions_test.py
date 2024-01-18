# pylint: disable=line-too-long,too-many-lines
"""
Tests for ``datajunction_server.sql.functions``.
"""

import pytest
from sqlalchemy.orm import Session

import datajunction_server.sql.functions as F
import datajunction_server.sql.parsing.types as ct
from datajunction_server.errors import DJException, DJNotImplementedException
from datajunction_server.sql.functions import (
    Avg,
    Coalesce,
    Count,
    Max,
    Min,
    Now,
    Sum,
    ToDate,
    function_registry,
)
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.sql.parsing.backends.exceptions import DJParseException
from datajunction_server.sql.parsing.types import (
    BigIntType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    NullType,
    StringType,
    WildcardType,
)


def test_missing_functions() -> None:
    """
    Test missing functions.
    """
    with pytest.raises(DJNotImplementedException) as excinfo:
        function_registry["INVALID_FUNCTION"]  # pylint: disable=pointless-statement
    assert (
        str(excinfo.value) == "The function `INVALID_FUNCTION` hasn't been implemented "
        "in DJ yet. You can file an issue at https://github.com/"
        "DataJunction/dj/issues/new?title=Function+missing:+"
        "INVALID_FUNCTION to request it to be added, or use the "
        "documentation at https://github.com/DataJunction/dj/blob"
        "/main/docs/functions.rst to implement it."
    )


def test_bad_combo_types() -> None:
    """
    Tests dispatch raises on bad types
    """
    with pytest.raises(TypeError) as exc:
        Avg.infer_type(ast.Column(ast.Name("x"), _type=StringType()))
    assert "got an invalid combination of types" in str(exc)


def test_abs(session: Session):
    """
    Test the `abs` Spark function
    """
    query = parse(
        """
    select abs(-1)
    """,
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore

    query = parse(
        """
    select abs(-1.1)
    """,
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.FloatType()  # type: ignore


def test_aggregate(session: Session):
    """
    Test the `aggregate` Spark function
    """
    query = parse(
        """
    select
      aggregate(items, '', (acc, x) -> (case
        when acc = '' then element_at(split(x, '::'), 1)
        when acc = 'a' then acc
        else element_at(split(x, '::'), 1) end)) as item
    from (
      select 1 as id, ARRAY('b', 'c', 'a', 'x', 'g', 'z') AS items
    )
    """,
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert query.select.projection[0].type == StringType()  # type: ignore


def test_approx_percentile(session: Session):
    """
    Test the `approx_percentile` Spark function
    """
    query_with_list = parse("SELECT approx_percentile(10.0, array(0.5, 0.4, 0.1), 100)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query_with_list.compile(ctx)
    assert not exc.errors
    assert query_with_list.select.projection[0].type == ct.ListType(element_type=ct.FloatType())  # type: ignore

    query_with_list = parse("SELECT approx_percentile(10.0, 0.5, 100)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query_with_list.compile(ctx)
    assert not exc.errors
    assert query_with_list.select.projection[0].type == ct.FloatType()  # type: ignore


def test_array(session: Session):
    """
    Test the `array` Spark function
    """
    query = parse(
        """
    SELECT array() FROM (select 1 as col)
    """,
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.ListType(element_type=ct.NullType())  # type: ignore

    query = parse(
        """
    SELECT array(1, 2, 3) FROM (select 1 as col)
    """,
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.ListType(element_type=ct.IntegerType())  # type: ignore


def test_array_agg(session: Session):
    """
    Test the `array_agg` Spark function
    """
    query = parse(
        """
    SELECT array_agg(col) FROM (select 1 as col)
    """,
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.ListType(element_type=ct.IntegerType())  # type: ignore

    query = parse(
        """
    SELECT array_agg(col) FROM (select 'foo' as col)
    """,
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.ListType(element_type=ct.StringType())  # type: ignore


def test_array_append(session: Session):
    """
    Test the `array_append` Spark function
    """
    query = parse("SELECT array_append(array('b', 'd', 'c', 'a'), 'd')")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.ListType(element_type=ct.StringType())  # type: ignore

    query = parse("SELECT array_append(array(1, 2, 3, 4), 5)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.ListType(element_type=ct.IntegerType())  # type: ignore

    query = parse("SELECT array_append(array(true, false, true, true), false)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.ListType(element_type=ct.BooleanType())  # type: ignore


def test_array_compact(session: Session):
    """
    Test the `array_compact` Spark function
    """
    query = parse(
        'SELECT array_compact(array(1, 2, 3, null)), array_compact(array("a", "b", "c"))',
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.ListType(element_type=ct.IntegerType())  # type: ignore
    assert query.select.projection[1].type == ct.ListType(element_type=ct.StringType())  # type: ignore


def test_array_contains(session: Session):
    """
    Test the `array_contains` Spark function
    """
    query = parse("select array_contains(array(1, 2, 3), 2)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.BooleanType()  # type: ignore


def test_array_distinct(session: Session):
    """
    Test the `array_distinct` Spark function
    """
    query = parse(
        """
        SELECT array_distinct(array(1, 2, 3, 3))
        """,
    )
    ctx = ast.CompileContext(session=session, exception=DJException())
    query.compile(ctx)
    assert query.select.projection[0].type == ct.ListType(element_type=ct.IntegerType())  # type: ignore

    query = parse(
        """
        SELECT array_distinct(array('a', 'b', 'b', 'z'))
        """,
    )
    ctx = ast.CompileContext(session=session, exception=DJException())
    query.compile(ctx)
    assert query.select.projection[0].type == ct.ListType(element_type=ct.StringType())  # type: ignore


def test_array_except(session: Session):
    """
    Test the `array_except` Spark function
    """
    query = parse(
        """
        SELECT array_except(array(1, 2, 3), array(1, 3, 5))
        """,
    )
    ctx = ast.CompileContext(session=session, exception=DJException())
    query.compile(ctx)
    assert query.select.projection[0].type == ct.ListType(element_type=ct.IntegerType())  # type: ignore

    query = parse(
        """
        SELECT array_except(array('a', 'b', 'b', 'z'), array('a', 'b'))
        """,
    )
    ctx = ast.CompileContext(session=session, exception=DJException())
    query.compile(ctx)
    assert query.select.projection[0].type == ct.ListType(element_type=ct.StringType())  # type: ignore


def test_array_intersect(session: Session):
    """
    Test the `array_intersect` Spark function
    """
    query = parse(
        """
        SELECT array_intersect(array(1, 2, 3), array(1, 3, 5))
        """,
    )
    ctx = ast.CompileContext(session=session, exception=DJException())
    query.compile(ctx)
    assert query.select.projection[0].type == ct.ListType(element_type=ct.IntegerType())  # type: ignore

    query = parse(
        """
        SELECT array_intersect(array('a', 'b', 'b', 'z'), array('a', 'b'))
        """,
    )
    ctx = ast.CompileContext(session=session, exception=DJException())
    query.compile(ctx)
    assert query.select.projection[0].type == ct.ListType(element_type=ct.StringType())  # type: ignore


def test_array_join(session: Session):
    """
    Test the `array_join` Spark function
    """
    query = parse(
        """
        SELECT array_join(array('hello', 'world'), ' ')
        """,
    )
    ctx = ast.CompileContext(session=session, exception=DJException())
    query.compile(ctx)
    assert query.select.projection[0].type == ct.StringType()  # type: ignore

    query = parse(
        """
        SELECT array_join(array('hello', null ,'world'), ' ', ',')
        """,
    )
    ctx = ast.CompileContext(session=session, exception=DJException())
    query.compile(ctx)
    assert query.select.projection[0].type == ct.StringType()  # type: ignore


def test_array_max(session: Session):
    """
    Test the `array_max` Spark function
    """
    query = parse(
        """
        SELECT array_max(array(1, 20, null, 3))
        """,
    )
    ctx = ast.CompileContext(session=session, exception=DJException())
    query.compile(ctx)
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore


def test_array_min(session: Session):
    """
    Test the `array_min` Spark function
    """
    query = parse(
        """
        SELECT array_min(array(1.0, 202.2, null, 3.333))
        """,
    )
    ctx = ast.CompileContext(session=session, exception=DJException())
    query.compile(ctx)
    assert query.select.projection[0].type == ct.FloatType()  # type: ignore


def test_array_position(session: Session):
    """
    Test the `array_position` function
    """
    query = parse(
        """
        SELECT array_position(array(1.0, 202.2, null, 3.333), 1.0)
        """,
    )
    ctx = ast.CompileContext(session=session, exception=DJException())
    query.compile(ctx)
    assert query.select.projection[0].type == ct.LongType()  # type: ignore


def test_array_remove(session: Session):
    """
    Test the `array_remove` function
    """
    query = parse(
        """
        SELECT array_remove(array(1.0, 202.2, null, 3.333), 1.0)
        """,
    )
    ctx = ast.CompileContext(session=session, exception=DJException())
    query.compile(ctx)
    assert query.select.projection[0].type == ct.ListType(element_type=ct.FloatType())  # type: ignore


def test_array_repeat(session: Session):
    """
    Test the `array_repeat` function
    """
    query = parse(
        """
        SELECT array_repeat('abc', 10), array_repeat(100, 10), array_repeat(1.23, 10)
        """,
    )
    ctx = ast.CompileContext(session=session, exception=DJException())
    query.compile(ctx)
    assert query.select.projection[0].type == ct.ListType(element_type=ct.StringType())  # type: ignore
    assert query.select.projection[1].type == ct.ListType(element_type=ct.IntegerType())  # type: ignore
    assert query.select.projection[2].type == ct.ListType(element_type=ct.FloatType())  # type: ignore


def test_array_size(session: Session):
    """
    Test the `array_size` function
    """
    query = parse(
        """
        SELECT array_size(array('abc', 'd', 'e', 'f'))
        """,
    )
    ctx = ast.CompileContext(session=session, exception=DJException())
    query.compile(ctx)
    assert query.select.projection[0].type == ct.LongType()  # type: ignore


def test_array_sort(session: Session):
    """
    Test the `array_sort` function
    """
    query = parse(
        """
        SELECT
          array_sort(array('b', 'd', null, 'c', 'a'))
        """,
    )
    ctx = ast.CompileContext(session=session, exception=DJException())
    query.compile(ctx)
    assert query.select.projection[0].type == ct.ListType(  # type: ignore
        element_type=ct.StringType(),
    )


def test_array_union(session: Session):
    """
    Test the `array_union` function
    """
    query = parse(
        """
        SELECT array_union(array('b', 'd', null), array('c', 'a'))
        """,
    )
    ctx = ast.CompileContext(session=session, exception=DJException())
    query.compile(ctx)
    assert query.select.projection[0].type == ct.ListType(  # type: ignore
        element_type=ct.StringType(),
    )


def test_array_overlap(session: Session):
    """
    Test the `array_overlap` function
    """
    query = parse(
        """
        SELECT arrays_overlap(array(1, 2, 3), array(3, 4, 5))
        """,
    )
    ctx = ast.CompileContext(session=session, exception=DJException())
    query.compile(ctx)
    assert query.select.projection[0].type == ct.BooleanType()  # type: ignore


def test_avg() -> None:
    """
    Test ``avg`` function.
    """
    assert (
        Avg.infer_type(ast.Column(ast.Name("x"), _type=IntegerType())) == DoubleType()
    )
    assert Avg.infer_type(ast.Column(ast.Name("x"), _type=FloatType())) == DoubleType()


def test_cardinality(session: Session):
    """
    Test the `cardinality` Spark function
    """
    query_with_list = parse("SELECT cardinality(array('b', 'd', 'c', 'a'))")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query_with_list.compile(ctx)
    assert not exc.errors
    assert query_with_list.select.projection[0].type == ct.IntegerType()  # type: ignore

    query_with_map = parse("SELECT cardinality(map('a', 1, 'b', 2))")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query_with_map.compile(ctx)
    assert not exc.errors
    assert query_with_map.select.projection[0].type == ct.IntegerType()  # type: ignore


def test_cbrt_func(session: Session):
    """
    Test the `cbrt` function
    """
    query = parse("SELECT cbrt(27), cbrt(64.0)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == FloatType()  # type: ignore
    assert query.select.projection[1].type == FloatType()  # type: ignore


def test_char_func(session: Session):
    """
    Test the `char` function
    """
    query = parse("SELECT char(65), char(97)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == StringType()  # type: ignore
    assert query.select.projection[1].type == StringType()  # type: ignore


def test_char_length_func(session: Session):
    """
    Test the `char_length` function
    """
    query = parse("SELECT char_length('hello'), char_length('world')")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == IntegerType()  # type: ignore
    assert query.select.projection[1].type == IntegerType()  # type: ignore


def test_character_length_func(session: Session):
    """
    Test the `character_length` function
    """
    query = parse("SELECT character_length('hello'), character_length('world')")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == IntegerType()  # type: ignore
    assert query.select.projection[1].type == IntegerType()  # type: ignore


def test_chr_func(session: Session):
    """
    Test the `chr` function
    """
    query = parse("SELECT chr(65), chr(97)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == StringType()  # type: ignore
    assert query.select.projection[1].type == StringType()  # type: ignore


@pytest.mark.parametrize(
    "types, expected",
    [
        ((ct.IntegerType(),), ct.BigIntType()),
        ((ct.FloatType(),), ct.BigIntType()),
        ((ct.DoubleType(),), ct.BigIntType()),
        ((ct.TinyIntType(), ct.IntegerType()), ct.DecimalType(precision=3, scale=0)),
        ((ct.SmallIntType(), ct.IntegerType()), ct.DecimalType(precision=5, scale=0)),
        ((ct.IntegerType(), ct.IntegerType()), ct.DecimalType(precision=10, scale=0)),
        ((ct.BigIntType(), ct.IntegerType()), ct.DecimalType(precision=20, scale=0)),
        ((ct.DoubleType(), ct.IntegerType()), ct.DecimalType(precision=30, scale=0)),
        ((ct.FloatType(), ct.IntegerType()), ct.DecimalType(precision=14, scale=0)),
        (
            (ct.DecimalType(10, 2), ct.IntegerType()),
            ct.DecimalType(precision=9, scale=0),
        ),
        (
            (ct.DecimalType(precision=9, scale=0),),
            ct.DecimalType(precision=10, scale=0),
        ),
    ],
)
def test_ceil(types, expected) -> None:
    """
    Test ``ceil`` function.
    """
    if len(types) == 1:
        assert F.Ceil.infer_type(ast.Column(ast.Name("x"), _type=types[0])) == expected
    else:
        assert (
            F.Ceil.infer_type(
                *(
                    ast.Column(ast.Name("x"), _type=types[0]),
                    ast.Number(0, _type=types[1]),
                )
            )
            == expected
        )


def test_ceil_ceiling_funcs(session: Session):
    """
    Test the `ceil` and `ceiling` functions
    """
    query = parse(
        "SELECT ceil(-0.1), ceil(5), ceil(3.1411, 3), ceil(3.1411, -3), "
        "ceiling(-0.1), ceiling(5), ceiling(3.1411, 3), ceiling(3.1411, -3)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == BigIntType()  # type: ignore
    assert query.select.projection[1].type == BigIntType()  # type: ignore
    assert query.select.projection[2].type == DecimalType(precision=14, scale=3)  # type: ignore
    assert query.select.projection[3].type == DecimalType(precision=14, scale=0)  # type: ignore
    assert query.select.projection[4].type == BigIntType()  # type: ignore
    assert query.select.projection[5].type == BigIntType()  # type: ignore
    assert query.select.projection[6].type == DecimalType(precision=14, scale=3)  # type: ignore
    assert query.select.projection[7].type == DecimalType(precision=14, scale=0)  # type: ignore


def test_coalesce_infer_type() -> None:
    """
    Test type inference in the ``Coalesce`` function.
    """
    assert (
        Coalesce.infer_type(
            ast.Column(ast.Name("x"), _type=StringType()),
            ast.Column(ast.Name("x"), _type=StringType()),
            ast.Column(ast.Name("x"), _type=StringType()),
        )
        == StringType()
    )

    assert (
        Coalesce.infer_type(
            ast.Column(ast.Name("x"), _type=IntegerType()),
            ast.Column(ast.Name("x"), _type=NullType()),
            ast.Column(ast.Name("x"), _type=BigIntType()),
        )
        == IntegerType()
    )

    assert (
        Coalesce.infer_type(
            ast.Column(ast.Name("x"), _type=StringType()),
            ast.Column(ast.Name("x"), _type=StringType()),
            ast.Column(ast.Name("x"), _type=NullType()),
        )
        == StringType()
    )


def test_concat_func(session: Session):
    """
    Test the `concat` function
    """
    query = parse(
        "SELECT concat('hello', '+', 'world'), concat(array(1, 2), array(3))",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.StringType()  # type: ignore
    assert query.select.projection[1].type == ct.ListType(ct.IntegerType())  # type: ignore


def test_concat_ws_func(session: Session):
    """
    Test the `concat_ws` function
    """
    query = parse(
        "SELECT concat_ws(',', 'hello', 'world'), concat_ws('-', 'spark', 'sql', 'function')",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == StringType()  # type: ignore
    assert query.select.projection[1].type == StringType()  # type: ignore


def test_collect_list(session: Session):
    """
    Test the `collect_list` function
    """
    query = parse("SELECT collect_list(col) FROM (SELECT (1), (2) AS col)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.ListType(  # type: ignore
        element_type=ct.IntegerType(),
    )


def test_collect_set(session: Session):
    """
    Test the `collect_set` function
    """
    query = parse("SELECT collect_set(col) FROM (SELECT (1), (2) AS col)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.ListType(  # type: ignore
        element_type=ct.IntegerType(),
    )


def test_contains_func(session: Session):
    """
    Test the `contains` function
    """
    query = parse(
        "SELECT contains('hello world', 'world'), contains('hello world', 'spark')",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.BooleanType()  # type: ignore
    assert query.select.projection[1].type == ct.BooleanType()  # type: ignore


def test_conv_func(session: Session):
    """
    Test the `conv` function
    """
    query = parse("SELECT conv('10', 10, 2), conv(15, 10, 16)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.StringType()  # type: ignore
    assert query.select.projection[1].type == ct.StringType()  # type: ignore


def test_convert_timezone_func(session: Session):
    """
    Test the `convert_timezone` function
    """
    query = parse(
        "SELECT convert_timezone('PST', 'EST', cast('2023-07-30 12:34:56' as timestamp))",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.TimestampType()  # type: ignore


def test_corr_func(session: Session):
    """
    Test the `corr` function
    """
    query = parse("SELECT corr(2.0, 3.0), corr(5, 10)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.FloatType()  # type: ignore
    assert query.select.projection[1].type == ct.FloatType()  # type: ignore


def test_cos_func(session: Session):
    """
    Test the `cos` function
    """
    query = parse("SELECT cos(0), cos(3.1416)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.FloatType()  # type: ignore
    assert query.select.projection[1].type == ct.FloatType()  # type: ignore


def test_cosh_func(session: Session):
    """
    Test the `cosh` function
    """
    query = parse("SELECT cosh(0), cosh(1.0)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.FloatType()  # type: ignore
    assert query.select.projection[1].type == ct.FloatType()  # type: ignore


def test_cot_func(session: Session):
    """
    Test the `cot` function
    """
    query = parse("SELECT cot(1), cot(0.7854)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.FloatType()  # type: ignore
    assert query.select.projection[1].type == ct.FloatType()  # type: ignore


def test_count() -> None:
    """
    Test ``Count`` function.
    """
    assert (
        Count.infer_type(ast.Column(ast.Name("x"), _type=WildcardType()))
        == BigIntType()
    )
    assert Count.is_aggregation is True


def test_count_if_func(session: Session):
    """
    Test the `count_if` function
    """
    query = parse("SELECT count_if(true), count_if(false)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore
    assert query.select.projection[1].type == ct.IntegerType()  # type: ignore


def test_count_min_sketch(session: Session):
    """
    Test the `count_min_sketch` function
    """
    query = parse(
        "SELECT count_min_sketch(col, 0.5, 0.5, 1) FROM (SELECT (1), (2), (1) AS col)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.BinaryType()  # type: ignore


def test_covar_pop_func(session: Session):
    """
    Test the `covar_pop` function
    """
    query = parse("SELECT covar_pop(1.0, 2.0), covar_pop(3, 4)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.FloatType()  # type: ignore
    assert query.select.projection[1].type == ct.FloatType()  # type: ignore


def test_covar_samp_func(session: Session):
    """
    Test the `covar_samp` function
    """
    query = parse("SELECT covar_samp(1.0, 2.0), covar_samp(3, 4)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.FloatType()  # type: ignore
    assert query.select.projection[1].type == ct.FloatType()  # type: ignore


def test_crc32_func(session: Session):
    """
    Test the `crc32` function
    """
    query = parse("SELECT crc32('hello'), crc32('world')")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.BigIntType()  # type: ignore
    assert query.select.projection[1].type == ct.BigIntType()  # type: ignore


def test_csc_func(session: Session):
    """
    Test the `csc` function
    """
    query = parse("SELECT csc(1), csc(0.7854)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.FloatType()  # type: ignore
    assert query.select.projection[1].type == ct.FloatType()  # type: ignore


def test_cume_dist_func(session: Session):
    """
    Test the `cume_dist` function
    """
    query = parse("SELECT cume_dist(), cume_dist()")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.FloatType()  # type: ignore
    assert query.select.projection[1].type == ct.FloatType()  # type: ignore


def test_curdate_func(session: Session):
    """
    Test the `curdate` function
    """
    query = parse("SELECT curdate(), curdate()")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.DateType()  # type: ignore
    assert query.select.projection[1].type == ct.DateType()  # type: ignore


def test_current_catalog_func(session: Session):
    """
    Test the `current_catalog` function
    """
    query = parse("SELECT current_catalog(), current_catalog()")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.StringType()  # type: ignore
    assert query.select.projection[1].type == ct.StringType()  # type: ignore


def test_current_database_func(session: Session):
    """
    Test the `current_database` function
    """
    query = parse("SELECT current_database(), current_database()")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.StringType()  # type: ignore
    assert query.select.projection[1].type == ct.StringType()  # type: ignore


def test_current_schema_func(session: Session):
    """
    Test the `current_schema` function
    """
    query = parse("SELECT current_schema(), current_schema()")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.StringType()  # type: ignore
    assert query.select.projection[1].type == ct.StringType()  # type: ignore


def test_current_timezone_current_user_funcs(session: Session):
    """
    Test the `current_timezone` function
    Test the `current_user` function
    """
    query = parse("SELECT current_timezone(), current_user()")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.StringType()  # type: ignore
    assert query.select.projection[1].type == ct.StringType()  # type: ignore


def test_date_func(session: Session):
    """
    Test the `date` function
    """
    query = parse(
        "SELECT date('2023-07-30'), date(cast('2023-07-30 12:34:56' as timestamp))",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.DateType()  # type: ignore
    assert query.select.projection[1].type == ct.DateType()  # type: ignore


def test_date_from_unix_date_func(session: Session):
    """
    Test the `date_from_unix_date` function
    """
    query = parse("SELECT date_from_unix_date(0), date_from_unix_date(18500)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.DateType()  # type: ignore
    assert query.select.projection[1].type == ct.DateType()  # type: ignore


def test_date_format(session: Session) -> None:
    """
    Test ``date_format`` function.
    """
    query_with_array = parse("SELECT date_format(NOW(), 'yyyyMMdd') as date_partition")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query_with_array.compile(ctx)
    assert not exc.errors
    assert query_with_array.select.projection[0].type == StringType()  # type: ignore


def test_date_part_func(session: Session):
    """
    Test the `date_part` function
    """
    query = parse(
        "SELECT date_part('year', cast('2023-07-30' as date)), date_part('hour', cast('2023-07-30 12:34:56' as timestamp))",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore
    assert query.select.projection[1].type == ct.IntegerType()  # type: ignore


def test_dj_logical_timestamp(session: Session) -> None:
    """
    Test ``DJ_LOGICAL_TIMESTAMP`` function.
    """
    query_with_array = parse("SELECT dj_logical_timestamp() as date_partition")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query_with_array.compile(ctx)
    assert not exc.errors
    assert query_with_array.select.projection[0].type == StringType()  # type: ignore


def test_dayofmonth_func(session: Session):
    """
    Test the `dayofmonth` function
    """
    query = parse(
        "SELECT dayofmonth(cast('2023-07-30' as date)), dayofmonth('2023-01-01')",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore
    assert query.select.projection[1].type == ct.IntegerType()  # type: ignore


def test_dayofweek_func(session: Session):
    """
    Test the `dayofweek` function
    """
    query = parse(
        "SELECT dayofweek(cast('2023-07-30' as date)), dayofweek('2023-01-01')",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore
    assert query.select.projection[1].type == ct.IntegerType()  # type: ignore


def test_dayofyear_func(session: Session):
    """
    Test the `dayofyear` function
    """
    query = parse(
        "SELECT dayofyear(cast('2023-07-30' as date)), dayofyear('2023-01-01')",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore
    assert query.select.projection[1].type == ct.IntegerType()  # type: ignore


def test_decimal_func(session: Session):
    """
    Test the `decimal` function
    """
    query = parse("SELECT decimal(123), decimal('456.78')")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.DecimalType(8, 6)  # type: ignore
    assert query.select.projection[1].type == ct.DecimalType(8, 6)  # type: ignore


def test_decode_func(session: Session):
    """
    Test the `decode` function
    """
    query = parse("SELECT decode(unhex('4D7953514C'), 'UTF-8')")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.StringType()  # type: ignore


def test_degrees_func(session: Session):
    """
    Test the `degrees` function
    """
    query = parse("SELECT degrees(1), degrees(3.141592653589793)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.FloatType()  # type: ignore
    assert query.select.projection[1].type == ct.FloatType()  # type: ignore


def test_double_func(session: Session):
    """
    Test the `double` function
    """
    query = parse("SELECT double('123.45'), double(67890)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.DoubleType()  # type: ignore
    assert query.select.projection[1].type == ct.DoubleType()  # type: ignore


def test_e_func(session: Session):
    """
    Test the `e` function
    """
    query = parse("SELECT e(), e()")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.FloatType()  # type: ignore
    assert query.select.projection[1].type == ct.FloatType()  # type: ignore


def test_element_at(session: Session):
    """
    Test the `element_at` Spark function
    """
    query_with_array = parse("SELECT element_at(array(1, 2, 3, 4), 2)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query_with_array.compile(ctx)
    assert not exc.errors
    assert query_with_array.select.projection[0].type == IntegerType()  # type: ignore

    query_with_map = parse("SELECT element_at(map(1, 'a', 2, 'b'), 2)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query_with_map.compile(ctx)
    assert not exc.errors
    assert query_with_map.select.projection[0].type == StringType()  # type: ignore


def test_elt_func(session: Session):
    """
    Test the `elt` function
    """
    query = parse("SELECT elt(1, 'a', 'b', 'c'), elt(3, 'd', 'e', 'f')")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.StringType()  # type: ignore
    assert query.select.projection[1].type == ct.StringType()  # type: ignore


def test_encode_func(session: Session):
    """
    Test the `encode` function
    """
    query = parse("SELECT encode('hello', 'UTF-8'), encode('world', 'UTF-8')")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.StringType()  # type: ignore
    assert query.select.projection[1].type == ct.StringType()  # type: ignore


def test_endswith_func(session: Session):
    """
    Test the `endswith` function
    """
    query = parse("SELECT endswith('hello', 'lo'), endswith('world', 'ld')")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.BooleanType()  # type: ignore
    assert query.select.projection[1].type == ct.BooleanType()  # type: ignore


def test_equal_null_func(session: Session):
    """
    Test the `equal_null` function
    """
    query = parse("SELECT equal_null('hello', 'hello'), equal_null(NULL, NULL)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.BooleanType()  # type: ignore
    assert query.select.projection[1].type == ct.BooleanType()  # type: ignore


def test_every_func(session: Session):
    """
    Test the `every` function
    """
    query = parse("SELECT every(col), every(col) FROM (SELECT (true), (false) AS col)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.BooleanType()  # type: ignore
    assert query.select.projection[1].type == ct.BooleanType()  # type: ignore


def test_exists_func(session: Session):
    """
    Test the `exists` function
    """
    query = parse("SELECT exists(array(1, 2, 3), x -> x % 2 > 0)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert query.select.projection[0].type == ct.BooleanType()  # type: ignore


def test_explode_outer_func(session: Session):
    """
    Test the `explode_outer` function
    """
    query = parse(
        "SELECT explode_outer(array(1, 2, 3)), explode_outer(map('key1', 'value1', 'key2', 'value2'))",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore
    assert query.select.projection[1].type == ct.StringType()  # type: ignore


def test_expm1_func(session: Session):
    """
    Test the `expm1` function
    """
    query = parse("SELECT expm1(1), expm1(0.5)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.FloatType()  # type: ignore
    assert query.select.projection[1].type == ct.FloatType()  # type: ignore


def test_factorial_func(session: Session):
    """
    Test the `factorial` function
    """
    query = parse("SELECT factorial(0), factorial(5)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore
    assert query.select.projection[1].type == ct.IntegerType()  # type: ignore


def test_filter(session: Session):
    """
    Test the `filter` function
    """
    query = parse("SELECT filter(col, s -> s != 3) FROM (SELECT array(1, 2, 3) AS col)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert query.select.projection[0].type == ct.ListType(  # type: ignore
        element_type=ct.IntegerType(),
    )

    query = parse(
        "SELECT filter(col, (s, i) -> s + i != 3) FROM (SELECT array(1, 2, 3) AS col)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert query.select.projection[0].type == ct.ListType(  # type: ignore
        element_type=ct.IntegerType(),
    )

    with pytest.raises(DJParseException):
        query = parse(
            "SELECT filter(col, (s, i, a) -> s + i != 3) FROM (SELECT array(1, 2, 3) AS col)",
        )
        exc = DJException()
        ctx = ast.CompileContext(session=session, exception=exc)
        query.compile(ctx)


def test_find_in_set_func(session: Session):
    """
    Test the `find_in_set` function
    """
    query = parse("SELECT find_in_set('b', 'a,b,c,d'), find_in_set('e', 'a,b,c,d')")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore
    assert query.select.projection[1].type == ct.IntegerType()  # type: ignore


def test_first_and_first_value(session: Session):
    """
    Test `first` and `first_value`
    """
    query = parse(
        "SELECT first(col), first(col, true), first_value(col), "
        "first_value(col, true) FROM (SELECT (1), (2) AS col)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore
    assert query.select.projection[1].type == ct.IntegerType()  # type: ignore
    assert query.select.projection[2].type == ct.IntegerType()  # type: ignore
    assert query.select.projection[3].type == ct.IntegerType()  # type: ignore


def test_flatten(session: Session):
    """
    Test `flatten`
    """
    query = parse("SELECT flatten(array(array(1, 2), array(3, 4)))")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.ListType(  # type: ignore
        element_type=ct.IntegerType(),
    )


def test_float_func(session: Session):
    """
    Test the `float` function
    """
    query = parse("SELECT float(123), float('456.78')")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.FloatType()  # type: ignore
    assert query.select.projection[1].type == ct.FloatType()  # type: ignore


@pytest.mark.parametrize(
    "types, expected",
    [
        ((ct.IntegerType(),), ct.BigIntType()),
        ((ct.FloatType(),), ct.BigIntType()),
        ((ct.DoubleType(),), ct.BigIntType()),
        ((ct.TinyIntType(), ct.IntegerType()), ct.DecimalType(precision=3, scale=0)),
        ((ct.SmallIntType(), ct.IntegerType()), ct.DecimalType(precision=5, scale=0)),
        ((ct.IntegerType(), ct.IntegerType()), ct.DecimalType(precision=10, scale=0)),
        ((ct.BigIntType(), ct.IntegerType()), ct.DecimalType(precision=20, scale=0)),
        ((ct.DoubleType(), ct.IntegerType()), ct.DecimalType(precision=30, scale=0)),
        ((ct.FloatType(), ct.IntegerType()), ct.DecimalType(precision=14, scale=0)),
        (
            (ct.DecimalType(10, 2), ct.IntegerType()),
            ct.DecimalType(precision=9, scale=0),
        ),
        (
            (ct.DecimalType(precision=9, scale=0),),
            ct.DecimalType(precision=10, scale=0),
        ),
    ],
)
def test_floor(types, expected) -> None:
    """
    Test ``floor`` function.
    """
    if len(types) == 1:
        assert F.Floor.infer_type(ast.Column(ast.Name("x"), _type=types[0])) == expected
    else:
        assert (
            F.Floor.infer_type(
                *(
                    ast.Column(ast.Name("x"), _type=types[0]),
                    ast.Number(0, _type=types[1]),
                )
            )
            == expected
        )


def test_forall_func(session: Session):
    """
    Test the `forall` function
    """
    query = parse(
        "SELECT forall(array(1, 2, 3), x -> x > 0), forall(array(1, 2, 3), x -> x < 0)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    # assert not exc.errors
    assert query.select.projection[0].type == ct.BooleanType()  # type: ignore
    assert query.select.projection[1].type == ct.BooleanType()  # type: ignore


def test_format_number_func(session: Session):
    """
    Test the `format_number` function
    """
    query = parse(
        "SELECT format_number(12345.6789, 2), format_number(98765.4321, '###.##')",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.StringType()  # type: ignore
    assert query.select.projection[1].type == ct.StringType()  # type: ignore


def test_format_string_func(session: Session):
    """
    Test the `format_string` function
    """
    query = parse(
        "SELECT format_string('%s %s', 'hello', 'world'), format_string('%d %d', 1, 2)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.StringType()  # type: ignore
    assert query.select.projection[1].type == ct.StringType()  # type: ignore


# TODO: Fix these two  # pylint: disable=fixme
# def test_from_csv_func(session: Session):
#     """
#     Test the `from_csv` function
#     """
#     query = parse("SELECT from_csv('1,2,3', 'a INT, b INT, c INT'), from_csv('4,5,6', 'x INT, y INT, z INT')")
#     exc = DJException()
#     ctx = ast.CompileContext(session=session, exception=exc)
#     query.compile(ctx)
#     assert not exc.errors
#     assert isinstance(query.select.projection[0].type, ct.StructType)  # type: ignore
#     assert isinstance(query.select.projection[1].type, ct.StructType)  # type: ignore


# TODO: Fix these two  # pylint: disable=fixme
# def test_from_json_func(session: Session):
#     """
#     Test the `from_json` function
#     """
#     query = parse("SELECT from_json('1,2,3', 'a INT, b INT, c INT'), from_json('4,5,6', 'x INT, y INT, z INT')")
#     exc = DJException()
#     ctx = ast.CompileContext(session=session, exception=exc)
#     query.compile(ctx)
#     assert not exc.errors
#     assert isinstance(query.select.projection[0].type, Union[ct.StructType, ct.ListType])  # type: ignore
#     assert isinstance(query.select.projection[1].type, Union[ct.StructType, ct.ListType])


def test_from_unix_time_func(session: Session):
    """
    Test the `from_unix_time` function
    """
    query = parse(
        "SELECT from_unixtime(1609459200, 'yyyy-MM-dd HH:mm:ss'), from_unixtime(1609459200, 'dd/MM/yyyy HH:mm:ss')",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.StringType()  # type: ignore
    assert query.select.projection[1].type == ct.StringType()  # type: ignore


def test_from_utc_timestamp_func(session: Session):
    """
    Test the `from_utc_timestamp` function
    """
    query = parse(
        "SELECT from_utc_timestamp('2023-01-01 00:00:00', 'PST'), "
        "from_utc_timestamp(cast('2023-01-01 00:00:00' as timestamp), 'IST')",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.TimestampType()  # type: ignore
    assert query.select.projection[1].type == ct.TimestampType()  # type: ignore


def test_get_func(session: Session):
    """
    Test the `get` function
    """
    query = parse("SELECT get(array(1, 2, 3), 0), get(array('a', 'b', 'c'), 1)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore
    assert query.select.projection[1].type == ct.StringType()  # type: ignore


def test_get_json_object_func(session: Session):
    """
    Test the `get_json_object` function
    """
    query = parse(
        "SELECT get_json_object('{\"key\": \"value\"}', '$.key'), "
        'get_json_object(\'{"key1": "value1", "key2": "value2"}\', \'$.key2\')',
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.StringType()  # type: ignore
    assert query.select.projection[1].type == ct.StringType()  # type: ignore


def test_getbit_func(session: Session):
    """
    Test the `getbit` function
    """
    query = parse("SELECT getbit(1010, 0), getbit(1010, 1)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore
    assert query.select.projection[1].type == ct.IntegerType()  # type: ignore


def test_greatest(session: Session):
    """
    Test `greatest`
    """
    query = parse("SELECT greatest(10, 9, 2, 4, 3)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore


def test_grouping_func(session: Session):
    """
    Test the `grouping` function
    """
    query = parse(
        "SELECT grouping(col1), grouping(col2) FROM "
        "(SELECT (1), (2) AS col1, (3), (4) AS col2) GROUP BY col1",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore
    assert query.select.projection[1].type == ct.IntegerType()  # type: ignore


def test_grouping_id_func(session: Session):
    """
    Test the `grouping_id` function
    """
    query = parse(
        "SELECT grouping_id(col1, col2) FROM "
        "(SELECT (1), (2) AS col1, (3), (4) AS col2) GROUP BY col1",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.BigIntType()  # type: ignore


def test_hash_func(session: Session):
    """
    Test the `hash` function
    """
    query = parse("SELECT hash('hello'), hash(123)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore
    assert query.select.projection[1].type == ct.IntegerType()  # type: ignore


def test_hex_func(session: Session):
    """
    Test the `hex` function
    """
    query = parse("SELECT hex('hello'), hex(123)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.StringType()  # type: ignore
    assert query.select.projection[1].type == ct.StringType()  # type: ignore


def test_histogram_numeric_func(session: Session):
    """
    Test the `histogram_numeric` function
    """
    query = parse("SELECT histogram_numeric(col, 5) FROM (SELECT (1), (2) AS col)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert isinstance(query.select.projection[0].type, ct.ListType)  # type: ignore
    assert isinstance(query.select.projection[0].type.element.type, ct.StructType)  # type: ignore


def test_hour_func(session: Session):
    """
    Test the `hour` function
    """
    query = parse(
        "SELECT hour(cast('2023-01-01 12:34:56' as timestamp)), hour('2023-01-01 23:45:56')",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore
    assert query.select.projection[1].type == ct.IntegerType()  # type: ignore


def test_hypot_func(session: Session):
    """
    Test the `hypot` function
    """
    query = parse("SELECT hypot(3, 4), hypot(5.0, 12.0)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.FloatType()  # type: ignore
    assert query.select.projection[1].type == ct.FloatType()  # type: ignore


def test_if(session: Session):
    """
    Test the `if` functions
    """
    query = parse(
        "SELECT if(col1 = 'x', NULL, 1) FROM (SELECT ('aee'), ('bee') AS col1)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore


def test_ilike_like_func(session: Session):
    """
    Test the `ilike`, `like` functions
    """
    query = parse(
        "SELECT col1 ilike '%pattern%', ilike(col1, '%pattern%'), "
        "like(col1, '%pattern%') FROM (SELECT ('aee'), ('bee') AS col1)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.BooleanType()  # type: ignore
    assert query.select.projection[1].type == ct.BooleanType()  # type: ignore
    assert query.select.projection[2].type == ct.BooleanType()  # type: ignore


def test_initcap_func(session: Session):
    """
    Test the `initcap` function
    """
    query = parse("SELECT initcap('hello world'), initcap('SQL function')")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.StringType()  # type: ignore
    assert query.select.projection[1].type == ct.StringType()  # type: ignore


def test_inline_func(session: Session):
    """
    Test the `inline` function
    """
    # This test assumes there's a table with a column of type ARRAY<STRUCT<a: INT, b: STRING>>
    query = parse(
        "SELECT inline(col1), inline_outer(array(struct(1, 'a'), struct(2, 'b')))"
        " FROM (SELECT (array(struct('a', 1, 'b', '222'))) AS col1)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert isinstance(query.select.projection[0].type, ct.StructType)  # type: ignore
    assert isinstance(query.select.projection[1].type, ct.StructType)  # type: ignore


def test_input_file_block_length_func(session: Session):
    """
    Test the `input_file_block_length` function
    """
    query = parse("SELECT input_file_block_length()")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.LongType()  # type: ignore


def test_input_file_block_start_func(session: Session):
    """
    Test the `input_file_block_start` function
    """
    query = parse("SELECT input_file_block_start()")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.LongType()  # type: ignore


def test_input_file_name_func(session: Session):
    """
    Test the `input_file_name` function
    """
    query = parse("SELECT input_file_name()")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.StringType()  # type: ignore


def test_int(session: Session):
    """
    Test the `int` function
    """
    query = parse("SELECT int('3')")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore


def test_instr_func(session: Session):
    """
    Test the `instr` function
    """
    query = parse("SELECT instr('hello world', 'world'), instr('hello world', 'SQL')")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore
    assert query.select.projection[1].type == ct.IntegerType()  # type: ignore


def test_isnan_func(session: Session):
    """
    Test the `isnan` function
    """
    query = parse("SELECT isnan(1/0), isnan(0.0/0.0)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.BooleanType()  # type: ignore
    assert query.select.projection[1].type == ct.BooleanType()  # type: ignore


def test_isnotnull_isnull(session: Session):
    """
    Test the `isnotnull`, `isnull` functions
    """
    query = parse("SELECT isnotnull(0), isnull(null)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.BooleanType()  # type: ignore
    assert query.select.projection[1].type == ct.BooleanType()  # type: ignore


def test_json_array_length_func(session: Session):
    """
    Test the `json_array_length` function
    """
    query = parse("SELECT json_array_length('[1, 2, 3]')")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore


def test_json_object_keys_func(session: Session):
    """
    Test the `json_object_keys` function
    """
    query = parse('SELECT json_object_keys(\'{"key1": "value1", "key2": "value2"}\')')
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert isinstance(query.select.projection[0].type, ct.ListType)  # type: ignore
    assert query.select.projection[0].type.element.type == ct.StringType()  # type: ignore


def test_json_tuple_func(session: Session):
    """
    Test the `json_tuple` function
    """
    query = parse(
        'SELECT json_tuple(\'{"key1": "value1", "key2": "value2"}\', \'key1\', \'key2\')',
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert isinstance(query.select.projection[0].type, ct.ListType)  # type: ignore


def test_kurtosis_func(session: Session):
    """
    Test the `kurtosis` function
    """
    query = parse("SELECT kurtosis(col) FROM (SELECT (1), (2), (3) AS col)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.DoubleType()  # type: ignore


def test_lag_func(session: Session):
    """
    Test the `lag` function
    """
    query = parse(
        "SELECT lag(col) OVER (ORDER BY col) FROM (SELECT (1), (2), (3) AS col)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    # The output type depends on the type of `col`
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore


def test_last_and_last_value(session: Session):
    """
    Test the `last` function
    """
    query = parse(
        "SELECT last(col) OVER (PARTITION BY col2 ORDER BY col3), "
        "last_value(col) OVER (PARTITION BY col2 ORDER BY col3) "
        "FROM (SELECT (1), (2), (3) AS col, (1), (2), (3) AS col2, "
        "(3), (4), (5) as col3)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    # The output type depends on the type of `col`
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore
    assert query.select.projection[1].type == ct.IntegerType()  # type: ignore


def test_last_day_func(session: Session):
    """
    Test the `last_day` function
    """
    query = parse("SELECT last_day('2023-01-01'), last_day(cast('2023-02-15' as date))")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.DateType()  # type: ignore
    assert query.select.projection[1].type == ct.DateType()  # type: ignore


def test_lcase_func(session: Session):
    """
    Test the `lcase` function
    """
    query = parse("SELECT lcase('HELLO'), lcase('WORLD')")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.StringType()  # type: ignore
    assert query.select.projection[1].type == ct.StringType()  # type: ignore


def test_lead_func(session: Session):
    """
    Test the `lead` function
    """
    query = parse(
        "SELECT lead(col, 1, 'N/A') OVER (ORDER BY col) FROM (SELECT ('1'), ('a'), ('x') AS col)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    # The output type depends on the type of `col`
    # Assuming `col` is of type StringType for this test
    assert query.select.projection[0].type == ct.StringType()  # type: ignore


def test_least_func(session: Session):
    """
    Test the `least` function
    """
    query = parse("SELECT least(10, 20, 30, 40)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore


# TODO: figure out why antlr parser fails
# def test_left_func(session: Session):
#     """
#     Test the `left` function
#     """
#     query = parse("SELECT left('hello world', 5)")
#     exc = DJException()
#     ctx = ast.CompileContext(session=session, exception=exc)
#     query.compile(ctx)
#     assert not exc.errors
#     assert query.select.projection[0].type == ct.StringType()  # type: ignore
#     assert query.select.projection[1].type == ct.StringType()  # type: ignore


def test_len_func(session: Session):
    """
    Test the `len` function
    """
    query = parse("SELECT len('hello'), len('world')")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore
    assert query.select.projection[1].type == ct.IntegerType()  # type: ignore


def test_like_func(session: Session):
    """
    Test the `like` function
    """
    query = parse(
        "SELECT col like '%pattern%' FROM (SELECT ('a'), ('b'), ('c') AS col)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.BooleanType()  # type: ignore


def test_localtimestamp_func(session: Session):
    """
    Test the `localtimestamp` function
    """
    query = parse("SELECT localtimestamp()")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.TimestampType()  # type: ignore


def test_locate_func(session: Session):
    """
    Test the `locate` function
    """
    query = parse(
        "SELECT locate('world', 'hello world'), locate('SQL', 'hello world', 2)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore
    assert query.select.projection[1].type == ct.IntegerType()  # type: ignore


def test_log_functions(session: Session):
    """
    Test the `log1p`, `log10` and `log2` functions
    """
    query = parse(
        "SELECT log1p(col), log10(col), log2(col) FROM (SELECT (1), (2), (3) as col)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.DoubleType()  # type: ignore
    assert query.select.projection[1].type == ct.DoubleType()  # type: ignore
    assert query.select.projection[2].type == ct.DoubleType()  # type: ignore


def test_lpad_func(session: Session):
    """
    Test the `lpad` function
    """
    query = parse("SELECT lpad('hello', 10, ' '), lpad('SQL', 5, '0')")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.StringType()  # type: ignore
    assert query.select.projection[1].type == ct.StringType()  # type: ignore


def test_ltrim_func(session: Session):
    """
    Test the `ltrim` function
    """
    query = parse("SELECT ltrim('   hello'), ltrim('-----world-', '-')")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.StringType()  # type: ignore
    assert query.select.projection[1].type == ct.StringType()  # type: ignore


def test_make_date_func(session: Session):
    """
    Test the `make_date` function
    """
    query = parse("SELECT make_date(2023, 7, 30), make_date(2023, 12, 31)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.DateType()  # type: ignore
    assert query.select.projection[1].type == ct.DateType()  # type: ignore


def test_make_dt_interval_func(session: Session):
    """
    Test the `make_dt_interval` function
    """
    query = parse("SELECT make_dt_interval(1, 2, 30, 45)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.DayTimeIntervalType()  # type: ignore


def test_make_interval_func(session: Session):
    """
    Test the `make_interval` function
    """
    query = parse("SELECT make_interval(1, 6)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.YearMonthIntervalType()  # type: ignore


def test_make_timestamp_func(session: Session):
    """
    Test the `make_timestamp` function
    """
    query = parse("SELECT make_timestamp(2023, 7, 30, 14, 45, 30)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.TimestampType()  # type: ignore


def test_make_timestamp_ltz_func(session: Session):
    """
    Test the `make_timestamp_ltz` function
    """
    query = parse(
        "SELECT make_timestamp_ltz(2023, 7, 30, 14, 45, 30, 'UTC'), "
        "make_timestamp_ltz(2023, 7, 30, 14, 45, 30)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.TimestampType()  # type: ignore
    assert query.select.projection[1].type == ct.TimestampType()  # type: ignore


def test_make_timestamp_ntz_func(session: Session):
    """
    Test the `make_timestamp_ntz` function
    """
    query = parse("SELECT make_timestamp_ntz(2023, 7, 30, 14, 45, 30)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.TimestampType()  # type: ignore


def test_make_ym_interval_func(session: Session):
    """
    Test the `make_ym_interval` function
    """
    query = parse("SELECT make_ym_interval(1, 6)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.YearMonthIntervalType()  # type: ignore


def test_map_concat_func(session: Session):
    """
    Test the `map_concat` function
    """
    query = parse("SELECT map_concat(map(1, 'a', 2, 'b'), map(1, 'c'))")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.MapType(key_type=ct.IntegerType(), value_type=ct.StringType())  # type: ignore


def test_map_contains_key_func(session: Session):
    """
    Test the `map_contains_key` function
    """
    query = parse("SELECT map_contains_key(map(1, 'a', 2, 'b'), 1)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.BooleanType()  # type: ignore


def test_map_entries_func(session: Session):
    """
    Test the `map_entries` function
    """
    query = parse("SELECT map_entries(map(1, 'a', 2, 'b'))")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.ListType(  # type: ignore
        element_type=ct.StructType(
            ct.NestedField(name="key", field_type=ct.IntegerType()),  # type: ignore
            ct.NestedField(name="value", field_type=ct.StringType()),  # type: ignore
        ),
    )  # type: ignore


def test_map_filter_func(session: Session):
    """
    Test the `map_filter` function
    """
    query = parse("SELECT map_filter(map(1, 0, 2, 2, 3, -1), (k, v) -> k > v)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert query.select.projection[0].type == ct.MapType(  # type: ignore
        key_type=ct.IntegerType(),
        value_type=ct.IntegerType(),
    )


def test_map_from_arrays_func(session: Session):
    """
    Test the `map_from_arrays` function
    """
    query = parse("SELECT map_from_arrays(array(1.0, 3.0), array('2', '4'))")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.MapType(  # type: ignore
        key_type=ct.FloatType(),
        value_type=ct.StringType(),
    )


def test_map_from_entries_func(session: Session):
    """
    Test the `map_from_entries` function
    """
    query = parse("SELECT map_from_entries(array(struct(1, 'a'), struct(2, 'b')))")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.MapType(  # type: ignore
        key_type=ct.IntegerType(),
        value_type=ct.StringType(),
    )


def test_map_keys_func(session: Session):
    """
    Test the `map_keys` function
    """
    query = parse("SELECT map_keys(map(1, 'a', 2, 'b'))")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.ListType(element_type=ct.IntegerType())  # type: ignore


def test_map_values_func(session: Session):
    """
    Test the `map_values` function
    """
    query = parse("SELECT map_values(map(1, 'a', 2, 'b'))")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.ListType(element_type=ct.StringType())  # type: ignore


def test_max() -> None:
    """
    Test ``Max`` function.
    """
    assert (
        Max.infer_type(ast.Column(ast.Name("x"), _type=IntegerType())) == IntegerType()
    )
    assert Max.infer_type(ast.Column(ast.Name("x"), _type=BigIntType())) == BigIntType()
    assert Max.infer_type(ast.Column(ast.Name("x"), _type=FloatType())) == FloatType()
    assert Max.infer_type(
        ast.Column(ast.Name("x"), _type=DecimalType(8, 6)),
    ) == DecimalType(8, 6)
    assert (
        Max.infer_type(
            ast.Column(ast.Name("x"), _type=StringType()),
        )
        == StringType()
    )


# TODO
# def test_map_zip_with_func(session: Session):
#     """
#     Test the `map_zip_with` function
#     """
#     # The third argument to map_zip_with is a function, which needs special handling
#     # Assuming that we have a function "func" defined elsewhere in the code
#     query = parse("SELECT map_zip_with(map_col1, map_col2, func) FROM table")
#     exc = DJException()
#     ctx = ast.CompileContext(session=session, exception=exc)
#     query.compile(ctx)
#     assert not exc.errors
#     assert isinstance(query.select.projection[0].type, ct.MapType)  # type: ignore


def test_mask_func(session: Session):
    """
    Test the `mask` function
    """
    query = parse(
        "SELECT mask('abcd-EFGH-8765-4321'), "
        "mask('abcd-EFGH-8765-4321', 'Q'), "
        "mask('AbCD123-@$#', 'Q', 'q'), "
        "mask('AbCD123-@$#', 'Q', 'q', 'd'), "
        "mask('AbCD123-@$#', 'Q', 'q', 'd', 'o')",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.StringType()  # type: ignore
    assert query.select.projection[1].type == ct.StringType()  # type: ignore
    assert query.select.projection[2].type == ct.StringType()  # type: ignore
    assert query.select.projection[3].type == ct.StringType()  # type: ignore
    assert query.select.projection[4].type == ct.StringType()  # type: ignore


def test_max_by_min_by_funcs(session: Session):
    """
    Test the `max_by` function
    """
    query = parse(
        "SELECT max_by(x, y), min_by(x, y) "
        "FROM (SELECT ('a'), ('b'), ('c') AS x, (10), (50), (20) AS y)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.StringType()  # type: ignore
    assert query.select.projection[1].type == ct.StringType()  # type: ignore


def test_md5_func(session: Session):
    """
    Test the `md5` function
    """
    query = parse("SELECT md5('Spark')")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.StringType()  # type: ignore


def test_mean_func(session: Session):
    """
    Test the `mean` function
    """
    query = parse("SELECT mean(col) FROM (SELECT (1), (2), (3) AS col)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.DoubleType()  # type: ignore


def test_median_func(session: Session):
    """
    Test the `median` function
    """
    query = parse("SELECT median(col) FROM (SELECT (1), (2), (3) AS col)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.DoubleType()  # type: ignore


def test_min() -> None:
    """
    Test ``Min`` function.
    """
    assert (
        Min.infer_type(ast.Column(ast.Name("x"), _type=IntegerType())) == IntegerType()
    )
    assert Min.infer_type(ast.Column(ast.Name("x"), _type=BigIntType())) == BigIntType()
    assert Min.infer_type(ast.Column(ast.Name("x"), _type=FloatType())) == FloatType()
    assert Min.infer_type(
        ast.Column(ast.Name("x"), _type=DecimalType(8, 6)),
    ) == DecimalType(8, 6)
    with pytest.raises(Exception):
        Min.infer_type(  # pylint: disable=expression-not-assigned
            ast.Column(ast.Name("x"), _type=StringType()),
        ) == StringType()


def test_minute(session: Session):
    """
    Test the `minute` function
    """
    query = parse(
        "SELECT minute('2009-07-30 12:58:59'), minute(cast('2009-07-30 12:58:59' as timestamp))",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore
    assert query.select.projection[1].type == ct.IntegerType()  # type: ignore


def test_mod(session: Session):
    """
    Test the `mod` function
    """
    query = parse(
        "SELECT MOD(2, 1.8)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.FloatType()  # type: ignore


def test_mode(session: Session):
    """
    Test the `mode` function
    """
    query = parse(
        "SELECT mode(col) FROM (SELECT (0), (10), (10) AS col)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore


def test_monotonically_increasing_id(session: Session):
    """
    Test the `monotonically_increasing_id` function
    """
    query = parse(
        "SELECT monotonically_increasing_id()",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.BigIntType()  # type: ignore


def test_months_between(session: Session):
    """
    Test the `months_between` function
    """
    query = parse(
        "SELECT months_between('1997-02-28 10:30:00', '1996-10-30'), "
        "months_between(cast('1997-02-28 10:30:00' as timestamp), cast('1996-10-30' as timestamp)), "
        "months_between('1997-02-28 10:30:00', '1996-10-30', false)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.FloatType()  # type: ignore


def test_named_struct_func(session: Session):
    """
    Test the `named_struct` function
    """
    query = parse("SELECT named_struct('name', 'cactus', 'age', 30)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.StructType(  # type: ignore
        ct.NestedField(name="name", field_type=ct.StringType()),  # type: ignore
        ct.NestedField(name="age", field_type=ct.IntegerType()),  # type: ignore
    )


def test_nanvl_func(session: Session):
    """
    Test the `nanvl` function
    """
    query = parse("SELECT nanvl(cast('NaN' as double), 123)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.DoubleType()  # type: ignore


def test_negative_func(session: Session):
    """
    Test the `negative` function
    """
    query = parse("SELECT negative(1)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore


def test_next_day_func(session: Session):
    """
    Test the `next_day` function
    """
    query = parse("SELECT next_day('2015-01-14', 'TU')")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.DateType()  # type: ignore


def test_not_func(session: Session):
    """
    Test the `not` function
    """
    query = parse("SELECT not(true)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.BooleanType()  # type: ignore


def test_now() -> None:
    """
    Test ``Now`` function.
    """
    assert Now.infer_type() == ct.TimestampType()


def test_nth_value_func(session: Session):
    """
    Test the `nth_value` function
    """
    query = parse(
        "SELECT a, b, nth_value(b, 2) OVER (PARTITION BY a ORDER BY b) FROM "
        "(SELECT ('A1'), ('A2'), ('A1') AS a, (2), (1), (3) AS b)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[2].type == ct.IntegerType()  # type: ignore


def test_ntile_func(session: Session):
    """
    Test the `ntile` function
    """
    query = parse(
        "SELECT a, b, ntile(2) OVER (PARTITION BY a ORDER BY b) FROM"
        "(SELECT ('A1'), ('A2'), ('A1') AS a, (2), (1), (3) AS b)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[2].type == ct.IntegerType()  # type: ignore


def test_nullif_func(session: Session):
    """
    Test the `nullif` function
    """
    query = parse("SELECT nullif(2, 2)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore


def test_nvl_func(session: Session):
    """
    Test the `nvl` function
    """
    query = parse("SELECT nvl(array('1'), array('2'))")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.ListType(element_type=ct.StringType())  # type: ignore


def test_nvl2_func(session: Session):
    """
    Test the `nvl2` function
    """
    query = parse("SELECT nvl2(3, NULL, 1)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore


def test_octet_length_func(session: Session):
    """
    Test the `octet_length` function
    """
    query = parse("SELECT octet_length('Spark SQL')")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore


def test_overlay_func(session: Session):
    """
    Test the `overlay` function
    TODO: support syntax like:  # pylint: disable=fixme
        SELECT overlay(encode('Spark SQL', 'utf-8') PLACING encode('_', 'utf-8') FROM 6);
    """
    query = parse("SELECT overlay('Hello World', 'J', 7)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.StringType()  # type: ignore


def test_percentile(session: Session):
    """
    Test the `percentile` function
    """
    query = parse("SELECT percentile(col, 0.3) FROM (SELECT (0), (10) AS col)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.FloatType()  # type: ignore

    query = parse(
        "SELECT percentile(col, array(0.25, 0.75)) FROM (SELECT (0), (10) AS col)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.ListType(element_type=ct.FloatType())  # type: ignore

    query = parse(
        "SELECT percentile(col, 0.5) FROM ("
        "SELECT (INTERVAL '0' MONTH), (INTERVAL '10' MONTH) AS col)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.FloatType()  # type: ignore

    query = parse(
        "SELECT percentile(col, 0.5, 3) FROM ("
        "SELECT (INTERVAL '0' MONTH), (INTERVAL '10' MONTH) AS col)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.FloatType()  # type: ignore

    query = parse(
        "SELECT percentile(col, array(0.2, 0.5)) "
        "FROM (SELECT (INTERVAL '0' MONTH), (INTERVAL '10' MONTH) AS col)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.ListType(ct.FloatType())  # type: ignore

    query = parse(
        "SELECT percentile(col, array(0.2, 0.5), 2) "
        "FROM (SELECT (INTERVAL '0' MONTH), (INTERVAL '10' MONTH) AS col)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.ListType(ct.FloatType())  # type: ignore


def test_random(session: Session):
    """
    Test `random`, `rand` and `randn`
    """
    query = parse(
        "SELECT random(), random(0), random(null), rand(), rand(0), "
        "rand(null), randn(), randn(0), randn(null)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    for column in query.select.projection:
        assert column.type == ct.FloatType()  # type: ignore


def test_rank(session: Session):
    """
    Test `rank`
    """
    query = parse(
        "SELECT rank() OVER (PARTITION BY col ORDER BY col) FROM (SELECT (1), (2) AS col)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore


def test_regexp_like(session: Session):
    """
    Test `regexp_like`
    """
    query = parse(
        "SELECT regexp_like('%SystemDrive%\\Users\\John', '%SystemDrive%\\Users.*')",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.BooleanType()  # type: ignore


def test_row_number(session: Session):
    """
    Test `row_number`
    """
    query = parse(
        "SELECT row_number() OVER (PARTITION BY col ORDER BY col) FROM (SELECT (1), (2) AS col)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore


def test_round(session: Session):
    """
    Test `round`
    """
    query = parse(
        "SELECT round(2.5, 0)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore

    query = parse(
        "SELECT round(2.5)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore


def test_sequence(session: Session):
    """
    Test `sequence`
    """
    query = parse("SELECT sequence(1, 10)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.ListType(element_type=ct.IntegerType())  # type: ignore

    query = parse("SELECT sequence(1, 10, 2)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.ListType(element_type=ct.IntegerType())  # type: ignore

    query = parse(
        "SELECT sequence(to_date('2018-01-01'), to_date('2018-03-01'), interval 1 month)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.ListType(element_type=ct.DateType())  # type: ignore

    query = parse(
        "SELECT sequence(to_timestamp('2018-01-01 00:00:00'), "
        "to_timestamp('2018-01-01 12:00:00'), interval 1 hour)",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.ListType(element_type=ct.TimestampType())  # type: ignore


def test_size(session: Session):
    """
    Test the `size` Spark function
    """
    query = parse("SELECT size(array('b', 'd', 'c', 'a'))")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore

    query = parse("SELECT size(map('a', 1, 'b', 2))")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore


def test_split(session: Session):
    """
    Test the `split` Spark function
    """
    query = parse("SELECT split('oneAtwoBthreeC', '[ABC]')")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.ListType(element_type=ct.StringType())  # type: ignore


def test_strpos(session: Session):
    """
    Test `strpos`
    """
    query = parse("SELECT strpos('abcde', 'cde'), strpos('abcde', 'cde', 4)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore
    assert query.select.projection[1].type == ct.IntegerType()  # type: ignore


def test_substring(session: Session):
    """
    Test `substring`
    """
    query = parse("SELECT substring('Spark SQL', 5), substring('Spark SQL', 5, 1)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.StringType()  # type: ignore
    assert query.select.projection[1].type == ct.StringType()  # type: ignore

    query = parse("SELECT substr('Spark SQL', 5), substring('Spark SQL', 5, 1)")
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.StringType()  # type: ignore
    assert query.select.projection[1].type == ct.StringType()  # type: ignore


def test_sum() -> None:
    """
    Test ``sum`` function.
    """
    assert (
        Sum.infer_type(ast.Column(ast.Name("x"), _type=IntegerType())) == BigIntType()
    )
    assert Sum.infer_type(ast.Column(ast.Name("x"), _type=FloatType())) == DoubleType()
    assert Sum.infer_type(
        ast.Column(ast.Name("x"), _type=DecimalType(8, 6)),
    ) == DecimalType(18, 6)


def test_transform(session: Session):
    """
    Test the `transform` Spark function
    """
    query = parse(
        """
        SELECT transform(array(1, 2, 3), x -> x + 1)
        """,
    )
    ctx = ast.CompileContext(session=session, exception=DJException())
    query.compile(ctx)
    assert query.select.projection[0].type == ct.ListType(element_type=ct.IntegerType())  # type: ignore

    query = parse(
        """
        SELECT transform(array(1, 2, 3), (x, i) -> x + i)
        """,
    )
    ctx = ast.CompileContext(session=session, exception=DJException())
    query.compile(ctx)
    assert query.select.projection[0].type == ct.ListType(element_type=ct.IntegerType())  # type: ignore


def test_to_date() -> None:
    """
    Test ``to_date`` function.
    """
    assert (
        ToDate.infer_type(ast.Column(ast.Name("x"), _type=StringType())) == DateType()
    )


def test_trim(session: Session):
    """
    Test `trim`
    """
    query = parse("SELECT trim('    lmgi   ')")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.StringType()  # type: ignore


def test_unhex_func(session: Session):
    """
    Test the `unhex` function
    """
    query = parse("SELECT unhex('4D'), unhex('7953514C')")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.BinaryType()  # type: ignore
    assert query.select.projection[1].type == ct.BinaryType()  # type: ignore


def test_upper(session: Session):
    """
    Test `upper`
    """
    query = parse("SELECT upper('abcde')")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.StringType()  # type: ignore
