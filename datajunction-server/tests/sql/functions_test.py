"""
Tests for ``datajunction_server.sql.functions``.
"""
# pylint: disable=line-too-long,too-many-lines

import pytest
from sqlmodel import Session

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


def test_ceil_func(session: Session):
    """
    Test the `ceil` function
    """
    query = parse("SELECT ceil(-0.1), ceil(5), ceil(3.1411, 3), ceil(3.1411, -3)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == BigIntType()  # type: ignore
    assert query.select.projection[1].type == BigIntType()  # type: ignore
    assert query.select.projection[2].type == DecimalType(precision=14, scale=3)  # type: ignore
    assert query.select.projection[3].type == DecimalType(precision=14, scale=0)  # type: ignore


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


def test_concat_ws_func(session: Session):
    """
    Test the `concat_ws` function
    """
    query = parse("SELECT concat_ws(',', 'hello', 'world'), concat_ws('-', 'spark', 'sql', 'function')")
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
    query = parse("SELECT contains('hello world', 'world'), contains('hello world', 'spark')")
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
    query = parse("SELECT convert_timezone('PST', 'EST', cast('2023-07-30 12:34:56' as timestamp))")
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


def test_now() -> None:
    """
    Test ``Now`` function.
    """
    assert Now.infer_type() == ct.TimestampType()


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
