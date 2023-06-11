"""
Tests for ``dj.sql.functions``.
"""
# pylint: disable=line-too-long

import pytest
from sqlmodel import Session

import dj.sql.functions as F
import dj.sql.parsing.types as ct
from dj.errors import DJException, DJNotImplementedException
from dj.sql.functions import (
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
from dj.sql.parsing import ast
from dj.sql.parsing.backends.antlr4 import parse
from dj.sql.parsing.types import (
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


def test_count() -> None:
    """
    Test ``Count`` function.
    """
    assert (
        Count.infer_type(ast.Column(ast.Name("x"), _type=WildcardType()))
        == BigIntType()
    )
    assert Count.is_aggregation is True


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


def test_first(session: Session):
    """
    Test `first`
    """
    query = parse("SELECT first(col), first(col, true) FROM (SELECT (1), (2) AS col)")
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    query.compile(ctx)
    assert not exc.errors
    assert query.select.projection[0].type == ct.IntegerType()  # type: ignore
    assert query.select.projection[1].type == ct.IntegerType()  # type: ignore


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
