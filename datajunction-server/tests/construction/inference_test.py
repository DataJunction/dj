"""Test type inference."""

# pylint: disable=W0621,C0325
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.errors import DJException
from datajunction_server.models.engine import Dialect
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.ast import CompileContext
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.sql.parsing.backends.exceptions import DJParseException
from datajunction_server.sql.parsing.types import (
    BigIntType,
    BooleanType,
    ColumnType,
    DateType,
    DayTimeIntervalType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IntegerType,
    ListType,
    MapType,
    NullType,
    StringType,
    TimestampType,
    TimeType,
)


@pytest.mark.asyncio
async def test_infer_column_with_table(construction_session: AsyncSession):
    """
    Test getting the type of a column that has a table
    """
    table = ast.Table(
        ast.Name("orders", namespace=ast.Name("dbt.source.jaffle_shop")),
    )
    ctx = CompileContext(
        session=construction_session,
        exception=DJException(),
    )
    await table.compile(ctx)
    assert table.columns[0].type == IntegerType()
    assert table.columns[1].type == IntegerType()
    assert table.columns[2].type == DateType()
    assert table.columns[3].type == StringType()


def test_infer_values():
    """
    Test inferring types from values directly
    """
    assert ast.String(value="foo").type == StringType()
    assert ast.Number(value=10).type == IntegerType()
    assert ast.Number(value=-10).type == IntegerType()
    assert ast.Number(value=922337203685477).type == BigIntType()
    assert ast.Number(value=-922337203685477).type == BigIntType()
    assert ast.Number(value=3.4e39).type == DoubleType()
    assert ast.Number(value=-3.4e39).type == DoubleType()
    assert ast.Number(value=3.4e38).type == FloatType()
    assert ast.Number(value=-3.4e38).type == FloatType()


def test_raise_on_invalid_infer_binary_op():
    """
    Test raising when trying to infer types from an invalid binary op
    """
    with pytest.raises(DJParseException) as exc_info:
        ast.BinaryOp(  # pylint: disable=expression-not-assigned
            op=ast.BinaryOpKind.Modulo,
            left=ast.String(value="foo"),
            right=ast.String(value="bar"),
        ).type

    assert (
        "Incompatible types in binary operation foo % bar. "
        "Got left string, right string."
    ) in str(exc_info.value)


@pytest.mark.asyncio
async def test_infer_column_with_an_aliased_table(construction_session: AsyncSession):
    """
    Test getting the type of a column that has an aliased table
    """
    ctx = CompileContext(
        session=construction_session,
        exception=DJException(),
    )
    table = ast.Table(
        ast.Name("orders", namespace=ast.Name("dbt.source.jaffle_shop")),
    )
    alias = ast.Alias(
        alias=ast.Name(
            name="foo",
            namespace=ast.Name(
                name="a",
                namespace=ast.Name(
                    name="b",
                    namespace=ast.Name("c"),
                ),
            ),
        ),
        child=table,
    )
    await alias.compile(ctx)

    assert alias.child.columns[0].type == IntegerType()
    assert alias.child.columns[1].type == IntegerType()
    assert alias.child.columns[2].type == DateType()
    assert alias.child.columns[3].type == StringType()
    assert alias.child.columns[4].type == TimestampType()


def test_raising_when_table_has_no_dj_node():
    """
    Test raising when getting the type of a column that has a table with no DJ node
    """
    table = ast.Table(ast.Name("orders"))
    col = ast.Column(ast.Name("status"), _table=table)

    with pytest.raises(DJParseException) as exc_info:
        col.type  # pylint: disable=pointless-statement

    assert ("Cannot resolve type of column orders.status") in str(exc_info.value)


def test_raising_when_select_has_multiple_expressions_in_projection():
    """
    Test raising when a select has more than one in projection
    """
    with pytest.raises(DJParseException) as exc_info:
        parse("select 1, 2").select.type  # pylint: disable=expression-not-assigned

    assert ("single expression in its projection") in str(exc_info.value)


def test_raising_when_between_different_types():
    """
    Test raising when a between has multiple types
    """
    with pytest.raises(DJParseException) as exc_info:
        parse(  # pylint: disable=expression-not-assigned
            "select 1 between 'hello' and TRUE",
        ).select.type

    assert ("BETWEEN expects all elements to have the same type") in str(exc_info.value)


def test_raising_when_unop_bad_type():
    """
    Test raising when a unop gets a bad type
    """
    with pytest.raises(DJParseException) as exc_info:
        parse(  # pylint: disable=expression-not-assigned
            "select not 'hello'",
        ).select.type

    assert ("Incompatible type in unary operation") in str(exc_info.value)


def test_raising_when_expression_has_no_parent():
    """
    Test raising when getting the type of a column that has no parent
    """
    col = ast.Column(ast.Name("status"), _table=None)

    with pytest.raises(DJParseException) as exc_info:
        col.type  # pylint: disable=pointless-statement

    assert "Cannot resolve type of column status that has no parent" in str(
        exc_info.value,
    )


@pytest.mark.asyncio
async def test_infer_map_subscripts(construction_session: AsyncSession):
    """
    Test inferring map subscript types
    """
    query = parse(
        """
        SELECT
          names_map["first"] as first_name,
          names_map["last"] as last_name,
          user_metadata["propensity_score"] as propensity_score,
          user_metadata["propensity_score"]["weighted"] as weighted_propensity_score,
          user_metadata["propensity_score"]["weighted"]["year"] as weighted_propensity_score_year
        FROM basic.source.users
    """,
    )
    exc = DJException()
    ctx = CompileContext(session=construction_session, exception=exc)
    await query.compile(ctx)
    types = [
        StringType(),
        StringType(),
        MapType(
            key_type=StringType(),
            value_type=MapType(key_type=StringType(), value_type=FloatType()),
        ),
        MapType(key_type=StringType(), value_type=FloatType()),
        FloatType(),
    ]
    assert types == [exp.type for exp in query.select.projection]  # type: ignore


@pytest.mark.asyncio
async def test_infer_types_complicated(construction_session: AsyncSession):
    """
    Test inferring complicated types
    """
    query = parse(
        """
      SELECT id+1-2/3*5%6&10|8^5,
      CAST('2022-01-01T12:34:56Z' AS TIMESTAMP),
      -- Raw('average({id})', 'INT', True),
      -- Raw('aggregate(array(1, 2, {id}), 0, (acc, x) -> acc + x, acc -> acc * 10)', 'INT'),
      -- Raw('NOW()', 'datetime'),
      -- DATE_TRUNC('day', '2014-03-10'),
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
      (select 5.0),
      CASE WHEN first_name = last_name THEN COUNT(DISTINCT first_name) ELSE
      COUNT(DISTINCT last_name) END
      FROM (
      SELECT id,
         first_name,
         last_name<>'yoyo' and last_name='yoyo' or last_name='yoyo' as bizarre,
         last_name
      FROM dbt.source.jaffle_shop.customers
        )
    """,
    )
    exc = DJException()
    ctx = CompileContext(session=construction_session, exception=exc)
    await query.compile(ctx)
    types = [
        IntegerType(),
        TimestampType(),
        TimestampType(),
        IntegerType(),
        NullType(),
        NullType(),
        IntegerType(),
        IntegerType(),
        IntegerType(),
        DoubleType(),
        BigIntType(),
        BigIntType(),
        BooleanType(),
        IntegerType(),
        BooleanType(),
        BooleanType(),
        BooleanType(),
        BooleanType(),
        BooleanType(),
        BooleanType(),
        BooleanType(),
        BooleanType(),
        BooleanType(),
        BooleanType(),
        BooleanType(),
        StringType(),
        StringType(),
        BooleanType(),
        StringType(),
        BooleanType(),
        FloatType(),
        BigIntType(),
    ]
    assert types == [exp.type for exp in query.select.projection]  # type: ignore


@pytest.mark.asyncio
async def test_infer_bad_case_types(construction_session: AsyncSession):
    """
    Test inferring mismatched case types.
    """
    with pytest.raises(Exception) as excinfo:
        query = parse(
            """
            SELECT
            CASE WHEN first_name = last_name THEN COUNT(DISTINCT first_name) ELSE last_name END
            FROM dbt.source.jaffle_shop.customers
            """,
        )
        ctx = CompileContext(
            session=construction_session,
            exception=DJException(),
        )
        await query.compile(ctx)
        [  # pylint: disable=pointless-statement
            exp.type for exp in query.select.projection  # type: ignore
        ]

    assert str(excinfo.value) == "Not all the same type in CASE! Found: bigint, string"


@pytest.mark.asyncio
async def test_infer_types_avg(construction_session: AsyncSession):
    """
    Test type inference of functions
    """

    query = parse(
        """
        SELECT
          AVG(id) OVER
           (PARTITION BY first_name ORDER BY last_name),
          AVG(CAST(id AS DECIMAL(8, 6))),
          AVG(CAST(id AS INTERVAL DAY TO SECOND)),
          STDDEV(id),
          stddev_samp(id),
          stddev_pop(id),
          variance(id),
          var_pop(id)
        FROM dbt.source.jaffle_shop.customers
    """,
    )
    exc = DJException()
    ctx = CompileContext(session=construction_session, exception=exc)
    await query.compile(ctx)
    types = [
        DoubleType(),
        DecimalType(12, 10),
        DayTimeIntervalType(),
        DoubleType(),
        DoubleType(),
        DoubleType(),
        DoubleType(),
        DoubleType(),
    ]
    assert types == [exp.type for exp in query.select.projection]  # type: ignore


@pytest.mark.asyncio
async def test_infer_types_min_max_sum_ceil(construction_session: AsyncSession):
    """
    Test type inference of functions
    """

    query = parse(
        """
        SELECT
          MIN(id) OVER
            (PARTITION BY first_name ORDER BY last_name),
          MAX(id) OVER
            (PARTITION BY first_name ORDER BY last_name),
          SUM(id) OVER
            (PARTITION BY first_name ORDER BY last_name),
          CEIL(id),
          PERCENT_RANK(id) OVER (PARTITION BY id ORDER BY id)
        FROM dbt.source.jaffle_shop.customers
    """,
    )
    exc = DJException()
    ctx = CompileContext(session=construction_session, exception=exc)
    await query.compile(ctx)
    types = [IntegerType(), IntegerType(), BigIntType(), BigIntType(), DoubleType()]
    assert types == [exp.type for exp in query.select.projection]  # type: ignore


@pytest.mark.asyncio
async def test_infer_types_count(construction_session: AsyncSession):
    """
    Test type inference of functions
    """

    query = parse(
        """
        SELECT
          COUNT(id) OVER
            (PARTITION BY first_name ORDER BY last_name),
          COUNT(DISTINCT last_name)
        FROM dbt.source.jaffle_shop.customers
    """,
    )
    exc = DJException()
    ctx = CompileContext(session=construction_session, exception=exc)
    await query.compile(ctx)
    types = [
        BigIntType(),
        BigIntType(),
    ]
    assert types == [exp.type for exp in query.select.projection]  # type: ignore


@pytest.mark.asyncio
async def test_infer_types_coalesce(construction_session: AsyncSession):
    """
    Test type inference of functions
    """

    query = parse(
        """
        SELECT
          COALESCE(5, NULL),
          COALESCE("random", NULL)
        FROM dbt.source.jaffle_shop.customers
    """,
    )
    exc = DJException()
    ctx = CompileContext(session=construction_session, exception=exc)
    await query.compile(ctx)
    types = [
        IntegerType(),
        StringType(),
    ]
    assert types == [exp.type for exp in query.select.projection]  # type: ignore


@pytest.mark.asyncio
async def test_infer_types_array_map(construction_session: AsyncSession):
    """
    Test type inference for arrays and maps
    """

    query = parse(
        """
        SELECT
          ARRAY(5, 6, 7, 8),
          MAP(1, 'a', 2, 'b', 3, 'c'),
          MAP(1.0, 'a', 2.0, 'b', 3.0, 'c'),
          MAP(CAST(1.0 AS DOUBLE), 'a', CAST(2.0 AS DOUBLE), 'b', CAST(3.0 AS DOUBLE), 'c')
        FROM dbt.source.jaffle_shop.customers
        """,
    )
    exc = DJException()
    ctx = CompileContext(session=construction_session, exception=exc)
    await query.compile(ctx)
    types = [
        ListType(IntegerType()),
        MapType(IntegerType(), StringType()),
        MapType(FloatType(), StringType()),
        MapType(DoubleType(), StringType()),
    ]
    assert types == [exp.type for exp in query.select.projection]  # type: ignore

    query = parse(
        """
        SELECT
          MAP(1, 'a', 2, 3, 'c')
        FROM dbt.source.jaffle_shop.customers
        """,
    )
    exc = DJException()
    ctx = CompileContext(session=construction_session, exception=exc)
    await query.compile(ctx)
    assert query.select.projection[0].type == MapType(  # type: ignore
        key_type=IntegerType(),
        value_type=StringType(),
    )

    query = parse(
        """
        SELECT
          ARRAY(1, 'a')
        FROM dbt.source.jaffle_shop.customers
        """,
    )
    exc = DJException()
    ctx = CompileContext(session=construction_session, exception=exc)
    await query.compile(ctx)
    with pytest.raises(DJParseException) as exc_info:
        query.select.projection[0].type  # type: ignore  # pylint: disable=pointless-statement
    assert "Multiple types int, string passed to array" in str(exc_info)


@pytest.mark.asyncio
async def test_infer_types_if(construction_session: AsyncSession):
    """
    Test type inference of IF
    """
    query = parse(
        """
        SELECT
          IF(1=2, 10, 20),
          IF(1=2, 'random', 20),
          IFNULL(1, 2, 3)
        FROM dbt.source.jaffle_shop.customers
        """,
    )
    exc = DJException()
    ctx = CompileContext(session=construction_session, exception=exc)
    await query.compile(ctx)
    assert query.select.projection[0].type == IntegerType()  # type: ignore
    with pytest.raises(DJException) as exc_info:
        query.select.projection[1].type  # type: ignore  # pylint: disable=pointless-statement
    assert (
        "The then result and else result must match in type! Got string and int"
        in str(exc_info)
    )
    assert query.select.projection[2].type == IntegerType()  # type: ignore


@pytest.mark.asyncio
async def test_infer_types_exp(construction_session: AsyncSession):
    """
    Test type inference of math functions
    """
    query = parse(
        """
        SELECT
          EXP(2),
          FLOOR(22.1),
          LENGTH('blah'),
          LEVENSHTEIN('a', 'b'),
          LN(5),
          LOG(10, 100),
          LOG2(2),
          LOG10(100),
          POW(1, 2),
          POWER(1, 2),
          ROUND(1.2, 0),
          ROUND(1.2, -1),
          ROUND(CAST(1.233 AS DOUBLE), 1),
          ROUND(CAST(1.233 AS DECIMAL(8, 6)), 20),
          CEIL(CAST(1.233 AS DECIMAL(8, 6))),
          SQRT(12)
        FROM dbt.source.jaffle_shop.customers
        """,
    )
    exc = DJException()
    ctx = CompileContext(session=construction_session, exception=exc)
    await query.compile(ctx)
    types = [
        DoubleType(),
        BigIntType(),
        IntegerType(),
        IntegerType(),
        DoubleType(),
        DoubleType(),
        DoubleType(),
        DoubleType(),
        DoubleType(),
        DoubleType(),
        IntegerType(),
        FloatType(),
        DoubleType(),
        DecimalType(precision=9, scale=6),
        DecimalType(precision=3, scale=0),
        DoubleType(),
    ]
    assert types == [exp.type for exp in query.select.projection]  # type: ignore
    assert query.select.projection[4].function().dialects == [  # type: ignore
        Dialect.SPARK,
        Dialect.DRUID,
    ]
    assert query.select.projection[5].function().dialects == [Dialect.SPARK]  # type: ignore
    assert query.select.projection[6].function().dialects == [Dialect.SPARK]  # type: ignore
    assert query.select.projection[7].function().dialects == [  # type: ignore
        Dialect.SPARK,
        Dialect.DRUID,
    ]


@pytest.mark.asyncio
async def test_infer_types_str(construction_session: AsyncSession):
    """
    Test type inference of EXP
    """
    query = parse(
        """
        SELECT
          LOWER('Extra'),
          SUBSTRING('e14', 1, 1),
          SUBSTRING('e14', 1, 1)
        FROM dbt.source.jaffle_shop.customers
        """,
    )
    exc = DJException()
    ctx = CompileContext(session=construction_session, exception=exc)
    await query.compile(ctx)
    types = [
        StringType(),
        StringType(),
        StringType(),
    ]
    assert types == [exp.type for exp in query.select.projection]  # type: ignore


def test_column_type_validation():
    """
    Test type inference of EXP
    """
    with pytest.raises(DJException) as exc_info:
        ColumnType.validate("decimal")
    assert "DJ does not recognize the type `decimal`" in str(exc_info)
    assert ColumnType.validate("decimal(10, 8)") == DecimalType(10, 8)

    assert ColumnType.validate("fixed(2)") == FixedType(2)
    assert ColumnType.validate("map<string, string>") == MapType(
        StringType(),
        StringType(),
    )
    assert ColumnType.validate("array<int>") == ListType(IntegerType())


@pytest.mark.asyncio
async def test_infer_types_datetime(construction_session: AsyncSession):
    """
    Test type inference of functions
    """

    query = parse(
        """
        SELECT
          CURRENT_DATE(),
          CURRENT_TIME(),
          CURRENT_TIMESTAMP(),

          NOW(),

          DATE_ADD('2020-01-01', 10),
          DATE_ADD(CURRENT_DATE(), 10),
          DATE_SUB('2020-01-01', 10),
          DATE_SUB(CURRENT_DATE(), 10),

          DATEDIFF('2020-01-01', '2021-01-01'),
          DATEDIFF(CURRENT_DATE(), CURRENT_DATE()),


          EXTRACT(YEAR FROM '2020-01-01 00:00:00'),
          EXTRACT(SECOND FROM '2020-01-01 00:00:00'),

          DAY("2022-01-01"),
          MONTH("2022-01-01"),
          WEEK("2022-01-01"),
          YEAR("2022-01-01")
        FROM dbt.source.jaffle_shop.customers
    """,
    )
    exc = DJException()
    ctx = CompileContext(session=construction_session, exception=exc)
    await query.compile(ctx)
    types = [
        DateType(),
        TimeType(),
        TimestampType(),
        TimestampType(),
        DateType(),
        DateType(),
        DateType(),
        DateType(),
        IntegerType(),
        IntegerType(),
        IntegerType(),
        DecimalType(precision=8, scale=6),
        IntegerType(),
        BigIntType(),
        BigIntType(),
        BigIntType(),
    ]
    assert types == [exp.type for exp in query.select.projection]  # type: ignore
