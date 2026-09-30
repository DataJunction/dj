"""
Property: a decomposed metric, rolled up from a finer grain, equals the metric
computed directly.

DJ's own decomposition (`MetricComponentExtractor._extract_base`) and component
rendering (`build_component_expression`) produce the SQL; DuckDB executes both
that SQL and the plain aggregate, and the two answers must agree.
"""

import duckdb
import pytest
import sqlglot
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from datajunction_server.construction.build_v3.decomposition import (
    build_component_expression,
)
from datajunction_server.sql.decompose import MetricComponentExtractor
from datajunction_server.sql.parsing.backends.antlr4 import parse
from tests.property.budget import examples
from tests.property.comparison import PropertyMismatch, same_value
from tests.property.known_issues import (
    COUNT_COMPONENT_NAME_COLLISION,
    COVARIANCE_COUNTS_HALF_NULL_ROWS,
    PRINTER_DROPS_PARENTHESES,
)

METRICS = [
    "SUM(x)",
    "MIN(x)",
    "MAX(x)",
    "COUNT(x)",
    "COUNT(*)",
    "COUNT_IF(x > 0)",
    "AVG(x)",
    "VAR_POP(x)",
    "VAR_SAMP(x)",
    "VARIANCE(x)",
    "STDDEV_POP(x)",
    "STDDEV_SAMP(x)",
    "STDDEV(x)",
    "COVAR_POP(x, y)",
    "COVAR_SAMP(x, y)",
    "CORR(x, y)",
]
BROKEN = {
    "VAR_SAMP(x)": PRINTER_DROPS_PARENTHESES,
    "VARIANCE(x)": PRINTER_DROPS_PARENTHESES,
    "STDDEV_SAMP(x)": PRINTER_DROPS_PARENTHESES,
    "STDDEV(x)": PRINTER_DROPS_PARENTHESES,
    "COVAR_POP(x, y)": COVARIANCE_COUNTS_HALF_NULL_ROWS,
    "COVAR_SAMP(x, y)": COVARIANCE_COUNTS_HALF_NULL_ROWS,
    "CORR(x, y)": COVARIANCE_COUNTS_HALF_NULL_ROWS,
}

values = st.one_of(st.none(), st.integers(-100, 100))
rows_strategy = st.lists(
    st.tuples(st.integers(0, 2), st.integers(0, 3), values, values),
    min_size=1,
    max_size=40,
)


def to_duckdb(spark_sql: str) -> str:
    return sqlglot.transpile(spark_sql, read="spark", write="duckdb")[0]


def decomposed_sql(metric: str) -> tuple[str, str]:
    """DJ's measures query (grain g, p) and its combiner rolled up to grain g."""
    components, derived = MetricComponentExtractor(0)._extract_base(
        parse(f"SELECT {metric} FROM t"),
    )
    measures = ", ".join(
        f"{build_component_expression(c)} AS {c.name}" for c in components
    )
    measures_sql = f"SELECT g, p, {measures} FROM t GROUP BY g, p"
    combiner = str(derived.select.projection[0])
    return measures_sql, f"SELECT g, {combiner} AS v FROM m GROUP BY g"


@pytest.fixture(scope="module")
def conn():
    with duckdb.connect(":memory:") as connection:
        yield connection


def check_rollup(conn, metric, column_type, rows):
    conn.execute(
        f"CREATE OR REPLACE TABLE t (g INTEGER, p INTEGER, "
        f"x {column_type}, y {column_type})",
    )
    conn.executemany("INSERT INTO t VALUES (?, ?, ?, ?)", rows)

    measures_sql, combine_sql = decomposed_sql(metric)
    conn.execute(f"CREATE OR REPLACE TABLE m AS {to_duckdb(measures_sql)}")
    rolled_up = dict(conn.execute(to_duckdb(combine_sql)).fetchall())
    direct = dict(
        conn.execute(f"SELECT g, {metric} FROM t GROUP BY g").fetchall(),
    )

    assert rolled_up.keys() == direct.keys()
    mismatches = {
        g: (rolled_up[g], direct[g])
        for g in direct
        if not same_value(rolled_up[g], direct[g])
    }
    if mismatches:
        raise PropertyMismatch(
            f"{metric}: (rolled-up, direct) by group = {mismatches}\n"
            f"measures: {measures_sql}\ncombine: {combine_sql}",
        )


@pytest.mark.parametrize("column_type", ["INTEGER", "DOUBLE"])
@pytest.mark.parametrize(
    "metric",
    [pytest.param(m, marks=BROKEN[m].skip()) if m in BROKEN else m for m in METRICS],
)
@settings(max_examples=examples(100))
@given(rows=rows_strategy)
def test_rollup_matches_direct_aggregate(conn, metric, column_type, rows):
    check_rollup(conn, metric, column_type, rows)


@PRINTER_DROPS_PARENTHESES.xfail()
def test_known_issue_sample_variance_rollup(conn):
    check_rollup(conn, "VAR_SAMP(x)", "INTEGER", [(0, 0, 0, None), (0, 0, 1, None)])


@COVARIANCE_COUNTS_HALF_NULL_ROWS.xfail()
def test_known_issue_covariance_with_one_side_null(conn):
    check_rollup(conn, "COVAR_POP(x, y)", "INTEGER", [(0, 0, 1, None), (0, 0, 0, 1)])


def components_of(metric: str):
    components, _ = MetricComponentExtractor(0)._extract_base(
        parse(f"SELECT {metric} FROM t"),
    )
    return components


def check_component_names(first: str, second: str, *, skip_known: bool) -> None:
    by_name = {c.name: c for c in components_of(first)}
    for component in components_of(second):
        other = by_name.get(component.name)
        if other is None:
            continue
        if skip_known:
            spelled_differently = {other.aggregation, component.aggregation} == {
                "COUNT",
                f"COUNT({component.expression})",
            }
            assume(not spelled_differently)
        if (other.expression, other.aggregation, other.merge) != (
            component.expression,
            component.aggregation,
            component.merge,
        ):
            raise PropertyMismatch(
                f"`{component.name}`: from {first} it is aggregation="
                f"{other.aggregation!r} over {other.expression!r}; from {second} it is "
                f"aggregation={component.aggregation!r} over {component.expression!r}",
            )


@settings(max_examples=examples(200))
@given(first=st.sampled_from(METRICS), second=st.sampled_from(METRICS))
def test_same_component_name_means_same_component(first, second):
    """
    Frozen measures are keyed by component name, so two metrics on the same
    parent that produce a component with the same name must define it the same
    way, or registering the second metric's measures fails.
    """
    check_component_names(first, second, skip_known=True)


@COUNT_COMPONENT_NAME_COLLISION.xfail()
def test_known_issue_count_component_collision():
    check_component_names("COUNT(x)", "COVAR_POP(x, y)", skip_known=False)
