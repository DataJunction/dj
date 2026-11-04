"""
Property: DJ's parser and printer preserve what an expression means.

Random expressions are parsed and printed by DJ, then the original and DJ's
version are both evaluated on DuckDB over random rows. Any difference in the
results means DJ changed the query's meaning.
"""

import re

import duckdb
import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from datajunction_server.sql.parsing.backends.antlr4 import parse
from tests.property.budget import examples

COLUMNS = ["a", "b", "c"]

numeric_leaf = st.one_of(
    st.sampled_from(COLUMNS),
    st.integers(-5, 5).map(str),
    st.just("NULL"),
)


def numeric_branches(children):
    return st.one_of(
        st.tuples(children, st.sampled_from(["+", "-", "*"]), children).map(
            lambda t: f"{t[0]} {t[1]} {t[2]}",
        ),
        children.map(lambda e: f"- {e}" if e.startswith("-") else f"-{e}"),
        children.map(lambda e: f"- {e}"),
        children.map(lambda e: f"({e})"),
        st.tuples(children, children).map(lambda t: f"COALESCE({t[0]}, {t[1]})"),
        children.map(lambda e: f"ABS({e})"),
        st.tuples(boolean_expr(children), children, children).map(
            lambda t: f"CASE WHEN {t[0]} THEN {t[1]} ELSE {t[2]} END",
        ),
        children.map(lambda e: f"CAST({e} AS BIGINT)"),
    )


def boolean_expr(numeric):
    comparison = st.tuples(
        numeric,
        st.sampled_from(["=", "<>", "<", "<=", ">", ">="]),
        numeric,
    ).map(lambda t: f"{t[0]} {t[1]} {t[2]}")
    atoms = st.one_of(
        comparison,
        numeric.map(lambda e: f"{e} IS NULL"),
        numeric.map(lambda e: f"{e} IS NOT NULL"),
        st.tuples(numeric, numeric, numeric).map(
            lambda t: f"{t[0]} BETWEEN {t[1]} AND {t[2]}",
        ),
        st.tuples(numeric, st.lists(numeric, min_size=1, max_size=3)).map(
            lambda t: f"{t[0]} IN ({', '.join(t[1])})",
        ),
    )
    return st.recursive(
        atoms,
        lambda inner: st.one_of(
            st.tuples(inner, st.sampled_from(["AND", "OR"]), inner).map(
                lambda t: f"{t[0]} {t[1]} {t[2]}",
            ),
            inner.map(lambda e: f"NOT {e}"),
            inner.map(lambda e: f"({e})"),
        ),
        max_leaves=4,
    )


numeric_expr = st.recursive(numeric_leaf, numeric_branches, max_leaves=6)

string_leaf = st.one_of(
    st.just("s"),
    st.sampled_from(["''", "'x'", "'it''s'", "'a%'", "'%_'", "'\\\\'"]),
)
string_expr = st.recursive(
    string_leaf,
    lambda inner: st.one_of(
        st.tuples(inner, inner).map(lambda t: f"CONCAT({t[0]}, {t[1]})"),
        inner.map(lambda e: f"UPPER({e})"),
        st.tuples(boolean_expr(numeric_leaf), inner, inner).map(
            lambda t: f"CASE WHEN {t[0]} THEN {t[1]} ELSE {t[2]} END",
        ),
    ),
    max_leaves=4,
)
string_predicates = st.one_of(
    st.tuples(string_expr, string_expr).map(lambda t: f"{t[0]} = {t[1]}"),
    st.tuples(string_expr, string_expr).map(lambda t: f"{t[0]} LIKE {t[1]}"),
    st.tuples(string_expr, string_expr).map(lambda t: f"{t[0]} NOT LIKE {t[1]}"),
)
multi_case = st.tuples(
    st.lists(
        st.tuples(boolean_expr(numeric_leaf), numeric_leaf),
        min_size=1,
        max_size=3,
    ),
    st.one_of(st.none(), numeric_leaf),
).map(
    lambda t: (
        "CASE "
        + " ".join(f"WHEN {w} THEN {v}" for w, v in t[0])
        + (f" ELSE {t[1]}" if t[1] is not None else "")
        + " END"
    ),
)

expressions = st.one_of(
    numeric_expr,
    boolean_expr(numeric_expr),
    string_expr,
    string_predicates,
    multi_case,
)

# DJ prints `-(-x)` as `--x`, which SQL reads as a comment
# (known_issues.PRINTER_DROPS_PARENTHESES); skipped so it doesn't mask other
# differences.
KNOWN_DOUBLE_MINUS = "--"

rows = st.lists(
    st.tuples(
        *[st.one_of(st.none(), st.integers(-3, 3))] * len(COLUMNS),
        st.one_of(st.none(), st.sampled_from(["", "x", "it's", "a%", "\\", "X"])),
    ),
    min_size=1,
    max_size=6,
)


def evaluate(conn, expression: str):
    return conn.execute(f"SELECT {expression} FROM t ORDER BY rowid").fetchall()


@pytest.fixture(scope="module")
def conn():
    with duckdb.connect(":memory:") as connection:
        yield connection


def dj_roundtrip(expression: str) -> str:
    query = parse(f"SELECT {expression} FROM t")
    return str(query.select.projection[0])


@settings(max_examples=examples(1000))
@given(expression=expressions, data=rows)
def test_dj_printing_preserves_meaning(conn, expression, data):
    conn.execute(
        "CREATE OR REPLACE TABLE t (a INTEGER, b INTEGER, c INTEGER, s VARCHAR)",
    )
    conn.executemany("INSERT INTO t VALUES (?, ?, ?, ?)", data)
    try:
        expected = evaluate(conn, expression)
    except duckdb.Error:
        assume(False)

    printed = dj_roundtrip(expression)
    assume(KNOWN_DOUBLE_MINUS not in printed)
    try:
        actual = evaluate(conn, printed)
    except duckdb.Error as exc:
        raise AssertionError(
            f"DJ's version no longer runs.\n  original: {expression}\n"
            f"  DJ:       {printed}\n  error:    {exc}",
        ) from exc
    assert actual == expected, (
        f"\n  original: {expression}\n  DJ:       {printed}\n"
        f"  expected: {expected}\n  got:      {actual}"
    )


def without_negative_zero(sql: str) -> str:
    # DJ reads the literal `-0` as 0; the value is the same either way.
    return re.sub(r"(?<![\w.])-0(?![\w.])", "0", sql)


@settings(max_examples=examples(1000))
@given(expression=expressions)
def test_dj_printing_is_stable(expression):
    once = dj_roundtrip(expression)
    assume(KNOWN_DOUBLE_MINUS not in once)
    assert without_negative_zero(dj_roundtrip(once)) == without_negative_zero(once)
