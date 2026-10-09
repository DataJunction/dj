"""
Property: an expression tree built in code prints as SQL that means the same
thing.

DJ assembles SQL by constructing AST nodes (metric combiners, combined filters,
derived-metric substitution). This property covers expression trees whose
operators have unambiguous precedence while the known nested-parentheses bug
has a separate, fixed repro. Each tree is rendered by DJ and by a reference
renderer that parenthesizes every sub-expression, then evaluated on DuckDB.
"""

import duckdb
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from datajunction_server.sql.parsing import ast
from tests.property.budget import examples
from tests.property.comparison import PropertyMismatch
from tests.property.known_issues import PRINTER_DROPS_PARENTHESES


class Expr:
    """A generated expression: its DJ AST and its fully parenthesized SQL."""

    def __init__(self, build, reference: str):
        self.build = build  # fresh AST on each call; nodes carry parent links
        self.reference = reference

    def __repr__(self) -> str:
        return self.reference


def column(name):
    return Expr(lambda: ast.Column(ast.Name(name)), name)


def number(value):
    return Expr(lambda: ast.Number(value), f"({value})" if value < 0 else str(value))


NULL = Expr(lambda: ast.Null(), "NULL")


def binary(op: ast.BinaryOpKind, left: Expr, right: Expr) -> Expr:
    return Expr(
        lambda: ast.BinaryOp(op=op, left=left.build(), right=right.build()),
        f"({left.reference} {op.value} {right.reference})",
    )


def negate(e: Expr) -> Expr:
    return Expr(
        lambda: ast.ArithmeticUnaryOp(
            op=ast.ArithmeticUnaryOpKind.Minus,
            expr=e.build(),
        ),
        f"(-{e.reference})",
    )


def not_(e: Expr) -> Expr:
    return Expr(
        lambda: ast.UnaryOp(op=ast.UnaryOpKind.Not, expr=e.build()),
        f"(NOT {e.reference})",
    )


def is_null(e: Expr, negated: bool) -> Expr:
    return Expr(
        lambda: ast.IsNull(expr=e.build(), negated=negated),
        f"({e.reference} IS {'NOT ' if negated else ''}NULL)",
    )


def between(e: Expr, low: Expr, high: Expr, negated: bool) -> Expr:
    return Expr(
        lambda: ast.Between(
            expr=e.build(),
            low=low.build(),
            high=high.build(),
            negated=negated,
        ),
        f"({e.reference} {'NOT ' if negated else ''}BETWEEN "
        f"{low.reference} AND {high.reference})",
    )


def function(name: str, *args: Expr) -> Expr:
    return Expr(
        lambda: ast.Function(ast.Name(name), args=[a.build() for a in args]),
        f"{name}({', '.join(a.reference for a in args)})",
    )


def case(condition: Expr, result: Expr, otherwise: Expr) -> Expr:
    return Expr(
        lambda: ast.Case(
            conditions=[condition.build()],
            results=[result.build()],
            else_result=otherwise.build(),
        ),
        f"(CASE WHEN {condition.reference} THEN {result.reference} "
        f"ELSE {otherwise.reference} END)",
    )


ARITHMETIC = [ast.BinaryOpKind.Plus, ast.BinaryOpKind.Minus, ast.BinaryOpKind.Multiply]
COMPARISON = [
    ast.BinaryOpKind.Eq,
    ast.BinaryOpKind.NotEq,
    ast.BinaryOpKind.Lt,
    ast.BinaryOpKind.GtEq,
]
LOGICAL = [ast.BinaryOpKind.And, ast.BinaryOpKind.Or]

numeric_leaves = st.one_of(
    st.sampled_from(["a", "b", "c"]).map(column),
    st.integers(0, 3).map(number),
    st.just(NULL),
)


base_comparison = st.builds(
    binary,
    st.sampled_from(COMPARISON),
    numeric_leaves,
    numeric_leaves,
)
numeric_atoms = st.one_of(
    numeric_leaves,
    st.builds(function, st.just("COALESCE"), numeric_leaves, numeric_leaves),
    st.builds(case, base_comparison, numeric_leaves, numeric_leaves),
)
boolean_atoms = st.one_of(
    st.builds(binary, st.sampled_from(COMPARISON), numeric_atoms, numeric_atoms),
    st.builds(is_null, numeric_atoms, st.booleans()),
    st.builds(between, numeric_atoms, numeric_leaves, numeric_leaves, st.booleans()),
)
expressions = st.one_of(
    numeric_atoms,
    st.builds(binary, st.sampled_from(ARITHMETIC), numeric_atoms, numeric_atoms),
    numeric_atoms.map(negate),
    boolean_atoms,
    st.builds(binary, st.sampled_from(LOGICAL), boolean_atoms, boolean_atoms),
    boolean_atoms.map(not_),
)


rows = st.lists(
    st.tuples(*[st.one_of(st.none(), st.integers(-3, 3))] * 3),
    min_size=1,
    max_size=6,
)


def evaluate(conn, sql: str):
    return conn.execute(f"SELECT {sql} FROM t ORDER BY rowid").fetchall()


@pytest.fixture(scope="module")
def conn():
    with duckdb.connect(":memory:") as connection:
        yield connection


@settings(max_examples=examples(500))
@given(expression=expressions, data=rows)
def test_printed_tree_means_the_same_as_the_tree(conn, expression, data):
    conn.execute("CREATE OR REPLACE TABLE t (a INTEGER, b INTEGER, c INTEGER)")
    conn.executemany("INSERT INTO t VALUES (?, ?, ?)", data)
    expected = evaluate(conn, expression.reference)

    printed = str(expression.build())
    try:
        actual = evaluate(conn, printed)
    except duckdb.Error as exc:
        raise AssertionError(
            f"DJ's SQL no longer runs.\n  tree: {expression.reference}\n"
            f"  DJ:   {printed}\n  error: {exc}",
        ) from exc
    assert actual == expected, (
        f"\n  tree:     {expression.reference}\n  DJ:       {printed}\n"
        f"  expected: {expected}\n  got:      {actual}"
    )


@PRINTER_DROPS_PARENTHESES.xfail()
def test_known_issue_nested_negation_prints_as_comment(conn):
    conn.execute("CREATE OR REPLACE TABLE t (a INTEGER, b INTEGER, c INTEGER)")
    conn.execute("INSERT INTO t VALUES (1, 2, 3)")
    expression = negate(negate(column("a")))
    expected = evaluate(conn, expression.reference)
    printed = str(expression.build())
    try:
        actual = evaluate(conn, printed)
    except duckdb.Error as exc:
        if printed.startswith("--"):
            raise PropertyMismatch(
                f"DJ printed {printed!r} for {expression!r}",
            ) from exc
        raise
    assert actual == expected
