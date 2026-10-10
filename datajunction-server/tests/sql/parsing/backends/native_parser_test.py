"""
The prototype native parser (ANTLR C++ runtime) must give the same DJ AST as the
Python parser. Skipped unless `native_parser/build.sh` has been run.
"""
# mypy: ignore-errors

import sys
from pathlib import Path

import pytest

NATIVE = Path(__file__).resolve().parents[4] / "native_parser"
sys.path[:0] = [str(NATIVE), str(NATIVE / "build")]
pytest.importorskip("dj_native_parse")

from datajunction_server.sql.parsing.backends.antlr4 import (  # noqa: E402
    parse_sql_with_sll_fallback,
    visit,
)
from datajunction_server.sql.parsing.structural import serialize_ast  # noqa: E402
from native_bridge import native_tree  # noqa: E402

QUERIES = Path(__file__).resolve().parents[1] / "queries"
RULE = "singleStatement"


def python_ast(sql):
    return visit(parse_sql_with_sll_fallback(sql, RULE))


def native_ast(sql):
    return visit(native_tree(sql, RULE))


def test_matches_the_python_parser_on_the_test_queries():
    files = sorted(QUERIES.rglob("*.sql"))
    assert files
    compared = 0
    for path in files:
        sql = path.read_text()
        try:
            expected = python_ast(sql)
        except Exception:  # not valid for the Python parser either
            with pytest.raises(Exception):
                native_ast(sql)
            continue
        actual = native_ast(sql)
        assert str(actual) == str(expected), path.name
        assert serialize_ast(actual, version=1) == serialize_ast(expected, version=1), (
            path.name
        )
        compared += 1
    assert compared > 100


@pytest.mark.parametrize(
    "sql",
    [
        "select Foo as Bar, a + b * 2 from Some_Table t where x in (1, 2) and y is not null",
        "SELECT CASE WHEN a > 1 THEN 'x' ELSE 'y' END, CAST(z AS DECIMAL(10, 2)) FROM t",
        "WITH c AS (SELECT 1 AS one) SELECT * FROM c JOIN d ON c.one = d.one",
        "SELECT a, SUM(b) OVER (PARTITION BY a ORDER BY c ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
    ],
)
def test_matches_on_typical_queries(sql):
    assert str(native_ast(sql)) == str(python_ast(sql))


@pytest.mark.parametrize("sql", ["SELECT 1 +", "select * from (", "SELEC 1"])
def test_rejects_invalid_sql_like_the_python_parser(sql):
    with pytest.raises(Exception):
        python_ast(sql)
    with pytest.raises(Exception):
        native_ast(sql)
