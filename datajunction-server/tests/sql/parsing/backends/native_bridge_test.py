"""
The native parser bridge, tested without the compiled extension: a fake
`dj_native_parse` returns flat arrays built from a Python-parsed tree.
"""
# mypy: ignore-errors

from array import array

import pytest
from antlr4 import ParserRuleContext
from antlr4.Token import Token
from antlr4.tree.Tree import TerminalNode

from datajunction_server.sql.parsing.backends import native
from datajunction_server.sql.parsing.backends.antlr4 import (
    SqlSyntaxError,
    build_string_parser,
    parse_sql_with_sll_fallback,
    visit,
)

RULE = "singleStatement"
SQL = "SELECT a, b + 1 AS c FROM t WHERE x IN (1, 2) AND y IS NOT NULL"
BASE_ATTRS = set(dir(ParserRuleContext(None, 0))) | {"parser"}


def flat_arrays(sql, rule):
    """Flatten the Python parse tree into the arrays the extension returns."""
    parser = build_string_parser(sql)
    tree = getattr(parser, rule)()
    tokens = parser.getTokenStream().tokens
    ids = {}
    order = []

    def walk(node):
        ids[id(node)] = len(order)
        order.append(node)
        for child in getattr(node, "children", None) or []:
            walk(child)

    walk(tree)
    names, kind, parent, tok, stt, stp = [], [], [], [], [], []
    for node in order:
        parent_ctx = node.parentCtx if hasattr(node, "parentCtx") else None
        parent.append(ids[id(parent_ctx)] if parent_ctx is not None else -1)
        if isinstance(node, TerminalNode):
            kind.append(-1)
            tok.append(node.symbol.tokenIndex)
            stt.append(-1)
            stp.append(-1)
            continue
        name = type(node).__name__
        if name not in names:
            names.append(name)
        kind.append(names.index(name))
        tok.append(-1)
        stt.append(node.start.tokenIndex if node.start else -1)
        stp.append(node.stop.tokenIndex if node.stop else -1)

    label_names, label_ids = [], {}
    owner, label, target = [], [], []

    def add_label(node, attr, value):
        is_list = isinstance(value, list)
        key = (attr, is_list)
        if key not in label_ids:
            label_ids[key] = len(label_names)
            label_names.append(key)
        for item in value if is_list else [value]:
            owner.append(ids[id(node)])
            label.append(label_ids[key])
            if isinstance(item, Token):
                target.append(-item.tokenIndex - 1)
            else:
                target.append(ids[id(item)])

    for node in order:
        if isinstance(node, TerminalNode):
            continue
        for attr in dir(node):
            if attr.startswith("_") or attr in BASE_ATTRS:
                continue
            value = getattr(node, attr, None)
            if isinstance(value, (Token, ParserRuleContext)) or (
                isinstance(value, list)
                and value
                and all(isinstance(v, (Token, ParserRuleContext)) for v in value)
            ):
                if not callable(value):
                    add_label(node, attr, value)

    def ints(values):
        return array("i", values).tobytes()

    return (
        names,
        ints(kind),
        ints(parent),
        ints(tok),
        ints(stt),
        ints(stp),
        ints(t.type for t in tokens),
        ints(t.start for t in tokens),
        ints(t.stop for t in tokens),
        ints(t.line for t in tokens),
        ints(t.column for t in tokens),
        ints(owner),
        ints(label),
        ints(target),
        label_names,
    )


class FakeExtension:
    """Stands in for the compiled `dj_native_parse` module."""

    @staticmethod
    def parse(sql, rule):
        if "boom" in sql:
            raise RuntimeError("line 1:0 boom")
        return flat_arrays(sql, rule)


@pytest.fixture
def fake_extension(monkeypatch):
    monkeypatch.setattr(native, "dj_native_parse", FakeExtension)


def python_ast(sql):
    return visit(parse_sql_with_sll_fallback(sql, RULE))


@pytest.mark.parametrize(
    "sql",
    [
        SQL,
        "SELECT CAST(id AS INTERVAL DAY TO SECOND) FROM t",
        "WITH c AS (SELECT 1 AS one) SELECT * FROM c JOIN d ON c.one = d.one",
    ],
)
def test_native_tree_rebuilds_the_python_tree(fake_extension, sql):
    expected = python_ast(sql)
    assert str(visit(native.native_tree(sql, RULE))) == str(expected)


def test_native_tree_wraps_extension_errors(fake_extension):
    with pytest.raises(SqlSyntaxError) as exc:
        native.native_tree("boom", RULE)
    assert str(exc.value) == "line 1:0 boom"


def test_hook_uses_the_native_tree_when_enabled(fake_extension, monkeypatch):
    monkeypatch.setattr(native, "ENABLED", True)
    calls = []
    real = native.native_tree
    monkeypatch.setattr(
        native,
        "native_tree",
        lambda sql, rule: calls.append(sql) or real(sql, rule),
    )
    tree = parse_sql_with_sll_fallback(SQL, RULE)
    assert calls == [SQL]
    assert str(visit(tree)) == str(python_ast_disabled(monkeypatch, SQL))
    assert parse_sql_with_sll_fallback(SQL, RULE, converter=lambda t: "done") == "done"


def python_ast_disabled(monkeypatch, sql):
    monkeypatch.setattr(native, "ENABLED", False)
    try:
        return python_ast(sql)
    finally:
        monkeypatch.setattr(native, "ENABLED", True)


def test_hook_falls_back_to_python_parser_on_native_failure(
    fake_extension,
    monkeypatch,
):
    monkeypatch.setattr(native, "ENABLED", True)
    with pytest.raises(Exception) as exc:  # invalid SQL, and native raises too
        parse_sql_with_sll_fallback("boom", RULE)
    assert not isinstance(exc.value, RuntimeError)
    monkeypatch.setattr(native, "native_tree", lambda sql, rule: None)
    assert str(visit(parse_sql_with_sll_fallback(SQL, RULE))) == str(
        python_ast_disabled(monkeypatch, SQL),
    )


def test_native_tree_handles_missing_bounds_and_list_labels(monkeypatch):
    def parse(sql, rule):
        out = list(flat_arrays(sql, rule))
        out[4] = array("i", [-1] * (len(out[4]) // 4)).tobytes()
        out[5] = out[4]
        out[11] = array("i", [0, 0]).tobytes()
        out[12] = array("i", [0, 0]).tobytes()
        out[13] = array("i", [1, 2]).tobytes()
        out[14] = [("extras", True)]
        return tuple(out)

    monkeypatch.setattr(
        native,
        "dj_native_parse",
        type("Fake", (), {"parse": staticmethod(parse)}),
    )
    root = native.native_tree(SQL, RULE)
    assert root.start is None
    assert root.stop is None
    assert len(root.extras) == 2
    assert [type(n).__name__ for n in root.extras] == [
        "StatementDefaultContext",
        "QueryContext",
    ]
