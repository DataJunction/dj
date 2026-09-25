"""
Declared child slots must agree with what reflective traversal would produce.
"""

import pathlib

import pytest

from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import (
    SqlParsingError,
    parse,
)
from datajunction_server.sql.parsing.backends.exceptions import DJParseException

TPCDS = pathlib.Path(__file__).parent / "queries" / "tpcds" / "sparksql"


def reflective_children(node: ast.Node) -> list:
    """
    Children derived from dataclass fields — the behaviour before declaring.
    """
    return list(
        node.fields(
            flat=True,
            nodes_only=True,
            obfuscated=False,
            nones=False,
            named=False,
        ),
    )


def walk(node: ast.Node):
    """
    Walk the tree using reflective children, independent of what is tested.
    """
    yield node
    for child in reflective_children(node):
        yield from walk(child)


def node_classes() -> list[type]:
    """Every Node subclass currently imported."""
    seen: set[type] = set()
    stack = [ast.Node]
    while stack:
        for sub in stack.pop().__subclasses__():
            if sub not in seen:
                seen.add(sub)
                stack.append(sub)
    return sorted(seen, key=lambda cls: cls.__name__)


def test_every_node_class_declares_child_fields():
    """A class without a declaration would fall back and traverse nothing."""
    missing = [
        cls.__name__ for cls in node_classes() if not hasattr(cls, "__child_fields__")
    ]
    assert missing == []


def test_declared_fields_are_real_public_fields():
    """Declared names must be actual, public dataclass fields of the class."""
    from dataclasses import fields as dataclass_fields  # noqa: PLC0415

    problems = {}
    for cls in node_classes():
        try:
            valid = {f.name for f in dataclass_fields(cls)}
        except TypeError:  # not a dataclass
            continue
        declared = set(cls.__child_fields__)
        unknown = declared - valid
        private = {name for name in declared if name.startswith("_")}
        if unknown or private:
            problems[cls.__name__] = {"unknown": unknown, "private": private}
    assert problems == {}


def test_declared_order_follows_field_order():
    """Traversal order is observable, so declarations keep dataclass order."""
    from dataclasses import fields as dataclass_fields  # noqa: PLC0415

    wrong = []
    for cls in node_classes():
        try:
            order = [f.name for f in dataclass_fields(cls)]
        except TypeError:
            continue
        declared = list(cls.__child_fields__)
        if declared != [name for name in order if name in set(declared)]:
            wrong.append(cls.__name__)
    assert wrong == []


@pytest.mark.parametrize(
    "query_file",
    sorted(TPCDS.glob("*.sql")),
    ids=lambda path: path.stem,
)
def test_declared_children_match_reflective(query_file):
    """
    On real SQL, declared traversal must yield exactly the reflective result.

    Same nodes, same order, same identities.
    """
    try:
        tree = parse(query_file.read_text())
    except (SqlParsingError, DJParseException) as exc:
        # A few TPC-DS files hold several statements, which DJ cannot parse.
        # That is a parser limit, not a traversal question.
        pytest.skip(f"{query_file.stem} does not parse: {exc}"[:120])
    checked = 0
    for node in walk(tree):
        assert list(node.children) == reflective_children(node), (
            f"{type(node).__name__} children diverge"
        )
        checked += 1
    assert checked > 0
