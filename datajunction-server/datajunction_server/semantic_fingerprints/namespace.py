"""Renders specs without the namespace they were deployed into."""

from __future__ import annotations

import re
from collections.abc import Iterable
from functools import lru_cache
from typing import TYPE_CHECKING, Any

from datajunction_server.utils import SEPARATOR

if TYPE_CHECKING:
    from datajunction_server.models.deployment import NodeSpec

NAMESPACE_MARKER = "__namespace__"


@lru_cache(maxsize=4096)
def parse_query(query: str):
    """Parses a query once per distinct text."""
    from datajunction_server.sql.parsing.backends import antlr4

    return antlr4.parse(query)


# Single-quoted strings, where '' is an escaped quote, are kept as written.
# Comments and quoted names are matched first so an apostrophe inside them
# is not read as the start of a string.
_SQL_TOKEN = re.compile(
    r'(?P<other>--[^\n]*|/\*.*?\*/|"(?:[^"]|"")*"|`[^`]*`)'
    r"|(?P<string>'(?:[^']|'')*')",
    re.DOTALL,
)
# Fields holding SQL text, where quoted strings are values, not names.
EXPRESSION_KEYS = frozenset({"query", "join_on", "filters", "default"})
# Fields naming warehouse tables, which no deploy namespace changes.
PHYSICAL_KEYS = frozenset({"catalog", "schema_", "table"})


def namespace_free_name(text: str, namespace: str) -> str:
    """Replaces a leading namespace in node names with a fixed marker."""
    if not namespace:
        return text
    return re.sub(
        rf"(?<![\w.]){re.escape(namespace)}\.",
        f"{NAMESPACE_MARKER}.",
        text,
    )


def _outside_strings(text: str, namespace: str) -> str:
    result = []
    position = 0
    for token in _SQL_TOKEN.finditer(text):
        result.append(namespace_free_name(text[position : token.start()], namespace))
        is_string = token.group("string") is not None
        result.append(
            token.group()
            if is_string
            else namespace_free_name(token.group(), namespace),
        )
        position = token.end()
    result.append(namespace_free_name(text[position:], namespace))
    return "".join(result)


def _rewrite(value: Any, namespace: str, key: str | None = None) -> Any:
    if isinstance(value, str):
        if key in PHYSICAL_KEYS:
            return value
        if key in EXPRESSION_KEYS:
            return _outside_strings(value, namespace)
        return namespace_free_name(value, namespace)
    if isinstance(value, dict):
        return {
            namespace_free_name(name, namespace): _rewrite(item, namespace, name)
            for name, item in value.items()
        }
    if isinstance(value, list):
        return [_rewrite(item, namespace, key) for item in value]
    return value


def own_namespace(spec: NodeSpec) -> str:
    """
    The namespace a spec was deployed into.

    Specs loaded from the database have no namespace set, so use the one
    in the node's name.
    """
    return spec.namespace or spec.rendered_name.rpartition(SEPARATOR)[0]


def namespace_free_spec(spec: NodeSpec, fields: Iterable[str]) -> NodeSpec:
    """
    Renders a spec with its own namespace replaced by a fixed marker.

    Only node names change. Quoted values and warehouse table names, and
    names in other namespaces, are left as written.
    """
    rendered = spec.rendered_spec()
    namespace = own_namespace(spec)
    if not namespace:
        return rendered
    values = rendered.model_dump(mode="json")
    for field in fields:
        values[field] = _rewrite(values[field], namespace, field)
    result = type(rendered).model_validate(values)
    result.namespace = None
    if result.rendered_query:
        result._query_ast = parse_query(result.rendered_query)
    return result
