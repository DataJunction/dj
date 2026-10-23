"""
A role-qualified filter is pushed into an inner scope only through a link with
that same role, and comes out as a plain column.

The scope resolver registered every dimension link under the role-less name, so
`dateint[allocation_snapshot]` matched a source linked to the same dimension
under another role (`measure`), and only the inner column was replaced, leaving
`utc_date[allocation_snapshot]` in the SQL for the engine to reject.
"""

from types import SimpleNamespace
from typing import Any, cast

from datajunction_server.construction.build_v3.cte import (
    _populate_scope_column_aliases,
    _rewrite_filter_for_scope,
)
from datajunction_server.database.node import NodeType
from datajunction_server.sql.parsing.backends.antlr4 import parse


def scope_for_source_linked_with_role(role: str | None):
    link = SimpleNamespace(
        role=role,
        foreign_keys_reversed={"ns.date.dateint": "ns.impressions.utc_date"},
    )
    node = SimpleNamespace(
        type=NodeType.SOURCE,
        current=SimpleNamespace(dimension_links=[link], columns=[]),
    )
    from_ = parse("SELECT 1 FROM ns.impressions AS impressions").select.from_
    assert from_ is not None
    table = from_.relations[0].primary
    scope: dict[str, tuple[str, str]] = {}
    _populate_scope_column_aliases(
        table,
        scope,
        cast(Any, SimpleNamespace(nodes={"ns.impressions": node})),
        {},
        {},
    )
    return scope


def pushed(filter_str: str, scope) -> str | None:
    rewritten = _rewrite_filter_for_scope(filter_str, {}, scope)
    return None if rewritten is None else str(rewritten)


def test_roled_filter_resolves_through_the_link_with_that_role():
    scope = scope_for_source_linked_with_role("measure")
    assert (
        pushed("ns.date.dateint[measure] BETWEEN 20261005 AND 20261103", scope)
        == "impressions.utc_date BETWEEN 20261005 AND 20261103"
    )


def test_roled_filter_is_not_pushed_through_a_link_with_another_role():
    scope = scope_for_source_linked_with_role("measure")
    assert pushed("ns.date.dateint[allocation_snapshot] = 20261019", scope) is None


def test_filter_with_an_unresolved_role_is_not_pushed_even_if_another_part_resolves():
    scope = scope_for_source_linked_with_role("measure")
    assert (
        pushed(
            "ns.date.dateint[measure] >= 1 AND ns.date.dateint[allocation_snapshot] = 2",
            scope,
        )
        is None
    )


def test_filter_without_a_role_still_resolves_through_a_linked_source():
    scope = scope_for_source_linked_with_role(None)
    assert (
        pushed("ns.date.dateint = 20261019", scope) == "impressions.utc_date = 20261019"
    )
