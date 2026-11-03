"""Request predicates belong to a consumer, not every matching table scan."""

import sqlite3
from copy import deepcopy
from types import SimpleNamespace

import pytest

from datajunction_server.construction.build_v3.filter_placement import place_filters
from datajunction_server.construction.build_v3.dimensions import (
    _resolve_filter_only_dim,
    parse_dimension_ref,
)
from datajunction_server.construction.build_v3.measures import build_select_ast
from datajunction_server.construction.build_v3.types import (
    BuildContext,
    PushdownFilters,
)
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.parsing.backends.antlr4 import parse


def context(bindings=None):
    return BuildContext(
        session=None,
        metrics=[],
        dimensions=[],
        nodes={
            "fact": SimpleNamespace(
                name="fact",
                current=SimpleNamespace(columns=[SimpleNamespace(name="date_id")]),
            ),
        },
        upstream_filter_bindings=bindings or {},
    )


def request(filters=None):
    return PushdownFilters(
        filters=filters or ["date.date_id = 20240101"],
        column_aliases={"date.date_id": "date_id"},
        parent_node_name="fact",
    )


def rows(query):
    with sqlite3.connect(":memory:") as db:
        db.executescript(
            "CREATE TABLE orders(amount INTEGER, date_id INTEGER);"
            "INSERT INTO orders VALUES (10, 20240101), (5, 20230101);",
        )
        return db.execute(str(query)).fetchall()


def test_outer_filter_preserves_all_time_scalar_count():
    query = parse(
        "SELECT o.date_id, (SELECT COUNT(*) FROM orders) AS total FROM orders o",
    )
    assert place_filters(context(), {"fact": query}, request()) == {
        "date.date_id = 20240101",
    }
    assert rows(query) == [(20240101, 2)]


@pytest.mark.parametrize("join, owner", [("LEFT", "b"), ("RIGHT", "a"), ("FULL", "b")])
@pytest.mark.parametrize(
    "predicate, expected",
    [("= 20240101", [(20240101,)]), ("IS NULL", [(None,)])],
)
def test_local_filter_constrains_outer_join_output(join, owner, predicate, expected):
    query = parse(
        f"SELECT {owner}.date_id FROM orders a {join} JOIN orders b "
        "ON a.amount = b.amount AND a.date_id = 20240101 AND b.date_id = 20240101",
    )
    place_filters(context(), {"fact": query}, request([f"date.date_id {predicate}"]))
    assert rows(query) == expected


@pytest.mark.parametrize(
    "body",
    [
        "SELECT o.amount + s.total FROM orders o CROSS JOIN "
        "(SELECT SUM(amount) AS total FROM orders) s",
        "SELECT a.amount FROM orders a JOIN orders b ON a.date_id = b.date_id",
        "SELECT amount FROM orders UNION ALL SELECT amount FROM orders",
        "SELECT amount FROM RANGE(2) AS t(amount)",
        "SELECT 1",
    ],
)
def test_ambiguous_hidden_key_is_rejected_without_partial_changes(body):
    query = parse(body)
    before = str(query)
    ctx = context({("fact", "date.date_id"): [("orders", "date_id")]})
    with pytest.raises(DJInvalidInputException, match="eligible relational owners"):
        place_filters(ctx, {"fact": query}, request())
    assert str(query) == before


def test_hidden_key_filters_only_the_row_source_not_its_scalar_reader():
    query = parse("SELECT (SELECT SUM(amount) FROM orders) AS total FROM orders o")
    ctx = context({("fact", "date.date_id"): [("orders", "date_id")]})
    place_filters(ctx, {"fact": query}, request())
    assert rows(query) == [(15,)]


def test_compound_filter_requires_every_reference_to_bind():
    query = parse("SELECT amount FROM orders")
    before = str(query)
    ctx = context({("fact", "date.date_id"): [("orders", "date_id")]})
    with pytest.raises(DJInvalidInputException, match="product.category"):
        place_filters(
            ctx,
            {"fact": query},
            request(["date.date_id = 20240101 OR product.category = 'B'"]),
        )
    assert str(query) == before


def test_shared_linked_transform_is_filtered_at_its_fact_occurrence():
    stage = parse("SELECT amount, date_id FROM orders")
    fact = parse(
        "SELECT amount, (SELECT COUNT(*) FROM dimension) AS dimension_count FROM stage",
    )
    dimension = parse("SELECT date_id FROM stage")
    before = str(stage), str(dimension)
    ctx = context({("fact", "date.date_id"): [("stage", "date_id")]})
    place_filters(
        ctx,
        {"fact": fact, "stage": stage, "dimension": dimension},
        request(),
    )
    assert (str(stage), str(dimension)) == before
    assert rows(f"WITH stage AS ({stage}), dimension AS ({dimension}) {fact}") == [
        (10, 2),
    ]


def test_shared_hidden_key_definition_is_rejected():
    stage = parse("SELECT amount FROM orders")
    queries = {
        "fact": parse("SELECT amount FROM stage"),
        "stage": stage,
        "dimension": parse("SELECT amount FROM stage"),
    }
    before = {name: str(query) for name, query in queries.items()}
    ctx = context({("fact", "date.date_id"): [("orders", "date_id")]})
    with pytest.raises(DJInvalidInputException, match="shared query definition"):
        place_filters(ctx, queries, request())
    assert {name: str(query) for name, query in queries.items()} == before


def test_unique_hidden_key_path_survives_aggregation_and_repeated_builds():
    authored = parse("SELECT SUM(amount) AS total FROM (SELECT amount FROM orders) o")
    ctx = context({("fact", "date.date_id"): [("orders", "date_id")]})
    results = []
    for _ in range(2):
        query = deepcopy(authored)
        place_filters(ctx, {"fact": query}, request())
        results.append(rows(query))
    assert results == [[(10,)], [(10,)]]
    assert "WHERE" not in str(authored)


def test_placement_state_does_not_leak_between_fact_groups():
    ctx = context(
        {
            ("first", "date.date_id"): [("orders", "date_id")],
            ("second", "date.date_id"): [("orders", "date_id")],
        },
    )
    authored = parse("SELECT SUM(amount) AS total FROM orders")
    for owner in ("first", "second"):
        query = deepcopy(authored)
        group_request = request()
        group_request.parent_node_name = owner
        place_filters(ctx, {owner: query}, group_request)
        assert rows(query) == [(10,)]
    assert "WHERE" not in str(authored)


@pytest.mark.parametrize(
    "body",
    [
        "WITH unused AS (SELECT date_id FROM orders) SELECT 1",
        "WITH history AS (SELECT amount FROM orders) "
        "SELECT amount, (SELECT SUM(amount) FROM history) AS total FROM history",
        "WITH history AS (SELECT amount FROM orders UNION ALL SELECT amount FROM history) "
        "SELECT amount FROM history",
        "WITH history AS (SELECT amount FROM history) SELECT amount FROM history",
    ],
)
def test_hidden_keys_in_unused_shared_or_recursive_definitions_are_rejected(body):
    query = parse(body)
    before = str(query)
    ctx = context({("fact", "date.date_id"): [("orders", "date_id")]})
    with pytest.raises(DJInvalidInputException):
        place_filters(ctx, {"fact": query}, request())
    assert str(query) == before


@pytest.mark.parametrize("hidden", [False, True])
def test_materialized_fact_requires_exposed_filter_keys(hidden):
    ctx = context({("fact", "date.date_id"): [("orders", "date_id")]} if hidden else {})
    if hidden:
        with pytest.raises(DJInvalidInputException, match="materialized fact"):
            place_filters(ctx, {}, request())
    else:
        assert place_filters(ctx, {}, request()) == set()


def test_local_filter_leaves_shared_fact_definition_to_its_consumer():
    fact = parse("SELECT date_id FROM orders")
    reader = parse("SELECT (SELECT COUNT(*) FROM fact) AS total")
    before = str(fact), str(reader)
    assert (
        place_filters(context(), {"fact": fact, "reader": reader}, request()) == set()
    )
    assert (str(fact), str(reader)) == before
    assert rows(f"WITH fact AS ({fact}) {reader}") == [(2,)]


def test_complete_hidden_predicate_cannot_span_different_consumers():
    query = parse(
        "SELECT o.amount FROM orders o JOIN products p ON o.amount = p.product_id",
    )
    before = str(query)
    ctx = context(
        {
            ("fact", "date.date_id"): [("orders", "date_id")],
            ("fact", "product.category"): [("products", "category")],
        },
    )
    with pytest.raises(DJInvalidInputException, match="complete predicate"):
        place_filters(
            ctx,
            {"fact": query},
            request(["date.date_id = 20240101 OR product.category = 'B'"]),
        )
    assert str(query) == before


def test_complete_hidden_predicate_keeps_or_on_one_consumer():
    query = parse("SELECT amount FROM orders o")
    ctx = context(
        {
            ("fact", "date.date_id"): [("orders", "date_id")],
            ("fact", "account.id"): [("orders", "amount")],
        },
    )
    place_filters(
        ctx,
        {"fact": query},
        request(["date.date_id = 20240101 OR account.id = 5"]),
    )
    assert rows(query) == [(10,), (5,)]


def test_all_filters_validate_before_any_query_is_changed():
    query = parse("SELECT date_id FROM orders")
    before = str(query)
    ctx = context({("fact", "product.category"): [("products", "category")]})
    with pytest.raises(DJInvalidInputException, match="eligible relational owners"):
        place_filters(
            ctx,
            {"fact": query},
            request(["date.date_id = 20240101", "product.category = 'B'"]),
        )
    assert str(query) == before


@pytest.mark.parametrize("missing_revision", [False, True])
def test_upstream_discovery_skips_missing_metadata(missing_revision):
    ctx = context()
    ctx.parent_map = {"fact": ["upstream"], "upstream": ["fact"]}
    if missing_revision:
        ctx.nodes["upstream"] = SimpleNamespace(current=None)
    assert not _resolve_filter_only_dim(
        ctx,
        ctx.nodes["fact"],
        parse_dimension_ref("date.date_id"),
        "date.date_id",
    )
    assert ctx.upstream_filter_bindings == {}


def test_multiple_explicit_scan_bounds_are_combined_before_aggregation():
    ctx = context()
    fact = ctx.nodes["fact"]
    fact.type = NodeType.TRANSFORM
    fact.current.query = "SELECT date_id FROM orders"
    fact.current.availability = None
    ctx.nodes["orders"] = SimpleNamespace(
        name="orders",
        type=NodeType.SOURCE,
        current=SimpleNamespace(catalog=None, schema_=None, table="orders"),
    )
    ctx.parent_map = {"fact": ["orders"]}
    ctx.upstream_pushdown_filters = {
        "fact": [
            parse("SELECT 1 WHERE date_id >= 20230102").select.where,
            parse("SELECT 1 WHERE date_id <= 20231231").select.where,
        ],
    }
    query, _, _ = build_select_ast(
        ctx,
        metric_expressions=[("count", parse("SELECT COUNT(*)").select.projection[0])],
        resolved_dimensions=[],
        parent_node=fact,
    )
    assert rows(query) == [(0,)]
    assert "WHERE" not in str(ctx.get_parsed_query(fact))
