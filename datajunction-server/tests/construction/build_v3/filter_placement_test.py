"""Request predicates belong to a consumer, not every matching table scan."""

import sqlite3
from copy import deepcopy
from types import SimpleNamespace

import pytest

from datajunction_server.construction.build_v3.filter_placement import place_filters
from datajunction_server.construction.build_v3.types import (
    BuildContext,
    PushdownFilters,
)
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.sql.parsing.backends.antlr4 import parse


def context(bindings=None):
    return BuildContext(
        session=None,
        metrics=[],
        dimensions=[],
        nodes={
            "fact": SimpleNamespace(
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


@pytest.mark.parametrize(
    "body",
    [
        "SELECT o.amount + s.total FROM orders o CROSS JOIN "
        "(SELECT SUM(amount) AS total FROM orders) s",
        "SELECT a.amount FROM orders a JOIN orders b ON a.date_id = b.date_id",
        "SELECT amount FROM orders UNION ALL SELECT amount FROM orders",
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
    ],
)
def test_hidden_keys_in_unused_shared_or_recursive_definitions_are_rejected(body):
    query = parse(body)
    before = str(query)
    ctx = context({("fact", "date.date_id"): [("orders", "date_id")]})
    with pytest.raises(DJInvalidInputException):
        place_filters(ctx, {"fact": query}, request())
    assert str(query) == before
