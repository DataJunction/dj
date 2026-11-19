"""Unsupported SQL shapes retain conservative dependencies."""

import asyncio

import pytest

from datajunction_server.models.deployment import ColumnSpec, SourceSpec, TransformSpec
from datajunction_server.semantic_fingerprints.v2 import (
    SemanticGraphSnapshot,
    compare_components,
    compute_v2_fingerprints,
)


def _source(table="orders"):
    return SourceSpec(
        name="ns.source",
        catalog="warehouse",
        schema_="sales",
        table=table,
        columns=[
            ColumnSpec(name="amount", type="double"),
            ColumnSpec(name="id", type="bigint"),
        ],
    )


def _evaluate(source, query):
    snapshot = SemanticGraphSnapshot(
        {
            "ns.source": source,
            "ns.project": TransformSpec(name="ns.project", query=query),
        },
    )
    return asyncio.run(compute_v2_fingerprints(snapshot))


@pytest.mark.parametrize(
    "query",
    [
        "WITH rows AS (SELECT amount FROM ns.source) SELECT amount FROM rows",
        "SELECT amount FROM ns.source UNION ALL SELECT id FROM ns.source",
        "SELECT a.amount FROM ns.source a JOIN ns.source b ON a.id = b.id",
        "SELECT unsupported_fn(amount) AS amount FROM ns.source",
        "SELECT SUM(amount) OVER (ORDER BY id) AS amount FROM ns.source",
        "SELECT amount FROM ns.source GROUP BY 1",
    ],
)
def test_unsupported_construct_is_unknown(query):
    before = _evaluate(_source(), query)
    after = _evaluate(_source(table="replacement"), query)

    assert before.nodes["ns.project"].precision == "unknown"
    assert before.nodes["ns.project"].reasons
    assert (
        compare_components(
            before.nodes["ns.project"].full,
            after.nodes["ns.project"].full,
        )
        == "indeterminate"
    )


def test_invalid_or_missing_source_type_is_unknown():
    invalid = _source()
    invalid.columns[0].type = "not_a_sql_type"
    invalid_result = _evaluate(invalid, "SELECT amount FROM ns.source")
    missing_result = _evaluate(
        _source().model_copy(update={"columns": []}),
        "SELECT amount FROM ns.source",
    )

    assert invalid_result.nodes["ns.project"].precision == "unknown"
    assert missing_result.nodes["ns.project"].precision == "unknown"


def test_duplicate_authored_or_captured_source_columns_are_unknown():
    duplicate = [
        ColumnSpec(name="amount", type="double"),
        ColumnSpec(name="AMOUNT", type="bigint"),
    ]
    authored_snapshot = SemanticGraphSnapshot(
        {
            "ns.source": _source().model_copy(update={"columns": duplicate}),
            "ns.project": TransformSpec(
                name="ns.project",
                query="SELECT amount FROM ns.source",
            ),
        },
    )
    captured_snapshot = SemanticGraphSnapshot(
        {
            "ns.source": _source().model_copy(update={"columns": None}),
            "ns.project": TransformSpec(
                name="ns.project",
                query="SELECT amount FROM ns.source",
            ),
        },
        resolved_columns={"ns.source": duplicate},
    )

    authored = asyncio.run(compute_v2_fingerprints(authored_snapshot))
    captured = asyncio.run(compute_v2_fingerprints(captured_snapshot))

    assert authored.nodes["ns.project"].precision == "unknown"
    assert captured.nodes["ns.project"].precision == "unknown"


def test_duplicate_provider_output_is_unknown():
    result = _evaluate(
        _source(),
        "SELECT amount AS value, id AS VALUE FROM ns.source",
    )

    assert result.nodes["ns.project"].precision == "unknown"


@pytest.mark.parametrize(
    ("query", "changed_query"),
    [
        (
            "SELECT CASE WHEN id > 0 AND id < 10 OR id = 20 "
            "THEN amount ELSE 0 END AS value FROM ns.source",
            "SELECT CASE WHEN id > 0 AND id < 10 OR id = 21 "
            "THEN amount ELSE 0 END AS value FROM ns.source",
        ),
        (
            "SELECT CAST(amount AS DOUBLE) AS value FROM ns.source",
            "SELECT CAST(amount AS BIGINT) AS value FROM ns.source",
        ),
    ],
)
def test_case_and_cast_remain_precise(query, changed_query):
    before = _evaluate(_source(), query)
    after = _evaluate(_source(), changed_query)

    assert before.nodes["ns.project"].precision == "precise"
    assert after.nodes["ns.project"].precision == "precise"
    assert (
        compare_components(
            before.nodes["ns.project"].full,
            after.nodes["ns.project"].full,
        )
        == "changed"
    )


@pytest.mark.parametrize(
    "query",
    [
        "SELECT amount FROM ns.source TABLESAMPLE (10 PERCENT)",
        "SELECT amount FROM ns.source VERSION AS OF 123",
        "SELECT amount FROM ns.source PIVOT (SUM(amount) FOR id IN (1))",
        "SELECT amount FROM ns.source UNPIVOT (value FOR kind IN (amount))",
        "SELECT amount FROM ns.source LATERAL VIEW EXPLODE(array(1)) t AS x",
        "SELECT amount FROM ns.source AS s(renamed)",
    ],
)
def test_unsupported_select_clauses_are_not_precise(query):
    result = _evaluate(_source(), query)

    assert result.nodes["ns.project"].precision == "unknown"
    assert result.nodes["ns.project"].reasons


def test_renamed_output_does_not_inherit_declared_type_by_position():
    source = _source()
    transform = TransformSpec(
        name="ns.renamed",
        query="SELECT amount AS renamed FROM ns.source",
        columns=[ColumnSpec(name="other", type="double")],
    )
    metric = TransformSpec(
        name="ns.consumer",
        query="SELECT renamed FROM ns.renamed",
    )
    before = asyncio.run(
        compute_v2_fingerprints(
            SemanticGraphSnapshot(
                {item.rendered_name: item for item in (source, transform, metric)},
            ),
        ),
    )

    changed_transform = transform.model_copy(
        update={"columns": [ColumnSpec(name="other", type="bigint")]},
    )
    after = asyncio.run(
        compute_v2_fingerprints(
            SemanticGraphSnapshot(
                {
                    item.rendered_name: item
                    for item in (source, changed_transform, metric)
                },
            ),
        ),
    )

    assert (
        compare_components(
            before.nodes["ns.consumer"].full,
            after.nodes["ns.consumer"].full,
        )
        == "unchanged"
    )
