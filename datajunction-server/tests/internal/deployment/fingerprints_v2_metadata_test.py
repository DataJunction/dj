"""Presentation metadata stays available without changing v2 result hashes."""

import asyncio
from copy import deepcopy

import pytest

from datajunction_server.models.deployment import (
    ColumnSpec,
    CubeSpec,
    DimensionSpec,
    MetricSpec,
    SourceSpec,
    TransformSpec,
)
from datajunction_server.models.filter_requirements import FilterRequirement
from datajunction_server.semantic_fingerprints.v2 import (
    SemanticGraphSnapshot,
    compare_components,
    compute_v2_fingerprints,
)


def _evaluate(specs, *, resolved_columns=None):
    specs = deepcopy(specs)
    for spec in specs.values():
        if hasattr(spec, "_query_ast"):
            spec._query_ast = None
    snapshot = SemanticGraphSnapshot(specs, resolved_columns=resolved_columns)
    return snapshot, asyncio.run(compute_v2_fingerprints(snapshot))


def _unchanged(before, after, name):
    return (
        compare_components(before.nodes[name].full, after.nodes[name].full)
        == "unchanged"
    )


def _with_presentation(spec, label):
    return spec.model_copy(
        update={
            "description": f"{label} prose",
            "display_name": label,
            "owners": [label],
            "tags": [label],
            "custom_metadata": {label: {"key": "value"}},
            "columns": [
                column.model_copy(
                    update={
                        "description": f"{label} column prose",
                        "display_name": f"{label} column",
                    },
                )
                for column in spec.columns or []
            ],
        },
    )


def _presentation_graph(label):
    specs = [
        SourceSpec(
            name="ns.source",
            catalog="warehouse",
            schema_="sales",
            table="orders",
            columns=[ColumnSpec(name="amount", type="double")],
        ),
        TransformSpec(
            name="ns.project",
            query="SELECT amount FROM ns.source",
            columns=[ColumnSpec(name="amount", type="double")],
        ),
        DimensionSpec(
            name="ns.dimension",
            query="SELECT amount FROM ns.source",
            columns=[ColumnSpec(name="amount", type="double")],
        ),
        MetricSpec(
            name="ns.total",
            query="SELECT SUM(amount) AS total FROM ns.project",
            columns=[ColumnSpec(name="total", type="double")],
        ),
        CubeSpec(
            name="ns.cube",
            metrics=["ns.total"],
            dimensions=["ns.dimension.amount"],
            columns=[ColumnSpec(name="ns.total", type="double")],
        ),
    ]
    return {spec.rendered_name: _with_presentation(spec, label) for spec in specs}


def test_presentation_metadata_is_retained_but_excluded_from_node_hashes():
    before_snapshot, before = _evaluate(_presentation_graph("original"))
    after_snapshot, after = _evaluate(_presentation_graph("updated"))

    assert set(before.nodes) == set(after.nodes)
    assert all(
        _unchanged(before, after, name) for name in before.nodes if name != "ns.cube"
    )
    assert before.nodes["ns.cube"].full.fingerprint == "unknown"
    assert after.nodes["ns.cube"].full.fingerprint == "unknown"
    assert (
        compare_components(before.nodes["ns.cube"].full, after.nodes["ns.cube"].full)
        == "indeterminate"
    )
    assert before_snapshot.specs["ns.source"].description == "original prose"
    assert after_snapshot.specs["ns.source"].description == "updated prose"
    assert (
        before_snapshot.specs["ns.source"].columns[0].description
        == "original column prose"
    )
    assert after_snapshot.specs["ns.total"].columns[0].display_name == "updated column"
    assert after_snapshot.specs["ns.total"].custom_metadata == {
        "updated": {"key": "value"},
    }


def test_resolved_external_column_presentation_metadata_does_not_change_consumers():
    source = SourceSpec(
        name="ns.external",
        catalog="warehouse",
        schema_="sales",
        table="external_table",
    )
    metric = MetricSpec(
        name="ns.total",
        query="SELECT SUM(amount) AS total FROM ns.external",
    )
    specs = {source.rendered_name: source, metric.rendered_name: metric}
    before_columns = [
        ColumnSpec(name="amount", type="double", description="old prose"),
    ]
    after_columns = [
        ColumnSpec(
            name="amount",
            type="double",
            description="new prose",
            display_name="Amount",
        ),
    ]
    before_snapshot, before = _evaluate(
        specs,
        resolved_columns={"ns.external": before_columns},
    )
    after_snapshot, after = _evaluate(
        specs,
        resolved_columns={"ns.external": after_columns},
    )

    assert _unchanged(before, after, "ns.total")
    assert before_snapshot.resolved_columns["ns.external"][0].description == "old prose"
    assert after_snapshot.resolved_columns["ns.external"][0].display_name == "Amount"


def test_fully_resolved_opaque_fallback_ignores_presentation_metadata():
    source = SourceSpec(
        name="ns.source",
        catalog="warehouse",
        schema_="sales",
        table="orders",
        columns=[ColumnSpec(name="amount", type="double")],
    )
    opaque = TransformSpec(
        name="ns.opaque",
        query="SELECT unsupported_udf(amount) AS value FROM ns.source",
        columns=[ColumnSpec(name="value", type="double")],
    )
    metric = MetricSpec(
        name="ns.total",
        query="SELECT SUM(value) AS total FROM ns.opaque",
    )
    specs = [source, opaque, metric]
    before_specs = {
        spec.rendered_name: _with_presentation(spec, "old") for spec in specs
    }
    after_specs = {
        spec.rendered_name: _with_presentation(spec, "new") for spec in specs
    }
    _, before = _evaluate(before_specs)
    _, after = _evaluate(after_specs)

    assert before.nodes["ns.opaque"].precision == "conservative"
    assert _unchanged(before, after, "ns.opaque")
    assert _unchanged(before, after, "ns.total")


def test_opaque_fallback_preserves_declared_output_type():
    source = SourceSpec(
        name="ns.source",
        catalog="warehouse",
        schema_="sales",
        table="orders",
        columns=[ColumnSpec(name="amount", type="double")],
    )
    before_opaque = TransformSpec(
        name="ns.opaque",
        query="SELECT unsupported_udf(amount) AS value FROM ns.source",
        columns=[ColumnSpec(name="value", type="double")],
    )
    after_opaque = before_opaque.model_copy(
        update={"columns": [ColumnSpec(name="value", type="bigint")]},
    )
    _, before = _evaluate(
        {"ns.source": source, "ns.opaque": before_opaque},
    )
    _, after = _evaluate(
        {"ns.source": source, "ns.opaque": after_opaque},
    )

    assert before.nodes["ns.opaque"].precision == "conservative"
    assert (
        compare_components(
            before.nodes["ns.opaque"].full,
            after.nodes["ns.opaque"].full,
        )
        == "changed"
    )


def test_standalone_cube_without_configured_context_is_unknown():
    source = SourceSpec(
        name="ns.source",
        catalog="warehouse",
        schema_="sales",
        table="orders",
        columns=[ColumnSpec(name="amount", type="double")],
    )
    metric = MetricSpec(name="ns.total", query="SELECT SUM(amount) FROM ns.source")
    before_cube = CubeSpec(
        name="ns.cube",
        metrics=["ns.total"],
    )
    _, before = _evaluate(
        {"ns.source": source, "ns.total": metric, "ns.cube": before_cube},
    )
    _, after = _evaluate(
        {"ns.source": source, "ns.total": metric, "ns.cube": before_cube},
    )

    assert before.nodes["ns.cube"].full.fingerprint == "unknown"
    assert after.nodes["ns.cube"].full.fingerprint == "unknown"
    assert (
        compare_components(
            before.nodes["ns.cube"].full,
            after.nodes["ns.cube"].full,
        )
        == "indeterminate"
    )


@pytest.mark.parametrize("default", [None, ""])
@pytest.mark.parametrize(
    ("query", "precision"),
    [
        ("SELECT SUM(amount) AS total FROM ns.source", "precise"),
        (
            "SELECT unsupported_udf(amount) AS total FROM ns.source",
            "conservative",
        ),
    ],
)
def test_warning_only_recommendation_does_not_change_result_fingerprint(
    default,
    query,
    precision,
):
    source = SourceSpec(
        name="ns.source",
        catalog="warehouse",
        schema_="sales",
        table="orders",
        columns=[
            ColumnSpec(name="amount", type="double"),
            ColumnSpec(name="id", type="bigint"),
        ],
    )

    def metric(attribute):
        return MetricSpec(
            name="ns.total",
            query=query,
            recommended_filters=[
                FilterRequirement(dimension=[attribute], default=default),
            ],
        )

    _, before = _evaluate(
        {"ns.source": source, "ns.total": metric("ns.source.amount")},
    )
    after_snapshot, after = _evaluate(
        {"ns.source": source, "ns.total": metric("ns.missing.name")},
    )

    assert before.nodes["ns.total"].precision == precision
    assert after.nodes["ns.total"].precision == precision
    assert _unchanged(before, after, "ns.total")
    assert after_snapshot.specs["ns.total"].recommended_filters[0].dimension == (
        "ns.missing.name",
    )
