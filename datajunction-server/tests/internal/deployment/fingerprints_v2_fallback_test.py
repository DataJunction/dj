"""Conservative version 2 fallback dependencies remain complete."""

from datajunction_server.models.deployment import (
    ColumnSpec,
    CubeSpec,
    DimensionJoinLinkSpec,
    DimensionSpec,
    MetricSpec,
    SourceSpec,
    TransformSpec,
)
from datajunction_server.semantic_fingerprints.v2 import (
    ComponentKey,
    SemanticGraphSnapshot,
    _evaluate_definitions,
    compare_components,
)
from datajunction_server.semantic_fingerprints.v2_dependencies import (
    build_component_definitions,
)


def _snapshot(*specs):
    return SemanticGraphSnapshot({spec.rendered_name: spec for spec in specs})


def _results(snapshot):
    definitions, _, _ = build_component_definitions(snapshot)
    return _evaluate_definitions(definitions)


def _full(results, name):
    return results[ComponentKey(name, "full").token]


def _source():
    return SourceSpec(
        name="ns.source",
        catalog="warehouse",
        schema_="sales",
        table="orders",
        columns=[ColumnSpec(name="amount", type="double")],
    )


def test_unsupported_derived_metric_depends_on_its_named_metric():
    source = _source()
    base = MetricSpec(name="ns.base", query="SELECT SUM(amount) FROM ns.source")
    derived = MetricSpec(name="ns.derived", query="SELECT ns.base * 2")
    before = _results(_snapshot(source, base, derived))

    changed_base = MetricSpec(
        name="ns.base",
        query="SELECT SUM(amount * 2) FROM ns.source",
    )
    after = _results(_snapshot(source, changed_base, derived))

    assert _full(before, "ns.derived").precision == "conservative"
    assert _full(before, "ns.derived").digest != _full(after, "ns.derived").digest


def test_unsupported_metric_fallback_keeps_required_dimension_dependency():
    source = _source().model_copy(
        update={
            "dimension_links": [
                DimensionJoinLinkSpec(
                    dimension_node="ns.region",
                    join_on="ns.source.amount = ns.region.amount",
                ),
            ],
        },
    )
    region_source = SourceSpec(
        name="ns.region_source",
        catalog="warehouse",
        schema_="sales",
        table="regions",
        columns=[ColumnSpec(name="amount", type="double")],
    )
    dimension = DimensionSpec(
        name="ns.region",
        query="SELECT amount, 'US' AS country FROM ns.region_source",
        columns=[
            ColumnSpec(name="amount", type="double"),
            ColumnSpec(name="country", type="string"),
        ],
        primary_key=["amount"],
    )
    metric = MetricSpec(
        name="ns.metric",
        query="SELECT unknown_udf(amount) FROM ns.source",
        required_dimensions=["ns.region.country"],
    )
    before = _results(_snapshot(source, region_source, dimension, metric))

    changed_dimension = DimensionSpec(
        name="ns.region",
        query="SELECT amount + 1 AS amount, 'US' AS country FROM ns.region_source",
        columns=[
            ColumnSpec(name="amount", type="double"),
            ColumnSpec(name="country", type="string"),
        ],
        primary_key=["amount"],
    )
    after = _results(_snapshot(source, region_source, changed_dimension, metric))

    assert _full(before, "ns.metric").precision == "conservative"
    assert _full(before, "ns.metric").digest != _full(after, "ns.metric").digest


def test_unsupported_linkable_fallback_keeps_optional_link_target_dependency():
    source = _source()
    dimension = DimensionSpec(name="ns.region", query="SELECT amount FROM ns.source")
    linked = TransformSpec(
        name="ns.linked",
        query="SELECT unknown_udf(amount) FROM ns.source",
        dimension_links=[
            DimensionJoinLinkSpec(dimension_node="ns.region", join_type="cross"),
        ],
    )
    before = _results(_snapshot(source, dimension, linked))

    changed_dimension = DimensionSpec(
        name="ns.region",
        query="SELECT amount + 1 AS amount FROM ns.source",
    )
    after = _results(_snapshot(source, changed_dimension, linked))

    assert _full(before, "ns.linked").precision == "conservative"
    assert _full(before, "ns.linked").digest != _full(after, "ns.linked").digest


def test_standalone_cube_route_context_without_configured_analysis_is_indeterminate():
    lookup = SourceSpec(
        name="ns.lookup",
        catalog="warehouse",
        schema_="sales",
        table="lookup",
        columns=[
            ColumnSpec(name="id", type="bigint"),
            ColumnSpec(name="name", type="string"),
        ],
    )
    fact = SourceSpec(
        name="ns.fact",
        catalog="warehouse",
        schema_="sales",
        table="fact",
        columns=[
            ColumnSpec(name="amount", type="double"),
            ColumnSpec(name="country_id", type="bigint"),
            ColumnSpec(name="region_id", type="bigint"),
        ],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="ns.country",
                join_on="ns.fact.country_id = ns.country.id",
            ),
        ],
    )
    country = DimensionSpec(
        name="ns.country",
        query="SELECT id, name FROM ns.lookup",
        columns=lookup.columns,
        primary_key=["id"],
    )
    region = country.model_copy(update={"name": "ns.region"})
    metric = MetricSpec(
        name="ns.metric",
        query="SELECT unknown_udf(amount) AS value FROM ns.fact",
    )
    cube = CubeSpec(
        name="ns.cube",
        metrics=["ns.metric"],
        dimensions=["ns.country.name"],
    )
    before = _results(_snapshot(lookup, fact, country, region, metric, cube))

    fact_with_route = fact.model_copy(
        update={
            "dimension_links": [
                DimensionJoinLinkSpec(
                    dimension_node="ns.region",
                    join_on="ns.fact.region_id = ns.region.id",
                ),
            ],
        },
        deep=True,
    )
    region_with_route = region.model_copy(
        update={
            "dimension_links": [
                DimensionJoinLinkSpec(
                    dimension_node="ns.country",
                    join_on="ns.region.id = ns.country.id",
                ),
            ],
        },
        deep=True,
    )
    after = _results(
        _snapshot(lookup, fact_with_route, country, region_with_route, metric, cube),
    )

    assert _full(before, "ns.cube").fingerprint == "unknown"
    assert _full(after, "ns.cube").fingerprint == "unknown"
    assert (
        compare_components(_full(before, "ns.cube"), _full(after, "ns.cube"))
        == "indeterminate"
    )


def test_unparseable_query_and_missing_required_parent_are_unknown():
    broken = TransformSpec(
        name="ns.broken",
        query="SELECT (",
        columns=[ColumnSpec(name="amount", type="double")],
    )
    broken_result = _results(_snapshot(broken))
    assert _full(broken_result, "ns.broken").fingerprint == "unknown"

    source = _source()
    metric = MetricSpec(
        name="ns.metric",
        query="SELECT unknown_udf(amount) FROM ns.source",
        required_dimensions=["ns.missing.country"],
    )
    missing_result = _results(_snapshot(source, metric))
    assert _full(missing_result, "ns.metric").fingerprint == "unknown"
