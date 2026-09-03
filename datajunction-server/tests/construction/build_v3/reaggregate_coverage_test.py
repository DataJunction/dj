"""Focused coverage tests for build-v3 reaggregation helpers."""

from types import SimpleNamespace

import pytest

from datajunction_server.construction.build_v3.cube_matcher import (
    _reaggregate_dimensions_for_cube_metrics,
    _reaggregate_requirements_for_cube_metrics,
    _reaggregate_requirements_for_decomposed_metrics,
    build_synthetic_grain_group,
)
from datajunction_server.construction.build_v3 import builder as builder_module
from datajunction_server.construction.build_v3 import metrics as metrics_module
from datajunction_server.construction.build_v3.builder import setup_build_context
from datajunction_server.construction.build_v3.measures import (
    build_grain_group_sql,
    build_select_ast,
    build_window_metric_grain_groups,
)
from datajunction_server.construction.build_v3.metrics import (
    _build_reaggregate_collapse_expression,
    _dimension_ref_base,
    _dimension_ref_role,
    _metric_parent_refs,
    _source_dimension_alias,
    build_window_agg_cte_from_base_metrics,
    generate_metrics_sql,
)
from datajunction_server.construction.build_v3.types import (
    BuildContext,
    ColumnMetadata,
    DecomposedMetricInfo,
    GeneratedMeasuresSQL,
    GrainGroup,
    GrainGroupSQL,
    ResolvedDimension,
)
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.decompose import (
    Aggregability,
    AggregationRule,
    MetricComponent,
)
from datajunction_server.models.dialect import Dialect
from datajunction_server.models.node import NodeType
from datajunction_server.models.reaggregate import (
    DimensionReaggregateRule,
    ReaggregationFunction,
)
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.sql.parsing.types import BigIntType, DoubleType, StringType


def _metric_node(name: str, query: str = "SELECT SUM(value) FROM test.parent"):
    return SimpleNamespace(
        name=name,
        type=NodeType.METRIC,
        current=SimpleNamespace(
            query=query,
            columns=[SimpleNamespace(type=DoubleType())],
        ),
    )


def _source_node(name: str = "test.parent"):
    return SimpleNamespace(
        name=name,
        type=NodeType.SOURCE,
        current=SimpleNamespace(
            catalog=SimpleNamespace(name="default"),
            schema_="analytics",
            table="parent",
            columns=[
                SimpleNamespace(name="id", type=BigIntType()),
                SimpleNamespace(name="value", type=DoubleType()),
                SimpleNamespace(name="region", type=StringType()),
                SimpleNamespace(name="date_id", type=BigIntType()),
            ],
        ),
    )


def _semi_additive_component(
    name: str = "balance_sum",
    dimension: str = "v3.date.date_id",
    fn: ReaggregationFunction = ReaggregationFunction.LAST_VALUE,
) -> MetricComponent:
    return MetricComponent(
        name=name,
        expression="value",
        aggregation="SUM",
        rule=AggregationRule(
            type=Aggregability.FULL,
            reaggregate=DimensionReaggregateRule(
                dimension=dimension,
                fn=fn,
            ),
        ),
    )


def _decomposed_metric(
    metric_name: str,
    component: MetricComponent | None = None,
    query: str = "SELECT SUM(value) FROM test.parent",
) -> DecomposedMetricInfo:
    return DecomposedMetricInfo(
        metric_node=_metric_node(metric_name, query),
        components=[component or _semi_additive_component()],
        aggregability=Aggregability.FULL,
        combiner=query,
        derived_ast=parse(query),
    )


def test_reaggregate_collapse_expression_handles_min_max_and_rejects_unsupported():
    """MIN/MAX collapse ignore the protected dimension; unsupported functions fail."""
    value_ref = ast.Column(name=ast.Name("balance"))
    protected_ref = ast.Column(name=ast.Name("date_id"))

    assert (
        str(
            _build_reaggregate_collapse_expression(
                ReaggregationFunction.MIN,
                Dialect.SPARK,
                value_ref,
                protected_ref,
            ),
        )
        == "MIN(balance)"
    )
    assert (
        str(
            _build_reaggregate_collapse_expression(
                ReaggregationFunction.MAX,
                Dialect.SPARK,
                value_ref,
                protected_ref,
            ),
        )
        == "MAX(balance)"
    )

    with pytest.raises(DJInvalidInputException, match="Unsupported semi-additive"):
        _build_reaggregate_collapse_expression(
            ReaggregationFunction.SUM,
            Dialect.SPARK,
            value_ref,
            protected_ref,
        )


def test_reaggregate_dimension_ref_helpers_handle_roleless_refs_without_alias():
    """Roleless refs return None for role and do not require an alias fallback."""
    ctx = BuildContext(session=SimpleNamespace(), metrics=[], dimensions=[])

    assert _dimension_ref_base("v3.date.date_id[order]") == "v3.date.date_id"
    assert _dimension_ref_role("v3.date.date_id") is None
    assert _source_dimension_alias(ctx, "v3.date.date_id") is None


def test_metric_parent_refs_adds_metric_refs_discovered_from_query():
    """Parsed-query metric references supplement the cached parent map."""
    balance = _metric_node("balance")
    extra = _metric_node("extra_metric")
    derived = _metric_node(
        "derived",
        "SELECT balance + extra_metric + raw_col FROM test.parent",
    )
    ctx = BuildContext(session=SimpleNamespace(), metrics=[], dimensions=[])
    ctx.nodes = {
        "balance": balance,
        "extra_metric": extra,
        "derived": derived,
    }
    ctx.parent_map = {"derived": ["balance"]}

    assert _metric_parent_refs(ctx, "derived") == ["balance", "extra_metric"]


def test_window_agg_from_base_metrics_collapses_semi_additive_derived_parent(
    monkeypatch,
):
    """Cross-fact window reaggregation replaces derived metric parents recursively."""
    monkeypatch.setattr(metrics_module, "get_column_full_name", lambda _col: "")
    balance = _metric_node("balance")
    extra = _metric_node("extra_metric")
    derived = _metric_node(
        "derived",
        "SELECT balance + extra_metric + raw_col FROM test.parent",
    )
    window = _metric_node("window_metric")
    ctx = BuildContext(
        session=SimpleNamespace(),
        metrics=["window_metric"],
        dimensions=["category"],
    )
    ctx.nodes = {
        "balance": balance,
        "extra_metric": extra,
        "derived": derived,
        "window_metric": window,
    }
    ctx.parent_map = {
        "window_metric": ["derived"],
        "derived": ["balance"],
    }
    ctx.alias_registry.register("v3.date.date_id")

    query = build_window_agg_cte_from_base_metrics(
        GrainGroupSQL(
            query=parse("SELECT category FROM base_metrics"),
            columns=[
                ColumnMetadata(
                    name="category",
                    semantic_name="category",
                    type="string",
                    semantic_type="dimension",
                ),
            ],
            grain=["category"],
            aggregability=Aggregability.FULL,
            metrics=[],
            parent_name="cross_fact",
            is_window_grain_group=True,
            window_metrics_served=["window_metric"],
        ),
        "base_metrics",
        ctx,
        {
            "balance": _decomposed_metric(
                "balance",
                _semi_additive_component(fn=ReaggregationFunction.MAX),
            ),
        },
    )

    rendered = str(query)
    assert "MAX(base_metrics.balance)" in rendered
    assert "extra_metric" in rendered
    assert "raw_col" in rendered


def test_generate_metrics_sql_rejects_multiple_base_grain_groups_with_reaggregate():
    """Live SQL does not allow protected-dimension fanout across base groups."""
    ctx = BuildContext(session=SimpleNamespace(), metrics=["balance"], dimensions=[])
    grain_groups = [
        GrainGroupSQL(
            query=parse("SELECT category, balance_sum FROM fact_a"),
            columns=[],
            grain=["category"],
            aggregability=Aggregability.FULL,
            metrics=["balance"],
            parent_name="fact_a",
            reaggregate_dimension_aliases={"balance_sum": "date_id"},
        ),
        GrainGroupSQL(
            query=parse("SELECT category, orders FROM fact_b"),
            columns=[],
            grain=["category"],
            aggregability=Aggregability.FULL,
            metrics=["orders"],
            parent_name="fact_b",
        ),
    ]
    measures_result = GeneratedMeasuresSQL(
        grain_groups=grain_groups,
        dialect=Dialect.SPARK,
        requested_dimensions=[],
        ctx=ctx,
    )

    with pytest.raises(DJInvalidInputException, match="multiple base grain groups"):
        generate_metrics_sql(ctx, measures_result, {})


def test_cube_reaggregate_requirements_skip_satisfied_dimensions_and_dedupe():
    """Cube requirement helpers skip requested protected dims and de-duplicate."""
    metric_revision = SimpleNamespace(
        name="balance",
        reaggregate={
            "rules": [
                {
                    "dimension": "v3.date.date_id[order]",
                    "fn": "last_value",
                },
            ],
        },
    )
    cube = SimpleNamespace(metric_node_revisions=lambda: [metric_revision])

    assert (
        _reaggregate_requirements_for_cube_metrics(
            cube,
            ["balance"],
            ["v3.date.date_id[order]"],
        )
        == []
    )

    duplicate_cube = SimpleNamespace(
        metric_node_revisions=lambda: [metric_revision, metric_revision],
    )
    assert _reaggregate_dimensions_for_cube_metrics(
        duplicate_cube,
        ["balance"],
        ["v3.product.category"],
    ) == ["v3.date.date_id[order]"]


def test_decomposed_reaggregate_requirements_dedupe_duplicate_components():
    """Duplicate component rules produce one cube materialization requirement."""
    rule = DimensionReaggregateRule(
        dimension="v3.date.date_id",
        fn=ReaggregationFunction.LAST_VALUE,
    )
    components = [
        MetricComponent(
            name=f"balance_sum_{idx}",
            expression="value",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL, reaggregate=rule),
        )
        for idx in range(2)
    ]
    decomposed = _decomposed_metric("balance", components[0])
    decomposed.components = components

    assert _reaggregate_requirements_for_decomposed_metrics(
        {"balance": decomposed},
        ["balance"],
        [],
    ) == [("balance", "v3.date.date_id", ReaggregationFunction.LAST_VALUE)]


def test_build_synthetic_grain_group_skips_duplicate_internal_dimension():
    """Filter-only protected dims can already be present in ctx.dimensions."""
    ctx = BuildContext(
        session=SimpleNamespace(),
        metrics=["balance"],
        dimensions=["v3.date.date_id"],
        dialect=Dialect.DRUID,
    )
    ctx.filter_dimensions = {"v3.date.date_id"}
    ctx.nodes = {"balance": _metric_node("balance")}
    ctx.parent_map = {"balance": ["test.parent"]}
    cube = SimpleNamespace(
        name="daily_balance_cube",
        availability=SimpleNamespace(table="daily_balance_cube"),
        materializations=[],
    )

    grain_group = build_synthetic_grain_group(
        ctx,
        {"balance": _decomposed_metric("balance")},
        cube,
    )

    assert grain_group.grain == ["date_id"]
    assert [
        col.name for col in grain_group.columns if col.semantic_type == "dimension"
    ] == [
        "date_id",
    ]


def test_build_select_ast_skips_non_output_dimensions():
    """Resolved dimensions outside output refs are join/group-only."""
    ctx = BuildContext(session=SimpleNamespace(), metrics=[], dimensions=[])
    parent = _source_node()
    resolved_dimensions = [
        ResolvedDimension(
            original_ref="test.parent.region",
            node_name="test.parent",
            column_name="region",
            role=None,
            join_path=None,
            is_local=True,
        ),
        ResolvedDimension(
            original_ref="test.parent.date_id",
            node_name="test.parent",
            column_name="date_id",
            role=None,
            join_path=None,
            is_local=True,
        ),
    ]

    query, _ = build_select_ast(
        ctx,
        metric_expressions=[],
        resolved_dimensions=resolved_dimensions,
        parent_node=parent,
        output_dimension_refs={"test.parent.region"},
    )

    rendered = str(query)
    assert "t1.region" in rendered
    assert "t1.date_id AS date_id" not in rendered


def test_build_grain_group_sql_handles_internal_alias_already_in_grain():
    """The internal reaggregate alias can match a requested dimension alias."""
    ctx = BuildContext(
        session=SimpleNamespace(),
        metrics=["balance"],
        dimensions=["test.parent.date_id"],
        use_materialized=False,
    )
    parent = _source_node()
    metric = _metric_node("balance")
    component = _semi_additive_component(dimension="test.parent.date_id")
    resolved_dimension = ResolvedDimension(
        original_ref="test.parent.date_id",
        node_name="test.parent",
        column_name="date_id",
        role=None,
        join_path=None,
        is_local=True,
    )
    grain_group = GrainGroup(
        parent_node=parent,
        aggregability=Aggregability.FULL,
        grain_columns=[],
        components=[(metric, component)],
        reaggregate_component_dimensions={component.name: "test.parent.date_id"},
    )

    result = build_grain_group_sql(
        ctx,
        grain_group,
        [resolved_dimension],
        {"balance": 1},
        output_dimension_refs={"test.parent.date_id"},
    )

    assert result.grain == ["date_id"]


def test_build_window_metric_grain_groups_skips_missing_base_components():
    """Window grain planning skips parent groups with no decomposed components."""
    ctx = BuildContext(session=SimpleNamespace(), metrics=["window"], dimensions=[])
    ctx.parent_map = {"window": ["missing_base"]}
    existing_group = GrainGroupSQL(
        query=parse("SELECT missing_base FROM test_parent_0"),
        columns=[],
        grain=[],
        aggregability=Aggregability.FULL,
        metrics=["missing_base"],
        parent_name="test.parent",
    )

    assert (
        build_window_metric_grain_groups(
            ctx,
            {"window": {"v3.date.week"}},
            [existing_group],
            {},
        )
        == []
    )


def test_build_window_metric_grain_groups_skips_when_parent_node_missing():
    """Window grain planning skips groups whose source parent is not loaded."""
    ctx = BuildContext(session=SimpleNamespace(), metrics=["window"], dimensions=[])
    ctx.nodes = {"missing_base": _metric_node("missing_base")}
    ctx.parent_map = {"window": ["missing_base"]}
    existing_group = GrainGroupSQL(
        query=parse("SELECT missing_base FROM missing_parent_0"),
        columns=[],
        grain=[],
        aggregability=Aggregability.FULL,
        metrics=["missing_base"],
        parent_name="missing.parent",
    )

    assert (
        build_window_metric_grain_groups(
            ctx,
            {"window": {"v3.date.week"}},
            [existing_group],
            {
                "window": _decomposed_metric("window"),
                "missing_base": _decomposed_metric("missing_base"),
            },
        )
        == []
    )


@pytest.mark.asyncio
async def test_setup_build_context_skips_duplicate_internal_reaggregate_dimension(
    monkeypatch,
):
    """setup_build_context does not duplicate internal reaggregate dimensions."""
    load_calls = []

    async def fake_load_nodes(ctx):
        load_calls.append(list(ctx.dimensions))
        ctx.nodes = {"balance": _metric_node("balance")}

    async def fake_decompose_and_group_metrics(ctx):
        return [], {"balance": _decomposed_metric("balance")}

    monkeypatch.setattr(builder_module, "load_nodes", fake_load_nodes)
    monkeypatch.setattr(
        builder_module,
        "decompose_and_group_metrics",
        fake_decompose_and_group_metrics,
    )
    monkeypatch.setattr(
        builder_module,
        "add_dimensions_from_metric_expressions",
        lambda _ctx, _decomposed_metrics: None,
    )
    monkeypatch.setattr(
        builder_module,
        "missing_reaggregate_dimensions",
        lambda _decomposed_metrics, _requested_dimensions: ["v3.date.date_id"],
    )

    ctx = await setup_build_context(
        session=SimpleNamespace(),
        metrics=["balance"],
        dimensions=["v3.date.date_id"],
    )

    assert ctx.dimensions == ["v3.date.date_id"]
    assert load_calls == [["v3.date.date_id"], ["v3.date.date_id"]]
