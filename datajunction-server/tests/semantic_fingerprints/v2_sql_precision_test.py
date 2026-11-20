"""Regressions for precise SQL expression dependencies."""

import asyncio

import pytest

import datajunction_server.database  # noqa: F401
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
    SemanticGraphSnapshot,
    compare_components,
)
from datajunction_server.semantic_fingerprints.v2_configured import (
    compute_configured_v2_fingerprints,
)
from datajunction_server.semantic_fingerprints.v2_dependencies import (
    _numeric_literals,
)
from datajunction_server.sql.parsing import ast


def _specs(metric_query: str):
    fact = SourceSpec(
        name="ns.fact",
        catalog="warehouse",
        schema_="payroll",
        table="fact",
        columns=[
            ColumnSpec(name="left_id", type="bigint"),
            ColumnSpec(name="right_id", type="bigint"),
        ],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="ns.left_dim",
                join_on="ns.fact.left_id = ns.left_dim.id",
            ),
            DimensionJoinLinkSpec(
                dimension_node="ns.right_dim",
                join_on="ns.fact.right_id = ns.right_dim.id",
            ),
        ],
    )
    left_source = SourceSpec(
        name="ns.left_source",
        catalog="warehouse",
        schema_="payroll",
        table="left_rates",
        columns=[
            ColumnSpec(name="id", type="bigint"),
            ColumnSpec(name="rate", type="double"),
        ],
        primary_key=["id"],
    )
    right_source = SourceSpec(
        name="ns.right_source",
        catalog="warehouse",
        schema_="payroll",
        table="right_rates",
        columns=[
            ColumnSpec(name="id", type="bigint"),
            ColumnSpec(name="rate", type="double"),
        ],
        primary_key=["id"],
    )
    left = DimensionSpec(
        name="ns.left_dim",
        query="SELECT id, rate FROM ns.left_source",
        columns=left_source.columns,
        primary_key=["id"],
    )
    right = DimensionSpec(
        name="ns.right_dim",
        query="SELECT id, rate FROM ns.right_source",
        columns=right_source.columns,
        primary_key=["id"],
    )
    metric = MetricSpec(name="ns.delta", query=metric_query)
    cube = CubeSpec(name="ns.delta_cube", metrics=["ns.delta"])
    nodes = (fact, left_source, right_source, left, right, metric, cube)
    return {node.rendered_name: node for node in nodes}


def _evaluate(specs):
    return asyncio.run(
        compute_configured_v2_fingerprints(
            SemanticGraphSnapshot(specs),
        ),
    )


def test_implicit_dimension_operands_keep_provider_identity():
    forward = _evaluate(
        _specs("SELECT SUM(ns.left_dim.rate - ns.right_dim.rate) FROM ns.fact"),
    )
    reverse = _evaluate(
        _specs("SELECT SUM(ns.right_dim.rate - ns.left_dim.rate) FROM ns.fact"),
    )

    for name in ("ns.delta", "ns.delta_cube"):
        assert (
            compare_components(forward.nodes[name].full, reverse.nodes[name].full)
            == "changed"
        )


def test_implicit_dimension_bindings_keep_relation_alias_normalization():
    qualified = _evaluate(
        _specs("SELECT SUM(ns.left_dim.rate - ns.right_dim.rate) FROM ns.fact"),
    )
    aliased = _evaluate(
        _specs("SELECT SUM((ns.left_dim.rate - ns.right_dim.rate)) FROM ns.fact f"),
    )

    for name in ("ns.delta", "ns.delta_cube"):
        assert (
            compare_components(qualified.nodes[name].full, aliased.nodes[name].full)
            == "unchanged"
        )


def _rollup_specs(
    *,
    first_adjustment: int = 0,
    having_threshold: int = 0,
    group_employee: bool = False,
    reverse_group_order: bool = False,
    cte_name: str = "base",
):
    source_columns = [
        ColumnSpec(name="department", type="string"),
        ColumnSpec(name="employee_id", type="bigint"),
        *[
            ColumnSpec(name=f"amount_{index:02d}", type="double")
            for index in range(1, 13)
        ],
    ]
    source = SourceSpec(
        name="ns.payroll_source",
        catalog="warehouse",
        schema_="payroll",
        table="payroll",
        columns=source_columns,
        primary_key=["employee_id"],
    )
    cte_projection = ["department", "employee_id"]
    for index in range(1, 13):
        amount = f"amount_{index:02d}"
        cte_projection.append(
            f"{amount} + {first_adjustment} AS {amount}" if index == 1 else amount
        )
    cte_sql = ", ".join(cte_projection)
    aggregate_projection = ["department"] + [
        f"SUM(amount_{index:02d}) AS total_{index:02d}" for index in range(1, 13)
    ]
    group_by = (
        "employee_id, department"
        if group_employee and reverse_group_order
        else "department, employee_id"
        if group_employee
        else "department"
    )
    query = (
        f"WITH {cte_name} AS ("
        f"SELECT {cte_sql} FROM ns.payroll_source"
        ") SELECT "
        f"{', '.join(aggregate_projection)} FROM {cte_name} "
        f"GROUP BY {group_by} "
        f"HAVING COUNT(*) > {having_threshold}"
    )
    rollup_columns = [
        ColumnSpec(name="department", type="string"),
        *[
            ColumnSpec(name=f"total_{index:02d}", type="double")
            for index in range(1, 13)
        ],
    ]
    rollup = TransformSpec(
        name="ns.payroll_rollup",
        query=query,
        columns=rollup_columns,
        primary_key=["department"],
    )
    metrics = [
        MetricSpec(
            name=f"ns.payroll_total_{index:02d}",
            query=f"SELECT SUM(total_{index:02d}) FROM ns.payroll_rollup",
        )
        for index in range(1, 13)
    ]
    cubes = [
        CubeSpec(
            name=f"ns.payroll_cube_{index:02d}",
            metrics=[metric.rendered_name],
        )
        for index, metric in enumerate(metrics, start=1)
    ]
    nodes = [source, rollup, *metrics, *cubes]
    return {node.rendered_name: node for node in nodes}


def test_pass_through_cte_grouping_prunes_unrelated_aggregate_outputs():
    before = _evaluate(_rollup_specs(first_adjustment=0))
    after = _evaluate(_rollup_specs(first_adjustment=1))

    assert before.nodes["ns.payroll_rollup"].precision == "precise"
    assert after.nodes["ns.payroll_rollup"].precision == "precise"
    assert (
        compare_components(
            before.nodes["ns.payroll_rollup"].outputs["total_01"],
            after.nodes["ns.payroll_rollup"].outputs["total_01"],
        )
        == "changed"
    )
    for index in range(2, 13):
        output = f"total_{index:02d}"
        metric = f"ns.payroll_total_{index:02d}"
        assert (
            compare_components(
                before.nodes["ns.payroll_rollup"].outputs[output],
                after.nodes["ns.payroll_rollup"].outputs[output],
            )
            == "unchanged"
        )
        assert (
            compare_components(before.nodes[metric].full, after.nodes[metric].full)
            == "unchanged"
        )
    assert (
        compare_components(
            before.nodes["ns.payroll_total_01"].full,
            after.nodes["ns.payroll_total_01"].full,
        )
        == "changed"
    )
    changed_metrics = {
        name
        for name in (f"ns.payroll_total_{index:02d}" for index in range(1, 13))
        if compare_components(before.nodes[name].full, after.nodes[name].full)
        == "changed"
    }
    changed_cubes = {
        name
        for name in (f"ns.payroll_cube_{index:02d}" for index in range(1, 13))
        if compare_components(before.nodes[name].full, after.nodes[name].full)
        == "changed"
    }
    assert changed_metrics == {"ns.payroll_total_01"}
    assert changed_cubes == {"ns.payroll_cube_01"}


def test_group_by_and_having_change_the_population_dependencies():
    before = _evaluate(_rollup_specs(having_threshold=0))
    after = _evaluate(_rollup_specs(having_threshold=1))

    assert (
        compare_components(
            before.nodes["ns.payroll_rollup"].rows,
            after.nodes["ns.payroll_rollup"].rows,
        )
        == "changed"
    )
    for index in range(1, 13):
        metric = f"ns.payroll_total_{index:02d}"
        assert (
            compare_components(before.nodes[metric].full, after.nodes[metric].full)
            == "changed"
        )


def test_group_key_change_invalidates_aggregate_populations():
    before = _evaluate(_rollup_specs())
    after = _evaluate(_rollup_specs(group_employee=True))

    assert (
        compare_components(
            before.nodes["ns.payroll_rollup"].rows,
            after.nodes["ns.payroll_rollup"].rows,
        )
        == "changed"
    )
    for index in range(1, 13):
        metric = f"ns.payroll_total_{index:02d}"
        assert (
            compare_components(before.nodes[metric].full, after.nodes[metric].full)
            == "changed"
        )


def test_group_key_order_is_irrelevant():
    before = _evaluate(
        _rollup_specs(group_employee=True, reverse_group_order=False),
    )
    after = _evaluate(
        _rollup_specs(group_employee=True, reverse_group_order=True),
    )

    for name in ("ns.payroll_rollup", "ns.payroll_total_01", "ns.payroll_cube_01"):
        assert compare_components(before.nodes[name].full, after.nodes[name].full) == (
            "unchanged"
        )


def test_simple_cte_alias_rename_is_normalized():
    before = _evaluate(_rollup_specs())
    after = _evaluate(_rollup_specs(cte_name="staged"))

    for name in (
        "ns.payroll_rollup",
        "ns.payroll_total_01",
        "ns.payroll_cube_01",
    ):
        assert (
            compare_components(before.nodes[name].full, after.nodes[name].full)
            == "unchanged"
        )


def test_unsupported_cte_shape_keeps_conservative_fallback():
    unsupported = _rollup_specs()
    unsupported["ns.payroll_rollup"].query = unsupported[
        "ns.payroll_rollup"
    ].query.replace(
        "SELECT department, employee_id, amount_01 + 0 AS amount_01",
        "SELECT DISTINCT department, employee_id, amount_01 + 0 AS amount_01",
    )
    result = _evaluate(unsupported)

    assert result.nodes["ns.payroll_rollup"].precision != "precise"


@pytest.mark.parametrize(
    "rewrite",
    [
        lambda query: query.replace("WITH base AS", 'WITH "base" AS').replace(
            " FROM base ",
            ' FROM "base" ',
        ),
        lambda query: query.replace(
            "amount_01 + 0 AS amount_01",
            "amount_01 + 0",
        ),
    ],
    ids=["quoted-cte-name", "unaliased-computed-output"],
)
def test_ambiguous_cte_identifiers_and_output_names_fallback(rewrite):
    unsupported = _rollup_specs()
    unsupported["ns.payroll_rollup"].query = rewrite(
        unsupported["ns.payroll_rollup"].query,
    )
    result = _evaluate(unsupported)

    assert result.nodes["ns.payroll_rollup"].precision != "precise"


def test_numeric_literal_binding_follows_sql_order_across_cte_and_grouping():
    query = (
        "WITH prepared AS ("
        "SELECT amount + 0.25 AS adjusted FROM ns.numeric_source "
        "WHERE amount > 1.5) "
        "SELECT SUM(adjusted) AS total FROM prepared "
        "GROUP BY adjusted / 2.75 HAVING SUM(adjusted) > 3.5"
    )
    source = SourceSpec(
        name="ns.numeric_source",
        catalog="warehouse",
        schema_="payroll",
        table="numeric_source",
        columns=[ColumnSpec(name="amount", type="double")],
    )
    rollup = TransformSpec(
        name="ns.numeric_rollup",
        query=query,
        columns=[ColumnSpec(name="total", type="double")],
    )
    numeric_ast = rollup.query_ast
    literal_map = _numeric_literals(numeric_ast, rollup.rendered_query)

    assert sorted(literal_map.values()) == ["0.25", "1.5", "2.75", "3.5"]
    assert all(
        str(number) == literal_map[id(number)]
        for number in numeric_ast.find_all(ast.Number)
    )

    graph = _evaluate(
        {
            source.rendered_name: source,
            rollup.rendered_name: rollup,
        },
    )
    assert graph.nodes["ns.numeric_rollup"].precision == "precise"

    for old, new in (
        ("0.25", "0.5"),
        ("1.5", "1.6"),
        ("2.75", "2.8"),
        ("3.5", "3.6"),
    ):
        changed_rollup = TransformSpec(
            name="ns.numeric_rollup",
            query=query.replace(old, new),
            columns=[ColumnSpec(name="total", type="double")],
        )
        after = _evaluate(
            {
                source.rendered_name: source,
                changed_rollup.rendered_name: changed_rollup,
            },
        )
        assert after.nodes["ns.numeric_rollup"].precision == "precise"
        assert (
            compare_components(
                graph.nodes["ns.numeric_rollup"].full,
                after.nodes["ns.numeric_rollup"].full,
            )
            == "changed"
        )


def test_exponential_cte_expansion_falls_back_within_a_fixed_budget():
    ctes = ["base AS (SELECT value FROM ns.source)"]
    previous = "base"
    for index in range(1, 12):
        current = f"stage_{index}"
        ctes.append(
            f"{current} AS (SELECT value + value AS value FROM {previous})",
        )
        previous = current
    source = SourceSpec(
        name="ns.source",
        catalog="warehouse",
        schema_="payroll",
        table="source",
        columns=[ColumnSpec(name="value", type="double")],
    )
    transform = TransformSpec(
        name="ns.expanding",
        query=f"WITH {', '.join(ctes)} SELECT value FROM {previous}",
    )

    result = _evaluate(
        {source.rendered_name: source, transform.rendered_name: transform},
    )

    assert result.nodes["ns.expanding"].precision != "precise"
    assert "cte_expansion_limit" in result.nodes["ns.expanding"].reasons

    repeated_cte_expression = " + ".join(["value"] * 250)
    repeated_output_expression = " + ".join(["prepared_value"] * 10)
    top_level = TransformSpec(
        name="ns.top_level_expanding",
        query=(
            "WITH prepared AS ("
            f"SELECT {repeated_cte_expression} AS prepared_value FROM ns.source) "
            f"SELECT {repeated_output_expression} AS total FROM prepared"
        ),
    )
    top_level_result = _evaluate(
        {
            source.rendered_name: source,
            top_level.rendered_name: top_level,
        },
    )

    assert top_level_result.nodes["ns.top_level_expanding"].precision != "precise"
    assert (
        "cte_expansion_limit"
        in top_level_result.nodes["ns.top_level_expanding"].reasons
    )

    linear_ctes = ["base AS (SELECT value FROM ns.source)"]
    previous = "base"
    for index in range(1, 33):
        current = f"pass_{index}"
        linear_ctes.append(f"{current} AS (SELECT value FROM {previous})")
        previous = current
    linear = TransformSpec(
        name="ns.deep_ctes",
        query=f"WITH {', '.join(linear_ctes)} SELECT value FROM {previous}",
    )
    linear_result = _evaluate(
        {source.rendered_name: source, linear.rendered_name: linear},
    )

    assert linear_result.nodes["ns.deep_ctes"].precision != "precise"
    assert "cte_count_limit" in linear_result.nodes["ns.deep_ctes"].reasons
