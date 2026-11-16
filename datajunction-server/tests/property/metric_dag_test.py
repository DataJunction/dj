"""Shared, nested metric formulas must survive staging and request ordering."""

from dataclasses import replace

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from tests.property.budget import examples
from tests.property.graph import (
    assert_same_results,
    build_graph,
    create_metric,
    reference,
)
from tests.property.interaction_graph import query
from tests.property.interaction_scenarios import formula_dags, grouped_scenarios
from tests.property.scenario import METRICS


@pytest.mark.asyncio
@pytest.mark.parametrize("filtered", [False, True])
@settings(max_examples=examples(10))
@given(
    scenario=grouped_scenarios(),
    formulas=formula_dags(),
    bases=st.tuples(st.sampled_from(METRICS), st.sampled_from(METRICS)),
    threshold=st.integers(-5, 5),
)
async def test_nested_shared_metrics_match_formula_evaluation(
    module__client_with_roads,
    duckdb_conn,
    filtered,
    scenario,
    formulas,
    bases,
    threshold,
):
    scenario = replace(scenario, metrics=bases)
    client, conn = module__client_with_roads, duckdb_conn
    graph = await build_graph(client, conn, scenario)
    metrics = [
        await create_metric(
            client,
            graph,
            f"base{i}",
            f"SELECT {m.definition} FROM {graph.fact}",
        )
        for i, m in enumerate(bases)
    ]
    for i, formula in enumerate(formulas):
        metrics.append(
            await create_metric(
                client,
                graph,
                f"derived{i}",
                f"SELECT {metrics[formula.left]} {formula.op} {metrics[formula.right]} AS value",
            ),
        )
    reference_sql, base_values = reference(
        conn,
        graph,
        scenario,
        ", ".join(m.reference for m in bases),
    )
    expected = {}
    for group, parents in base_values.items():
        values = list(parents)
        for formula in formulas:
            values.append(formula.evaluate(values))
        if not filtered or (values[-1] is not None and values[-1] > threshold):
            expected[group] = tuple(values)
    dimensions = [graph.dimension(d) for d in scenario.dimensions]
    filters = [f"{metrics[-1]} > {threshold}"] if filtered else []
    # Root-only traverses implicit dependencies; all/reversed add parents to the
    # output and change cache traversal order. Compare every requested value.
    for indices in (
        [len(metrics) - 1],
        list(range(len(metrics))),
        list(reversed(range(len(metrics)))),
    ):
        sql, actual = await query(
            client,
            conn,
            [metrics[i] for i in indices],
            dimensions,
            filters,
        )
        assert_same_results(
            actual,
            {g: tuple(v[i] for i in indices) for g, v in expected.items()},
            f"{scenario!r}\n{formulas!r}\n{reference_sql}\n{sql}",
        )
