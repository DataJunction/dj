"""Sketch extension SQL contracts; these do not establish native engine accuracy."""

import pytest
import sqlglot
from hypothesis import given, settings
from hypothesis import strategies as st
from sqlglot import exp

from tests.construction.build_v3.sketch_family_e2e_test import (
    digest_family as digest_family,
)
from tests.property.budget import examples
from tests.property.graph import build_graph, create_metric, post
from tests.property.interaction_scenarios import grouped_scenarios


@pytest.mark.asyncio
@pytest.mark.parametrize("dialect", ["spark", "trino", "druid"])
@settings(max_examples=examples(6))
@given(
    scenario=grouped_scenarios(),
    compression=st.integers(20, 500),
    quantile=st.integers(1, 99),
)
async def test_sketch_tuning_identity_and_combiner_contract(
    module__client_with_roads,
    duckdb_conn,
    digest_family,
    dialect,
    scenario,
    compression,
    quantile,
):
    client = module__client_with_roads
    graph = await build_graph(client, duckdb_conn, scenario)
    metrics = [
        await create_metric(
            client,
            graph,
            f"p{q}",
            f"SELECT APPROX_PERCENTILE(x, {q / 100}) FROM {graph.fact}",
            reaggregate={"fn": "tdigest", "params": {"compression": compression}},
        )
        for q in dict.fromkeys([quantile, 100 - quantile])
    ]
    response = await client.get(
        "/sql/measures/v3/",
        params={
            "metrics": metrics,
            "dimensions": [graph.dimension("label")],
            "dialect": dialect,
            "use_materialized": False,
        },
    )
    assert response.status_code == 200, response.json()
    body = response.json()
    # Different quantiles reuse one tuned state, not independently built states.
    components = [c for group in body["grain_groups"] for c in group["components"]]
    names = {c["name"] for c in components}
    assert len(names) == 1, body
    assert f"_digest_c{compression}_" in next(iter(names))
    assert all(c["merge"].lower() == "merge_digest" for c in components)
    quantiles = dict(zip(metrics, dict.fromkeys([quantile, 100 - quantile])))
    assert {f["name"] for f in body["metric_formulas"]} == set(metrics)
    for formula in body["metric_formulas"]:
        q = quantiles[formula["name"]]
        sql = formula["combiner"]
        parsed = sqlglot.parse_one("SELECT " + sql, read="spark")
        assert any(
            float(n.this) == q / 100
            for n in parsed.find_all(exp.Literal)
            if not n.is_string
        ), sql
        assert (
            "DIGEST_QUANTILE(" if dialect == "druid" else "digest_quantiles("
        ) in sql
    # Phase 1 is emitted in Spark even when the reader uses Druid. Its tuning
    # argument must survive as a literal, independently of the component name.
    for group in body["grain_groups"]:
        parsed = sqlglot.parse_one(group["sql"], read="spark")
        builds = [
            f
            for f in parsed.find_all(exp.Anonymous)
            if f.name.lower() == "build_digest"
        ]
        assert builds, group["sql"]
        assert all(
            len(f.expressions) == 2 and float(f.expressions[1].this.this) == compression
            for f in builds
        )
    plan = await post(
        client,
        "/preaggs/plan/",
        {"metrics": metrics, "dimensions": [graph.dimension("label")]},
    )
    tables = []
    for index, preagg in enumerate(plan["preaggs"]):
        table = f"{graph.fact_table}_digest_{index}"
        await post(
            client,
            f"/preaggs/{preagg['id']}/availability/",
            {
                "catalog": "default",
                "schema_": "pbt",
                "table": table,
                "valid_through_ts": 20990101,
            },
        )
        tables.append(table)
    response = await client.get(
        "/sql/measures/v3/",
        params={
            "metrics": metrics,
            "dimensions": [graph.dimension("label")],
            "dialect": dialect,
        },
    )
    assert response.status_code == 200, response.json()
    stored_sql = "\n".join(g["sql"] for g in response.json()["grain_groups"])
    assert tables and all(table in stored_sql for table in tables), stored_sql
    parsed = sqlglot.parse_one(stored_sql, read="spark")
    merges = [
        f for f in parsed.find_all(exp.Anonymous) if f.name.lower() == "merge_digest"
    ]
    assert merges and all(
        len(f.expressions) == 2 and float(f.expressions[1].this.this) == compression
        for f in merges
    ), stored_sql
    # A state built at another accuracy must never satisfy this request.
    incompatible = await create_metric(
        client,
        graph,
        "different_accuracy",
        f"SELECT APPROX_PERCENTILE(x, 0.5) FROM {graph.fact}",
        reaggregate={"fn": "tdigest", "params": {"compression": compression + 1}},
    )
    response = await client.get(
        "/sql/measures/v3/",
        params={
            "metrics": [incompatible],
            "dimensions": [graph.dimension("label")],
            "dialect": dialect,
        },
    )
    assert response.status_code == 200, response.json()
    raw_sql = "\n".join(g["sql"] for g in response.json()["grain_groups"])
    assert not any(table in raw_sql for table in tables), raw_sql
    assert "build_digest(" in raw_sql, raw_sql
