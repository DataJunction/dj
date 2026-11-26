"""Exact cross-fact aggregates must collapse identities before the fact join."""

from copy import deepcopy
from types import SimpleNamespace

import duckdb
import pytest

from datajunction_server.construction.build_v3.metrics import (
    _build_pre_agg_wrapper_cte,
    build_base_metric_expression,
    generate_metrics_sql,
)
from datajunction_server.construction.build_v3.types import (
    BuildContext,
    ColumnMetadata,
    DecomposedMetricInfo,
    GeneratedMeasuresSQL,
    GrainGroupSQL,
)
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.decompose import (
    Aggregability,
    AggregationRule,
    MetricComponent,
)
from datajunction_server.models.dialect import Dialect
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.reaggregate import (
    DimensionReaggregateRule,
    ReaggregationFunction,
)
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.sql.parsing.types import DoubleType


def component(name, aggregation=None, level=None, merge=None):
    return MetricComponent(
        name=name,
        expression=name,
        aggregation=aggregation,
        merge=merge,
        rule=AggregationRule(
            type=Aggregability.LIMITED if level else Aggregability.FULL,
            level=level,
        ),
    )


def metric(name, components, combiner):
    node = SimpleNamespace(
        name="test." + name,
        type=NodeType.METRIC,
        current=SimpleNamespace(
            query="SELECT " + combiner + " FROM test.fact",
            columns=[SimpleNamespace(type=DoubleType())],
        ),
    )
    return DecomposedMetricInfo(
        metric_node=node,
        components=components,
        aggregability=(
            Aggregability.LIMITED
            if any(c.rule.type == Aggregability.LIMITED for c in components)
            else Aggregability.FULL
        ),
        combiner=combiner,
        derived_ast=parse("SELECT " + combiner),
    )


def group(parent, sql, components, metrics, grain, merged=False):
    return GrainGroupSQL(
        query=parse(sql),
        columns=[
            ColumnMetadata(
                name=name,
                type="int",
                semantic_name="test.dim." + name,
                semantic_type="dimension",
            )
            for name in grain
        ],
        grain=grain,
        aggregability=(
            Aggregability.LIMITED
            if any(c.rule.type == Aggregability.LIMITED for c in components)
            else Aggregability.FULL
        ),
        metrics=["test." + name for name in metrics],
        parent_name="test." + parent,
        components=components,
        component_aliases={c.name: c.name for c in components},
        component_aggregabilities={c.name: c.rule.type for c in components},
        is_merged=merged,
        dialect=Dialect.DUCKDB,
    )


def scenario(*, own_full=False):
    volume = component("volume", "SUM", merge="SUM")
    visitors = component("account_id", level=["account_id"])
    mobile = component("mobile_id", level=["mobile_id"])
    components = [visitors, mobile]
    metrics = {
        "test.downloads": metric("downloads", [volume], "SUM(volume)"),
        "test.visitors": metric("visitors", [visitors], "COUNT(DISTINCT account_id)"),
        "test.mobile_visitors": metric(
            "mobile_visitors",
            [mobile],
            "COUNT(DISTINCT mobile_id)",
        ),
    }
    projection = ""
    if own_full:
        minutes = component("minutes", "SUM", merge="SUM")
        components.append(minutes)
        metrics["test.minutes"] = metric("minutes", [minutes], "SUM(minutes)")
        projection = ", SUM(minutes) AS minutes"
    groups = [
        group(
            "downloads",
            "SELECT day, SUM(volume) AS volume FROM downloads GROUP BY day",
            [volume],
            ["downloads"],
            ["day"],
        ),
        group(
            "visits",
            "SELECT day, account_id, mobile_id"
            + projection
            + " FROM visits GROUP BY day, account_id, mobile_id",
            components,
            ["visitors", "mobile_visitors"] + (["minutes"] if own_full else []),
            ["day", "account_id", "mobile_id"],
            merged=True,
        ),
    ]
    return groups, metrics


def compile_groups(groups, metrics, dimensions=("day",), filter_dimensions=()):
    refs = ["test.dim." + d for d in dimensions]
    ctx = BuildContext(
        session=None,
        metrics=list(metrics),
        dimensions=refs,
        dialect=Dialect.DUCKDB,
        nodes={name: m.metric_node for name, m in metrics.items()},
    )
    ctx.filter_dimensions = set(filter_dimensions)
    result = GeneratedMeasuresSQL(
        grain_groups=deepcopy(groups),
        dialect=Dialect.DUCKDB,
        requested_dimensions=refs,
        ctx=ctx,
        decomposed_metrics=metrics,
    )
    return generate_metrics_sql(ctx, result, metrics)


def evidence():
    conn = duckdb.connect()
    conn.execute("CREATE TABLE downloads(day INT, volume INT)")
    conn.execute("INSERT INTO downloads VALUES (1, 43)")
    conn.execute(
        "CREATE TABLE visits(day INT, account_id INT, mobile_id INT, minutes INT)",
    )
    conn.execute(
        "INSERT INTO visits VALUES "
        "(1, 101, 101, 5), (1, 101, 101, 7), "
        "(1, 102, 102, 11), (1, 103, NULL, 13), (1, NULL, NULL, 17)",
    )
    return conn


@pytest.mark.parametrize("reverse_groups", [False, True])
@pytest.mark.parametrize("reverse_metrics", [False, True])
@pytest.mark.parametrize("own_full", [False, True])
def test_merged_distincts_do_not_multiply_other_facts(
    reverse_groups,
    reverse_metrics,
    own_full,
):
    groups, metrics = scenario(own_full=own_full)
    if reverse_groups:
        groups.reverse()
    if reverse_metrics:
        metrics = dict(reversed(list(metrics.items())))
    result = compile_groups(groups, metrics)
    with evidence() as conn:
        rows = conn.execute(result.sql).fetchall()
    assert len(rows) == 1, result.sql
    actual = dict(zip([c.semantic_name for c in result.columns], rows[0]))
    assert actual == {
        "test.dim.day": 1,
        "test.downloads": 43,
        "test.visitors": 3,
        "test.mobile_visitors": 2,
        **({"test.minutes": 53} if own_full else {}),
    }, result.sql


@pytest.mark.parametrize(
    ("operator", "expected"),
    [("COUNT", 3), ("SUM", 306), ("AVG", 102)],
)
def test_distinct_operator_is_kept_without_wrapper(operator, expected):
    key = component("account_id", level=["account_id"])
    decomposed = metric("distinct_value", [key], operator + "(DISTINCT account_id)")
    gg = group(
        "visits",
        "SELECT account_id FROM visits",
        [key],
        ["distinct_value"],
        ["account_id"],
    )
    expression, _ = build_base_metric_expression(decomposed, "v", gg)
    with evidence() as conn:
        actual = conn.execute("SELECT " + str(expression) + " FROM visits v").fetchone()
    assert actual == (expected,)


@pytest.mark.parametrize("with_wrapper", [False, True])
@pytest.mark.parametrize(
    "combiner",
    [
        "COUNT(DISTINCT account_id) + 1",
        "COUNT(DISTINCT account_id) + COUNT(DISTINCT 0)",
    ],
)
def test_distinct_combiner_arithmetic_is_preserved(with_wrapper, combiner):
    groups, metrics = scenario()
    metrics["test.visitors"] = metric(
        "visitors",
        metrics["test.visitors"].components,
        combiner,
    )
    if not with_wrapper:
        groups = [groups[1]]
        del metrics["test.downloads"]
    result = compile_groups(groups, metrics)
    with evidence() as conn:
        expected = (1, 43, 4, 2) if with_wrapper else (1, 4, 2)
        assert conn.execute(result.sql).fetchall() == [expected], result.sql


def test_shared_component_is_aggregated_once_in_wrapper():
    groups, metrics = scenario(own_full=True)
    groups[1].components.extend(deepcopy(groups[1].components))
    result = compile_groups(groups, metrics)
    with evidence() as conn:
        assert conn.execute(result.sql).fetchall() == [(1, 43, 3, 2, 53)], result.sql


def test_direct_wrapper_keeps_legacy_count_contract():
    key = component("account_id", level=["account_id"])
    gg = group(
        "visits",
        "SELECT day, account_id FROM visits",
        [key],
        ["visitors"],
        ["day", "account_id"],
    )
    wrapper, _ = _build_pre_agg_wrapper_cte("visits", gg)
    with evidence() as conn:
        assert conn.execute("SELECT * FROM " + str(wrapper)).fetchall() == [(1, 3)]


async def api_post(client, path, payload):
    response = await client.post(path, json=payload)
    assert response.status_code in (200, 201), (path, response.json())
    return response.json()


async def api_model(client):
    for name, columns in {
        "cf_downloads": [("day", "int"), ("volume", "int")],
        "cf_visits": [
            ("day", "int"),
            ("account_id", "int"),
            ("platform", "string"),
            ("minutes", "int"),
        ],
        "cf_days": [("day", "int"), ("region", "string")],
    }.items():
        await api_post(
            client,
            "/nodes/source/",
            {
                "name": "default." + name,
                "catalog": "default",
                "schema_": "cross_fact",
                "table": name,
                "mode": "published",
                "columns": [{"name": n, "type": t} for n, t in columns],
            },
        )
    await api_post(
        client,
        "/nodes/dimension/",
        {
            "name": "default.cf_day",
            "query": "SELECT day, region FROM default.cf_days",
            "primary_key": ["day"],
            "mode": "published",
        },
    )
    for fact in ("cf_downloads", "cf_visits"):
        await api_post(
            client,
            "/nodes/default." + fact + "/link",
            {
                "dimension_node": "default.cf_day",
                "join_type": "left",
                "join_on": "default." + fact + ".day = default.cf_day.day",
            },
        )
    definitions = {
        "cf_volume": "SUM(volume) FROM default.cf_downloads",
        "cf_visitors": "COUNT(DISTINCT account_id) FROM default.cf_visits",
        "cf_mobile": (
            "COUNT(DISTINCT CASE WHEN platform = 'mobile' THEN account_id END)"
            " FROM default.cf_visits"
        ),
        "cf_minutes": "SUM(minutes) FROM default.cf_visits",
        "cf_average": "AVG(minutes) FROM default.cf_visits",
        "cf_distinct_sum": "SUM(DISTINCT account_id) FROM default.cf_visits",
        "cf_distinct_avg": "AVG(DISTINCT account_id) FROM default.cf_visits",
    }
    for name, definition in definitions.items():
        await api_post(
            client,
            "/nodes/metric/",
            {
                "name": "default." + name,
                "query": "SELECT " + definition,
                "mode": "published",
            },
        )


def api_evidence():
    conn = duckdb.connect()
    conn.execute("ATTACH ':memory:' AS \"default\"")
    conn.execute('CREATE SCHEMA "default".cross_fact')
    conn.execute(
        'CREATE TABLE "default".cross_fact.cf_days AS '
        "SELECT 1 AS day, 'north' AS region",
    )
    conn.execute(
        'CREATE TABLE "default".cross_fact.cf_downloads AS '
        "SELECT 1 AS day, 43 AS volume",
    )
    conn.execute(
        'CREATE TABLE "default".cross_fact.cf_visits '
        "(day INT, account_id INT, platform VARCHAR, minutes INT)",
    )
    conn.execute(
        'INSERT INTO "default".cross_fact.cf_visits VALUES '
        "(1, 101, 'mobile', 5), (1, 101, 'mobile', 7), "
        "(1, 102, 'mobile', 11), (1, 103, 'tv', 13), (1, NULL, 'tv', 17)",
    )
    return conn


async def api_sql(client, metrics, dimensions=("default.cf_day.day",), **options):
    response = await client.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["default." + m for m in metrics],
            "dimensions": list(dimensions),
            "dialect": "duckdb",
            "use_materialized": False,
            **options,
        },
    )
    assert response.status_code == 200, response.json()
    return response.json()["sql"]


@pytest.mark.parametrize("null_key", [False, True])
async def test_real_metrics_api_collapses_merged_distinct_group(client, null_key):
    await api_model(client)
    names = ["cf_volume", "cf_visitors", "cf_mobile", "cf_minutes", "cf_average"]
    with api_evidence() as conn:
        if not null_key:
            conn.execute(
                'DELETE FROM "default".cross_fact.cf_visits WHERE account_id IS NULL',
            )
        for requested in (names, names[::-1]):
            sql = await api_sql(client, requested)
            rows = conn.execute(sql).fetchall()
            assert len(rows) == 1, sql
            assert dict(zip(requested, rows[0][1:])) == {
                "cf_volume": 43,
                "cf_visitors": 3,
                "cf_mobile": 2,
                "cf_minutes": 53 if null_key else 36,
                "cf_average": 10.6 if null_key else 9,
            }, sql


@pytest.mark.parametrize("with_wrapper", [False, True])
@pytest.mark.parametrize(
    ("name", "expected"),
    [("cf_distinct_sum", 306), ("cf_distinct_avg", 102)],
)
async def test_real_extractor_distinct_operator(client, with_wrapper, name, expected):
    await api_model(client)
    names = ["cf_volume", name, "cf_mobile"] if with_wrapper else [name]
    sql = await api_sql(client, names)
    with api_evidence() as conn:
        rows = conn.execute(sql).fetchall()
    assert len(rows) == 1, sql
    expected_values = [43, expected, 2] if with_wrapper else [expected]
    assert rows == [(1, *expected_values)], sql


@pytest.mark.parametrize("name", ["cf_distinct_sum", "cf_distinct_avg"])
@pytest.mark.parametrize("derived", [False, True])
@pytest.mark.parametrize("reverse", [False, True])
async def test_shared_distinct_operator_conflict_is_explicit(
    client,
    name,
    derived,
    reverse,
):
    await api_model(client)
    metrics = ["default.cf_volume", "default.cf_visitors", "default." + name]
    if derived:
        await api_post(
            client,
            "/nodes/metric/",
            {
                "name": "default.cf_conflict",
                "query": "SELECT default.cf_visitors + default." + name,
                "mode": "published",
            },
        )
        metrics = ["default.cf_volume", "default.cf_conflict"]
    if reverse:
        metrics.reverse()
    response = await client.get(
        "/sql/metrics/v3/",
        params={
            "metrics": metrics,
            "dimensions": ["default.cf_day.day"],
            "dialect": "duckdb",
            "use_materialized": False,
        },
    )
    assert response.status_code in (400, 422), response.json()
    assert "distinct" in str(response.json()).lower()
    assert "conflict" in str(response.json()).lower()


@pytest.mark.parametrize(("operator", "expected"), [("SUM", 30), ("AVG", 15)])
@pytest.mark.parametrize("derived", ["direct", "same_fact", "cross_fact"])
@pytest.mark.parametrize("reverse", [False, True])
async def test_api_separate_fact_distinct_operators(
    client,
    operator,
    expected,
    derived,
    reverse,
):
    await api_model(client)
    await api_post(
        client,
        "/nodes/source/",
        {
            "name": "default.cf_purchases",
            "catalog": "default",
            "schema_": "cross_fact",
            "table": "cf_purchases",
            "mode": "published",
            "columns": [
                {"name": "day", "type": "int"},
                {"name": "account_id", "type": "int"},
            ],
        },
    )
    await api_post(
        client,
        "/nodes/default.cf_purchases/link",
        {
            "dimension_node": "default.cf_day",
            "join_type": "left",
            "join_on": "default.cf_purchases.day = default.cf_day.day",
        },
    )
    await api_post(
        client,
        "/nodes/metric/",
        {
            "name": "default.cf_purchase_value",
            "query": "SELECT " + operator + "(DISTINCT account_id)"
            " FROM default.cf_purchases",
            "mode": "published",
        },
    )
    name = "cf_purchase_value"
    if derived != "direct":
        name = "cf_purchase_derived"
        await api_post(
            client,
            "/nodes/metric/",
            {
                "name": "default." + name,
                "query": (
                    "SELECT default.cf_purchase_value * 2"
                    if derived == "same_fact"
                    else "SELECT default.cf_visitors + default.cf_purchase_value"
                ),
                "mode": "published",
            },
        )
        expected = expected * 2 if derived == "same_fact" else expected + 3
    names = ["cf_visitors", name]
    if reverse:
        names.reverse()
    with api_evidence() as conn:
        conn.execute(
            'CREATE TABLE "default".cross_fact.cf_purchases (day INT, account_id INT)',
        )
        conn.execute(
            'INSERT INTO "default".cross_fact.cf_purchases VALUES '
            "(1,10), (1,10), (1,20), (1,NULL)",
        )
        sql = await api_sql(client, names)
        rows = conn.execute(sql).fetchall()
    assert len(rows) == 1, sql
    assert rows[0][0] == 1, sql
    assert dict(zip(names, rows[0][1:])) == {"cf_visitors": 3, name: expected}, sql


@pytest.mark.parametrize("raw_metric", [False, True])
def test_unsafe_window_cross_fact_fanout_is_rejected(raw_metric):
    groups, metrics = scenario()
    if raw_metric:
        groups[0].query = parse("SELECT day, volume FROM downloads")
        groups[0].components = []
        groups[0].aggregability = Aggregability.NONE
        metrics["test.downloads"] = metric("downloads", [], "MAX_BY(volume, day)")
        metrics["test.downloads"].aggregability = Aggregability.NONE
    metrics["test.previous"] = metric(
        "previous",
        [],
        "LAG(test.downloads) OVER (ORDER BY test.dim.day)",
    )
    with pytest.raises(DJInvalidInputException, match="window.*fan.?out"):
        compile_groups(groups, metrics)


def test_single_fact_window_keeps_finer_grain_keys():
    groups, metrics = scenario()
    metrics = {
        name: value for name, value in metrics.items() if name != "test.downloads"
    }
    metrics["test.previous"] = metric(
        "previous",
        [],
        "LAG(test.visitors) OVER (ORDER BY test.dim.day)",
    )
    result = compile_groups([groups[1]], metrics)
    with evidence() as conn:
        assert conn.execute(result.sql).fetchall() == [(1, 3, 2, None)]


@pytest.mark.parametrize("operator", ["COUNT", "MIN", "MAX"])
def test_cross_fact_windows_keep_duplicate_insensitive_inputs(operator):
    groups, metrics = scenario()
    if operator == "COUNT":
        value = component("volume", level=["volume"])
        groups[0] = group(
            "downloads",
            "SELECT day, volume FROM downloads GROUP BY day, volume",
            [value],
            ["downloads"],
            ["day", "volume"],
        )
        metrics["test.downloads"] = metric(
            "downloads",
            [value],
            "COUNT(DISTINCT volume)",
        )
    else:
        value = component("volume", operator, merge=operator)
        groups[0].query = parse(
            f"SELECT day, {operator}(volume) AS volume FROM downloads GROUP BY day",
        )
        groups[0].components = [value]
        metrics["test.downloads"] = metric("downloads", [value], operator + "(volume)")
    metrics["test.previous"] = metric(
        "previous",
        [],
        "LAG(test.downloads) OVER (ORDER BY test.dim.day)",
    )
    result = compile_groups(groups, metrics)
    with evidence() as conn:
        assert conn.execute(result.sql).fetchall() == [
            (1, 1 if operator == "COUNT" else 43, 3, 2, None),
        ], result.sql


async def test_api_global_fixed_grain_uses_unique_join_inputs(client):
    await api_model(client)
    await api_post(
        client,
        "/nodes/metric/",
        {
            "name": "default.cf_global_volume",
            "query": "SELECT SUM(volume) FROM default.cf_downloads",
            "fixed_grain": [],
            "mode": "published",
        },
    )
    await api_post(
        client,
        "/nodes/metric/",
        {
            "name": "default.cf_global_per_visitor",
            "query": "SELECT default.cf_global_volume / default.cf_visitors",
            "mode": "published",
        },
    )
    with api_evidence() as conn:
        conn.execute("INSERT INTO \"default\".cross_fact.cf_days VALUES (2, 'north')")
        conn.execute('INSERT INTO "default".cross_fact.cf_downloads VALUES (2, 7)')
        conn.execute(
            'INSERT INTO "default".cross_fact.cf_visits VALUES '
            "(2, 101, 'mobile', 3), (2, 104, 'tv', 2)",
        )
        sql = await api_sql(client, ["cf_global_volume", "cf_visitors"])
        assert set(conn.execute(sql).fetchall()) == {(1, 50, 3), (2, 50, 2)}, sql
        sql = await api_sql(client, ["cf_global_per_visitor"])
        assert set(conn.execute(sql).fetchall()) == {(1, 50 / 3), (2, 25)}, sql


async def test_api_window_restriction_only_applies_to_cross_fact_finer_grain(client):
    await api_model(client)
    await api_post(
        client,
        "/nodes/metric/",
        {
            "name": "default.cf_previous",
            "query": "SELECT LAG(default.cf_volume) OVER (ORDER BY default.cf_day.day)",
            "mode": "published",
        },
    )
    response = await client.get(
        "/sql/metrics/v3/",
        params={
            "metrics": [
                "default.cf_volume",
                "default.cf_visitors",
                "default.cf_mobile",
                "default.cf_previous",
            ],
            "dimensions": ["default.cf_day.day"],
            "dialect": "duckdb",
            "use_materialized": False,
        },
    )
    assert response.status_code in (400, 422), response.json()
    assert "SQL window metrics with cross-fact fanout" in str(response.json())
    with api_evidence() as conn:
        for names, expected in [
            (["cf_volume", "cf_previous"], (1, 43, None)),
            (["cf_volume", "cf_minutes", "cf_previous"], (1, 43, 53, None)),
        ]:
            sql = await api_sql(client, names)
            assert conn.execute(sql).fetchall() == [expected], sql


def test_selected_distinct_key_and_multiple_dimensions_keep_output_grain():
    groups, metrics = scenario()
    groups[0].query = parse(
        "SELECT day, 'north' AS region, 101 AS account_id, SUM(volume) AS volume "
        "FROM downloads GROUP BY day",
    )
    groups[0].grain = ["day", "region", "account_id"]
    groups[1].query = parse(
        "SELECT day, CASE WHEN account_id IN (101, 102) THEN 'north' ELSE 'south' END "
        "AS region, account_id, mobile_id FROM visits "
        "GROUP BY day, region, account_id, mobile_id",
    )
    groups[1].grain = ["day", "region", "account_id", "mobile_id"]
    result = compile_groups(groups, metrics, dimensions=("day", "region", "account_id"))
    with evidence() as conn:
        rows = conn.execute(result.sql).fetchall()
    assert set(rows) == {
        (1, "north", 101, 43, 1, 1),
        (1, "north", 102, None, 1, 1),
        (1, "south", 103, None, 1, 0),
        (1, "south", None, None, 0, 0),
    }, result.sql


def test_filter_only_dimensions_collapse_before_cross_fact_join():
    groups, metrics = scenario()
    groups[0].query = parse(
        "SELECT 1 AS day, 'a' AS platform, 20 AS volume UNION ALL SELECT 1, 'b', 23",
    )
    groups[0].grain = ["day", "platform"]
    groups[1].query = parse(
        "SELECT day, CASE WHEN mobile_id IS NULL THEN 'tv' ELSE 'mobile' END AS platform, "
        "account_id, mobile_id FROM visits GROUP BY day, platform, account_id, mobile_id",
    )
    groups[1].grain = ["day", "platform", "account_id", "mobile_id"]
    result = compile_groups(
        groups,
        metrics,
        dimensions=("day", "platform"),
        filter_dimensions={"test.dim.platform"},
    )
    with evidence() as conn:
        assert conn.execute(result.sql).fetchall() == [(1, 43, 3, 2)], result.sql


@pytest.mark.parametrize("own_full", [False, True])
def test_wrapped_mixed_group_keeps_semi_additive_join_guard(own_full):
    groups, metrics = scenario(own_full=own_full)
    groups[0].query = parse(
        "SELECT 1 AS day, 1 AS snapshot_day, 20 AS volume UNION ALL SELECT 1, 2, 23",
    )
    groups[0].grain.append("snapshot_day")
    groups[0].reaggregate_dimension_aliases = {"volume": "snapshot_day"}
    groups[0].components[0].rule.reaggregate = DimensionReaggregateRule(
        dimension="test.dim.snapshot_day",
        fn=ReaggregationFunction.LAST_VALUE,
    )
    if own_full:
        # The protected grain would duplicate SUM(minutes) from the wrapper.
        with pytest.raises(DJInvalidInputException, match="Semi-additive live SQL"):
            compile_groups(groups, metrics)
    else:
        result = compile_groups(groups, metrics)
        with evidence() as conn:
            assert conn.execute(result.sql).fetchall() == [(1, 23, 3, 2)], result.sql


async def test_api_filter_only_day_preserves_exact_membership_across_days(client):
    await api_model(client)
    with api_evidence() as conn:
        conn.execute("INSERT INTO \"default\".cross_fact.cf_days VALUES (2, 'north')")
        conn.execute('INSERT INTO "default".cross_fact.cf_downloads VALUES (2, 7)')
        conn.execute(
            'INSERT INTO "default".cross_fact.cf_visits VALUES '
            "(2, 101, 'mobile', 3), (2, 104, 'tv', 2)",
        )
        sql = await api_sql(
            client,
            ["cf_volume", "cf_visitors", "cf_mobile", "cf_minutes"],
            dimensions=["default.cf_day.region"],
            filters=["default.cf_day.day BETWEEN 1 AND 2"],
        )
        assert conn.execute(sql).fetchall() == [("north", 50, 4, 2, 58)], sql


@pytest.mark.parametrize("unrelated_first", [False, True])
def test_wrapper_recovers_merge_metadata_and_fixed_arguments(unrelated_first):
    token = component("tokens", "STRING_AGG", merge="STRING_AGG")
    token.merge_args = ["'|'"]
    decomposed = metric("tokens", [token], "STRING_AGG(tokens, '|')")
    unrelated = metric(
        "other_tokens",
        [component("tokens", "MAX", merge="MAX")],
        "MAX(tokens)",
    )
    metrics = {"test.tokens": decomposed, "test.other_tokens": unrelated}
    if unrelated_first:
        metrics = dict(reversed(list(metrics.items())))
    planned = token.model_copy(deep=True)
    planned.aggregation = None
    planned.merge = None
    planned.merge_args = []
    gg = group(
        "visits",
        "SELECT 1 AS day, 101 AS account_id, 'a' AS tokens "
        "UNION ALL SELECT 1, 102, 'b'",
        [planned],
        ["tokens"],
        ["day", "account_id"],
        merged=True,
    )
    wrapper, _ = _build_pre_agg_wrapper_cte(
        "states",
        gg,
        ["day"],
        metrics,
    )
    with duckdb.connect() as conn:
        conn.execute("CREATE TABLE states(day INT, account_id INT, tokens VARCHAR)")
        conn.execute("INSERT INTO states VALUES (1,101,'a'), (1,102,'b')")
        assert conn.execute("SELECT * FROM " + str(wrapper)).fetchall() == [(1, "a|b")]


def test_wrapper_alias_reserves_later_components_and_dimensions():
    key = component("account_id", level=["account_id"])
    value = component("account_id_agg", "SUM", merge="SUM")
    gg = group(
        "visits",
        "SELECT account_id, mobile_id, minutes AS account_id_agg FROM visits",
        [key, value],
        [],
        ["account_id", "mobile_id"],
        merged=True,
    )
    wrapper, _ = _build_pre_agg_wrapper_cte("states", gg, ["account_id"])
    assert gg.component_aliases == {
        "account_id": "account_id_agg_1",
        "account_id_agg": "account_id_agg",
    }
    with evidence() as conn:
        conn.execute(
            "CREATE VIEW states AS SELECT account_id, minutes AS account_id_agg FROM visits",
        )
        assert set(conn.execute("SELECT * FROM " + str(wrapper)).fetchall()) == {
            (101, 1, 12),
            (102, 1, 11),
            (103, 1, 13),
            (None, 0, 17),
        }


def test_wrapper_rejects_missing_distinct_input():
    key = component("key", level=["key"])
    key.rule.level = None
    gg = group("visits", "SELECT 1 AS day", [key], [], ["day"])
    gg.component_aliases = {}
    with pytest.raises(DJInvalidInputException, match="Missing DISTINCT input"):
        _build_pre_agg_wrapper_cte("visits", gg)


@pytest.mark.parametrize("aggregability", [Aggregability.FULL, Aggregability.NONE])
def test_wrapper_rejects_components_without_safe_merge(aggregability):
    value = component("value", "SUM")
    value.rule.type = aggregability
    gg = group("visits", "SELECT 1 AS day, 4 AS value", [value], [], ["day"])
    with pytest.raises(DJInvalidInputException, match="Cannot pre-aggregate component"):
        _build_pre_agg_wrapper_cte("visits", gg)


def test_legacy_limited_operator_without_combiner_quantifier():
    key = component("account_id", "SUM", level=["account_id"])
    decomposed = metric("sum", [key], "SUM(account_id)")
    gg = group(
        "visits",
        "SELECT day, account_id FROM visits",
        [key],
        ["sum"],
        ["day", "account_id"],
    )
    expression, _ = build_base_metric_expression(decomposed, "v", gg)
    with evidence() as conn:
        assert conn.execute(
            "SELECT " + str(expression) + " FROM visits v",
        ).fetchone() == (306,)
    gg.is_pre_aggregated = True
    expression, _ = build_base_metric_expression(decomposed, "v", gg)
    with duckdb.connect() as conn:
        assert conn.execute(
            "SELECT " + str(expression) + " FROM (SELECT 306 AS account_id) v",
        ).fetchone() == (306,)
