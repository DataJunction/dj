"""CTE sharing must preserve each independently optimized query's bindings."""

from types import SimpleNamespace

import duckdb
import pytest

from datajunction_server.construction.build_v3.combiners import _create_ctes
from datajunction_server.construction.build_v3.metrics import (
    collect_and_build_ctes,
    generate_metrics_sql,
)
from datajunction_server.construction.build_v3.types import (
    BuildContext,
    ColumnMetadata,
    DecomposedMetricInfo,
    GeneratedMeasuresSQL,
    GrainGroupSQL,
)
from datajunction_server.models.decompose import (
    Aggregability,
    AggregationRule,
    MetricComponent,
)
from datajunction_server.models.dialect import Dialect
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse


def group(sql, parent="fact"):
    return GrainGroupSQL(
        query=parse(sql),
        columns=[],
        grain=[],
        aggregability=Aggregability.FULL,
        metrics=[],
        parent_name=parent,
    )


def combine(groups):
    ctes, aliases = collect_and_build_ctes(groups)
    query = parse(
        "SELECT "
        + ", ".join(f"{alias}.value" for alias in aliases)
        + " FROM "
        + " CROSS JOIN ".join(aliases),
    )
    query.ctes = ctes
    return ast.to_sql(query, Dialect.DUCKDB), ctes


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("same_output", [False, True])
def test_conflicting_ctes_keep_their_expressions(reverse, same_output):
    """Equal names, including equal output names, do not imply equal bodies."""
    right_column = "value" if same_output else "volume"
    groups = [
        group(
            "WITH window_cte AS (SELECT MAX(CASE WHEN cnt > 0 THEN 1 ELSE 0 END) AS value FROM evidence) "
            "SELECT value FROM window_cte",
            "membership",
        ),
        group(
            f"WITH window_cte AS (SELECT SUM(cnt) AS {right_column} FROM evidence) "
            f"SELECT {right_column} AS value FROM window_cte",
            "volume",
        ),
    ]
    if reverse:
        groups.reverse()
    original = [str(g.query) for g in groups]
    sql, _ = combine(groups)
    with duckdb.connect() as conn:
        conn.execute("CREATE TABLE evidence(cnt INT)")
        conn.execute("INSERT INTO evidence VALUES (2), (3), (0)")
        assert conn.execute(sql).fetchall() == ([(5, 1)] if reverse else [(1, 5)])
    assert [str(g.query) for g in groups] == original


@pytest.mark.parametrize("values", [(3, 7), (3, 3), (3, 7, 7)])
def test_reader_identity_includes_resolved_dependency(values):
    groups = [
        group(
            f"WITH upstream AS (SELECT {value} AS value), "
            "reader AS (SELECT upstream.value FROM upstream) "
            "SELECT reader.value FROM reader",
            f"fact{i}",
        )
        for i, value in enumerate(values)
    ]
    sql, ctes = combine(groups)
    with duckdb.connect() as conn:
        assert conn.execute(sql).fetchall() == [values]
    assert len(ctes) == 2 * len(set(values)) + len(values)


def test_aliases_qualified_columns_shadowing_and_physical_tables():
    groups = [
        group("WITH shared AS (SELECT 3 AS value) SELECT value FROM shared", "a"),
        group(
            """
            WITH shared AS (SELECT 7 AS value),
            reader AS (
                SELECT shared.value + x.value
                    + (SELECT shared.value FROM physical.shared) AS value
                FROM shared JOIN shared AS x ON shared.value = x.value
                WHERE EXISTS (
                    WITH shared AS (SELECT 100 AS value)
                    SELECT shared.value FROM shared WHERE shared.value = 100
                )
                AND EXISTS (SELECT 1 FROM physical.shared AS shared
                            WHERE shared.value = 11)
            )
            SELECT reader.value FROM reader
        """,
            "b",
        ),
    ]
    # The parser currently drops WITH inside EXISTS; build that scope explicitly.
    exists = next(groups[1].query.find_all(ast.UnaryOp))
    assert exists.op == ast.UnaryOpKind.Exists
    nested = parse(
        "WITH shared AS (SELECT 100 AS value) SELECT shared.value FROM shared "
        "WHERE shared.value = 100",
    )
    nested.parenthesized = True
    exists.expr = nested
    sql, _ = combine(groups)
    with duckdb.connect() as conn:
        conn.execute("CREATE SCHEMA physical")
        conn.execute("CREATE TABLE physical.shared(value INT)")
        conn.execute("INSERT INTO physical.shared VALUES (11)")
        assert conn.execute(str(groups[0].query)).fetchall() == [(3,)]
        assert conn.execute(str(groups[1].query)).fetchall() == [(25,)]
        assert conn.execute(sql).fetchall() == [(3, 25)]


def test_lifted_cte_does_not_capture_another_groups_physical_table():
    groups = [
        group("WITH shared AS (SELECT 3 AS value) SELECT value FROM shared", "a"),
        group("SELECT shared.value FROM shared", "b"),
    ]
    sql, _ = combine(groups)
    with duckdb.connect() as conn:
        conn.execute("CREATE TABLE shared(value INT)")
        conn.execute("INSERT INTO shared VALUES (11)")
        assert conn.execute(sql).fetchall() == [(3, 11)]


def test_generated_names_do_not_capture_later_names():
    groups = [
        group("WITH shared AS (SELECT 3 AS value) SELECT value FROM shared", "fact"),
        group(
            "WITH shared AS (SELECT 7 AS value), "
            "shared__1 AS (SELECT 100 AS value), "
            "fact_0 AS (SELECT 10 AS value), "
            "fact_1 AS (SELECT 20 AS value) "
            "SELECT shared.value + shared__1.value + fact_0.value + fact_1.value AS value "
            "FROM shared CROSS JOIN shared__1 CROSS JOIN fact_0 CROSS JOIN fact_1",
            "fact",
        ),
        group(
            "WITH shared__2 AS (SELECT 1000 AS value) SELECT value FROM shared__2",
            "fact_0",
        ),
    ]
    sql, ctes = combine(groups)
    names = [cte.alias.name.casefold() for cte in ctes]
    assert len(names) == len(set(names))
    with duckdb.connect() as conn:
        assert conn.execute(sql).fetchall() == [(3, 137, 1000)]


@pytest.mark.parametrize("binding", ["namespace", "relation", "cte"])
def test_compiled_column_bindings(binding):
    groups = [
        group(
            f"WITH shared AS (SELECT {value} AS value) SELECT shared.value FROM shared",
            f"fact{i}",
        )
        for i, value in enumerate([3, 7])
    ]
    for gg in groups:
        column = next(gg.query.select.find_all(ast.Column))
        if binding == "namespace":
            column.namespace_table()
        else:
            column.name.namespace = None
            column.add_table(
                gg.query.ctes[0]
                if binding == "cte"
                else next(gg.query.select.from_.find_all(ast.Table)),
            )
    sql, _ = combine(groups)
    with duckdb.connect() as conn:
        assert conn.execute(sql).fetchall() == [(3, 7)]


def test_relation_alias_wrapper_keeps_its_binding():
    groups = [
        group(
            f"WITH shared AS (SELECT {value} AS value) SELECT r.value FROM shared AS r",
            f"fact{i}",
        )
        for i, value in enumerate([3, 7])
    ]
    for gg in groups:
        relation = gg.query.select.from_.relations[0]
        table = relation.primary
        alias = table.alias
        table.alias = None
        relation.primary = ast.Alias(child=table, alias=alias)
    sql, _ = combine(groups)
    with duckdb.connect() as conn:
        assert conn.execute(sql).fetchall() == [(3, 7)]


def test_preaggregation_wrapper_does_not_capture_existing_names():
    limited = group("SELECT day, account_id FROM accounts", "fact")
    limited.aggregability = Aggregability.LIMITED
    limited.grain = ["day", "account_id"]
    limited.components = [
        MetricComponent(
            name="value",
            expression="account_id",
            aggregation="COUNT",
            rule=AggregationRule(type=Aggregability.LIMITED, level=["account_id"]),
        ),
    ]
    other = group(
        "WITH fact_0_agg AS (SELECT 9 AS value) SELECT value FROM fact_0_agg",
        "other",
    )
    sql, ctes = combine([limited, other])
    assert limited.is_pre_aggregated
    names = [cte.alias.name.casefold() for cte in ctes]
    assert len(names) == len(set(names))
    with duckdb.connect() as conn:
        conn.execute("CREATE TABLE accounts(day INT, account_id INT)")
        conn.execute("INSERT INTO accounts VALUES (1,1),(1,2),(1,2)")
        assert conn.execute(sql).fetchall() == [(2, 9)]


def test_fixed_wrapper_names_remain_available():
    groups = [
        group(
            "WITH base_metrics AS (SELECT 3 AS value), "
            "__metric_filtered AS (SELECT base_metrics.value + 4 AS value FROM base_metrics), "
            "__windowed AS (SELECT value * 2 AS value FROM __metric_filtered) "
            "SELECT value FROM __windowed",
        ),
    ]
    sql, ctes = combine(groups)
    assert not {"base_metrics", "__metric_filtered", "__windowed"} & {
        cte.alias.name for cte in ctes
    }
    with duckdb.connect() as conn:
        assert conn.execute(sql).fetchall() == [(14,)]


def test_quoted_cte_names_keep_quoting():
    groups = [
        group(
            f'WITH "shared name" AS (SELECT {value} AS value) '
            'SELECT value FROM "shared name"',
            f"fact{i}",
        )
        for i, value in enumerate([3, 7])
    ]
    sql, _ = combine(groups)
    with duckdb.connect() as conn:
        assert conn.execute(sql).fetchall() == [(3, 7)]


@pytest.mark.parametrize(
    "collision",
    ["base_metrics", "__metric_filtered", "fact_day", "fact_day_agg"],
)
@pytest.mark.parametrize("source", ["cte", "physical", "qualified"])
def test_window_and_filter_wrappers_avoid_existing_names(collision, source):
    """Execute the full final generator with collisions in every wrapper stage."""
    definitions = {
        "example.total": "SELECT SUM(cnt_sum)",
        "example.previous": "SELECT LAG(example.total) OVER (ORDER BY example.date.day)",
    }
    nodes = {
        name: SimpleNamespace(
            name=name,
            type=NodeType.METRIC,
            current=SimpleNamespace(query=query, columns=[SimpleNamespace(type="int")]),
        )
        for name, query in definitions.items()
    }
    component = MetricComponent(
        name="cnt_sum",
        expression="cnt",
        aggregation="SUM",
        rule=AggregationRule(type=Aggregability.FULL),
    )
    decomposed = {
        name: DecomposedMetricInfo(
            metric_node=nodes[name],
            components=[component] if name == "example.total" else [],
            aggregability=Aggregability.FULL,
            combiner=query,
            derived_ast=parse(query),
        )
        for name, query in definitions.items()
    }
    ctx = BuildContext(
        session=SimpleNamespace(),
        metrics=list(definitions),
        dimensions=["example.date.day"],
        dialect=Dialect.DUCKDB,
        nodes=nodes,
        parent_map={"example.previous": ["example.total"]},
        metric_filters=["example.total > 0"],
        decomposed_metrics=decomposed,
    )
    day = ColumnMetadata(
        name="day",
        semantic_name="example.date.day",
        type="int",
        semantic_type="dimension",
    )
    base = group(
        f"WITH {collision} AS (SELECT day, cnt_sum FROM evidence) "
        f"SELECT day, cnt_sum FROM {collision}",
        "fact",
    )
    table = {"cte": "evidence", "physical": collision}.get(source, f"q.{collision}")
    if source != "cte":
        base.query = parse(f"SELECT day, cnt_sum FROM {table}")
    base.grain = ["day"]
    base.columns = [day]
    base.metrics = ["example.total"]
    base.components = [component]
    window = group("SELECT day FROM evidence", "fact")
    window.columns = [day]
    window.is_window_grain_group = True
    window.window_order_by_dim = "example.date.day"
    window.window_metrics_served = ["example.previous"]
    measures = GeneratedMeasuresSQL(
        grain_groups=[base, window],
        dialect=Dialect.DUCKDB,
        requested_dimensions=ctx.dimensions,
        ctx=ctx,
    )
    generated = generate_metrics_sql(ctx, measures, decomposed)
    names = [cte.alias.name.casefold() for cte in generated.query.ctes]
    assert len(names) == len(set(names))
    # Window lookback densification looks up this fixed name. Only a relation
    # that a top-level CTE could actually capture may displace it.
    assert ("base_metrics" in names) == (
        collision != "base_metrics" or source != "physical"
    )
    with duckdb.connect() as conn:
        conn.execute("CREATE SCHEMA q")
        conn.execute(f"CREATE TABLE {table}(day INT, cnt_sum INT)")
        conn.execute(f"INSERT INTO {table} VALUES (1,2),(2,3)")
        assert sorted(conn.execute(generated.sql).fetchall()) == [
            (1, 2, None),
            (2, 3, 2),
        ]


def test_combined_measures_keep_variants_and_output_names():
    """The combined-measures path lifts grain-group CTEs the same way."""
    groups = [
        group(
            f"WITH shared AS (SELECT {value} AS value), "
            "gg1 AS (SELECT shared.value FROM shared) "
            "SELECT gg1.value FROM gg1",
            f"fact{i}",
        )
        for i, value in enumerate([3, 7])
    ]
    original = [str(g.query) for g in groups]
    ctes = _create_ctes(groups, ["gg1", "gg2"])
    names = [cte.alias.name.casefold() for cte in ctes]
    assert len(names) == len(set(names))
    assert names[-2:] == ["gg1", "gg2"]
    query = parse("SELECT gg1.value, gg2.value FROM gg1 CROSS JOIN gg2")
    query.ctes = ctes
    with duckdb.connect() as conn:
        assert conn.execute(ast.to_sql(query, Dialect.DUCKDB)).fetchall() == [(3, 7)]
    assert [str(g.query) for g in groups] == original


async def test_api_independent_pruning_keeps_membership_and_volume(client):
    """Exercise real nodes, pruning, metric composition and native DuckDB SQL."""
    ns = "cteidentity"
    response = await client.post(f"/namespaces/{ns}/")
    assert response.status_code in (200, 201), response.text
    response = await client.post(
        "/nodes/source/",
        json={
            "name": f"{ns}.evidence",
            "catalog": "default",
            "schema_": ns,
            "table": "evidence",
            "columns": [
                {"name": "day", "type": "int"},
                {"name": "account_id", "type": "int"},
                {"name": "cnt", "type": "int"},
            ],
        },
    )
    assert response.status_code in (200, 201), response.text
    for name, query, kind in [
        (
            "download_window",
            f"SELECT day, account_id, MAX(CASE WHEN cnt > 0 THEN 1 ELSE 0 END) AS downloaded_any, "
            f"SUM(cnt) AS download_cnt FROM {ns}.evidence GROUP BY day, account_id",
            "transform",
        ),
        (
            "membership",
            f"SELECT day, account_id, downloaded_any FROM {ns}.download_window",
            "transform",
        ),
        (
            "volume",
            f"SELECT day, account_id, download_cnt FROM {ns}.download_window",
            "transform",
        ),
        (
            "downloaded_accounts",
            f"SELECT SUM(downloaded_any) FROM {ns}.membership",
            "metric",
        ),
        ("downloads", f"SELECT SUM(download_cnt) FROM {ns}.volume", "metric"),
    ]:
        response = await client.post(
            f"/nodes/{kind}/",
            json={
                "name": f"{ns}.{name}",
                "query": query,
                "mode": "published",
            },
        )
        assert response.status_code in (200, 201), response.text

    response = await client.post(
        "/nodes/dimension/",
        json={
            "name": f"{ns}.date",
            "query": f"SELECT day FROM {ns}.evidence GROUP BY day",
            "primary_key": ["day"],
            "mode": "published",
        },
    )
    assert response.status_code in (200, 201), response.text
    for name in ["membership", "volume"]:
        response = await client.post(
            f"/nodes/{ns}.{name}/link",
            json={
                "dimension_node": f"{ns}.date",
                "join_on": f"{ns}.{name}.day = {ns}.date.day",
                "join_type": "left",
            },
        )
        assert response.status_code in (200, 201), response.text

    metrics = [f"{ns}.downloaded_accounts", f"{ns}.downloads"]
    response = await client.get(
        "/sql/measures/v3/",
        params={
            "metrics": metrics,
            "dimensions": [f"{ns}.date.day"],
            "dialect": "duckdb",
            "use_materialized": False,
        },
    )
    assert response.status_code == 200, response.text
    projections = []
    for gg in response.json()["grain_groups"]:
        cte = next(
            c for c in parse(gg["sql"]).ctes if c.alias.name == f"{ns}_download_window"
        )
        projections.append({str(expr.alias_or_name) for expr in cte.select.projection})
    assert len(projections) == 2
    assert any("downloaded_any" in p and "download_cnt" not in p for p in projections)
    assert any("download_cnt" in p and "downloaded_any" not in p for p in projections)

    with duckdb.connect() as conn:
        conn.execute("ATTACH ':memory:' AS \"default\"")
        conn.execute(f'CREATE SCHEMA "default".{ns}')
        conn.execute(
            f'CREATE TABLE "default".{ns}.evidence(day INT, account_id INT, cnt INT)',
        )
        conn.execute(
            f'INSERT INTO "default".{ns}.evidence VALUES (1,1,2),(1,1,3),(1,2,0),(2,1,4)',
        )
        for requested, expected in [
            ([metrics[0]], [(1, 1), (2, 1)]),
            ([metrics[1]], [(1, 5), (2, 4)]),
            (metrics, [(1, 1, 5), (2, 1, 4)]),
            (list(reversed(metrics)), [(1, 5, 1), (2, 4, 1)]),
        ]:
            response = await client.get(
                "/sql/metrics/v3/",
                params={
                    "metrics": requested,
                    "dimensions": [f"{ns}.date.day"],
                    "dialect": "duckdb",
                    "use_materialized": False,
                },
            )
            assert response.status_code == 200, response.text
            assert sorted(conn.execute(response.json()["sql"]).fetchall()) == expected

        # The combined-measures endpoint lifts the same grain-group CTEs.
        response = await client.get(
            "/sql/measures/v3/combined",
            params={
                "metrics": metrics,
                "dimensions": [f"{ns}.date.day"],
                "dialect": "duckdb",
            },
        )
        assert response.status_code == 200, response.text
        combined = conn.execute(response.json()["sql"])
        assert [column[0].split("_sum_")[0] for column in combined.description] == [
            "day",
            "downloaded_any",
            "download_cnt",
        ]
        assert sorted(combined.fetchall()) == [(1, 1, 5), (2, 1, 4)]
