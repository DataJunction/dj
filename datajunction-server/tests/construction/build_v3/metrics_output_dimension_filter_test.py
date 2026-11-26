"""Output predicates use the dimensions exposed by a full outer metric join."""

from dataclasses import replace
from datetime import date

import duckdb
import pytest
import pytest_asyncio
from datajunction_server.construction.build_v3 import builder
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.utils import get_session
from sqlalchemy.ext.asyncio import async_sessionmaker

from tests.conftest import patch_session_contexts

D1, D2, D3, D4 = (date(2024, 1, day) for day in range(1, 5))
DAY = "default.evidence_calendar.day"
REGION = "default.evidence_calendar.region"
DOWNLOADS = "default.evidence_downloads"
VISITORS = "default.evidence_visitors"
PURCHASES = "default.evidence_purchases"
RUNNING_VISITORS = "default.evidence_running_visitors"
LEFT_DOWNLOADS = "default.evidence_left_downloads"
LEFT_VISITORS = "default.evidence_left_visitors"
BOOLEAN_DAY = "default.evidence_boolean_calendar.day"
ENABLED = "default.evidence_boolean_calendar.enabled"
BOOLEAN_DOWNLOADS = "default.evidence_boolean_downloads"
BOOLEAN_VISITORS = "default.evidence_boolean_visitors"


async def post(client, path, body):
    response = await client.post(path, json=body)
    assert response.status_code in (200, 201), response.text
    return response.json()


@pytest.fixture(scope="module")
def evidence_api(module__clean_client):
    """Keep real background writes in this module's clean API database."""
    client = module__clean_client
    session = client.app.dependency_overrides[get_session]()
    session_factory = async_sessionmaker(bind=session.bind, expire_on_commit=False)

    async def new_session():
        return session_factory()

    # Request sessions are isolated by the clean client. Background jobs also
    # need its database, rather than a prior module's global session manager.
    with patch_session_contexts(new_session):
        yield client


@pytest_asyncio.fixture(scope="module")
async def evidence_client(evidence_api):
    """Install invented, independent facts through the real database-backed API."""
    client = evidence_api
    await post(client, "/namespaces/default/", {})

    async def source(name, table, columns):
        await post(
            client,
            "/nodes/source/",
            {
                "name": name,
                "catalog": "default",
                "schema_": "evidence",
                "table": table,
                "mode": "published",
                "columns": [{"name": name, "type": type_} for name, type_ in columns],
            },
        )

    await source(
        "default.evidence_dates",
        "dates",
        [
            ("k", "int"),
            ("day", "date"),
            ("region", "string"),
            ("enabled", "int"),
        ],
    )
    await post(
        client,
        "/nodes/dimension/",
        {
            "name": "default.evidence_calendar",
            "query": "SELECT k, day, region, enabled FROM default.evidence_dates",
            "primary_key": ["k"],
            "mode": "published",
        },
    )
    for fact, metric, join_type in [
        ("downloads", DOWNLOADS, "inner"),
        ("visits", VISITORS, "inner"),
        ("purchases", PURCHASES, "inner"),
        ("downloads", LEFT_DOWNLOADS, "left"),
        ("visits", LEFT_VISITORS, "left"),
    ]:
        node = f"default.evidence_{fact}_{join_type}_fact"
        await source(node, fact, [("k", "int"), ("amount", "int")])
        await post(
            client,
            f"/nodes/{node}/link",
            {
                "dimension_node": "default.evidence_calendar",
                # Inner lookups isolate the NULL cases from null extension
                # during pushdown; left lookups cover the original regression.
                # Metric groups are always combined with FULL OUTER JOIN.
                "join_type": join_type,
                "join_on": f"{node}.k = default.evidence_calendar.k",
            },
        )
        await post(
            client,
            "/nodes/metric/",
            {
                "name": metric,
                "query": f"SELECT SUM(amount) FROM {node}",
            },
        )
    await post(
        client,
        "/nodes/metric/",
        {
            "name": RUNNING_VISITORS,
            "query": f"SELECT SUM({VISITORS}) OVER (ORDER BY {DAY} "
            "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)",
            "required_dimensions": [DAY],
        },
    )
    return client


@pytest.fixture
def evidence_db():
    """Execute native DuckDB output unchanged against explicit literal evidence."""
    with duckdb.connect() as conn:
        conn.execute("""ATTACH ':memory:' AS "default" """)
        conn.execute('CREATE SCHEMA "default".evidence')
        conn.execute("""CREATE TABLE "default".evidence.dates AS
            SELECT * FROM (VALUES
                (1, DATE '2024-01-01', 'east', 1),
                (2, DATE '2024-01-02', 'east', 1),
                (3, DATE '2024-01-03', 'east', 0),
                (4, DATE '2024-01-04', 'west', 1),
                (5, NULL::DATE, 'east', 1),
                (6, DATE '2024-01-01', 'west', 0),
                (7, NULL::DATE, 'west', 1)
            ) AS t(k, day, region, enabled)""")
        for fact, rows in [
            ("downloads", "(1, 2), (3, 4), (5, 5), (6, 11)"),
            ("visits", "(2, 1), (3, 3), (5, 6), (7, 9)"),
            ("purchases", "(4, 7), (3, 8)"),
        ]:
            conn.execute(
                f'CREATE TABLE "default".evidence.{fact} AS '
                f"SELECT * FROM (VALUES {rows}) AS t(k, amount)",
            )
        yield conn


async def metrics_sql(client, metrics, dimensions, filters, dialect="duckdb"):
    response = await client.get(
        "/sql/metrics/v3/",
        params={
            "metrics": metrics,
            "dimensions": dimensions,
            "filters": filters,
            "dialect": dialect,
            "use_materialized": False,
        },
    )
    assert response.status_code == 200, response.text
    return response.json()["sql"]


@pytest_asyncio.fixture(scope="module")
async def boolean_client(evidence_client):
    await post(
        evidence_client,
        "/nodes/source/",
        {
            "name": "default.evidence_boolean_dates",
            "catalog": "default",
            # Match the dimension's physical and semantic table qualifiers so
            # this isolates output filtering from dimension-CTE qualification.
            "schema_": "default",
            "table": "evidence_boolean_calendar",
            "columns": [
                {"name": "k", "type": "int"},
                {"name": "day", "type": "date"},
                {"name": "enabled", "type": "boolean"},
            ],
        },
    )
    await post(
        evidence_client,
        "/nodes/dimension/",
        {
            "name": "default.evidence_boolean_calendar",
            "query": "SELECT k, day, enabled FROM default.evidence_boolean_dates",
            "primary_key": ["k"],
        },
    )
    for fact, metric in [
        ("downloads", BOOLEAN_DOWNLOADS),
        ("visits", BOOLEAN_VISITORS),
    ]:
        node = f"default.evidence_boolean_{fact}_fact"
        await post(
            evidence_client,
            "/nodes/source/",
            {
                "name": node,
                "catalog": "default",
                "schema_": "evidence",
                "table": f"boolean_{fact}",
                "columns": [
                    {"name": "k", "type": "int"},
                    {"name": "amount", "type": "int"},
                ],
            },
        )
        await post(
            evidence_client,
            f"/nodes/{node}/link",
            {
                "dimension_node": "default.evidence_boolean_calendar",
                "join_type": "left",
                "join_on": f"{node}.k = default.evidence_boolean_calendar.k",
            },
        )
        await post(
            evidence_client,
            "/nodes/metric/",
            {"name": metric, "query": f"SELECT SUM(amount) FROM {node}"},
        )
    return evidence_client


@pytest.fixture
def boolean_db():
    with duckdb.connect() as conn:
        conn.execute("""ATTACH ':memory:' AS "default" """)
        conn.execute('CREATE SCHEMA "default"."default"')
        conn.execute('CREATE SCHEMA "default".evidence')
        conn.execute("""CREATE TABLE "default"."default".evidence_boolean_calendar AS
            SELECT * FROM (VALUES
                (1, DATE '2024-01-01', TRUE),
                (2, DATE '2024-01-02', TRUE),
                (3, DATE '2024-01-03', FALSE),
                (4, DATE '2024-01-04', NULL::BOOLEAN)
            ) AS t(k, day, enabled)""")
        conn.execute("""CREATE TABLE "default".evidence.boolean_downloads AS
            SELECT 1 AS k, 2 AS amount""")
        conn.execute("""CREATE TABLE "default".evidence.boolean_visits AS
            SELECT * FROM (VALUES (2, 1), (3, 3), (4, 7)) AS t(k, amount)""")
        yield conn


@pytest.mark.asyncio
@pytest.mark.parametrize("reverse_metrics", [False, True])
@pytest.mark.parametrize(
    "filters, expected",
    [
        ([ENABLED], [(D1, True, 2, None), (D2, True, None, 1)]),
        ([f"{ENABLED} = TRUE"], [(D1, True, 2, None), (D2, True, None, 1)]),
        ([f"NOT {ENABLED}"], [(D3, False, None, 3)]),
        (
            [ENABLED, f"{BOOLEAN_DAY} IS NOT NULL"],
            [(D1, True, 2, None), (D2, True, None, 1)],
        ),
    ],
    ids=["bare", "explicit-true", "not", "compound"],
)
async def test_boolean_output_filter(
    boolean_client,
    boolean_db,
    compiler_groups,
    reverse_metrics,
    filters,
    expected,
):
    metrics = [BOOLEAN_DOWNLOADS, BOOLEAN_VISITORS]
    if reverse_metrics:
        metrics.reverse()
        expected = [
            (day, enabled, visits, downloads)
            for day, enabled, downloads, visits in expected
        ]
    sql = await metrics_sql(
        boolean_client,
        metrics,
        [BOOLEAN_DAY, ENABLED],
        filters,
    )
    assert sorted(boolean_db.execute(sql).fetchall()) == expected, sql


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "predicate",
    [
        f"{DAY} = '2024-01-02'",
        f"{DAY} BETWEEN '2024-01-02' AND '2024-01-02'",
    ],
)
async def test_visit_only_day_survives_output_filter(
    evidence_client,
    evidence_db,
    predicate,
):
    sql = await metrics_sql(
        evidence_client,
        [LEFT_DOWNLOADS, LEFT_VISITORS],
        [DAY],
        [predicate],
    )
    assert evidence_db.execute(sql).fetchall() == [(D2, None, 1)], sql


@pytest.fixture(params=[False, True], ids=["forward-groups", "reverse-groups"])
def compiler_groups(request, monkeypatch):
    """Change actual compiler inputs, independently of requested metric order."""
    generate = builder.generate_metrics_sql
    seen = []

    def generate_in_order(ctx, measures_result, *args, **kwargs):
        original = [tuple(group.metrics) for group in measures_result.grain_groups]
        groups = measures_result.grain_groups
        if request.param:
            groups = list(reversed(groups))
        assert [tuple(group.metrics) for group in groups] == (
            list(reversed(original)) if request.param else original
        )
        seen.append(original)
        return generate(
            ctx,
            replace(measures_result, grain_groups=groups),
            *args,
            **kwargs,
        )

    monkeypatch.setattr(builder, "generate_metrics_sql", generate_in_order)
    yield
    assert seen, "The real API must reach the compiler under test"
    assert all(len(groups) >= 2 for groups in seen)


@pytest.mark.asyncio
@pytest.mark.parametrize("reverse_metrics", [False, True])
@pytest.mark.parametrize(
    "predicate, expected",
    [
        (f"{DAY} = '2024-01-01'", [(D1, 13, None)]),
        (f"{DAY} = '2024-01-02'", [(D2, None, 1)]),
        (f"{DAY} = '2024-01-03'", [(D3, 4, 3)]),
        (f"{DAY} = '2024-01-04'", []),
        (
            f"{DAY} BETWEEN '2024-01-01' AND '2024-01-03'",
            [(D1, 13, None), (D2, None, 1), (D3, 4, 3)],
        ),
        (f"{DAY} BETWEEN '2024-01-02' AND '2024-01-02'", [(D2, None, 1)]),
        (f"{DAY} BETWEEN '2024-02-01' AND '2024-02-03'", []),
        (f"{DAY} IN ('2024-01-02', '2024-01-04')", [(D2, None, 1)]),
        (f"{DAY} NOT IN ('2024-01-01')", [(D2, None, 1), (D3, 4, 3)]),
    ],
)
async def test_sparse_dates_in_both_metric_and_group_orders(
    evidence_client,
    evidence_db,
    compiler_groups,
    reverse_metrics,
    predicate,
    expected,
):
    metrics = [VISITORS, DOWNLOADS] if reverse_metrics else [DOWNLOADS, VISITORS]
    sql = await metrics_sql(evidence_client, metrics, [DAY], [predicate])
    if reverse_metrics:
        expected = [(day, visitors, downloads) for day, downloads, visitors in expected]
    assert sorted(evidence_db.execute(sql).fetchall()) == expected, sql


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "predicate, expected",
    [
        (
            f"{DAY} BETWEEN '2024-01-01' AND '2024-01-04'",
            [
                (D1, "east", 2, None, None),
                (D1, "west", 11, None, None),
                (D2, "east", None, 1, None),
                (D3, "east", 4, 3, 8),
                (D4, "west", None, None, 7),
            ],
        ),
        (
            (
                f"({DAY} = '2024-01-02' AND {REGION} = 'east') OR "
                f"({DAY} = '2024-01-04' AND {REGION} = 'west')"
            ),
            [
                (D2, "east", None, 1, None),
                (D4, "west", None, None, 7),
            ],
        ),
        (f"{DAY} IS NULL AND {REGION} = 'west'", [(None, "west", None, 9, None)]),
        (f"{DAY} IS NULL AND {REGION} = 'east'", [(None, "east", 5, 6, None)]),
    ],
)
async def test_three_groups_and_multiple_dimensions(
    evidence_client,
    evidence_db,
    compiler_groups,
    predicate,
    expected,
):
    sql = await metrics_sql(
        evidence_client,
        [DOWNLOADS, VISITORS, PURCHASES],
        [DAY, REGION],
        [predicate],
    )
    assert sorted(evidence_db.execute(sql).fetchall()) == expected, sql


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "predicate, expected",
    [
        (f"{DAY} IS NULL", [(None, 5, 15)]),
        (f"{DAY} IS NOT NULL", [(D1, 13, None), (D2, None, 1), (D3, 4, 3)]),
        (f"{DAY} = '2024-01-02' OR {DAY} IS NULL", [(None, 5, 15), (D2, None, 1)]),
    ],
)
async def test_null_output_dimensions(
    evidence_client,
    evidence_db,
    compiler_groups,
    predicate,
    expected,
):
    sql = await metrics_sql(
        evidence_client,
        [DOWNLOADS, VISITORS],
        [DAY],
        [predicate],
    )
    actual = evidence_db.execute(sql).fetchall()
    assert sorted(actual, key=repr) == sorted(expected, key=repr), sql


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "enabled, expected",
    [
        ("= 1", [(D1, 2, None), (D2, None, 1)]),
        ("IN (0, 1)", [(D1, 13, None), (D2, None, 1), (D3, 4, 3)]),
    ],
)
async def test_filter_only_dimension_does_not_change_output_grain(
    evidence_client,
    evidence_db,
    compiler_groups,
    enabled,
    expected,
):
    sql = await metrics_sql(
        evidence_client,
        [DOWNLOADS, VISITORS],
        [DAY],
        [
            f"default.evidence_calendar.enabled {enabled}",
            f"{DAY} BETWEEN '2024-01-01' AND '2024-01-03'",
        ],
    )
    cursor = evidence_db.execute(sql)
    assert len(cursor.description) == 3
    assert sorted(cursor.fetchall()) == expected, sql
    query = parse(sql)
    assert "enabled" not in str(query.select.where)
    assert "enabled" not in str(query.select.group_by)


@pytest.mark.asyncio
async def test_single_group_keeps_qualified_dimension_reference(
    evidence_client,
    evidence_db,
):
    sql = await metrics_sql(
        evidence_client,
        [VISITORS],
        [DAY],
        [f"{DAY} = '2024-01-02'"],
    )
    assert evidence_db.execute(sql).fetchall() == [(D2, 1)], sql
    query = parse(sql)
    columns = list(query.select.where.find_all(ast.Column))
    assert columns and all(col.name.namespace is not None for col in columns)
    assert "COALESCE" not in str(query.select.where)


@pytest.mark.asyncio
async def test_metric_filter_stays_after_aggregation(
    evidence_client,
    evidence_db,
    compiler_groups,
):
    sql = await metrics_sql(
        evidence_client,
        [DOWNLOADS, VISITORS],
        [DAY],
        [
            f"{DAY} BETWEEN '2024-01-01' AND '2024-01-03'",
            f"{VISITORS} >= 2",
        ],
    )
    assert evidence_db.execute(sql).fetchall() == [(D3, 4, 3)], sql
    query = parse(sql)
    assert query.select.having is not None
    assert "SUM" in str(query.select.having)


@pytest.mark.asyncio
async def test_window_stage_uses_already_coalesced_dimensions(
    evidence_client,
    evidence_db,
    compiler_groups,
):
    sql = await metrics_sql(
        evidence_client,
        [DOWNLOADS, VISITORS, RUNNING_VISITORS],
        [DAY],
        [
            f"{DAY} BETWEEN '2024-01-01' AND '2024-01-03'",
            f"{RUNNING_VISITORS} > 1",
        ],
    )
    assert evidence_db.execute(sql).fetchall() == [(D3, 4, 3, 4)], sql
    assert "base_metrics" in sql


@pytest.mark.asyncio
@pytest.mark.parametrize("dialect", ["spark", "trino"])
async def test_rendered_filter_matches_projected_dimensions(
    evidence_client,
    compiler_groups,
    dialect,
):
    """Generation contracts only; these engines are not executed here."""
    sql = await metrics_sql(
        evidence_client,
        [DOWNLOADS, VISITORS, PURCHASES],
        [DAY, REGION],
        [f"{DAY} = '2024-01-04' AND {REGION} = 'west'"],
        dialect=dialect,
    )
    query = parse(sql)
    projected = {str(col.child) for col in query.select.projection[:2]}
    functions = list(query.select.where.find_all(ast.Function))
    assert {str(func) for func in functions} == projected
    assert all(
        func.name.name.upper() == "COALESCE" and len(func.args) == 3
        for func in functions
    )
