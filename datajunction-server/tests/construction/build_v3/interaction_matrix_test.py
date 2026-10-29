"""SQL contracts for feature interactions; see interaction_matrix.md for scope.

The core table covers every pair of its four axes in twenty requests. Grain,
window, and routing interactions are separate because they have constraints
and need assertions about particular SQL stages, not just successful builds.
"""

from dataclasses import dataclass
from itertools import combinations, product

import duckdb
import pytest
import pytest_asyncio
import sqlglot
from sqlglot import exp

from tests.examples import BUILD_V3

from .projection_invariant import unprojected_references


AGGREGATIONS = {
    "sum": "v3.total_revenue",
    "average": "v3.avg_unit_price",
    "distinct": "v3.order_count",
    "raw": "v3.top_product_by_revenue",
    "conditional": "v3.completed_order_revenue",
}
CATEGORY = "v3.product.category"
SUBCATEGORY = "v3.product.subcategory"
DAY = "v3.date.date_id[order]"
WEEK = "v3.date.week[order]"


class SQLExpressionMismatch(AssertionError):
    """A derived expression disagrees with its independently selected parents."""


@dataclass(frozen=True)
class Case:
    aggregation: str
    composition: str
    facts: str
    filter: str

    @property
    def id(self):
        return "-".join(self.values)

    @property
    def values(self):
        return (self.aggregation, self.composition, self.facts, self.filter)


# Deliberately explicit: adding/removing a row is reviewable, and the coverage
# assertion below reports the precise pairs that a change leaves uncovered.
CASES = [
    Case("sum", "base", "same", "none"),
    Case("sum", "derived", "same", "grouped"),
    Case("sum", "nested", "cross", "filter_only"),
    Case("sum", "base", "cross", "having"),
    Case("average", "derived", "cross", "none"),
    Case("average", "nested", "cross", "grouped"),
    Case("average", "base", "same", "filter_only"),
    Case("average", "derived", "same", "having"),
    Case("distinct", "nested", "same", "none"),
    Case("distinct", "base", "same", "grouped"),
    Case("distinct", "derived", "cross", "filter_only"),
    Case("distinct", "nested", "cross", "having"),
    Case("raw", "base", "cross", "none"),
    Case("raw", "derived", "cross", "grouped"),
    Case("raw", "nested", "same", "filter_only"),
    Case("raw", "base", "same", "having"),
    Case("conditional", "derived", "same", "none"),
    Case("conditional", "nested", "same", "grouped"),
    Case("conditional", "base", "cross", "filter_only"),
    Case("conditional", "derived", "cross", "having"),
]


def test_core_matrix_covers_every_pair():
    """Prevent edits to the case table from silently removing pair coverage."""
    axes = {
        "aggregation": tuple(AGGREGATIONS),
        "composition": ("base", "derived", "nested"),
        "facts": ("same", "cross"),
        "filter": ("none", "grouped", "filter_only", "having"),
    }
    missing = []
    for left, right in combinations(axes, 2):
        covered = {(getattr(c, left), getattr(c, right)) for c in CASES}
        missing.extend(
            (left, a, right, b)
            for a, b in product(axes[left], axes[right])
            if (a, b) not in covered
        )
    assert not missing, f"Uncovered SQL-generation pairs: {missing}"
    assert len({c.values for c in CASES}) == len(CASES)


@pytest.fixture(scope="module")
def sql_binder():
    """Bind emitted SQL against empty physical tables, without running it.

    Schemas come from the input model, not DJ's generated projections. DuckDB
    can therefore catch missing columns, ambiguous names and invalid grouping
    independently of DJ's own parser. No query results or data service needed.
    """
    types = {
        "int": "INTEGER",
        "float": "FLOAT",
        "string": "VARCHAR",
        "timestamp": "TIMESTAMP",
    }
    with duckdb.connect(":memory:") as conn:
        conn.execute("ATTACH ':memory:' AS \"default\"")
        conn.execute('CREATE SCHEMA "default".v3')
        for endpoint, spec in BUILD_V3:
            if endpoint != "/nodes/source/":
                continue
            columns = ", ".join(
                f'"{column["name"]}" {types[column["type"]]}'
                for column in spec["columns"]
            )
            conn.execute(f'CREATE TABLE "default".v3.{spec["table"]} ({columns})')
        yield conn


def assert_sql_contract(sql, columns, binder):
    """Check column closure, independent parsing/binding, and output shape."""
    assert not unprojected_references(sql), sql
    query = sqlglot.parse_one(sql, read="spark")
    for guard in query.find_all(exp.Nullif):
        inner = guard.this.unnest()
        assert not (
            isinstance(inner, exp.Nullif)
            and guard.expression == inner.expression == exp.Literal.number(0)
        ), "Repeated NULLIF(_, 0) guard"
    binder.execute("EXPLAIN " + query.sql(dialect="duckdb"))
    bound = binder.execute("DESCRIBE " + query.sql(dialect="duckdb")).fetchall()
    assert [column[0] for column in bound] == [c["name"] for c in columns]
    return query


async def create_metric(client, name, query, **declarations):
    response = await client.post(
        "/nodes/metric/",
        json={"name": name, "query": query, "mode": "published", **declarations},
    )
    assert response.status_code == 201, response.json()


async def build(
    client,
    metrics,
    dimensions,
    filters=(),
    expected_error=None,
    **options,
):
    response = await client.get(
        "/sql/metrics/v3/",
        params={
            "metrics": metrics,
            "dimensions": dimensions,
            "filters": list(filters),
            "dialect": "spark",
            "use_materialized": False,
            **options,
        },
    )
    if expected_error is not None:
        assert response.status_code == 422, response.json()
        assert expected_error in response.json()["message"]
        return None
    assert response.status_code == 200, response.json()
    return response.json()


def projections(query):
    source = query.args["from_"].this.name if query.args.get("from_") else ""
    if source == "__metric_filtered" or (
        len(query.expressions) == 1 and isinstance(query.expressions[0], exp.Star)
    ):
        cte = next(cte for cte in query.ctes if cte.alias == source)
        return projections(cte.this)
    return {p.alias_or_name: p.this for p in query.expressions}


def metric_definition(query, name):
    """Follow simple output aliases to the CTE that computes the metric."""
    return column_definition(query, projections(query)[name])


def column_definition(query, value):
    ctes = {cte.alias: cte.this for cte in query.ctes}
    while isinstance(value, exp.Column) and value.table in ctes:
        value = projections(ctes[value.table])[value.name]
    return value


def grouped_projection_names(query):
    """Resolve both expression and positional GROUP BY items to output names."""
    group = query.args.get("group")
    names = set()
    if group is None:
        return names
    for item in group.expressions:
        if isinstance(item, exp.Literal) and item.is_int:
            names.add(query.expressions[int(item.this) - 1].alias_or_name)
        else:
            for projection in query.expressions:
                expression = (
                    projection.this if isinstance(projection, exp.Alias) else projection
                )
                if expression == item:
                    names.add(projection.alias_or_name)
    return names


def assert_ratio(query, name, numerator, denominator, *, nested=False):
    """Selected parents and their derived value must use the same aggregates.

    The +1 and *2 distinguish nested expression grouping from a flat inlining.
    The parents' aggregation contracts are checked separately by each caller.
    """
    outputs = projections(query)

    def canonical(expression):
        expression = expression.copy()
        for node in reversed(list(expression.walk())):
            if isinstance(node, exp.Paren) and node.parent is not None:
                node.replace(node.this)
            # Qualify bare raw inputs with the sole source, while retaining
            # explicit qualifiers so mismatched relation names still fail.
            elif (
                isinstance(node, exp.Column)
                and not node.table
                and not query.args.get("joins")
                and query.args.get("from_")
            ):
                node.set(
                    "table",
                    exp.to_identifier(query.args["from_"].this.alias_or_name),
                )
        return expression.unnest()

    value = outputs[name].unnest()
    if nested:
        assert isinstance(value, exp.Mul) and value.expression == exp.Literal.number(2)
        value = value.this.unnest()
        assert isinstance(value, exp.Add) and value.expression == exp.Literal.number(1)
        value = value.this.unnest()
    assert isinstance(value, exp.Div)
    divisor = value.expression.unnest()
    # Some stages emit NULLIF explicitly, others rely on Spark division. This
    # contract compares the operands, not engine-specific zero handling.
    if isinstance(divisor, exp.Nullif) and divisor.expression == exp.Literal.number(
        0,
    ):
        divisor = divisor.this.unnest()
        assert not (
            isinstance(divisor, exp.Nullif)
            and divisor.expression == exp.Literal.number(0)
        ), "Repeated NULLIF(_, 0) guard"
    for actual, expected in (
        (value.this, outputs[numerator]),
        (divisor, outputs[denominator]),
    ):
        if canonical(actual) != canonical(expected):
            raise SQLExpressionMismatch(
                f"actual operand: {actual}\nexpected operand: {expected}",
            )


@pytest.mark.parametrize(
    ("numerator", "denominator"),
    [
        ("wrong.x", "source.y"),
        ("source.x", "NULLIF(NULLIF(source.y, 0), 0)"),
    ],
    ids=["wrong-qualifier", "repeated-nullif"],
)
def test_ratio_contract_rejects_incorrect_operands(numerator, denominator):
    query = sqlglot.parse_one(
        f"SELECT source.x AS numerator, source.y AS denominator, "
        f"{numerator} / {denominator} AS ratio FROM source",
    )
    with pytest.raises(AssertionError):
        assert_ratio(query, "ratio", "numerator", "denominator")


@pytest_asyncio.fixture(scope="module")
async def matrix_client(module__client_with_build_v3):
    client = module__client_with_build_v3
    for kind, base in AGGREGATIONS.items():
        for facts, denominator in (
            ("same", "v3.total_quantity"),
            ("cross", "v3.page_view_count"),
        ):
            name = f"v3.matrix_{kind}_{facts}"
            await create_metric(client, name, f"SELECT {base} / {denominator}")
            await create_metric(client, name + "_nested", f"SELECT ({name} + 1) * 2")
    for modifier in ("global", "partition", "first_value", "last_value", "min", "max"):
        declaration = (
            {"fixed_grain": [] if modifier == "global" else [CATEGORY]}
            if modifier in ("global", "partition")
            else {"reaggregate": {"rules": [{"dimension": DAY, "fn": modifier}]}}
        )
        name = f"v3.matrix_{modifier}"
        await create_metric(
            client,
            name,
            "SELECT SUM(line_total) FROM v3.order_details",
            **declaration,
        )
        denominator = "v3.total_quantity"
        if modifier not in ("global", "partition"):
            denominator = name + "_quantity"
            await create_metric(
                client,
                denominator,
                "SELECT SUM(quantity) FROM v3.order_details",
                **declaration,
            )
        await create_metric(
            client,
            name + "_ratio",
            f"SELECT {name} / {denominator}",
        )
        await create_metric(
            client,
            name + "_lag",
            f"SELECT LAG({name}, 1) OVER (ORDER BY {WEEK})",
        )
    await create_metric(
        client,
        "v3.matrix_trailing_ratio",
        "SELECT v3.trailing_7d_revenue / v3.total_revenue",
    )
    await create_metric(
        client,
        "v3.matrix_trailing_nested",
        "SELECT (v3.matrix_trailing_ratio + 1) * 2",
    )
    await create_metric(
        client,
        "v3.matrix_weekly_ratio",
        "SELECT v3.wow_revenue_change / v3.total_revenue",
    )
    return client


@pytest.mark.parametrize("case", [pytest.param(case, id=case.id) for case in CASES])
async def test_core_sql_interactions(matrix_client, sql_binder, case):
    base = AGGREGATIONS[case.aggregation]
    denominator = "v3.total_quantity" if case.facts == "same" else "v3.page_view_count"
    metric = (
        base
        if case.composition == "base"
        else f"v3.matrix_{case.aggregation}_{case.facts}"
        + ("_nested" if case.composition == "nested" else "")
    )
    metrics = list(dict.fromkeys([metric, base, denominator]))
    filters = {
        "none": [],
        "grouped": [f"{CATEGORY} = 'Electronics'"],
        "filter_only": [f"{SUBCATEGORY} = 'Smartphones'"],
        "having": [f"{metric} > 10"],
    }[case.filter]
    if (
        case.aggregation == "raw"
        and case.facts == "cross"
        and case.composition != "base"
    ):
        await build(
            matrix_client,
            metrics,
            [CATEGORY],
            filters,
            expected_error="references non-decomposable",
        )
        return
    result = await build(matrix_client, metrics, [CATEGORY], filters)
    query = assert_sql_contract(result["sql"], result["columns"], sql_binder)
    assert [c["semantic_entity"] for c in result["columns"]] == [CATEGORY, *metrics]

    # Decomposable facts aggregate independently before their final join.
    # MAX_BY instead needs the raw-grain inputs from its fact's CTE.
    cross_joins = [j for j in query.args.get("joins", []) if j.side == "FULL"]
    assert len(cross_joins) == (1 if case.facts == "cross" else 0)
    if cross_joins:
        ctes = {cte.alias: cte.this for cte in query.ctes}
        for relation in (query.args["from_"].this, cross_joins[0].this):
            assert relation.name in ctes
            if case.aggregation != "raw" or relation is cross_joins[0].this:
                assert ctes[relation.name].args.get("group") is not None

    if case.aggregation == "average":
        assert not list(query.find_all(exp.Avg)), (
            "AVG must merge sum/count, not averages"
        )
        assert any(isinstance(n, exp.Count) for n in query.walk())
        assert isinstance(projections(query)[base.split(".")[-1]], (exp.Div, exp.Max))
    elif case.aggregation == "distinct":
        assert any(isinstance(n.this, exp.Distinct) for n in query.find_all(exp.Count))
        assert any(
            "order_id" in str(cte.args.get("group"))
            for cte in (c.this for c in query.ctes)
        )
    elif case.aggregation == "raw":
        assert list(query.find_all(exp.ArgMax)), "MAX_BY must retain its raw inputs"
    elif case.aggregation == "conditional":
        assert list(query.find_all(exp.Case)), "Conditional accumulation must survive"
    else:
        assert any(
            isinstance(n.this, exp.Column) and n.this.name == "line_total"
            for n in query.find_all(exp.Sum)
        )

    if case.composition != "base":
        assert_ratio(
            query,
            metric.split(".")[-1],
            base.split(".")[-1],
            denominator.split(".")[-1],
            nested=case.composition == "nested",
        )
    if case.filter == "having":
        assert query.args.get("having") is not None
        assert query.args["having"].find(exp.GT) is not None
    elif case.filter in ("grouped", "filter_only"):
        column = "category" if case.filter == "grouped" else "subcategory"
        predicates = [w for w in query.find_all(exp.Where) if w.find(exp.EQ)]
        assert any(
            column in {c.name for c in w.find_all(exp.Column)} for w in predicates
        )
        if case.filter == "filter_only":
            assert all(c["name"] != "subcategory" for c in result["columns"])
            assert "subcategory" not in str(query.args.get("group"))


@pytest.mark.parametrize(
    ("modifier", "filter_kind", "keep_day"),
    [
        pytest.param(
            modifier,
            filter_kind,
            keep_day,
            id=f"{modifier}-{filter_kind}-{'keep' if keep_day else 'collapse'}-day",
        )
        for modifier, filter_kind, keep_day in product(
            ("global", "partition", "first_value", "last_value", "min", "max"),
            ("none", "dimension", "having"),
            (False, True),
        )
    ],
)
async def test_grain_derived_filter_interactions(
    matrix_client,
    sql_binder,
    modifier,
    filter_kind,
    keep_day,
):
    """Broadcast/collapse only the declared operand, before forming its ratio."""
    name = f"matrix_{modifier}"
    denominator = (
        "v3.total_quantity"
        if modifier in ("global", "partition")
        else f"v3.{name}_quantity"
    )
    dimensions = [CATEGORY, SUBCATEGORY] + ([DAY] if keep_day else [])
    filters = {
        "none": [],
        "dimension": [f"{SUBCATEGORY} = 'Smartphones'"],
        "having": [f"{denominator} > 0"],
    }[filter_kind]
    result = await build(
        matrix_client,
        [f"v3.{name}", denominator, f"v3.{name}_ratio"],
        dimensions,
        filters,
    )
    query = assert_sql_contract(result["sql"], result["columns"], sql_binder)
    outputs = projections(query)
    assert_ratio(query, name + "_ratio", name, denominator.split(".")[-1])
    value = metric_definition(query, name)
    if modifier in ("global", "partition"):
        assert isinstance(metric_definition(query, "total_quantity"), exp.Sum)
        assert isinstance(value, exp.Window)
        assert isinstance(value.this, exp.Sum) and isinstance(value.this.this, exp.Sum)
        assert [c.name for c in value.args.get("partition_by", [])] == (
            [] if modifier == "global" else ["category"]
        )
    elif keep_day:
        assert isinstance(value, exp.Sum)
        assert isinstance(metric_definition(query, denominator.split(".")[-1]), exp.Sum)
    else:
        collapse_type = {
            "first_value": exp.ArgMin,
            "last_value": exp.ArgMax,
            "min": exp.Min,
            "max": exp.Max,
        }[modifier]
        assert isinstance(value, collapse_type)
        assert isinstance(
            metric_definition(query, denominator.split(".")[-1]),
            collapse_type,
        )
        assert "date_id_order" not in outputs
        assert any(
            "date_id_order" in grouped_projection_names(cte.this) for cte in query.ctes
        )
    assert bool(list(query.find_all(exp.Having))) == (
        filter_kind == "having" and modifier not in ("global", "partition")
    )
    if filter_kind == "having" and modifier in ("global", "partition"):
        assert query.args["where"].find(exp.GT) is not None
        assert "total_quantity" in query.args["where"].sql()


@pytest.mark.parametrize(
    "modifier",
    ["global", "partition", "first_value", "last_value", "min", "max"],
)
async def test_window_over_grain_modified_parent(matrix_client, sql_binder, modifier):
    """Snapshot parents collapse before LAG; fixed-grain parents are refused."""
    if modifier in ("global", "partition"):
        response = await matrix_client.get(
            "/sql/metrics/v3/",
            params={
                "metrics": [f"v3.matrix_{modifier}_lag", f"v3.matrix_{modifier}"],
                "dimensions": [CATEGORY, WEEK],
                "filters": [f"{CATEGORY} = 'Electronics'"],
                "use_materialized": False,
            },
        )
        assert response.status_code == 422, response.json()
        assert "a frame cannot accumulate it" in response.json()["message"]
        return
    result = await build(
        matrix_client,
        [f"v3.matrix_{modifier}_lag", f"v3.matrix_{modifier}"],
        [CATEGORY, WEEK],
        [f"{CATEGORY} = 'Electronics'"],
    )
    query = assert_sql_contract(result["sql"], result["columns"], sql_binder)
    windows = [w for w in query.find_all(exp.Window) if isinstance(w.this, exp.Lag)]
    assert windows
    for window in windows:
        assert [c.name for c in window.args["order"].find_all(exp.Column)] == [
            "week_order",
        ]
        assert [c.name for c in window.args.get("partition_by", [])] == ["category"]
    collapse_type = {
        "first_value": exp.ArgMin,
        "last_value": exp.ArgMax,
        "min": exp.Min,
        "max": exp.Max,
    }[modifier]
    for window in windows:
        assert isinstance(column_definition(query, window.this.this), collapse_type)


async def make_preagg(client, binder, metrics, dimensions, table):
    """Publish availability and mirror the declared physical schema locally."""
    response = await client.post(
        "/preaggs/plan/",
        json={"metrics": metrics, "dimensions": dimensions},
    )
    assert response.status_code == 201, response.json()
    preaggs = response.json()["preaggs"]
    assert len(preaggs) == 1, response.json()
    preagg = preaggs[0]
    assert preagg["columns"]
    assert_sql_contract(preagg["sql"], preagg["columns"], binder)
    binder.execute('CREATE SCHEMA IF NOT EXISTS "default".matrix_preaggs')
    columns = ", ".join(f'"{c["name"]}" {c["type"]}' for c in preagg["columns"])
    binder.execute(f'CREATE TABLE "default".matrix_preaggs.{table} ({columns})')
    response = await client.post(
        f"/preaggs/{preagg['id']}/availability/",
        json={
            "catalog": "default",
            "schema_": "matrix_preaggs",
            "table": table,
            "valid_through_ts": 4102444800000,
        },
    )
    assert response.status_code == 200, response.json()


@pytest.mark.parametrize(
    ("shape", "filter_kind"),
    [
        pytest.param(
            shape,
            filter_kind,
            id=f"{shape}-{filter_kind}",
        )
        for shape, filter_kind in product(
            ("global", "partition", "snapshot", "average", "distinct"),
            ("filter_only", "having"),
        )
    ],
)
@pytest.mark.parametrize("use_materialized", [False, True], ids=["live", "preagg"])
async def test_grain_derived_preagg_interactions(
    client_with_build_v3,
    sql_binder,
    shape,
    filter_kind,
    use_materialized,
):
    """Pre-aggregation must preserve the operands' grain and filter semantics."""
    client = client_with_build_v3
    base = f"v3.matrix_routed_{shape}"
    declaration = {
        "global": {"fixed_grain": []},
        "partition": {"fixed_grain": [CATEGORY]},
        "snapshot": {
            "reaggregate": {"rules": [{"dimension": DAY, "fn": "last_value"}]},
        },
        "average": {},
        "distinct": {},
    }[shape]
    expression = {
        "average": "AVG(unit_price)",
        "distinct": "COUNT(DISTINCT order_id)",
    }.get(shape, "SUM(line_total)")
    await create_metric(
        client,
        base,
        f"SELECT {expression} FROM v3.order_details",
        **declaration,
    )
    denominator = "v3.total_quantity"
    if shape == "snapshot":
        denominator = base + "_quantity"
        await create_metric(
            client,
            denominator,
            "SELECT SUM(quantity) FROM v3.order_details",
            **declaration,
        )
    await create_metric(client, base + "_ratio", f"SELECT {base} / {denominator}")
    metrics = [base, denominator, base + "_ratio"]
    # Keep the date so a snapshot's collapse remains possible. Keep subcategory
    # so a filter-only predicate can still run before the coarser query rollup.
    table = f"routed_{shape}_{filter_kind}_{use_materialized}"
    await make_preagg(client, sql_binder, metrics, [CATEGORY, SUBCATEGORY, DAY], table)
    filters = (
        [f"{SUBCATEGORY} = 'Smartphones'"]
        if filter_kind == "filter_only"
        else [base + "_ratio > 10"]
        if shape in ("average", "distinct", "snapshot")
        else ["v3.total_quantity > 10"]
    )
    result = await build(
        client,
        metrics,
        [CATEGORY],
        filters,
        use_materialized=use_materialized,
    )
    query = assert_sql_contract(result["sql"], result["columns"], sql_binder)
    # Non-FULL components need an exact grain match. This finer distinct
    # pre-agg must safely fall back to the live retained-key plan.
    assert (f"matrix_preaggs.{table}" in result["sql"]) == (
        use_materialized and shape != "distinct"
    )
    assert_ratio(
        query,
        base.split(".")[-1] + "_ratio",
        base.split(".")[-1],
        denominator.split(".")[-1],
    )
    value = metric_definition(query, base.split(".")[-1])
    if shape in ("global", "partition"):
        assert isinstance(value, exp.Window)
        assert [c.name for c in value.args.get("partition_by", [])] == (
            [] if shape == "global" else ["category"]
        )
    elif shape == "snapshot":
        assert isinstance(value, exp.ArgMax)
        assert "date_id_order" in value.sql()
    elif shape == "average":
        assert isinstance(value, exp.Div)
        assert not list(query.find_all(exp.Avg))
    else:
        assert any(isinstance(c.this, exp.Distinct) for c in query.find_all(exp.Count))
    assert bool(list(query.find_all(exp.Having))) == (
        filter_kind == "having" and shape not in ("global", "partition")
    )
    if filter_kind == "having" and shape in ("global", "partition"):
        assert query.args["where"].find(exp.GT) is not None
        assert "total_quantity" in query.args["where"].sql()
    assert [c["semantic_entity"] for c in result["columns"]] == [CATEGORY, *metrics]


async def test_cross_fact_derived_filter_with_partial_preagg(
    client_with_build_v3,
    sql_binder,
):
    """One fact may roll up a pre-agg while another is built from live rows."""
    client = client_with_build_v3
    await create_metric(
        client,
        "v3.matrix_partial_ratio",
        "SELECT v3.total_revenue / v3.page_view_count",
    )
    await make_preagg(
        client,
        sql_binder,
        ["v3.total_revenue"],
        [CATEGORY, SUBCATEGORY],
        "partial_revenue",
    )
    result = await build(
        client,
        ["v3.matrix_partial_ratio", "v3.total_revenue", "v3.page_view_count"],
        [CATEGORY],
        [f"{SUBCATEGORY} = 'Smartphones'", "v3.matrix_partial_ratio > 1"],
        use_materialized=True,
    )
    query = assert_sql_contract(result["sql"], result["columns"], sql_binder)
    tables = {t.name for t in query.find_all(exp.Table)}
    assert "partial_revenue" in tables and "page_views" in tables
    assert "orders" not in tables and "order_items" not in tables
    assert any(j.side == "FULL" for j in query.args["joins"])
    assert query.args.get("having") is not None
    assert_ratio(query, "matrix_partial_ratio", "total_revenue", "page_view_count")


@pytest.mark.parametrize(
    ("modifier", "other_metric"),
    [
        ("partition", "v3.page_view_count"),
        ("last_value", "v3.page_view_count"),
        ("last_value", "v3.total_quantity"),
    ],
)
async def test_grain_modifiers_fail_closed(matrix_client, modifier, other_metric):
    """A join must not fan out a protected grain or misbind its partition."""
    response = await matrix_client.get(
        "/sql/metrics/v3/",
        params={
            "metrics": [f"v3.matrix_{modifier}", other_metric],
            "dimensions": [CATEGORY],
            "filters": [f"{SUBCATEGORY} = 'Smartphones'"],
            "dialect": "spark",
            "use_materialized": False,
        },
    )
    assert response.status_code == 422, response.json()
    expected = (
        "another fact table"
        if modifier == "partition"
        else "multiple base grain groups"
    )
    assert expected in response.json()["message"]


@pytest.mark.parametrize(
    "time_filter",
    [False, True],
    ids=["all-dates", "narrow-date-range"],
)
@pytest.mark.parametrize(
    "derived",
    [False, True],
    ids=["window", "derived-over-window"],
)
async def test_rolling_window_filter_interactions(
    matrix_client,
    sql_binder,
    time_filter,
    derived,
):
    """Filter-only product predicates run before the window; dates restrict output after it."""
    filters = [f"{SUBCATEGORY} = 'Smartphones'"]
    if time_filter:
        filters.append(f"{DAY} BETWEEN 20240108 AND 20240110")
    metrics = ["v3.trailing_7d_revenue", "v3.total_revenue"]
    if derived:
        metrics.insert(0, "v3.matrix_trailing_ratio")
    result = await build(
        matrix_client,
        metrics,
        [CATEGORY, DAY],
        filters,
    )
    query = assert_sql_contract(result["sql"], result["columns"], sql_binder)
    if derived:
        assert_ratio(
            query,
            "matrix_trailing_ratio",
            "trailing_7d_revenue",
            "total_revenue",
        )
    frames = [w for w in query.find_all(exp.Window) if w.args.get("spec")]
    assert frames
    for window in frames:
        assert window.args["spec"].args["start"].this == "6"
        assert [c.name for c in window.args.get("partition_by", [])] == ["category"]
    if time_filter:
        # Narrow result bounds belong above the window. The scan must include
        # six earlier positions, and the dense date spine must remain joined.
        assert query.args.get("where") is not None
        bounds = query.args["where"].find(exp.Between)
        assert bounds is not None
        assert bounds.args["low"] == exp.Literal.number(20240108)
        assert bounds.args["high"] == exp.Literal.number(20240110)
        assert any(cte.alias.startswith("__wl_low_") for cte in query.ctes)
        assert any(
            j.side == "LEFT" and j.this.alias == "__agg"
            for j in query.find_all(exp.Join)
        )


@pytest.mark.parametrize("select_parents", [False, True])
async def test_nested_distinct_uses_planned_parents(
    matrix_client,
    sql_binder,
    select_parents,
):
    """A nested cross-fact formula must consume counts without counting them again."""
    metric = "v3.matrix_distinct_cross_nested"
    metrics = [metric]
    if select_parents:
        # Request the intermediate after its child to exercise dependency order.
        metrics += ["v3.order_count", "v3.page_view_count", "v3.matrix_distinct_cross"]
    result = await build(matrix_client, metrics, [CATEGORY], [metric + " > 10"])
    query = assert_sql_contract(result["sql"], result["columns"], sql_binder)
    expression = projections(query)[metric.split(".")[-1]]
    assert not list(expression.find_all(exp.Count))
    numerator = next(expression.find_all(exp.Max))
    assert isinstance(numerator.this, exp.Column) and numerator.this.name == "order_id"
    divisor = next(expression.find_all(exp.Div)).expression.unnest()
    if isinstance(divisor, exp.Nullif):
        divisor = divisor.this.unnest()
    assert isinstance(divisor, exp.Sum) and isinstance(divisor.this, exp.Column)
    count = column_definition(query, divisor.this)
    assert isinstance(count, exp.Count) and isinstance(count.this, exp.Column)
    assert count.this.name == "view_id"
    assert not list(query.args["having"].find_all(exp.Count))


@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.parametrize("select_parents", [False, True])
async def test_rolling_formula_parent_selection(
    matrix_client,
    sql_binder,
    nested,
    select_parents,
):
    """Unselected and shared derived dependencies resolve at the window stage."""
    metric = "v3.matrix_trailing_nested" if nested else "v3.matrix_trailing_ratio"
    metrics = [metric]
    if select_parents:
        metrics += ["v3.total_revenue", "v3.trailing_7d_revenue"]
    result = await build(matrix_client, metrics, [CATEGORY, DAY])
    query = assert_sql_contract(result["sql"], result["columns"], sql_binder)
    expression = projections(query)[metric.split(".")[-1]]
    assert len(list(expression.find_all(exp.Window))) == 1
    assert all(c.table != "v3" for c in expression.find_all(exp.Column))
    if select_parents:
        assert_ratio(
            query,
            metric.split(".")[-1],
            "trailing_7d_revenue",
            "total_revenue",
            nested=nested,
        )


@pytest.mark.parametrize(
    "filter_metric",
    ["v3.total_revenue", "v3.matrix_trailing_ratio"],
)
async def test_rolling_metric_filter_runs_after_window(
    matrix_client,
    sql_binder,
    filter_metric,
):
    """Filtering a base or window value must preserve the rolling frame's input."""
    result = await build(
        matrix_client,
        ["v3.matrix_trailing_ratio", "v3.total_revenue"],
        [CATEGORY, DAY],
        [filter_metric + " > 10", f"{DAY} BETWEEN 20240108 AND 20240110"],
    )
    query = assert_sql_contract(result["sql"], result["columns"], sql_binder)
    ctes = {cte.alias: cte.this for cte in query.ctes}
    windowed = ctes["__metric_filtered"]
    assert list(windowed.find_all(exp.Window))
    assert windowed.args.get("having") is None
    assert windowed.args.get("where") is None
    assert ctes["__windowed"].args["where"].this.name == "__metric_filter"
    assert query.args["where"].find(exp.Between) is not None
    assert "__metric_filter" not in [c["name"] for c in result["columns"]]


async def test_formula_reads_coarser_window_cte(matrix_client, sql_binder):
    """Weekly windows are computed at week grain before joining the daily formula."""
    result = await build(
        matrix_client,
        ["v3.matrix_weekly_ratio", "v3.wow_revenue_change", "v3.total_revenue"],
        [CATEGORY, DAY, WEEK],
    )
    query = assert_sql_contract(result["sql"], result["columns"], sql_binder)
    assert_ratio(query, "matrix_weekly_ratio", "wow_revenue_change", "total_revenue")
    formula = projections(query)["matrix_weekly_ratio"]
    assert not list(formula.find_all(exp.Window))
    wow = projections(query)["wow_revenue_change"]
    assert isinstance(wow, exp.Column) and wow.table != "base_metrics"
