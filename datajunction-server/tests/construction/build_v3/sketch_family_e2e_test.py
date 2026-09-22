"""
A sketch family, end to end, through the HTTP endpoints.

The rest of the suite tests this machinery in pieces -- `merge_args_test` on
the merge call, `accumulate_type_test` on inferred types, `serialize_target_test`
on the write-time conversion, `reaggregate_test` on the spec. Each one holds a
`ComponentDef` or a `MetricComponent` in its hand and asserts about it. None of
them puts a family behind the server and asks what SQL comes out.

This does. One metric, one grain, followed from raw rows to the number the
engine returns:

    accumulate   build_digest(line_total, 200.0)   from source, per partition
    merge        merge_digest(digest, 200.0)       from the pre-agg table
    combine      digest_quantiles(digest, ...)[0]  Spark / Trino
                 DIGEST_QUANTILE(digest, 0.95)     Druid, merge fused in

OSS registers no family of its own -- they live downstream with the
engine-specific functions -- so the fixture below supplies one, the same way
`decompose_test.registered_family` does for the unit tests. That makes this
file the executable form of the extension contract: a downstream deployment
has to register *both* a decomposition and the functions it names, and if
either half is missing the failure is a 500 out of the measures endpoint, not
something a type checker catches.

`serialize` is the one hook not asserted here. It applies when writing a
measures table for a target rather than when reading, so no query endpoint
renders it; `serialize_target_test` and `cube_druid_sketch_spec_test` cover it.
"""

import re

import pytest
from httpx import AsyncClient

from datajunction_server.models.dialect import Dialect
from datajunction_server.models.materialization import MaterializationTarget
from datajunction_server.models.reaggregate import ReaggregationFunction
from datajunction_server.sql.decompose import (
    FAMILY_DECOMPOSITION_REGISTRY,
    AggDecomposition,
    ComponentDef,
    decomposes_family,
    make_func,
)
from datajunction_server.sql.functions import Function, function_registry
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing import types as ct

from . import assert_sql_equal

METRIC = "v3.p95_line_total"
DIMENSIONS = ["v3.product.category"]
COMPRESSION = 200
PREAGG_TABLE = "analytics.preaggs.v3_digest_by_cat"


# ---------------------------------------------------------------------------
# A stand-in sketch family: four functions and a decomposition that uses them.
# Shaped after a real t-digest -- an accumulate that takes a tuning argument, a
# merge that must repeat it, and a combiner whose form differs on Druid -- so
# the parts of the machinery that only a sketch exercises are actually hit.
# ---------------------------------------------------------------------------


class _DigestFunction(Function):
    """Base class, so the fixture can find these to register and unregister."""


class BuildDigest(_DigestFunction):
    """build_digest(col, compression) -> digest. Phase 1."""

    is_aggregation = True
    dialects = [Dialect.SPARK, Dialect.TRINO]


class MergeDigest(_DigestFunction):
    """merge_digest(digest, compression) -> digest. Phase 2."""

    is_aggregation = True
    dialects = [Dialect.SPARK, Dialect.TRINO]


class DigestQuantiles(_DigestFunction):
    """digest_quantiles(digest, quantiles) -> array<double>. Phase 3, non-Druid."""

    is_aggregation = False
    dialects = [Dialect.SPARK, Dialect.TRINO]


class DigestQuantile(_DigestFunction):
    """DIGEST_QUANTILE(digest, q) -> double. Phase 2+3 fused, Druid only."""

    is_aggregation = True
    dialects = [Dialect.DRUID]


@BuildDigest.register
def infer_type(col: ct.ColumnType, compression: ct.ColumnType) -> ct.BinaryType:
    return ct.BinaryType()


@MergeDigest.register
def infer_type(digest: ct.ColumnType, compression: ct.ColumnType) -> ct.BinaryType:
    return ct.BinaryType()


@DigestQuantiles.register
def infer_type(digest: ct.ColumnType, quantiles: ct.ColumnType) -> ct.ListType:
    return ct.ListType(element_type=ct.DoubleType())


@DigestQuantile.register
def infer_type(digest: ct.ColumnType, quantile: ct.ColumnType) -> ct.DoubleType:
    return ct.DoubleType()


@pytest.fixture
def digest_family():
    """
    Register the family and its functions, then put the registries back.

    Both registries are process-global, so leaving either populated would leak
    into every later test in the session.
    """
    saved: dict[str, type | None] = {}
    for cls in _DigestFunction.__subclasses__():
        snake_cased = re.sub(r"(?<!^)(?=[A-Z])", "_", cls.__name__).upper()
        for key in (cls.__name__.upper(), snake_cased):
            saved[key] = function_registry.get(key)
            function_registry[key] = cls

    @decomposes_family(ReaggregationFunction.TDIGEST)
    class _DigestDecomposition(AggDecomposition):
        @property
        def compression(self) -> int:
            return int(self.params.get("compression", 100))

        @property
        def compression_literal(self) -> str:
            return f"CAST({self.compression}.0 AS DOUBLE)"

        @property
        def components(self) -> list[ComponentDef]:
            return [
                ComponentDef(
                    # Compression rides in the suffix so two differently tuned
                    # sketches over the same column get distinct columns.
                    suffix=f"_digest_c{self.compression}",
                    accumulate=f"build_digest({{}}, {self.compression_literal})",
                    merge="merge_digest",
                    merge_args=(self.compression_literal,),
                    serialize="digest_to_bytes({})",
                    serialize_targets=(MaterializationTarget.DRUID,),
                    serialize_type="binary",
                ),
            ]

        def combine(self, components, func, dialect=Dialect.SPARK):
            column, fraction = components[0].name, func.args[1]
            if dialect == Dialect.DRUID:
                return make_func("DIGEST_QUANTILE", column, fraction)
            return ast.Subscript(
                expr=make_func(
                    "digest_quantiles",
                    column,
                    make_func("array", fraction),
                ),
                index=ast.Number(0),
            )

    try:
        yield _DigestDecomposition
    finally:
        FAMILY_DECOMPOSITION_REGISTRY.pop(ReaggregationFunction.TDIGEST, None)
        for key, previous in saved.items():
            if previous is None:
                function_registry.pop(key, None)
            else:
                function_registry[key] = previous


@pytest.fixture
async def percentile_metric(client_with_build_v3: AsyncClient, digest_family):
    """
    A percentile metric that opts into the family.

    The aggregation is a plain APPROX_PERCENTILE -- nothing about the query
    mentions a sketch. Only `reaggregate.fn` selects the family, which is the
    point of the family registry: the metric keeps its readable definition.
    """
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": METRIC,
            "description": "95th percentile line total",
            "query": (
                "SELECT APPROX_PERCENTILE(line_total, 0.95) FROM v3.order_details"
            ),
            "mode": "published",
            "reaggregate": {
                "fn": "tdigest",
                "params": {"compression": COMPRESSION},
            },
        },
    )
    assert response.status_code in (200, 201), response.json()
    return client_with_build_v3


async def _measures(client: AsyncClient, dialect: str = "spark") -> dict:
    response = await client.get(
        "/sql/measures/v3/",
        params={"metrics": [METRIC], "dimensions": DIMENSIONS, "dialect": dialect},
    )
    assert response.status_code == 200, response.json()
    return response.json()


async def _publish_preagg(client: AsyncClient) -> None:
    response = await client.post(
        "/preaggs/plan",
        json={
            "metrics": [METRIC],
            "dimensions": DIMENSIONS,
            "strategy": "full",
            "schedule": "0 0 * * *",
        },
    )
    assert response.status_code == 201, response.text

    catalog, schema, table = PREAGG_TABLE.split(".")
    response = await client.post(
        f"/preaggs/{response.json()['preaggs'][0]['id']}/availability/",
        json={
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "valid_through_ts": 1704067200,
        },
    )
    assert response.status_code == 200, response.text


@pytest.mark.asyncio
async def test_accumulate_renders_the_tuning_param(percentile_metric):
    """
    Phase 1: `params` reaches the generated SQL as a real argument.

    `compression` is declared once on the metric's reaggregate spec and has to
    survive into the accumulate call. If it were dropped the query would still
    run -- `build_digest` has a one-argument form -- and quietly build sketches
    at the wrong accuracy, so this is worth pinning on the SQL rather than on
    the spec.
    """
    (group,) = (await _measures(percentile_metric))["grain_groups"]

    assert_sql_equal(
        group["sql"],
        f"""
        WITH v3_order_details AS (
          SELECT oi.product_id,
                 oi.quantity * oi.unit_price AS line_total
          FROM default.v3.orders o
          JOIN default.v3.order_items oi ON o.order_id = oi.order_id
        ),
        v3_product AS (
          SELECT product_id, category FROM default.v3.products
        )
        SELECT t2.category,
               build_digest(t1.line_total, CAST({COMPRESSION}.0 AS DOUBLE))
                 line_total_digest_c{COMPRESSION}_e1f61696
        FROM v3_order_details t1
        LEFT OUTER JOIN v3_product t2 ON t1.product_id = t2.product_id
        GROUP BY t2.category
        """,
    )


@pytest.mark.asyncio
async def test_component_identity_carries_the_compression(percentile_metric):
    """
    The column name encodes the tuning, and the recorded type is the sketch's.

    Both matter downstream: the name keeps a sketch built at one accuracy from
    satisfying a query that asked for another, and the type is what the
    materialized table is created from.
    """
    (group,) = (await _measures(percentile_metric))["grain_groups"]
    (component,) = group["components"]
    (column,) = [c for c in group["columns"] if c["semantic_type"] != "dimension"]

    assert component["name"] == f"line_total_digest_c{COMPRESSION}_e1f61696"
    assert component["merge"] == "merge_digest"
    assert component["aggregability"] == "full"
    assert column["type"] == "binary"


@pytest.mark.asyncio
async def test_merge_from_preagg_repeats_the_tuning_param(percentile_metric):
    """
    Phase 2: reading stored sketches back, with `merge_args` applied.

    The accumulate is gone and the source CTEs with it -- a single scan of the
    pre-agg table. The compression has to appear a second time here: `merge` is
    a bare function name (the Druid aggregator lookup and the semi-additive
    rewrite both match on it), so the argument cannot be folded into it and
    travels as `merge_args` instead.
    """
    await _publish_preagg(percentile_metric)
    (group,) = (await _measures(percentile_metric))["grain_groups"]

    assert_sql_equal(
        group["sql"],
        f"""
        SELECT category,
               merge_digest(line_total_digest_c{COMPRESSION}_e1f61696,
                            CAST({COMPRESSION}.0 AS DOUBLE))
                 line_total_digest_c{COMPRESSION}_e1f61696
        FROM {PREAGG_TABLE}
        GROUP BY category
        """,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dialect, expected",
    [
        (
            "spark",
            f"digest_quantiles(line_total_digest_c{COMPRESSION}_e1f61696, "
            "array(0.95))[0]",
        ),
        (
            "druid",
            f"DIGEST_QUANTILE(line_total_digest_c{COMPRESSION}_e1f61696, 0.95)",
        ),
    ],
)
async def test_combiner_takes_the_shape_the_engine_needs(
    percentile_metric,
    dialect,
    expected,
):
    """
    Phase 3: the combiner, and the one place the dialect legitimately matters.

    Druid has no scalar reader for the sketch, so it fuses merge and combine
    into one aggregating call; Spark and Trino read the merged column with a
    separate extractor. That is a difference in shape, which no transpiler can
    derive -- unlike function spelling or array index base, which the
    transpilation layer handles and a decomposition must therefore leave alone.

    The requested quantile is authored in the metric query, not the reaggregate
    spec, so it has to survive decomposition and reappear here. That is what
    lets p50/p95/p99 share one sketch column.
    """
    payload = await _measures(percentile_metric, dialect=dialect)
    (formula,) = payload["metric_formulas"]

    assert formula["combiner"] == expected


@pytest.mark.asyncio
async def test_metrics_layer_recomputes_from_source(percentile_metric):
    """
    Final metrics layer: recomputes from source, pre-agg or not.

    Pinning current behaviour, not endorsing it. The pre-agg published here is
    the same one `test_merge_from_preagg_repeats_the_tuning_param` reads, and
    the measures layer picks it up. The metrics layer never consults the
    matcher: `build_metrics_sql` takes the cube branch only when
    `use_materialized and dialect == Dialect.DRUID`, and otherwise builds grain
    groups from source.

    So no sketch appears below at all -- a bare APPROX_PERCENTILE off the fact
    -- and the combiner asserted above never reaches rendered SQL on this path.
    Defensible if scanning source on Trino is cheap enough, but it does mean
    the sketch columns are written for Druid's benefit alone.
    """
    await _publish_preagg(percentile_metric)

    response = await percentile_metric.get(
        "/sql/",
        params={"metrics": [METRIC], "dimensions": DIMENSIONS},
    )
    assert response.status_code == 200, response.json()

    assert_sql_equal(
        response.json()["sql"],
        """
        WITH v3_DOT_order_details AS (
          SELECT o.order_id,
                 oi.line_number,
                 o.customer_id,
                 o.order_date,
                 o.from_location_id,
                 o.to_location_id,
                 o.status,
                 oi.product_id,
                 oi.quantity,
                 oi.unit_price,
                 oi.quantity * oi.unit_price AS line_total
          FROM v3.orders AS o
          JOIN v3.order_items AS oi ON o.order_id = oi.order_id
        ),
        v3_DOT_product AS (
          SELECT v3_DOT_src_products.product_id,
                 v3_DOT_src_products.name,
                 v3_DOT_src_products.category,
                 v3_DOT_src_products.subcategory,
                 v3_DOT_src_products.price
          FROM v3.products AS v3_DOT_src_products
        ),
        v3_DOT_order_details_metrics AS (
          SELECT v3_DOT_product.category v3_DOT_product_DOT_category,
                 APPROX_PERCENTILE(v3_DOT_order_details.line_total, 0.95)
                   v3_DOT_p95_line_total
          FROM v3_DOT_order_details
          LEFT JOIN v3_DOT_product
            ON v3_DOT_order_details.product_id = v3_DOT_product.product_id
          GROUP BY v3_DOT_product.category
        )
        SELECT v3_DOT_order_details_metrics.v3_DOT_product_DOT_category,
               v3_DOT_order_details_metrics.v3_DOT_p95_line_total
        FROM v3_DOT_order_details_metrics
        """,
    )
