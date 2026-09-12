"""Tests for the `fixed_grain` metric declaration."""

import time
from types import SimpleNamespace

import pytest

from datajunction_server.construction.build_v3.cube_matcher import (
    find_matching_cube,
)
from datajunction_server.construction.build_v3.metrics import (
    validate_fixed_grain_query_shape,
)
from datajunction_server.database.node import Node
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.decompose import Aggregability
from datajunction_server.models.deployment import ChangeTier, MetricSpec
from datajunction_server.sql.decompose import MetricComponentExtractor
from datajunction_server.sql.parsing.backends.antlr4 import parse


def test_fixed_grain_change_tier_is_major():
    """Where an aggregate is computed changes the number it returns."""
    assert MetricSpec.field_change_tier("fixed_grain") == ChangeTier.MAJOR


def test_global_grain_is_not_the_same_declaration_as_query_grain():
    """`[]` (global grain) and an absent `fixed_grain` (query grain) differ."""
    query = "SELECT SUM(line_total) FROM v3.order_details"
    global_grain = MetricSpec(name="m", namespace="v3", query=query, fixed_grain=[])
    query_grain = MetricSpec(name="m", namespace="v3", query=query)

    assert global_grain.fixed_grain == []
    assert query_grain.fixed_grain is None
    assert global_grain != query_grain
    assert "fixed_grain" in query_grain.diff(global_grain)


def test_rendered_fixed_grain_resolves_prefixes():
    """A parameterized dimension path resolves against the spec's namespace."""
    spec = MetricSpec(
        name="m",
        namespace="v3",
        query="SELECT SUM(line_total) FROM v3.order_details",
        fixed_grain=["${prefix}genre_dim.genre", "country"],
    )
    assert spec.rendered_fixed_grain == ["v3.genre_dim.genre", "country"]


def test_rendered_fixed_grain_preserves_the_empty_list():
    """`[]` must not collapse to `None` -- they are different declarations."""
    spec = MetricSpec(
        name="m",
        namespace="v3",
        query="SELECT SUM(line_total) FROM v3.order_details",
        fixed_grain=[],
    )
    assert spec.rendered_fixed_grain == []


@pytest.mark.asyncio
async def test_fixed_grain_round_trips_through_the_api(client_with_build_v3):
    """Declared on create, stored, and read back."""
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.total_line_total_global",
            "description": "Total price at the global grain",
            "query": "SELECT SUM(line_total) FROM v3.order_details",
            "fixed_grain": [],
            "mode": "published",
        },
    )
    assert response.status_code == 201, response.json()

    response = await client_with_build_v3.get("/nodes/v3.total_line_total_global/")
    assert response.status_code == 200, response.json()
    assert response.json()["fixed_grain"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("suffix", "grain", "expected"),
    [
        ("unresolvable", ["v3.product.no_such_column"], 422),
        ("resolvable", ["v3.product.category"], 201),
        ("global", [], 201),
    ],
    ids=["unresolvable", "resolvable", "global-resolves-nothing"],
)
async def test_grain_dimensions_are_validated_at_write_time(
    client_with_build_v3,
    suffix,
    grain,
    expected,
):
    """A grain naming a column that does not exist is rejected on create.

    `reaggregate` is checked on this path too. Without it the node is stored
    `valid`, `POST /nodes/{name}/validate/` also reports it clean, and the
    first sign of trouble is a permanent failure at query time.
    """
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": f"v3.write_validated_{suffix}",
            "description": "grain validated at write time",
            "query": "SELECT SUM(line_total) FROM v3.order_details",
            "fixed_grain": grain,
            "mode": "published",
        },
    )
    assert response.status_code == expected, response.json()
    if expected == 422:
        assert "declares a fixed grain referencing dimension columns" in str(
            response.json(),
        )


@pytest.mark.asyncio
async def test_fixed_grain_shape_is_validated_at_write_time(client_with_build_v3):
    """A non-mergeable aggregate with a fixed_grain is rejected on create.

    Previously this only failed once queried (`GET /sql/metrics/v3/`); the
    metric would sit `valid` in the meantime with a declaration it could
    never honor.
    """
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.distinct_orders_with_grain",
            "description": "Non-mergeable aggregate with a fixed grain",
            "query": "SELECT COUNT(DISTINCT order_id) FROM v3.order_details",
            "fixed_grain": [],
            "mode": "published",
        },
    )
    assert response.status_code == 422, response.json()
    assert "Unsupported fixed_grain metric shape" in str(response.json())


@pytest.mark.asyncio
async def test_fixed_grain_shape_validate_endpoint_reports_it_too(
    client_with_build_v3,
    session,
):
    """`POST /nodes/{name}/validate/` also runs the shape check.

    Exercises `validate_node_data_v2`'s branch directly, the same way
    `test_validate_endpoint_reports_an_unresolvable_grain` does for the
    dimension check.
    """
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.grain_shape_goes_bad",
            "description": "Starts valid, mutated to a bad shape",
            "query": "SELECT SUM(line_total) FROM v3.order_details",
            "fixed_grain": [],
            "mode": "published",
        },
    )
    assert response.status_code == 201, response.json()

    node = await Node.get_by_name(session, "v3.grain_shape_goes_bad")
    node.current.query = "SELECT COUNT(DISTINCT order_id) FROM v3.order_details"
    await session.commit()

    response = await client_with_build_v3.post(
        "/nodes/v3.grain_shape_goes_bad/validate/",
    )
    assert response.status_code == 200, response.json()
    body = response.json()
    assert body["status"] == "invalid", body
    assert any(
        "Unsupported fixed_grain metric shape" in error["message"]
        for error in body["errors"]
    ), body["errors"]


@pytest.mark.asyncio
async def test_validate_endpoint_reports_an_unresolvable_grain(
    client_with_build_v3,
    session,
):
    """`POST /nodes/{name}/validate/` must not call a broken node clean.

    Creation now rejects an unresolvable grain, so the way to reach this state
    is a dimension that disappeared after the fact — which is the situation the
    endpoint exists to surface.
    """
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.grain_goes_stale",
            "description": "Total within category",
            "query": "SELECT SUM(line_total) FROM v3.order_details",
            "fixed_grain": ["v3.product.category"],
            "mode": "published",
        },
    )
    assert response.status_code == 201, response.json()

    # The grain's dimension stops resolving.
    node = await Node.get_by_name(session, "v3.grain_goes_stale")
    node.current.fixed_grain = ["v3.product.no_longer_here"]
    await session.commit()

    response = await client_with_build_v3.post(
        "/nodes/v3.grain_goes_stale/validate/",
    )
    assert response.status_code == 200, response.json()
    body = response.json()
    assert body["status"] == "invalid", body
    assert any(
        "declares a fixed grain referencing dimension columns" in error["message"]
        for error in body["errors"]
    ), body["errors"]


@pytest.mark.asyncio
async def test_fixed_grain_rejected_on_non_metric_nodes(client_with_build_v3):
    """A non-metric node declaring `fixed_grain` is rejected at write time."""
    response = await client_with_build_v3.post(
        "/nodes/transform/",
        json={
            "name": "v3.transform_grain",
            "description": "Not a metric",
            "query": "SELECT order_id FROM v3.order_details",
            "fixed_grain": [],
            "mode": "published",
        },
    )
    assert response.status_code == 422, response.json()
    assert "only for metrics" in str(response.json())


@pytest.mark.asyncio
async def test_fixed_grain_survives_an_unrelated_patch(client_with_build_v3):
    """A PATCH that never mentions the grain carries it forward."""
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.total_line_total_kept",
            "description": "Before",
            "query": "SELECT SUM(line_total) FROM v3.order_details",
            "fixed_grain": [],
            "mode": "published",
        },
    )
    assert response.status_code == 201, response.json()

    response = await client_with_build_v3.patch(
        "/nodes/v3.total_line_total_kept/",
        json={"description": "After"},
    )
    assert response.status_code == 200, response.json()
    assert response.json()["fixed_grain"] == []


@pytest.mark.asyncio
async def test_fixed_grain_cleared_by_explicit_null(client_with_build_v3):
    """An explicit null returns the metric to the query grain."""
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.total_line_total_cleared",
            "description": "Global",
            "query": "SELECT SUM(line_total) FROM v3.order_details",
            "fixed_grain": [],
            "mode": "published",
        },
    )
    assert response.status_code == 201, response.json()

    response = await client_with_build_v3.patch(
        "/nodes/v3.total_line_total_cleared/",
        json={"fixed_grain": None},
    )
    assert response.status_code == 200, response.json()
    assert response.json()["fixed_grain"] is None


@pytest.mark.asyncio
async def test_fixed_grain_exposed_in_graphql(client_with_build_v3):
    """Clients can read the grain, so a BI tool can explain the number."""
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.total_line_total_gql",
            "description": "Global",
            "query": "SELECT SUM(line_total) FROM v3.order_details",
            "fixed_grain": [],
            "mode": "published",
        },
    )
    assert response.status_code == 201, response.json()

    response = await client_with_build_v3.post(
        "/graphql",
        json={
            "query": """
                { findNodes(names: ["v3.total_line_total_gql"]) {
                    current { fixedGrain }
                } }
            """,
        },
    )
    assert response.status_code == 200, response.json()
    data = response.json()["data"]["findNodes"]
    assert data[0]["current"]["fixedGrain"] == []


@pytest.mark.asyncio
async def test_global_grain_broadcasts_as_an_unpartitioned_window(
    client_with_build_v3,
):
    """A metric fixed at the global grain is fanned out with `OVER ()`."""
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.total_line_total_bcast",
            "description": "Total at the global grain",
            "query": "SELECT SUM(line_total) FROM v3.order_details",
            "fixed_grain": [],
            "mode": "published",
        },
    )
    assert response.status_code == 201, response.json()

    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.total_line_total_bcast"],
            "dimensions": ["v3.order_details.order_id"],
        },
    )
    assert response.status_code == 200, response.json()
    sql = response.json()["sql"]
    assert "SUM(SUM(order_details_0.line_total_sum_e1f61696)) OVER ()" in sql
    assert "AS total_line_total_bcast" in sql


@pytest.mark.asyncio
async def test_partitioned_grain_broadcasts_within_the_partition(
    client_with_build_v3,
):
    """A grain of `[status]` totals within status, repeated across finer rows."""
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.status_line_total",
            "description": "Total within status",
            "query": "SELECT SUM(line_total) FROM v3.order_details",
            "fixed_grain": ["v3.order_details.status"],
            "mode": "published",
        },
    )
    assert response.status_code == 201, response.json()

    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.status_line_total"],
            "dimensions": [
                "v3.order_details.status",
                "v3.order_details.order_id",
            ],
        },
    )
    assert response.status_code == 200, response.json()
    sql = response.json()["sql"]
    assert "PARTITION BY order_details_0.status" in sql
    assert "AS status_line_total" in sql


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("expression", "broadcast_is_numerator"),
    [
        ("v3.ratio_revenue / v3.ratio_total", False),
        ("v3.ratio_total / v3.ratio_revenue", True),
    ],
    ids=["share-of-total", "index-against-total"],
)
async def test_ratio_broadcasts_only_the_fixed_grain_operand(
    client_with_build_v3,
    expression,
    broadcast_is_numerator,
):
    """The canonical shape from issue #2245: a ratio against a global total.

    Both operands decompose to the *same* measure, so this pins down that the
    window lands on the fixed-grain side specifically rather than on whichever
    merge call the combiner happens to reach first.
    """
    for name, extra in (
        ("v3.ratio_revenue", {}),
        ("v3.ratio_total", {"fixed_grain": []}),
    ):
        response = await client_with_build_v3.post(
            "/nodes/metric/",
            json={
                "name": name,
                "description": name,
                "query": "SELECT SUM(line_total) FROM v3.order_details",
                "mode": "published",
                **extra,
            },
        )
        assert response.status_code == 201, response.json()

    derived = (
        f"v3.ratio_{'share' if expression.startswith('v3.ratio_revenue') else 'index'}"
    )
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": derived,
            "description": "ratio against the global total",
            "query": f"SELECT {expression}",
            "mode": "published",
        },
    )
    assert response.status_code == 201, response.json()

    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={
            "metrics": [derived],
            "dimensions": ["v3.order_details.order_id"],
        },
    )
    assert response.status_code == 200, response.json()
    sql = response.json()["sql"]

    # Exactly one window in the whole query: both operands decompose to the same
    # measure, so a second `OVER` would mean the plain one was broadcast too.
    assert sql.count("OVER (") == 1, sql

    # The window sits on the fixed-grain parent, computed once in the CTE and
    # referenced by name — not inlined into the ratio.
    windowed = next(line for line in sql.splitlines() if "OVER (" in line)
    assert "AS ratio_total" in windowed, windowed
    plain = next(line for line in sql.splitlines() if "AS ratio_revenue" in line)
    assert "OVER" not in plain, plain

    # The ratio divides the two parent columns in the declared order.
    projection = next(
        line
        for line in sql.splitlines()
        if line.strip().endswith(derived.split(".")[1])
    )
    numerator, denominator = (
        ("ratio_total", "ratio_revenue")
        if broadcast_is_numerator
        else ("ratio_revenue", "ratio_total")
    )
    assert projection.index(numerator) < projection.index(denominator), projection


@pytest.mark.asyncio
async def test_ratio_broadcasts_both_metrics_fixed_to_the_same_grain(
    client_with_build_v3,
):
    """Each fixed-grain parent keeps its own broadcast in a derived ratio.

    The parents deliberately share one underlying component, exercising the
    component-deduplication path as well as the common declared grain.
    """
    fixed_grain = ["v3.order_details.status"]
    parents = ("v3.fixed_status_numerator", "v3.fixed_status_denominator")
    for name in parents:
        response = await client_with_build_v3.post(
            "/nodes/metric/",
            json={
                "name": name,
                "description": name,
                "query": "SELECT SUM(line_total) FROM v3.order_details",
                "fixed_grain": fixed_grain,
                "mode": "published",
            },
        )
        assert response.status_code == 201, response.json()

    derived = "v3.fixed_status_ratio"
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": derived,
            "description": "Ratio of metrics fixed to the same grain",
            "query": f"SELECT {parents[0]} / {parents[1]}",
            "mode": "published",
        },
    )
    assert response.status_code == 201, response.json()

    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={
            "metrics": [derived],
            "dimensions": [
                "v3.order_details.status",
                "v3.order_details.order_id",
            ],
        },
    )
    assert response.status_code == 200, response.json()
    sql = response.json()["sql"]

    assert sql.count("PARTITION BY order_details_0.status") == 2, sql
    for parent in parents:
        alias = parent.rsplit(".", 1)[-1]
        windowed = next(line for line in sql.splitlines() if f"AS {alias}" in line)
        assert "PARTITION BY order_details_0.status" in windowed


@pytest.mark.asyncio
async def test_partitioned_grain_allowed_beside_a_window_metric(client_with_build_v3):
    """A window metric adds a grain group for the SAME fact table.

    The cross-fact refusal must count base grain groups only, or a period-over-
    period metric alongside a partitioned grain is refused for being on another
    fact table when it is not.
    """
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.grain_beside_window",
            "description": "Total within category",
            "query": "SELECT SUM(line_total) FROM v3.order_details",
            "fixed_grain": ["v3.product.category"],
            "mode": "published",
        },
    )
    assert response.status_code == 201, response.json()

    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={
            # wow_revenue_change is a window metric on the same fact table.
            "metrics": ["v3.grain_beside_window", "v3.wow_revenue_change"],
            "dimensions": ["v3.product.category", "v3.date.date_id[order]"],
        },
    )
    assert response.status_code == 200, response.json()
    sql = response.json()["sql"]
    assert "PARTITION BY order_details_0.category" in sql
    assert "LEFT OUTER JOIN order_details_week" in sql


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "params",
    [
        {},
        {"filters": ["v3.product.category = 'Electronics'"]},
    ],
    ids=["not-mentioned", "even-pinned-by-an-equality-filter"],
)
async def test_grain_absent_from_the_query_is_refused(client_with_build_v3, params):
    """A fixed_grain metric can broadcast only when its grain is contained
    in the query grain. If `category` is absent, the grains are
    incompatible: producing one scalar per `order_id` would require widening
    the output grain or applying a separate collapse operation. This
    broadcast-only path does neither, even when a filter happens to pin the
    dimension to one value.
    """
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.grain_needs_category",
            "description": "Total within category",
            "query": "SELECT SUM(line_total) FROM v3.order_details",
            "fixed_grain": ["v3.product.category"],
            "mode": "published",
        },
    )
    assert response.status_code in (201, 409), response.json()

    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.grain_needs_category"],
            "dimensions": ["v3.order_details.order_id"],
            **params,
        },
    )
    assert response.status_code == 422, response.json()
    assert "not a requested output dimension" in response.json()["message"]


@pytest.mark.asyncio
async def test_broadcast_replaces_the_merge_inside_the_combiner(
    client_with_build_v3,
):
    """The window goes around the aggregate, not around the whole combiner.

    `ROUND(SUM(x), 2)` must broadcast to `ROUND(SUM(SUM(x)) OVER (), 2)`; wrapping
    the combiner instead rounds before totalling and returns a different number.
    """
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.rounded_total_global",
            "description": "Rounded total at the global grain",
            "query": "SELECT ROUND(SUM(line_total), 2) FROM v3.order_details",
            "fixed_grain": [],
            "mode": "published",
        },
    )
    assert response.status_code == 201, response.json()

    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.rounded_total_global"],
            "dimensions": ["v3.order_details.order_id"],
        },
    )
    assert response.status_code == 200, response.json()
    sql = response.json()["sql"]
    # ROUND wraps the broadcast window, not the other way around: rounding
    # before totalling would return a different number.
    assert "ROUND(SUM(SUM(" in sql.replace(" ", "")
    assert "SUM(ROUND(" not in sql.replace(" ", "")


class TestFixedGrainGuards:
    """The seams where a broadcast cannot be expressed, and so must refuse."""

    @staticmethod
    def _decomposed(fixed_grain, aggregability="full", merge="SUM", ncomp=1):
        """A minimal stand-in for DecomposedMetricInfo."""
        rule = SimpleNamespace(fixed_grain=fixed_grain)
        comps = [
            SimpleNamespace(name=f"c{i}", merge=merge, rule=rule) for i in range(ncomp)
        ]
        return SimpleNamespace(
            metric_node=SimpleNamespace(name="v3.m"),
            components=comps,
            aggregability=Aggregability(aggregability),
        )

    def test_absent_grain_is_left_alone(self):
        """No declaration, nothing to validate."""
        assert validate_fixed_grain_query_shape(self._decomposed(None)) is None

    def test_partitioned_grain_refused_across_grain_groups(self):
        """A partition naming one group's column is ungrouped in the joined SELECT."""
        with pytest.raises(DJInvalidInputException) as exc:
            validate_fixed_grain_query_shape(
                self._decomposed(["v3.d.x"]),
                grain_group_count=2,
            )
        assert "another fact table" in str(exc.value)

    def test_global_grain_allowed_across_grain_groups(self):
        """`OVER ()` names no column, so it survives the join unambiguously."""
        assert (
            validate_fixed_grain_query_shape(
                self._decomposed([]),
                grain_group_count=2,
            )
            is None
        )


async def _cube_over_a_grain(client, suffix, grain, cube_dimensions):
    """A metric with `grain`, a cube with `cube_dimensions`, and availability."""
    response = await client.post(
        "/nodes/metric/",
        json={
            "name": f"v3.cov_metric_{suffix}",
            "description": f"cov {suffix}",
            "query": "SELECT SUM(line_total) FROM v3.order_details",
            "fixed_grain": grain,
            "mode": "published",
        },
    )
    assert response.status_code == 201, response.json()
    response = await client.post(
        "/nodes/cube/",
        json={
            "name": f"v3.cov_cube_{suffix}",
            "metrics": [f"v3.cov_metric_{suffix}"],
            "dimensions": cube_dimensions,
            "mode": "published",
            "description": f"cov cube {suffix}",
        },
    )
    assert response.status_code == 201, response.json()
    response = await client.post(
        f"/data/v3.cov_cube_{suffix}/availability/",
        json={
            "catalog": "default",
            "schema_": "analytics",
            "table": f"cov_cube_{suffix}",
            "valid_through_ts": int(time.time() * 1000),
        },
    )
    assert response.status_code == 200, response.json()


@pytest.mark.asyncio
async def test_cube_missing_the_grain_dimension_is_not_used(
    client_with_build_v3,
    session,
):
    """A cube that dropped the partition dimension cannot express the window.

    The broadcast partitions over the cube's own columns, so serving from a cube
    without that column would emit SQL naming a column the table does not have.
    Skipped rather than refused at the cube-matching step: the fact table is a
    plausible alternative. The fact-table fallback then refuses on its own
    terms, since `subcategory` isn't requested or filtered there either --
    the same rule as an unpinned `v3.product.category` elsewhere in this file.
    """
    await _cube_over_a_grain(
        client_with_build_v3,
        "missing",
        ["v3.product.subcategory"],
        ["v3.product.category"],
    )

    matched = await find_matching_cube(
        session,
        metrics=["v3.cov_metric_missing"],
        dimensions=["v3.product.category"],
    )
    assert matched is None

    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.cov_metric_missing"],
            "dimensions": ["v3.product.category"],
        },
    )
    assert response.status_code == 422, response.json()
    assert "not a requested output dimension" in response.json()["message"]


@pytest.mark.asyncio
async def test_cube_that_keeps_the_grain_dimension_is_still_used(
    client_with_build_v3,
    session,
):
    """Coverage, not avoidance: a cube carrying the partition is fine to use."""
    await _cube_over_a_grain(
        client_with_build_v3,
        "kept",
        ["v3.product.category"],
        ["v3.product.category"],
    )

    matched = await find_matching_cube(
        session,
        metrics=["v3.cov_metric_kept"],
        dimensions=["v3.product.category"],
    )
    assert matched is not None
    assert matched.name == "v3.cov_cube_kept"


@pytest.mark.asyncio
async def test_cube_serves_a_fixed_grain_metric_with_its_broadcast(
    client_with_build_v3,
    session,
):
    """A matched cube keeps the broadcast, so it is used rather than skipped.

    The window is applied by `generate_metrics_sql`, which the cube path also
    goes through, so reading the measure from a cube changes nothing about it.
    Druid computes the window itself (verified on v37), so the resulting query
    is servable.
    """
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.cube_skip_global",
            "description": "Global total",
            "query": "SELECT SUM(line_total) FROM v3.order_details",
            "fixed_grain": [],
            "mode": "published",
        },
    )
    assert response.status_code == 201, response.json()

    response = await client_with_build_v3.post(
        "/nodes/cube/",
        json={
            "name": "v3.cube_with_fixed_grain",
            "metrics": ["v3.cube_skip_global"],
            "dimensions": ["v3.product.category"],
            "mode": "published",
            "description": "Cube over a fixed-grain metric",
        },
    )
    assert response.status_code == 201, response.json()

    response = await client_with_build_v3.post(
        "/data/v3.cube_with_fixed_grain/availability/",
        json={
            "catalog": "default",
            "schema_": "analytics",
            "table": "cube_with_fixed_grain",
            "valid_through_ts": int(time.time() * 1000),
        },
    )
    assert response.status_code == 200, response.json()

    matched = await find_matching_cube(
        session,
        metrics=["v3.cube_skip_global"],
        dimensions=["v3.product.category"],
    )
    assert matched is not None

    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.cube_skip_global"],
            "dimensions": ["v3.product.category"],
        },
    )
    assert response.status_code == 200, response.json()
    sql = response.json()["sql"]
    # Served straight from the cube's own columns -- no fact-table CTEs.
    assert "FROM cube_with_fixed_grain" in sql
    assert "FROM v3_order_details" not in sql
    assert "SUM(SUM(cube_with_fixed_grain_0.line_total_sum_e1f61696)) OVER ()" in sql


@pytest.mark.asyncio
async def test_window_metric_over_a_fixed_grain_parent_is_refused(
    client_with_build_v3,
):
    """A broadcast value is repeated across rows, so a frame re-adds it N times."""
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.window_parent_global",
            "description": "Global total",
            "query": "SELECT SUM(line_total) FROM v3.order_details",
            "fixed_grain": [],
            "mode": "published",
        },
    )
    assert response.status_code == 201, response.json()

    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.trailing_over_global",
            "description": "Rolling sum of a broadcast value",
            "query": """
                SELECT
                    SUM(v3.window_parent_global) OVER (
                        ORDER BY v3.date.date_id[order]
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                    )
            """,
            "required_dimensions": ["v3.date.date_id[order]"],
            "mode": "published",
        },
    )
    assert response.status_code == 201, response.json()

    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.trailing_over_global"],
            "dimensions": ["v3.date.date_id[order]"],
        },
    )
    assert response.status_code == 422, response.json()
    message = response.json()["message"]
    assert "a frame cannot accumulate it" in message
    # The advice must not describe something the guard itself refuses.
    assert "Referencing it beside a window is fine" in message


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("window_fn", "expected"),
    [("SUM", 422), ("MAX", 200), ("AVG", 200)],
    ids=["accumulating-refused", "max-allowed", "avg-allowed"],
)
async def test_only_accumulating_frames_over_a_broadcast_are_refused(
    client_with_build_v3,
    window_fn,
    expected,
):
    """`MAX`/`AVG` over a row-constant column return it unchanged.

    Only a frame that sums or counts the broadcast re-adds it per row, so those
    are the only ones worth refusing.
    """
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.acc_global",
            "description": "Global total",
            "query": "SELECT SUM(line_total) FROM v3.order_details",
            "fixed_grain": [],
            "mode": "published",
        },
    )
    assert response.status_code in (201, 409), response.json()

    name = f"v3.acc_{window_fn.lower()}"
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": name,
            "description": name,
            "query": (
                f"SELECT {window_fn}(v3.acc_global) OVER ("
                "ORDER BY v3.date.date_id[order] "
                "ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)"
            ),
            "required_dimensions": ["v3.date.date_id[order]"],
            "mode": "published",
        },
    )
    assert response.status_code == 201, response.json()

    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={"metrics": [name], "dimensions": ["v3.date.date_id[order]"]},
    )
    assert response.status_code == expected, response.json()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("window_fn", "partition_col", "expected"),
    [
        ("AVG", "v3.product.category", 200),
        ("AVG", "v3.customer.name", 422),
        ("MAX", "v3.customer.name", 200),
        ("FIRST_VALUE", "v3.customer.name", 200),
    ],
    ids=[
        "avg-matching-partition-allowed",
        "avg-mismatched-partition-refused",
        "max-mismatched-partition-allowed",
        "first-value-mismatched-partition-allowed",
    ],
)
async def test_avg_over_a_partitioned_grain_requires_a_matching_window(
    client_with_build_v3,
    window_fn,
    partition_col,
    expected,
):
    """AVG must preserve the declared grain; invariant windows need not.

    Every request includes `category`, so a mismatch reaches the window
    guard rather than failing the output-grain check first.
    """
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.category_line_total",
            "description": "Total within category",
            "query": "SELECT SUM(line_total) FROM v3.order_details",
            "fixed_grain": ["v3.product.category"],
            "mode": "published",
        },
    )
    assert response.status_code in (201, 409), response.json()

    name = f"v3.grain_partition_{window_fn.lower()}_{partition_col.split('.')[-1]}"
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": name,
            "description": name,
            "query": (
                f"SELECT {window_fn}(v3.category_line_total) OVER "
                f"(PARTITION BY {partition_col})"
            ),
            "required_dimensions": [partition_col],
            "mode": "published",
        },
    )
    assert response.status_code == 201, response.json()

    dimensions = list(dict.fromkeys(["v3.product.category", partition_col]))
    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={"metrics": [name], "dimensions": dimensions},
    )
    assert response.status_code == expected, response.json()


@pytest.mark.asyncio
async def test_avg_over_a_transformed_partition_column_is_refused(
    client_with_build_v3,
):
    """A transformation on the partition column doesn't count as covering it.

    `PARTITION BY UPPER(category)` groups differently than `PARTITION BY
    category` -- rows for `Books` and `BOOKS` would share a frame even
    though they're different categories at the declared grain. Naming the
    column somewhere inside the expression isn't the same as naming it as
    the partition itself.
    """
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.category_line_total_2",
            "description": "Total within category",
            "query": "SELECT SUM(line_total) FROM v3.order_details",
            "fixed_grain": ["v3.product.category"],
            "mode": "published",
        },
    )
    assert response.status_code in (201, 409), response.json()

    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.grain_partition_transformed",
            "description": "AVG over an uppercased category",
            "query": (
                "SELECT AVG(v3.category_line_total_2) OVER "
                "(PARTITION BY UPPER(v3.product.category))"
            ),
            "required_dimensions": ["v3.product.category"],
            "mode": "published",
        },
    )
    assert response.status_code == 201, response.json()

    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.grain_partition_transformed"],
            "dimensions": ["v3.product.category"],
        },
    )
    assert response.status_code == 422, response.json()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("partition_col", "expected"),
    [
        ("v3.product.category", 200),
        ("v3.customer.name", 422),
    ],
    ids=["matching-partition-allowed", "mismatched-partition-refused"],
)
async def test_avg_over_an_inherited_grain_checks_the_real_grain(
    client_with_build_v3,
    partition_col,
    expected,
):
    """A metric with no `fixed_grain` of its own can still inherit one.

    `v3.scaled_category_total` declares no grain -- it just multiplies
    `v3.category_line_total_4`, which is fixed at `category`. An `AVG` over
    it must still be checked against `category`, not treated as safe just
    because `v3.scaled_category_total` itself has nothing declared.
    """
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.category_line_total_4",
            "description": "Total within category",
            "query": "SELECT SUM(line_total) FROM v3.order_details",
            "fixed_grain": ["v3.product.category"],
            "mode": "published",
        },
    )
    assert response.status_code in (201, 409), response.json()

    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.scaled_category_total",
            "description": "Category total, scaled",
            "query": "SELECT v3.category_line_total_4 * 1.1",
            "mode": "published",
        },
    )
    assert response.status_code in (201, 409), response.json()

    name = f"v3.grain_inherited_{partition_col.split('.')[-1]}"
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": name,
            "description": name,
            "query": (
                "SELECT AVG(v3.scaled_category_total) OVER "
                f"(PARTITION BY {partition_col})"
            ),
            "required_dimensions": [partition_col],
            "mode": "published",
        },
    )
    assert response.status_code == 201, response.json()

    dimensions = list(dict.fromkeys(["v3.product.category", partition_col]))
    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={"metrics": [name], "dimensions": dimensions},
    )
    assert response.status_code == expected, response.json()


@pytest.mark.asyncio
async def test_window_beside_a_broadcast_divisor_is_allowed(client_with_build_v3):
    """A trailing window expressed as a share of a global total.

    The frame is over an ordinary metric; the broadcast is only the divisor, so
    nothing accumulates it. Refusing this was the original guard's headline
    false positive — and its own message told the author to write exactly this.
    """
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.beside_global",
            "description": "Global total",
            "query": "SELECT SUM(line_total) FROM v3.order_details",
            "fixed_grain": [],
            "mode": "published",
        },
    )
    assert response.status_code == 201, response.json()

    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.trailing_share",
            "description": "Trailing 7d revenue as a share of the global total",
            "query": (
                "SELECT SUM(v3.total_revenue) OVER ("
                "ORDER BY v3.date.date_id[order] "
                "ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) / v3.beside_global"
            ),
            "required_dimensions": ["v3.date.date_id[order]"],
            "mode": "published",
        },
    )
    assert response.status_code == 201, response.json()

    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.trailing_share"],
            "dimensions": ["v3.date.date_id[order]"],
        },
    )
    assert response.status_code == 200, response.json()
    sql = response.json()["sql"]
    # The frame is over the ordinary metric; the broadcast is a plain divisor.
    assert "ROWS BETWEEN 6 PRECEDING AND CURRENT ROW" in sql
    assert "/ base_metrics.beside_global" in sql


@pytest.mark.asyncio
async def test_base_metric_may_window_inside_its_own_aggregate(
    client_with_build_v3,
):
    """A window in the argument is not the metric's aggregate being windowed.

    `SUM(x - LAG(x) OVER (...))` decomposes with the window preserved inside the
    component; the broadcast then applies on top. Refusing it confuses "has a
    window somewhere" with "its own aggregate is already windowed".
    """
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.lag_inside_global",
            "description": "Global total of a period delta",
            "query": (
                "SELECT SUM(line_total - LAG(line_total, 1) OVER "
                "(ORDER BY order_id)) FROM v3.order_details"
            ),
            "fixed_grain": [],
            "mode": "published",
        },
    )
    assert response.status_code == 201, response.json()

    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.lag_inside_global"],
            "dimensions": ["v3.order_details.order_id"],
        },
    )
    assert response.status_code == 200, response.json()


@pytest.mark.asyncio
async def test_role_qualified_grain_dimension(client_with_build_v3):
    """`v3.date.date_id[order]` is a dimension reference, not SQL.

    Interpolating it into a `SELECT` to parse it raises, because the role
    brackets are not valid SQL — so the partition column is built directly
    from the reference instead. Queried without requesting or filtering it,
    it's refused for the same reason as an unpinned `v3.product.category`:
    an order's own date isn't guaranteed unique per whatever grain the query
    actually asks for.
    """
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.role_qualified_grain",
            "description": "Total within order date",
            "query": "SELECT SUM(line_total) FROM v3.order_details",
            "fixed_grain": ["v3.date.date_id[order]"],
            "mode": "published",
        },
    )
    assert response.status_code == 201, response.json()

    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.role_qualified_grain"],
            "dimensions": ["v3.order_details.order_id"],
        },
    )
    assert response.status_code == 422, response.json()
    assert "not a requested output dimension" in response.json()["message"]


def test_broadcast_skips_a_merge_call_for_another_component():
    """Only the declaring component's merge is windowed.

    A combiner can hold several calls with the same merge name; wrapping the
    first one found would broadcast the wrong measure.
    """
    from datajunction_server.sql.decompose import _broadcast_in_combiner

    query_ast = parse("SELECT SUM(other_sum) + SUM(line_total_sum) FROM t")
    _broadcast_in_combiner(query_ast, "line_total_sum", "SUM", [])

    rendered = str(query_ast)
    assert "SUM(SUM(line_total_sum)) OVER ()" in rendered.replace("  ", " "), rendered
    # The unrelated aggregate is left alone.
    assert "SUM(SUM(other_sum))" not in rendered, rendered


def test_unfindable_merge_expression_is_refused():
    """A combiner with no recognisable merge call cannot be broadcast."""
    from datajunction_server.sql.decompose import _broadcast_in_combiner

    query_ast = parse("SELECT MAX(other_col) FROM t")
    with pytest.raises(DJInvalidInputException) as exc:
        _broadcast_in_combiner(query_ast, "line_total_sum", "SUM", [])
    assert "could not find the component merge expression" in str(exc.value)


@pytest.mark.asyncio
async def test_derived_metric_cannot_declare_a_grain(client_with_build_v3):
    """A derived metric has no aggregate of its own to pin to a grain."""
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.derived_with_grain",
            "description": "Arithmetic over two metrics",
            "query": "SELECT v3.total_revenue / v3.total_quantity",
            "fixed_grain": [],
            "mode": "published",
        },
    )
    # Creation succeeds because the write path cannot classify this as derived
    # without loading and traversing its metric parents.
    assert response.status_code == 201, response.json()

    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.derived_with_grain"],
            "dimensions": ["v3.order_details.order_id"],
        },
    )
    assert response.status_code == 422, response.json()
    assert "A grain must be declared on a base metric" in response.json()["message"]


@pytest.mark.asyncio
async def test_fixed_grain_is_null_for_non_metric_nodes(client_with_build_v3):
    """A non-metric node reports a null grain over GraphQL."""
    response = await client_with_build_v3.post(
        "/nodes/transform/",
        json={
            "name": "v3.grain_null_transform",
            "description": "Not a metric",
            "query": "SELECT order_id FROM v3.order_details",
            "mode": "published",
        },
    )
    assert response.status_code == 201, response.json()

    response = await client_with_build_v3.post(
        "/graphql",
        json={
            "query": """
                { findNodes(names: ["v3.grain_null_transform"]) {
                    current { fixedGrain }
                } }
            """,
        },
    )
    assert response.status_code == 200, response.json()
    data = response.json()["data"]["findNodes"]
    assert data[0]["current"]["fixedGrain"] is None


def test_non_decomposable_metric_cannot_declare_a_grain():
    """A non-decomposable aggregate yields no components to carry the grain."""
    extractor = MetricComponentExtractor(1)
    query_ast = parse("SELECT MIN_BY(a, b) FROM test.parent")
    components, _ = extractor._extract_base(query_ast)
    assert components == []

    with pytest.raises(DJInvalidInputException) as exc:
        extractor._extract_base(
            parse("SELECT MIN_BY(a, b) FROM test.parent"),
            None,
            [],
        )
    assert "decomposable aggregate" in str(exc.value)


@pytest.mark.asyncio
async def test_the_broadcast_lives_in_the_combiner(client_with_build_v3, session):
    """The window is part of the decomposed metric, not added per query.

    Every consumer renders the combiner: the query builder, and cube
    materialization, which stores it as a string. Putting the window here is
    what lets a materialized cube serve the metric at all.
    """
    from datajunction_server.construction.build_v3.decomposition import (
        decompose_metric,
    )
    from datajunction_server.database.node import Node as DBNode

    for name, grain in (
        ("v3.combiner_global", []),
        ("v3.combiner_category", ["v3.product.category"]),
    ):
        response = await client_with_build_v3.post(
            "/nodes/metric/",
            json={
                "name": name,
                "description": name,
                "query": "SELECT SUM(line_total) FROM v3.order_details",
                "fixed_grain": grain,
                "mode": "published",
            },
        )
        assert response.status_code == 201, response.json()

    node = await DBNode.get_by_name(session, "v3.combiner_global")
    decomposed = await decompose_metric(session, node)
    assert "OVER ()" in str(decomposed.combiner_ast).replace("OVER  ()", "OVER ()")

    node = await DBNode.get_by_name(session, "v3.combiner_category")
    decomposed = await decompose_metric(session, node)
    combiner = str(decomposed.combiner_ast)
    assert "OVER" in combiner and "PARTITION BY" in combiner
    # Unresolved: each consumer replaces the dimension reference for itself.
    assert "v3.product.category" in combiner
