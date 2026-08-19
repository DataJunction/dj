"""
Tests for the cardinality-aware fan-out guard (DJ issue #1531).

When a metric's dimension join path crosses a link declared ONE_TO_MANY or
MANY_TO_MANY, one fact row matches many dimension rows, duplicating the fact's
measures. Additive aggregations (SUM/COUNT/AVG) over the duplicated rows
over-count. The V3 builder still generates SQL (DJ cannot statically prove a
filter makes the join safe, so a hard error would be a false positive) but
attaches a ``FANOUT_RISK`` warning to the response.

These tests exercise that guard end-to-end through the /sql/measures/v3/ and
/sql/metrics/v3/ endpoints, plus the negative cases that must NOT trip it
(safe cardinalities and fan-out-immune aggregations).
"""

import pytest


def fanout_warnings(payload: dict) -> list[dict]:
    """Return the FANOUT_RISK warnings on a measures/metrics SQL response."""
    return [
        warning
        for warning in payload.get("warnings") or []
        if warning.get("code") == "FANOUT_RISK"
    ]


@pytest.fixture
async def setup_fanout_links(client_with_build_v3):
    """
    Add a dimension that fans out from ``v3.order_details``.

    A single order line item maps to many promotions (one order has several
    promo codes applied), so the link is declared ``one_to_many``. A second
    ``many_to_many`` link is added to exercise that cardinality too. A ``MIN``
    metric is added to cover the duplication-immune aggregation case.

    Defined locally so only the fan-out tests pay the setup cost; the global
    BUILD_V3 fixture is unaffected.
    """
    response = await client_with_build_v3.post(
        "/nodes/source/",
        json={
            "name": "v3.src_order_promotions",
            "description": "Promotions applied to orders (many per order)",
            "columns": [
                {"name": "promo_code", "type": "string"},
                {"name": "order_id", "type": "int"},
                {"name": "discount", "type": "float"},
                {"name": "campaign", "type": "string"},
            ],
            "mode": "published",
            "catalog": "default",
            "schema_": "v3",
            "table": "order_promotions",
        },
    )
    assert response.status_code in (200, 201, 409), response.json()

    response = await client_with_build_v3.post(
        "/nodes/dimension/",
        json={
            "name": "v3.order_promotion",
            "description": "Promotion dimension keyed on promo_code",
            "query": (
                "SELECT promo_code, order_id, discount, campaign "
                "FROM v3.src_order_promotions"
            ),
            "primary_key": ["promo_code"],
            "mode": "published",
        },
    )
    assert response.status_code in (200, 201, 409), response.json()

    # order_details -> order_promotion: one order line maps to many promotions.
    response = await client_with_build_v3.post(
        "/nodes/v3.order_details/link",
        json={
            "dimension_node": "v3.order_promotion",
            "join_type": "left",
            "join_on": "v3.order_details.order_id = v3.order_promotion.order_id",
            "join_cardinality": "one_to_many",
        },
    )
    assert response.status_code in (200, 201, 409), response.json()

    # A second dimension linked many_to_many, to exercise that cardinality.
    response = await client_with_build_v3.post(
        "/nodes/source/",
        json={
            "name": "v3.src_order_channels",
            "description": "Sales channels associated with orders (many-to-many)",
            "columns": [
                {"name": "channel_id", "type": "int"},
                {"name": "order_id", "type": "int"},
                {"name": "channel_name", "type": "string"},
            ],
            "mode": "published",
            "catalog": "default",
            "schema_": "v3",
            "table": "order_channels",
        },
    )
    assert response.status_code in (200, 201, 409), response.json()

    response = await client_with_build_v3.post(
        "/nodes/dimension/",
        json={
            "name": "v3.order_channel",
            "description": "Channel dimension keyed on channel_id",
            "query": (
                "SELECT channel_id, order_id, channel_name FROM v3.src_order_channels"
            ),
            "primary_key": ["channel_id"],
            "mode": "published",
        },
    )
    assert response.status_code in (200, 201, 409), response.json()

    response = await client_with_build_v3.post(
        "/nodes/v3.order_details/link",
        json={
            "dimension_node": "v3.order_channel",
            "join_type": "left",
            "join_on": "v3.order_details.order_id = v3.order_channel.order_id",
            "join_cardinality": "many_to_many",
        },
    )
    assert response.status_code in (200, 201, 409), response.json()

    # A MIN metric: its merge function is MIN (non-additive), so it's
    # duplication-immune and must NOT trip the guard even across a fan-out link.
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.min_unit_price",
            "description": "Minimum unit price (duplication-immune aggregation)",
            "query": "SELECT MIN(unit_price) FROM v3.order_details",
            "mode": "published",
        },
    )
    assert response.status_code in (200, 201, 409), response.json()

    # A COUNT_IF metric: merge=SUM (additive), so it inflates under fan-out exactly
    # like SUM/COUNT. Its phase-1 aggregation name is "COUNT_IF" (not SUM/COUNT/AVG),
    # so keying on the merge function — not the aggregation name — is what catches it.
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.completed_order_count",
            "description": "Count of completed order lines (conditional count)",
            "query": "SELECT COUNT_IF(status = 'completed') FROM v3.order_details",
            "mode": "published",
        },
    )
    assert response.status_code in (200, 201, 409), response.json()

    # A MEDIAN metric: non-decomposable (holistic), so it builds via raw
    # passthrough and is aggregated over the duplicated rows. It depends on the
    # multiset of rows, so a fan-out distorts it and the guard MUST fire.
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.median_unit_price",
            "description": "Median unit price (non-decomposable, duplication-sensitive)",
            "query": "SELECT MEDIAN(unit_price) FROM v3.order_details",
            "mode": "published",
        },
    )
    assert response.status_code in (200, 201, 409), response.json()

    # A MAX_BY metric: non-decomposable too, but argmax reads a single extreme
    # row, so duplication can't change it. It must NOT trip the guard.
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.unit_price_at_max_order",
            "description": "Unit price of the highest order_id (duplication-immune argmax)",
            "query": "SELECT MAX_BY(unit_price, order_id) FROM v3.order_details",
            "mode": "published",
        },
    )
    assert response.status_code in (200, 201, 409), response.json()

    # Non-decomposable (MAX_BY) but embeds COUNT(DISTINCT). Both terms are immune, so
    # the metric is too — the embedded-DISTINCT case that must NOT trip the guard.
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.price_at_max_order_per_distinct_order",
            "description": "Argmax price over a distinct-order count (both immune)",
            "query": (
                "SELECT MAX_BY(unit_price, order_id) / COUNT(DISTINCT order_id) "
                "FROM v3.order_details"
            ),
            "mode": "published",
        },
    )
    assert response.status_code in (200, 201, 409), response.json()

    # An APPROX_COUNT_DISTINCT metric: decomposable, but its component merges via
    # hll_union_agg (not SUM), so duplication can't inflate it. Must NOT warn.
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.approx_order_count",
            "description": "Approximate distinct orders (HLL, duplication-immune)",
            "query": "SELECT APPROX_COUNT_DISTINCT(order_id) FROM v3.order_details",
            "mode": "published",
        },
    )
    assert response.status_code in (200, 201, 409), response.json()

    # A metric that SUMs a dimension attribute reachable only through the fan-out
    # link. The join is emitted even when no promotion dimension is requested, so the
    # guard must see it — this is the metric-expression-dimension fan-out case.
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.total_promo_discount",
            "description": "Total promotion discount (SUM over a fan-out dimension attr)",
            "query": "SELECT SUM(v3.order_promotion.discount) FROM v3.order_details",
            "mode": "published",
        },
    )
    assert response.status_code in (200, 201, 409), response.json()


class TestFanoutGuardWarns:
    """Queries that cross a fan-out link with an additive metric must warn (not block)."""

    @pytest.mark.asyncio
    async def test_sum_over_one_to_many_warns(
        self,
        client_with_build_v3,
        setup_fanout_links,
    ):
        """SUM across a one_to_many link inflates → 200 with an actionable warning."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_promotion.campaign"],
            },
        )
        assert response.status_code == 200, response.json()
        warnings = fanout_warnings(response.json())
        assert len(warnings) == 1, response.json()
        message = warnings[0]["message"]
        assert "v3.total_revenue" in message
        assert "fan-out" in message
        assert "one_to_many" in message
        # debug carries the same facts structurally, so clients don't parse the prose.
        assert warnings[0]["debug"] == {
            "metrics": ["v3.total_revenue"],
            "base_metrics": ["v3.total_revenue"],
            "links": [
                {
                    "node": "v3.order_details",
                    "dimension": "v3.order_promotion",
                    "role": None,
                    "join_cardinality": "one_to_many",
                },
            ],
            "parent": "v3.order_details",
        }

    @pytest.mark.asyncio
    async def test_sum_over_many_to_many_warns(
        self,
        client_with_build_v3,
        setup_fanout_links,
    ):
        """SUM across a many_to_many link inflates → 200 with a warning."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_channel.channel_name"],
            },
        )
        assert response.status_code == 200, response.json()
        warnings = fanout_warnings(response.json())
        assert warnings, response.json()
        assert "many_to_many" in warnings[0]["message"]

    @pytest.mark.asyncio
    async def test_avg_over_one_to_many_warns(
        self,
        client_with_build_v3,
        setup_fanout_links,
    ):
        """AVG decomposes into SUM/COUNT components, both inflate → 200 + warning."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.avg_unit_price"],
                "dimensions": ["v3.order_promotion.campaign"],
            },
        )
        assert response.status_code == 200, response.json()
        assert fanout_warnings(response.json()), response.json()

    @pytest.mark.asyncio
    async def test_count_if_over_one_to_many_warns(
        self,
        client_with_build_v3,
        setup_fanout_links,
    ):
        """COUNT_IF has merge=SUM (additive) → inflates → 200 + warning.

        COUNT_IF's phase-1 aggregation name is not in {SUM,COUNT,AVG}, so a name-based
        guard would miss it; keying on the merge function is what catches it.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.completed_order_count"],
                "dimensions": ["v3.order_promotion.campaign"],
            },
        )
        assert response.status_code == 200, response.json()
        assert fanout_warnings(response.json()), response.json()

    @pytest.mark.asyncio
    async def test_median_over_one_to_many_warns(
        self,
        client_with_build_v3,
        setup_fanout_links,
    ):
        """A non-decomposable holistic metric (MEDIAN) inflates → 200 + warning.

        MEDIAN has no components and no SUM merge, so the merge check alone can't
        see it; it's caught via the grain group's non-decomposable metrics.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.median_unit_price"],
                "dimensions": ["v3.order_promotion.campaign"],
            },
        )
        assert response.status_code == 200, response.json()
        warnings = fanout_warnings(response.json())
        assert len(warnings) == 1, response.json()
        assert "v3.median_unit_price" in warnings[0]["message"]

    @pytest.mark.asyncio
    async def test_metric_expression_dimension_fanout_warns(
        self,
        client_with_build_v3,
        setup_fanout_links,
    ):
        """A dimension attribute SUM'd inside the metric expression still fans out.

        No promotion dimension is requested, but the metric references
        v3.order_promotion.discount, so the build emits the one_to_many join anyway.
        The guard must scan the emitted join paths, not just the requested dimensions.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_promo_discount"],
                "dimensions": [],
            },
        )
        assert response.status_code == 200, response.json()
        warnings = fanout_warnings(response.json())
        assert len(warnings) == 1, response.json()
        assert "v3.total_promo_discount" in warnings[0]["message"]

    @pytest.mark.asyncio
    async def test_metrics_endpoint_also_warns(
        self,
        client_with_build_v3,
        setup_fanout_links,
    ):
        """The guard fires on /sql/metrics/v3/ as well, not just measures."""
        response = await client_with_build_v3.get(
            "/sql/metrics/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_promotion.campaign"],
            },
        )
        assert response.status_code == 200, response.json()
        assert fanout_warnings(response.json()), response.json()

    @pytest.mark.asyncio
    async def test_node_sql_endpoint_also_warns(
        self,
        client_with_build_v3,
        setup_fanout_links,
    ):
        """
        /sql/{node_name}/ builds a metric node through v3 as well, so its
        TranslatedSQL response has to carry the warning too.
        """
        response = await client_with_build_v3.get(
            "/sql/v3.total_revenue/",
            params={"dimensions": ["v3.order_promotion.campaign"]},
        )
        assert response.status_code == 200, response.json()
        assert fanout_warnings(response.json()), response.json()

    @pytest.mark.asyncio
    async def test_derived_metric_warning_names_the_requested_metric(
        self,
        client_with_build_v3,
        setup_fanout_links,
    ):
        """
        A derived metric is decomposed into its base metrics before grain grouping,
        so the inflating aggregation belongs to a metric the caller never named.
        The warning must name what was requested (v3.avg_order_value) and keep the
        base metric in debug.

        v3.avg_order_value = v3.total_revenue / v3.order_count, and only the SUM in
        v3.total_revenue inflates -- v3.order_count is COUNT(DISTINCT ...).
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.avg_order_value"],
                "dimensions": ["v3.order_promotion.campaign"],
            },
        )
        assert response.status_code == 200, response.json()
        warnings = fanout_warnings(response.json())
        assert len(warnings) == 1, response.json()
        assert warnings[0]["debug"]["metrics"] == ["v3.avg_order_value"]
        assert warnings[0]["debug"]["base_metrics"] == ["v3.total_revenue"]
        message = warnings[0]["message"]
        assert "v3.avg_order_value" in message
        assert "v3.total_revenue" not in message

    @pytest.mark.asyncio
    async def test_warning_names_only_the_inflated_metric(
        self,
        client_with_build_v3,
        setup_fanout_links,
    ):
        """
        A requested metric that survives duplication must stay out of the warning.

        v3.total_revenue (SUM) and v3.order_count (COUNT(DISTINCT ...)) share a parent,
        so both land in the same grain group, but only the SUM inflates. Naming both
        would send the caller to fix a metric that is already correct.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue", "v3.order_count"],
                "dimensions": ["v3.order_promotion.campaign"],
            },
        )
        assert response.status_code == 200, response.json()
        warnings = fanout_warnings(response.json())
        assert len(warnings) == 1, response.json()
        assert warnings[0]["debug"]["metrics"] == ["v3.total_revenue"]
        assert warnings[0]["debug"]["base_metrics"] == ["v3.total_revenue"]
        assert "v3.order_count" not in warnings[0]["message"]

    @pytest.mark.asyncio
    async def test_window_metric_over_one_to_many_warns(
        self,
        client_with_build_v3,
        setup_fanout_links,
    ):
        """
        A period-over-period (window) metric warns when its base measure grain
        crosses a fan-out link.

        Requesting the window's ORDER BY dimension, a second dimension from the
        same date node (so it is filtered out of the window grain), plus the
        fan-out dimension forces a separate window grain group at the fan-out
        grain, so the guard must fire on that grain group too (not just the base
        metric path).
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.wow_revenue_change"],
                "dimensions": [
                    "v3.date.week[order]",
                    "v3.date.month[order]",
                    "v3.order_promotion.campaign",
                ],
            },
        )
        assert response.status_code == 200, response.json()
        warnings = fanout_warnings(response.json())
        assert warnings, response.json()
        # Derived from v3.total_revenue, so the same requested-name rule applies.
        assert warnings[0]["debug"]["metrics"] == ["v3.wow_revenue_change"]
        assert warnings[0]["debug"]["base_metrics"] == ["v3.total_revenue"]


class TestFanoutGuardAllows:
    """Queries that are safe (or fan-out-immune) must NOT warn."""

    @pytest.mark.asyncio
    async def test_count_distinct_over_one_to_many_ok(
        self,
        client_with_build_v3,
        setup_fanout_links,
    ):
        """COUNT(DISTINCT) has no merge function → fan-out-immune → no warning."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.order_count"],
                "dimensions": ["v3.order_promotion.campaign"],
            },
        )
        assert response.status_code == 200, response.json()
        assert not fanout_warnings(response.json()), response.json()

    @pytest.mark.asyncio
    async def test_min_over_one_to_many_ok(
        self,
        client_with_build_v3,
        setup_fanout_links,
    ):
        """MIN's merge function is MIN, not additive → duplication-immune → no warning."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.min_unit_price"],
                "dimensions": ["v3.order_promotion.campaign"],
            },
        )
        assert response.status_code == 200, response.json()
        assert not fanout_warnings(response.json()), response.json()

    @pytest.mark.asyncio
    async def test_max_by_over_one_to_many_ok(
        self,
        client_with_build_v3,
        setup_fanout_links,
    ):
        """MAX_BY is non-decomposable but argmax reads one extreme row → immune → no warning."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.unit_price_at_max_order"],
                "dimensions": ["v3.order_promotion.campaign"],
            },
        )
        assert response.status_code == 200, response.json()
        assert not fanout_warnings(response.json()), response.json()

    @pytest.mark.asyncio
    async def test_approx_count_distinct_over_one_to_many_ok(
        self,
        client_with_build_v3,
        setup_fanout_links,
    ):
        """APPROX_COUNT_DISTINCT merges via hll_union_agg (not SUM) → immune → no warning."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.approx_order_count"],
                "dimensions": ["v3.order_promotion.campaign"],
            },
        )
        assert response.status_code == 200, response.json()
        assert not fanout_warnings(response.json()), response.json()

    @pytest.mark.asyncio
    async def test_embedded_count_distinct_over_one_to_many_ok(
        self,
        client_with_build_v3,
        setup_fanout_links,
    ):
        """
        A non-decomposable metric embedding COUNT(DISTINCT) alongside an argmax is
        duplication-immune. Keying invariance on the Count class alone would flag it;
        reading the DISTINCT off the call keeps it quiet.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.price_at_max_order_per_distinct_order"],
                "dimensions": ["v3.order_promotion.campaign"],
            },
        )
        assert response.status_code == 200, response.json()
        assert not fanout_warnings(response.json()), response.json()

    @pytest.mark.asyncio
    async def test_sum_over_safe_many_to_one_ok(
        self,
        client_with_build_v3,
        setup_fanout_links,
    ):
        """SUM across the default many_to_one customer link does not fan out → no warning."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.customer.name"],
            },
        )
        assert response.status_code == 200, response.json()
        assert not fanout_warnings(response.json()), response.json()

    @pytest.mark.asyncio
    async def test_sum_without_fanout_dimension_ok(
        self,
        client_with_build_v3,
        setup_fanout_links,
    ):
        """
        The same SUM metric is fine as long as the fan-out dimension is not
        requested — the unsafe link is never traversed.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
            },
        )
        assert response.status_code == 200, response.json()
        assert not fanout_warnings(response.json()), response.json()


class TestFanoutGuardCombinedEndpoint:
    """The combined endpoint must surface the warning on BOTH the compute-from-source
    and the pre-agg (Druid) paths — the pre-agg path builds measures from source
    internally, so it has the warning available and must not drop it."""

    @pytest.mark.asyncio
    async def test_combined_source_path_warns(
        self,
        client_with_build_v3,
        setup_fanout_links,
    ):
        """SUM across a one_to_many link, combined-from-source → 200 + warning."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/combined",
            params={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_promotion.campaign"],
            },
        )
        assert response.status_code == 200, response.json()
        assert fanout_warnings(response.json()), response.json()

    @pytest.mark.asyncio
    async def test_combined_preagg_path_warns(
        self,
        client_with_build_v3,
        setup_fanout_links,
    ):
        """
        The pre-agg (Druid) path warns at query time.

        ``use_preagg_tables=true`` reads from pre-agg tables, but the builder still
        computes measures from source internally to derive the grain groups, so the
        fan-out risk is known and must be surfaced.
        """
        response = await client_with_build_v3.get(
            "/sql/measures/v3/combined",
            params={
                "use_preagg_tables": "true",
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_promotion.campaign"],
            },
        )
        assert response.status_code == 200, response.json()
        assert response.json()["use_preagg_tables"] is True
        warnings = fanout_warnings(response.json())
        assert len(warnings) == 1, response.json()
        assert "one_to_many" in warnings[0]["message"]

    @pytest.mark.asyncio
    async def test_combined_preagg_path_without_fanout_ok(
        self,
        client_with_build_v3,
        setup_fanout_links,
    ):
        """The pre-agg path does not warn when no fan-out dimension is requested."""
        response = await client_with_build_v3.get(
            "/sql/measures/v3/combined",
            params={
                "use_preagg_tables": "true",
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_details.status"],
            },
        )
        assert response.status_code == 200, response.json()
        assert not fanout_warnings(response.json()), response.json()


class TestFanoutGuardPreaggPlan:
    """The /preaggs/plan endpoint must surface the warning too — it hands back
    materialization SQL that Flow B (user-managed) callers run in their own
    engine, so they must see the fan-out risk before materializing inflated
    measures."""

    @pytest.mark.asyncio
    async def test_preaggs_plan_over_one_to_many_warns(
        self,
        client_with_build_v3,
        setup_fanout_links,
    ):
        """Planning a pre-agg for SUM across a one_to_many link → 201 + warning."""
        response = await client_with_build_v3.post(
            "/preaggs/plan",
            json={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.order_promotion.campaign"],
                "strategy": "full",
            },
        )
        assert response.status_code == 201, response.json()
        warnings = fanout_warnings(response.json())
        assert len(warnings) == 1, response.json()
        assert "one_to_many" in warnings[0]["message"]

    @pytest.mark.asyncio
    async def test_preaggs_plan_without_fanout_ok(
        self,
        client_with_build_v3,
        setup_fanout_links,
    ):
        """Planning a pre-agg without a fan-out dimension does not warn."""
        response = await client_with_build_v3.post(
            "/preaggs/plan",
            json={
                "metrics": ["v3.total_revenue"],
                "dimensions": ["v3.product.category"],
                "strategy": "full",
            },
        )
        assert response.status_code == 201, response.json()
        assert not fanout_warnings(response.json()), response.json()
