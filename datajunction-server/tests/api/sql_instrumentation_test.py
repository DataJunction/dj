"""
Tests for SQL endpoint instrumentation: structured per-request log lines emitted
alongside ``dj.sql.build_latency_ms`` so slow requests can be replayed by
metrics/dimensions/filters from log search (Radar) without parsing URLs.
"""

import logging
from typing import Any

import pytest
from httpx import AsyncClient


def _structured_extras(caplog) -> list[dict[str, Any]]:
    """
    Return a dict-of-extras for every log record we attached an ``endpoint``
    extra to. Reading via getattr keeps type checkers happy — LogRecord doesn't
    declare these attributes, but logging adds them when ``extra={}`` is passed.
    """
    out: list[dict[str, Any]] = []
    for r in caplog.records:
        endpoint = getattr(r, "endpoint", None)
        if endpoint is None:
            continue
        out.append(
            {
                "endpoint": endpoint,
                "query_type": getattr(r, "query_type", None),
                "query_version": getattr(r, "query_version", None),
                "metrics": getattr(r, "metrics", None),
                "dimensions": getattr(r, "dimensions", None),
                "filters": getattr(r, "filters", None),
                "elapsed_ms": getattr(r, "elapsed_ms", None),
            },
        )
    return out


@pytest.mark.asyncio
async def test_v3_measures_endpoint_emits_structured_log(
    client_with_build_v3: AsyncClient,
    caplog,
):
    """`/sql/measures/v3/` should emit a structured log with extra={...}."""
    caplog.set_level(logging.INFO, logger="datajunction_server.api.sql")
    response = await client_with_build_v3.get(
        "/sql/measures/v3/",
        params={
            "metrics": ["v3.total_revenue"],
            "dimensions": ["v3.order_details.status"],
        },
    )
    assert response.status_code == 200

    matching = [
        e for e in _structured_extras(caplog) if e["endpoint"] == "/sql/measures/v3/"
    ]
    assert matching, "no structured log for /sql/measures/v3/"
    extra = matching[-1]
    assert extra["query_type"] == "measures"
    assert extra["query_version"] == "v3"
    assert extra["metrics"] == ["v3.total_revenue"]
    assert extra["dimensions"] == ["v3.order_details.status"]
    assert extra["filters"] is not None
    assert isinstance(extra["elapsed_ms"], float)


@pytest.mark.asyncio
async def test_v3_metrics_endpoint_emits_structured_log(
    client_with_build_v3: AsyncClient,
    caplog,
):
    """`/sql/metrics/v3/` should emit a structured log with extra={...}."""
    caplog.set_level(logging.INFO, logger="datajunction_server.api.sql")
    response = await client_with_build_v3.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.total_revenue"],
            "dimensions": ["v3.order_details.status"],
        },
    )
    assert response.status_code == 200

    matching = [
        e for e in _structured_extras(caplog) if e["endpoint"] == "/sql/metrics/v3/"
    ]
    assert matching, "no structured log for /sql/metrics/v3/"
    extra = matching[-1]
    assert extra["query_type"] == "metrics"
    assert extra["query_version"] == "v3"
    assert extra["metrics"] is not None
    assert extra["dimensions"] is not None
    assert extra["filters"] is not None
    assert isinstance(extra["elapsed_ms"], float)


@pytest.mark.asyncio
async def test_v3_combined_measures_endpoint_emits_structured_log(
    client_with_build_v3: AsyncClient,
    caplog,
):
    """`/sql/measures/v3/combined` should emit a structured log with extra={...}."""
    caplog.set_level(logging.INFO, logger="datajunction_server.api.sql")
    response = await client_with_build_v3.get(
        "/sql/measures/v3/combined",
        params={
            "metrics": ["v3.total_revenue"],
            "dimensions": ["v3.order_details.status"],
        },
    )
    assert response.status_code == 200

    matching = [
        e
        for e in _structured_extras(caplog)
        if e["endpoint"] == "/sql/measures/v3/combined"
    ]
    assert matching, "no structured log for /sql/measures/v3/combined"
    extra = matching[-1]
    assert extra["query_type"] == "measures_combined"
    assert extra["query_version"] == "v3"
    assert extra["metrics"] is not None
    assert extra["dimensions"] is not None
    assert extra["filters"] is not None
    assert isinstance(extra["elapsed_ms"], float)


@pytest.mark.asyncio
async def test_v2_measures_endpoint_emits_structured_log(
    client_with_roads: AsyncClient,
    caplog,
):
    """
    The legacy `/sql/measures/v2/` endpoint routes through QueryCacheManager.
    Its fallback() should emit a structured log (with extra={...}) tagged
    query_version=v2 — matching the v2 builder it actually runs.
    """
    caplog.set_level(logging.INFO, logger="QueryCacheManager")
    response = await client_with_roads.get(
        "/sql/measures/v2",
        params={
            "metrics": ["default.num_repair_orders"],
            "dimensions": ["default.dispatcher.company_name"],
        },
        headers={"Cache-Control": "no-cache, no-store"},
    )
    assert response.status_code == 200

    matching = _structured_extras(caplog)
    assert matching, "no structured log emitted by QueryCacheManager.fallback"
    extra = matching[-1]
    assert extra["query_version"] == "v2"
    assert extra["metrics"] == ["default.num_repair_orders"]
    assert extra["dimensions"] == ["default.dispatcher.company_name"]
    assert extra["filters"] is not None
    assert isinstance(extra["elapsed_ms"], float)
