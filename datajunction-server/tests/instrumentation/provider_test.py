"""
Tests for ``datajunction_server.instrumentation.provider``.
"""

import pytest

from datajunction_server.instrumentation.provider import (
    MetricsProvider,
    NoOpMetricsProvider,
    get_metrics_provider,
    set_metrics_provider,
)


class _SpyProvider(MetricsProvider):
    """Minimal concrete provider that records all calls for inspection."""

    def __init__(self):
        self.counters: list[tuple] = []
        self.gauges: list[tuple] = []
        self.timers: list[tuple] = []

    def counter(self, name, value=1, tags=None):
        self.counters.append((name, value, tags))

    def gauge(self, name, value, tags=None):
        self.gauges.append((name, value, tags))

    def timer(self, name, value_ms, tags=None):
        self.timers.append((name, value_ms, tags))


@pytest.fixture(autouse=True)
def reset_provider():
    """Restore the default NoOpMetricsProvider after every test."""
    original = get_metrics_provider()
    yield
    set_metrics_provider(original)


def test_no_op_provider_counter_is_safe():
    NoOpMetricsProvider().counter("some.metric", 3, {"tag": "val"})


def test_no_op_provider_gauge_is_safe():
    NoOpMetricsProvider().gauge("some.gauge", 42.0)


def test_no_op_provider_timer_is_safe():
    NoOpMetricsProvider().timer("some.timer", 123.4, None)


def test_get_metrics_provider_returns_default_noop():
    assert isinstance(get_metrics_provider(), NoOpMetricsProvider)


def test_set_metrics_provider_replaces_singleton():
    spy = _SpyProvider()
    set_metrics_provider(spy)
    assert get_metrics_provider() is spy


def test_set_and_use_custom_provider():
    spy = _SpyProvider()
    set_metrics_provider(spy)

    p = get_metrics_provider()
    p.counter("dj.cache.hit", tags={"query_type": "METRICS"})
    p.gauge("dj.db.pool.checked_out", 5.0)
    p.timer("dj.request", 42.0, {"route": "/health"})

    assert spy.counters == [("dj.cache.hit", 1, {"query_type": "METRICS"})]
    assert spy.gauges == [("dj.db.pool.checked_out", 5.0, None)]
    assert spy.timers == [("dj.request", 42.0, {"route": "/health"})]
