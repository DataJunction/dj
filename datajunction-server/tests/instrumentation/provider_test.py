"""
Tests for ``datajunction_server.instrumentation.provider``.
"""

import pytest

from datajunction_server.instrumentation.provider import (
    MetricsProvider,
    NoOpMetricsProvider,
    get_metrics_provider,
    set_metrics_provider,
    timed,
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


# ---------------------------------------------------------------------------
# @timed decorator tests
# ---------------------------------------------------------------------------


def test_timed_sync_emits_timer():
    spy = _SpyProvider()
    set_metrics_provider(spy)

    @timed("dj.test.sync_ms")
    def do_work(x):
        return x * 2

    result = do_work(3)

    assert result == 6
    assert len(spy.timers) == 1
    name, value_ms, tags = spy.timers[0]
    assert name == "dj.test.sync_ms"
    assert value_ms >= 0
    assert tags is None


@pytest.mark.asyncio
async def test_timed_async_emits_timer():
    spy = _SpyProvider()
    set_metrics_provider(spy)

    @timed("dj.test.async_ms")
    async def do_work_async(x):
        return x + 1

    result = await do_work_async(5)

    assert result == 6
    assert len(spy.timers) == 1
    name, value_ms, tags = spy.timers[0]
    assert name == "dj.test.async_ms"
    assert value_ms >= 0
    assert tags is None


def test_timed_sync_static_tags():
    spy = _SpyProvider()
    set_metrics_provider(spy)

    @timed("dj.test.tagged_ms", {"query_type": "NODE"})
    def do_work():
        return 42

    do_work()

    assert spy.timers[0][2] == {"query_type": "NODE"}


@pytest.mark.asyncio
async def test_timed_async_callable_tags():
    spy = _SpyProvider()
    set_metrics_provider(spy)

    @timed("dj.test.callable_tags_ms", lambda self, *a, **kw: {"kind": self.kind})
    async def do_work(self):
        return self.kind

    class _Ctx:
        kind = "METRICS"

    await do_work(_Ctx())

    assert spy.timers[0][2] == {"kind": "METRICS"}


def test_timed_sync_emits_even_on_exception():
    spy = _SpyProvider()
    set_metrics_provider(spy)

    @timed("dj.test.exc_ms")
    def boom():
        raise ValueError("oops")

    with pytest.raises(ValueError):
        boom()

    assert len(spy.timers) == 1
    assert spy.timers[0][0] == "dj.test.exc_ms"


@pytest.mark.asyncio
async def test_timed_async_emits_even_on_exception():
    spy = _SpyProvider()
    set_metrics_provider(spy)

    @timed("dj.test.async_exc_ms")
    async def async_boom():
        raise RuntimeError("async oops")

    with pytest.raises(RuntimeError):
        await async_boom()

    assert len(spy.timers) == 1
    assert spy.timers[0][0] == "dj.test.async_exc_ms"


def test_timed_preserves_function_name_and_docstring():
    @timed("dj.test.meta_ms")
    def my_function():
        """My docstring."""
        return 1

    assert my_function.__name__ == "my_function"
    assert my_function.__doc__ == "My docstring."


@pytest.mark.asyncio
async def test_timed_async_preserves_function_name():
    @timed("dj.test.async_meta_ms")
    async def my_async_function():
        """Async docstring."""
        return 2

    assert my_async_function.__name__ == "my_async_function"
    assert my_async_function.__doc__ == "Async docstring."
