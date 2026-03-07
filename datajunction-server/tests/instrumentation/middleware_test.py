"""
Tests for ``datajunction_server.instrumentation.middleware``.
"""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import datajunction_server.instrumentation.middleware as mw_module
from datajunction_server.instrumentation.middleware import (
    DJInstrumentationMiddleware,
    _emit_pool_gauges,
)
from datajunction_server.instrumentation.provider import (
    MetricsProvider,
    set_metrics_provider,
    get_metrics_provider,
)


class _SpyProvider(MetricsProvider):
    def __init__(self):
        self.gauges: list[tuple] = []
        self.timers: list[tuple] = []

    def counter(self, name, value=1, tags=None):
        pass

    def gauge(self, name, value, tags=None):
        self.gauges.append((name, value, tags))

    def timer(self, name, value_ms, tags=None):
        self.timers.append((name, value_ms, tags))


@pytest.fixture(autouse=True)
def reset_provider():
    original = get_metrics_provider()
    original_in_flight = mw_module._in_flight
    yield
    set_metrics_provider(original)
    mw_module._in_flight = original_in_flight


def _make_app_with_route(status_code: int = 200) -> FastAPI:
    """Build a minimal FastAPI app with the instrumentation middleware attached."""
    app = FastAPI()
    app.add_middleware(DJInstrumentationMiddleware)

    @app.get("/ping")
    def ping():
        return {"ok": True}

    return app


# ---------------------------------------------------------------------------
# _emit_pool_gauges
# ---------------------------------------------------------------------------


def test_emit_pool_gauges_happy_path():
    spy = _SpyProvider()
    mock_pool = MagicMock()
    mock_pool.size.return_value = 20
    mock_pool.checkedout.return_value = 5
    mock_pool.overflow.return_value = 0

    mock_engine = MagicMock()
    mock_engine.pool = mock_pool

    mock_manager = MagicMock()
    mock_manager.writer_engine = mock_engine

    with patch(
        "datajunction_server.utils.get_session_manager",
        return_value=mock_manager,
    ):
        _emit_pool_gauges(spy)

    gauge_names = [name for name, _, _ in spy.gauges]
    assert "dj.db.pool.size" in gauge_names
    assert "dj.db.pool.checked_out" in gauge_names
    assert "dj.db.pool.available" in gauge_names
    assert "dj.db.pool.overflow" in gauge_names

    by_name = {name: val for name, val, _ in spy.gauges}
    assert by_name["dj.db.pool.size"] == 20
    assert by_name["dj.db.pool.checked_out"] == 5
    assert by_name["dj.db.pool.available"] == 15
    assert by_name["dj.db.pool.overflow"] == 0


def test_emit_pool_gauges_swallows_exception():
    """If the session manager is not initialised, no exception should propagate."""
    spy = _SpyProvider()
    with patch(
        "datajunction_server.utils.get_session_manager",
        side_effect=RuntimeError("not initialised"),
    ):
        _emit_pool_gauges(spy)  # must not raise

    assert spy.gauges == []


# ---------------------------------------------------------------------------
# DJInstrumentationMiddleware via TestClient
# ---------------------------------------------------------------------------


def test_middleware_emits_request_timer_on_success():
    spy = _SpyProvider()
    set_metrics_provider(spy)

    mock_pool = MagicMock()
    mock_pool.size.return_value = 10
    mock_pool.checkedout.return_value = 2
    mock_pool.overflow.return_value = 0
    mock_engine = MagicMock()
    mock_engine.pool = mock_pool
    mock_manager = MagicMock()
    mock_manager.writer_engine = mock_engine

    app = _make_app_with_route()
    with patch(
        "datajunction_server.utils.get_session_manager",
        return_value=mock_manager,
    ):
        client = TestClient(app)
        response = client.get("/ping")

    assert response.status_code == 200

    timer_names = [name for name, _, _ in spy.timers]
    assert "dj.request" in timer_names

    timer_tags = {name: tags for name, _, tags in spy.timers}
    assert timer_tags["dj.request"]["status_code"] == "200"
    assert timer_tags["dj.request"]["method"] == "GET"
    assert timer_tags["dj.request"]["route"] == "/ping"

    # in-flight should go up then back down to 0
    gauge_names = [name for name, _, _ in spy.gauges]
    assert "dj.request.in_flight" in gauge_names
    in_flight_values = [
        val for name, val, _ in spy.gauges if name == "dj.request.in_flight"
    ]
    assert in_flight_values[0] == 1  # incremented on entry
    assert in_flight_values[-1] == 0  # back to 0 after exit


def test_middleware_uses_url_path_when_route_missing():
    """When no route is matched (e.g. 404), fall back to request.url.path."""
    spy = _SpyProvider()
    set_metrics_provider(spy)

    mock_manager = MagicMock()
    mock_manager.writer_engine.pool.size.return_value = 10
    mock_manager.writer_engine.pool.checkedout.return_value = 0
    mock_manager.writer_engine.pool.overflow.return_value = 0

    app = _make_app_with_route()
    with patch(
        "datajunction_server.utils.get_session_manager",
        return_value=mock_manager,
    ):
        client = TestClient(app, raise_server_exceptions=False)
        client.get("/nonexistent")

    timer_tags = {name: tags for name, _, tags in spy.timers}
    assert "dj.request" in timer_tags
    assert timer_tags["dj.request"]["route"] == "/nonexistent"
