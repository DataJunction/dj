"""
Tests for ``datajunction_server.instrumentation.events``.
"""

import pytest

from datajunction_server.instrumentation import events


@pytest.fixture(autouse=True)
def reset_publisher_and_identity():
    """Restore the default no-op publisher and clear identity after every test."""
    original_publish = events._publish  # pylint: disable=protected-access
    yield
    events.set_publisher(original_publish)
    # Clear any identity a test left set so it can't bleed into the next test.
    events._identity.set(None)  # pylint: disable=protected-access
    # Reset the rate-limit clock so a test that opened the window can't suppress
    # the warning in a later test (the global isn't otherwise reset).
    events._last_failure_log = 0.0  # pylint: disable=protected-access


def test_emit_is_noop_by_default():
    """Default publisher is a no-op; emit must not raise."""
    events.emit("anything", foo="bar")


def test_set_publisher_receives_event():
    captured: list[dict] = []
    events.set_publisher(captured.append)

    events.emit("query_requested", metrics=["m1"], cube_hit=True)

    assert len(captured) == 1
    event = captured[0]
    assert event["event_type"] == "query_requested"
    assert event["metrics"] == ["m1"]
    assert event["cube_hit"] is True
    assert isinstance(event["ts_ms"], int)
    assert event["ts_ms"] > 0


def test_emit_merges_identity():
    captured: list[dict] = []
    events.set_publisher(captured.append)

    token = events.set_identity({"caller": "alice@nflx", "client": "ui"})
    try:
        events.emit("request", path="/sql")
    finally:
        events.reset_identity(token)

    event = captured[0]
    assert event["caller"] == "alice@nflx"
    assert event["client"] == "ui"
    assert event["path"] == "/sql"
    assert event["event_type"] == "request"


def test_emit_without_identity_has_no_caller():
    captured: list[dict] = []
    events.set_publisher(captured.append)

    events.emit("request", path="/sql")

    event = captured[0]
    assert "caller" not in event
    assert event["path"] == "/sql"


def test_identity_reset_clears_subsequent_emits():
    captured: list[dict] = []
    events.set_publisher(captured.append)

    token = events.set_identity({"caller": "bob@nflx"})
    events.emit("request", path="/a")
    events.reset_identity(token)
    events.emit("request", path="/b")

    assert captured[0]["caller"] == "bob@nflx"
    assert "caller" not in captured[1]


def test_field_overrides_identity_collision():
    """Explicit fields take precedence over identity keys."""
    captured: list[dict] = []
    events.set_publisher(captured.append)

    token = events.set_identity({"caller": "alice@nflx", "client": "ui"})
    try:
        events.emit("request", client="mcp")  # override
    finally:
        events.reset_identity(token)

    assert captured[0]["caller"] == "alice@nflx"
    assert captured[0]["client"] == "mcp"


def test_system_keys_win_over_fields_and_identity():
    """``event_type``/``ts_ms`` are system-owned and cannot be clobbered.

    ``event_type`` is positional so it can't even reach ``**fields`` — Python
    rejects it. The reachable clobber vectors are an identity dict carrying
    ``event_type``/``ts_ms`` and a ``ts_ms`` field; the system values win in
    both cases.
    """
    captured: list[dict] = []
    events.set_publisher(captured.append)

    token = events.set_identity({"event_type": "from_identity", "ts_ms": -1})
    try:
        events.emit("query_requested", ts_ms=0)
    finally:
        events.reset_identity(token)

    event = captured[0]
    assert event["event_type"] == "query_requested"
    # ts_ms is the canonical generated timestamp, not the field/identity value.
    assert event["ts_ms"] > 0


def test_emit_swallows_publisher_exceptions():
    def boom(event):
        raise RuntimeError("publisher broke")

    events.set_publisher(boom)

    # Must not raise
    events.emit("request", path="/sql")


def test_publish_failure_increments_counter(monkeypatch):
    """A failing publisher bumps ``dj.events.publish_failed`` and never raises."""
    calls: list[tuple[str, dict]] = []

    class _FakeCounter:
        def counter(self, name, tags=None):
            calls.append((name, tags or {}))

    monkeypatch.setattr(
        "datajunction_server.instrumentation.provider.get_metrics_provider",
        lambda: _FakeCounter(),
    )

    def boom(event):
        raise RuntimeError("publisher broke")

    events.set_publisher(boom)
    events.emit("query_requested", metrics=["m1"])

    assert ("dj.events.publish_failed", {"event_type": "query_requested"}) in calls


def test_counter_still_fires_when_failure_logger_raises(monkeypatch):
    """If the publisher raises AND the failure-path logger raises, emit() must
    not propagate AND the drop counter must still fire. Regression test: the
    counter and the warning are recorded under independent guards, so a broken
    logging handler cannot suppress the counter (they previously shared one
    ``try`` and a raising warning skipped the counter entirely)."""
    calls: list[tuple[str, dict]] = []

    class _FakeCounter:
        def counter(self, name, tags=None):
            calls.append((name, tags or {}))

    monkeypatch.setattr(
        "datajunction_server.instrumentation.provider.get_metrics_provider",
        lambda: _FakeCounter(),
    )

    def boom(event):
        raise RuntimeError("publisher broke")

    def boom_warning(*args, **kwargs):
        raise RuntimeError("logging handler broke")

    events.set_publisher(boom)
    monkeypatch.setattr(events._logger, "warning", boom_warning)
    events._last_failure_log = 0.0  # force the rate-limit window open

    # Must not raise even though both the publisher and the logger blow up...
    events.emit("query_requested", metrics=["m1"])

    # ...and the counter still fired despite the warning handler raising.
    assert ("dj.events.publish_failed", {"event_type": "query_requested"}) in calls


def test_emit_never_raises_when_identity_is_not_a_mapping(monkeypatch):
    """A deployment that misuses ``set_identity`` with a non-mapping must not
    make emit() raise into application code: event construction (``{**identity}``,
    which would raise ``TypeError``) happens inside the guard, and the drop is
    counted."""
    calls: list[tuple[str, dict]] = []

    class _FakeCounter:
        def counter(self, name, tags=None):
            calls.append((name, tags or {}))

    monkeypatch.setattr(
        "datajunction_server.instrumentation.provider.get_metrics_provider",
        lambda: _FakeCounter(),
    )
    captured: list[dict] = []
    events.set_publisher(captured.append)

    # A non-mapping identity (a deployment bug): ``{**identity}`` raises TypeError.
    events._identity.set(object())  # type: ignore[arg-type]

    events.emit("query_requested", metrics=["m1"])  # must not raise

    # Nothing was published (construction failed) but the drop was counted.
    assert captured == []
    assert ("dj.events.publish_failed", {"event_type": "query_requested"}) in calls


def test_async_publisher_is_detected_not_silently_dropped(monkeypatch):
    """An async publisher returns an un-awaited coroutine; emit() detects it and
    counts a failure instead of dropping the event silently."""
    calls: list[tuple[str, dict]] = []

    class _FakeCounter:
        def counter(self, name, tags=None):
            calls.append((name, tags or {}))

    monkeypatch.setattr(
        "datajunction_server.instrumentation.provider.get_metrics_provider",
        lambda: _FakeCounter(),
    )

    async def async_publish(event):  # pragma: no cover - never awaited
        pass

    events.set_publisher(async_publish)
    events.emit("query_requested", metrics=["m1"])  # must not raise

    assert ("dj.events.publish_failed", {"event_type": "query_requested"}) in calls


def test_event_type_in_fields_dict_merges_harmlessly():
    """``event_type`` is positional-only, so a fields dict carrying an
    ``event_type`` key merges in (and the canonical value wins) instead of
    raising TypeError."""
    captured: list[dict] = []
    events.set_publisher(captured.append)

    events.emit("query_requested", **{"event_type": "spoofed", "ts_ms": 0})

    event = captured[0]
    assert event["event_type"] == "query_requested"
    assert event["ts_ms"] > 0


def test_publish_failure_warning_is_rate_limited_but_counter_always_fires(monkeypatch):
    """The failure WARNING is rate-limited (one per ``_FAILURE_LOG_INTERVAL_S``),
    but the ``dj.events.publish_failed`` counter fires on *every* drop — so a
    second failure inside the window skips the log yet still bumps the counter."""
    calls: list[tuple[str, dict]] = []
    warnings: list = []

    class _FakeCounter:
        def counter(self, name, tags=None):
            calls.append((name, tags or {}))

    monkeypatch.setattr(
        "datajunction_server.instrumentation.provider.get_metrics_provider",
        lambda: _FakeCounter(),
    )
    monkeypatch.setattr(events._logger, "warning", lambda *a, **k: warnings.append(a))

    def boom(event):
        raise RuntimeError("publisher broke")

    events.set_publisher(boom)
    events.emit("query_requested", metrics=["m1"])  # opens the rate-limit window
    events.emit("query_requested", metrics=["m2"])  # inside window: no warning

    # The counter fires on both drops...
    assert (
        calls.count(("dj.events.publish_failed", {"event_type": "query_requested"}))
        == 2
    )
    # ...but the WARNING is logged only once (second drop is rate-limited).
    assert len(warnings) == 1


def test_publish_failure_counter_failure_is_swallowed(monkeypatch):
    """If even the failure-counter path raises, emit() still must not raise."""

    def broken_provider():
        raise RuntimeError("provider unavailable")

    monkeypatch.setattr(
        "datajunction_server.instrumentation.provider.get_metrics_provider",
        broken_provider,
    )

    def boom(event):
        raise RuntimeError("publisher broke")

    events.set_publisher(boom)
    # Must not raise even though both the publisher and the counter fail.
    events.emit("request", path="/sql")
