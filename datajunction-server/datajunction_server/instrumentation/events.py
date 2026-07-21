"""
Structured event emission.

Application code calls ``events.emit(event_type, **fields)`` from anywhere. It
is a no-op by default; a deployment installs a real publisher at startup via
``set_publisher(fn)``. Events are enriched with the current request identity
(caller/client/trace_id) when a deployment populates it via ``set_identity`` —
this package never sets identity itself, so without a deployment it stays empty.
"""

import inspect
import logging
import time
from contextvars import ContextVar
from typing import Any, Callable

_logger = logging.getLogger(__name__)

# The failure warning is rate-limited so a broken publisher can't flood logs;
# the counter fires on every failure regardless.
_FAILURE_LOG_INTERVAL_S = 60.0
_last_failure_log = 0.0

_identity: ContextVar[dict[str, Any] | None] = ContextVar(
    "dj_identity",
    default=None,
)


def set_identity(identity: dict[str, Any]):
    """Set per-request identity (caller, client, trace_id, ...). Returns a token."""
    return _identity.set(identity)


def reset_identity(token) -> None:
    """Reset the identity ContextVar using the token returned by ``set_identity``."""
    _identity.reset(token)


def _noop_publish(event: dict[str, Any]) -> None:
    """Default no-op publisher. Replaced via ``set_publisher`` at startup."""


# ``Any`` (not ``None``): emit() inspects the return value to catch a
# mis-installed async publisher returning an un-awaited coroutine.
_publish: Callable[[dict[str, Any]], Any] = _noop_publish


def set_publisher(fn: Callable[[dict[str, Any]], None]) -> None:
    """Install the publisher (once, at startup). ``fn`` must be synchronous."""
    global _publish
    _publish = fn


def emit(event_type: str, /, **fields: Any) -> None:
    """Emit a structured event, carrying the current identity context.

    Precedence: identity < explicit ``fields`` < system keys ``event_type`` /
    ``ts_ms`` (which can't be clobbered). ``event_type`` is positional-only so a
    ``fields`` dict carrying that key merges in rather than raising ``TypeError``.
    """
    try:
        # Built inside the guard so a bad set_identity() value (non-mapping)
        # can't make {**identity} raise into application code.
        identity = _identity.get() or {}
        event = {
            **identity,
            **fields,
            "event_type": event_type,
            "ts_ms": int(time.time() * 1000),
        }
        result = _publish(event)
        if inspect.iscoroutine(result):  # async publisher: never awaited, so dropped
            result.close()
            _note_publish_failure(event_type)
    except Exception:  # pylint: disable=broad-except
        _note_publish_failure(event_type)


def _note_publish_failure(event_type: str) -> None:
    """Record a dropped event without ever raising.

    Counter and warning are guarded independently so a broken logging handler
    can't suppress the counter (the drop signal, so it goes first).
    """
    global _last_failure_log
    try:
        # Lazy import avoids a cycle and keeps emit()'s success path cheap.
        from datajunction_server.instrumentation.provider import (  # pylint: disable=import-outside-toplevel
            get_metrics_provider,
        )

        get_metrics_provider().counter(
            "dj.events.publish_failed",
            tags={"event_type": event_type},
        )
    except Exception:  # pylint: disable=broad-except
        pass

    try:
        now = time.monotonic()
        if now - _last_failure_log >= _FAILURE_LOG_INTERVAL_S:
            _logger.warning(
                "event publish failed (event_type=%s); suppressing further "
                "warnings for %ds",
                event_type,
                int(_FAILURE_LOG_INTERVAL_S),
                exc_info=True,
            )
            # Stamp only on success, so a failed log doesn't open the window.
            _last_failure_log = now
    except Exception:  # pylint: disable=broad-except
        pass
