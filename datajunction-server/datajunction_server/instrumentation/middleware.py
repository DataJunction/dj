"""
Instrumentation middleware for the DJ server.

Emits on every HTTP request:
  - dj.db.pool.size / checked_out / available / overflow  (gauges)
  - dj.request.in_flight  (gauge — concurrent requests in progress)
  - dj.request  (timer in ms, tagged with route + method + status_code)

Implemented as a *pure ASGI* middleware (not a Starlette ``BaseHTTPMiddleware``
subclass). ``BaseHTTPMiddleware`` runs ``call_next`` on a separate asyncio
task, which on Python 3.12+ no longer copies the greenlet context that
SQLAlchemy's async drivers need. Lazy-loaded ORM relationships then explode
with ``MissingGreenlet`` while the response is being assembled. A pure ASGI
middleware runs in the same task as the app, so the greenlet binding flows
through cleanly.
"""

import logging
import time

from datajunction_server.instrumentation.provider import get_metrics_provider

_in_flight: int = 0

logger = logging.getLogger(__name__)


class DJInstrumentationMiddleware:
    """Emit DB pool gauges and per-request timing on every HTTP request."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        global _in_flight
        provider = get_metrics_provider()
        _emit_pool_gauges(provider)

        _in_flight += 1
        provider.gauge("dj.request.in_flight", _in_flight)

        status_code = 500

        async def send_wrapper(message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            await send(message)

        start = time.monotonic()
        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            _in_flight -= 1
            provider.gauge("dj.request.in_flight", _in_flight)
            elapsed_ms = (time.monotonic() - start) * 1000
            # ``scope["route"]`` is populated by Starlette's router when a
            # route matches; for unmatched paths (e.g. 404) we fall back to
            # the raw URL path from the scope.
            route = scope.get("route")
            route_path = route.path if route else scope.get("path", "")
            provider.timer(
                "dj.request",
                elapsed_ms,
                {
                    "route": route_path,
                    "method": scope.get("method", ""),
                    "status_code": str(status_code),
                },
            )


def _emit_pool_gauges(provider) -> None:
    """Emit DB connection pool saturation metrics. Silently skips on error."""
    # Inline import to avoid a circular dependency: utils → database → models → utils.
    from datajunction_server.utils import get_session_manager  # noqa: PLC0415

    try:
        pool = get_session_manager().writer_engine.pool
        pool_size = pool.size()
        checked_out = pool.checkedout()
        overflow = pool.overflow()
        provider.gauge("dj.db.pool.size", pool_size)
        provider.gauge("dj.db.pool.checked_out", checked_out)
        provider.gauge("dj.db.pool.available", pool_size - checked_out)
        provider.gauge("dj.db.pool.overflow", overflow)
    except Exception:  # pylint: disable=broad-except
        logger.debug("Could not emit pool gauges", exc_info=True)
