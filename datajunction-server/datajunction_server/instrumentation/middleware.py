"""
Instrumentation middleware for the DJ server.

Emits on every HTTP request:
  - dj.db.pool.size / checked_out / available / overflow  (gauges)
  - dj.request  (timer in ms, tagged with route + method + status_code)
"""

import logging
import time

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from datajunction_server.instrumentation.provider import get_metrics_provider

logger = logging.getLogger(__name__)


class DJInstrumentationMiddleware(BaseHTTPMiddleware):
    """Emit DB pool gauges and per-request timing on every HTTP request."""

    async def dispatch(self, request: Request, call_next) -> Response:
        provider = get_metrics_provider()
        _emit_pool_gauges(provider)

        start = time.monotonic()
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            elapsed_ms = (time.monotonic() - start) * 1000
            route = request.scope.get("route")
            route_path = route.path if route else request.url.path
            provider.timer(
                "dj.request",
                elapsed_ms,
                {
                    "route": route_path,
                    "method": request.method,
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
