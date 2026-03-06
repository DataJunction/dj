"""
Generic metrics provider abstraction.

The OSS server ships with a no-op provider so that every instrumentation call
is a safe pass-through with no external dependencies.  Internal builds replace
the provider at startup via ``set_metrics_provider``.

Usage (internal server startup)::

    from datajunction_server.instrumentation.provider import set_metrics_provider
    set_metrics_provider(NflxMetricsProvider())

Usage (emit a metric anywhere in the codebase)::

    from datajunction_server.instrumentation.provider import get_metrics_provider
    get_metrics_provider().counter("dj.cache.hit", tags={"query_type": "METRICS"})
"""

from abc import ABC, abstractmethod
from typing import Any


class MetricsProvider(ABC):
    """Abstract metrics provider. Override in environments with a metrics backend."""

    @abstractmethod
    def counter(
        self,
        name: str,
        value: int = 1,
        tags: dict[str, Any] | None = None,
    ) -> None:
        """Increment a counter metric."""

    @abstractmethod
    def gauge(
        self,
        name: str,
        value: float,
        tags: dict[str, Any] | None = None,
    ) -> None:
        """Record a point-in-time gauge value."""

    @abstractmethod
    def timer(
        self,
        name: str,
        value_ms: float,
        tags: dict[str, Any] | None = None,
    ) -> None:
        """Record a latency or count value. Use for p50/p95/p99 histograms."""


class NoOpMetricsProvider(MetricsProvider):
    """Default no-op provider. All operations are harmless pass-throughs."""

    def counter(
        self,
        name: str,
        value: int = 1,
        tags: dict[str, Any] | None = None,
    ) -> None:
        pass

    def gauge(
        self,
        name: str,
        value: float,
        tags: dict[str, Any] | None = None,
    ) -> None:
        pass

    def timer(
        self,
        name: str,
        value_ms: float,
        tags: dict[str, Any] | None = None,
    ) -> None:
        pass


_provider: MetricsProvider = NoOpMetricsProvider()


def get_metrics_provider() -> MetricsProvider:
    """Return the current global metrics provider."""
    return _provider


def set_metrics_provider(provider: MetricsProvider) -> None:
    """
    Set the global metrics provider.

    Call once at application startup, before the first request is handled.
    """
    global _provider
    _provider = provider
