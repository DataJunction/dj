"""Configurable query client implementations"""

from datajunction_server.query_clients.base import BaseQueryServiceClient
from datajunction_server.query_clients.http import HttpQueryServiceClient

__all__ = [
    "BaseQueryServiceClient",
    "HttpQueryServiceClient",
    "SnowflakeClient",
]


def __getattr__(name):
    """Lazy import for optional clients to avoid import errors."""
    if name == "SnowflakeClient":
        from datajunction_server.query_clients.snowflake import SnowflakeClient

        return SnowflakeClient
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
