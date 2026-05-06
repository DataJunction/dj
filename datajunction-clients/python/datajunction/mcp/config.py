"""
Configuration for the DataJunction MCP stdio→HTTP proxy CLI.

The CLI connects to a hosted DJ ``/mcp`` endpoint over Streamable HTTP and
re-exposes its tools to local stdio MCP clients (e.g. Claude Desktop).

Settings are read from env on every ``get_mcp_settings()`` call (no caching),
so override behavior is predictable in tests and shell-set values are picked
up at startup.

Resolution order for the upstream MCP URL:

1. ``DJ_MCP_URL`` if set — full URL of the ``/mcp`` endpoint.
2. ``DJ_API_URL`` if set — backwards-compatible fallback for users of the
   pre-migration ``dj-mcp`` CLI. We append ``/mcp`` to it.
3. ``http://localhost:8000/mcp`` — local-dev default.
"""

from __future__ import annotations

import os
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _resolve_default_mcp_url() -> str:
    """Fall back to the legacy ``DJ_API_URL`` env var when ``DJ_MCP_URL``
    is unset.

    Pre-migration deployments configured the stdio CLI with ``DJ_API_URL``
    (the base DJ API URL); the new CLI talks directly to ``/mcp``. To
    avoid silently breaking those setups we read ``DJ_API_URL`` and
    append the path.
    """
    explicit = os.environ.get("DJ_MCP_URL")
    if explicit:
        return explicit
    legacy = os.environ.get("DJ_API_URL")
    if legacy:
        return legacy.rstrip("/") + "/mcp"
    return "http://localhost:8000/mcp"


class MCPSettings(BaseSettings):
    """Settings for the dj-mcp stdio CLI."""

    # Hosted DJ MCP endpoint. Override with ``DJ_MCP_URL``; ``DJ_API_URL``
    # is accepted as a backwards-compatible fallback (see module docstring).
    mcp_url: str = Field(
        default_factory=_resolve_default_mcp_url,
        alias="DJ_MCP_URL",
    )

    # Bearer token forwarded as Authorization on every upstream request.
    # Use ``DJ_API_TOKEN`` to align with the rest of the DJ tooling.
    dj_api_token: Optional[str] = Field(default=None, alias="DJ_API_TOKEN")

    # Request timeout for upstream calls.
    request_timeout: float = 30.0

    model_config = SettingsConfigDict(
        case_sensitive=False,
        populate_by_name=True,
    )


def get_mcp_settings() -> MCPSettings:
    """Return MCP settings. Re-reads env on every call."""
    return MCPSettings()
