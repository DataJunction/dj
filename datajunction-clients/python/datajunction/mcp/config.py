"""
Configuration for the DataJunction MCP stdio→HTTP proxy CLI.

The CLI connects to a hosted DJ ``/mcp`` endpoint over Streamable HTTP and
re-exposes its tools to local stdio MCP clients (e.g. Claude Desktop).

Settings are read from env on every ``get_mcp_settings()`` call (no caching),
so override behaviour is predictable in tests and shell-set values picked
up at startup.
"""

from __future__ import annotations

from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MCPSettings(BaseSettings):
    """Settings for the dj-mcp stdio CLI."""

    # Hosted DJ MCP endpoint. Defaults to localhost for dev. Override with
    # ``DJ_MCP_URL`` for hosted deployments.
    mcp_url: str = Field(default="http://localhost:8000/mcp", alias="DJ_MCP_URL")

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
