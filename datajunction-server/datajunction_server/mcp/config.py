"""
Configuration for DataJunction MCP Server
"""
import os
from typing import Optional
from pydantic_settings import BaseSettings


class MCPSettings(BaseSettings):
    """Settings for MCP server"""

    # DJ API connection
    dj_api_url: str = os.getenv("DJ_API_URL", "http://localhost:8000")
    dj_api_token: Optional[str] = os.getenv("DJ_API_TOKEN")

    # Optional: Basic auth (if not using token)
    dj_username: Optional[str] = os.getenv("DJ_USERNAME")
    dj_password: Optional[str] = os.getenv("DJ_PASSWORD")

    # Request timeout
    request_timeout: int = 30

    class Config:
        env_prefix = "DJ_MCP_"


def get_mcp_settings() -> MCPSettings:
    """Get MCP settings singleton"""
    return MCPSettings()
