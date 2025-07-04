"""
Tests for whoami router
"""

import pytest
from httpx import AsyncClient

from datajunction_server.internal.access.authentication.tokens import decode_token


@pytest.mark.asyncio
async def test_whoami(module__client: AsyncClient):
    """
    Test /whoami endpoint
    """
    response = await module__client.get("/whoami/")
    assert response.status_code in (200, 201)
    assert response.json()["username"] == "dj"


@pytest.mark.asyncio
async def test_short_lived_token(module__client: AsyncClient):
    """
    Test getting a short-lived token from the /token endpoint
    """
    response = await module__client.get("/token/")
    assert response.status_code in (200, 201)
    data = response.json()
    user = decode_token(data["token"])
    assert user["username"] == "dj"
