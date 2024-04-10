"""
Tests for the custom API routers.
"""
import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_api_router_trailing_slashes(
    client_with_basic: AsyncClient,
) -> None:
    """
    Test that the API router used by our endpoints will send both routes without
    trailing slashes and those with trailing slashes to right place.
    """
    response = await client_with_basic.get("/attributes/")
    assert response.status_code in (200, 201)
    assert len(response.json()) > 0

    response = await client_with_basic.get("/attributes")
    assert response.status_code in (200, 201)
    assert len(response.json()) > 0

    response = await client_with_basic.get("/namespaces/")
    assert response.status_code in (200, 201)
    assert len(response.json()) > 0

    response = await client_with_basic.get("/namespaces")
    assert response.status_code in (200, 201)
    assert len(response.json()) > 0

    response = await client_with_basic.get("/namespaces/basic?type_=source")
    assert response.status_code in (200, 201)
    assert len(response.json()) > 0
