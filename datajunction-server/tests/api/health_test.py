"""
Tests for the healthcheck API.
"""
import asyncio

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
async def test_successful_health(module__client: AsyncClient) -> None:
    """
    Test ``GET /health/``.
    """
    response = await module__client.get("/health/")
    data = response.json()
    assert data == [{"name": "database", "status": "ok"}]


@pytest.mark.asyncio
async def test_failed_health(
    session: AsyncSession,
    client: AsyncClient,
    mocker,
) -> None:
    """
    Test failed healthcheck.
    """
    future: asyncio.Future = asyncio.Future()
    future.set_result(mocker.MagicMock())
    session.execute = mocker.MagicMock(return_value=future)
    response = await client.get("/health/")
    data = response.json()
    assert data == [{"name": "database", "status": "failed"}]
