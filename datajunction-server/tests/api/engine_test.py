"""
Tests for the engine API.
"""
import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_engine_adding_a_new_engine(
    client: AsyncClient,
) -> None:
    """
    Test adding an engine
    """
    response = await client.post(
        "/engines/",
        json={
            "name": "spark",
            "version": "3.3.1",
            "dialect": "spark",
        },
    )
    data = response.json()
    assert response.status_code == 201
    assert data == {
        "dialect": "spark",
        "name": "spark",
        "uri": None,
        "version": "3.3.1",
    }


@pytest.mark.asyncio
async def test_engine_list(
    client: AsyncClient,
) -> None:
    """
    Test listing engines
    """
    response = await client.post(
        "/engines/",
        json={
            "name": "spark",
            "version": "2.4.4",
            "dialect": "spark",
        },
    )
    assert response.status_code == 201

    response = await client.post(
        "/engines/",
        json={
            "name": "spark",
            "version": "3.3.0",
            "dialect": "spark",
        },
    )
    assert response.status_code == 201

    response = await client.post(
        "/engines/",
        json={
            "name": "spark",
            "version": "3.3.1",
            "dialect": "spark",
        },
    )
    assert response.status_code == 201

    response = await client.get("/engines/")
    assert response.status_code == 200
    data = response.json()
    assert data == [
        {
            "name": "spark",
            "uri": None,
            "version": "2.4.4",
            "dialect": "spark",
        },
        {
            "name": "spark",
            "uri": None,
            "version": "3.3.0",
            "dialect": "spark",
        },
        {
            "name": "spark",
            "uri": None,
            "version": "3.3.1",
            "dialect": "spark",
        },
    ]


@pytest.mark.asyncio
async def test_engine_get_engine(
    client: AsyncClient,
) -> None:
    """
    Test getting an engine
    """
    response = await client.post(
        "/engines/",
        json={
            "name": "spark",
            "version": "3.3.1",
            "dialect": "spark",
        },
    )
    assert response.status_code == 201

    response = await client.get(
        "/engines/spark/3.3.1",
    )
    assert response.status_code == 200
    data = response.json()
    assert data == {
        "name": "spark",
        "uri": None,
        "version": "3.3.1",
        "dialect": "spark",
    }


@pytest.mark.asyncio
async def test_engine_raise_on_engine_already_exists(
    client: AsyncClient,
) -> None:
    """
    Test raise on engine already exists
    """
    response = await client.post(
        "/engines/",
        json={
            "name": "spark",
            "version": "3.3.1",
            "dialect": "spark",
        },
    )
    assert response.status_code == 201

    response = await client.post(
        "/engines/",
        json={
            "name": "spark",
            "version": "3.3.1",
            "dialect": "spark",
        },
    )
    assert response.status_code == 409
    data = response.json()
    assert data == {"detail": "Engine already exists: `spark` version `3.3.1`"}
