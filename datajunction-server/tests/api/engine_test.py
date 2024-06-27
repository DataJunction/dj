"""
Tests for the engine API.
"""
import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_engine_adding_a_new_engine(
    module__client: AsyncClient,
) -> None:
    """
    Test adding an engine
    """
    response = await module__client.post(
        "/engines/",
        json={
            "name": "spark-one",
            "version": "3.3.1",
            "dialect": "spark",
        },
    )
    data = response.json()
    assert response.status_code == 201
    assert data == {
        "dialect": "spark",
        "name": "spark-one",
        "uri": None,
        "version": "3.3.1",
    }


@pytest.mark.asyncio
async def test_engine_list(
    module__client: AsyncClient,
) -> None:
    """
    Test listing engines
    """
    response = await module__client.post(
        "/engines/",
        json={
            "name": "spark-foo",
            "version": "2.4.4",
            "dialect": "spark",
        },
    )
    assert response.status_code == 201

    response = await module__client.post(
        "/engines/",
        json={
            "name": "spark-foo",
            "version": "3.3.0",
            "dialect": "spark",
        },
    )
    assert response.status_code == 201

    response = await module__client.post(
        "/engines/",
        json={
            "name": "spark-foo",
            "version": "3.3.1",
            "dialect": "spark",
        },
    )
    assert response.status_code == 201

    response = await module__client.get("/engines/")
    assert response.status_code == 200
    data = [engine for engine in response.json() if engine["name"] == "spark-foo"]
    assert data == [
        {
            "name": "spark-foo",
            "uri": None,
            "version": "2.4.4",
            "dialect": "spark",
        },
        {
            "name": "spark-foo",
            "uri": None,
            "version": "3.3.0",
            "dialect": "spark",
        },
        {
            "name": "spark-foo",
            "uri": None,
            "version": "3.3.1",
            "dialect": "spark",
        },
    ]


@pytest.mark.asyncio
async def test_engine_get_engine(
    module__client: AsyncClient,
) -> None:
    """
    Test getting an engine
    """
    response = await module__client.post(
        "/engines/",
        json={
            "name": "spark-two",
            "version": "3.3.1",
            "dialect": "spark",
        },
    )
    assert response.status_code == 201

    response = await module__client.get(
        "/engines/spark-two/3.3.1",
    )
    assert response.status_code == 200
    data = response.json()
    assert data == {
        "name": "spark-two",
        "uri": None,
        "version": "3.3.1",
        "dialect": "spark",
    }


@pytest.mark.asyncio
async def test_engine_raise_on_engine_already_exists(
    module__client: AsyncClient,
) -> None:
    """
    Test raise on engine already exists
    """
    response = await module__client.post(
        "/engines/",
        json={
            "name": "spark-three",
            "version": "3.3.1",
            "dialect": "spark",
        },
    )
    assert response.status_code == 201

    response = await module__client.post(
        "/engines/",
        json={
            "name": "spark-three",
            "version": "3.3.1",
            "dialect": "spark",
        },
    )
    assert response.status_code == 409
    data = response.json()
    assert data == {"detail": "Engine already exists: `spark-three` version `3.3.1`"}
