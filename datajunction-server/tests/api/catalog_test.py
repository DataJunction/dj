"""
Tests for the catalog API.
"""
import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_catalog_adding_a_new_catalog(
    client: AsyncClient,
) -> None:
    """
    Test adding a catalog
    """
    response = await client.post(
        "/catalogs/",
        json={
            "name": "dev",
        },
    )
    data = response.json()
    assert response.status_code == 201
    assert data == {"name": "dev", "engines": []}


@pytest.mark.asyncio
async def test_catalog_list(
    client: AsyncClient,
) -> None:
    """
    Test listing catalogs
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
        "/catalogs/",
        json={
            "name": "dev",
            "engines": [
                {
                    "name": "spark",
                    "version": "3.3.1",
                    "dialect": "spark",
                },
            ],
        },
    )
    assert response.status_code == 201

    response = await client.post(
        "/catalogs/",
        json={
            "name": "test",
        },
    )
    assert response.status_code == 201

    response = await client.post(
        "/catalogs/",
        json={
            "name": "prod",
        },
    )
    assert response.status_code == 201

    response = await client.get("/catalogs/")
    assert response.status_code == 200
    assert sorted(response.json(), key=lambda v: v["name"]) == sorted(
        [
            {"name": "unknown", "engines": []},
            {
                "name": "dev",
                "engines": [
                    {
                        "name": "spark",
                        "version": "3.3.1",
                        "uri": None,
                        "dialect": "spark",
                    },
                ],
            },
            {"name": "test", "engines": []},
            {"name": "prod", "engines": []},
        ],
        key=lambda v: v["name"],  # type: ignore
    )


@pytest.mark.asyncio
async def test_catalog_get_catalog(
    client: AsyncClient,
) -> None:
    """
    Test getting a catalog
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
        "/catalogs/",
        json={
            "name": "dev",
            "engines": [
                {
                    "name": "spark",
                    "version": "3.3.1",
                    "dialect": "spark",
                },
            ],
        },
    )
    assert response.status_code == 201

    response = await client.get(
        "/catalogs/dev",
    )
    assert response.status_code == 200
    data = response.json()
    assert data == {
        "name": "dev",
        "engines": [
            {"name": "spark", "uri": None, "version": "3.3.1", "dialect": "spark"},
        ],
    }


@pytest.mark.asyncio
async def test_catalog_adding_a_new_catalog_with_engines(
    client: AsyncClient,
) -> None:
    """
    Test adding a catalog with engines
    """
    response = await client.post(
        "/engines/",
        json={
            "name": "spark",
            "uri": None,
            "version": "3.3.1",
            "dialect": "spark",
        },
    )
    data = response.json()
    assert response.status_code == 201

    response = await client.post(
        "/catalogs/",
        json={
            "name": "dev",
            "engines": [
                {
                    "name": "spark",
                    "version": "3.3.1",
                    "dialect": "spark",
                },
            ],
        },
    )
    data = response.json()
    assert response.status_code == 201
    assert data == {
        "name": "dev",
        "engines": [
            {
                "name": "spark",
                "uri": None,
                "version": "3.3.1",
                "dialect": "spark",
            },
        ],
    }


@pytest.mark.asyncio
async def test_catalog_adding_a_new_catalog_then_attaching_engines(
    client: AsyncClient,
) -> None:
    """
    Test adding a catalog then attaching a catalog
    """
    response = await client.post(
        "/engines/",
        json={
            "name": "spark",
            "uri": None,
            "version": "3.3.1",
            "dialect": "spark",
        },
    )
    data = response.json()
    assert response.status_code == 201

    response = await client.post(
        "/catalogs/",
        json={
            "name": "dev",
        },
    )
    assert response.status_code == 201

    await client.post(
        "/catalogs/dev/engines/",
        json=[
            {
                "name": "spark",
                "version": "3.3.1",
                "dialect": "spark",
            },
        ],
    )

    response = await client.get("/catalogs/dev/")
    data = response.json()
    assert data == {
        "name": "dev",
        "engines": [
            {
                "name": "spark",
                "uri": None,
                "version": "3.3.1",
                "dialect": "spark",
            },
        ],
    }


@pytest.mark.asyncio
async def test_catalog_adding_without_duplicating(
    client: AsyncClient,
) -> None:
    """
    Test adding a catalog and having existing catalogs not re-added
    """
    response = await client.post(
        "/engines/",
        json={
            "name": "spark",
            "uri": None,
            "version": "2.4.4",
            "dialect": "spark",
        },
    )
    data = response.json()
    assert response.status_code == 201

    response = await client.post(
        "/engines/",
        json={
            "name": "spark",
            "version": "3.3.0",
            "dialect": "spark",
        },
    )
    data = response.json()
    assert response.status_code == 201

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

    response = await client.post(
        "/catalogs/",
        json={
            "name": "dev",
            "dialect": "spark",
        },
    )
    assert response.status_code == 201

    response = await client.post(
        "/catalogs/dev/engines/",
        json=[
            {
                "name": "spark",
                "version": "2.4.4",
                "dialect": "spark",
            },
            {
                "name": "spark",
                "version": "3.3.0",
                "dialect": "spark",
            },
            {
                "name": "spark",
                "version": "3.3.1",
                "dialect": "spark",
            },
        ],
    )
    assert response.status_code == 201

    response = await client.post(
        "/catalogs/dev/engines/",
        json=[
            {
                "name": "spark",
                "version": "2.4.4",
                "dialect": "spark",
            },
            {
                "name": "spark",
                "version": "3.3.0",
                "dialect": "spark",
            },
            {
                "name": "spark",
                "version": "3.3.1",
                "dialect": "spark",
            },
        ],
    )
    assert response.status_code == 201
    data = response.json()
    assert data == {
        "name": "dev",
        "engines": [
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
        ],
    }


@pytest.mark.asyncio
async def test_catalog_raise_on_adding_a_new_catalog_with_nonexistent_engines(
    client: AsyncClient,
) -> None:
    """
    Test raising an error when adding a catalog with engines that do not exist
    """
    response = await client.post(
        "/catalogs/",
        json={
            "name": "dev",
            "engines": [
                {
                    "name": "spark",
                    "version": "4.0.0",
                    "dialect": "spark",
                },
            ],
        },
    )
    data = response.json()
    assert response.status_code == 404
    assert data == {"detail": "Engine not found: `spark` version `4.0.0`"}


@pytest.mark.asyncio
async def test_catalog_raise_on_catalog_already_exists(
    client: AsyncClient,
) -> None:
    """
    Test raise on catalog already exists
    """
    response = await client.post(
        "/catalogs/",
        json={
            "name": "dev",
        },
    )
    assert response.status_code == 201

    response = await client.post(
        "/catalogs/",
        json={
            "name": "dev",
        },
    )
    data = response.json()
    assert response.status_code == 409
    assert data == {"detail": "Catalog already exists: `dev`"}
