"""
Tests for the catalog API.
"""
import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_catalog_adding_a_new_catalog(
    module__client: AsyncClient,
) -> None:
    """
    Test adding a catalog
    """
    response = await module__client.post(
        "/catalogs/",
        json={
            "name": "dev-1",
        },
    )
    data = response.json()
    assert response.status_code == 201
    assert data == {"name": "dev-1", "engines": []}


@pytest.mark.asyncio
async def test_catalog_list(
    module__client: AsyncClient,
) -> None:
    """
    Test listing catalogs
    """
    response = await module__client.post(
        "/engines/",
        json={
            "name": "spark",
            "version": "3.3.1",
            "dialect": "spark",
        },
    )
    assert response.status_code == 201

    response = await module__client.post(
        "/catalogs/",
        json={
            "name": "cat-dev",
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

    response = await module__client.post(
        "/catalogs/",
        json={
            "name": "cat-test",
        },
    )
    assert response.status_code == 201

    response = await module__client.post(
        "/catalogs/",
        json={
            "name": "cat-prod",
        },
    )
    assert response.status_code == 201

    response = await module__client.get("/catalogs/")
    assert response.status_code == 200
    filtered_response = [
        cat for cat in response.json() if cat["name"].startswith("cat-")
    ]
    assert sorted(filtered_response, key=lambda v: v["name"]) == sorted(
        [
            {
                "name": "cat-dev",
                "engines": [
                    {
                        "name": "spark",
                        "version": "3.3.1",
                        "uri": None,
                        "dialect": "spark",
                    },
                ],
            },
            {"name": "cat-test", "engines": []},
            {"name": "cat-prod", "engines": []},
        ],
        key=lambda v: v["name"],  # type: ignore
    )


@pytest.mark.asyncio
async def test_catalog_get_catalog(
    module__client: AsyncClient,
) -> None:
    """
    Test getting a catalog
    """
    response = await module__client.post(
        "/engines/",
        json={
            "name": "one-spark",
            "version": "3.3.1",
            "dialect": "spark",
        },
    )
    assert response.status_code == 201

    response = await module__client.post(
        "/catalogs/",
        json={
            "name": "one-dev",
            "engines": [
                {
                    "name": "one-spark",
                    "version": "3.3.1",
                    "dialect": "spark",
                },
            ],
        },
    )
    assert response.status_code == 201

    response = await module__client.get(
        "/catalogs/one-dev",
    )
    assert response.status_code == 200
    data = response.json()
    assert data == {
        "name": "one-dev",
        "engines": [
            {"name": "one-spark", "uri": None, "version": "3.3.1", "dialect": "spark"},
        ],
    }


@pytest.mark.asyncio
async def test_catalog_adding_a_new_catalog_with_engines(
    module__client: AsyncClient,
) -> None:
    """
    Test adding a catalog with engines
    """
    response = await module__client.post(
        "/engines/",
        json={
            "name": "two-spark",
            "uri": None,
            "version": "3.3.1",
            "dialect": "spark",
        },
    )
    data = response.json()
    assert response.status_code == 201

    response = await module__client.post(
        "/catalogs/",
        json={
            "name": "two-dev",
            "engines": [
                {
                    "name": "two-spark",
                    "version": "3.3.1",
                    "dialect": "spark",
                },
            ],
        },
    )
    data = response.json()
    assert response.status_code == 201
    assert data == {
        "name": "two-dev",
        "engines": [
            {
                "name": "two-spark",
                "uri": None,
                "version": "3.3.1",
                "dialect": "spark",
            },
        ],
    }


@pytest.mark.asyncio
async def test_catalog_adding_a_new_catalog_then_attaching_engines(
    module__client: AsyncClient,
) -> None:
    """
    Test adding a catalog then attaching a catalog
    """
    response = await module__client.post(
        "/engines/",
        json={
            "name": "spark-3",
            "uri": None,
            "version": "3.3.1",
            "dialect": "spark",
        },
    )
    data = response.json()
    assert response.status_code == 201

    response = await module__client.post(
        "/catalogs/",
        json={
            "name": "dev-3",
        },
    )
    assert response.status_code == 201

    await module__client.post(
        "/catalogs/dev-3/engines/",
        json=[
            {
                "name": "spark-3",
                "version": "3.3.1",
                "dialect": "spark",
            },
        ],
    )

    response = await module__client.get("/catalogs/dev-3/")
    data = response.json()
    assert data == {
        "name": "dev-3",
        "engines": [
            {
                "name": "spark-3",
                "uri": None,
                "version": "3.3.1",
                "dialect": "spark",
            },
        ],
    }


@pytest.mark.asyncio
async def test_catalog_adding_without_duplicating(
    module__client: AsyncClient,
) -> None:
    """
    Test adding a catalog and having existing catalogs not re-added
    """
    response = await module__client.post(
        "/engines/",
        json={
            "name": "spark-4",
            "uri": None,
            "version": "2.4.4",
            "dialect": "spark",
        },
    )
    data = response.json()
    assert response.status_code == 201

    response = await module__client.post(
        "/engines/",
        json={
            "name": "spark-4",
            "version": "3.3.0",
            "dialect": "spark",
        },
    )
    data = response.json()
    assert response.status_code == 201

    response = await module__client.post(
        "/engines/",
        json={
            "name": "spark-4",
            "version": "3.3.1",
            "dialect": "spark",
        },
    )
    data = response.json()
    assert response.status_code == 201

    response = await module__client.post(
        "/catalogs/",
        json={
            "name": "dev-4",
            "dialect": "spark",
        },
    )
    assert response.status_code == 201

    response = await module__client.post(
        "/catalogs/dev-4/engines/",
        json=[
            {
                "name": "spark-4",
                "version": "2.4.4",
                "dialect": "spark",
            },
            {
                "name": "spark-4",
                "version": "3.3.0",
                "dialect": "spark",
            },
            {
                "name": "spark-4",
                "version": "3.3.1",
                "dialect": "spark",
            },
        ],
    )
    assert response.status_code == 201

    response = await module__client.post(
        "/catalogs/dev-4/engines/",
        json=[
            {
                "name": "spark-4",
                "version": "2.4.4",
                "dialect": "spark",
            },
            {
                "name": "spark-4",
                "version": "3.3.0",
                "dialect": "spark",
            },
            {
                "name": "spark-4",
                "version": "3.3.1",
                "dialect": "spark",
            },
        ],
    )
    assert response.status_code == 201
    data = response.json()
    assert data == {
        "name": "dev-4",
        "engines": [
            {
                "name": "spark-4",
                "uri": None,
                "version": "2.4.4",
                "dialect": "spark",
            },
            {
                "name": "spark-4",
                "uri": None,
                "version": "3.3.0",
                "dialect": "spark",
            },
            {
                "name": "spark-4",
                "uri": None,
                "version": "3.3.1",
                "dialect": "spark",
            },
        ],
    }


@pytest.mark.asyncio
async def test_catalog_raise_on_adding_a_new_catalog_with_nonexistent_engines(
    module__client: AsyncClient,
) -> None:
    """
    Test raising an error when adding a catalog with engines that do not exist
    """
    response = await module__client.post(
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
    module__client: AsyncClient,
) -> None:
    """
    Test raise on catalog already exists
    """
    response = await module__client.post(
        "/catalogs/",
        json={
            "name": "dev",
        },
    )
    assert response.status_code == 201

    response = await module__client.post(
        "/catalogs/",
        json={
            "name": "dev",
        },
    )
    data = response.json()
    assert response.status_code == 409
    assert data == {"detail": "Catalog already exists: `dev`"}
