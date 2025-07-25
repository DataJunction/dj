"""
Tests for the catalog API.
"""

import pytest
from httpx import AsyncClient


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

    response = await module__client.post(
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

    response = await module__client.post(
        "/catalogs/",
        json={
            "name": "test",
        },
    )

    response = await module__client.post(
        "/catalogs/",
        json={
            "name": "prod",
        },
    )
    query = """
    {
        listCatalogs{
            name
        }
    }
    """

    response = await module__client.post("/graphql", json={"query": query})
    assert response.status_code == 200
    assert response.json() == {
        "data": {
            "listCatalogs": [
                {"name": "default"},
                {"name": "dj_metadata"},
                {"name": "dev"},
                {"name": "test"},
                {"name": "prod"},
            ],
        },
    }
