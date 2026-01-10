"""
Tests for the catalog API.
"""

import pytest
from httpx import AsyncClient


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

    response = await client.post(
        "/catalogs/",
        json={
            "name": "test",
        },
    )

    response = await client.post(
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

    response = await client.post("/graphql", json={"query": query})
    assert response.status_code == 200
    catalog_names = {c["name"] for c in response.json()["data"]["listCatalogs"]}
    # These catalogs should be present
    assert "default" in catalog_names
    assert "dj_metadata" in catalog_names
    assert "dev" in catalog_names
    assert "test" in catalog_names
    assert "prod" in catalog_names
