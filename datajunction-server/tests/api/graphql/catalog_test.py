"""
Tests for the catalog API.
"""

from fastapi.testclient import TestClient


def test_catalog_list(
    client: TestClient,
) -> None:
    """
    Test listing catalogs
    """
    response = client.post(
        "/engines/",
        json={
            "name": "spark",
            "version": "3.3.1",
            "dialect": "spark",
        },
    )

    response = client.post(
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

    response = client.post(
        "/catalogs/",
        json={
            "name": "test",
        },
    )

    response = client.post(
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

    response = client.post("/graphql", json={"query": query})
    assert response.status_code == 200
    assert response.json() == {
        "data": {
            "listCatalogs": [
                {"name": "unknown"},
                {"name": "dev"},
                {"name": "test"},
                {"name": "prod"},
            ],
        },
    }
