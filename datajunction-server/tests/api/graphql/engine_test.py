"""
Tests for the engine API.
"""
import pytest
from httpx import AsyncClient


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

    response = await client.post(
        "/engines/",
        json={
            "name": "spark",
            "version": "3.3.0",
            "dialect": "spark",
        },
    )

    response = await client.post(
        "/engines/",
        json={
            "name": "spark",
            "version": "3.3.1",
            "dialect": "spark",
        },
    )
    query = """
    {
        listEngines{
            name
            uri
            version
            dialect
        }
    }
    """

    response = await client.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data == {
        "data": {
            "listEngines": [
                {"name": "spark", "uri": None, "version": "2.4.4", "dialect": "SPARK"},
                {"name": "spark", "uri": None, "version": "3.3.0", "dialect": "SPARK"},
                {"name": "spark", "uri": None, "version": "3.3.1", "dialect": "SPARK"},
            ],
        },
    }
