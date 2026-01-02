"""
Tests for the engine API.
"""

import pytest
from httpx import AsyncClient


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
            "name": "spark",
            "version": "2.4.4",
            "dialect": "spark",
        },
    )

    response = await module__client.post(
        "/engines/",
        json={
            "name": "spark",
            "version": "3.3.0",
            "dialect": "spark",
        },
    )

    response = await module__client.post(
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

    response = await module__client.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    engines = data["data"]["listEngines"]

    # Check that our created spark engines are present
    engine_keys = {(e["name"], e["version"]) for e in engines}
    assert ("spark", "2.4.4") in engine_keys
    assert ("spark", "3.3.0") in engine_keys
    assert ("spark", "3.3.1") in engine_keys

    # Check dj_system engine exists (URI will vary by environment)
    dj_system = next((e for e in engines if e["name"] == "dj_system"), None)
    assert dj_system is not None
    assert dj_system["dialect"] == "POSTGRES"


@pytest.mark.asyncio
async def test_list_dialects(
    module__client: AsyncClient,
) -> None:
    """
    Test listing dialects
    """
    query = """
    {
        listDialects{
            name
            pluginClass
        }
    }
    """
    response = await module__client.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data == {
        "data": {
            "listDialects": [
                {
                    "name": "spark",
                    "pluginClass": "SQLTranspilationPlugin",
                },
                {
                    "name": "trino",
                    "pluginClass": "SQLTranspilationPlugin",
                },
                {
                    "name": "sqlite",
                    "pluginClass": "SQLGlotTranspilationPlugin",
                },
                {
                    "name": "snowflake",
                    "pluginClass": "SQLGlotTranspilationPlugin",
                },
                {
                    "name": "redshift",
                    "pluginClass": "SQLGlotTranspilationPlugin",
                },
                {
                    "name": "postgres",
                    "pluginClass": "SQLGlotTranspilationPlugin",
                },
                {
                    "name": "duckdb",
                    "pluginClass": "SQLGlotTranspilationPlugin",
                },
                {
                    "name": "druid",
                    "pluginClass": "SQLTranspilationPlugin",
                },
                {
                    "name": "clickhouse",
                    "pluginClass": "SQLGlotTranspilationPlugin",
                },
            ],
        },
    }
