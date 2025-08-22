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
    assert data == {
        "data": {
            "listEngines": [
                {
                    "dialect": "POSTGRES",
                    "name": "dj_system",
                    "uri": "postgresql+psycopg://readonly_user:readonly_pass@postgres_metadata:5432/dj",
                    "version": "",
                },
                {"name": "spark", "uri": None, "version": "2.4.4", "dialect": "SPARK"},
                {"name": "spark", "uri": None, "version": "3.3.0", "dialect": "SPARK"},
                {"name": "spark", "uri": None, "version": "3.3.1", "dialect": "SPARK"},
            ],
        },
    }


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
