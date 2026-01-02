"""
Tests for the engine API.

Uses isolated_client to ensure a clean dialect registry and database state.
"""

import pytest
from httpx import AsyncClient

from datajunction_server.models.dialect import DialectRegistry
from datajunction_server.transpilation import (
    SQLTranspilationPlugin,
    SQLGlotTranspilationPlugin,
)


@pytest.fixture
def clean_dialect_registry():
    """Clear and reset the dialect registry with default plugins.

    Order matches the expected test output (from /dialects/ endpoint).
    """
    DialectRegistry._registry.clear()
    # Register in the order expected by test_dialects_list:
    # spark, trino (SQLTranspilationPlugin)
    # sqlite, snowflake, redshift, postgres, duckdb (SQLGlotTranspilationPlugin)
    # druid (SQLTranspilationPlugin)
    # clickhouse (SQLGlotTranspilationPlugin)
    DialectRegistry.register("spark", SQLTranspilationPlugin)
    DialectRegistry.register("trino", SQLTranspilationPlugin)
    DialectRegistry.register("sqlite", SQLGlotTranspilationPlugin)
    DialectRegistry.register("snowflake", SQLGlotTranspilationPlugin)
    DialectRegistry.register("redshift", SQLGlotTranspilationPlugin)
    DialectRegistry.register("postgres", SQLGlotTranspilationPlugin)
    DialectRegistry.register("duckdb", SQLGlotTranspilationPlugin)
    DialectRegistry.register("druid", SQLTranspilationPlugin)
    DialectRegistry.register("clickhouse", SQLGlotTranspilationPlugin)
    yield
    # Optional cleanup after test


@pytest.mark.asyncio
async def test_engine_adding_a_new_engine(
    isolated_client: AsyncClient,
    clean_dialect_registry,
) -> None:
    """
    Test adding an engine
    """
    response = await isolated_client.post(
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
    isolated_client: AsyncClient,
    clean_dialect_registry,
) -> None:
    """
    Test listing engines
    """
    response = await isolated_client.post(
        "/engines/",
        json={
            "name": "spark-foo",
            "version": "2.4.4",
            "dialect": "spark",
        },
    )
    assert response.status_code == 201

    response = await isolated_client.post(
        "/engines/",
        json={
            "name": "spark-foo",
            "version": "3.3.0",
            "dialect": "spark",
        },
    )
    assert response.status_code == 201

    response = await isolated_client.post(
        "/engines/",
        json={
            "name": "spark-foo",
            "version": "3.3.1",
            "dialect": "spark",
        },
    )
    assert response.status_code == 201

    response = await isolated_client.get("/engines/")
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
    isolated_client: AsyncClient,
    clean_dialect_registry,
) -> None:
    """
    Test getting an engine
    """
    response = await isolated_client.post(
        "/engines/",
        json={
            "name": "spark-two",
            "version": "3.3.1",
            "dialect": "spark",
        },
    )
    assert response.status_code == 201

    response = await isolated_client.get(
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
    isolated_client: AsyncClient,
    clean_dialect_registry,
) -> None:
    """
    Test raise on engine already exists
    """
    response = await isolated_client.post(
        "/engines/",
        json={
            "name": "spark-three",
            "version": "3.3.1",
            "dialect": "spark",
        },
    )
    assert response.status_code == 201

    response = await isolated_client.post(
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


@pytest.mark.asyncio
async def test_dialects_list(
    isolated_client: AsyncClient,
    clean_dialect_registry,
) -> None:
    """
    Test listing dialects
    """
    response = await isolated_client.get("/dialects/")
    assert response.status_code == 200
    assert response.json() == [
        {
            "name": "spark",
            "plugin_class": "SQLTranspilationPlugin",
        },
        {
            "name": "trino",
            "plugin_class": "SQLTranspilationPlugin",
        },
        {
            "name": "sqlite",
            "plugin_class": "SQLGlotTranspilationPlugin",
        },
        {
            "name": "snowflake",
            "plugin_class": "SQLGlotTranspilationPlugin",
        },
        {
            "name": "redshift",
            "plugin_class": "SQLGlotTranspilationPlugin",
        },
        {
            "name": "postgres",
            "plugin_class": "SQLGlotTranspilationPlugin",
        },
        {
            "name": "duckdb",
            "plugin_class": "SQLGlotTranspilationPlugin",
        },
        {
            "name": "druid",
            "plugin_class": "SQLTranspilationPlugin",
        },
        {
            "name": "clickhouse",
            "plugin_class": "SQLGlotTranspilationPlugin",
        },
    ]
