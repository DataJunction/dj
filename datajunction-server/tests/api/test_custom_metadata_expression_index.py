"""Tests for per-key expression index creation on schema registration.

Tier-1 index: numeric-typed schemas (type='number' or type='integer') get a
functional index on (custom_metadata->>'key')::numeric for range/sort queries.
String and other types do NOT get an index (equality served by GIN).

Key names chosen to be unique to this file to avoid cross-test index leakage
when the testcontainer DB persists across the session.
"""

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
async def test_numeric_schema_builds_expression_index(
    client: AsyncClient,
    session: AsyncSession,
) -> None:
    """Registering a filterable numeric schema must create ix_cm_threshold_num."""
    resp = await client.post(
        "/custom-metadata/schemas/",
        json={"key": "threshold_num", "json_schema": {"type": "number"}},
    )
    assert resp.status_code in (200, 201)

    result = await session.execute(
        text(
            "SELECT indexname FROM pg_indexes "
            "WHERE tablename='noderevision' "
            "AND indexname='ix_cm_threshold_num'",
        ),
    )
    assert result.scalar() == "ix_cm_threshold_num"


@pytest.mark.asyncio
async def test_integer_schema_builds_expression_index(
    client: AsyncClient,
    session: AsyncSession,
) -> None:
    """Registering a filterable integer schema must create ix_cm_priority_int."""
    resp = await client.post(
        "/custom-metadata/schemas/",
        json={"key": "priority_int", "json_schema": {"type": "integer"}},
    )
    assert resp.status_code in (200, 201)

    result = await session.execute(
        text(
            "SELECT indexname FROM pg_indexes "
            "WHERE tablename='noderevision' "
            "AND indexname='ix_cm_priority_int'",
        ),
    )
    assert result.scalar() == "ix_cm_priority_int"


@pytest.mark.asyncio
async def test_string_schema_builds_no_expression_index(
    client: AsyncClient,
    session: AsyncSession,
) -> None:
    """Registering a string schema must NOT create an expression index (GIN covers equality)."""
    resp = await client.post(
        "/custom-metadata/schemas/",
        json={"key": "label_str", "json_schema": {"type": "string"}},
    )
    assert resp.status_code in (200, 201)

    result = await session.execute(
        text(
            "SELECT indexname FROM pg_indexes "
            "WHERE tablename='noderevision' "
            "AND indexname='ix_cm_label_str'",
        ),
    )
    assert result.scalar() is None


@pytest.mark.asyncio
async def test_non_filterable_numeric_schema_builds_no_index(
    client: AsyncClient,
    session: AsyncSession,
) -> None:
    """A numeric schema with filterable=False must NOT build an expression index."""
    resp = await client.post(
        "/custom-metadata/schemas/",
        json={
            "key": "hidden_score",
            "json_schema": {"type": "number"},
            "filterable": False,
        },
    )
    assert resp.status_code in (200, 201)

    result = await session.execute(
        text(
            "SELECT indexname FROM pg_indexes "
            "WHERE tablename='noderevision' "
            "AND indexname='ix_cm_hidden_score'",
        ),
    )
    assert result.scalar() is None
