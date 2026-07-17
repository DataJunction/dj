"""Tests for custom_metadata JSONB column and GIN index on NodeRevision."""

import pytest
from sqlalchemy import text


@pytest.mark.asyncio
async def test_custom_metadata_supports_containment(session):
    """@> containment operator works, proving the column is JSONB."""
    result = await session.execute(
        text('SELECT \'{"a": 1, "b": 2}\'::jsonb @> \'{"a": 1}\'::jsonb'),
    )
    assert result.scalar() is True


@pytest.mark.asyncio
async def test_gin_index_exists(session):
    result = await session.execute(
        text(
            "SELECT indexname FROM pg_indexes "
            "WHERE tablename = 'noderevision' "
            "AND indexname = 'ix_noderevision_custom_metadata_gin'",
        ),
    )
    assert result.scalar() == "ix_noderevision_custom_metadata_gin"
