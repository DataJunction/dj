"""Tests for the CustomMetadataSchema ORM model and registry table."""

import datetime

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from datajunction_server.database.custom_metadata_schema import CustomMetadataSchema
from datajunction_server.typing import UTCDatetime


@pytest.mark.asyncio
async def test_insert_and_read_schema(session):
    row = CustomMetadataSchema(
        key="table_group",
        node_type=None,
        namespace=None,
        json_schema={"type": "string"},
        value_kind="string",
        filterable=True,
        description="arc table group",
    )
    session.add(row)
    await session.commit()
    fetched = await session.get(CustomMetadataSchema, row.id)
    assert fetched.key == "table_group"
    assert fetched.json_schema == {"type": "string"}
    assert fetched.filterable is True


@pytest.mark.asyncio
async def test_new_columns_round_trip(session):
    """updated_by_id and reserved round-trip through the ORM."""
    row = CustomMetadataSchema(
        key="contact",
        node_type=None,
        namespace=None,
        json_schema={"type": "string"},
        updated_by_id=None,
        reserved=True,
    )
    session.add(row)
    await session.commit()
    fetched = await session.get(CustomMetadataSchema, row.id)
    assert fetched.updated_by_id is None
    assert fetched.reserved is True


@pytest.mark.asyncio
async def test_reserved_defaults_to_false(session):
    """reserved defaults to False when not supplied."""
    row = CustomMetadataSchema(
        key="tag",
        node_type=None,
        namespace=None,
        json_schema={"type": "string"},
    )
    session.add(row)
    await session.commit()
    fetched = await session.get(CustomMetadataSchema, row.id)
    assert fetched.reserved is False


@pytest.mark.asyncio
async def test_soft_deleted_row_does_not_block_reregistration(session):
    """A soft-deleted row and a live row can coexist for the same scope.

    The unique index on (key, node_type, namespace) is partial, so it only
    constrains live rows. Otherwise a retired key could never be registered
    again in the same scope: every read path filters ``deactivated_at IS NULL``,
    so the writer would see no live row, INSERT, and collide with the tombstone.
    """
    original = CustomMetadataSchema(
        key="retired_key",
        node_type="metric",
        namespace="default",
        json_schema={"type": "string"},
    )
    session.add(original)
    await session.commit()

    original.deactivated_at = UTCDatetime(
        2026,
        7,
        12,
        tzinfo=datetime.UTC,
    )
    await session.commit()

    replacement = CustomMetadataSchema(
        key="retired_key",
        node_type="metric",
        namespace="default",
        json_schema={"type": "integer"},
    )
    session.add(replacement)
    await session.commit()

    rows = (
        (
            await session.execute(
                select(CustomMetadataSchema).where(
                    CustomMetadataSchema.key == "retired_key",
                ),
            )
        )
        .scalars()
        .all()
    )
    assert len(rows) == 2
    live = [row for row in rows if row.deactivated_at is None]
    assert len(live) == 1
    assert live[0].json_schema == {"type": "integer"}


@pytest.mark.asyncio
async def test_duplicate_live_scope_is_still_rejected(session):
    """Two live rows for the same scope remain a unique violation."""
    session.add(
        CustomMetadataSchema(
            key="duplicated_key",
            node_type=None,
            namespace=None,
            json_schema={"type": "string"},
        ),
    )
    await session.commit()

    session.add(
        CustomMetadataSchema(
            key="duplicated_key",
            node_type=None,
            namespace=None,
            json_schema={"type": "string"},
        ),
    )
    with pytest.raises(IntegrityError):
        await session.commit()
    await session.rollback()
