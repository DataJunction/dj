"""Tests for the CustomMetadataSchema ORM model and registry table."""

import pytest
from datajunction_server.database.custom_metadata_schema import CustomMetadataSchema


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
    """owner, updated_by_id, reserved round-trip through the ORM."""
    row = CustomMetadataSchema(
        key="contact",
        node_type=None,
        namespace=None,
        json_schema={"type": "string"},
        owner="team-platform",
        updated_by_id=None,
        reserved=True,
    )
    session.add(row)
    await session.commit()
    fetched = await session.get(CustomMetadataSchema, row.id)
    assert fetched.owner == "team-platform"
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
