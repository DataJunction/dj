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
