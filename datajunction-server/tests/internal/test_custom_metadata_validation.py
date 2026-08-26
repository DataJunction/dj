"""Tests for custom_metadata write-time validation helper."""

import pytest

from datajunction_server.database.custom_metadata_schema import CustomMetadataSchema
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.internal.custom_metadata import validate_custom_metadata
from datajunction_server.models.node_type import NodeType


@pytest.mark.asyncio
async def test_registered_key_valid_passes(session):
    session.add(
        CustomMetadataSchema(
            key="table_group",
            json_schema={"type": "string"},
            value_kind="string",
        ),
    )
    await session.commit()
    await validate_custom_metadata(
        session,
        None,
        NodeType.METRIC,
        {"table_group": "ads"},
    )  # no raise


@pytest.mark.asyncio
async def test_registered_key_invalid_raises(session):
    session.add(
        CustomMetadataSchema(
            key="table_group",
            json_schema={"type": "string"},
            value_kind="string",
        ),
    )
    await session.commit()
    with pytest.raises(DJInvalidInputException):
        await validate_custom_metadata(
            session,
            None,
            NodeType.METRIC,
            {"table_group": 123},
        )


@pytest.mark.asyncio
async def test_unregistered_key_passes(session):
    await validate_custom_metadata(
        session,
        None,
        NodeType.METRIC,
        {"anything": {"nested": True}},
    )  # no raise


@pytest.mark.asyncio
async def test_empty_and_none_pass(session):
    await validate_custom_metadata(session, None, NodeType.METRIC, None)
    await validate_custom_metadata(session, None, NodeType.METRIC, {})


@pytest.mark.asyncio
async def test_unregistered_key_skipped_while_registered_key_exists(session):
    """Unregistered key is skipped (line 89) even when a schema exists for another key."""
    session.add(
        CustomMetadataSchema(
            key="registered",
            json_schema={"type": "string"},
            value_kind="string",
        ),
    )
    await session.commit()
    await validate_custom_metadata(
        session,
        None,
        NodeType.METRIC,
        {"registered": "ok", "unregistered": {"anything": 1}},
    )  # no raise: unregistered key is skipped
