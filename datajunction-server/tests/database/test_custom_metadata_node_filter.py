"""Tests for filtering nodes by custom_metadata predicates via find_by."""

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.node import Node, NodeRevision, NodeType
from datajunction_server.database.user import OAuthProvider, User
from datajunction_server.models.custom_metadata import (
    CustomMetadataFilter,
    CustomMetadataOp,
)


@pytest_asyncio.fixture
async def _user(session: AsyncSession) -> User:
    """A minimal user needed for node creation."""
    user = User(username="cm_filter_test_user", oauth_provider=OAuthProvider.BASIC)
    session.add(user)
    await session.commit()
    return user


@pytest_asyncio.fixture
async def nodes_with_metadata(session: AsyncSession, _user: User):
    """Seed two metric nodes with different custom_metadata values."""
    ads_node = Node(
        name="default.ads_metric",
        type=NodeType.METRIC,
        current_version="v1",
        created_by_id=_user.id,
    )
    ads_revision = NodeRevision(
        node=ads_node,
        name=ads_node.name,
        type=ads_node.type,
        version="v1",
        query="SELECT COUNT(1) FROM ads",
        custom_metadata={"table_group": "ads"},
        created_by_id=_user.id,
    )

    growth_node = Node(
        name="default.growth_metric",
        type=NodeType.METRIC,
        current_version="v1",
        created_by_id=_user.id,
    )
    growth_revision = NodeRevision(
        node=growth_node,
        name=growth_node.name,
        type=growth_node.type,
        version="v1",
        query="SELECT COUNT(1) FROM growth",
        custom_metadata={"table_group": "growth"},
        created_by_id=_user.id,
    )

    session.add_all([ads_node, ads_revision, growth_node, growth_revision])
    await session.commit()
    return ads_node, growth_node


@pytest.mark.asyncio
async def test_find_by_custom_metadata_eq(session: AsyncSession, nodes_with_metadata):
    """Filtering by EQ returns only the node with the matching custom_metadata value."""
    results = await Node.find_by(
        session,
        custom_metadata_filters=[
            CustomMetadataFilter(
                key="table_group",
                op=CustomMetadataOp.EQ,
                value="ads",
            ),
        ],
    )
    names = {n.name for n in results}
    assert names == {"default.ads_metric"}
    assert "default.growth_metric" not in names


@pytest.mark.asyncio
async def test_find_by_custom_metadata_exists(
    session: AsyncSession,
    nodes_with_metadata,
):
    """Filtering by EXISTS returns all nodes that have the key regardless of value."""
    results = await Node.find_by(
        session,
        custom_metadata_filters=[
            CustomMetadataFilter(key="table_group", op=CustomMetadataOp.EXISTS),
        ],
    )
    names = {n.name for n in results}
    assert "default.ads_metric" in names
    assert "default.growth_metric" in names


@pytest.mark.asyncio
async def test_find_by_custom_metadata_no_match(
    session: AsyncSession,
    nodes_with_metadata,
):
    """Filtering by a non-existent value returns an empty list."""
    results = await Node.find_by(
        session,
        custom_metadata_filters=[
            CustomMetadataFilter(
                key="table_group",
                op=CustomMetadataOp.EQ,
                value="unknown",
            ),
        ],
    )
    names = {n.name for n in results}
    assert "default.ads_metric" not in names
    assert "default.growth_metric" not in names
