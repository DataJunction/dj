"""Tests for filtering nodes by custom_metadata predicates via find_by."""

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.node import Node, NodeRevision, NodeType
from datajunction_server.database.user import OAuthProvider, User
from datajunction_server.models.node import NodeMode
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

    nested_node = Node(
        name="default.nested_metric",
        type=NodeType.METRIC,
        current_version="v1",
        created_by_id=_user.id,
    )
    nested_revision = NodeRevision(
        node=nested_node,
        name=nested_node.name,
        type=nested_node.type,
        version="v1",
        query="SELECT COUNT(1) FROM nested",
        custom_metadata={
            "system": {"lifecycle": "actively_maintained", "score": 7},
            "odd.key": "literal",
        },
        created_by_id=_user.id,
    )

    session.add_all(
        [
            ads_node,
            ads_revision,
            growth_node,
            growth_revision,
            nested_node,
            nested_revision,
        ],
    )
    await session.commit()
    return ads_node, growth_node, nested_node


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
    assert {n.name for n in results} == {
        "default.ads_metric",
        "default.growth_metric",
    }


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


@pytest.mark.asyncio
async def test_find_by_custom_metadata_with_an_already_joined_revision(
    session: AsyncSession,
    nodes_with_metadata,
):
    """A custom_metadata filter must not re-join the revision another filter joined.

    `find_by` joins the current revision once and tracks it, because a second join
    of the same alias is a SQLAlchemy error rather than a slower query. Every other
    revision-dependent filter shares that flag, so the case worth pinning is a
    custom_metadata filter arriving *after* something else has already joined --
    here a `mode` filter, which joins up front.
    """
    results = await Node.find_by(
        session,
        mode=NodeMode.PUBLISHED,
        custom_metadata_filters=[
            CustomMetadataFilter(
                key="table_group",
                op=CustomMetadataOp.EQ,
                value="ads",
            ),
        ],
    )
    assert {n.name for n in results} == {"default.ads_metric"}


@pytest.mark.asyncio
async def test_find_by_nested_path_eq(session: AsyncSession, nodes_with_metadata):
    """A dotted key addresses a nested value via JSONB containment."""
    results = await Node.find_by(
        session,
        custom_metadata_filters=[
            CustomMetadataFilter(
                key="system.lifecycle",
                op=CustomMetadataOp.EQ,
                value="actively_maintained",
            ),
        ],
    )
    assert {n.name for n in results} == {"default.nested_metric"}


@pytest.mark.asyncio
async def test_find_by_nested_path_eq_no_match(
    session: AsyncSession,
    nodes_with_metadata,
):
    """A dotted key with the wrong nested value matches nothing."""
    results = await Node.find_by(
        session,
        custom_metadata_filters=[
            CustomMetadataFilter(
                key="system.lifecycle",
                op=CustomMetadataOp.EQ,
                value="deprecated",
            ),
        ],
    )
    assert {n.name for n in results} == set()


@pytest.mark.asyncio
async def test_find_by_nested_path_exists(session: AsyncSession, nodes_with_metadata):
    """EXISTS on a dotted key tests the nested key, not the top-level one."""
    present = await Node.find_by(
        session,
        custom_metadata_filters=[
            CustomMetadataFilter(key="system.lifecycle", op=CustomMetadataOp.EXISTS),
        ],
    )
    assert {n.name for n in present} == {"default.nested_metric"}

    absent = await Node.find_by(
        session,
        custom_metadata_filters=[
            CustomMetadataFilter(key="system.owner", op=CustomMetadataOp.EXISTS),
        ],
    )
    assert {n.name for n in absent} == set()


@pytest.mark.asyncio
async def test_find_by_nested_path_range(session: AsyncSession, nodes_with_metadata):
    """Range operators extract a nested number and compare it numerically."""
    matches = await Node.find_by(
        session,
        custom_metadata_filters=[
            CustomMetadataFilter(
                key="system.score",
                op=CustomMetadataOp.GT,
                value=5,
            ),
        ],
    )
    assert {n.name for n in matches} == {"default.nested_metric"}

    misses = await Node.find_by(
        session,
        custom_metadata_filters=[
            CustomMetadataFilter(
                key="system.score",
                op=CustomMetadataOp.LT,
                value=5,
            ),
        ],
    )
    assert {n.name for n in misses} == set()


@pytest.mark.asyncio
async def test_find_by_escaped_dot_is_a_literal_key(
    session: AsyncSession,
    nodes_with_metadata,
):
    r"""``odd\.key`` addresses a top-level key that contains a dot."""
    escaped = await Node.find_by(
        session,
        custom_metadata_filters=[
            CustomMetadataFilter(
                key=r"odd\.key",
                op=CustomMetadataOp.EQ,
                value="literal",
            ),
        ],
    )
    assert {n.name for n in escaped} == {"default.nested_metric"}

    # Unescaped, the same text is a path into a non-existent "odd" object.
    as_path = await Node.find_by(
        session,
        custom_metadata_filters=[
            CustomMetadataFilter(
                key="odd.key",
                op=CustomMetadataOp.EQ,
                value="literal",
            ),
        ],
    )
    assert {n.name for n in as_path} == set()


@pytest.mark.asyncio
async def test_count_by_applies_custom_metadata_filters(
    session: AsyncSession,
    nodes_with_metadata,
):
    """``count_by`` backs GraphQL totalCount, so it must honour the same filters."""
    assert (
        await Node.count_by(
            session,
            custom_metadata_filters=[
                CustomMetadataFilter(
                    key="system.lifecycle",
                    op=CustomMetadataOp.EQ,
                    value="actively_maintained",
                ),
            ],
        )
        == 1
    )
