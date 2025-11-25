"""
Tests for group membership resolution service.
"""

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.group_member import GroupMember
from datajunction_server.database.user import OAuthProvider, PrincipalKind, User
from datajunction_server.internal.access.group_membership import (
    GroupMembershipService,
    PostgresGroupMembershipService,
    StaticGroupMembershipService,
    get_group_membership_service,
)


@pytest_asyncio.fixture
async def alice(session: AsyncSession) -> User:
    """Create alice user."""
    user = User(
        username="alice",
        email="alice@company.com",
        name="Alice",
        oauth_provider=OAuthProvider.BASIC,
        kind=PrincipalKind.USER,
    )
    session.add(user)
    await session.commit()
    await session.refresh(user)
    return user


@pytest_asyncio.fixture
async def bob(session: AsyncSession) -> User:
    """Create bob user."""
    user = User(
        username="bob",
        email="bob@company.com",
        name="Bob",
        oauth_provider=OAuthProvider.BASIC,
        kind=PrincipalKind.USER,
    )
    session.add(user)
    await session.commit()
    await session.refresh(user)
    return user


@pytest_asyncio.fixture
async def eng_group(session: AsyncSession) -> User:
    """Create engineering group."""
    group = User(
        username="eng-team",
        email="eng-team@company.com",
        name="Engineering Team",
        oauth_provider=OAuthProvider.GOOGLE,
        kind=PrincipalKind.GROUP,
    )
    session.add(group)
    await session.commit()
    await session.refresh(group)
    return group


@pytest_asyncio.fixture
async def data_group(session: AsyncSession) -> User:
    """Create data group."""
    group = User(
        username="data-team",
        email="data-team@company.com",
        name="Data Team",
        oauth_provider=OAuthProvider.GOOGLE,
        kind=PrincipalKind.GROUP,
    )
    session.add(group)
    await session.commit()
    await session.refresh(group)
    return group


# PostgresGroupMembershipService Tests


@pytest.mark.asyncio
async def test_postgres_service_is_user_in_group_true(
    session: AsyncSession,
    alice: User,
    eng_group: User,
):
    """Test that user is correctly identified as member of group."""
    # Add alice to eng_group
    membership = GroupMember(
        group_id=eng_group.id,
        member_id=alice.id,
    )
    session.add(membership)
    await session.commit()

    # Check membership
    service = PostgresGroupMembershipService()
    is_member = await service.is_user_in_group(session, "alice", "eng-team")

    assert is_member is True


@pytest.mark.asyncio
async def test_postgres_service_is_user_in_group_false(
    session: AsyncSession,
    alice: User,
    bob: User,
    eng_group: User,
):
    """Test that user is correctly identified as NOT a member."""
    # Add only alice to eng_group
    membership = GroupMember(
        group_id=eng_group.id,
        member_id=alice.id,
    )
    session.add(membership)
    await session.commit()

    # Check bob's membership (should be False)
    service = PostgresGroupMembershipService()
    is_member = await service.is_user_in_group(session, "bob", "eng-team")

    assert is_member is False


@pytest.mark.asyncio
async def test_postgres_service_is_user_in_group_nonexistent_user(
    session: AsyncSession,
    eng_group: User,
):
    """Test checking membership for nonexistent user."""
    service = PostgresGroupMembershipService()
    is_member = await service.is_user_in_group(session, "nonexistent", "eng-team")

    assert is_member is False


@pytest.mark.asyncio
async def test_postgres_service_is_user_in_group_nonexistent_group(
    session: AsyncSession,
    alice: User,
):
    """Test checking membership for nonexistent group."""
    service = PostgresGroupMembershipService()
    is_member = await service.is_user_in_group(session, "alice", "nonexistent-group")

    assert is_member is False


@pytest.mark.asyncio
async def test_postgres_service_get_user_groups_single(
    session: AsyncSession,
    alice: User,
    eng_group: User,
):
    """Test getting user's groups when user is in one group."""
    # Add alice to eng_group
    membership = GroupMember(
        group_id=eng_group.id,
        member_id=alice.id,
    )
    session.add(membership)
    await session.commit()

    # Get alice's groups
    service = PostgresGroupMembershipService()
    groups = await service.get_user_groups(session, "alice")

    assert len(groups) == 1
    assert "eng-team" in groups


@pytest.mark.asyncio
async def test_postgres_service_get_user_groups_multiple(
    session: AsyncSession,
    alice: User,
    eng_group: User,
    data_group: User,
):
    """Test getting user's groups when user is in multiple groups."""
    # Add alice to both groups
    membership1 = GroupMember(
        group_id=eng_group.id,
        member_id=alice.id,
    )
    membership2 = GroupMember(
        group_id=data_group.id,
        member_id=alice.id,
    )
    session.add_all([membership1, membership2])
    await session.commit()

    # Get alice's groups
    service = PostgresGroupMembershipService()
    groups = await service.get_user_groups(session, "alice")

    assert len(groups) == 2
    assert "eng-team" in groups
    assert "data-team" in groups


@pytest.mark.asyncio
async def test_postgres_service_get_user_groups_none(
    session: AsyncSession,
    alice: User,
    eng_group: User,
):
    """Test getting user's groups when user is not in any groups."""
    # Don't add alice to any group

    service = PostgresGroupMembershipService()
    groups = await service.get_user_groups(session, "alice")

    assert len(groups) == 0


@pytest.mark.asyncio
async def test_postgres_service_get_user_groups_nonexistent_user(
    session: AsyncSession,
):
    """Test getting groups for nonexistent user."""
    service = PostgresGroupMembershipService()
    groups = await service.get_user_groups(session, "nonexistent")

    assert len(groups) == 0


# StaticGroupMembershipService Tests


@pytest.mark.asyncio
async def test_static_service_is_user_in_group_always_false(
    session: AsyncSession,
    alice: User,
    eng_group: User,
):
    """Test that static service always returns False for membership."""
    # Even if we add alice to group in database
    membership = GroupMember(
        group_id=eng_group.id,
        member_id=alice.id,
    )
    session.add(membership)
    await session.commit()

    # Static service should still return False
    service = StaticGroupMembershipService()
    is_member = await service.is_user_in_group(session, "alice", "eng-team")

    assert is_member is False


@pytest.mark.asyncio
async def test_static_service_get_user_groups_always_empty(
    session: AsyncSession,
    alice: User,
    eng_group: User,
):
    """Test that static service always returns empty list."""
    # Even if we add alice to group in database
    membership = GroupMember(
        group_id=eng_group.id,
        member_id=alice.id,
    )
    session.add(membership)
    await session.commit()

    # Static service should still return empty
    service = StaticGroupMembershipService()
    groups = await service.get_user_groups(session, "alice")

    assert len(groups) == 0


# Factory Function Tests


def test_get_group_membership_service_postgres(monkeypatch):
    """Test factory returns PostgresGroupMembershipService."""
    # Mock settings
    from unittest.mock import MagicMock

    mock_settings = MagicMock()
    mock_settings.group_membership_provider = "postgres"

    def mock_get_settings():
        return mock_settings

    monkeypatch.setattr(
        "datajunction_server.utils.get_settings",
        mock_get_settings,
    )

    service = get_group_membership_service()
    assert isinstance(service, PostgresGroupMembershipService)


def test_get_group_membership_service_static(monkeypatch):
    """Test factory returns StaticGroupMembershipService."""
    from unittest.mock import MagicMock

    mock_settings = MagicMock()
    mock_settings.group_membership_provider = "static"

    def mock_get_settings():
        return mock_settings

    monkeypatch.setattr(
        "datajunction_server.utils.get_settings",
        mock_get_settings,
    )

    service = get_group_membership_service()
    assert isinstance(service, StaticGroupMembershipService)


def test_get_group_membership_service_unknown_provider(monkeypatch):
    """Test factory raises ValueError for unknown provider."""
    from unittest.mock import MagicMock

    mock_settings = MagicMock()
    mock_settings.group_membership_provider = "nonexistent"

    def mock_get_settings():
        return mock_settings

    monkeypatch.setattr(
        "datajunction_server.utils.get_settings",
        mock_get_settings,
    )

    with pytest.raises(ValueError) as exc_info:
        get_group_membership_service()

    assert "Unknown group_membership_provider" in str(exc_info.value)
    assert "nonexistent" in str(exc_info.value)
    assert "postgres" in str(exc_info.value)
    assert "static" in str(exc_info.value)


# Custom Subclass Discovery Tests


def test_custom_subclass_discovery(monkeypatch):
    """Test that custom subclasses are automatically discovered."""
    from unittest.mock import MagicMock

    # Create a custom service
    class CustomGroupMembershipService(GroupMembershipService):
        name = "custom"

        async def is_user_in_group(self, session, username, group_name):
            return True

        async def get_user_groups(self, session, username):
            return ["custom-group"]

    # Mock settings to use custom provider
    mock_settings = MagicMock()
    mock_settings.group_membership_provider = "custom"

    def mock_get_settings():
        return mock_settings

    monkeypatch.setattr(
        "datajunction_server.utils.get_settings",
        mock_get_settings,
    )

    # Factory should discover and return custom service
    service = get_group_membership_service()
    assert isinstance(service, CustomGroupMembershipService)
    assert service.name == "custom"


def test_subclass_without_name_not_registered(monkeypatch):
    """Test that subclass without 'name' attribute is not registered."""
    from unittest.mock import MagicMock

    # Create a subclass without name attribute
    class InvalidService(GroupMembershipService):
        # Missing 'name' attribute!

        async def is_user_in_group(self, session, username, group_name):
            return True

        async def get_user_groups(self, session, username):
            return []

    mock_settings = MagicMock()
    mock_settings.group_membership_provider = "postgres"

    def mock_get_settings():
        return mock_settings

    monkeypatch.setattr(
        "datajunction_server.utils.get_settings",
        mock_get_settings,
    )

    # Should still work with built-in providers
    service = get_group_membership_service()
    assert isinstance(service, PostgresGroupMembershipService)


# Integration Tests


@pytest.mark.asyncio
async def test_multiple_users_in_same_group(
    session: AsyncSession,
    alice: User,
    bob: User,
    eng_group: User,
):
    """Test multiple users can be members of the same group."""
    # Add both alice and bob to eng_group
    membership1 = GroupMember(group_id=eng_group.id, member_id=alice.id)
    membership2 = GroupMember(group_id=eng_group.id, member_id=bob.id)
    session.add_all([membership1, membership2])
    await session.commit()

    service = PostgresGroupMembershipService()

    # Both should be members
    assert await service.is_user_in_group(session, "alice", "eng-team") is True
    assert await service.is_user_in_group(session, "bob", "eng-team") is True

    # Both should see the group in their group list
    alice_groups = await service.get_user_groups(session, "alice")
    bob_groups = await service.get_user_groups(session, "bob")

    assert "eng-team" in alice_groups
    assert "eng-team" in bob_groups


@pytest.mark.asyncio
async def test_user_in_multiple_groups(
    session: AsyncSession,
    alice: User,
    eng_group: User,
    data_group: User,
):
    """Test user can be member of multiple groups."""
    # Add alice to both groups
    membership1 = GroupMember(group_id=eng_group.id, member_id=alice.id)
    membership2 = GroupMember(group_id=data_group.id, member_id=alice.id)
    session.add_all([membership1, membership2])
    await session.commit()

    service = PostgresGroupMembershipService()

    # Should be member of both
    assert await service.is_user_in_group(session, "alice", "eng-team") is True
    assert await service.is_user_in_group(session, "alice", "data-team") is True

    # Should see both groups
    alice_groups = await service.get_user_groups(session, "alice")
    assert len(alice_groups) == 2
    assert "eng-team" in alice_groups
    assert "data-team" in alice_groups
