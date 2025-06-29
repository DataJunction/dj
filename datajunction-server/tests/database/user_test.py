from typing import cast
import pytest
import pytest_asyncio
import pytest_asyncio
from datajunction_server.database.user import OAuthProvider, User
from sqlalchemy.ext.asyncio import AsyncSession


@pytest_asyncio.fixture
async def user(session: AsyncSession) -> User:
    """
    A user fixture.
    """
    user = User(
        username="testuser",
        oauth_provider=OAuthProvider.BASIC,
    )
    session.add(user)
    await session.commit()
    return user


@pytest_asyncio.fixture
async def another_user(session: AsyncSession) -> User:
    """
    Another user fixture.
    """
    user = User(
        username="anotheruser",
        oauth_provider=OAuthProvider.BASIC,
    )
    session.add(user)
    await session.commit()
    return user


@pytest.mark.asyncio
async def test_get_by_usernames(
    session: AsyncSession,
    user: User,
    another_user: User,
):
    users = await User.get_by_usernames(session, usernames=["testuser", "anotheruser"])
    assert len(users) == 2
    assert users[0].id == user.id
    assert users[0].username == user.username

    assert users[1].id == another_user.id
    assert users[1].username == another_user.username


@pytest.mark.asyncio
async def test_get_by_username(
    session: AsyncSession,
    user: User,
    another_user: User,
):
    retrieved_user = cast(
        User,
        await User.get_by_username(session, username="testuser"),
    )
    assert retrieved_user.id == user.id
    assert retrieved_user.username == user.username

    retrieved_another_user = cast(
        User,
        await User.get_by_username(session, username="anotheruser"),
    )
    assert retrieved_another_user.id == another_user.id
    assert retrieved_another_user.username == another_user.username
