"""
Group management APIs.
"""

from typing import List

from fastapi import Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.group_member import GroupMember
from datajunction_server.database.user import OAuthProvider, PrincipalKind, User
from datajunction_server.errors import DJAlreadyExistsException, DJDoesNotExistException
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.models.group import GroupOutput
from datajunction_server.models.user import UserOutput
from datajunction_server.utils import get_session, get_settings

settings = get_settings()
router = SecureAPIRouter(tags=["groups"])


@router.post("/groups/", response_model=GroupOutput, status_code=201)
async def register_group(
    username: str,
    email: str | None = None,
    name: str | None = None,
    *,
    session: AsyncSession = Depends(get_session),
) -> User:
    """
    Register a group in DJ.

    This makes the group available for assignment as a node owner.
    Group membership can be managed via the membership endpoints (Postgres provider)
    or resolved externally (LDAP, etc.).

    Args:
        username: Unique identifier for the group (e.g., 'eng-team')
        email: Optional email for the group
        name: Display name (defaults to username)
    """
    existing = await User.get_by_username(session, username)
    if existing:
        raise DJAlreadyExistsException(message=f"Group {username} already exists")

    # Create group
    group = User(
        username=username,
        email=email,
        name=name or username,
        kind=PrincipalKind.GROUP,
        oauth_provider=OAuthProvider.BASIC,
    )
    session.add(group)
    await session.commit()
    await session.refresh(group)
    return group


@router.get("/groups/", response_model=List[GroupOutput])
async def list_groups(
    *,
    session: AsyncSession = Depends(get_session),
) -> List[User]:
    """
    List all registered groups.
    """
    statement = (
        select(User).where(User.kind == PrincipalKind.GROUP).order_by(User.username)
    )
    result = await session.execute(statement)
    return list(result.scalars().all())


@router.get("/groups/{group_name}", response_model=GroupOutput)
async def get_group(
    group_name: str,
    *,
    session: AsyncSession = Depends(get_session),
) -> User:
    """
    Get a group by name.
    """
    group = await User.get_by_username(session, group_name)
    if not group or group.kind != PrincipalKind.GROUP:
        raise HTTPException(status_code=404, detail=f"Group {group_name} not found")

    return group


@router.post("/groups/{group_name}/members/", status_code=201)
async def add_group_member(
    group_name: str,
    member_username: str,
    *,
    session: AsyncSession = Depends(get_session),
) -> dict:
    """
    Add a member to a group (Postgres provider only).

    For external providers, membership is managed externally and this endpoint is disabled.
    """
    if settings.group_membership_provider != "postgres":
        raise HTTPException(
            status_code=400,
            detail=f"Membership management not supported for provider: {settings.group_membership_provider}",
        )

    # Verify group exists
    group = await User.get_by_username(session, group_name)
    if not group or group.kind != PrincipalKind.GROUP:
        raise DJDoesNotExistException(message=f"Group {group_name} not found")

    # Verify member exists
    member = await User.get_by_username(session, member_username)
    if not member:
        raise DJDoesNotExistException(message=f"User {member_username} not found")

    # Check if already a member
    existing = await session.execute(
        select(GroupMember).where(
            GroupMember.group_id == group.id,
            GroupMember.member_id == member.id,
        ),
    )
    if existing.scalar_one_or_none():
        raise HTTPException(
            status_code=409,
            detail=f"{member_username} is already a member of {group_name}",
        )

    # Add membership
    membership = GroupMember(
        group_id=group.id,
        member_id=member.id,
    )
    session.add(membership)
    await session.commit()

    return {"message": f"Added {member_username} to {group_name}"}


@router.delete("/groups/{group_name}/members/{member_username}", status_code=204)
async def remove_group_member(
    group_name: str,
    member_username: str,
    *,
    session: AsyncSession = Depends(get_session),
) -> None:
    """
    Remove a member from a group (Postgres provider only).
    """
    if settings.group_membership_provider != "postgres":
        raise HTTPException(
            status_code=400,
            detail=f"Membership management not supported for provider: {settings.group_membership_provider}",
        )

    # Verify group and member exist
    group = await User.get_by_username(session, group_name)
    if not group or group.kind != PrincipalKind.GROUP:
        raise DJDoesNotExistException(message=f"Group {group_name} not found")

    member = await User.get_by_username(session, member_username)
    if not member:
        raise DJDoesNotExistException(message=f"User {member_username} not found")

    # Remove membership
    result = await session.execute(
        select(GroupMember).where(
            GroupMember.group_id == group.id,
            GroupMember.member_id == member.id,
        ),
    )
    membership = result.scalar_one_or_none()

    if not membership:
        raise HTTPException(
            status_code=404,
            detail=f"{member_username} is not a member of {group_name}",
        )

    await session.delete(membership)
    await session.commit()


@router.get("/groups/{group_name}/members/", response_model=List[UserOutput])
async def list_group_members(
    group_name: str,
    *,
    session: AsyncSession = Depends(get_session),
) -> List[User]:
    """
    List members of a group.

    For Postgres provider: queries group_members table.
    For external providers: returns empty (membership resolved externally).
    """
    # Verify group exists
    group = await User.get_by_username(session, group_name)
    if not group or group.kind != PrincipalKind.GROUP:
        raise HTTPException(status_code=404, detail=f"Group {group_name} not found")

    # Only return members for postgres provider
    if settings.group_membership_provider != "postgres":
        return []

    # Query members
    statement = (
        select(User)
        .join(GroupMember, GroupMember.member_id == User.id)
        .where(GroupMember.group_id == group.id)
        .order_by(User.username)
    )
    result = await session.execute(statement)
    return list(result.scalars().all())
