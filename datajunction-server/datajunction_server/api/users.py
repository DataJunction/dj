"""
User related APIs.
"""

from typing import List, Optional

from fastapi import Depends, Query
from sqlalchemy import distinct, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from datajunction_server.database.column import Column
from datajunction_server.database.history import ActivityType, EntityType, History
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.user import User
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.models.node import NodeMinimumDetail
from datajunction_server.models.user import UserActivity, UserOutput
from datajunction_server.utils import get_current_user, get_session, get_settings

settings = get_settings()
router = SecureAPIRouter(tags=["users"])


@router.get("/users/me", response_model=UserOutput)
async def current_user(
    current_user: Optional[User] = Depends(  # pylint: disable=redefined-outer-name
        get_current_user,
    ),
) -> UserOutput:
    """
    Returns info on the current authenticated user, if any
    """
    return current_user  # type: ignore


@router.get("/users/{username}", response_model=List[NodeMinimumDetail])
async def list_nodes_by_username(
    username: str,
    *,
    session: AsyncSession = Depends(get_session),
    activity_types: List[str] = Query([ActivityType.CREATE, ActivityType.UPDATE]),
) -> List[NodeMinimumDetail]:
    """
    List all nodes with the specified activity type(s) by the user
    """
    statement = select(distinct(History.entity_name)).where(
        (History.user == username)
        & (History.entity_type == EntityType.NODE)
        & (History.activity_type.in_(activity_types)),
    )
    result = await session.execute(statement)
    nodes = await Node.get_by_names(
        session=session,
        names=list(set(result.scalars().all())),
        options=[
            joinedload(Node.current).options(
                selectinload(NodeRevision.cube_elements)
                .selectinload(Column.node_revisions)
                .options(
                    selectinload(NodeRevision.node),
                ),
            ),
        ],
    )
    return [node.current for node in nodes]


@router.get("/users", response_model=List[UserActivity])
async def list_users(session: AsyncSession = Depends(get_session)) -> List[str]:
    """
    List all users ordered by activity count
    """
    statement = (
        select(
            User.username,
            func.count(History.id).label("count"),  # pylint: disable=not-callable
        )
        .join(
            History,  # pylint: disable=superfluous-parens
            onclause=(User.username == History.user),
            isouter=True,
        )
        .group_by(User.username)
        .order_by(func.count(History.id).desc())  # pylint: disable=not-callable
    )
    result = await session.execute(statement)
    return [
        UserActivity(username=user_activity[0], count=user_activity[1])
        for user_activity in result.all()
    ]
