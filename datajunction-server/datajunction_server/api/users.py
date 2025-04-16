"""
User related APIs.
"""

from typing import List, Union

from fastapi import Depends, Query
from sqlalchemy import distinct, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from datajunction_server.database.column import Column
from datajunction_server.database.history import History
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.user import User
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.history import ActivityType, EntityType
from datajunction_server.models.node import NodeMinimumDetail
from datajunction_server.models.user import UserActivity
from datajunction_server.utils import get_session, get_settings

settings = get_settings()
router = SecureAPIRouter(tags=["users"])


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


@router.get("/users", response_model=List[Union[str, UserActivity]])
async def list_users_with_activity(
    session: AsyncSession = Depends(get_session),
    *,
    with_activity: bool = False,
) -> List[Union[str, UserActivity]]:
    """
    Lists all users. The endpoint will include user activity counts if the
    `with_activity` flag is set to true.
    """
    if not with_activity:
        statement = select(User.username)
        result = await session.execute(statement)
        return result.scalars().all()

    statement = (
        select(
            User.username,
            func.count(History.id).label("count"),
        )
        .join(
            History,
            onclause=(User.username == History.user),
            isouter=True,
        )
        .group_by(User.username)
        .order_by(func.count(History.id).desc())
    )
    result = await session.execute(statement)
    return [
        UserActivity(username=user_activity[0], count=user_activity[1])
        for user_activity in result.all()
    ]
