"""
User related APIs.
"""

from typing import List

from fastapi import Depends, Query
from sqlalchemy import distinct, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from datajunction_server.database.column import Column
from datajunction_server.database.history import ActivityType, EntityType, History
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.models.node import NodeMinimumDetail
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
    List all nodes created by the user
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


@router.get("/users", response_model=List[str])
async def list_users(session: AsyncSession = Depends(get_session)) -> List[str]:
    """
    List all users
    """
    statement = select(distinct(History.user))
    result = await session.execute(statement)
    return list({user for user in result.scalars().all() if user})
