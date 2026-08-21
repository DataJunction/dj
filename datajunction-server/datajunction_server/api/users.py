"""
User related APIs.
"""

from collections import defaultdict

from fastapi import Depends, Query
from sqlalchemy import distinct, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.history import History
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.tag import Tag, TagNodeRelationship
from datajunction_server.database.user import User
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.history import ActivityType, EntityType
from datajunction_server.models.node import NodeMinimumDetail
from datajunction_server.models.tag import TagMinimum
from datajunction_server.models.user import UserActivity
from datajunction_server.utils import get_session, get_settings

settings = get_settings()
router = SecureAPIRouter(tags=["users"])


@router.get("/users/{username}", response_model=list[NodeMinimumDetail])
async def list_nodes_by_username(
    username: str,
    *,
    session: AsyncSession = Depends(get_session),
    activity_types: list[str] = Query([ActivityType.CREATE, ActivityType.UPDATE]),
) -> list[NodeMinimumDetail]:
    """
    List all nodes with the specified activity type(s) by the user
    """
    statement = select(distinct(History.entity_name)).where(
        (History.user == username)
        & (History.entity_type == EntityType.NODE)
        & (History.activity_type.in_(activity_types)),
    )
    result = await session.execute(statement)
    names = list(set(result.scalars().all()))
    if not names:
        return []

    # Column-only select of exactly the fields `NodeMinimumDetail` needs, joined
    # to the node's current revision. This avoids hydrating `Node`/`NodeRevision`
    # ORM entities altogether, which would otherwise fan out into the mapper's
    # eager-loaded relationships (`created_by`, `node`, `catalog`, etc.) for
    # every node the user has ever touched.
    nodes_statement = (
        select(
            Node.name,
            Node.type,
            Node.current_version,
            NodeRevision.display_name,
            NodeRevision.description,
            NodeRevision.status,
            NodeRevision.mode,
            NodeRevision.updated_at,
        )
        .join(
            NodeRevision,
            (NodeRevision.node_id == Node.id)
            & (NodeRevision.version == Node.current_version),
        )
        .where(Node.name.in_(names))
    )
    rows = (await session.execute(nodes_statement)).all()

    tags_statement = (
        select(Node.name, Tag.name)
        .select_from(Node)
        .join(TagNodeRelationship, TagNodeRelationship.node_id == Node.id)
        .join(Tag, Tag.id == TagNodeRelationship.tag_id)
        .where(Node.name.in_(names))
    )
    tags_by_node: dict[str, list[TagMinimum]] = defaultdict(list)
    for node_name, tag_name in (await session.execute(tags_statement)).all():
        tags_by_node[node_name].append(TagMinimum(name=tag_name))

    history_statement = select(
        distinct(History.entity_name),
        History.user,
    ).where(
        History.entity_name.in_(names)
        & (History.entity_type == EntityType.NODE)
        & (History.user.is_not(None)),
    )
    editors_by_node: dict[str, set[str]] = defaultdict(set)
    for node_name, editor in (await session.execute(history_statement)).all():
        editors_by_node[node_name].add(editor)

    return [
        NodeMinimumDetail(
            name=row.name,
            display_name=row.display_name,
            description=row.description,
            version=row.current_version,
            type=row.type,
            status=row.status,
            mode=row.mode,
            updated_at=row.updated_at,
            tags=tags_by_node.get(row.name, []),
            edited_by=list(editors_by_node.get(row.name, set())),
        )
        for row in rows
    ]


@router.get("/users", response_model=list[str | UserActivity])
async def list_users_with_activity(
    session: AsyncSession = Depends(get_session),
    *,
    with_activity: bool = False,
) -> list[str | UserActivity]:
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
