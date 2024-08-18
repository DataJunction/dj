"""
Tag related APIs.
"""

from typing import List, Optional

from fastapi import Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from datajunction_server.database import Node
from datajunction_server.database.history import ActivityType, EntityType, History
from datajunction_server.database.tag import Tag
from datajunction_server.database.user import User
from datajunction_server.errors import DJAlreadyExistsException, DJDoesNotExistException
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.models.node import NodeMinimumDetail
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.tag import CreateTag, TagOutput, UpdateTag
from datajunction_server.utils import (
    get_and_update_current_user,
    get_session,
    get_settings,
)

settings = get_settings()
router = SecureAPIRouter(tags=["tags"])


async def get_tags_by_name(
    session: AsyncSession,
    names: List[str],
) -> List[Tag]:
    """
    Retrieves a list of tags by name
    """
    statement = select(Tag).where(Tag.name.in_(names))  # type: ignore  # pylint: disable=no-member
    tags = (await session.execute(statement)).scalars().all()
    difference = set(names) - {tag.name for tag in tags}
    if difference:
        raise DJDoesNotExistException(
            message=f"Tags not found: {', '.join(difference)}",
        )
    return tags


async def get_tag_by_name(
    session: AsyncSession,
    name: str,
    raise_if_not_exists: bool = False,
    for_update: bool = False,
):
    """
    Retrieves a tag by its name.
    """
    statement = select(Tag).where(Tag.name == name)
    if for_update:
        statement = statement.with_for_update().execution_options(
            populate_existing=True,
        )
    tag = (await session.execute(statement)).scalars().one_or_none()
    if not tag and raise_if_not_exists:
        raise DJDoesNotExistException(  # pragma: no cover
            message=(f"A tag with name `{name}` does not exist."),
            http_status_code=404,
        )
    return tag


@router.get("/tags/", response_model=List[TagOutput])
async def list_tags(
    tag_type: Optional[str] = None, *, session: AsyncSession = Depends(get_session)
) -> List[TagOutput]:
    """
    List all available tags.
    """
    statement = select(Tag)
    if tag_type:
        statement = statement.where(Tag.tag_type == tag_type)
    result = await session.execute(statement)
    return result.scalars().all()


@router.get("/tags/{name}/", response_model=TagOutput)
async def get_a_tag(
    name: str, *, session: AsyncSession = Depends(get_session)
) -> TagOutput:
    """
    Return a tag by name.
    """
    tag = await get_tag_by_name(session, name, raise_if_not_exists=True)
    return tag


@router.post("/tags/", response_model=TagOutput, status_code=201)
async def create_a_tag(
    data: CreateTag,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_and_update_current_user),
) -> TagOutput:
    """
    Create a tag.
    """
    tag = await get_tag_by_name(session, data.name, raise_if_not_exists=False)
    if tag:
        raise DJAlreadyExistsException(
            message=f"A tag with name `{data.name}` already exists!",
            http_status_code=500,
        )
    tag = Tag(
        name=data.name,
        tag_type=data.tag_type,
        description=data.description,
        display_name=data.display_name,
        tag_metadata=data.tag_metadata,
        created_by_id=current_user.id,
    )
    session.add(tag)
    session.add(
        History(
            entity_type=EntityType.TAG,
            entity_name=tag.name,
            activity_type=ActivityType.CREATE,
            user=current_user.username,
        ),
    )
    await session.commit()
    await session.refresh(tag)
    return tag


@router.patch("/tags/{name}/", response_model=TagOutput)
async def update_a_tag(
    name: str,
    data: UpdateTag,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_and_update_current_user),
) -> TagOutput:
    """
    Update a tag.
    """
    tag = await get_tag_by_name(
        session,
        name,
        raise_if_not_exists=True,
        for_update=True,
    )

    if data.description:
        tag.description = data.description
    if data.tag_metadata:
        tag.tag_metadata = data.tag_metadata
    if data.display_name:
        tag.display_name = data.display_name
    session.add(tag)
    session.add(
        History(
            entity_type=EntityType.TAG,
            entity_name=tag.name,
            activity_type=ActivityType.UPDATE,
            details=data.dict(),
            user=current_user.username,
        ),
    )
    await session.commit()
    await session.refresh(tag)
    return tag


@router.get("/tags/{name}/nodes/", response_model=List[NodeMinimumDetail])
async def list_nodes_for_a_tag(
    name: str,
    node_type: Optional[NodeType] = None,
    *,
    session: AsyncSession = Depends(get_session),
) -> List[NodeMinimumDetail]:
    """
    Find nodes tagged with the tag, filterable by node type.
    """
    statement = (
        select(Tag)
        .where(Tag.name == name)
        .options(joinedload(Tag.nodes).options(joinedload(Node.current)))
    )
    tag = (await session.execute(statement)).unique().scalars().one_or_none()
    if not tag:
        raise DJDoesNotExistException(
            message=f"A tag with name `{name}` does not exist.",
            http_status_code=404,
        )
    if not node_type:
        return sorted(
            [node.current for node in tag.nodes if not node.deactivated_at],
            key=lambda x: x.name,
        )
    return sorted(
        [
            node.current
            for node in tag.nodes
            if node.type == node_type and not node.deactivated_at
        ],
        key=lambda x: x.name,
    )
