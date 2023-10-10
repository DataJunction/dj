"""
Tag related APIs.
"""

from typing import List, Optional

from fastapi import Depends
from sqlalchemy.orm import joinedload
from sqlmodel import Session, select

from datajunction_server.errors import DJDoesNotExistException, DJException
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.models import History, User
from datajunction_server.models.history import ActivityType, EntityType
from datajunction_server.models.node import NodeMinimumDetail, NodeType
from datajunction_server.models.tag import CreateTag, Tag, TagOutput, UpdateTag
from datajunction_server.utils import get_current_user, get_session, get_settings

settings = get_settings()
router = SecureAPIRouter(tags=["tags"])


def get_tags_by_name(
    session: Session,
    names: List[str],
) -> List[Tag]:
    """
    Retrieves a list of tags by name
    """
    statement = select(Tag).where(Tag.name.in_(names))  # type: ignore  # pylint: disable=no-member
    tags = session.exec(statement).all()
    difference = set(names) - {tag.name for tag in tags}
    if difference:
        raise DJDoesNotExistException(
            message=f"Tags not found: {', '.join(difference)}",
        )
    return tags


def get_tag_by_name(
    session: Session,
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
    tag = session.exec(statement).one_or_none()
    if not tag and raise_if_not_exists:
        raise DJException(  # pragma: no cover
            message=(f"A tag with name `{name}` does not exist."),
            http_status_code=404,
        )
    return tag


@router.get("/tags/", response_model=List[TagOutput])
def list_tags(
    tag_type: Optional[str] = None, *, session: Session = Depends(get_session)
) -> List[TagOutput]:
    """
    List all available tags.
    """
    statement = select(Tag)
    if tag_type:
        statement = statement.where(Tag.tag_type == tag_type)
    return session.exec(statement).all()


@router.get("/tags/{name}/", response_model=Tag)
def get_a_tag(name: str, *, session: Session = Depends(get_session)) -> Tag:
    """
    Return a tag by name.
    """
    tag = get_tag_by_name(session, name, raise_if_not_exists=True)
    return tag


@router.post("/tags/", response_model=Tag, status_code=201)
def create_a_tag(
    data: CreateTag,
    session: Session = Depends(get_session),
    current_user: Optional[User] = Depends(get_current_user),
) -> Tag:
    """
    Create a tag.
    """
    tag = get_tag_by_name(session, data.name, raise_if_not_exists=False)
    if tag:
        raise DJException(
            message=f"A tag with name `{data.name}` already exists!",
            http_status_code=500,
        )
    tag = Tag.from_orm(data)
    session.add(tag)
    session.add(
        History(
            entity_type=EntityType.TAG,
            entity_name=tag.name,
            activity_type=ActivityType.CREATE,
            user=current_user.username if current_user else None,
        ),
    )
    session.commit()
    session.refresh(tag)
    return tag


@router.patch("/tags/{name}/", response_model=Tag)
def update_a_tag(
    name: str,
    data: UpdateTag,
    session: Session = Depends(get_session),
    current_user: Optional[User] = Depends(get_current_user),
) -> Tag:
    """
    Update a tag.
    """
    tag = get_tag_by_name(session, name, raise_if_not_exists=True, for_update=True)

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
            details=data,
            user=current_user.username if current_user else None,
        ),
    )
    session.commit()
    session.refresh(tag)
    return tag


@router.get("/tags/{name}/nodes/", response_model=List[NodeMinimumDetail])
def list_nodes_for_a_tag(
    name: str,
    node_type: Optional[NodeType] = None,
    *,
    session: Session = Depends(get_session),
) -> List[NodeMinimumDetail]:
    """
    Find nodes tagged with the tag, filterable by node type.
    """
    statement = select(Tag).where(Tag.name == name).options(joinedload(Tag.nodes))
    tag = session.exec(statement).unique().one_or_none()
    if not tag:
        raise DJException(
            message=f"A tag with name `{name}` does not exist.",
            http_status_code=404,
        )
    if not node_type:
        return sorted([node.current for node in tag.nodes], key=lambda x: x.name)
    return sorted(
        [node.current for node in tag.nodes if node.type == node_type],
        key=lambda x: x.name,
    )
