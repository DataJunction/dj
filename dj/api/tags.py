"""
Tag related APIs.
"""

from typing import List, Optional

from fastapi import APIRouter, Depends
from sqlalchemy.orm import joinedload
from sqlmodel import Session, select

from dj.errors import DJException
from dj.models.node import NodeType
from dj.models.tag import CreateTag, Tag, TagOutput, UpdateTag
from dj.utils import get_session

router = APIRouter()


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
        raise DJException(
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
    session.commit()
    session.refresh(tag)
    return tag


@router.patch("/tags/{name}/", response_model=Tag)
def update_a_tag(
    name: str,
    data: UpdateTag,
    session: Session = Depends(get_session),
) -> Tag:
    """
    Update a tag.
    """
    tag = get_tag_by_name(session, name, raise_if_not_exists=True, for_update=True)

    if data.description:
        tag.description = data.description
    if data.tag_metadata:
        tag.tag_metadata = data.tag_metadata
    session.add(tag)
    session.commit()
    session.refresh(tag)
    return tag


@router.get("/tags/{name}/nodes/", response_model=List[str])
def list_nodes_for_a_tag(
    name: str,
    node_type: Optional[NodeType] = None,
    *,
    session: Session = Depends(get_session),
) -> List[str]:
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
        return sorted([node.name for node in tag.nodes])
    return sorted([node.name for node in tag.nodes if node.type == node_type])
