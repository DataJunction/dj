"""
Collection related APIs.
"""

import logging
from http import HTTPStatus
from typing import List

from fastapi import Depends, HTTPException, Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.operators import is_

from datajunction_server.database.collection import Collection
from datajunction_server.database.node import Node
from datajunction_server.database.user import User
from datajunction_server.errors import DJException
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.models.collection import CollectionDetails, CollectionInfo
from datajunction_server.utils import (
    get_and_update_current_user,
    get_session,
    get_settings,
)

_logger = logging.getLogger(__name__)
settings = get_settings()
router = SecureAPIRouter(tags=["collections"])


@router.post(
    "/collections/",
    response_model=CollectionInfo,
    status_code=HTTPStatus.CREATED,
)
async def create_a_collection(
    data: CollectionInfo,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_and_update_current_user),
) -> CollectionInfo:
    """
    Create a Collection
    """
    try:
        await Collection.get_by_name(session, data.name, raise_if_not_exists=True)
    except DJException:
        pass
    else:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail=f"Collection already exists: `{data.name}`",
        )

    collection = Collection(
        name=data.name,
        description=data.description,
        created_by_id=current_user.id,
    )
    session.add(collection)
    await session.commit()
    await session.refresh(collection)

    return CollectionInfo.from_orm(collection)


@router.delete("/collections/{name}", status_code=HTTPStatus.NO_CONTENT)
async def delete_a_collection(
    name: str,
    *,
    session: AsyncSession = Depends(get_session),
):
    """
    Delete a collection
    """
    collection = await Collection.get_by_name(session, name)
    await session.delete(collection)
    await session.commit()
    return Response(status_code=HTTPStatus.NO_CONTENT)


@router.get("/collections/")
async def list_collections(
    *,
    session: AsyncSession = Depends(get_session),
) -> List[CollectionInfo]:
    """
    List all collections
    """
    collections = await session.execute(
        select(Collection).where(is_(Collection.deactivated_at, None)),
    )
    return collections.scalars().all()


@router.get("/collections/{name}")
async def get_collection(
    name: str,
    *,
    session: AsyncSession = Depends(get_session),
) -> CollectionDetails:
    """
    Get a collection and its nodes
    """
    collection = await Collection.get_by_name(
        session,
        name=name,
        raise_if_not_exists=True,
    )
    return collection  # type: ignore


@router.post(
    "/collections/{name}/nodes/",
    status_code=HTTPStatus.NO_CONTENT,
    name="Add Nodes to a Collection",
)
async def add_nodes_to_collection(
    name: str,
    data: List[str],
    *,
    session: AsyncSession = Depends(get_session),
):
    """
    Add one or more nodes to a collection
    """
    collection = await Collection.get_by_name(session, name, raise_if_not_exists=True)
    nodes = await Node.get_by_names(session=session, names=data)
    if not nodes:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f"Cannot add nodes to collection, no nodes found: `{data}`",
        )
    collection.nodes.extend(nodes)  # type: ignore
    session.add(collection)
    await session.commit()
    await session.refresh(collection)
    return Response(status_code=HTTPStatus.NO_CONTENT)


@router.post(
    "/collections/{name}/remove/",
    status_code=HTTPStatus.NO_CONTENT,
    name="Delete Nodes from a Collection",
)
async def delete_nodes_from_collection(
    name: str,
    data: List[str],
    *,
    session: AsyncSession = Depends(get_session),
):
    """
    Delete one or more nodes from a collection
    """
    collection = await Collection.get_by_name(session, name)
    nodes = await Node.get_by_names(session=session, names=data)
    for node in nodes:
        if node in collection.nodes:  # type: ignore
            collection.nodes.remove(node)  # type: ignore
    await session.commit()
    await session.refresh(collection)
    return Response(status_code=HTTPStatus.NO_CONTENT)
