"""
Collection related APIs.
"""
import logging
from http import HTTPStatus
from typing import List, Any

from fastapi import Depends, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql.operators import is_

from datajunction_server.api.helpers import (
    get_collection_by_name,
)
from datajunction_server.database.collection import Collection
from datajunction_server.database.node import Node
from datajunction_server.errors import DJException
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.models.collection import CollectionInfo, CreateCollection
from datajunction_server.utils import get_session, get_settings

_logger = logging.getLogger(__name__)
settings = get_settings()
router = SecureAPIRouter(tags=["collections"])


@router.post("/collections/", response_model=CollectionInfo, status_code=201)
async def create_a_collection(
    data: CreateCollection,
    *,
    session: AsyncSession = Depends(get_session),
) -> CollectionInfo:
    """
    Create a Collection
    """
    try:
        await get_collection_by_name(session, data.name)
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
    )
    session.add(collection)
    await session.commit()
    await session.refresh(collection)

    return CollectionInfo.from_orm(collection)

@router.delete("/collections/{name}", status_code=HTTPStatus.OK)
async def delete_a_collection(
     name: str,
    *,
    session: AsyncSession = Depends(get_session),
) -> JSONResponse:
    """
    Delete a collection
    """
    collection = await get_collection_by_name(session, name)
    await session.delete(collection)
    await session.commit()
    return {"message": f"Collection `{name}` successfully deleted"}

@router.get("/collections/", response_model=Any)
async def list_collections(
    *,
    session: AsyncSession = Depends(get_session),
) -> Any:
    """
    List all collections
    """
    collections = await session.execute(select(Collection).where(is_(Collection.deactivated_at, None)))
    return collections.scalars().all()

@router.get("/collections/{name}", response_model=Any)
async def get_collection(
    name: str,
    *,
    session: AsyncSession = Depends(get_session),
) -> Any:
    """
    Get a collection and it's nodes
    """
    results = await session.execute(select(Collection).where(Collection.name==name))
    collections = results.scalars().all()
    if not collections:
        raise HTTPException(status_code=404, detail="Collection not found")

    return collections[0]

@router.post(
    "/collections/{name}/nodes/",
    response_model=CollectionInfo,
    status_code=201,
    name="Add Nodes to a Collection",
)
async def add_nodes_to_collection(
    name: str,
    data: List[str],
    *,
    session: AsyncSession = Depends(get_session),
) -> CollectionInfo:
    """
    Add one or more nodes to a collection
    """
    collection = await get_collection_by_name(session, name)
    nodes = await Node.get_by_names(session=session, names=data)
    if not nodes:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f"Cannot add nodes to collection, no nodes found: `{data}`",
        )
    collection.nodes.extend(nodes)
    session.add(collection)
    await session.commit()
    await session.refresh(collection)
    return CollectionInfo.from_orm(collection)
