"""
Catalog related APIs.
"""

import logging
from http import HTTPStatus
from typing import List

from fastapi import Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from datajunction_server.api.engines import EngineInfo
from datajunction_server.api.helpers import get_catalog_by_name
from datajunction_server.database.catalog import Catalog
from datajunction_server.database.engine import Engine
from datajunction_server.errors import DJException
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.engines import get_engine
from datajunction_server.models.catalog import CatalogInfo
from datajunction_server.utils import get_session, get_settings

_logger = logging.getLogger(__name__)
settings = get_settings()
router = SecureAPIRouter(tags=["catalogs"])

UNKNOWN_CATALOG_ID = 0


@router.get("/catalogs/", response_model=List[CatalogInfo])
async def list_catalogs(
    *,
    session: AsyncSession = Depends(get_session),
) -> List[CatalogInfo]:
    """
    List all available catalogs
    """
    statement = select(Catalog).options(joinedload(Catalog.engines))
    return [
        CatalogInfo.from_orm(catalog)
        for catalog in (await session.execute(statement)).unique().scalars()
    ]


@router.get("/catalogs/{name}/", response_model=CatalogInfo, name="Get a Catalog")
async def get_catalog(
    name: str,
    *,
    session: AsyncSession = Depends(get_session),
) -> CatalogInfo:
    """
    Return a catalog by name
    """
    return await get_catalog_by_name(session, name)


@router.post(
    "/catalogs/",
    response_model=CatalogInfo,
    status_code=201,
    name="Add A Catalog",
)
async def add_catalog(
    data: CatalogInfo,
    *,
    session: AsyncSession = Depends(get_session),
) -> CatalogInfo:
    """
    Add a Catalog
    """
    try:
        await get_catalog_by_name(session, data.name)
    except DJException:
        pass
    else:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail=f"Catalog already exists: `{data.name}`",
        )

    catalog = Catalog(
        name=data.name,
        engines=[
            Engine(
                name=engine.name,
                version=engine.version,
                uri=engine.uri,
                dialect=engine.dialect,
            )
            for engine in data.engines  # type: ignore
        ],
    )
    catalog.engines.extend(
        await list_new_engines(
            session=session,
            catalog=catalog,
            create_engines=data.engines,  # type: ignore
        ),
    )
    session.add(catalog)
    await session.commit()
    await session.refresh(catalog, ["engines"])

    return CatalogInfo.from_orm(catalog)


@router.post(
    "/catalogs/{name}/engines/",
    response_model=CatalogInfo,
    status_code=201,
    name="Add Engines to a Catalog",
)
async def add_engines_to_catalog(
    name: str,
    data: List[EngineInfo],
    *,
    session: AsyncSession = Depends(get_session),
) -> CatalogInfo:
    """
    Attach one or more engines to a catalog
    """
    catalog = await get_catalog_by_name(session, name)
    catalog.engines.extend(
        await list_new_engines(session=session, catalog=catalog, create_engines=data),
    )
    session.add(catalog)
    await session.commit()
    await session.refresh(catalog)
    return CatalogInfo.from_orm(catalog)


async def list_new_engines(
    session: AsyncSession,
    catalog: Catalog,
    create_engines: List[EngineInfo],
) -> List[Engine]:
    """
    Filter to engines that are not already set on a catalog
    """
    new_engines = []
    for engine_ref in create_engines:
        already_set = False
        engine = await get_engine(session, engine_ref.name, engine_ref.version)
        for set_engine in catalog.engines:
            if engine.name == set_engine.name and engine.version == set_engine.version:
                already_set = True
        if not already_set:
            new_engines.append(engine)
    return new_engines


async def default_catalog(session: AsyncSession = Depends(get_session)):
    """
    Loads a default catalog for nodes that are pure SQL and don't belong in any
    particular catalog. This typically applies to on-the-fly user-defined dimensions.
    """
    statement = select(Catalog).filter(Catalog.id == UNKNOWN_CATALOG_ID)
    catalogs = (await session.execute(statement)).all()
    if not catalogs:
        unknown = Catalog(
            id=UNKNOWN_CATALOG_ID,
            name="unknown",
        )
        session.add(unknown)
        await session.commit()
