"""
Catalog related APIs.
"""

import logging
from http import HTTPStatus
from typing import List

from fastapi import Depends, HTTPException
from sqlmodel import Session, select

from datajunction_server.api.engines import EngineInfo, get_engine
from datajunction_server.api.helpers import get_catalog_by_name, get_catalog_by_name_async
from datajunction_server.errors import DJException
from datajunction_server.internal.authentication.http import SecureAPIRouter
from datajunction_server.models.catalog import Catalog, CatalogInfo
from datajunction_server.utils import get_session, get_settings, get_async_session
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

_logger = logging.getLogger(__name__)
settings = get_settings()
router = SecureAPIRouter(tags=["catalogs"])


@router.get("/catalogs/", response_model=List[CatalogInfo])
async def list_catalogs(
    async_session: AsyncSession = Depends(get_async_session)
) -> List[CatalogInfo]:
    """
    List all available catalogs
    """
    results = await async_session.execute(
        select(Catalog).options(selectinload(Catalog.engines))
    )
    results = results.scalars().all()
    return results


@router.get("/catalogs/{name}/", response_model=CatalogInfo, name="Get a Catalog")
async def get_catalog(
    name: str,
    *,
    async_session: AsyncSession = Depends(get_async_session)
) -> CatalogInfo:
    """
    Return a catalog by name
    """
    catalog = await get_catalog_by_name_async(async_session, name)
    return catalog


@router.post(
    "/catalogs/",
    response_model=CatalogInfo,
    status_code=201,
    name="Add A Catalog",
)
def add_catalog(
    data: CatalogInfo,
    *,
    session: Session = Depends(get_session),
) -> CatalogInfo:
    """
    Add a Catalog
    """
    try:
        get_catalog_by_name(session, data.name)
    except DJException:
        pass
    else:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail=f"Catalog already exists: `{data.name}`",
        )

    catalog = Catalog.from_orm(data)
    catalog.engines.extend(
        list_new_engines(
            session=session,
            catalog=catalog,
            create_engines=data.engines,
        ),
    )
    session.add(catalog)
    session.commit()
    session.refresh(catalog)

    return catalog


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
    session: Session = Depends(get_session),
) -> CatalogInfo:
    """
    Attach one or more engines to a catalog
    """
    catalog = get_catalog_by_name(session, name)
    catalog.engines.extend(
        list_new_engines(session=session, catalog=catalog, create_engines=data),
    )
    session.add(catalog)
    session.commit()
    session.refresh(catalog)
    return catalog


def list_new_engines(
    session: Session,
    catalog: Catalog,
    create_engines: List[EngineInfo],
) -> List[EngineInfo]:
    """
    Filter to engines that are not already set on a catalog
    """
    new_engines = []
    for engine_ref in create_engines:
        already_set = False
        engine = get_engine(session, engine_ref.name, engine_ref.version)
        for set_engine in catalog.engines:
            if engine.name == set_engine.name and engine.version == set_engine.version:
                already_set = True
        if not already_set:
            new_engines.append(engine)
    return new_engines
