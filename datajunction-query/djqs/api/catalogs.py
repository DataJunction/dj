"""
Catalog related APIs.
"""

import logging
from http import HTTPStatus
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session, select

from djqs.api.engines import EngineInfo
from djqs.api.helpers import get_catalog, get_engine
from djqs.exceptions import DJException
from djqs.models.catalog import Catalog, CatalogInfo
from djqs.models.engine import BaseEngineInfo
from djqs.utils import get_session

_logger = logging.getLogger(__name__)
get_router = APIRouter(tags=["Catalogs & Engines"])
post_router = APIRouter(tags=["Catalogs & Engines - Dynamic Configuration"])


@get_router.get("/catalogs/", response_model=List[CatalogInfo])
def list_catalogs(*, session: Session = Depends(get_session)) -> List[CatalogInfo]:
    """
    List all available catalogs
    """
    return list(session.exec(select(Catalog)))


@get_router.get("/catalogs/{name}/", response_model=CatalogInfo)
def read_catalog(name: str, *, session: Session = Depends(get_session)) -> CatalogInfo:
    """
    Return a catalog by name
    """
    return get_catalog(session, name)


@post_router.post("/catalogs/", response_model=CatalogInfo, status_code=201)
def add_catalog(
    data: CatalogInfo,
    *,
    session: Session = Depends(get_session),
) -> CatalogInfo:
    """
    Add a Catalog
    """
    try:
        get_catalog(session, data.name)
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


@post_router.post(
    "/catalogs/{name}/engines/",
    response_model=CatalogInfo,
    status_code=201,
)
def add_engines_to_catalog(
    name: str,
    data: List[BaseEngineInfo],
    *,
    session: Session = Depends(get_session),
) -> CatalogInfo:
    """
    Attach one or more engines to a catalog
    """
    catalog = get_catalog(session, name)
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
