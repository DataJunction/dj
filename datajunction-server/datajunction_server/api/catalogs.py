"""
Catalog related APIs.
"""

import logging
from http import HTTPStatus
from typing import List

from fastapi import Depends, HTTPException
from sqlmodel import Session, select

from datajunction_server.api.engines import EngineInfo
from datajunction_server.api.helpers import get_catalog_by_name
from datajunction_server.errors import DJException
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.engines import get_engine
from datajunction_server.models.catalog import Catalog, CatalogInfo
from datajunction_server.utils import get_session, get_settings

_logger = logging.getLogger(__name__)
settings = get_settings()
router = SecureAPIRouter(tags=["catalogs"])

UNKNOWN_CATALOG_ID = 0


@router.get("/catalogs/", response_model=List[CatalogInfo])
def list_catalogs(*, session: Session = Depends(get_session)) -> List[CatalogInfo]:
    """
    List all available catalogs
    """
    return list(session.exec(select(Catalog)))


@router.get("/catalogs/{name}/", response_model=CatalogInfo, name="Get a Catalog")
def get_catalog(name: str, *, session: Session = Depends(get_session)) -> CatalogInfo:
    """
    Return a catalog by name
    """
    return get_catalog_by_name(session, name)


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
def add_engines_to_catalog(
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


def default_catalog(session: Session = Depends(get_session)):
    """
    Loads a default catalog for nodes that are pure SQL and don't belong in any
    particular catalog. This typically applies to on-the-fly user-defined dimensions.
    """
    statement = select(Catalog).filter(Catalog.id == UNKNOWN_CATALOG_ID)
    catalogs = session.exec(statement).all()
    if not catalogs:
        unknown = Catalog(
            id=UNKNOWN_CATALOG_ID,
            name="unknown",
        )
        session.add(unknown)
        session.commit()
