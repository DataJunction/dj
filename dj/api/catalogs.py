"""
Catalog related APIs.
"""

import logging
from http import HTTPStatus
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.exc import NoResultFound
from sqlmodel import Session, SQLModel, select

from dj.api.engines import EngineInfo, get_engine
from dj.models.catalog import Catalog
from dj.utils import get_session

_logger = logging.getLogger(__name__)
router = APIRouter()


class CatalogInfo(SQLModel):
    """
    Class for catalog creation
    """

    name: str
    engines: List[EngineInfo] = []


def get_catalog(session: Session, name: str) -> Catalog:
    """
    Return a Catalog instance given a catalog name
    """
    statement = select(Catalog).where(Catalog.name == name)
    try:
        catalog = session.exec(statement).one()
    except NoResultFound as exc:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f"Catalog not found: `{name}`",
        ) from exc
    return catalog


@router.get("/catalogs/", response_model=List[CatalogInfo])
def list_catalogs(*, session: Session = Depends(get_session)) -> List[CatalogInfo]:
    """
    List all available catalogs
    """
    return list(session.exec(select(Catalog)))


@router.get("/catalogs/{name}/", response_model=CatalogInfo)
def read_catalog(name: str, *, session: Session = Depends(get_session)) -> CatalogInfo:
    """
    Return a catalog by name
    """
    return get_catalog(session, name)


@router.post("/catalogs/", response_model=CatalogInfo)
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
    except HTTPException:
        pass
    else:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail=f"Catalog already exists: `{data.name}`",
        )

    catalog = Catalog.from_orm(data)
    catalog.engines.extend(
        filter_to_new_engines(
            session=session,
            catalog=catalog,
            create_engines=data.engines,
        ),
    )
    session.add(catalog)
    session.commit()
    session.refresh(catalog)

    return catalog


@router.post("/catalogs/{name}/engines/", response_model=CatalogInfo)
def add_engines_to_catalog(
    name: str,
    data: List[EngineInfo],
    *,
    session: Session = Depends(get_session),
) -> CatalogInfo:
    """
    Attach one or more engines to a catalog
    """
    catalog = get_catalog(session, name)
    catalog.engines.extend(
        filter_to_new_engines(session=session, catalog=catalog, create_engines=data),
    )
    session.add(catalog)
    session.commit()
    session.refresh(catalog)
    return catalog


def filter_to_new_engines(
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
