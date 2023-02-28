"""
Helper functions for API
"""
from http import HTTPStatus

from fastapi import HTTPException
from sqlalchemy.exc import NoResultFound
from sqlmodel import Session, select

from djqs.errors import DJException
from djqs.models.catalog import Catalog
from djqs.models.engine import Engine


def get_catalog(session: Session, name: str) -> Catalog:
    """
    Get a catalog by name
    """
    statement = select(Catalog).where(Catalog.name == name)
    catalog = session.exec(statement).one_or_none()
    if not catalog:
        raise DJException(
            message=f"Catalog with name `{name}` does not exist.",
            http_status_code=404,
        )
    return catalog


def get_engine(session: Session, name: str, version: str) -> Engine:
    """
    Return an Engine instance given an engine name and version
    """
    statement = (
        select(Engine).where(Engine.name == name).where(Engine.version == version)
    )
    try:
        engine = session.exec(statement).one()
    except NoResultFound as exc:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f"Engine not found: `{name}` version `{version}`",
        ) from exc
    return engine
