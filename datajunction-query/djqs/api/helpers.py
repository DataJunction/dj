"""
Helper functions for API
"""
from http import HTTPStatus
from typing import Any, Dict, List, Optional

from fastapi import HTTPException
from sqlalchemy import inspect
from sqlalchemy.exc import NoResultFound, NoSuchTableError, OperationalError
from sqlmodel import Session, create_engine, select

from djqs.exceptions import DJException, DJTableNotFound
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


def get_columns(
    table: str,
    schema: Optional[str],
    catalog: Optional[str],
    uri: Optional[str],
    extra_params: Optional[Dict[str, Any]],
) -> List[Dict[str, str]]:  # pragma: no cover
    """
    Return all columns in a given table.
    """
    if not uri:
        raise DJException("Cannot retrieve columns without a uri")

    engine = create_engine(uri, **extra_params)
    try:
        inspector = inspect(engine)
        column_metadata = inspector.get_columns(
            table,
            schema=schema,
        )
    except NoSuchTableError as exc:  # pylint: disable=broad-except
        raise DJTableNotFound(
            message=f"No such table `{table}` in schema `{schema}` in catalog `{catalog}`",
            http_status_code=404,
        ) from exc
    except OperationalError as exc:
        if "unknown database" in str(exc):
            raise DJException(message=f"No such schema `{schema}`") from exc
        raise

    return [
        {"name": column["name"], "type": column["type"].python_type.__name__.upper()}
        for column in column_metadata
    ]
