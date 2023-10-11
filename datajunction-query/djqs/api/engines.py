"""
Engine related APIs.
"""

from http import HTTPStatus
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session, select

from djqs.api.helpers import get_engine
from djqs.models.engine import BaseEngineInfo, Engine, EngineInfo
from djqs.utils import get_session

get_router = APIRouter(tags=["Catalogs & Engines"])
post_router = APIRouter(tags=["Catalogs & Engines - Dynamic Configuration"])


@get_router.get("/engines/", response_model=List[EngineInfo])
def list_engines(*, session: Session = Depends(get_session)) -> List[EngineInfo]:
    """
    List all available engines
    """
    return list(session.exec(select(Engine)))


@get_router.get("/engines/{name}/{version}/", response_model=BaseEngineInfo)
def list_engine(
    name: str, version: str, *, session: Session = Depends(get_session)
) -> BaseEngineInfo:
    """
    Return an engine by name and version
    """
    return get_engine(session, name, version)


@post_router.post("/engines/", response_model=BaseEngineInfo, status_code=201)
def add_engine(
    data: EngineInfo,
    *,
    session: Session = Depends(get_session),
) -> BaseEngineInfo:
    """
    Add an Engine
    """
    try:
        get_engine(session, data.name, data.version)
    except HTTPException:
        pass
    else:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail=f"Engine already exists: `{data.name}` version `{data.version}`",
        )

    engine = Engine.from_orm(data)
    session.add(engine)
    session.commit()
    session.refresh(engine)

    return engine
