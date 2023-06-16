"""
Engine related APIs.
"""

from http import HTTPStatus
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlmodel import Session, select

from dj.api.helpers import get_engine
from dj.models.engine import Engine, EngineInfo
from dj.utils import get_session

router = APIRouter()


@router.get("/engines/", response_model=List[EngineInfo])
def list_engines(*, session: Session = Depends(get_session)) -> List[EngineInfo]:
    """
    List all available engines
    """
    return list(session.exec(select(Engine)))


@router.get("/engines/{name}/{version}/", response_model=EngineInfo)
def get_an_engine(
    name: str, version: str, *, session: Session = Depends(get_session)
) -> EngineInfo:
    """
    Return an engine by name and version
    """
    return get_engine(session, name, version)


@router.post("/engines/", response_model=EngineInfo, status_code=201)
def add_an_engine(
    data: EngineInfo,
    *,
    session: Session = Depends(get_session),
) -> EngineInfo:
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
