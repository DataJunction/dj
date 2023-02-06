"""
Engine related APIs.
"""

from http import HTTPStatus
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.exc import NoResultFound
from sqlmodel import Session, SQLModel, select

from dj.models.engine import Engine
from dj.utils import get_session

router = APIRouter()


class EngineInfo(SQLModel):
    """
    Class for engine creation
    """

    name: str
    version: str


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


@router.get("/engines/", response_model=List[EngineInfo])
def list_engines(*, session: Session = Depends(get_session)) -> List[EngineInfo]:
    """
    List all available engines
    """
    return list(session.exec(select(Engine)))


@router.get("/engines/{name}/{version}/", response_model=EngineInfo)
def list_engine(
    name: str, version: str, *, session: Session = Depends(get_session)
) -> EngineInfo:
    """
    Return an engine by name and version
    """
    return get_engine(session, name, version)


@router.post("/engines/", response_model=EngineInfo)
def add_engine(
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
