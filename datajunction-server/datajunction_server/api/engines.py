"""
Engine related APIs.
"""

from http import HTTPStatus
from typing import List

from fastapi import Depends, HTTPException
from sqlmodel import Session, select

from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.engines import get_engine
from datajunction_server.models.engine import Engine, EngineInfo
from datajunction_server.utils import get_session, get_settings

settings = get_settings()
router = SecureAPIRouter(tags=["engines"])


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


@router.post(
    "/engines/",
    response_model=EngineInfo,
    status_code=201,
    name="Add An Engine",
)
def add_engine(
    data: EngineInfo,
    *,
    session: Session = Depends(get_session),
) -> EngineInfo:
    """
    Add a new engine
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
