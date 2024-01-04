"""
Engine related APIs.
"""

from http import HTTPStatus
from typing import List

from fastapi import Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from datajunction_server.database.engine import Engine
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.engines import get_engine
from datajunction_server.models.engine import EngineInfo
from datajunction_server.utils import get_session, get_settings

settings = get_settings()
router = SecureAPIRouter(tags=["engines"])


@router.get("/engines/", response_model=List[EngineInfo])
def list_engines(*, session: Session = Depends(get_session)) -> List[EngineInfo]:
    """
    List all available engines
    """
    return [
        EngineInfo.from_orm(engine)
        for engine in session.execute(select(Engine)).scalars()
    ]


@router.get("/engines/{name}/{version}/", response_model=EngineInfo)
def get_an_engine(
    name: str, version: str, *, session: Session = Depends(get_session)
) -> EngineInfo:
    """
    Return an engine by name and version
    """
    return EngineInfo.from_orm(get_engine(session, name, version))


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

    engine = Engine(
        name=data.name,
        version=data.version,
        uri=data.uri,
        dialect=data.dialect,
    )
    session.add(engine)
    session.commit()
    session.refresh(engine)

    return EngineInfo.from_orm(engine)
