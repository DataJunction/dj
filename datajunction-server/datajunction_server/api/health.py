"""
Application healthchecks.
"""

import enum
from typing import List

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlmodel import Session, SQLModel

from datajunction_server.utils import get_session, get_settings

settings = get_settings()

router = APIRouter(tags=["health"])


class HealthcheckStatus(str, enum.Enum):
    """
    Possible health statuses.
    """

    OK = "ok"
    FAILED = "failed"


class HealthCheck(SQLModel):
    """
    A healthcheck response.
    """

    name: str
    status: HealthcheckStatus


async def database_health(session: Session) -> HealthcheckStatus:
    """
    The status of the database.
    """
    try:
        result = session.execute(select(1)).one()
        health_status = (
            HealthcheckStatus.OK if result == (1,) else HealthcheckStatus.FAILED
        )
        return health_status
    except Exception:  # pylint: disable=broad-except
        return HealthcheckStatus.FAILED


@router.get("/health/", response_model=List[HealthCheck])
async def health_check(session: Session = Depends(get_session)) -> List[HealthCheck]:
    """
    Healthcheck for services.
    """
    return [
        HealthCheck(
            name="database",
            status=await database_health(session),
        ),
    ]
