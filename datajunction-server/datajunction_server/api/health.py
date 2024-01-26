"""
Application healthchecks.
"""

from typing import List

from fastapi import APIRouter, Depends
from pydantic.main import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from datajunction_server.enum import StrEnum
from datajunction_server.utils import get_session, get_settings

settings = get_settings()

router = APIRouter(tags=["health"])


class HealthcheckStatus(StrEnum):
    """
    Possible health statuses.
    """

    OK = "ok"
    FAILED = "failed"


class HealthCheck(BaseModel):
    """
    A healthcheck response.
    """

    name: str
    status: HealthcheckStatus


def database_health(session: Session) -> HealthcheckStatus:
    """
    The status of the database.
    """
    try:
        result = session.execute(select(1)).one()
        health_status = (
            HealthcheckStatus.OK if result == (1,) else HealthcheckStatus.FAILED
        )
        return health_status
    except Exception:  # pylint: disable=broad-except  # pragma: no cover
        return HealthcheckStatus.FAILED  # pragma: no cover


@router.get("/health/", response_model=List[HealthCheck])
def health_check(
    session: Session = Depends(get_session),
) -> List[HealthCheck]:
    """
    Healthcheck for services.
    """
    return [
        HealthCheck(
            name="database",
            status=database_health(session),
        ),
    ]
