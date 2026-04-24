"""
Application healthchecks.
"""

from typing import List

from fastapi import APIRouter, Depends
from pydantic.main import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

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


async def database_health(session: AsyncSession) -> HealthcheckStatus:
    """
    The status of the database.
    """
    try:
        result = (await session.execute(select(1))).one()
        health_status = (
            HealthcheckStatus.OK if result == (1,) else HealthcheckStatus.FAILED
        )
        return health_status
    except Exception:  # pragma: no cover
        return HealthcheckStatus.FAILED  # pragma: no cover


@router.get("/health/", response_model=List[HealthCheck])
async def health_check(
    session: AsyncSession = Depends(get_session),
) -> List[HealthCheck]:
    """
    Healthcheck for services.
    """
    return [
        HealthCheck(
            name="database",
            status=await database_health(session),
        ),
    ]


class ServerSettings(BaseModel):
    """
    Non-sensitive server configuration.
    """

    default_catalog: str | None
    virtual_catalog: str


@router.get("/settings/", response_model=ServerSettings)
async def server_settings() -> ServerSettings:
    """
    Returns non-sensitive server configuration.
    """
    return ServerSettings(
        default_catalog=settings.seed_setup.default_catalog_name,
        virtual_catalog=settings.seed_setup.virtual_catalog_name,
    )
