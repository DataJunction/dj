"""Helper functions for engines."""
from http import HTTPStatus

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.engine import Engine


async def get_engine(session: AsyncSession, name: str, version: str) -> Engine:
    """
    Return an Engine instance given an engine name and version
    """
    statement = (
        select(Engine)
        .where(Engine.name == name)
        .where(Engine.version == (version or ""))
    )
    try:
        engine = (await session.execute(statement)).scalar_one()
    except NoResultFound as exc:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f"Engine not found: `{name}` version `{version}`",
        ) from exc
    return engine
