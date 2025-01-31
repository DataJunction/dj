"""
Engine related queries.
"""

from typing import List

from sqlalchemy import select
from strawberry.types import Info

from datajunction_server.api.graphql.scalars.catalog_engine import Engine
from datajunction_server.database.engine import Engine as DBEngine


async def list_engines(
    *,
    info: Info = None,
) -> List[Engine]:
    """
    List all available engines
    """
    session = info.context["session"]  # type: ignore
    return [
        Engine.from_pydantic(engine)  # type: ignore #pylint: disable=E1101
        for engine in (await session.execute(select(DBEngine))).scalars().all()
    ]
