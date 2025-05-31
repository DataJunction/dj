"""
Engine related queries.
"""

from typing import List

from sqlalchemy import select
from strawberry.types import Info

from datajunction_server.models.dialect import DialectRegistry
from datajunction_server.api.graphql.scalars.catalog_engine import Engine, DialectInfo
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


async def list_dialects(
    *,
    info: Info = None,
) -> List[DialectInfo]:
    """
    List all supported dialects
    """
    return [
        DialectInfo(  # type: ignore
            name=dialect,
            plugin_class=plugin.__name__,
        )
        for dialect, plugin in DialectRegistry._registry.items()
    ]
