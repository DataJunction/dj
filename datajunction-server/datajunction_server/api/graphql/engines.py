"""
Engine related APIs.
"""
from typing import List

import strawberry
from sqlalchemy import select
from strawberry.types import Info

from datajunction_server.database.engine import Engine
from datajunction_server.models.engine import Dialect as _Dialect
from datajunction_server.models.engine import EngineInfo as _EngineInfo

Dialect = strawberry.enum(_Dialect)


@strawberry.experimental.pydantic.type(model=_EngineInfo, all_fields=True)
class EngineInfo:  # pylint: disable=R0903
    """
    Class for a EngineInfo.
    """


async def list_engines(
    *,
    info: Info = None,
) -> List[EngineInfo]:
    """
    List all available engines
    """
    session = info.context["session"]  # type: ignore
    return [
        EngineInfo.from_pydantic(_EngineInfo.from_orm(engine))  # type: ignore #pylint: disable=E1101
        for engine in (await session.execute(select(Engine))).scalars().all()
    ]
