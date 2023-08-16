"""
Engine related APIs.
"""
from typing import List

import strawberry
from sqlmodel import select
from strawberry.types import Info

from datajunction_server.models.engine import Dialect as _Dialect
from datajunction_server.models.engine import Engine
from datajunction_server.models.engine import EngineInfo as _EngineInfo

Dialect = strawberry.enum(_Dialect)


@strawberry.experimental.pydantic.type(model=_EngineInfo, all_fields=True)
class EngineInfo:  # pylint: disable=R0903
    """
    Class for a EngineInfo.
    """


def list_engines(
    *,
    info: Info = None,
) -> List[EngineInfo]:
    """
    List all available engines
    """
    session = info.context["session"]  # type: ignore
    return [
        EngineInfo.from_pydantic(engine)  # type: ignore #pylint: disable=E1101
        for engine in session.exec(select(Engine))
    ]
