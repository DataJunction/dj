"""
Engine related APIs.
"""
from typing import List
from sqlmodel import select
import strawberry
from sqlmodel import select
from strawberry.types import Info
from datajunction_server.models.engine import Engine, EngineInfo as _EngineInfo, Dialect as _Dialect

Dialect = strawberry.enum(_Dialect)
@strawberry.experimental.pydantic.type(model=_EngineInfo, all_fields=True)
class EngineInfo:
    """
    Class for a EngineInfo.
    """
    
def list_engines(*,
                  info: Info = None,  
                  ) -> List[EngineInfo]:
    """
    List all available engines
    """
    session = info.context["session"] # type: ignore
    return [EngineInfo.from_pydantic(engine) for engine in session.exec(select(Engine))]#type: ignore
