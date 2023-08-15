"""
Catalog related APIs.
"""

from typing import List

from sqlmodel import select
import strawberry
from sqlmodel import select
from strawberry.types import Info

from datajunction_server.models.catalog import Catalog, CatalogInfo as _CatalogInfo

from .engines import EngineInfo

@strawberry.experimental.pydantic.type(model=_CatalogInfo, all_fields=True)
class CatalogInfo:
    """
    Class for a CatalogInfo.
    """


def list_catalogs(*,
                  info: Info = None,
                  ) -> List[CatalogInfo]:
    """
    List all available catalogs
    """
    session = info.context["session"]# type: ignore
    return [CatalogInfo.from_pydantic(catalog) for catalog in session.exec(select(Catalog))]#type: ignore