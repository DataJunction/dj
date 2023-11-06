"""
Catalog related APIs.
"""

from typing import List

import strawberry
from sqlmodel import select
from strawberry.types import Info

from datajunction_server.models.catalog import Catalog
from datajunction_server.models.catalog import CatalogInfo as _CatalogInfo

from .engines import EngineInfo  # pylint: disable=W0611


@strawberry.experimental.pydantic.type(model=_CatalogInfo, all_fields=True)
class CatalogInfo:  # pylint: disable=R0903
    """
    Class for a CatalogInfo.
    """


def list_catalogs(
    *,
    info: Info = None,
) -> List[CatalogInfo]:
    """
    List all available catalogs
    """
    session = info.context["session"]  # type: ignore
    catalogs = session.exec(select(Catalog)).all()
    return [
        CatalogInfo(
            name=catalog.name,
        ) for catalog in catalogs
    ]
