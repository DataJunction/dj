"""
Catalog related APIs.
"""

from typing import List

import strawberry
from sqlalchemy import select
from strawberry.types import Info

from datajunction_server.models.catalog import CatalogInfo as _CatalogInfo

from ...database.catalog import Catalog
from .engines import EngineInfo  # pylint: disable=W0611


@strawberry.experimental.pydantic.type(model=_CatalogInfo, all_fields=True)
class CatalogInfo:  # pylint: disable=R0903
    """
    Class for a CatalogInfo.
    """


async def list_catalogs(
    *,
    info: Info = None,
) -> List[CatalogInfo]:
    """
    List all available catalogs
    """
    session = info.context["session"]  # type: ignore
    return [
        CatalogInfo.from_pydantic(_CatalogInfo.from_orm(catalog))  # type: ignore #pylint: disable=E1101
        for catalog in (await session.execute(select(Catalog))).scalars().all()
    ]
