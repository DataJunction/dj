"""
Catalog related queries.
"""

from typing import List

from sqlalchemy import select
from strawberry.types import Info

from datajunction_server.api.graphql.scalars.catalog_engine import Catalog
from datajunction_server.database.catalog import Catalog as DBCatalog


async def list_catalogs(
    *,
    info: Info = None,
) -> List[Catalog]:
    """
    List all available catalogs
    """
    session = info.context["session"]  # type: ignore
    return [
        Catalog.from_pydantic(catalog)  # type: ignore
        for catalog in (await session.execute(select(DBCatalog))).scalars().all()
    ]
