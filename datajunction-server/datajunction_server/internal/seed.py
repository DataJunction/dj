"""
Catalog related APIs.
"""

import logging

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.models.dialect import Dialect
from datajunction_server.database.catalog import Catalog
from datajunction_server.database.engine import Engine
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.utils import get_session, get_settings

LOGGER = logging.getLogger(__name__)
settings = get_settings()
router = SecureAPIRouter(tags=["catalogs"])


async def seed_default_catalogs(session: AsyncSession):
    """
    Seeds two default catalogs:
    * An virtual catalog for nodes that are pure SQL and don't belong in any
    particular catalog. This typically applies to on-the-fly user-defined dimensions.
    * A DJ system catalog that contains all system tables modeled in DJ
    """
    seed_catalogs = [
        settings.seed_setup.virtual_catalog_name,
        settings.seed_setup.system_catalog_name,
    ]
    LOGGER.info("Checking seeded catalogs: %s", seed_catalogs)
    catalogs = await Catalog.get_by_names(session, names=seed_catalogs)
    existing_catalog_names = {catalog.name for catalog in catalogs}
    LOGGER.info("Found existing seeded catalogs: %s", existing_catalog_names)

    if settings.seed_setup.virtual_catalog_name not in existing_catalog_names:
        LOGGER.info("Virtual catalog not found, adding...")
        unknown = Catalog(name=settings.seed_setup.virtual_catalog_name)
        session.add(unknown)
        await session.commit()

    if settings.seed_setup.system_catalog_name not in existing_catalog_names:
        LOGGER.info("System catalog not found, adding...")
        system_engine = await Engine.get_by_name(
            session,
            name=settings.seed_setup.system_engine_name,
        )
        if not system_engine:
            LOGGER.info("System engine not found, adding...")
            system_engine = Engine(
                name=settings.seed_setup.system_engine_name,
                version="",
                uri=settings.reader_db.uri,
                dialect=Dialect.POSTGRES,
            )
            session.add(system_engine)
        system_catalog = Catalog(name=settings.seed_setup.system_catalog_name)
        session.add(system_catalog)
        system_catalog.engines.append(system_engine)
        await session.commit()
        LOGGER.info("Added system catalog and engines")


async def seed_system_nodes(session: AsyncSession = Depends(get_session)):
    """
    Loads system metrics, dimensions etc.
    """
    pass

    # existing_names = {node.name for node in existing}
