"""
Seed module for seeding default catalogs and more in the metadata database.
"""

import logging

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.models.dialect import Dialect
from datajunction_server.database.catalog import Catalog
from datajunction_server.database.engine import Engine
from datajunction_server.utils import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


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
    logger.info("Checking seeded catalogs: %s", seed_catalogs)
    catalogs = await Catalog.get_by_names(session, names=seed_catalogs)
    existing_catalog_names = {catalog.name for catalog in catalogs}
    logger.info("Found existing seeded catalogs: %s", existing_catalog_names)

    if settings.seed_setup.virtual_catalog_name not in existing_catalog_names:
        logger.info("Virtual catalog not found, adding...")
        unknown = Catalog(name=settings.seed_setup.virtual_catalog_name)
        session.add(unknown)
        await session.commit()

    if settings.seed_setup.system_catalog_name not in existing_catalog_names:
        logger.info("System catalog not found, adding...")
        system_engine = await Engine.get_by_name(
            session,
            name=settings.seed_setup.system_engine_name,
        )
        if not system_engine:  # pragma: no cover
            logger.info("System engine not found, adding...")
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

        logger.info("Added system catalog and engines")
