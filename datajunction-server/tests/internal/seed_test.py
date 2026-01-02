import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.catalog import Catalog
from datajunction_server.database.engine import Engine
from datajunction_server.internal.seed import seed_default_catalogs
from datajunction_server.utils import get_settings


@pytest.mark.asyncio
async def test_seed_default_catalogs_adds_missing_catalogs(clean_session: AsyncSession):
    """
    Test that seeding adds missing catalogs.
    Uses clean_session because this test creates catalogs from scratch.
    """
    session = clean_session
    settings = get_settings()

    # Run the seeding function
    await seed_default_catalogs(session)

    # Check that the catalogs were added
    catalogs = await Catalog.get_by_names(
        session,
        names=[
            settings.seed_setup.virtual_catalog_name,
            settings.seed_setup.system_catalog_name,
        ],
    )
    catalog_names = [catalog.name for catalog in catalogs]
    assert settings.seed_setup.virtual_catalog_name in catalog_names
    assert settings.seed_setup.system_catalog_name in catalog_names


@pytest.mark.asyncio
async def test_seed_default_catalogs_noop_if_both_exist(clean_session: AsyncSession):
    """
    Test that seeding is a no-op if both catalogs already exist.
    Uses clean_session because this test creates catalogs from scratch.
    """
    session = clean_session
    settings = get_settings()
    virtual_catalog = Catalog(
        name=settings.seed_setup.virtual_catalog_name,
        engines=[Engine(name="virtual_engine", version="")],
    )
    system_catalog = Catalog(
        name=settings.seed_setup.system_catalog_name,
        engines=[Engine(name="system_engine", version="")],
    )
    session.add(virtual_catalog)
    session.add(system_catalog)
    await session.commit()

    # Run the seeding function
    await seed_default_catalogs(session)

    # Check that no new catalogs were added
    catalogs = await Catalog.get_by_names(
        session,
        names=[
            settings.seed_setup.virtual_catalog_name,
            settings.seed_setup.system_catalog_name,
        ],
    )
    assert len(catalogs) == 2


@pytest.mark.asyncio
async def test_seed_default_catalogs_virtual_exists(clean_session: AsyncSession):
    """
    Test that seeding adds missing system catalog when virtual exists.
    Uses clean_session because this test creates catalogs from scratch.
    """
    session = clean_session
    settings = get_settings()
    virtual_catalog = Catalog(
        name=settings.seed_setup.virtual_catalog_name,
        engines=[Engine(name="virtual_engine", version="")],
    )
    session.add(virtual_catalog)
    await session.commit()

    # Run the seeding function
    await seed_default_catalogs(session)

    # Check that only the system catalog was added
    catalogs = await Catalog.get_by_names(
        session,
        names=[
            settings.seed_setup.virtual_catalog_name,
            settings.seed_setup.system_catalog_name,
        ],
    )
    assert len(catalogs) == 2


@pytest.mark.asyncio
async def test_seed_default_catalogs_system_exists(clean_session: AsyncSession):
    """
    Test that seeding adds missing virtual catalog when system exists.
    Uses clean_session because this test creates catalogs from scratch.
    """
    session = clean_session
    settings = get_settings()
    system_catalog = Catalog(
        name=settings.seed_setup.system_catalog_name,
        engines=[Engine(name="system_engine", version="")],
    )
    session.add(system_catalog)
    await session.commit()

    # Run the seeding function
    await seed_default_catalogs(session)

    # Check that only the virtual catalog was added
    catalogs = await Catalog.get_by_names(
        session,
        names=[
            settings.seed_setup.virtual_catalog_name,
            settings.seed_setup.system_catalog_name,
        ],
    )
    assert len(catalogs) == 2
