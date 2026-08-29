#!/usr/bin/env python
"""
Script to populate the template database with all examples.
Run as a subprocess to avoid event loop conflicts with pytest-asyncio.

Usage: python populate_template.py <database_url>
"""

# ruff: noqa: E402 - env vars must be set before importing datajunction_server modules

import asyncio
import os
import sys
from http.client import HTTPException

from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.pool import StaticPool

# Add tests directory to path for examples import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from examples import COLUMN_MAPPINGS, EXAMPLES, SERVICE_SETUP
from helpers.template_app import configure_database_env, template_app_client

# Get database URL from command line
template_db_url = sys.argv[1]
reader_db_url = configure_database_env(template_db_url)

# Imported after configure_database_env so they read the settings above.
from datajunction_server.api.attributes import default_attribute_types
from datajunction_server.database.base import Base
from datajunction_server.database.user import User
from datajunction_server.internal.seed import seed_default_catalogs
from datajunction_server.models.user import OAuthProvider


# Helper functions (copied from conftest.py)
async def post_and_raise_if_error(client: AsyncClient, endpoint: str, json: dict):
    """Post the payload to the client and raise if there's an error"""
    response = await client.post(endpoint, json=json)
    if response.status_code not in (200, 201):
        raise HTTPException(response.text)


async def post_and_dont_raise_if_error(client: AsyncClient, endpoint: str, json: dict):
    """Post the payload to the client and don't raise if there's an error"""
    await client.post(endpoint, json=json)


async def load_examples_in_client(
    client: AsyncClient,
    examples_to_load: list[str] | None = None,
):
    """Load the DJ client with examples"""
    # Basic service setup always has to be done
    for endpoint, json in SERVICE_SETUP:
        await post_and_dont_raise_if_error(
            client=client,
            endpoint="http://test" + endpoint,
            json=json,
        )

    # Load only the selected examples if any are specified
    if examples_to_load is not None:
        for example_name in examples_to_load:
            for endpoint, json in EXAMPLES[example_name]:
                await post_and_raise_if_error(
                    client=client,
                    endpoint=endpoint,
                    json=json,
                )
        return client

    # Load all examples if none are specified
    for example_name, examples in EXAMPLES.items():
        for endpoint, json in examples:
            await post_and_raise_if_error(
                client=client,
                endpoint=endpoint,
                json=json,
            )
    return client


async def create_default_user(session: AsyncSession) -> User:
    """Create the default DJ user."""
    new_user = User(
        username="dj",
        password="dj",
        email="dj@datajunction.io",
        name="DJ",
        oauth_provider=OAuthProvider.BASIC,
        is_admin=False,
    )
    existing_user = await User.get_by_username(session, new_user.username)
    if not existing_user:
        session.add(new_user)
        await session.commit()
        user = new_user
    else:
        user = existing_user
    await session.refresh(user)
    return user


async def main():
    print(f"Populating template database: {template_db_url}")

    engine = create_async_engine(url=template_db_url, poolclass=StaticPool)
    async with engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm;"))
        await conn.run_sync(Base.metadata.create_all)
    print("Tables created")
    await engine.dispose()

    async with template_app_client(
        template_db_url,
        reader_db_url,
        column_mappings=COLUMN_MAPPINGS,
    ) as (session, test_client):
        await default_attribute_types(session)
        await seed_default_catalogs(session)
        await create_default_user(session)
        print("Default data seeded")

        print("Loading examples via HTTP client...")
        await load_examples_in_client(test_client, None)  # None = load ALL examples
        print("Examples loaded")

    print("Template database populated successfully!")


if __name__ == "__main__":
    asyncio.run(main())
