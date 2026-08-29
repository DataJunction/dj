#!/usr/bin/env python
"""
Script to populate the template database with all examples.
Run as a subprocess to avoid event loop conflicts with pytest-asyncio.

Usage: python populate_template.py <database_url> [EXAMPLE_NAME ...]

With no example names every example is loaded, which is what the shared
all-examples template wants. Naming a subset builds a smaller template for a
module that only needs part of the fixture data.
"""

# ruff: noqa: E402 - env vars must be set before importing datajunction_server modules

import asyncio
import os
import sys
from http.client import HTTPException

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

# Add tests directory to path for examples import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import examples as examples_module
from examples import COLUMN_MAPPINGS, EXAMPLES, SERVICE_SETUP
from helpers.template_app import (
    configure_database_env,
    create_schema,
    template_app_client,
)

# Get database URL from command line
template_db_url = sys.argv[1]
examples_to_load = sys.argv[2:] or None
reader_db_url = configure_database_env(template_db_url)

# Imported after configure_database_env so they read the settings above.
from datajunction_server.api.attributes import default_attribute_types
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
            # Most names are EXAMPLES keys, but some fixture sets are only
            # module-level constants in tests/examples.py.
            example = EXAMPLES.get(example_name) or getattr(
                examples_module,
                example_name,
            )
            for endpoint, json in example:
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

    await create_schema(template_db_url)
    print("Tables created")

    async with template_app_client(
        template_db_url,
        reader_db_url,
        column_mappings=COLUMN_MAPPINGS,
    ) as (session, test_client):
        await default_attribute_types(session)
        await seed_default_catalogs(session)
        await create_default_user(session)
        print("Default data seeded")

        print(f"Loading examples via HTTP client: {examples_to_load or 'ALL'}")
        await load_examples_in_client(test_client, examples_to_load)
        print("Examples loaded")

    print("Template database populated successfully!")


if __name__ == "__main__":
    asyncio.run(main())
