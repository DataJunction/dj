#!/usr/bin/env python
"""
Script to populate the template database with all examples.
Run as a subprocess to avoid event loop conflicts with pytest-asyncio.

Usage: python populate_template.py <database_url>
"""

import asyncio
import os
import sys
from datetime import timedelta
from http.client import HTTPException
from typing import Dict, List, Optional

import httpx
from cachelib.simple import SimpleCache
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

# Get database URL from command line
template_db_url = sys.argv[1]
reader_db_url = template_db_url.replace("dj:dj@", "readonly_user:readonly@")

# Set environment variables BEFORE importing any datajunction_server modules
# This ensures the Settings class picks up these values
os.environ["DJ_DATABASE__URI"] = template_db_url
os.environ["WRITER_DB__URI"] = template_db_url
os.environ["READER_DB__URI"] = reader_db_url

# Add tests directory to path for examples import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from examples import COLUMN_MAPPINGS, EXAMPLES, SERVICE_SETUP  # noqa: E402

# Import config first and clear cache to ensure our env vars are used
from datajunction_server.config import DatabaseConfig, Settings  # noqa: E402
from datajunction_server.utils import get_settings  # noqa: E402

# Clear the lru_cache on get_settings to force it to re-read
get_settings.cache_clear()

# Now import the rest of the modules - they should use our settings
from datajunction_server.api.main import app  # noqa: E402
from datajunction_server.api.attributes import default_attribute_types  # noqa: E402
from datajunction_server.database.base import Base  # noqa: E402
from datajunction_server.database.column import Column  # noqa: E402
from datajunction_server.database.engine import Engine  # noqa: E402
from datajunction_server.database.user import User  # noqa: E402
from datajunction_server.internal.access.authentication.tokens import create_token  # noqa: E402
from datajunction_server.internal.access.authorization import (  # noqa: E402
    get_authorization_service,
    PassthroughAuthorizationService,
)
from datajunction_server.internal.seed import seed_default_catalogs  # noqa: E402
from datajunction_server.models.dialect import register_dialect_plugin  # noqa: E402
from datajunction_server.models.query import QueryCreate, QueryWithResults  # noqa: E402
from datajunction_server.models.user import OAuthProvider  # noqa: E402
from datajunction_server.service_clients import QueryServiceClient  # noqa: E402
from datajunction_server.transpilation import SQLTranspilationPlugin  # noqa: E402
from datajunction_server.typing import QueryState  # noqa: E402
from datajunction_server.utils import get_session, get_query_service_client  # noqa: E402

# Verify our settings are correct
actual_settings = get_settings()
print(f"Using writer_db: {actual_settings.writer_db.uri}")
print(
    f"Using reader_db: {actual_settings.reader_db.uri if actual_settings.reader_db else 'None'}",
)

# Import seed module to patch its cached settings
from datajunction_server.internal import seed as seed_module  # noqa: E402

# Create template settings (matching what get_settings() should return)
template_settings = Settings(
    writer_db=DatabaseConfig(uri=template_db_url),
    reader_db=DatabaseConfig(uri=reader_db_url),
    repository="/path/to/repository",
    results_backend=SimpleCache(default_timeout=0),
    celery_broker=None,
    redis_cache=None,
    query_service=None,
    secret="a-fake-secretkey",
    transpilation_plugins=["default"],
)

# Patch the cached settings in seed module
seed_module.settings = template_settings

# Register dialect plugins
register_dialect_plugin("spark", SQLTranspilationPlugin)
register_dialect_plugin("trino", SQLTranspilationPlugin)
register_dialect_plugin("druid", SQLTranspilationPlugin)


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
    examples_to_load: Optional[List[str]] = None,
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

    engine = create_async_engine(
        url=template_db_url,
        poolclass=StaticPool,
    )

    # Create all tables
    async with engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm;"))
        await conn.run_sync(Base.metadata.create_all)
    print("Tables created")

    async_session_factory = async_sessionmaker(
        bind=engine,
        autocommit=False,
        expire_on_commit=False,
    )

    async with async_session_factory() as session:
        # Seed default data
        await default_attribute_types(session)
        await seed_default_catalogs(session)
        await create_default_user(session)
        print("Default data seeded")

        # Create mock query service client
        qs_client = QueryServiceClient(uri="query_service:8001")

        def mock_get_columns_for_table(
            catalog: str,
            schema: str,
            table: str,
            engine: Optional[Engine] = None,
            request_headers: Optional[Dict[str, str]] = None,
        ) -> List[Column]:
            return COLUMN_MAPPINGS.get(f"{catalog}.{schema}.{table}", [])

        def mock_submit_query(
            query_create: QueryCreate,
            request_headers: Optional[Dict[str, str]] = None,
        ) -> QueryWithResults:
            return QueryWithResults(
                id="bd98d6be-e2d2-413e-94c7-96d9411ddee2",
                submitted_query=query_create.submitted_query,
                state=QueryState.FINISHED,
                results=[
                    {"columns": [], "rows": [], "sql": query_create.submitted_query},
                ],
                errors=[],
            )

        qs_client.get_columns_for_table = mock_get_columns_for_table  # type: ignore
        qs_client.submit_query = mock_submit_query  # type: ignore

        # Override dependencies
        def get_session_override() -> AsyncSession:
            return session

        def get_settings_override() -> Settings:
            return template_settings

        def get_passthrough_auth_service():
            """Override to approve all requests in tests."""
            return PassthroughAuthorizationService()

        def get_query_service_client_override(request=None):
            return qs_client

        app.dependency_overrides[get_session] = get_session_override
        app.dependency_overrides[get_settings] = get_settings_override
        app.dependency_overrides[get_authorization_service] = (
            get_passthrough_auth_service
        )
        app.dependency_overrides[get_query_service_client] = (
            get_query_service_client_override
        )

        # Create JWT token
        jwt_token = create_token(
            {"username": "dj"},
            secret="a-fake-secretkey",
            iss="http://localhost:8000/",
            expires_delta=timedelta(hours=24),
        )

        # Load ALL examples
        print("Loading examples via HTTP client...")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as test_client:
            test_client.headers.update({"Authorization": f"Bearer {jwt_token}"})
            await load_examples_in_client(test_client, None)  # None = load ALL examples
        print("Examples loaded")

        app.dependency_overrides.clear()

    await engine.dispose()
    print("Template database populated successfully!")


if __name__ == "__main__":
    asyncio.run(main())
