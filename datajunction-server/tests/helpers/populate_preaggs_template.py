#!/usr/bin/env python
"""
Create the pre-aggregations that ``tests/api/preaggregations_test.py`` needs.

The target database is already a clone of the main template, so the BUILD_V3
examples these preaggs sit on top of are present. This only runs the ten
``/preaggs/plan`` calls and prints the resulting ids, so the caller can turn
the database into a template that every test clones instead of re-planning.

Run as a subprocess to avoid event loop conflicts with pytest-asyncio.

Usage: python populate_preaggs_template.py <database_url>
"""

# ruff: noqa: E402 - env vars must be set before importing datajunction_server modules

import asyncio
import json
import os
import sys
from datetime import timedelta
from unittest.mock import MagicMock

import httpx
from cachelib.simple import SimpleCache
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

db_url = sys.argv[1]
reader_db_url = db_url.replace("dj:dj@", "readonly_user:readonly@")

os.environ["DJ_DATABASE__URI"] = db_url
os.environ["WRITER_DB__URI"] = db_url
os.environ["READER_DB__URI"] = reader_db_url

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the database package first: importing datajunction_server.utils directly
# trips a circular import through datajunction_server.database.catalog.
import datajunction_server.database  # noqa: F401
from datajunction_server.config import DatabaseConfig, Settings
from datajunction_server.utils import get_settings

get_settings.cache_clear()

from datajunction_server.api.main import app
from datajunction_server.internal import seed as seed_module
from datajunction_server.internal.access.authentication.tokens import create_token
from datajunction_server.internal.access.authorization import (
    PassthroughAuthorizationService,
    get_authorization_service,
)
from datajunction_server.models.dialect import register_dialect_plugin
from datajunction_server.transpilation import SQLTranspilationPlugin
from datajunction_server.utils import get_query_service_client, get_session

template_settings = Settings(
    writer_db=DatabaseConfig(uri=db_url),
    reader_db=DatabaseConfig(uri=reader_db_url),
    repository="/path/to/repository",
    results_backend=SimpleCache(default_timeout=0),
    celery_broker=None,
    redis_cache=None,
    query_service=None,
    secret="a-fake-secretkey",
    transpilation_plugins=["default"],
)
seed_module.settings = template_settings

register_dialect_plugin("spark", SQLTranspilationPlugin)
register_dialect_plugin("trino", SQLTranspilationPlugin)
register_dialect_plugin("druid", SQLTranspilationPlugin)

# Kept in the same order as the fixture's preagg1..preagg10 so the printed ids
# line up positionally with the names the tests use.
PREAGG_SPECS: list[dict] = [
    {
        "metrics": ["v3.total_revenue", "v3.total_quantity"],
        "dimensions": ["v3.order_details.status"],
        "strategy": "full",
        "schedule": "0 0 * * *",
    },
    {
        "metrics": ["v3.total_revenue", "v3.avg_unit_price"],
        "dimensions": ["v3.order_details.status", "v3.product.category"],
        "strategy": "full",
        "schedule": "0 * * * *",
    },
    {
        "metrics": ["v3.max_unit_price"],
        "dimensions": ["v3.order_details.status"],
        "strategy": "full",
    },
    {
        "metrics": ["v3.total_revenue"],
        "dimensions": ["v3.product.category"],
    },
    {
        "metrics": ["v3.order_count"],
        "dimensions": ["v3.order_details.status"],
        "strategy": "full",
        "schedule": "0 0 * * *",
    },
    {
        "metrics": ["v3.min_unit_price"],
        "dimensions": ["v3.order_details.status"],
        "strategy": "full",
        "schedule": "0 0 * * *",
    },
    {
        "metrics": ["v3.total_revenue"],
        "dimensions": ["v3.customer.customer_id"],
    },
    {
        "metrics": ["v3.page_view_count"],
        "dimensions": ["v3.product.category"],
        "strategy": "full",
        "schedule": "0 0 * * *",
    },
    {
        "metrics": ["v3.session_count"],
        "dimensions": ["v3.product.category"],
        "strategy": "full",
        "schedule": "0 0 * * *",
    },
    {
        "metrics": ["v3.visitor_count"],
        "dimensions": ["v3.product.category"],
    },
]


async def main() -> None:
    engine = create_async_engine(url=db_url, poolclass=StaticPool)
    session_factory = async_sessionmaker(bind=engine, expire_on_commit=False)

    async with session_factory() as session:

        def get_session_override() -> AsyncSession:
            return session

        def get_settings_override() -> Settings:
            return template_settings

        def get_passthrough_auth_service():
            return PassthroughAuthorizationService()

        def get_query_service_client_override(request=None):
            return MagicMock()

        app.dependency_overrides[get_session] = get_session_override
        app.dependency_overrides[get_settings] = get_settings_override
        app.dependency_overrides[get_authorization_service] = (
            get_passthrough_auth_service
        )
        app.dependency_overrides[get_query_service_client] = (
            get_query_service_client_override
        )

        jwt_token = create_token(
            {"username": "dj"},
            secret="a-fake-secretkey",
            iss="http://localhost:8000/",
            expires_delta=timedelta(hours=24),
        )

        preagg_ids: list[int] = []
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            client.headers.update({"Authorization": f"Bearer {jwt_token}"})
            for index, spec in enumerate(PREAGG_SPECS, start=1):
                response = await client.post("/preaggs/plan", json=spec)
                if response.status_code != 201:
                    raise RuntimeError(
                        f"preagg {index} failed ({response.status_code}): "
                        f"{response.text}",
                    )
                preagg_ids.append(response.json()["preaggs"][0]["id"])

        await session.commit()
        app.dependency_overrides.clear()

    await engine.dispose()
    print("PREAGG_IDS " + json.dumps(preagg_ids))


if __name__ == "__main__":
    asyncio.run(main())
