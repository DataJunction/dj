"""Tests for per-key expression index creation on schema registration.

Tier-1 index: numeric-typed schemas (type='number' or type='integer') get a
functional index on (custom_metadata->>'key')::numeric for range/sort queries.
String and other types do NOT get an index (equality served by GIN).

Key names chosen to be unique to this file to avoid cross-test index leakage
when the testcontainer DB persists across the session.

These register global keys (namespace omitted), which requires an admin caller,
so the tests use an authenticated admin client.
"""

from datetime import timedelta

import httpx
import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.api.main import app
from datajunction_server.database.user import User
from datajunction_server.internal.access.authentication.tokens import create_token
from datajunction_server.models.user import OAuthProvider


@pytest_asyncio.fixture
async def admin_client(client: AsyncClient, session: AsyncSession):
    """An AsyncClient authenticated as an admin user (needed for global keys).

    Depends on ``client`` so its app-level get_session override (pointing at the
    test database) is installed before we build the admin-authenticated client.
    """
    admin_user = User(
        username="admin_expr_index",
        email=None,
        name=None,
        oauth_provider=OAuthProvider.BASIC,
        is_admin=True,
    )
    session.add(admin_user)
    await session.commit()
    admin_token = create_token(
        {"username": "admin_expr_index"},
        secret="a-fake-secretkey",
        iss="http://localhost:8000/",
        expires_delta=timedelta(hours=24),
    )
    async with AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {admin_token}"},
    ) as authed:
        yield authed


@pytest.mark.asyncio
async def test_numeric_schema_builds_expression_index(
    admin_client: AsyncClient,
    session: AsyncSession,
) -> None:
    """Registering a filterable numeric schema must create ix_cm_threshold_num."""
    resp = await admin_client.post(
        "/metadata-schemas/",
        json={"key": "threshold_num", "json_schema": {"type": "number"}},
    )
    assert resp.status_code in (200, 201)

    result = await session.execute(
        text(
            "SELECT indexname FROM pg_indexes "
            "WHERE tablename='noderevision' "
            "AND indexname='ix_cm_threshold_num'",
        ),
    )
    assert result.scalar() == "ix_cm_threshold_num"


@pytest.mark.asyncio
async def test_integer_schema_builds_expression_index(
    admin_client: AsyncClient,
    session: AsyncSession,
) -> None:
    """Registering a filterable integer schema must create ix_cm_priority_int."""
    resp = await admin_client.post(
        "/metadata-schemas/",
        json={"key": "priority_int", "json_schema": {"type": "integer"}},
    )
    assert resp.status_code in (200, 201)

    result = await session.execute(
        text(
            "SELECT indexname FROM pg_indexes "
            "WHERE tablename='noderevision' "
            "AND indexname='ix_cm_priority_int'",
        ),
    )
    assert result.scalar() == "ix_cm_priority_int"


@pytest.mark.asyncio
async def test_string_schema_builds_no_expression_index(
    admin_client: AsyncClient,
    session: AsyncSession,
) -> None:
    """Registering a string schema must NOT create an expression index (GIN covers equality)."""
    resp = await admin_client.post(
        "/metadata-schemas/",
        json={"key": "label_str", "json_schema": {"type": "string"}},
    )
    assert resp.status_code in (200, 201)

    result = await session.execute(
        text(
            "SELECT indexname FROM pg_indexes "
            "WHERE tablename='noderevision' "
            "AND indexname='ix_cm_label_str'",
        ),
    )
    assert result.scalar() is None


@pytest.mark.asyncio
async def test_non_filterable_numeric_schema_builds_no_index(
    admin_client: AsyncClient,
    session: AsyncSession,
) -> None:
    """A numeric schema with filterable=False must NOT build an expression index."""
    resp = await admin_client.post(
        "/metadata-schemas/",
        json={
            "key": "hidden_score",
            "json_schema": {"type": "number"},
            "filterable": False,
        },
    )
    assert resp.status_code in (200, 201)

    result = await session.execute(
        text(
            "SELECT indexname FROM pg_indexes "
            "WHERE tablename='noderevision' "
            "AND indexname='ix_cm_hidden_score'",
        ),
    )
    assert result.scalar() is None
