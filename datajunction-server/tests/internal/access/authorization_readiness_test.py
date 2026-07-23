"""Tests for RBAC activation readiness."""

import logging

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.config import Settings
from datajunction_server.database.user import OAuthProvider, PrincipalKind, User
from datajunction_server.internal.access.authorization.readiness import (
    verify_rbac_admins,
)


@pytest.mark.asyncio
async def test_permissive_rbac_does_not_require_admins(
    clean_session: AsyncSession,
) -> None:
    settings = Settings(
        authorization_provider="rbac",
        default_access_policy="permissive",
        rbac_require_admin=False,
        rbac_admin_users=[],
    )

    await verify_rbac_admins(clean_session, settings)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("policy", "require_admin"),
    [("restrictive", False), ("permissive", True)],
)
async def test_enforcement_requires_admin_usernames(
    clean_session: AsyncSession,
    policy: str,
    require_admin: bool,
) -> None:
    settings = Settings(
        authorization_provider="rbac",
        default_access_policy=policy,
        rbac_require_admin=require_admin,
        rbac_admin_users=[],
    )

    with pytest.raises(RuntimeError, match="RBAC_ADMIN_USERS"):
        await verify_rbac_admins(clean_session, settings)


@pytest.mark.asyncio
async def test_admin_verification_reports_invalid_principals(
    clean_session: AsyncSession,
) -> None:
    clean_session.add_all(
        [
            User(
                username="ordinary-user",
                oauth_provider=OAuthProvider.BASIC,
                kind=PrincipalKind.USER,
                is_admin=False,
            ),
            User(
                username="admin-group",
                oauth_provider=OAuthProvider.BASIC,
                kind=PrincipalKind.GROUP,
                is_admin=True,
            ),
        ],
    )
    await clean_session.commit()
    settings = Settings(
        authorization_provider="rbac",
        rbac_require_admin=True,
        rbac_admin_users=["missing", "ordinary-user", "admin-group"],
    )

    with pytest.raises(RuntimeError) as exc_info:
        await verify_rbac_admins(clean_session, settings)

    message = str(exc_info.value)
    assert "missing users: missing" in message
    assert "non-user principals: admin-group" in message
    assert "users without is_admin: ordinary-user" in message


@pytest.mark.asyncio
async def test_admin_verification_accepts_seeded_admin(
    clean_session: AsyncSession,
    caplog: pytest.LogCaptureFixture,
) -> None:
    clean_session.add(
        User(
            username="break-glass",
            oauth_provider=OAuthProvider.BASIC,
            kind=PrincipalKind.USER,
            is_admin=True,
        ),
    )
    await clean_session.commit()
    settings = Settings(
        authorization_provider="rbac",
        rbac_require_admin=True,
        rbac_admin_users=["break-glass"],
    )

    with caplog.at_level(
        logging.INFO,
        logger="datajunction_server.internal.access.authorization.readiness",
    ):
        await verify_rbac_admins(clean_session, settings)

    assert "rbac_admin_readiness_verified admins=break-glass" in caplog.messages
