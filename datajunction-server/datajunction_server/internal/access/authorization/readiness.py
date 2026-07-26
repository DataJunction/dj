"""RBAC activation readiness checks."""

import logging

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.config import Settings
from datajunction_server.database.user import PrincipalKind, User

logger = logging.getLogger(__name__)


async def verify_rbac_admins(
    session: AsyncSession,
    settings: Settings,
) -> None:
    """Verify the configured human break-glass admins before enforcement."""
    admin_required = settings.authorization_provider == "rbac" and (
        settings.rbac_require_admin or settings.default_access_policy == "restrictive"
    )
    if not admin_required:
        return

    expected_usernames = list(dict.fromkeys(settings.rbac_admin_users))
    if not expected_usernames:
        raise RuntimeError(
            "RBAC enforcement requires RBAC_ADMIN_USERS to name at least one "
            "seeded break-glass admin",
        )

    users = await User.get_by_usernames(
        session,
        expected_usernames,
        raise_if_not_exists=False,
        options=[],
    )
    users_by_name = {user.username: user for user in users}
    missing = [
        username for username in expected_usernames if username not in users_by_name
    ]
    wrong_kind = [user.username for user in users if user.kind != PrincipalKind.USER]
    not_admin = [
        user.username
        for user in users
        if user.kind == PrincipalKind.USER and not user.is_admin
    ]

    problems = []
    if missing:
        problems.append(f"missing users: {', '.join(missing)}")
    if wrong_kind:
        problems.append(f"non-user principals: {', '.join(wrong_kind)}")
    if not_admin:
        problems.append(f"users without is_admin: {', '.join(not_admin)}")
    if problems:
        raise RuntimeError(
            "RBAC break-glass admin verification failed: " + "; ".join(problems),
        )

    logger.info(
        "rbac_admin_readiness_verified admins=%s",
        ",".join(expected_usernames),
    )
