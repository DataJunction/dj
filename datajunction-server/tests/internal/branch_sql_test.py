"""
Tests that SQL builds for a branch node missing from the branch use the real builder.
"""

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.user import User
from datajunction_server.internal.access.authorization.context import AuthContext
from datajunction_server.internal.access.authorization.validator import AccessChecker
from datajunction_server.internal.sql import build_node_sql


@pytest.mark.asyncio
async def test_node_sql_for_inherited_node(
    client_with_namespaced_roads: AsyncClient,
    session: AsyncSession,
):
    """
    A node the branch lacks builds the same SQL as the default branch's node.
    """
    session.add(
        NodeNamespace(
            namespace="foo",
            github_repo_path="org/repo",
            default_branch="bar",
        ),
    )
    await session.flush()
    session.add(
        NodeNamespace(namespace="foo.feat", git_branch="feat", parent_namespace="foo"),
    )
    await session.commit()
    user = (await session.execute(select(User))).scalars().first()
    assert user is not None
    access_checker = AccessChecker(await AuthContext.from_user(session, user))

    expected = await build_node_sql(
        session,
        "foo.bar.hard_hat",
        access_checker=access_checker,
    )
    inherited = await build_node_sql(
        session,
        "foo.feat.hard_hat",
        access_checker=access_checker,
    )
    assert inherited == expected
