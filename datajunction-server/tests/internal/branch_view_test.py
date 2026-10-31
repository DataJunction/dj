"""
Tests that a default branch node, seen through a branch, matches a real copy.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.node import Node
from datajunction_server.internal.branch_view import branch_view
from datajunction_server.models.node import NodeOutput, NodeRevisionOutput

# Values a copy gets fresh or does not carry over.
FRESH = {
    "id",
    "node_id",
    "version",
    "current_version",
    "created_at",
    "updated_at",
    "created_by",
    "availability",
    "git_info",
}


async def make_branch(client: AsyncClient) -> None:
    """
    Branches `foo.bar` into `foo.feature_x`, copying its nodes.
    """
    await client.patch(
        "/namespaces/foo.bar/git",
        json={"github_repo_path": "myorg/myrepo", "git_branch": "main"},
    )
    with patch("datajunction_server.api.branches.GitHubService") as github_class:
        github = MagicMock()
        github.create_branch = AsyncMock(
            return_value={"ref": "refs/heads/feature-x", "object": {"sha": "abc"}},
        )
        github_class.return_value = github
        response = await client.post(
            "/namespaces/foo.bar/branches",
            json={"branch_name": "feature-x"},
        )
    assert response.status_code == 201


async def load_output(session: AsyncSession, name: str) -> NodeOutput:
    """
    Loads a node the way the node endpoint does.
    """
    node = (
        await session.execute(
            select(Node).where(Node.name == name).options(*NodeOutput.load_options()),
        )
    ).unique().scalar_one()
    return NodeOutput.model_validate(node)


def comparable(output: NodeOutput | NodeRevisionOutput) -> dict:
    """
    The fields a copy is expected to match.
    """
    return {
        key: value
        for key, value in output.model_dump(mode="json").items()
        if key not in FRESH
    }


@pytest.mark.asyncio
async def test_branch_view_matches_copy(
    client_with_namespaced_roads: AsyncClient,
    session: AsyncSession,
):
    """
    Every node in `foo.bar`, viewed as `foo.feature_x`, equals its real copy.
    """
    await make_branch(client_with_namespaced_roads)
    names = (
        (
            await session.execute(
                select(Node.name)
                .where(Node.name.like("foo.bar.%"), Node.deactivated_at.is_(None))
                .order_by(Node.name),
            )
        )
        .scalars()
        .all()
    )
    assert names
    views = {}
    copies = {}
    for name in names:
        main = await load_output(session, name)
        views[name] = comparable(branch_view(main, "foo.bar", "foo.feature_x"))
        copied = await load_output(session, name.replace("foo.bar.", "foo.feature_x."))
        copies[name] = comparable(copied)
    differing = {
        name: {
            key: repr((views[name][key], copies[name][key]))[:600]
            for key in views[name]
            if views[name][key] != copies[name][key]
        }
        for name in names
    }
    assert {name: diff for name, diff in differing.items() if diff} == {}
