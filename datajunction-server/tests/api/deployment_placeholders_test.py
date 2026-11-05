"""
Tests for placeholder rows left when a deployment deletes branch nodes.
"""

from datetime import UTC, datetime

import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.construction.utils import get_dj_node
from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.node import Node, NodeRevision, NodeType
from datajunction_server.database.user import User
from datajunction_server.errors import DJErrorException
from datajunction_server.internal.branch_scope import (
    drop_placeholders,
    leave_placeholders,
    placeholder_clause,
)
from datajunction_server.models.deployment import (
    ColumnSpec,
    DeploymentSpec,
    SourceSpec,
)
from tests.api.deployments_test import deploy_and_wait, impact_and_poll


@pytest_asyncio.fixture
async def namespaces(session: AsyncSession) -> None:
    """
    A git root `tomb` with branches `main` and `feat_x`, and a plain namespace.
    """
    session.add(
        NodeNamespace(
            namespace="tomb",
            github_repo_path="org/repo",
            default_branch="main",
        ),
    )
    await session.flush()
    session.add_all(
        [
            NodeNamespace(
                namespace="tomb.main",
                git_branch="main",
                parent_namespace="tomb",
            ),
            NodeNamespace(
                namespace="tomb.feat_x",
                git_branch="feat-x",
                parent_namespace="tomb",
            ),
            NodeNamespace(namespace="plain"),
        ],
    )
    await session.commit()


def source(name: str) -> SourceSpec:
    return SourceSpec(
        name=name,
        description="Test source",
        catalog="default",
        schema="test",
        table=name,
        columns=[ColumnSpec(name="id", type="int")],
    )


async def deploy(
    client: AsyncClient,
    namespace: str,
    names: list[str],
) -> dict:
    data = await deploy_and_wait(
        client,
        DeploymentSpec(namespace=namespace, nodes=[source(name) for name in names]),
    )
    assert data["status"] == "success", data
    return data


async def placeholders(session: AsyncSession) -> list[tuple]:
    """
    Name, type and deactivation of every placeholder row, with its revision count.
    """
    session.expire_all()
    rows = (
        await session.execute(
            select(Node.name, Node.type, Node.deactivated_at.is_not(None)).where(
                placeholder_clause(),
            ),
        )
    ).all()
    return sorted(tuple(row) for row in rows)


async def revision_count(session: AsyncSession, name: str) -> int:
    session.expire_all()
    return len(
        (
            await session.execute(
                select(NodeRevision.id)
                .join(Node, Node.id == NodeRevision.node_id)
                .where(Node.name == name),
            )
        )
        .scalars()
        .all(),
    )


@pytest_asyncio.fixture
async def deleted_in_branch(
    client: AsyncClient,
    session: AsyncSession,
    namespaces: None,
) -> None:
    """
    The default branch holds `a`; the branch had `a` and `b`, then drops `a`.
    """
    await deploy(client, "tomb.main", ["a"])
    await deploy(client, "tomb.feat_x", ["a", "b"])
    data = await deploy(client, "tomb.feat_x", ["b"])
    assert [
        (r["name"], r["operation"], r["status"])
        for r in data["results"]
        if r["operation"] == "delete"
    ] == [("tomb.feat_x.a", "delete", "success")]


@pytest.mark.asyncio
class TestPlaceholdersOnDelete:
    async def test_branch_delete_leaves_placeholder(
        self,
        client: AsyncClient,
        session: AsyncSession,
        deleted_in_branch: None,
    ):
        """
        A deleted branch node is hidden from the default branch's copy.
        """
        assert await placeholders(session) == [
            ("tomb.feat_x.a", NodeType.SOURCE, True),
        ]
        assert await revision_count(session, "tomb.feat_x.a") == 0
        node = (
            await session.execute(select(Node).where(Node.name == "tomb.feat_x.a"))
        ).scalar_one()
        assert node.namespace == "tomb.feat_x"
        assert node.created_by.username == "dj"

        assert await Node.get_by_name(session, "tomb.feat_x.a") is None
        assert await Node.get_by_name(session, "tomb.feat_x.a", inherit=True) is None
        assert await Node.get_by_names(session, ["tomb.feat_x.a"], inherit=True) == []
        main_node = await Node.get_by_name(session, "tomb.main.a")
        assert main_node is not None
        assert main_node.name == "tomb.main.a"
        assert (await client.get("/nodes/tomb.feat_x.a/")).status_code == 404

    async def test_no_placeholder_outside_branches(
        self,
        client: AsyncClient,
        session: AsyncSession,
        namespaces: None,
    ):
        """
        Plain namespaces and the default branch leave nothing behind.
        """
        await deploy(client, "plain", ["a", "b"])
        await deploy(client, "plain", ["b"])
        await deploy(client, "tomb.main", ["a", "b"])
        data = await deploy(client, "tomb.main", ["b"])
        assert [r["name"] for r in data["results"] if r["operation"] == "delete"] == [
            "tomb.main.a",
        ]
        assert await placeholders(session) == []
        assert (await client.get("/nodes/tomb.main.a/")).status_code == 404

    async def test_no_placeholder_for_unheld_name(
        self,
        client: AsyncClient,
        session: AsyncSession,
        namespaces: None,
    ):
        """
        Only names the default branch holds get a placeholder.
        """
        await deploy(client, "tomb.main", ["a", "gone"])
        await deploy(client, "tomb.feat_x", ["a", "c", "gone", "keep"])
        await client.delete("/nodes/tomb.main.gone/hard/")
        await deploy(client, "tomb.feat_x", ["keep"])
        assert await placeholders(session) == [
            ("tomb.feat_x.a", NodeType.SOURCE, True),
        ]

    async def test_dry_run_leaves_none(
        self,
        client: AsyncClient,
        session: AsyncSession,
        namespaces: None,
    ):
        """
        An impact analysis rolls back, and adds no placeholders.
        """
        await deploy(client, "tomb.main", ["a"])
        await deploy(client, "tomb.feat_x", ["a", "b"])
        data = await impact_and_poll(
            client,
            DeploymentSpec(namespace="tomb.feat_x", nodes=[source("b")]),
        )
        assert sorted(
            (r["name"], r["operation"], r["status"]) for r in data["results"]
        ) == [
            ("tomb.feat_x.a", "delete", "success"),
            ("tomb.feat_x.b", "noop", "skipped"),
        ]
        assert await placeholders(session) == []
        assert (await client.get("/nodes/tomb.feat_x.a/")).status_code == 200


@pytest.mark.asyncio
class TestPlaceholdersAndLaterDeploys:
    async def test_re_add_replaces_placeholder(
        self,
        client: AsyncClient,
        session: AsyncSession,
        deleted_in_branch: None,
    ):
        """
        Deploying the node again makes it active, with no placeholder left.
        """
        data = await deploy(client, "tomb.feat_x", ["a", "b"])
        assert sorted(
            (r["name"], r["operation"], r["status"])
            for r in data["results"]
            if r["deploy_type"] == "node"
        ) == [
            ("tomb.feat_x.a", "create", "success"),
            ("tomb.feat_x.b", "noop", "skipped"),
        ]
        assert await placeholders(session) == []
        assert await revision_count(session, "tomb.feat_x.a") == 1
        node = await Node.get_by_name(session, "tomb.feat_x.a", inherit=True)
        assert node is not None
        assert (node.name, node.deactivated_at) == ("tomb.feat_x.a", None)
        assert (await client.get("/nodes/tomb.feat_x.a/")).status_code == 200

    async def test_plan_ignores_placeholder(
        self,
        client: AsyncClient,
        session: AsyncSession,
        deleted_in_branch: None,
    ):
        """
        A placeholder is not an existing node when the plan is built.
        """
        data = await impact_and_poll(
            client,
            DeploymentSpec(namespace="tomb.feat_x", nodes=[source("b")]),
        )
        assert sorted(
            (r["name"], r["operation"], r["status"]) for r in data["results"]
        ) == [("tomb.feat_x.b", "noop", "skipped")]
        data = await impact_and_poll(
            client,
            DeploymentSpec(namespace="tomb.feat_x", nodes=[source("a"), source("b")]),
        )
        assert sorted(
            (r["name"], r["operation"], r["status"]) for r in data["results"]
        ) == [
            ("tomb.feat_x.a", "create", "success"),
            ("tomb.feat_x.b", "noop", "skipped"),
        ]
        assert await placeholders(session) == [
            ("tomb.feat_x.a", NodeType.SOURCE, True),
        ]

    async def test_api_create_replaces_placeholder(
        self,
        client: AsyncClient,
        session: AsyncSession,
        deleted_in_branch: None,
    ):
        """
        Creating the node through the API replaces its placeholder.
        """
        response = await client.post(
            "/nodes/source/",
            json={
                "name": "tomb.feat_x.a",
                "description": "Back again",
                "catalog": "default",
                "schema_": "test",
                "table": "a",
                "columns": [{"name": "id", "type": "int"}],
                "mode": "published",
            },
        )
        assert response.status_code == 200, response.json()
        assert response.json()["name"] == "tomb.feat_x.a"
        assert response.json()["version"] == "v1.0"
        assert await placeholders(session) == []
        assert await revision_count(session, "tomb.feat_x.a") == 1


@pytest.mark.asyncio
class TestPlaceholderEndpoints:
    async def test_restore_removes_placeholder(
        self,
        client: AsyncClient,
        session: AsyncSession,
        deleted_in_branch: None,
    ):
        """
        Restoring a placeholder shows the default branch's node again.
        """
        response = await client.post("/nodes/tomb.feat_x.a/restore/")
        assert response.status_code == 200, response.json()
        assert response.json() == {
            "message": "Node `tomb.feat_x.a` has been successfully restored.",
        }
        assert await placeholders(session) == []
        node = await Node.get_by_name(session, "tomb.feat_x.a", inherit=True)
        assert node is not None
        assert node.name == "tomb.main.a"
        assert (await client.get("/nodes/tomb.feat_x.a/")).status_code == 404

    async def test_restore_twice(
        self,
        client: AsyncClient,
        deleted_in_branch: None,
    ):
        """
        A second restore finds no deleted node.
        """
        assert (await client.post("/nodes/tomb.feat_x.a/restore/")).status_code == 200
        response = await client.post("/nodes/tomb.feat_x.a/restore/")
        assert response.status_code == 404
        assert response.json()["message"] == (
            "A node with name `tomb.feat_x.a` does not exist."
        )

    async def test_patch_placeholder(
        self,
        client: AsyncClient,
        deleted_in_branch: None,
    ):
        """
        A placeholder cannot be edited.
        """
        response = await client.patch(
            "/nodes/tomb.feat_x.a/",
            json={"description": "nope"},
        )
        assert response.status_code == 404
        assert response.json()["message"] == (
            "A node with name `tomb.feat_x.a` does not exist."
        )

    async def test_hard_delete_placeholder(
        self,
        client: AsyncClient,
        session: AsyncSession,
        deleted_in_branch: None,
    ):
        """
        A hard delete removes the placeholder.
        """
        response = await client.delete("/nodes/tomb.feat_x.a/hard/")
        assert response.status_code == 200, response.json()
        assert response.json() == {
            "message": "The node `tomb.feat_x.a` has been completely removed.",
            "impact": [],
        }
        assert await placeholders(session) == []

    async def test_namespace_hard_delete(
        self,
        client: AsyncClient,
        session: AsyncSession,
        deleted_in_branch: None,
    ):
        """
        Placeholders neither block nor appear in a namespace hard delete.
        """
        await client.delete("/nodes/tomb.feat_x.b/hard/")
        assert await placeholders(session) == [
            ("tomb.feat_x.a", NodeType.SOURCE, True),
        ]
        response = await client.delete("/namespaces/tomb.feat_x/hard/")
        assert response.status_code == 200, response.json()
        assert response.json() == {
            "message": "The namespace `tomb.feat_x` has been completely removed.",
            "impact": {
                "deleted_namespaces": ["tomb.feat_x"],
                "deleted_nodes": [],
                "impacted": {"downstreams": [], "links": []},
                "materialization_failures": [],
            },
        }
        assert await placeholders(session) == []
        session.expire_all()
        assert (
            await session.execute(
                select(Node.name).where(Node.namespace == "tomb.feat_x"),
            )
        ).all() == []

    async def test_namespace_hard_delete_cascade(
        self,
        client: AsyncClient,
        session: AsyncSession,
        deleted_in_branch: None,
    ):
        """
        A cascading hard delete reports only the real nodes.
        """
        response = await client.delete(
            "/namespaces/tomb.feat_x/hard/",
            params={"cascade": True},
        )
        assert response.status_code == 200, response.json()
        assert response.json()["impact"]["deleted_nodes"] == ["tomb.feat_x.b"]
        assert await placeholders(session) == []

    async def test_namespace_restore_skips_placeholders(
        self,
        client: AsyncClient,
        session: AsyncSession,
        deleted_in_branch: None,
    ):
        """
        Restoring a namespace leaves its placeholders alone.
        """
        response = await client.delete(
            "/namespaces/tomb.feat_x/",
            params={"cascade": True},
        )
        assert response.status_code == 200, response.json()
        response = await client.post(
            "/namespaces/tomb.feat_x/restore/",
            params={"cascade": True},
        )
        assert response.status_code == 201, response.json()
        assert response.json() == {
            "message": (
                "Namespace `tomb.feat_x` has been restored. The following nodes"
                " have also been restored: tomb.feat_x.b"
            ),
        }
        assert await placeholders(session) == [
            ("tomb.feat_x.a", NodeType.SOURCE, True),
        ]


@pytest.mark.asyncio
class TestPlaceholderLookups:
    async def test_cube_lookup(
        self,
        session: AsyncSession,
        current_user: User,
    ):
        """
        A placeholder is not a cube.
        """
        session.add(
            Node(
                name="tomb.feat_x.cube",
                type=NodeType.CUBE,
                namespace="tomb.feat_x",
                created_by_id=current_user.id,
                deactivated_at=datetime.now(UTC),
            ),
        )
        await session.commit()
        assert await Node.get_cube_by_name(session, "tomb.feat_x.cube") is None

    async def test_compile_lookup(
        self,
        session: AsyncSession,
        current_user: User,
    ):
        """
        A placeholder is an unknown node when compiling.
        """
        session.add(
            Node(
                name="tomb.feat_x.src",
                type=NodeType.SOURCE,
                namespace="tomb.feat_x",
                created_by_id=current_user.id,
                deactivated_at=datetime.now(UTC),
            ),
        )
        await session.commit()
        with pytest.raises(DJErrorException) as exc_info:
            await get_dj_node(session, "tomb.feat_x.src", {NodeType.SOURCE})
        assert exc_info.value.dj_error.message == (
            f"No node `tomb.feat_x.src` exists of kind {NodeType.SOURCE}."
        )

    async def test_helpers_without_work(
        self,
        session: AsyncSession,
        current_user: User,
    ):
        """
        Names outside a branch, or none at all, add and remove nothing.
        """
        assert await leave_placeholders(session, {"plain.a": NodeType.SOURCE}, 1) == []
        assert await drop_placeholders(session, []) == []
        assert await drop_placeholders(session, ["plain.a"]) == []
