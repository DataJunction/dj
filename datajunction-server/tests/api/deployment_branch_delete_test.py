"""
Tests for deployments that delete nodes from a git branch namespace.
"""

from collections.abc import Sequence

import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.history import History
from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.internal.branch_scope import names_in_default
from datajunction_server.models.deployment import (
    ColumnSpec,
    DeploymentSpec,
    NodeSpec,
    SourceSpec,
    TransformSpec,
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


def source(name: str, description: str = "Test source") -> SourceSpec:
    return SourceSpec(
        name=name,
        description=description,
        catalog="default",
        schema="test",
        table=name,
        columns=[ColumnSpec(name="id", type="int")],
    )


async def deploy(
    client: AsyncClient,
    namespace: str,
    nodes: Sequence[str | NodeSpec],
) -> dict:
    specs = [source(node) if isinstance(node, str) else node for node in nodes]
    data = await deploy_and_wait(
        client,
        DeploymentSpec(namespace=namespace, nodes=specs),
    )
    assert data["status"] == "success", data
    return data


def outcomes(data: dict) -> list[tuple[str, str, str]]:
    """
    Name, operation and status of each node result.
    """
    return sorted(
        (r["name"], r["operation"], r["status"])
        for r in data["results"]
        if r["deploy_type"] == "node"
    )


async def node_rows(session: AsyncSession, namespace: str) -> list[tuple]:
    """
    Name, whether deactivated, and revision count of the nodes in a namespace.
    """
    session.expire_all()
    rows = (
        await session.execute(
            select(Node.name, Node.deactivated_at.is_not(None), Node.id)
            .where(Node.namespace == namespace)
            .order_by(Node.name),
        )
    ).all()
    counts = []
    for name, deactivated, node_id in rows:
        revisions = (
            (
                await session.execute(
                    select(NodeRevision.id).where(NodeRevision.node_id == node_id),
                )
            )
            .scalars()
            .all()
        )
        counts.append((name, deactivated, len(revisions)))
    return counts


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
    assert outcomes(data) == [
        ("tomb.feat_x.a", "delete", "success"),
        ("tomb.feat_x.b", "noop", "skipped"),
    ]


@pytest.mark.asyncio
class TestBranchDelete:
    async def test_branch_delete_deactivates(
        self,
        client: AsyncClient,
        session: AsyncSession,
        deleted_in_branch: None,
    ):
        """
        A deleted branch node is deactivated, and hides the default branch's copy.
        """
        assert await node_rows(session, "tomb.feat_x") == [
            ("tomb.feat_x.a", True, 1),
            ("tomb.feat_x.b", False, 1),
        ]
        assert await node_rows(session, "tomb.main") == [("tomb.main.a", False, 1)]

        assert await Node.get_by_name(session, "tomb.feat_x.a") is None
        assert await Node.get_by_name(session, "tomb.feat_x.a", inherit=True) is None
        assert await Node.get_by_names(session, ["tomb.feat_x.a"], inherit=True) == []
        assert (await client.get("/nodes/tomb.feat_x.a/")).status_code == 404
        assert (await client.get("/nodes/tomb.main.a/")).status_code == 200

        history = (
            await session.execute(
                select(History.entity_name, History.activity_type, History.user).where(
                    History.entity_name == "tomb.feat_x.a",
                    History.activity_type == "delete",
                ),
            )
        ).all()
        assert [tuple(row) for row in history] == [("tomb.feat_x.a", "delete", "dj")]

    async def test_plain_and_default_hard_delete(
        self,
        client: AsyncClient,
        session: AsyncSession,
        namespaces: None,
    ):
        """
        Plain namespaces and the default branch still remove deleted nodes.
        """
        await deploy(client, "plain", ["a", "b"])
        await deploy(client, "plain", ["b"])
        await deploy(client, "tomb.main", ["a", "b"])
        await deploy(client, "tomb.main", ["b"])
        assert await node_rows(session, "plain") == [("plain.b", False, 1)]
        assert await node_rows(session, "tomb.main") == [("tomb.main.b", False, 1)]

    async def test_unheld_name_hard_deleted(
        self,
        client: AsyncClient,
        session: AsyncSession,
        namespaces: None,
    ):
        """
        A deleted name the default branch lacks is removed outright.
        """
        await deploy(client, "tomb.main", ["a"])
        await deploy(client, "tomb.feat_x", ["a", "c", "keep"])
        await deploy(client, "tomb.feat_x", ["keep"])
        assert await node_rows(session, "tomb.feat_x") == [
            ("tomb.feat_x.a", True, 1),
            ("tomb.feat_x.keep", False, 1),
        ]

    async def test_dry_run_changes_nothing(
        self,
        client: AsyncClient,
        session: AsyncSession,
        namespaces: None,
    ):
        """
        An impact analysis reports the delete and rolls it back.
        """
        await deploy(client, "tomb.main", ["a"])
        await deploy(client, "tomb.feat_x", ["a", "b"])
        data = await impact_and_poll(
            client,
            DeploymentSpec(namespace="tomb.feat_x", nodes=[source("b")]),
        )
        assert outcomes(data) == [
            ("tomb.feat_x.a", "delete", "success"),
            ("tomb.feat_x.b", "noop", "skipped"),
        ]
        assert await node_rows(session, "tomb.feat_x") == [
            ("tomb.feat_x.a", False, 1),
            ("tomb.feat_x.b", False, 1),
        ]

    async def test_downstream_deleted_together(
        self,
        client: AsyncClient,
        session: AsyncSession,
        namespaces: None,
    ):
        """
        A node and the transform built on it are deactivated together.
        """
        transform = TransformSpec(name="t", query="SELECT id FROM ${prefix}a")
        await deploy(client, "tomb.main", ["a"])
        await deploy(client, "tomb.feat_x", [source("a"), transform])
        data = await deploy(client, "tomb.feat_x", ["keep"])
        assert [
            (r["name"], r["operation"], r["status"])
            for r in sorted(data["results"], key=lambda r: r["name"])
            if r["operation"] == "delete"
        ] == [
            ("tomb.feat_x.a", "delete", "success"),
            ("tomb.feat_x.t", "delete", "success"),
        ]
        assert data["downstream_impacts"] == []
        assert await node_rows(session, "tomb.feat_x") == [
            ("tomb.feat_x.a", True, 1),
            ("tomb.feat_x.keep", False, 1),
        ]

    async def test_referenced_node_blocks_delete(
        self,
        client: AsyncClient,
        session: AsyncSession,
        namespaces: None,
    ):
        """
        A node something else still uses is not deactivated.
        """
        await deploy(client, "tomb.main", ["a"])
        await deploy(client, "tomb.feat_x", ["a"])
        await deploy(
            client,
            "tomb.other",
            [TransformSpec(name="u", query="SELECT id FROM tomb.feat_x.a")],
        )
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace="tomb.feat_x", nodes=[source("keep")]),
        )
        assert data["status"] == "failed"
        assert [
            (r["name"], r["operation"], r["status"])
            for r in data["results"]
            if r["operation"] == "delete"
        ] == [("tomb.feat_x.a", "delete", "failed")]
        assert await node_rows(session, "tomb.feat_x") == [
            ("tomb.feat_x.a", False, 1),
            ("tomb.feat_x.keep", False, 1),
        ]


@pytest.mark.asyncio
class TestBranchReAdd:
    async def test_re_add_unchanged(
        self,
        client: AsyncClient,
        session: AsyncSession,
        deleted_in_branch: None,
    ):
        """
        Deploying the node again makes it active.
        """
        data = await deploy(client, "tomb.feat_x", ["a", "b"])
        assert outcomes(data) == [
            ("tomb.feat_x.a", "noop", "skipped"),
            ("tomb.feat_x.b", "noop", "skipped"),
        ]
        assert await node_rows(session, "tomb.feat_x") == [
            ("tomb.feat_x.a", False, 1),
            ("tomb.feat_x.b", False, 1),
        ]
        node = await Node.get_by_name(session, "tomb.feat_x.a", inherit=True)
        assert node is not None
        assert (node.name, node.deactivated_at) == ("tomb.feat_x.a", None)

    async def test_re_add_changed(
        self,
        client: AsyncClient,
        session: AsyncSession,
        deleted_in_branch: None,
    ):
        """
        Deploying the node again with changes updates it.
        """
        data = await deploy(
            client,
            "tomb.feat_x",
            [source("a", "Back with changes"), "b"],
        )
        assert outcomes(data) == [
            ("tomb.feat_x.a", "update", "success"),
            ("tomb.feat_x.b", "noop", "skipped"),
        ]
        assert await node_rows(session, "tomb.feat_x") == [
            ("tomb.feat_x.a", False, 2),
            ("tomb.feat_x.b", False, 1),
        ]
        response = await client.get("/nodes/tomb.feat_x.a/")
        assert response.status_code == 200
        assert response.json()["description"] == "Back with changes"

    async def test_plan_ignores_undeclared(
        self,
        client: AsyncClient,
        session: AsyncSession,
        deleted_in_branch: None,
    ):
        """
        A deactivated node the spec leaves out is not an existing node.
        """
        data = await impact_and_poll(
            client,
            DeploymentSpec(namespace="tomb.feat_x", nodes=[source("b")]),
        )
        assert outcomes(data) == [("tomb.feat_x.b", "noop", "skipped")]
        assert await node_rows(session, "tomb.feat_x") == [
            ("tomb.feat_x.a", True, 1),
            ("tomb.feat_x.b", False, 1),
        ]

    async def test_api_create(
        self,
        client: AsyncClient,
        session: AsyncSession,
        deleted_in_branch: None,
    ):
        """
        Creating the node through the API brings it back.
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
        assert await node_rows(session, "tomb.feat_x") == [
            ("tomb.feat_x.a", False, 2),
            ("tomb.feat_x.b", False, 1),
        ]


@pytest.mark.asyncio
class TestBranchRestore:
    async def test_restore(
        self,
        client: AsyncClient,
        session: AsyncSession,
        deleted_in_branch: None,
    ):
        """
        Restoring brings back the branch's own node.
        """
        response = await client.post("/nodes/tomb.feat_x.a/restore/")
        assert response.status_code == 200, response.json()
        assert response.json() == {
            "message": "Node `tomb.feat_x.a` has been successfully restored.",
        }
        node = await Node.get_by_name(session, "tomb.feat_x.a", inherit=True)
        assert node is not None
        assert (node.name, node.deactivated_at) == ("tomb.feat_x.a", None)
        assert (await client.get("/nodes/tomb.feat_x.a/")).status_code == 200
        assert await node_rows(session, "tomb.main") == [("tomb.main.a", False, 1)]


@pytest.mark.asyncio
class TestBranchNamespaceDelete:
    async def test_hard_delete_cascade(
        self,
        client: AsyncClient,
        session: AsyncSession,
        deleted_in_branch: None,
    ):
        """
        A cascading hard delete removes the deactivated node too.
        """
        response = await client.delete(
            "/namespaces/tomb.feat_x/hard/",
            params={"cascade": True},
        )
        assert response.status_code == 200, response.json()
        assert response.json()["impact"]["deleted_nodes"] == [
            "tomb.feat_x.a",
            "tomb.feat_x.b",
        ]
        assert await node_rows(session, "tomb.feat_x") == []
        assert await node_rows(session, "tomb.main") == [("tomb.main.a", False, 1)]

    async def test_names_in_default(
        self,
        client: AsyncClient,
        session: AsyncSession,
        deleted_in_branch: None,
    ):
        """
        Only branch names the default branch holds are returned.
        """
        assert await names_in_default(
            session,
            ["tomb.feat_x.a", "tomb.feat_x.b", "tomb.main.a", "plain.a"],
        ) == {"tomb.feat_x.a"}
        assert await names_in_default(session, ["plain.a"]) == set()
