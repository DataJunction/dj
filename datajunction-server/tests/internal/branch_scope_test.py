"""
Tests for resolving branch node names through the default branch.
"""

from datetime import UTC, datetime

import pytest
import pytest_asyncio
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.node import Node, NodeType
from datajunction_server.database.user import OAuthProvider, User
from datajunction_server.internal.branch_scope import (
    inherited_names,
    parent_node_names,
)
from datajunction_server.internal.namespaces import get_git_info_for_namespace


@pytest_asyncio.fixture
async def user(session: AsyncSession) -> User:
    """
    A user to satisfy Node.created_by_id.
    """
    user = User(username="branch_scope_user", oauth_provider=OAuthProvider.BASIC)
    session.add(user)
    await session.commit()
    return user


@pytest_asyncio.fixture
async def namespaces(session: AsyncSession) -> None:
    """
    A git root `shared` with branches `main` and `feat-x`, a plain namespace,
    and a branch with no parent link.
    """
    session.add(
        NodeNamespace(
            namespace="shared",
            github_repo_path="org/repo",
            default_branch="main",
        ),
    )
    await session.flush()
    session.add_all(
        [
            NodeNamespace(
                namespace="shared.main",
                git_branch="main",
                parent_namespace="shared",
            ),
            NodeNamespace(
                namespace="shared.feat_x",
                git_branch="feat-x",
                parent_namespace="shared",
            ),
            NodeNamespace(namespace="plain"),
            NodeNamespace(namespace="solo", github_repo_path="org/solo"),
            NodeNamespace(namespace="solo.dev", git_branch="dev"),
        ],
    )
    await session.commit()


async def add_nodes(
    session: AsyncSession,
    user: User,
    names: list[str],
    node_type: NodeType = NodeType.TRANSFORM,
    deactivated: bool = False,
) -> None:
    """
    Persists bare nodes with the given names.
    """
    session.add_all(
        [
            Node(
                name=name,
                type=node_type,
                created_by_id=user.id,
                deactivated_at=datetime.now(UTC) if deactivated else None,
            )
            for name in names
        ],
    )
    await session.commit()


@pytest.mark.asyncio
class TestParentNodeNames:
    """
    Tests for ``parent_node_names``.
    """

    async def test_maps_branch_names_to_default_branch(
        self,
        session: AsyncSession,
        namespaces: None,
    ):
        """
        Multi-segment suffixes carry over to the default branch.
        """
        assert await parent_node_names(
            session,
            ["shared.feat_x.orders", "shared.feat_x.member.metrics.orders"],
        ) == {
            "shared.feat_x.orders": "shared.main.orders",
            "shared.feat_x.member.metrics.orders": "shared.main.member.metrics.orders",
        }

    async def test_names_without_namespace_need_no_query(
        self,
        session: AsyncSession,
    ):
        """
        Bare names have no namespace to resolve.
        """
        assert await parent_node_names(session, ["orphan"]) == {}

    async def test_many_namespaces_use_one_query(
        self,
        session: AsyncSession,
        namespaces: None,
    ):
        """
        Names across many namespaces share one query, not one each.
        """
        names = [f"shared.feat_x.team_{i}.orders" for i in range(20)]
        statements: list[str] = []

        def record(conn, cursor, statement, parameters, context, executemany):
            statements.append(statement)

        engine = session.get_bind()
        event.listen(engine, "before_cursor_execute", record)
        try:
            parents = await parent_node_names(session, names)
        finally:
            event.remove(engine, "before_cursor_execute", record)
        assert parents == {
            name: name.replace("shared.feat_x", "shared.main") for name in names
        }
        assert len(statements) == 1

    async def test_loads_root_outside_branch_hierarchy(
        self,
        session: AsyncSession,
    ):
        """
        A branch can link to a git root that is not one of its ancestors.
        """
        session.add(
            NodeNamespace(
                namespace="elsewhere",
                github_repo_path="org/elsewhere",
                default_branch="trunk",
            ),
        )
        await session.flush()
        session.add(
            NodeNamespace(
                namespace="team.work",
                git_branch="work",
                parent_namespace="elsewhere",
            ),
        )
        await session.commit()
        assert await parent_node_names(session, ["team.work.orders"]) == {
            "team.work.orders": "elsewhere.trunk.orders",
        }

    async def test_skips_default_branch_and_plain_namespaces(
        self,
        session: AsyncSession,
        namespaces: None,
    ):
        """
        Only names inside a non-default branch have a parent.
        """
        assert (
            await parent_node_names(
                session,
                [
                    "shared.main.orders",
                    "plain.orders",
                    "unknown.orders",
                    "solo.dev.orders",
                    "orphan",
                ],
            )
            == {}
        )


@pytest.mark.asyncio
class TestInheritedNames:
    """
    Tests for ``inherited_names``.
    """

    async def test_leaves_out_names_the_branch_holds(
        self,
        session: AsyncSession,
        user: User,
        namespaces: None,
    ):
        """
        Active and deactivated branch rows both keep their name from falling back.
        """
        await add_nodes(session, user, ["shared.feat_x.kept"])
        await add_nodes(session, user, ["shared.feat_x.deleted"], deactivated=True)
        assert await inherited_names(
            session,
            [
                "shared.feat_x.kept",
                "shared.feat_x.deleted",
                "shared.feat_x.absent",
                "plain.absent",
            ],
        ) == {"shared.feat_x.absent": "shared.main.absent"}

    async def test_no_branch_names(self, session: AsyncSession, namespaces: None):
        """
        Names outside a branch have nothing to inherit.
        """
        assert await inherited_names(session, ["plain.orders"]) == {}


@pytest.mark.asyncio
class TestInheritedLookup:
    """
    Tests for the ``inherit`` option on the ``Node`` lookups.
    """

    async def test_get_by_name_hit_is_unchanged(
        self,
        session: AsyncSession,
        user: User,
        namespaces: None,
    ):
        """
        A node the branch holds is returned even when main has one too.
        """
        await add_nodes(session, user, ["shared.main.orders", "shared.feat_x.orders"])
        node = await Node.get_by_name(session, "shared.feat_x.orders", inherit=True)
        assert node is not None
        assert node.name == "shared.feat_x.orders"

    async def test_get_by_name_falls_back_on_miss(
        self,
        session: AsyncSession,
        user: User,
        namespaces: None,
    ):
        """
        A name missing from the branch resolves to main's node.
        """
        await add_nodes(session, user, ["shared.main.orders"])
        node = await Node.get_by_name(session, "shared.feat_x.orders", inherit=True)
        assert node is not None
        assert node.name == "shared.main.orders"

    async def test_get_by_name_is_exact_by_default(
        self,
        session: AsyncSession,
        user: User,
        namespaces: None,
    ):
        """
        Without ``inherit`` a missing name stays missing.
        """
        await add_nodes(session, user, ["shared.main.orders"])
        assert await Node.get_by_name(session, "shared.feat_x.orders") is None

    async def test_get_by_name_deactivated_row_hides_main(
        self,
        session: AsyncSession,
        user: User,
        namespaces: None,
    ):
        """
        A node deleted in the branch does not fall back to main's node.
        """
        await add_nodes(session, user, ["shared.main.orders"])
        await add_nodes(session, user, ["shared.feat_x.orders"], deactivated=True)
        assert (
            await Node.get_by_name(session, "shared.feat_x.orders", inherit=True)
            is None
        )

    async def test_get_by_name_miss_outside_branch(
        self,
        session: AsyncSession,
        user: User,
        namespaces: None,
    ):
        """
        A miss in a plain namespace has nothing to inherit from.
        """
        await add_nodes(session, user, ["shared.main.orders"])
        assert await Node.get_by_name(session, "plain.orders", inherit=True) is None

    async def test_get_by_name_missing_everywhere(
        self,
        session: AsyncSession,
        namespaces: None,
    ):
        """
        A name main lacks too is still missing.
        """
        assert (
            await Node.get_by_name(session, "shared.feat_x.orders", inherit=True)
            is None
        )

    async def test_get_by_name_inactive_parent_follows_flag(
        self,
        session: AsyncSession,
        user: User,
        namespaces: None,
    ):
        """
        A deactivated node in main only resolves with ``include_inactive``.
        """
        await add_nodes(session, user, ["shared.main.orders"], deactivated=True)
        assert (
            await Node.get_by_name(session, "shared.feat_x.orders", inherit=True)
            is None
        )
        node = await Node.get_by_name(
            session,
            "shared.feat_x.orders",
            include_inactive=True,
            inherit=True,
        )
        assert node is not None
        assert node.name == "shared.main.orders"

    async def test_get_by_name_rejects_locking_inherited(
        self,
        session: AsyncSession,
    ):
        """
        An inherited node cannot be locked for update.
        """
        with pytest.raises(ValueError) as exc_info:
            await Node.get_by_name(
                session,
                "shared.feat_x.orders",
                for_update=True,
                inherit=True,
            )
        assert str(exc_info.value) == "An inherited node cannot be locked for update."

    async def test_get_by_name_raises_when_not_found(
        self,
        session: AsyncSession,
        namespaces: None,
    ):
        """
        ``raise_if_not_exists`` applies after the fallback finds nothing.
        """
        with pytest.raises(Exception) as exc_info:
            await Node.get_by_name(
                session,
                "shared.feat_x.orders",
                raise_if_not_exists=True,
                inherit=True,
            )
        assert exc_info.value.message == (
            "A node with name `shared.feat_x.orders` does not exist."
        )

    async def test_get_by_names_mixes_hits_and_fallbacks(
        self,
        session: AsyncSession,
        user: User,
        namespaces: None,
    ):
        """
        Hits, fallbacks, tombstones and misses resolve together, in input order.
        """
        await add_nodes(
            session,
            user,
            [
                "shared.main.a",
                "shared.main.b",
                "shared.main.c",
                "shared.feat_x.b",
                "plain.d",
            ],
        )
        await add_nodes(session, user, ["shared.feat_x.c"], deactivated=True)
        nodes = await Node.get_by_names(
            session,
            [
                "plain.d",
                "shared.feat_x.a",
                "shared.feat_x.b",
                "shared.feat_x.c",
                "shared.feat_x.missing",
            ],
            inherit=True,
        )
        assert [node.name for node in nodes] == [
            "plain.d",
            "shared.main.a",
            "shared.feat_x.b",
        ]

    async def test_get_by_names_is_exact_by_default(
        self,
        session: AsyncSession,
        user: User,
        namespaces: None,
    ):
        """
        Without ``inherit`` only exact names come back.
        """
        await add_nodes(session, user, ["shared.main.a", "shared.feat_x.b"])
        nodes = await Node.get_by_names(
            session,
            ["shared.feat_x.a", "shared.feat_x.b"],
        )
        assert [node.name for node in nodes] == ["shared.feat_x.b"]

    async def test_get_cube_by_name_falls_back_on_miss(
        self,
        session: AsyncSession,
        user: User,
        namespaces: None,
    ):
        """
        A cube missing from the branch resolves to main's cube.
        """
        await add_nodes(
            session,
            user,
            ["shared.main.cube", "shared.main.other_cube"],
            node_type=NodeType.CUBE,
        )
        await add_nodes(
            session,
            user,
            ["shared.feat_x.other_cube"],
            node_type=NodeType.CUBE,
        )
        cube = await Node.get_cube_by_name(session, "shared.feat_x.cube", inherit=True)
        assert cube is not None
        assert cube.name == "shared.main.cube"
        own = await Node.get_cube_by_name(
            session,
            "shared.feat_x.other_cube",
            inherit=True,
        )
        assert own is not None
        assert own.name == "shared.feat_x.other_cube"
        assert await Node.get_cube_by_name(session, "shared.feat_x.cube") is None

    async def test_hit_costs_no_extra_queries(
        self,
        session: AsyncSession,
        user: User,
        namespaces: None,
    ):
        """
        Asking to inherit adds no queries when the branch has the node.
        """
        await add_nodes(session, user, ["shared.feat_x.orders"])
        statements: list[str] = []

        def record(conn, cursor, statement, parameters, context, executemany):
            statements.append(statement)

        engine = session.get_bind()
        event.listen(engine, "before_cursor_execute", record)
        try:
            await Node.get_by_name(session, "shared.feat_x.orders")
            exact = len(statements)
            statements.clear()
            session.expire_all()
            await Node.get_by_name(session, "shared.feat_x.orders", inherit=True)
            inherited = len(statements)
        finally:
            event.remove(engine, "before_cursor_execute", record)
        assert exact == inherited


@pytest.mark.asyncio
class TestGitInfoActiveOnly:
    """
    Tests for hiding deactivated namespaces when resolving git info.
    """

    async def test_deactivated_root_is_hidden(self, session: AsyncSession):
        """
        A deactivated git root outside the branch's hierarchy gives no git info.
        """
        session.add(
            NodeNamespace(
                namespace="elsewhere",
                github_repo_path="org/elsewhere",
                default_branch="trunk",
                deactivated_at=datetime.now(UTC),
            ),
        )
        await session.flush()
        session.add(
            NodeNamespace(
                namespace="team.work",
                git_branch="work",
                parent_namespace="elsewhere",
            ),
        )
        await session.commit()
        assert (
            await get_git_info_for_namespace(
                session,
                "team.work",
                active_only=True,
            )
            is None
        )
        info = await get_git_info_for_namespace(session, "team.work")
        assert info is not None
        assert info["default_branch"] == "trunk"
