"""
Tests for resolving branch node names through the default branch.
"""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio
from sqlalchemy import event
from sqlalchemy.orm import load_only
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.attributetype import AttributeType, ColumnAttribute
from datajunction_server.database.catalog import Catalog
from datajunction_server.database.column import Column
from datajunction_server.database.engine import Engine
from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.node import Node, NodeRevision, NodeType
from datajunction_server.database.user import OAuthProvider, User
from datajunction_server.internal.branch_scope import (
    fetch_with_inherited,
    inherited_names,
    parent_node_names,
)
from datajunction_server.errors import DJNodeNotFound
from datajunction_server.internal.client import (
    python_client_code_for_setting_column_attributes,
)
from datajunction_server.internal.cube_materializations import _display_names
from datajunction_server.internal.namespaces import get_git_info_for_namespace
from datajunction_server.internal.sql import (
    build_node_sql,
    generate_dimensions_sql,
    generate_metrics_sql,
)
from datajunction_server.models.dialect import Dialect
from datajunction_server.sql.parsing.types import StringType


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

    async def test_get_by_name_loads_name_with_narrow_options(
        self,
        session: AsyncSession,
        user: User,
        namespaces: None,
    ):
        """
        An inherited node has its name loaded even when the caller loads only ids.
        """
        await add_nodes(session, user, ["shared.main.orders"])
        node = await Node.get_by_name(
            session,
            "shared.feat_x.orders",
            options=[load_only(Node.id)],
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


async def add_revised_nodes(
    session: AsyncSession,
    user: User,
    display_names: dict[str, str],
    deactivated: bool = False,
) -> None:
    """
    Persists nodes with a current revision carrying the given display names.
    """
    for name, display_name in display_names.items():
        node = Node(
            name=name,
            type=NodeType.METRIC,
            current_version="v1",
            created_by_id=user.id,
            deactivated_at=datetime.now(UTC) if deactivated else None,
        )
        session.add(node)
        await session.flush()
        session.add(
            NodeRevision(
                node_id=node.id,
                name=name,
                display_name=display_name,
                type=NodeType.METRIC,
                version="v1",
                created_by_id=user.id,
            ),
        )
    await session.commit()


@pytest.mark.asyncio
class TestFetchWithInherited:
    """
    Tests for ``fetch_with_inherited`` through the cube display name lookup.
    """

    async def fetch(self, session: AsyncSession, names: list[str]) -> dict[str, str]:
        """
        Looks up display names, falling back to the default branch.
        """
        return await fetch_with_inherited(
            session,
            names,
            lambda asked: _display_names(session, asked),
        )

    async def test_hit_is_unchanged(
        self,
        session: AsyncSession,
        user: User,
        namespaces: None,
    ):
        """
        A metric the branch holds keeps its own display name.
        """
        await add_revised_nodes(
            session,
            user,
            {"shared.main.m": "Main M", "shared.feat_x.m": "Branch M"},
        )
        assert await self.fetch(session, ["shared.feat_x.m"]) == {
            "shared.feat_x.m": "Branch M",
        }

    async def test_falls_back_on_miss(
        self,
        session: AsyncSession,
        user: User,
        namespaces: None,
    ):
        """
        A metric absent from the branch takes main's display name.
        """
        await add_revised_nodes(
            session,
            user,
            {"shared.main.m": "Main M", "shared.feat_x.n": "Branch N", "plain.p": "P"},
        )
        assert await self.fetch(
            session,
            ["shared.feat_x.m", "shared.feat_x.n", "plain.p", "plain.q"],
        ) == {
            "shared.feat_x.m": "Main M",
            "shared.feat_x.n": "Branch N",
            "plain.p": "P",
        }

    async def test_deactivated_row_blocks_fallback(
        self,
        session: AsyncSession,
        user: User,
        namespaces: None,
    ):
        """
        A metric deleted in the branch does not fall back to main.
        """
        await add_revised_nodes(session, user, {"shared.main.m": "Main M"})
        await add_nodes(session, user, ["shared.feat_x.m"], deactivated=True)
        assert await self.fetch(session, ["shared.feat_x.m"]) == {}

    async def test_missing_in_main_too(
        self,
        session: AsyncSession,
        namespaces: None,
    ):
        """
        A name main lacks as well stays missing.
        """
        assert await self.fetch(session, ["shared.feat_x.m"]) == {}


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


async def add_full_node(
    session: AsyncSession,
    user: User,
    name: str,
    node_type: NodeType,
    deactivated: bool = False,
    **revision_fields,
) -> None:
    """
    Persists a node with a current revision carrying the given fields.
    """
    node = Node(
        name=name,
        type=node_type,
        current_version="v1",
        created_by_id=user.id,
        deactivated_at=datetime.now(UTC) if deactivated else None,
    )
    session.add(node)
    await session.flush()
    session.add(
        NodeRevision(
            node_id=node.id,
            name=name,
            type=node_type,
            version="v1",
            created_by_id=user.id,
            **revision_fields,
        ),
    )
    await session.commit()


async def add_catalog(session: AsyncSession, name: str, dialect: Dialect) -> Catalog:
    """
    Persists a catalog with one engine of the given dialect.
    """
    catalog = Catalog(
        name=name,
        engines=[Engine(name=name, version="1", uri="x://", dialect=dialect)],
    )
    session.add(catalog)
    await session.commit()
    return catalog


@pytest.mark.asyncio
class TestClientCodeInherits:
    """
    Tests for column attribute client code on a branch node.
    """

    async def add_orders(
        self,
        session: AsyncSession,
        user: User,
        name: str,
        attribute: AttributeType,
        deactivated: bool = False,
    ) -> None:
        """
        Persists a node with one column carrying the attribute.
        """
        await add_full_node(
            session,
            user,
            name,
            NodeType.TRANSFORM,
            deactivated=deactivated,
            columns=[
                Column(
                    name="c",
                    type=StringType(),
                    order=0,
                    attributes=[ColumnAttribute(attribute_type=attribute)],
                ),
            ],
        )

    @pytest_asyncio.fixture
    async def attributes(self, session: AsyncSession) -> dict[str, AttributeType]:
        """
        Two attribute types, one for main and one for the branch.
        """
        types = {
            name: AttributeType(namespace="system", name=name, description=name)
            for name in ("from_main", "from_branch")
        }
        session.add_all(types.values())
        await session.commit()
        return types

    @staticmethod
    def code(attribute: str) -> str:
        """
        The expected client code for a column with one attribute.
        """
        return (
            "orders.set_column_attributes(\n"
            '    "c",\n'
            "    [\n"
            f'        ColumnAttribute(namespace="system", name="{attribute}"),\n'
            "    ],\n"
            ")"
        )

    async def test_falls_back_on_miss(
        self,
        session: AsyncSession,
        user: User,
        namespaces: None,
        attributes: dict[str, AttributeType],
    ):
        """
        A node absent from the branch uses main's attributes.
        """
        await self.add_orders(
            session,
            user,
            "shared.main.orders",
            attributes["from_main"],
        )
        assert await python_client_code_for_setting_column_attributes(
            session,
            "shared.feat_x.orders",
        ) == self.code("from_main")

    async def test_hit_is_unchanged(
        self,
        session: AsyncSession,
        user: User,
        namespaces: None,
        attributes: dict[str, AttributeType],
    ):
        """
        A node the branch holds keeps its own attributes.
        """
        await self.add_orders(
            session,
            user,
            "shared.main.orders",
            attributes["from_main"],
        )
        await self.add_orders(
            session,
            user,
            "shared.feat_x.orders",
            attributes["from_branch"],
        )
        assert await python_client_code_for_setting_column_attributes(
            session,
            "shared.feat_x.orders",
        ) == self.code("from_branch")

    async def test_deactivated_row_blocks_fallback(
        self,
        session: AsyncSession,
        user: User,
        namespaces: None,
        attributes: dict[str, AttributeType],
    ):
        """
        A node deleted in the branch stays missing.
        """
        await self.add_orders(
            session,
            user,
            "shared.main.orders",
            attributes["from_main"],
        )
        await self.add_orders(
            session,
            user,
            "shared.feat_x.orders",
            attributes["from_branch"],
            deactivated=True,
        )
        with pytest.raises(DJNodeNotFound) as exc_info:
            await python_client_code_for_setting_column_attributes(
                session,
                "shared.feat_x.orders",
            )
        assert exc_info.value.message == (
            "A node with name `shared.feat_x.orders` does not exist."
        )


@pytest.mark.asyncio
class TestSqlInherits:
    """
    Tests for the SQL builders on branch nodes and cubes.
    """

    @staticmethod
    async def node_sql_dialect(session: AsyncSession) -> Dialect | None:
        """
        The dialect ``build_node_sql`` built with for the branch node.
        """
        build = AsyncMock(
            return_value=SimpleNamespace(sql="SELECT 1", columns=[], warnings=[]),
        )
        with patch(
            "datajunction_server.construction.build_v3.node_query.build_node_sql_v3",
            build,
        ):
            translated = await build_node_sql(
                session,
                "shared.feat_x.orders",
                access_checker=None,  # type: ignore[arg-type]
            )
        assert build.call_args.kwargs["dialect"] == translated.dialect
        return translated.dialect

    async def test_node_sql_uses_catalog_of_inherited_node(
        self,
        session: AsyncSession,
        user: User,
        namespaces: None,
    ):
        """
        A node absent from the branch builds with main's engine.
        """
        spark = await add_catalog(session, "spark_catalog", Dialect.SPARK)
        await add_full_node(
            session,
            user,
            "shared.main.orders",
            NodeType.TRANSFORM,
            catalog_id=spark.id,
        )
        assert await self.node_sql_dialect(session) == Dialect.SPARK

    async def test_node_sql_hit_is_unchanged(
        self,
        session: AsyncSession,
        user: User,
        namespaces: None,
    ):
        """
        A node the branch holds builds with its own engine.
        """
        spark = await add_catalog(session, "spark_catalog", Dialect.SPARK)
        trino = await add_catalog(session, "trino_catalog", Dialect.TRINO)
        await add_full_node(
            session,
            user,
            "shared.main.orders",
            NodeType.TRANSFORM,
            catalog_id=spark.id,
        )
        await add_full_node(
            session,
            user,
            "shared.feat_x.orders",
            NodeType.TRANSFORM,
            catalog_id=trino.id,
        )
        assert await self.node_sql_dialect(session) == Dialect.TRINO

    async def test_node_sql_deactivated_row_blocks_fallback(
        self,
        session: AsyncSession,
        user: User,
        namespaces: None,
    ):
        """
        A node deleted in the branch stays missing.
        """
        spark = await add_catalog(session, "spark_catalog", Dialect.SPARK)
        await add_full_node(
            session,
            user,
            "shared.main.orders",
            NodeType.TRANSFORM,
            catalog_id=spark.id,
        )
        await add_nodes(session, user, ["shared.feat_x.orders"], deactivated=True)
        with pytest.raises(DJNodeNotFound) as exc_info:
            await build_node_sql(
                session,
                "shared.feat_x.orders",
                access_checker=None,  # type: ignore[arg-type]
            )
        assert exc_info.value.message == (
            "A node with name `shared.feat_x.orders` does not exist."
        )

    @staticmethod
    async def metrics_filters(session: AsyncSession) -> list[str]:
        """
        The filters ``generate_metrics_sql`` merged for the cube it found.
        """
        prepare = AsyncMock(
            return_value=SimpleNamespace(
                dialect=Dialect.SPARK,
                filters=SimpleNamespace(filters=[]),
                execution=None,
            ),
        )
        build = AsyncMock(return_value=SimpleNamespace(warnings=[]))
        with (
            patch(
                "datajunction_server.construction.build_v3.prepare_metrics_query",
                prepare,
            ),
            patch("datajunction_server.construction.build_v3.build_metrics_sql", build),
        ):
            await generate_metrics_sql(
                session,
                metrics=["m"],
                dimensions=[],
                cube="shared.feat_x.cube",
            )
        return prepare.call_args.args[3]

    async def test_metrics_sql_falls_back_to_main_cube(
        self,
        session: AsyncSession,
        user: User,
        namespaces: None,
    ):
        """
        A cube absent from the branch contributes main's cube filters.
        """
        await add_full_node(
            session,
            user,
            "shared.main.cube",
            NodeType.CUBE,
            cube_filters=["main_filter"],
        )
        assert await self.metrics_filters(session) == ["main_filter"]

    async def test_metrics_sql_cube_hit_is_unchanged(
        self,
        session: AsyncSession,
        user: User,
        namespaces: None,
    ):
        """
        A cube the branch holds contributes its own filters.
        """
        await add_full_node(
            session,
            user,
            "shared.main.cube",
            NodeType.CUBE,
            cube_filters=["main_filter"],
        )
        await add_full_node(
            session,
            user,
            "shared.feat_x.cube",
            NodeType.CUBE,
            cube_filters=["branch_filter"],
        )
        assert await self.metrics_filters(session) == ["branch_filter"]

    async def test_metrics_sql_deactivated_cube_blocks_fallback(
        self,
        session: AsyncSession,
        user: User,
        namespaces: None,
    ):
        """
        A cube deleted in the branch is still the one found, never main's.
        """
        await add_full_node(
            session,
            user,
            "shared.main.cube",
            NodeType.CUBE,
            cube_filters=["main_filter"],
        )
        await add_full_node(
            session,
            user,
            "shared.feat_x.cube",
            NodeType.CUBE,
            deactivated=True,
            cube_filters=["branch_filter"],
        )
        assert await self.metrics_filters(session) == ["branch_filter"]

    @staticmethod
    async def pinned_cube(session: AsyncSession) -> str | None:
        """
        The name of the cube revision ``generate_dimensions_sql`` pinned.
        """
        generate = AsyncMock(return_value="generated")
        with (
            patch("datajunction_server.internal.sql.generate_metrics_sql", generate),
            patch(
                "datajunction_server.construction.build_v3.node_query."
                "project_dimension_values_sql",
                lambda generated, dimensions, orderby, limit: generated,
            ),
        ):
            assert (
                await generate_dimensions_sql(
                    session,
                    dimensions=[],
                    cube="shared.feat_x.cube",
                )
                == "generated"
            )
        matched = generate.call_args.kwargs["matched_cube"]
        return matched.name if matched else None

    async def test_dimensions_sql_falls_back_to_main_cube(
        self,
        session: AsyncSession,
        user: User,
        namespaces: None,
    ):
        """
        A cube absent from the branch is pinned from main.
        """
        await add_full_node(session, user, "shared.main.cube", NodeType.CUBE)
        assert await self.pinned_cube(session) == "shared.main.cube"

    async def test_dimensions_sql_cube_hit_is_unchanged(
        self,
        session: AsyncSession,
        user: User,
        namespaces: None,
    ):
        """
        A cube the branch holds is the one pinned.
        """
        await add_full_node(session, user, "shared.main.cube", NodeType.CUBE)
        await add_full_node(session, user, "shared.feat_x.cube", NodeType.CUBE)
        assert await self.pinned_cube(session) == "shared.feat_x.cube"

    async def test_dimensions_sql_deactivated_cube_blocks_fallback(
        self,
        session: AsyncSession,
        user: User,
        namespaces: None,
    ):
        """
        A cube deleted in the branch is still the one pinned, never main's.
        """
        await add_full_node(session, user, "shared.main.cube", NodeType.CUBE)
        await add_full_node(
            session,
            user,
            "shared.feat_x.cube",
            NodeType.CUBE,
            deactivated=True,
        )
        assert await self.pinned_cube(session) == "shared.feat_x.cube"
