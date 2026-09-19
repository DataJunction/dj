import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.node import Node, NodeType
from datajunction_server.database.user import OAuthProvider, User

# Postgres/psycopg cap a statement at 65,535 bind parameters. A naive
# `Node.name.in_(names)` expands to one bind parameter per name, so this needs
# to comfortably clear that limit to prove the fix.
_OVER_PARAM_LIMIT = 70_000


@pytest_asyncio.fixture
async def a_user(session: AsyncSession) -> User:
    """
    A user to satisfy Node.created_by_id.
    """
    user = User(username="node_test_user", oauth_provider=OAuthProvider.BASIC)
    session.add(user)
    await session.commit()
    return user


@pytest_asyncio.fixture
async def a_node(session: AsyncSession, a_user: User) -> Node:
    """
    A single persisted node to look up amid a huge list of names.
    """
    node = Node(
        name="basic.source.node_test_target",
        type=NodeType.SOURCE,
        created_by_id=a_user.id,
    )
    session.add(node)
    await session.commit()
    return node


@pytest.mark.asyncio
class TestGetByNames:
    """
    Tests for ``Node.get_by_names``, in particular that it does not build a SQL
    statement with one bind parameter per requested name (which blows past
    Postgres/psycopg's 65,535 bind parameter cap for large inputs).
    """

    async def test_empty_list_returns_empty(self, session: AsyncSession):
        """
        Empty input short-circuits to an empty result without querying.
        """
        assert await Node.get_by_names(session, []) == []

    async def test_large_name_list_does_not_exceed_bind_parameter_limit(
        self,
        session: AsyncSession,
        a_node: Node,
    ):
        """
        A names list larger than Postgres's 65,535 bind parameter cap must not
        raise, and must still find the one real match. Mostly-nonexistent
        names are fine: the point is the parameter count, not the hit rate.
        """
        names = [f"missing.node.{i}" for i in range(_OVER_PARAM_LIMIT)]
        # Insert the real name in the middle so ordering is also exercised.
        insert_at = _OVER_PARAM_LIMIT // 2
        names.insert(insert_at, a_node.name)

        results = await Node.get_by_names(session, names, options=[])

        assert len(results) == 1
        assert results[0].name == a_node.name

    async def test_ordering_matches_input_order(
        self,
        session: AsyncSession,
        a_node: Node,
    ):
        """
        Results come back ordered to match the input names list, not database
        order, and names with no match are simply skipped.
        """
        other = Node(
            name="basic.source.node_test_other",
            type=NodeType.SOURCE,
            created_by_id=a_node.created_by_id,
        )
        session.add(other)
        await session.commit()

        names = [other.name, "does.not.exist", a_node.name]
        results = await Node.get_by_names(session, names, options=[])

        assert [node.name for node in results] == [other.name, a_node.name]
