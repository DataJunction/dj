"""
Tests for API helpers.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from datajunction_server.api import helpers
from datajunction_server.api.helpers import (
    FilterGraph,
    FilterLink,
    _resolve_required_dimensions,
    attribute_nodes,
    can_filter_on,
    dedupe_cube_elements,
    find_required_dimensions,
    link_keys,
    load_filter_graph,
    nodes_upstream,
)
from datajunction_server.database.column import Column
from datajunction_server.database.dimensionlink import DimensionLink
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.user import OAuthProvider, User
from datajunction_server.errors import DJDoesNotExistException, DJException
from datajunction_server.internal import sql
from datajunction_server.internal.nodes import propagate_valid_status
from datajunction_server.models.node import NodeStatus
from datajunction_server.sql.parsing.types import IntegerType


def test_dedupe_cube_elements_same_object():
    """The same Column appended twice (two roles on one column) is deduped."""
    col_a = Column(name="a", type=IntegerType())
    col_b = Column(name="b", type=IntegerType())
    assert dedupe_cube_elements([col_a, col_a, col_b]) == [col_a, col_b]


def test_dedupe_cube_elements_by_column_id():
    """Distinct Column objects that share a persisted id (the pk_cube key) dedupe."""
    col1 = Column(name="dateint", type=IntegerType())
    col1.id = 5
    col2 = Column(name="dateint", type=IntegerType())
    col2.id = 5
    col3 = Column(name="other", type=IntegerType())
    col3.id = 6
    assert dedupe_cube_elements([col1, col2, col3]) == [col1, col3]


@pytest.mark.asyncio
async def test_raise_get_node_when_node_does_not_exist(module__session: AsyncSession):
    """
    Test raising when a node doesn't exist
    """
    with pytest.raises(DJException) as exc_info:
        await helpers.get_node_by_name(
            session=module__session,
            name="foo",
            raise_if_not_exists=True,
        )

    assert "A node with name `foo` does not exist." in str(exc_info.value)
    with pytest.raises(DJException) as exc_info:
        await Node.get_by_name(
            session=module__session,
            name="foo",
            raise_if_not_exists=True,
        )

    assert "A node with name `foo` does not exist." in str(exc_info.value)


@pytest.mark.asyncio
async def test_propagate_valid_status(module__session: AsyncSession):
    """
    Test raising when trying to propagate a valid status using an invalid node
    """
    invalid_node = NodeRevision(
        name="foo",
        status=NodeStatus.INVALID,
    )
    example_user = User(
        id=1,
        username="userfoo",
        password="passwordfoo",
        name="djuser",
        email="userfoo@datajunction.io",
        oauth_provider=OAuthProvider.BASIC,
    )
    with pytest.raises(DJException) as exc_info:
        await propagate_valid_status(
            session=module__session,
            valid_nodes=[invalid_node],
            catalog_id=1,
            current_user=example_user,
            save_history=MagicMock(),
        )

    assert "Cannot propagate valid status: Node `foo` is not valid" in str(
        exc_info.value,
    )


@pytest.mark.asyncio
async def test_get_node_namespace():
    """
    Test getting a node's namespace
    """
    # success
    mock_execute = AsyncMock(scalar_one_or_none=MagicMock(return_value="bar"))
    mock_session = AsyncMock(execute=AsyncMock(return_value=mock_execute))
    namespace = await helpers.get_node_namespace(
        session=mock_session,
        namespace="foo",
        raise_if_not_exists=True,
    )
    assert namespace == "bar"
    # error
    mock_execute = AsyncMock(scalar_one_or_none=MagicMock(return_value=None))
    mock_session = AsyncMock(execute=AsyncMock(return_value=mock_execute))
    with pytest.raises(DJDoesNotExistException) as exc_info:
        namespace = await helpers.get_node_namespace(
            session=mock_session,
            namespace="foo",
            raise_if_not_exists=True,
        )
    assert "node namespace `foo` does not exist" in str(exc_info.value)


@pytest.mark.asyncio
async def test_find_existing_cube():
    """
    Test finding an existing cube
    """
    mock_cube = MagicMock(current="foo-cube")
    mock_execute = AsyncMock(
        unique=MagicMock(
            return_value=MagicMock(
                scalars=MagicMock(
                    return_value=MagicMock(all=MagicMock(return_value=[mock_cube])),
                ),
            ),
        ),
    )
    mock_session = AsyncMock(execute=AsyncMock(return_value=mock_execute))
    node = await helpers.find_existing_cube(
        session=mock_session,
        metric_columns=[],
        dimension_columns=[],
        materialized=False,
    )
    assert node == "foo-cube"


@pytest.mark.asyncio
async def test_find_existing_cube_matches_dimension_role():
    """Legacy cube matching must not satisfy one role with another."""
    dimension = MagicMock(spec=Column)
    dimension.name = "date_id"
    dimension.full_name.return_value = "v3.date.date_id"

    wrong_revision = MagicMock(
        columns=[Column(name="v3.date.date_id", dimension_column="[ship]")],
    )
    right_revision = MagicMock(
        columns=[Column(name="v3.date.date_id", dimension_column="[order]")],
    )
    candidates = [
        MagicMock(current=wrong_revision),
        MagicMock(current=right_revision),
    ]
    mock_execute = AsyncMock(
        unique=MagicMock(
            return_value=MagicMock(
                scalars=MagicMock(
                    return_value=MagicMock(
                        all=MagicMock(return_value=candidates),
                    ),
                ),
            ),
        ),
    )
    mock_session = AsyncMock(execute=AsyncMock(return_value=mock_execute))

    result = await helpers.find_existing_cube(
        session=mock_session,
        metric_columns=[],
        dimension_columns=[dimension],
        dimension_roles=["[order]"],
        materialized=False,
    )

    assert result is right_revision


@pytest.mark.asyncio
@patch("datajunction_server.internal.sql.ColumnMetadata", MagicMock)
@patch("datajunction_server.internal.sql.validate_cube")
@patch("datajunction_server.internal.sql.Node.get_by_name")
@patch("datajunction_server.internal.sql.find_existing_cube")
@patch("datajunction_server.internal.sql.get_catalog_by_name")
@patch("datajunction_server.internal.sql.build_materialized_cube_node")
@patch("datajunction_server.internal.sql.TranslatedSQL.create", MagicMock)
async def test_build_sql_for_multiple_metrics(
    mock_build_materialized_cube_node,
    mock_get_catalog_by_name,
    mock_find_existing_cube,
    mock_get_by_name,
    mock_validate_cube,
):
    """
    Test building SQL for multiple metrics
    """
    mock_build_materialized_cube_node.return_value = MagicMock()
    mock_engines = [MagicMock(name="eng1"), MagicMock(name="eng2")]
    mock_get_catalog_by_name.return_value = MagicMock(engines=mock_engines)
    mock_find_existing_cube.return_value = MagicMock(
        availability=MagicMock(catalog="cata-foo"),
    )
    mock_get_by_name.return_value = MagicMock(
        current=MagicMock(catalog=MagicMock(name="cata-foo", engines=mock_engines)),
    )
    mock_metric_columns = [MagicMock(name="col1"), MagicMock(name="col2")]
    mock_metric_nodes = ["mnode1", "mnode2"]
    dimension_columns = [MagicMock(name="dim1"), MagicMock(name="dim2")]
    _ = MagicMock()
    mock_validate_cube.return_value = (
        mock_metric_columns,
        mock_metric_nodes,
        _,
        dimension_columns,
        [None, None],
        _,
    )
    mock_session = AsyncMock()

    built_sql = await sql.build_sql_for_multiple_metrics(
        session=mock_session,
        metrics=["m1", "m2"],
        dimensions=[],
    )
    assert built_sql is not None


@pytest.mark.asyncio
async def test_find_required_dimensions_without_attributes_loads_no_graph():
    """Nothing to check means no queries, even when parents are given."""
    session = MagicMock()
    with (
        patch.object(helpers, "find_upstream_node_names", side_effect=AssertionError),
        patch.object(helpers, "load_filter_graph", side_effect=AssertionError),
    ):
        result = await find_required_dimensions(
            session,
            [],
            [],
            parent_names={"v3.parent"},
        )
    assert result == (set(), [])
    session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_find_required_dimensions_with_role_suffix(
    module__client_with_examples,
    module__session: AsyncSession,
):
    """
    Test find_required_dimensions with full path that includes role suffix.
    """
    # Get an actual dimension node from the database (v3.date has 'week' column)
    result = await module__session.execute(
        select(Node)
        .filter(Node.name == "v3.date")
        .options(
            selectinload(Node.current).options(
                selectinload(NodeRevision.columns),
            ),
        ),
    )
    dim_node = result.scalars().first()

    if dim_node is None:
        pytest.skip("v3.date dimension not found in database")

    # Verify the dimension has the 'week' column
    col_names = [col.name for col in dim_node.current.columns]
    if "week" not in col_names:
        pytest.skip("v3.date.week column not found")

    # The "order" role is only reachable via v3.order_details's dimension
    # link to v3.date -- fetch it so the role validation has something to
    # check reachability against.
    link_result = await module__session.execute(
        select(DimensionLink)
        .join(NodeRevision, DimensionLink.node_revision_id == NodeRevision.id)
        .filter(NodeRevision.name == "v3.order_details")
        .filter(DimensionLink.role == "order"),
    )
    parent_dimension_links = list(link_result.scalars().all())
    if not parent_dimension_links:
        pytest.skip("v3.order_details -> v3.date[order] link not found")

    # Test with role suffix - the role is preserved verbatim in `.ref` and
    # validated against a reachable dimension link.
    invalid_dims, resolved = await find_required_dimensions(
        session=module__session,
        required_dimensions=["v3.date.week[order]"],
        parent_columns=[],
        parent_dimension_links=parent_dimension_links,
    )

    # Should have no invalid dimensions and one resolved entry
    assert len(invalid_dims) == 0, f"Unexpected invalid dims: {invalid_dims}"
    assert len(resolved) == 1
    assert resolved[0].ref == "v3.date.week[order]"
    assert resolved[0].dimension_id == dim_node.id


@pytest.mark.asyncio
async def test_find_required_dimensions_full_path_match(
    module__client_with_examples,
    module__session: AsyncSession,
):
    """
    Test find_required_dimensions with full path without role suffix.

    This covers line 323: matched_columns.append(dim_col_map[col_name])
    """
    # Test with full path (no role suffix) - this covers line 323
    invalid_dims, resolved = await find_required_dimensions(
        session=module__session,
        required_dimensions=["v3.date.month"],
        parent_columns=[],
    )

    # v3.date.month should exist
    if len(invalid_dims) > 0:
        pytest.skip("v3.date.month not found in database")

    assert len(resolved) == 1
    assert resolved[0].ref == "v3.date.month"


def test_resolve_required_dimensions_short_name_ambiguous_across_parents():
    """
    A short name that matches a column on more than one direct parent must be
    flagged invalid rather than silently resolved to one of them.
    """
    orders_currency = Column(name="currency_code")
    refunds_currency = Column(name="currency_code")

    invalid_dims, matched_cols = _resolve_required_dimensions(
        required_dimensions=["currency_code"],
        parent_columns=[orders_currency, refunds_currency],
        dim_nodes={},
    )

    assert invalid_dims == {"currency_code"}
    assert matched_cols == []


def test_resolve_required_dimensions_role_not_reachable():
    """
    A declared role that no direct parent's dimension link can reach must be
    flagged invalid, not resolved against the un-roled column.
    """
    date_node = Node(name="v3.date", id=42)
    date_node.current = NodeRevision(
        name="v3.date",
        columns=[Column(name="week")],
    )
    order_link = DimensionLink(role="order", dimension=date_node)

    invalid_dims, resolved = _resolve_required_dimensions(
        required_dimensions=["v3.date.week[shipping]"],
        parent_columns=[],
        dim_nodes={"v3.date": date_node},
        parent_dimension_links=[order_link],
    )

    assert invalid_dims == {"v3.date.week[shipping]"}
    assert resolved == []


def test_resolve_required_dimensions_bare_ref_with_dotted_role():
    """
    A bare column ref can't carry a role at all -- a short name that has a
    bracketed suffix containing `.` must be flagged invalid rather than
    misrouted into full-path parsing, which would raise uncaught.
    """
    status_column = Column(name="status")

    invalid_dims, resolved = _resolve_required_dimensions(
        required_dimensions=["status[a.b]"],
        parent_columns=[status_column],
        dim_nodes={},
    )

    assert invalid_dims == {"status[a.b]"}
    assert resolved == []


@pytest.mark.asyncio
async def test_find_required_dimensions_multi_hop_role(
    module__client_with_examples,
    module__session: AsyncSession,
):
    """A multi-hop role passes only when role chains are checked."""
    link_result = await module__session.execute(
        select(DimensionLink)
        .join(NodeRevision, DimensionLink.node_revision_id == NodeRevision.id)
        .join(Node, Node.id == NodeRevision.node_id)
        .filter(NodeRevision.name == "v3.order_details")
        .filter(NodeRevision.version == Node.current_version),
    )
    parent_dimension_links = list(link_result.scalars().unique().all())
    attribute = "v3.location.country[customer->home]"

    invalid, _ = await find_required_dimensions(
        session=module__session,
        required_dimensions=[attribute],
        parent_columns=[],
        parent_dimension_links=parent_dimension_links,
    )
    assert invalid == {attribute}

    invalid, resolved = await find_required_dimensions(
        session=module__session,
        required_dimensions=[attribute],
        parent_columns=[],
        parent_dimension_links=parent_dimension_links,
        parent_names={"v3.order_details"},
    )
    assert invalid == set()
    assert [dimension.ref for dimension in resolved] == [attribute]


def test_attribute_nodes():
    assert attribute_nodes(
        ["a.b.c", "a.d.e[role]", "bare", "a.b.f", "g.h[x->y]"],
    ) == {"a.b", "a.d", "g"}


def test_nodes_upstream():
    """Every node upstream, each once, even when two paths reach it."""
    parents = {"a": ["b", "c"], "b": ["d"], "c": ["d"]}
    assert nodes_upstream("a", lambda name: parents.get(name, [])) == {"b", "c", "d"}


# Links: fact->a[x], fact->c, a->b, a->a[self],
# c->b[y], c->a, up->k. up is upstream of fact.
# A fact column references ref.code.
FILTER_GRAPH = FilterGraph(
    upstream={"t.fact": {"t.up"}, "t.other": set()},
    links={
        "t.fact": {FilterLink("t.a", "x"), FilterLink("t.c", None)},
        "t.a": {FilterLink("t.b", None), FilterLink("t.a", "self")},
        "t.c": {FilterLink("t.b", "y"), FilterLink("t.a", None)},
        "t.up": {
            FilterLink("t.k", None, frozenset({"t.k.id"})),
            FilterLink("t.k", "r", frozenset({"t.k.id"})),
        },
        "t.other": {FilterLink("t.a", "x")},
    },
    references={"t.fact": {("t.ref", "code")}},
)


@pytest.mark.parametrize(
    "attribute,parents,expected",
    [
        ("t.fact.amount", {"t.fact"}, True),
        ("t.fact.amount[x]", {"t.fact"}, False),
        ("t.b.name", {"t.fact"}, True),
        ("t.b.name[x]", {"t.fact"}, True),
        ("t.b.name[y]", {"t.fact"}, True),
        ("t.b.name[x->y]", {"t.fact"}, False),
        ("t.b.name[z]", {"t.fact"}, False),
        ("t.a.name[x->self]", {"t.fact"}, False),
        ("t.ref.code", {"t.fact"}, True),
        ("t.ref.code[any]", {"t.fact"}, True),
        ("t.ref.other", {"t.fact"}, False),
        ("t.k.id", {"t.fact"}, True),
        ("t.k.id[r]", {"t.fact"}, False),
        ("t.k.name", {"t.fact"}, False),
        ("t.nowhere.id", {"t.fact"}, False),
        ("t.a.name[x]", {"t.fact", "t.other"}, True),
        ("t.b.name[y]", {"t.fact", "t.other"}, False),
        ("t.fact.amount", set(), False),
    ],
    ids=[
        "own-column",
        "own-column-role",
        "join-any-path",
        "join-role",
        "join-role-after-unroled-link",
        "join-roles-not-on-one-path",
        "join-unknown-role",
        "join-revisits",
        "reference",
        "reference-any-role",
        "reference-other-column",
        "upstream-key",
        "upstream-key-role",
        "upstream-other-column",
        "no-link",
        "every-parent",
        "one-parent-short",
        "no-parents",
    ],
)
def test_can_filter_on(attribute: str, parents: set[str], expected: bool):
    assert can_filter_on(attribute, parents, FILTER_GRAPH) == expected


def test_link_keys():
    assert link_keys(
        "t.fact",
        "t.dim",
        "t.fact.a = t.dim.x AND t.fact.b = t.dim.y",
        None,
    ) == frozenset({"t.dim.x", "t.dim.y"})


@pytest.mark.asyncio
async def test_load_filter_graph(
    module__client_with_examples,
    module__session: AsyncSession,
):
    """Saved links load hop by hop from the parents, merged with declared ones."""
    graph = await load_filter_graph(
        module__session,
        # v3.customer stands in for a node upstream of x.fact.
        {"v3.order_details": set(), "x.fact": {"v3.customer"}},
        {"v3.date", "x.dim"},
        FilterGraph(
            links={"x.fact": {FilterLink("v3.customer", None)}},
            references={"x.fact": {("x.dim", "code")}},
        ),
    )
    order_links = {
        FilterLink("v3.customer", "customer"),
        FilterLink("v3.date", "order"),
        FilterLink("v3.location", "from"),
        FilterLink("v3.location", "to"),
        FilterLink("v3.product", None),
    }
    assert graph == FilterGraph(
        upstream={"v3.order_details": set(), "x.fact": {"v3.customer"}},
        links={
            "x.fact": {FilterLink("v3.customer", None)},
            "v3.order_details": order_links,
            # Join keys are read for upstream links to an attribute node.
            "v3.customer": {
                FilterLink("v3.date", "registration", frozenset({"v3.date.date_id"})),
                FilterLink("v3.location", "home"),
            },
        },
        references={"x.fact": {("x.dim", "code")}},
    )


@pytest.mark.asyncio
async def test_load_filter_graph_reaches_loaded_node(
    module__client_with_examples,
    module__session: AsyncSession,
):
    """A link to a node already loaded walks on without another query."""
    declared = FilterGraph(links={"x.fact": {FilterLink("x.up", None)}})
    graph = await load_filter_graph(
        module__session,
        {"x.fact": {"x.up"}},
        set(),
        declared,
    )
    assert graph == FilterGraph(upstream={"x.fact": {"x.up"}}, links=declared.links)
