from contextlib import asynccontextmanager
import random
from typing import AsyncGenerator
from unittest.mock import MagicMock
from uuid import uuid4
import pytest_asyncio
from datajunction_server.api.attributes import default_attribute_types
from datajunction_server.database.column import Column
from datajunction_server.database.catalog import Catalog
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.sql.parsing.types import IntegerType, StringType
from datajunction_server.database.user import User

from datajunction_server.models.node_type import NodeType
from datajunction_server.internal.deployment.orchestrator import (
    DeploymentOrchestrator,
    DeploymentSpec,
)
from datajunction_server.internal.deployment.utils import (
    DeploymentContext,
    extract_node_graph,
    topological_levels,
    _find_upstreams_for_node,
)
from datajunction_server.models.deployment import (
    DeploymentResult,
    NodeSpec,
    TransformSpec,
    SourceSpec,
    MetricSpec,
    DimensionSpec,
    CubeSpec,
)
from sqlalchemy.ext.asyncio import AsyncSession
from datajunction_server.errors import (
    DJGraphCycleException,
    DJInvalidDeploymentConfig,
)
from datajunction_server.database.node import Node
from datajunction_server.models.node import (
    NodeType,
)
import pytest


@pytest.fixture
def basic_nodes():
    """
    A basic set of nodes for testing
    """
    transform_node = TransformSpec(
        name="example.transform_node",
        node_type=NodeType.TRANSFORM,
        query="SELECT id, name FROM ${prefix}catalog.facts.clicks",
    )
    source_node = SourceSpec(
        name="catalog.facts.clicks",
        node_type=NodeType.SOURCE,
        catalog="catalog",
        schema="facts",
        table="clicks",
    )
    metric_node = MetricSpec(
        name="example.metric_node",
        node_type=NodeType.METRIC,
        query="SELECT SUM(value) FROM ${prefix}example.transform_node",
    )
    dimension_node = DimensionSpec(
        name="example.dimension_node",
        node_type=NodeType.DIMENSION,
        query="SELECT id, category FROM catalog.dim.categories",
        primary_key=["id"],
    )
    cube_node = CubeSpec(
        name="example.cube_node",
        node_type=NodeType.CUBE,
        metrics=["${prefix}example.metric_node"],
        dimensions=["${prefix}example.dimension_node.category"],
    )
    return [transform_node, source_node, metric_node, dimension_node, cube_node]


def test_extract_node_graph(basic_nodes):
    dag = extract_node_graph(basic_nodes)
    assert dag == {
        "catalog.facts.clicks": [],
        "example.cube_node": [
            "example.metric_node",
            "example.dimension_node",
        ],
        "example.dimension_node": [
            "catalog.dim.categories",
        ],
        "example.transform_node": ["catalog.facts.clicks"],
        "example.metric_node": ["example.transform_node"],
    }


def test_graph_complex():
    # Base source nodes
    clicks = SourceSpec(
        name="catalog.facts.clicks",
        node_type=NodeType.SOURCE,
        catalog="catalog",
        schema="facts",
        table="clicks",
    )
    users = SourceSpec(
        name="catalog.dim.users",
        node_type=NodeType.SOURCE,
        catalog="catalog",
        schema="dim",
        table="users",
    )

    # Transform nodes
    transform_clicks = TransformSpec(
        name="example.transform_clicks",
        node_type=NodeType.TRANSFORM,
        query="SELECT id, user_id FROM catalog.facts.clicks",
    )
    transform_users = TransformSpec(
        name="example.transform_users",
        node_type=NodeType.TRANSFORM,
        query="SELECT id, country FROM catalog.dim.users",
    )
    combined_transform = TransformSpec(
        name="example.combined_transform",
        node_type=NodeType.TRANSFORM,
        query="""
            SELECT c.id, c.user_id, u.country
            FROM example.transform_clicks c
            JOIN example.transform_users u ON c.user_id = u.id
        """,
    )

    # Metric nodes
    metric_total = MetricSpec(
        name="example.metric_total",
        node_type=NodeType.METRIC,
        query="SELECT SUM(amount) FROM example.combined_transform",
    )
    metric_per_country = MetricSpec(
        name="example.metric_per_country",
        node_type=NodeType.METRIC,
        query="SELECT country, SUM(amount) FROM example.combined_transform GROUP BY country",
    )

    nodes = [
        clicks,
        users,
        transform_clicks,
        transform_users,
        combined_transform,
        metric_total,
        metric_per_country,
    ]

    dag = extract_node_graph(nodes)

    expected_dag = {
        "catalog.dim.users": [],
        "catalog.facts.clicks": [],
        "example.transform_clicks": ["catalog.facts.clicks"],
        "example.transform_users": ["catalog.dim.users"],
        "example.combined_transform": [
            "example.transform_clicks",
            "example.transform_users",
        ],
        "example.metric_total": ["example.combined_transform"],
        "example.metric_per_country": ["example.combined_transform"],
    }

    assert dag == expected_dag
    assert topological_levels(dag) == [
        [
            "example.metric_per_country",
            "example.metric_total",
        ],
        [
            "example.combined_transform",
        ],
        [
            "example.transform_clicks",
            "example.transform_users",
        ],
        [
            "catalog.dim.users",
            "catalog.facts.clicks",
        ],
    ]


def test_topological_levels(basic_nodes):
    dag = extract_node_graph(basic_nodes)

    assert topological_levels(dag) == [
        ["example.cube_node"],
        ["example.dimension_node", "example.metric_node"],
        ["catalog.dim.categories", "example.transform_node"],
        ["catalog.facts.clicks"],
    ]
    assert topological_levels(dag, ascending=False) == [
        ["catalog.facts.clicks"],
        ["catalog.dim.categories", "example.transform_node"],
        ["example.dimension_node", "example.metric_node"],
        ["example.cube_node"],
    ]

    dag["catalog.facts.clicks"].append("example.cube_node")
    with pytest.raises(DJGraphCycleException):
        topological_levels(dag)


def generate_random_dag(num_nodes: int = 10, max_deps: int = 3):
    """
    Generate a random DAG of nodes for testing extract_node_graph.
    """
    nodes: list[NodeSpec] = []

    for i in range(num_nodes):
        node_type = random.choice(
            [NodeType.SOURCE, NodeType.TRANSFORM, NodeType.METRIC, NodeType.DIMENSION],
        )
        name = f"node_{i}"
        # dependencies can only point to previous nodes to avoid cycles
        possible_deps = [n.name for n in nodes]
        num_deps = random.randint(0, min(max_deps, len(possible_deps)))
        deps = random.sample(possible_deps, num_deps) if possible_deps else []

        if node_type == NodeType.SOURCE:
            nodes.append(SourceSpec(name=name, node_type=node_type, table=f"table_{i}"))
        else:
            # build query referencing dependencies (simplified)
            if deps:
                query = f"SELECT 1 AS col, 2 AS col2 FROM {deps[0]}"  # reference first dep as table
                if len(deps) > 1:
                    query += "".join(
                        f" JOIN {dep} ON 1=1" for dep in deps[1:]
                    )  # join others
            else:
                query = "SELECT 1 AS dummy"  # no dependencies
            if node_type == NodeType.TRANSFORM:
                nodes.append(TransformSpec(name=name, node_type=node_type, query=query))
            elif node_type == NodeType.METRIC:
                nodes.append(MetricSpec(name=name, node_type=node_type, query=query))
            elif node_type == NodeType.DIMENSION:
                nodes.append(
                    DimensionSpec(
                        name=name,
                        node_type=node_type,
                        query=query,
                        primary_key=["col"],
                    ),
                )

    return nodes


@pytest.mark.skip(reason="For stress testing with a large random DAG")
def test_random_dag():
    nodes = generate_random_dag(num_nodes=1000, max_deps=30)
    dag = extract_node_graph(nodes)

    # sanity checks
    for node in nodes:
        assert (  # sources may have no deps
            node.name in dag or node.node_type == NodeType.SOURCE
        )
        if node.name in dag:
            for dep in dag[node.name]:
                assert dep in [n.name for n in nodes]  # all deps are within nodes


@pytest_asyncio.fixture
async def catalog(session: AsyncSession) -> Catalog:
    """
    A database fixture.
    """

    catalog = Catalog(name="prod", uuid=uuid4())
    session.add(catalog)
    await session.commit()
    return catalog


@asynccontextmanager
async def external_source_node(
    session: AsyncSession,
    current_user: User,
    catalog: Catalog,
) -> AsyncGenerator[Node, None]:
    """
    A source node fixture.
    """
    node = Node(
        name="catalog.dim.categories",
        type=NodeType.SOURCE,
        current_version="v1.0",
        created_by_id=current_user.id,
    )
    node_revision = NodeRevision(
        node=node,
        name=node.name,
        catalog_id=catalog.id,
        schema_="public",
        table="categories",
        type=node.type,
        version="v1.0",
        columns=[
            Column(name="id", type=IntegerType(), order=0),
            Column(name="name", type=StringType(), order=1),
        ],
        created_by_id=current_user.id,
    )
    session.add(node_revision)
    await session.commit()
    yield node


def create_orchestrator(
    session: AsyncSession,
    current_user: User,
    nodes: list[NodeSpec],
) -> DeploymentOrchestrator:
    """
    Create a deployment orchestrator for testing.
    """
    context = MagicMock(autospec=DeploymentContext)
    context.current_user = current_user
    context.save_history = mock_save_history
    deployment_spec = DeploymentSpec(
        namespace="example",
        nodes=nodes,
        tags=[],
    )
    return DeploymentOrchestrator(
        deployment_spec=deployment_spec,
        deployment_id="test-deployment",
        session=session,
        context=context,
    )


async def test_check_external_dependencies(
    session: AsyncSession,
    basic_nodes: list[NodeSpec],
    current_user: User,
    catalog: Catalog,
):
    """
    If a dependency is not in the deployment DAG but does already exist in the
    system, check_external_deps should return it without raising.
    """
    # No external dependencies
    valid_nodes = [
        node_spec
        for node_spec in basic_nodes
        if node_spec.name not in ("example.dimension_node", "example.cube_node")
    ]
    orchestrator = create_orchestrator(session, current_user, valid_nodes)
    node_graph = extract_node_graph(valid_nodes)
    external_deps = await orchestrator.check_external_deps(node_graph)
    assert external_deps == set()

    # One external dependency that doesn't exist yet
    nodes = [
        node_spec for node_spec in basic_nodes if node_spec.name != "example.cube_node"
    ]
    orchestrator = create_orchestrator(session, current_user, nodes)
    node_graph = extract_node_graph(nodes)
    with pytest.raises(DJInvalidDeploymentConfig) as excinfo:
        await orchestrator.check_external_deps(node_graph)
    assert (
        str(excinfo.value)
        == "The following dependencies are not in the deployment and do not pre-exist in the system: catalog.dim.categories"
    )

    # External dependency exists in the system
    async with external_source_node(session, current_user, catalog) as _:
        orchestrator = create_orchestrator(session, current_user, basic_nodes)
        node_graph = extract_node_graph(basic_nodes)
        external_deps = await orchestrator.check_external_deps(node_graph)
        assert external_deps == {"catalog.dim.categories"}


async def mock_save_history(event, session):
    return


@pytest_asyncio.fixture
async def categories(session: AsyncSession, catalog: Catalog, current_user: User):
    node = Node(
        name="catalog.dim.categories",
        type=NodeType.DIMENSION,
        current_version="v1.0",
        created_by_id=current_user.id,
    )
    node_revision = NodeRevision(
        node=node,
        name=node.name,
        type=node.type,
        catalog_id=catalog.id,
        version="v1.0",
        columns=[
            Column(
                name="id",
                type=IntegerType(),
                attributes=[],
                order=0,
            ),
            Column(name="name", type=StringType(), attributes=[], order=1),
            Column(name="dateint", type=IntegerType(), attributes=[], order=2),
        ],
        query="SELECT 1 AS id, 'some' AS name, 20250101 AS dateint",
        created_by_id=current_user.id,
    )
    session.add(node_revision)
    await session.commit()
    return node


@pytest_asyncio.fixture
async def date(session: AsyncSession, catalog: Catalog, current_user: User) -> Node:
    node = Node(
        name="catalog.dim.date",
        type=NodeType.DIMENSION,
        current_version="v1.0",
        created_by_id=current_user.id,
    )
    node_revision = NodeRevision(
        node=node,
        name=node.name,
        type=node.type,
        catalog_id=catalog.id,
        version="v1.0",
        columns=[
            Column(
                name="dateint",
                type=IntegerType(),
                attributes=[],
                order=0,
            ),
            Column(name="month", type=IntegerType(), attributes=[], order=1),
        ],
        query="SELECT 20250101 AS dateint, 1 AS month",
        created_by_id=current_user.id,
    )
    session.add(node_revision)
    await session.commit()
    return node


async def test_deploy_delete_node_success(
    session: AsyncSession,
    current_user: User,
    categories: Node,
):
    await default_attribute_types(session)
    orchestrator = create_orchestrator(session, current_user, [])
    result = await orchestrator._deploy_delete_node(categories.name)
    assert result == DeploymentResult(
        name="catalog.dim.categories",
        deploy_type=DeploymentResult.Type.NODE,
        status=DeploymentResult.Status.SUCCESS,
        operation=DeploymentResult.Operation.DELETE,
        message="Node catalog.dim.categories has been removed.",
    )
    assert await Node.get_by_name(session, categories.name) is None


async def test_deploy_delete_node_failure(
    session: AsyncSession,
    current_user: User,
    categories: Node,
):
    await default_attribute_types(session)
    orchestrator = create_orchestrator(session, current_user, [])
    result = await orchestrator._deploy_delete_node(categories.name + "bogus")
    assert result == DeploymentResult(
        name="catalog.dim.categoriesbogus",
        deploy_type=DeploymentResult.Type.NODE,
        status=DeploymentResult.Status.FAILED,
        operation=DeploymentResult.Operation.DELETE,
        message="A node with name `catalog.dim.categoriesbogus` does not exist.",
    )


def test_find_upstreams_for_derived_metric():
    """
    Test that derived metrics (metrics with no FROM clause referencing other metrics)
    correctly extract their dependencies from column references.
    """
    # Derived metric that references two other metrics
    derived_metric = MetricSpec(
        name="example.derived_ratio",
        node_type=NodeType.METRIC,
        query="SELECT example.metric_a / example.metric_b",
    )
    name, upstreams = _find_upstreams_for_node(derived_metric)
    assert name == "example.derived_ratio"
    # Should extract both the full metric reference and the parent namespace
    assert "example.metric_a" in upstreams
    assert "example.metric_b" in upstreams

    # Derived metric with dimension attribute reference
    derived_with_dim = MetricSpec(
        name="example.filtered_metric",
        node_type=NodeType.METRIC,
        query="SELECT ns.other_metric * ns.dimension.column_value",
    )
    name, upstreams = _find_upstreams_for_node(derived_with_dim)
    assert name == "example.filtered_metric"
    # Should include both the full column reference and the parent (dimension node)
    assert "ns.other_metric" in upstreams
    assert "ns.dimension.column_value" in upstreams
    assert "ns.dimension" in upstreams


async def test_duplicate_nodes_in_deployment_spec(
    session: AsyncSession,
    current_user: User,
):
    """
    Test that duplicate node names in the deployment spec are detected early
    and reported as an error.
    """
    # Create duplicate nodes with same name
    node1 = TransformSpec(
        name="example.duplicate_node",
        node_type=NodeType.TRANSFORM,
        query="SELECT 1 AS col",
    )
    node2 = TransformSpec(
        name="example.duplicate_node",
        node_type=NodeType.TRANSFORM,
        query="SELECT 2 AS col",
    )
    node3 = TransformSpec(
        name="example.another_duplicate",
        node_type=NodeType.TRANSFORM,
        query="SELECT 3 AS col",
    )
    node4 = TransformSpec(
        name="example.another_duplicate",
        node_type=NodeType.TRANSFORM,
        query="SELECT 4 AS col",
    )

    orchestrator = create_orchestrator(
        session,
        current_user,
        [node1, node2, node3, node4],
    )
    with pytest.raises(DJInvalidDeploymentConfig) as excinfo:
        await orchestrator._validate_deployment_resources()

    error_message = str(excinfo.value)
    assert "example.another_duplicate" in error_message
    assert "example.duplicate_node" in error_message


async def test_check_external_deps_dimension_attribute_reference(
    session: AsyncSession,
    current_user: User,
    catalog: Catalog,
    categories: Node,
):
    """
    Test that dimension.column references are correctly handled in external dependency checks.
    The check should not fail when we reference dimension.column but the dimension node exists.
    """
    # Create a metric that references a dimension attribute (categories.dateint)
    metric_with_dim_attr = MetricSpec(
        name="example.metric_with_dim_attr",
        node_type=NodeType.METRIC,
        # This derived metric references a dimension attribute
        query="SELECT catalog.dim.categories.dateint",
    )

    orchestrator = create_orchestrator(
        session,
        current_user,
        [metric_with_dim_attr],
    )
    node_graph = extract_node_graph([metric_with_dim_attr])

    # Should not raise because catalog.dim.categories exists
    external_deps = await orchestrator.check_external_deps(node_graph)
    # The dimension node should be in external deps (not the attribute)
    assert "catalog.dim.categories" in external_deps


async def test_check_external_deps_namespace_prefix_filtering(
    session: AsyncSession,
    current_user: User,
    catalog: Catalog,
    categories: Node,
):
    """
    Test that namespace prefixes in external dependencies are filtered correctly.
    When a derived metric references ns.metric_a, and we extract both ns.metric_a
    and ns as potential deps, 'ns' should be filtered if it matches the deployment
    namespace or if there are found nodes that start with 'ns.'.
    """
    # Create a metric that references catalog.dim.categories (which exists)
    # The derived metric extraction will add both the full path and the parent
    metric = MetricSpec(
        name="test.metric",
        node_type=NodeType.METRIC,
        query="SELECT catalog.dim.categories.dateint * 2",
    )

    orchestrator = create_orchestrator(session, current_user, [metric])
    node_graph = extract_node_graph([metric])

    # The node_graph will have deps like catalog.dim.categories.dateint and catalog.dim.categories
    # check_external_deps should handle this correctly - categories exists, so the attribute
    # reference should be filtered out
    external_deps = await orchestrator.check_external_deps(node_graph)
    # catalog.dim.categories should be in external deps (the actual node)
    assert "catalog.dim.categories" in external_deps


async def test_virtual_catalog_fallback_for_parentless_nodes(
    session: AsyncSession,
    current_user: User,
):
    """
    Test that the virtual catalog exists and can be retrieved for nodes
    without parents (e.g., hardcoded dimensions).
    """
    await default_attribute_types(session)

    # Ensure virtual catalog exists and can be retrieved
    virtual_catalog = await Catalog.get_virtual_catalog(session)
    assert virtual_catalog is not None
    # The catalog should have a valid name (configured in settings)
    assert virtual_catalog.name is not None
    assert len(virtual_catalog.name) > 0


async def test_validate_node_deletion_no_references(
    session: AsyncSession,
    current_user: User,
):
    """
    Test that deleting a node with no references succeeds.
    """
    await default_attribute_types(session)

    # Create a simple node
    source_node = Node(
        name="test.source",
        namespace="test",
        type=NodeType.SOURCE,
        current_version="1",
        created_by_id=current_user.id,
    )
    session.add(source_node)
    source_revision = NodeRevision(
        name="test.source",
        node=source_node,
        type=NodeType.SOURCE,
        version="1",
        query="SELECT 1",
        created_by_id=current_user.id,
    )
    session.add(source_revision)
    source_node.current = source_revision
    await session.commit()

    # Create orchestrator and validate deletion (should not raise)
    deployment_spec = DeploymentSpec(
        namespace="test",
        nodes=[],
    )
    context = DeploymentContext(current_user=current_user)
    orchestrator = DeploymentOrchestrator(
        deployment_id="test-deployment",
        session=session,
        deployment_spec=deployment_spec,
        context=context,
    )

    node_spec = SourceSpec(
        name="source",
        namespace="test",
        catalog="cat",
        schema="sch",
        table="tbl",
    )
    await orchestrator._validate_node_deletion([node_spec])  # Should not raise


async def test_validate_node_deletion_with_parent_reference(
    session: AsyncSession,
    current_user: User,
):
    """
    Test that deleting a node that is referenced as a parent fails.
    """
    await default_attribute_types(session)

    # Create parent node
    parent_node = Node(
        name="test.parent",
        namespace="test",
        type=NodeType.SOURCE,
        current_version="1",
        created_by_id=current_user.id,
    )
    session.add(parent_node)
    parent_revision = NodeRevision(
        name="test.parent",
        node=parent_node,
        type=NodeType.SOURCE,
        version="1",
        query="SELECT 1",
        created_by_id=current_user.id,
    )
    session.add(parent_revision)
    parent_node.current = parent_revision

    # Create child node that depends on parent
    child_node = Node(
        name="test.child",
        namespace="test",
        type=NodeType.TRANSFORM,
        current_version="1",
        created_by_id=current_user.id,
    )
    session.add(child_node)
    child_revision = NodeRevision(
        name="test.child",
        node=child_node,
        type=NodeType.TRANSFORM,
        version="1",
        query="SELECT * FROM test.parent",
        parents=[parent_node],
        created_by_id=current_user.id,
    )
    session.add(child_revision)
    child_node.current = child_revision
    await session.commit()

    # Try to delete parent node
    deployment_spec = DeploymentSpec(
        namespace="test",
        nodes=[],
    )
    context = DeploymentContext(current_user=current_user)
    orchestrator = DeploymentOrchestrator(
        deployment_id="test-deployment",
        session=session,
        deployment_spec=deployment_spec,
        context=context,
    )

    parent_spec = SourceSpec(
        name="parent",
        namespace="test",
        catalog="cat",
        schema="sch",
        table="tbl",
    )

    # Validate node deletion returns references dict
    references = await orchestrator._validate_node_deletion([parent_spec])
    assert "test.parent" in references
    assert "test.child" in references["test.parent"]

    # _delete_nodes should return FAILED result for referenced node
    results = await orchestrator._delete_nodes([parent_spec])
    assert len(results) == 1
    assert results[0].status == DeploymentResult.Status.FAILED
    assert "test.parent" in results[0].message
    assert "test.child" in results[0].message


async def test_validate_node_deletion_with_dimension_link(
    session: AsyncSession,
    current_user: User,
):
    """
    Test that deleting a node that is referenced as a dimension link fails.
    """
    await default_attribute_types(session)

    # Create dimension node
    dim_node = Node(
        name="test.user_dim",
        namespace="test",
        type=NodeType.DIMENSION,
        current_version="1",
        created_by_id=current_user.id,
    )
    session.add(dim_node)
    dim_revision = NodeRevision(
        name="test.user_dim",
        node=dim_node,
        type=NodeType.DIMENSION,
        version="1",
        query="SELECT user_id FROM users",
        created_by_id=current_user.id,
    )
    dim_revision.columns = [
        Column(name="user_id", type=IntegerType(), order=0),
    ]
    session.add(dim_revision)
    dim_node.current = dim_revision

    # Create source node with dimension link
    source_node = Node(
        name="test.events",
        namespace="test",
        type=NodeType.SOURCE,
        current_version="1",
        created_by_id=current_user.id,
    )
    session.add(source_node)
    source_revision = NodeRevision(
        name="test.events",
        node=source_node,
        type=NodeType.SOURCE,
        version="1",
        query="SELECT event_id, user_id FROM events",
        created_by_id=current_user.id,
    )
    session.add(source_revision)
    source_node.current = source_revision

    # Add dimension link
    from datajunction_server.database.dimensionlink import DimensionLink

    dim_link = DimensionLink(
        node_revision=source_revision,
        dimension=dim_node,
        join_sql="events.user_id = test.user_dim.user_id",
        role=None,
    )
    session.add(dim_link)
    await session.commit()

    # Try to delete dimension node
    deployment_spec = DeploymentSpec(
        namespace="test",
        nodes=[],
    )
    context = DeploymentContext(current_user=current_user)
    orchestrator = DeploymentOrchestrator(
        deployment_id="test-deployment",
        session=session,
        deployment_spec=deployment_spec,
        context=context,
    )

    dim_spec = DimensionSpec(
        name="user_dim",
        namespace="test",
        query="SELECT user_id FROM users",
    )

    # Validate node deletion returns references dict
    references = await orchestrator._validate_node_deletion([dim_spec])
    assert "test.user_dim" in references
    assert any("test.events" in ref for ref in references["test.user_dim"])
    assert any("dimension link" in ref for ref in references["test.user_dim"])

    # _delete_nodes should return FAILED result
    results = await orchestrator._delete_nodes([dim_spec])
    assert len(results) == 1
    assert results[0].status == DeploymentResult.Status.FAILED
    assert "dimension link" in results[0].message


async def test_validate_node_deletion_cross_namespace_reference(
    session: AsyncSession,
    current_user: User,
):
    """
    Test that deleting a node fails if referenced from another namespace.
    """
    await default_attribute_types(session)

    # Create node in namespace 'shared'
    shared_node = Node(
        name="shared.common_dim",
        namespace="shared",
        type=NodeType.DIMENSION,
        current_version="1",
        created_by_id=current_user.id,
    )
    session.add(shared_node)
    shared_revision = NodeRevision(
        name="shared.common_dim",
        node=shared_node,
        type=NodeType.DIMENSION,
        version="1",
        query="SELECT id FROM common",
        created_by_id=current_user.id,
    )
    session.add(shared_revision)
    shared_node.current = shared_revision

    # Create node in namespace 'app' that references 'shared'
    app_node = Node(
        name="app.metric",
        namespace="app",
        type=NodeType.METRIC,
        current_version="1",
        created_by_id=current_user.id,
    )
    session.add(app_node)
    app_revision = NodeRevision(
        name="app.metric",
        node=app_node,
        type=NodeType.METRIC,
        version="1",
        query="SELECT COUNT(*) FROM shared.common_dim",
        parents=[shared_node],
        created_by_id=current_user.id,
    )
    session.add(app_revision)
    app_node.current = app_revision
    await session.commit()

    # Try to delete shared node from shared namespace deployment
    deployment_spec = DeploymentSpec(
        namespace="shared",
        nodes=[],
    )
    context = DeploymentContext(current_user=current_user)
    orchestrator = DeploymentOrchestrator(
        deployment_id="test-deployment",
        session=session,
        deployment_spec=deployment_spec,
        context=context,
    )

    shared_spec = DimensionSpec(
        name="common_dim",
        namespace="shared",
        query="SELECT id FROM common",
    )

    # Validate node deletion returns cross-namespace references
    references = await orchestrator._validate_node_deletion([shared_spec])
    assert "shared.common_dim" in references
    assert "app.metric" in references["shared.common_dim"]

    # _delete_nodes should return FAILED result
    results = await orchestrator._delete_nodes([shared_spec])
    assert len(results) == 1
    assert results[0].status == DeploymentResult.Status.FAILED
    assert "app.metric" in results[0].message


async def test_validate_node_deletion_multiple_nodes_self_reference(
    session: AsyncSession,
    current_user: User,
):
    """
    Test that deleting multiple nodes that reference each other succeeds.
    """
    await default_attribute_types(session)

    # Create two nodes that reference each other (being deleted together)
    node_a = Node(
        name="test.node_a",
        namespace="test",
        type=NodeType.SOURCE,
        current_version="1",
        created_by_id=current_user.id,
    )
    session.add(node_a)
    node_a_revision = NodeRevision(
        name="test.node_a",
        node=node_a,
        type=NodeType.SOURCE,
        version="1",
        query="SELECT 1",
        created_by_id=current_user.id,
    )
    session.add(node_a_revision)
    node_a.current = node_a_revision

    node_b = Node(
        name="test.node_b",
        namespace="test",
        type=NodeType.TRANSFORM,
        current_version="1",
        created_by_id=current_user.id,
    )
    session.add(node_b)
    node_b_revision = NodeRevision(
        name="test.node_b",
        node=node_b,
        type=NodeType.TRANSFORM,
        version="1",
        query="SELECT * FROM test.node_a",
        parents=[node_a],
        created_by_id=current_user.id,
    )
    session.add(node_b_revision)
    node_b.current = node_b_revision
    await session.commit()

    # Delete both nodes together (should succeed)
    deployment_spec = DeploymentSpec(
        namespace="test",
        nodes=[],
    )
    context = DeploymentContext(current_user=current_user)
    orchestrator = DeploymentOrchestrator(
        deployment_id="test-deployment",
        session=session,
        deployment_spec=deployment_spec,
        context=context,
    )

    spec_a = SourceSpec(
        name="node_a",
        namespace="test",
        catalog="cat",
        schema="sch",
        table="tbl",
    )
    spec_b = TransformSpec(
        name="node_b",
        namespace="test",
        query="SELECT * FROM test.node_a",
    )

    # Should not raise because both are being deleted
    await orchestrator._validate_node_deletion([spec_a, spec_b])


async def test_validate_node_deletion_empty_list(
    session: AsyncSession,
    current_user: User,
):
    """
    Test that validating an empty deletion list returns early without error.
    """
    await default_attribute_types(session)

    deployment_spec = DeploymentSpec(
        namespace="test",
        nodes=[],
    )
    context = DeploymentContext(current_user=current_user)
    orchestrator = DeploymentOrchestrator(
        deployment_id="test-deployment",
        session=session,
        deployment_spec=deployment_spec,
        context=context,
    )

    # Should return early without error
    await orchestrator._validate_node_deletion([])


async def test_validate_node_deletion_nonexistent_node(
    session: AsyncSession,
    current_user: User,
):
    """
    Test that deleting a node that doesn't exist in the database returns early.
    """
    await default_attribute_types(session)

    deployment_spec = DeploymentSpec(
        namespace="test",
        nodes=[],
    )
    context = DeploymentContext(current_user=current_user)
    orchestrator = DeploymentOrchestrator(
        deployment_id="test-deployment",
        session=session,
        deployment_spec=deployment_spec,
        context=context,
    )

    # Try to delete a node that doesn't exist
    fake_spec = SourceSpec(
        name="nonexistent",
        namespace="test",
        catalog="cat",
        schema="sch",
        table="tbl",
    )

    # Should return early without error (no nodes found to delete)
    await orchestrator._validate_node_deletion([fake_spec])


async def test_validate_node_deletion_multiple_references_to_same_node(
    session: AsyncSession,
    current_user: User,
):
    """
    Test that deleting a node referenced by multiple other nodes shows all references.
    """
    await default_attribute_types(session)

    # Create a parent node
    parent_node = Node(
        name="test.parent",
        namespace="test",
        type=NodeType.SOURCE,
        current_version="1",
        created_by_id=current_user.id,
    )
    session.add(parent_node)
    parent_revision = NodeRevision(
        name="test.parent",
        node=parent_node,
        type=NodeType.SOURCE,
        version="1",
        query="SELECT 1",
        created_by_id=current_user.id,
    )
    session.add(parent_revision)
    parent_node.current = parent_revision

    # Create three children that all depend on parent
    for child_name in ["child_a", "child_b", "child_c"]:
        child_node = Node(
            name=f"test.{child_name}",
            namespace="test",
            type=NodeType.TRANSFORM,
            current_version="1",
            created_by_id=current_user.id,
        )
        session.add(child_node)
        child_revision = NodeRevision(
            name=f"test.{child_name}",
            node=child_node,
            type=NodeType.TRANSFORM,
            version="1",
            query="SELECT * FROM test.parent",
            parents=[parent_node],
            created_by_id=current_user.id,
        )
        session.add(child_revision)
        child_node.current = child_revision

    await session.commit()

    # Try to delete parent node
    deployment_spec = DeploymentSpec(
        namespace="test",
        nodes=[],
    )
    context = DeploymentContext(current_user=current_user)
    orchestrator = DeploymentOrchestrator(
        deployment_id="test-deployment",
        session=session,
        deployment_spec=deployment_spec,
        context=context,
    )

    parent_spec = SourceSpec(
        name="parent",
        namespace="test",
        catalog="cat",
        schema="sch",
        table="tbl",
    )

    # Validate node deletion returns all references
    references = await orchestrator._validate_node_deletion([parent_spec])
    assert "test.parent" in references
    # All three children should be listed
    assert "test.child_a" in references["test.parent"]
    assert "test.child_b" in references["test.parent"]
    assert "test.child_c" in references["test.parent"]

    # _delete_nodes should return FAILED result with all references
    results = await orchestrator._delete_nodes([parent_spec])
    assert len(results) == 1
    assert results[0].status == DeploymentResult.Status.FAILED
    error_message = results[0].message
    assert "test.child_a" in error_message
    assert "test.child_b" in error_message
    assert "test.child_c" in error_message


async def test_validate_node_deletion_both_parent_and_dimension_link(
    session: AsyncSession,
    current_user: User,
):
    """
    Test that a node referenced as both parent and dimension link shows both types.
    """
    await default_attribute_types(session)

    # Create a dimension/source node that will be referenced in two ways
    shared_node = Node(
        name="test.shared",
        namespace="test",
        type=NodeType.DIMENSION,
        current_version="1",
        created_by_id=current_user.id,
    )
    session.add(shared_node)
    shared_revision = NodeRevision(
        name="test.shared",
        node=shared_node,
        type=NodeType.DIMENSION,
        version="1",
        query="SELECT id FROM shared_table",
        created_by_id=current_user.id,
    )
    shared_revision.columns = [
        Column(name="id", type=IntegerType(), order=0),
    ]
    session.add(shared_revision)
    shared_node.current = shared_revision

    # Create a transform that uses it as a parent
    transform_node = Node(
        name="test.transform",
        namespace="test",
        type=NodeType.TRANSFORM,
        current_version="1",
        created_by_id=current_user.id,
    )
    session.add(transform_node)
    transform_revision = NodeRevision(
        name="test.transform",
        node=transform_node,
        type=NodeType.TRANSFORM,
        version="1",
        query="SELECT * FROM test.shared",
        parents=[shared_node],
        created_by_id=current_user.id,
    )
    session.add(transform_revision)
    transform_node.current = transform_revision

    # Create a source that uses it as a dimension link
    source_node = Node(
        name="test.source",
        namespace="test",
        type=NodeType.SOURCE,
        current_version="1",
        created_by_id=current_user.id,
    )
    session.add(source_node)
    source_revision = NodeRevision(
        name="test.source",
        node=source_node,
        type=NodeType.SOURCE,
        version="1",
        query="SELECT event_id FROM events",
        created_by_id=current_user.id,
    )
    session.add(source_revision)
    source_node.current = source_revision

    from datajunction_server.database.dimensionlink import DimensionLink

    dim_link = DimensionLink(
        node_revision=source_revision,
        dimension=shared_node,
        join_sql="test.source.event_id = test.shared.id",
        role=None,
    )
    session.add(dim_link)
    await session.commit()

    # Try to delete shared node
    deployment_spec = DeploymentSpec(
        namespace="test",
        nodes=[],
    )
    context = DeploymentContext(current_user=current_user)
    orchestrator = DeploymentOrchestrator(
        deployment_id="test-deployment",
        session=session,
        deployment_spec=deployment_spec,
        context=context,
    )

    shared_spec = DimensionSpec(
        name="shared",
        namespace="test",
        query="SELECT id FROM shared_table",
    )

    # Validate node deletion returns both types of references
    references = await orchestrator._validate_node_deletion([shared_spec])
    assert "test.shared" in references
    # Should show both types of references
    assert "test.transform" in references["test.shared"]
    assert any("test.source" in ref for ref in references["test.shared"])
    assert any("dimension link" in ref for ref in references["test.shared"])

    # _delete_nodes should return FAILED result with both types
    results = await orchestrator._delete_nodes([shared_spec])
    assert len(results) == 1
    assert results[0].status == DeploymentResult.Status.FAILED
    error_message = results[0].message
    assert "test.transform" in error_message
    assert "test.source" in error_message
    assert "dimension link" in error_message


async def test_validate_node_deletion_mixed_scenarios(
    session: AsyncSession,
    current_user: User,
):
    """
    Test deleting multiple nodes where some have references and some don't.
    """
    await default_attribute_types(session)

    # Create standalone node (no references)
    standalone_node = Node(
        name="test.standalone",
        namespace="test",
        type=NodeType.SOURCE,
        current_version="1",
        created_by_id=current_user.id,
    )
    session.add(standalone_node)
    standalone_revision = NodeRevision(
        name="test.standalone",
        node=standalone_node,
        type=NodeType.SOURCE,
        version="1",
        query="SELECT 1",
        created_by_id=current_user.id,
    )
    session.add(standalone_revision)
    standalone_node.current = standalone_revision

    # Create referenced node A
    node_a = Node(
        name="test.referenced_a",
        namespace="test",
        type=NodeType.SOURCE,
        current_version="1",
        created_by_id=current_user.id,
    )
    session.add(node_a)
    node_a_revision = NodeRevision(
        name="test.referenced_a",
        node=node_a,
        type=NodeType.SOURCE,
        version="1",
        query="SELECT 1",
        created_by_id=current_user.id,
    )
    session.add(node_a_revision)
    node_a.current = node_a_revision

    # Create referenced node B
    node_b = Node(
        name="test.referenced_b",
        namespace="test",
        type=NodeType.SOURCE,
        current_version="1",
        created_by_id=current_user.id,
    )
    session.add(node_b)
    node_b_revision = NodeRevision(
        name="test.referenced_b",
        node=node_b,
        type=NodeType.SOURCE,
        version="1",
        query="SELECT 1",
        created_by_id=current_user.id,
    )
    session.add(node_b_revision)
    node_b.current = node_b_revision

    # Create external nodes that reference A and B
    child_of_a = Node(
        name="test.child_of_a",
        namespace="test",
        type=NodeType.TRANSFORM,
        current_version="1",
        created_by_id=current_user.id,
    )
    session.add(child_of_a)
    child_of_a_revision = NodeRevision(
        name="test.child_of_a",
        node=child_of_a,
        type=NodeType.TRANSFORM,
        version="1",
        query="SELECT * FROM test.referenced_a",
        parents=[node_a],
        created_by_id=current_user.id,
    )
    session.add(child_of_a_revision)
    child_of_a.current = child_of_a_revision

    child_of_b = Node(
        name="test.child_of_b",
        namespace="test",
        type=NodeType.TRANSFORM,
        current_version="1",
        created_by_id=current_user.id,
    )
    session.add(child_of_b)
    child_of_b_revision = NodeRevision(
        name="test.child_of_b",
        node=child_of_b,
        type=NodeType.TRANSFORM,
        version="1",
        query="SELECT * FROM test.referenced_b",
        parents=[node_b],
        created_by_id=current_user.id,
    )
    session.add(child_of_b_revision)
    child_of_b.current = child_of_b_revision

    await session.commit()

    # Try to delete all three: standalone (ok), referenced_a (fail), referenced_b (fail)
    deployment_spec = DeploymentSpec(
        namespace="test",
        nodes=[],
    )
    context = DeploymentContext(current_user=current_user)
    orchestrator = DeploymentOrchestrator(
        deployment_id="test-deployment",
        session=session,
        deployment_spec=deployment_spec,
        context=context,
    )

    specs = [
        SourceSpec(
            name="standalone",
            namespace="test",
            catalog="cat",
            schema="sch",
            table="tbl",
        ),
        SourceSpec(
            name="referenced_a",
            namespace="test",
            catalog="cat",
            schema="sch",
            table="tbl",
        ),
        SourceSpec(
            name="referenced_b",
            namespace="test",
            catalog="cat",
            schema="sch",
            table="tbl",
        ),
    ]

    # Validate shows references for both referenced nodes
    references = await orchestrator._validate_node_deletion(specs)
    assert "test.referenced_a" in references
    assert "test.child_of_a" in references["test.referenced_a"]
    assert "test.referenced_b" in references
    assert "test.child_of_b" in references["test.referenced_b"]
    assert "test.standalone" not in references

    # Delete nodes - should have mixed results
    results = await orchestrator._delete_nodes(specs)
    assert len(results) == 3

    # Find results by name
    standalone_result = next(r for r in results if r.name == "test.standalone")
    ref_a_result = next(r for r in results if r.name == "test.referenced_a")
    ref_b_result = next(r for r in results if r.name == "test.referenced_b")

    # Standalone should succeed
    assert standalone_result.status == DeploymentResult.Status.SUCCESS

    # Referenced nodes should fail
    assert ref_a_result.status == DeploymentResult.Status.FAILED
    assert "test.child_of_a" in ref_a_result.message
    assert ref_b_result.status == DeploymentResult.Status.FAILED
    assert "test.child_of_b" in ref_b_result.message


async def test_validate_node_deletion_self_referential(
    session: AsyncSession,
    current_user: User,
):
    """
    Test that deleting a self-referential node succeeds (edge case).
    """
    await default_attribute_types(session)

    # Create a self-referential node (recursive CTE)
    recursive_node = Node(
        name="test.recursive",
        namespace="test",
        type=NodeType.TRANSFORM,
        current_version="1",
        created_by_id=current_user.id,
    )
    session.add(recursive_node)

    # Node references itself as a parent
    recursive_revision = NodeRevision(
        name="test.recursive",
        node=recursive_node,
        type=NodeType.TRANSFORM,
        version="1",
        query="WITH RECURSIVE cte AS (SELECT 1) SELECT * FROM cte",
        created_by_id=current_user.id,
    )
    session.add(recursive_revision)
    recursive_node.current = recursive_revision

    # Add self as parent
    recursive_revision.parents = [recursive_node]
    await session.commit()

    # Delete the self-referential node (should succeed)
    deployment_spec = DeploymentSpec(
        namespace="test",
        nodes=[],
    )
    context = DeploymentContext(current_user=current_user)
    orchestrator = DeploymentOrchestrator(
        deployment_id="test-deployment",
        session=session,
        deployment_spec=deployment_spec,
        context=context,
    )

    recursive_spec = TransformSpec(name="recursive", namespace="test", query="SELECT 1")

    # Should not raise - self-reference is allowed when deleting the node
    await orchestrator._validate_node_deletion([recursive_spec])


async def test_check_external_deps_phantom_parent_path_for__metric(
    session: AsyncSession,
    current_user: User,
):
    """
    When a derived metric references another metric (e.g., `new_namespace.total_clicks`),
    the parent_path heuristic in _find_upstreams_for_node adds the namespace itself
    (i.e., `new_namespace`) as a speculative dependency. This phantom dependency is
    NOT a real node -- it's a namespace. check_external_deps should
    recognize it as such and not raise.
    """
    source = SourceSpec(
        name="source.clicks",
        node_type=NodeType.SOURCE,
        catalog="catalog",
        schema_="facts",
        table="clicks",
    )
    metric_a = MetricSpec(
        name="new_namespace.total_clicks",
        node_type=NodeType.METRIC,
        query="SELECT COUNT(*) FROM source.clicks",
    )
    derived = MetricSpec(
        name="new_namespace.double_total_clicks",
        node_type=NodeType.METRIC,
        query="SELECT new_namespace.total_clicks * 2",
    )
    nodes = [source, metric_a, derived]
    orchestrator = create_orchestrator(session, current_user, nodes)
    node_graph = extract_node_graph(nodes)

    # The phantom "new_namespace" should be recognized as a namespace prefix
    # of the in-deployment node "new_namespace.total_clicks", not flagged
    # as a missing dependency.
    external_deps = await orchestrator.check_external_deps(node_graph)
    assert "new_namespace" not in external_deps


async def test_validate_node_deletion_many_references_formatting(
    session: AsyncSession,
    current_user: User,
):
    """
    Test that error message is readable when a node has many references.
    """
    await default_attribute_types(session)

    # Create a parent node
    parent_node = Node(
        name="test.popular_dim",
        namespace="test",
        type=NodeType.DIMENSION,
        current_version="1",
        created_by_id=current_user.id,
    )
    session.add(parent_node)
    parent_revision = NodeRevision(
        name="test.popular_dim",
        node=parent_node,
        type=NodeType.DIMENSION,
        version="1",
        query="SELECT id FROM popular",
        created_by_id=current_user.id,
    )
    session.add(parent_revision)
    parent_node.current = parent_revision

    # Create 15 children that all depend on parent
    for i in range(15):
        child_node = Node(
            name=f"test.child_{i:02d}",
            namespace="test",
            type=NodeType.TRANSFORM,
            current_version="1",
            created_by_id=current_user.id,
        )
        session.add(child_node)
        child_revision = NodeRevision(
            name=f"test.child_{i:02d}",
            node=child_node,
            type=NodeType.TRANSFORM,
            version="1",
            query="SELECT * FROM test.popular_dim",
            parents=[parent_node],
            created_by_id=current_user.id,
        )
        session.add(child_revision)
        child_node.current = child_revision

    await session.commit()

    # Try to delete parent node
    deployment_spec = DeploymentSpec(
        namespace="test",
        nodes=[],
    )
    context = DeploymentContext(current_user=current_user)
    orchestrator = DeploymentOrchestrator(
        deployment_id="test-deployment",
        session=session,
        deployment_spec=deployment_spec,
        context=context,
    )

    parent_spec = DimensionSpec(
        name="popular_dim",
        namespace="test",
        query="SELECT id FROM popular",
    )

    # Validate node deletion returns all 15 references
    references = await orchestrator._validate_node_deletion([parent_spec])
    assert "test.popular_dim" in references
    assert len(references["test.popular_dim"]) == 15

    # Verify all 15 children are listed
    for i in range(15):
        assert f"test.child_{i:02d}" in references["test.popular_dim"]

    # _delete_nodes should return FAILED result with all references
    results = await orchestrator._delete_nodes([parent_spec])
    assert len(results) == 1
    assert results[0].status == DeploymentResult.Status.FAILED
    error_message = results[0].message

    # Verify all 15 children are in the error message
    for i in range(15):
        assert f"test.child_{i:02d}" in error_message

    # Verify error message is properly formatted (uses commas)
    assert ", " in error_message
