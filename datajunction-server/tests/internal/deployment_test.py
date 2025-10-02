from contextlib import asynccontextmanager
import random
from typing import AsyncGenerator, cast
from unittest.mock import MagicMock, patch
from uuid import uuid4
import pytest_asyncio
from datajunction_server.api.attributes import default_attribute_types
from datajunction_server.database.column import Column
from datajunction_server.database.catalog import Catalog
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.tag import Tag
from datajunction_server.sql.parsing.types import IntegerType, StringType
from datajunction_server.database.user import User

from datajunction_server.models.node_type import NodeType
from datajunction_server.models.partition import (
    Granularity,
    PartitionType,
)
from datajunction_server.internal.deployment.orchestrator import (
    DeploymentOrchestrator,
    DeploymentSpec,
)
from datajunction_server.internal.deployment.utils import (
    DeploymentContext,
    extract_node_graph,
    topological_levels,
)
from datajunction_server.internal.deployment.deployment import (
    deploy_column_properties,
    deploy_node_tags,
)
from datajunction_server.models.deployment import (
    ColumnSpec,
    DeploymentResult,
    NodeSpec,
    PartitionSpec,
    TransformSpec,
    SourceSpec,
    MetricSpec,
    DimensionSpec,
    CubeSpec,
)
from sqlalchemy.ext.asyncio import AsyncSession
from datajunction_server.errors import (
    DJDoesNotExistException,
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
        table="catalog.facts.clicks",
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
        table="catalog.facts.clicks",
    )
    users = SourceSpec(
        name="catalog.dim.users",
        node_type=NodeType.SOURCE,
        table="catalog.dim.users",
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


async def test_deploy_column_properties_partition(
    session: AsyncSession,
    current_user: User,
    categories: Node,
):
    with patch(
        "datajunction_server.internal.deployment.deployment.session_context",
    ) as mock_session_context:
        await default_attribute_types(session)
        mock_session_context.return_value = session
        node_spec = SourceSpec(
            name="catalog.dim.categories",
            node_type=NodeType.SOURCE,
            columns=[
                ColumnSpec(name="id", type="int"),
                ColumnSpec(name="name", type="string"),
                ColumnSpec(
                    name="dateint",
                    attributes=[],
                    type="int",
                    partition=PartitionSpec(
                        type=PartitionType.TEMPORAL,
                        granularity=Granularity.DAY,
                        format="yyyyMMDD",
                    ),
                ),
            ],
            table="catalog.dim.categories",
        )
        await deploy_column_properties(
            categories.name,
            node_spec,
            current_user.username,
            mock_save_history,
        )
        node = cast(Node, await Node.get_by_name(session, categories.name))
        assert node.current.columns[2].partition.type_ == PartitionType.TEMPORAL
        assert node.current.columns[2].partition.granularity == Granularity.DAY
        assert node.current.columns[2].partition.format == "yyyyMMDD"

        # Update partition
        node_spec.columns[2].partition.granularity = Granularity.MONTH  # type: ignore
        await deploy_column_properties(
            categories.name,
            node_spec,
            current_user.username,
            mock_save_history,
        )
        node = cast(Node, await Node.get_by_name(session, categories.name))
        assert node.current.columns[2].partition.type_ == PartitionType.TEMPORAL
        assert node.current.columns[2].partition.granularity == Granularity.MONTH

        # Remove partition
        node_spec.columns[2].partition = None  # type: ignore
        await deploy_column_properties(
            categories.name,
            node_spec,
            current_user.username,
            mock_save_history,
        )
        node = await Node.get_by_name(session, categories.name)  # type: ignore
        assert not node.current.columns[2].partition_id


async def test_deploy_column_properties_attributes(
    session: AsyncSession,
    current_user: User,
    categories: Node,
):
    with patch(
        "datajunction_server.internal.deployment.deployment.session_context",
    ) as mock_session_context:
        await default_attribute_types(session)
        mock_session_context.return_value = session
        node_spec = SourceSpec(
            name="catalog.dim.categories",
            node_type=NodeType.SOURCE,
            columns=[
                ColumnSpec(name="id", attributes=["hidden"], type="int"),
                ColumnSpec(
                    name="name",
                    attributes=["primary_key", "hidden"],
                    type="string",
                ),
                ColumnSpec(name="dateint", type="int"),
            ],
            table="catalog.dim.categories",
        )

        await deploy_column_properties(
            categories.name,
            node_spec,
            current_user.username,
            mock_save_history,
        )

        node = cast(Node, await Node.get_by_name(session, categories.name))
        node.current.columns[0].attributes.sort(key=lambda a: a.attribute_type.name)
        assert [
            attr.attribute_type.name for attr in node.current.columns[0].attributes
        ] == ["hidden"]
        assert {
            attr.attribute_type.name for attr in node.current.columns[1].attributes
        } == {"hidden", "primary_key"}


async def test_deploy_column_properties_desc(
    session: AsyncSession,
    current_user: User,
    categories: Node,
):
    with patch(
        "datajunction_server.internal.deployment.deployment.session_context",
    ) as mock_session_context:
        await default_attribute_types(session)
        mock_session_context.return_value = session
        node_spec = SourceSpec(
            name="catalog.dim.categories",
            node_type=NodeType.SOURCE,
            columns=[
                ColumnSpec(name="id", display_name="Identifier", type="int"),
                ColumnSpec(
                    name="name",
                    description="Category name",
                    type="string",
                ),
                ColumnSpec(name="dateint", type="int"),
            ],
            table="catalog.dim.categories",
        )
        await deploy_column_properties(
            categories.name,
            node_spec,
            current_user.username,
            mock_save_history,
        )
        node = cast(Node, await Node.get_by_name(session, categories.name))
        node.current.columns[0].display_name == "Identifier"
        node.current.columns[0].description == ""
        node.current.columns[1].display_name == "Category"
        node.current.columns[1].description == "Category name"


async def test_deploy_column_properties_reset(
    session: AsyncSession,
    current_user: User,
    categories: Node,
):
    with patch(
        "datajunction_server.internal.deployment.deployment.session_context",
    ) as mock_session_context:
        await default_attribute_types(session)
        mock_session_context.return_value = session
        node_spec = SourceSpec(
            name="catalog.dim.categories",
            node_type=NodeType.SOURCE,
            columns=[
                ColumnSpec(
                    name="id",
                    display_name="ID",
                    attributes=["primary_key"],
                    type="int",
                ),
                ColumnSpec(
                    name="name",
                    description="Category name",
                    attributes=["hidden"],
                    type="string",
                ),
                ColumnSpec(
                    name="dateint",
                    type="int",
                    display_name="Date",
                    partition=PartitionSpec(
                        type=PartitionType.TEMPORAL,
                        granularity=Granularity.DAY,
                        format="yyyyMMDD",
                    ),
                ),
            ],
            table="catalog.dim.categories",
        )
        await deploy_column_properties(
            categories.name,
            node_spec,
            current_user.username,
            mock_save_history,
        )

        # Reset columns to default properties
        node_spec.columns = []
        await deploy_column_properties(
            categories.name,
            node_spec,
            current_user.username,
            mock_save_history,
        )
        node = cast(Node, await Node.get_by_name(session, categories.name))
        node.current.columns[0].display_name == "Id"
        node.current.columns[0].description == ""
        assert len(node.current.columns[0].attributes) == 1
        assert not node.current.columns[0].partition_id

        node.current.columns[1].display_name == "Name"
        node.current.columns[1].description == ""
        assert not node.current.columns[1].attributes
        assert not node.current.columns[1].partition_id

        node.current.columns[2].display_name == "Dateint"
        node.current.columns[2].description == ""
        assert not node.current.columns[2].attributes
        assert not node.current.columns[2].partition_id


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


async def test_deploy_node_tags(
    session: AsyncSession,
    current_user: User,
    categories: Node,
):
    with patch(
        "datajunction_server.internal.deployment.deployment.session_context",
    ) as mock_session_context:
        await default_attribute_types(session)
        mock_session_context.return_value = session
        with pytest.raises(DJDoesNotExistException) as excinfo:
            await deploy_node_tags(categories.name, tag_names=["tag1", "tag2"])
        assert "Tags not found" in str(excinfo.value)

        # Create tags
        tag = Tag(name="tag1", created_by_id=current_user.id, tag_type="default")
        session.add(tag)
        tag = Tag(name="tag2", created_by_id=current_user.id, tag_type="default")
        session.add(tag)
        await session.commit()
        # Retry deploying tags
        await deploy_node_tags(categories.name, tag_names=["tag1", "tag2"])

        node = cast(Node, await Node.get_by_name(session, categories.name))
        assert sorted([tag.name for tag in node.tags]) == ["tag1", "tag2"]

        # Update tags
        await deploy_node_tags(
            node_name=categories.name,
            tag_names=["tag2"],
        )
        node = cast(Node, await Node.get_by_name(session, categories.name))
        assert sorted([tag.name for tag in node.tags]) == ["tag2"]
