from contextlib import asynccontextmanager
import random
from typing import AsyncGenerator
from uuid import uuid4
import pytest_asyncio
from datajunction_server.database.column import Column
from datajunction_server.database.catalog import Catalog
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.sql.parsing.types import IntegerType, StringType
from datajunction_server.database.user import User
from datajunction_server.internal.deployment import (
    check_external_deps,
    extract_node_graph,
    topological_levels,
)
from datajunction_server.models.deployment import (
    NodeSpec,
    TransformSpec,
    SourceSpec,
    MetricSpec,
    DimensionSpec,
    CubeSpec,
)
from sqlalchemy.ext.asyncio import AsyncSession
from datajunction_server.errors import DJGraphCycleException, DJInvalidDeploymentConfig
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
        current_version="v1",
        created_by_id=current_user.id,
    )
    node_revision = NodeRevision(
        node=node,
        name=node.name,
        catalog_id=catalog.id,
        schema_="public",
        table="categories",
        type=node.type,
        version="v1",
        columns=[
            Column(name="id", type=IntegerType(), order=0),
            Column(name="name", type=StringType(), order=1),
        ],
        created_by_id=current_user.id,
    )
    session.add(node_revision)
    await session.commit()
    yield node


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
    node_graph = extract_node_graph(valid_nodes)
    assert await check_external_deps(session, node_graph, valid_nodes) == set()

    # One external dependency (the dimension node depends on the existing source)
    nodes = [
        node_spec for node_spec in basic_nodes if node_spec.name != "example.cube_node"
    ]
    node_graph = extract_node_graph(nodes)
    with pytest.raises(DJInvalidDeploymentConfig) as excinfo:
        await check_external_deps(session, node_graph, nodes)
    assert (
        str(excinfo.value)
        == "The following dependencies are not in the deployment and do not pre-exist in the system: catalog.dim.categories"
    )

    # Create the external source node
    async with external_source_node(session, current_user, catalog) as _:
        # Now check_external_deps should pass
        node_graph = extract_node_graph(basic_nodes)
        assert await check_external_deps(session, node_graph, basic_nodes) == {
            "catalog.dim.categories",
        }
