import random
from datajunction_server.internal.yaml import extract_node_graph, topological_levels
from datajunction_server.models.yaml import (
    ColumnYAML,
    DeploymentYAML,
    NodeYAML,
    TransformYAML,
    SourceYAML,
    MetricYAML,
    DimensionYAML,
    CubeYAML,
    DimensionJoinLinkYAML,
)
from datajunction_server.models.node import NodeType
import pytest


@pytest.fixture(scope="module")
def basic_nodes():
    """
    A basic set of nodes for testing
    """
    transform_node = TransformYAML(
        name="example.transform_node",
        node_type=NodeType.TRANSFORM,
        query="SELECT id, name FROM catalog.facts.clicks",
    )
    source_node = SourceYAML(
        name="catalog.facts.clicks",
        node_type=NodeType.SOURCE,
        table="catalog.facts.clicks",
    )
    metric_node = MetricYAML(
        name="example.metric_node",
        node_type=NodeType.METRIC,
        query="SELECT SUM(value) FROM example.transform_node",
    )
    dimension_node = DimensionYAML(
        name="example.dimension_node",
        node_type=NodeType.DIMENSION,
        query="SELECT id, category FROM catalog.dim.categories",
        primary_key=["id"],
    )
    cube_node = CubeYAML(
        name="example.cube_node",
        node_type=NodeType.CUBE,
        metrics=["example.metric_node"],
        dimensions=["example.dimension_node.category"],
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


def test_graph_complex():
    # Base source nodes
    clicks = SourceYAML(
        name="catalog.facts.clicks",
        node_type=NodeType.SOURCE,
        table="catalog.facts.clicks",
    )
    users = SourceYAML(
        name="catalog.dim.users",
        node_type=NodeType.SOURCE,
        table="catalog.dim.users",
    )

    # Transform nodes
    transform_clicks = TransformYAML(
        name="example.transform_clicks",
        node_type=NodeType.TRANSFORM,
        query="SELECT id, user_id FROM catalog.facts.clicks",
    )
    transform_users = TransformYAML(
        name="example.transform_users",
        node_type=NodeType.TRANSFORM,
        query="SELECT id, country FROM catalog.dim.users",
    )
    combined_transform = TransformYAML(
        name="example.combined_transform",
        node_type=NodeType.TRANSFORM,
        query="""
            SELECT c.id, c.user_id, u.country
            FROM example.transform_clicks c
            JOIN example.transform_users u ON c.user_id = u.id
        """,
    )

    # Metric nodes
    metric_total = MetricYAML(
        name="example.metric_total",
        node_type=NodeType.METRIC,
        query="SELECT SUM(amount) FROM example.combined_transform",
    )
    metric_per_country = MetricYAML(
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


def generate_random_dag(num_nodes: int = 10, max_deps: int = 3):
    """
    Generate a random DAG of nodes for testing extract_node_graph.
    """
    nodes: list[NodeYAML] = []

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
            nodes.append(SourceYAML(name=name, node_type=node_type, table=f"table_{i}"))
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
                nodes.append(TransformYAML(name=name, node_type=node_type, query=query))
            elif node_type == NodeType.METRIC:
                nodes.append(MetricYAML(name=name, node_type=node_type, query=query))
            elif node_type == NodeType.DIMENSION:
                nodes.append(
                    DimensionYAML(
                        name=name,
                        node_type=node_type,
                        query=query,
                        primary_key=["col"],
                    ),
                )

    return nodes


def test_random_dag():
    nodes = generate_random_dag(num_nodes=1000, max_deps=30)
    dag = extract_node_graph(nodes)

    # sanity checks
    for node in nodes:
        assert (
            node.name in dag or node.node_type == NodeType.SOURCE
        )  # sources may have no deps
        if node.name in dag:
            for dep in dag[node.name]:
                assert dep in [n.name for n in nodes]  # all deps are within nodes


@pytest.mark.asyncio
async def test_deploy_failed_on_non_existent_deps(module__client, basic_nodes):
    response = await module__client.post(
        "/namespaces/random_namespace/deploy",
        json=DeploymentYAML(nodes=basic_nodes).dict(),
    )
    assert (
        response.json()["message"]
        == "The following dependencies are not in the deployment and do not pre-exist in the system: catalog.dim.categories"
    )


@pytest.mark.asyncio
async def test_deploy_succeeds_with_existing_deps(module__client, basic_nodes):
    # First deploy the source node that other nodes depend on
    source_node = [n for n in basic_nodes if n.node_type == NodeType.SOURCE][0]
    response = await module__client.post(
        "/namespaces/catalog/deploy",
        json=DeploymentYAML(nodes=[source_node]).dict(),
    )
    assert response.status_code == 200, response.text

    # Now deploy all nodes together
    response = await module__client.post(
        "/namespaces/example/deploy",
        json=DeploymentYAML(nodes=basic_nodes).dict(),
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["deployed"] == [n.name for n in basic_nodes]
    assert data["skipped"] == []
    assert data["errors"] == {}


def node_class_for_path(path: str):
    if path.startswith("/nodes/source/"):
        return SourceYAML
    elif path.startswith("/nodes/dimension/"):
        return DimensionYAML
    elif path.startswith("/nodes/transform/"):
        return TransformYAML
    elif path.startswith("/nodes/metric/"):
        return MetricYAML
    elif "/link" in path:
        return "link"
    else:
        raise ValueError(f"Unknown node path type: {path}")


def to_columns(columns_list):
    return [ColumnYAML(name=c["name"], type=c["type"]) for c in columns_list]


@pytest.fixture(scope="module")
def default_repair_orders():
    return SourceYAML(
        name="default.repair_orders",
        description="""All repair orders""",
        table="default.roads.repair_orders",
        columns=[
            ColumnYAML(
                name="repair_order_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="municipality_id",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="hard_hat_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="order_date",
                type="timestamp",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="required_date",
                type="timestamp",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="dispatched_date",
                type="timestamp",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="dispatcher_id",
                type="int",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[
            DimensionJoinLinkYAML(
                dimension_node="${prefix}default.repair_order",
                join_type="inner",
                join_on="${prefix}default.repair_orders.repair_order_id = ${prefix}default.repair_order.repair_order_id",
            ),
            DimensionJoinLinkYAML(
                dimension_node="${prefix}default.dispatcher",
                join_type="inner",
                join_on="${prefix}default.repair_orders.dispatcher_id = ${prefix}default.dispatcher.dispatcher_id",
            ),
        ],
    )


@pytest.fixture(scope="module")
def default_repair_orders_view():
    return SourceYAML(
        name="default.repair_orders_view",
        description="""All repair orders (view)""",
        query="""CREATE OR REPLACE VIEW roads.repair_orders_view AS SELECT * FROM roads.repair_orders""",
        table="default.roads.repair_orders_view",
        columns=[
            ColumnYAML(
                name="repair_order_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="municipality_id",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="hard_hat_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="order_date",
                type="timestamp",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="required_date",
                type="timestamp",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="dispatched_date",
                type="timestamp",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="dispatcher_id",
                type="int",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[],
    )


@pytest.fixture(scope="module")
def default_repair_order_details():
    return SourceYAML(
        name="default.repair_order_details",
        description="""Details on repair orders""",
        table="default.roads.repair_order_details",
        columns=[
            ColumnYAML(
                name="repair_order_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="repair_type_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="price",
                type="float",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="quantity",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="discount",
                type="float",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[
            DimensionJoinLinkYAML(
                dimension_node="${prefix}default.repair_order",
                join_type="inner",
                join_on="${prefix}default.repair_order_details.repair_order_id = ${prefix}default.repair_order.repair_order_id",
            ),
            DimensionJoinLinkYAML(
                dimension_node="${prefix}default.repair_order",
                join_type="inner",
                join_on="${prefix}default.repair_order_details.repair_order_id = ${prefix}default.repair_order.repair_order_id",
            ),
        ],
    )


@pytest.fixture(scope="module")
def default_repair_type():
    return SourceYAML(
        name="default.repair_type",
        description="""Information on types of repairs""",
        table="default.roads.repair_type",
        columns=[
            ColumnYAML(
                name="repair_type_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="repair_type_name",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="contractor_id",
                type="int",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[
            DimensionJoinLinkYAML(
                dimension_node="${prefix}default.contractor",
                join_type="inner",
                join_on="${prefix}default.repair_type.contractor_id = ${prefix}default.contractor.contractor_id",
            ),
        ],
    )


@pytest.fixture(scope="module")
def default_contractors():
    return SourceYAML(
        name="default.contractors",
        description="""Information on contractors""",
        table="default.roads.contractors",
        columns=[
            ColumnYAML(
                name="contractor_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="company_name",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="contact_name",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="contact_title",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="address",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="city",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="state",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="postal_code",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="country",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="phone",
                type="string",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[
            DimensionJoinLinkYAML(
                dimension_node="${prefix}default.us_state",
                join_type="inner",
                join_on="${prefix}default.contractors.state = ${prefix}default.us_state.state_short",
            ),
        ],
    )


@pytest.fixture(scope="module")
def default_municipality_municipality_type():
    return SourceYAML(
        name="default.municipality_municipality_type",
        description="""Lookup table for municipality and municipality types""",
        table="default.roads.municipality_municipality_type",
        columns=[
            ColumnYAML(
                name="municipality_id",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="municipality_type_id",
                type="string",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[],
    )


@pytest.fixture(scope="module")
def default_municipality_type():
    return SourceYAML(
        name="default.municipality_type",
        description="""Information on municipality types""",
        table="default.roads.municipality_type",
        columns=[
            ColumnYAML(
                name="municipality_type_id",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="municipality_type_desc",
                type="string",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[],
    )


@pytest.fixture(scope="module")
def default_municipality():
    return SourceYAML(
        name="default.municipality",
        description="""Information on municipalities""",
        table="default.roads.municipality",
        columns=[
            ColumnYAML(
                name="municipality_id",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="contact_name",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="contact_title",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="local_region",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="phone",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="state_id",
                type="int",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[],
    )


@pytest.fixture(scope="module")
def default_dispatchers():
    return SourceYAML(
        name="default.dispatchers",
        description="""Information on dispatchers""",
        table="default.roads.dispatchers",
        columns=[
            ColumnYAML(
                name="dispatcher_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="company_name",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="phone",
                type="string",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[],
    )


@pytest.fixture(scope="module")
def default_hard_hats():
    return SourceYAML(
        name="default.hard_hats",
        description="""Information on employees""",
        table="default.roads.hard_hats",
        columns=[
            ColumnYAML(
                name="hard_hat_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="last_name",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="first_name",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="title",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="birth_date",
                type="timestamp",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="hire_date",
                type="timestamp",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="address",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="city",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="state",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="postal_code",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="country",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="manager",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="contractor_id",
                type="int",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[],
    )


@pytest.fixture(scope="module")
def default_hard_hat_state():
    return SourceYAML(
        name="default.hard_hat_state",
        description="""Lookup table for employee's current state""",
        table="default.roads.hard_hat_state",
        columns=[
            ColumnYAML(
                name="hard_hat_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="state_id",
                type="string",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[],
    )


@pytest.fixture(scope="module")
def default_us_states():
    return SourceYAML(
        name="default.us_states",
        description="""Information on different types of repairs""",
        table="default.roads.us_states",
        columns=[
            ColumnYAML(
                name="state_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="state_name",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="state_abbr",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="state_region",
                type="int",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[],
    )


@pytest.fixture(scope="module")
def default_us_region():
    return SourceYAML(
        name="default.us_region",
        description="""Information on US regions""",
        table="default.roads.us_region",
        columns=[
            ColumnYAML(
                name="us_region_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnYAML(
                name="us_region_description",
                type="string",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[],
    )


@pytest.fixture(scope="module")
def default_repair_order():
    return DimensionYAML(
        name="default.repair_order",
        description="""Repair order dimension""",
        query="""
                        SELECT
                        repair_order_id,
                        municipality_id,
                        hard_hat_id,
                        order_date,
                        required_date,
                        dispatched_date,
                        dispatcher_id
                        FROM ${prefix}default.repair_orders
                    """,
        primary_key=["repair_order_id"],
        dimension_links=[
            DimensionJoinLinkYAML(
                dimension_node="${prefix}default.dispatcher",
                join_type="inner",
                join_on="${prefix}default.repair_order.dispatcher_id = ${prefix}default.dispatcher.dispatcher_id",
            ),
            DimensionJoinLinkYAML(
                dimension_node="${prefix}default.municipality_dim",
                join_type="inner",
                join_on="${prefix}default.repair_order.municipality_id = ${prefix}default.municipality_dim.municipality_id",
            ),
            DimensionJoinLinkYAML(
                dimension_node="${prefix}default.hard_hat",
                join_type="inner",
                join_on="${prefix}default.repair_order.hard_hat_id = ${prefix}default.hard_hat.hard_hat_id",
            ),
        ],
    )


@pytest.fixture(scope="module")
def default_contractor():
    return DimensionYAML(
        name="default.contractor",
        description="""Contractor dimension""",
        query="""
                        SELECT
                        contractor_id,
                        company_name,
                        contact_name,
                        contact_title,
                        address,
                        city,
                        state,
                        postal_code,
                        country,
                        phone
                        FROM ${prefix}default.contractors
                    """,
        primary_key=["contractor_id"],
        dimension_links=[],
    )


@pytest.fixture(scope="module")
def default_hard_hat():
    return DimensionYAML(
        name="default.hard_hat",
        description="""Hard hat dimension""",
        query="""
                        SELECT
                        hard_hat_id,
                        last_name,
                        first_name,
                        title,
                        birth_date,
                        hire_date,
                        address,
                        city,
                        state,
                        postal_code,
                        country,
                        manager,
                        contractor_id
                        FROM ${prefix}default.hard_hats
                    """,
        primary_key=["hard_hat_id"],
        dimension_links=[
            DimensionJoinLinkYAML(
                dimension_node="${prefix}default.us_state",
                join_type="inner",
                join_on="${prefix}default.hard_hat.state = ${prefix}default.us_state.state_short",
            ),
        ],
    )


@pytest.fixture(scope="module")
def default_us_state():
    return DimensionYAML(
        name="default.us_state",
        description="""US state dimension""",
        query="""
                        SELECT
                        state_id,
                        state_name,
                        state_abbr AS state_short,
                        state_region
                        FROM ${prefix}default.us_states s
                    """,
        primary_key=["state_short"],
        dimension_links=[],
    )


@pytest.fixture(scope="module")
def default_dispatcher():
    return DimensionYAML(
        name="default.dispatcher",
        description="""Dispatcher dimension""",
        query="""
                        SELECT
                        dispatcher_id,
                        company_name,
                        phone
                        FROM ${prefix}default.dispatchers
                    """,
        primary_key=["dispatcher_id"],
        dimension_links=[],
    )


@pytest.fixture(scope="module")
def default_municipality_dim():
    return DimensionYAML(
        name="default.municipality_dim",
        description="""Municipality dimension""",
        query="""
                        SELECT
                        m.municipality_id AS municipality_id,
                        contact_name,
                        contact_title,
                        local_region,
                        state_id,
                        mmt.municipality_type_id AS municipality_type_id,
                        mt.municipality_type_desc AS municipality_type_desc
                        FROM ${prefix}default.municipality AS m
                        LEFT JOIN ${prefix}default.municipality_municipality_type AS mmt
                        ON m.municipality_id = mmt.municipality_id
                        LEFT JOIN ${prefix}default.municipality_type AS mt
                        ON mmt.municipality_type_id = mt.municipality_type_desc
                    """,
        primary_key=["municipality_id"],
        dimension_links=[],
    )


@pytest.fixture(scope="module")
def default_regional_level_agg():
    return TransformYAML(
        name="default.regional_level_agg",
        description="""Regional-level aggregates""",
        query="""
WITH ro as (SELECT
        repair_order_id,
        municipality_id,
        hard_hat_id,
        order_date,
        required_date,
        dispatched_date,
        dispatcher_id
    FROM ${prefix}default.repair_orders)
            SELECT
    usr.us_region_id,
    us.state_name,
    CONCAT(us.state_name, '-', usr.us_region_description) AS location_hierarchy,
    EXTRACT(YEAR FROM ro.order_date) AS order_year,
    EXTRACT(MONTH FROM ro.order_date) AS order_month,
    EXTRACT(DAY FROM ro.order_date) AS order_day,
    COUNT(DISTINCT CASE WHEN ro.dispatched_date IS NOT NULL THEN ro.repair_order_id ELSE NULL END) AS completed_repairs,
    COUNT(DISTINCT ro.repair_order_id) AS total_repairs_dispatched,
    SUM(rd.price * rd.quantity) AS total_amount_in_region,
    AVG(rd.price * rd.quantity) AS avg_repair_amount_in_region,
    -- ELEMENT_AT(ARRAY_SORT(COLLECT_LIST(STRUCT(COUNT(*) AS cnt, rt.repair_type_name AS repair_type_name)), (left, right) -> case when left.cnt < right.cnt then 1 when left.cnt > right.cnt then -1 else 0 end), 0).repair_type_name AS most_common_repair_type,
    AVG(DATEDIFF(ro.dispatched_date, ro.order_date)) AS avg_dispatch_delay,
    COUNT(DISTINCT c.contractor_id) AS unique_contractors
FROM ro
JOIN
    ${prefix}default.municipality m ON ro.municipality_id = m.municipality_id
JOIN
    ${prefix}default.us_states us ON m.state_id = us.state_id
                         AND AVG(rd.price * rd.quantity) >
                            (SELECT AVG(price * quantity) FROM ${prefix}default.repair_order_details WHERE repair_order_id = ro.repair_order_id)
JOIN
    ${prefix}default.us_states us ON m.state_id = us.state_id
JOIN
    ${prefix}default.us_region usr ON us.state_region = usr.us_region_id
JOIN
    ${prefix}default.repair_order_details rd ON ro.repair_order_id = rd.repair_order_id
JOIN
    ${prefix}default.repair_type rt ON rd.repair_type_id = rt.repair_type_id
JOIN
    ${prefix}default.contractors c ON rt.contractor_id = c.contractor_id
GROUP BY
    usr.us_region_id,
    EXTRACT(YEAR FROM ro.order_date),
    EXTRACT(MONTH FROM ro.order_date),
    EXTRACT(DAY FROM ro.order_date)""",
        primary_key=[
            "us_region_id",
            "state_name",
            "order_year",
            "order_month",
            "order_day",
        ],
        dimension_links=[],
    )


@pytest.fixture(scope="module")
def default_national_level_agg():
    return TransformYAML(
        name="default.national_level_agg",
        description="""National level aggregates""",
        query="""SELECT SUM(rd.price * rd.quantity) AS total_amount_nationwide FROM ${prefix}default.repair_order_details rd""",
        dimension_links=[],
    )


@pytest.fixture(scope="module")
def default_repair_orders_fact():
    return TransformYAML(
        name="default.repair_orders_fact",
        description="""Fact transform with all details on repair orders""",
        query="""SELECT
  repair_orders.repair_order_id,
  repair_orders.municipality_id,
  repair_orders.hard_hat_id,
  repair_orders.dispatcher_id,
  repair_orders.order_date,
  repair_orders.dispatched_date,
  repair_orders.required_date,
  repair_order_details.discount,
  repair_order_details.price,
  repair_order_details.quantity,
  repair_order_details.repair_type_id,
  repair_order_details.price * repair_order_details.quantity AS total_repair_cost,
  repair_orders.dispatched_date - repair_orders.order_date AS time_to_dispatch,
  repair_orders.dispatched_date - repair_orders.required_date AS dispatch_delay
FROM
  ${prefix}default.repair_orders repair_orders
JOIN
  ${prefix}default.repair_order_details repair_order_details
ON repair_orders.repair_order_id = repair_order_details.repair_order_id""",
        dimension_links=[
            DimensionJoinLinkYAML(
                dimension_node="${prefix}default.municipality_dim",
                join_type="inner",
                join_on="${prefix}default.repair_orders_fact.municipality_id = ${prefix}default.municipality_dim.municipality_id",
            ),
            DimensionJoinLinkYAML(
                dimension_node="${prefix}default.hard_hat",
                join_type="inner",
                join_on="${prefix}default.repair_orders_fact.hard_hat_id = ${prefix}default.hard_hat.hard_hat_id",
            ),
            DimensionJoinLinkYAML(
                dimension_node="${prefix}default.dispatcher",
                join_type="inner",
                join_on="${prefix}default.repair_orders_fact.dispatcher_id = ${prefix}default.dispatcher.dispatcher_id",
            ),
        ],
    )


@pytest.fixture(scope="module")
def default_regional_repair_efficiency():
    return MetricYAML(
        name="default.regional_repair_efficiency",
        description="""For each US region (as defined in the us_region table), we want to calculate:
            Regional Repair Efficiency = (Number of Completed Repairs / Total Repairs Dispatched) ×
                                         (Total Repair Amount in Region / Total Repair Amount Nationwide) × 100
            Here:
                A "Completed Repair" is one where the dispatched_date is not null.
                "Total Repair Amount in Region" is the total amount spent on repairs in a given region.
                "Total Repair Amount Nationwide" is the total amount spent on all repairs nationwide.""",
        query="""SELECT
    (SUM(rm.completed_repairs) * 1.0 / SUM(rm.total_repairs_dispatched)) *
    (SUM(rm.total_amount_in_region) * 1.0 / SUM(na.total_amount_nationwide)) * 100
FROM
    ${prefix}default.regional_level_agg rm
CROSS JOIN
    ${prefix}default.national_level_agg na""",
        dimension_links=[],
    )


@pytest.fixture(scope="module")
def default_num_repair_orders():
    return MetricYAML(
        name="default.num_repair_orders",
        description="""Number of repair orders""",
        query="""SELECT count(repair_order_id) FROM ${prefix}default.repair_orders_fact""",
        dimension_links=[],
    )


@pytest.fixture(scope="module")
def default_avg_repair_price():
    return MetricYAML(
        name="default.avg_repair_price",
        description="""Average repair price""",
        query="""SELECT avg(repair_orders_fact.price) FROM ${prefix}default.repair_orders_fact repair_orders_fact""",
        dimension_links=[],
    )


@pytest.fixture(scope="module")
def default_total_repair_cost():
    return MetricYAML(
        name="default.total_repair_cost",
        description="""Total repair cost""",
        query="""SELECT sum(total_repair_cost) FROM ${prefix}default.repair_orders_fact""",
        dimension_links=[],
    )


@pytest.fixture(scope="module")
def default_avg_length_of_employment():
    return MetricYAML(
        name="default.avg_length_of_employment",
        description="""Average length of employment""",
        query="""SELECT avg(CAST(NOW() AS DATE) - hire_date) FROM ${prefix}default.hard_hat""",
        dimension_links=[],
    )


@pytest.fixture(scope="module")
def default_discounted_orders_rate():
    return MetricYAML(
        name="default.discounted_orders_rate",
        description="""Proportion of Discounted Orders""",
        query="""
                SELECT
                  cast(sum(if(discount > 0.0, 1, 0)) as double) / count(*)
                    AS default_DOT_discounted_orders_rate
                FROM ${prefix}default.repair_orders_fact
                """,
        dimension_links=[],
    )


@pytest.fixture(scope="module")
def default_total_repair_order_discounts():
    return MetricYAML(
        name="default.total_repair_order_discounts",
        description="""Total repair order discounts""",
        query="""SELECT sum(price * discount) FROM ${prefix}default.repair_orders_fact""",
        dimension_links=[],
    )


@pytest.fixture(scope="module")
def default_avg_repair_order_discounts():
    return MetricYAML(
        name="default.avg_repair_order_discounts",
        description="""Average repair order discounts""",
        query="""SELECT avg(price * discount) FROM ${prefix}default.repair_orders_fact""",
        dimension_links=[],
    )


@pytest.fixture(scope="module")
def default_avg_time_to_dispatch():
    return MetricYAML(
        name="default.avg_time_to_dispatch",
        description="""Average time to dispatch a repair order""",
        query="""SELECT avg(cast(repair_orders_fact.time_to_dispatch as int)) FROM ${prefix}default.repair_orders_fact repair_orders_fact""",
        dimension_links=[],
    )


@pytest.fixture(scope="module")
def default_repairs_cube():
    return CubeYAML(
        name="default.repairs_cube",
        display_name="Repairs Cube",
        description="""Cube for analyzing repair orders""",
        dimensions=[
            "${prefix}default.hard_hat.state",
            "${prefix}default.dispatcher.company_name",
            "${prefix}default.municipality_dim.local_region",
        ],
        metrics=[
            "${prefix}default.num_repair_orders",
            "${prefix}default.avg_repair_price",
            "${prefix}default.total_repair_cost",
        ],
    )


@pytest.fixture(scope="module")
def roads_nodes(
    default_repair_orders,
    default_repair_orders_view,
    default_repair_order_details,
    default_repair_type,
    default_contractors,
    default_municipality_municipality_type,
    default_municipality_type,
    default_municipality,
    default_dispatchers,
    default_hard_hats,
    default_hard_hat_state,
    default_us_states,
    default_us_region,
    default_repair_order,
    default_contractor,
    default_hard_hat,
    default_us_state,
    default_dispatcher,
    default_municipality_dim,
    default_regional_level_agg,
    default_national_level_agg,
    default_repair_orders_fact,
    default_regional_repair_efficiency,
    default_num_repair_orders,
    default_avg_repair_price,
    default_total_repair_cost,
    default_avg_length_of_employment,
    default_discounted_orders_rate,
    default_total_repair_order_discounts,
    default_avg_repair_order_discounts,
    default_avg_time_to_dispatch,
    default_repairs_cube,
):
    return [
        default_repair_orders,
        default_repair_orders_view,
        default_repair_order_details,
        default_repair_type,
        default_contractors,
        default_municipality_municipality_type,
        default_municipality_type,
        default_municipality,
        default_dispatchers,
        default_hard_hats,
        default_hard_hat_state,
        default_us_states,
        default_us_region,
        default_repair_order,
        default_contractor,
        default_hard_hat,
        default_us_state,
        default_dispatcher,
        default_municipality_dim,
        default_regional_level_agg,
        default_national_level_agg,
        default_repair_orders_fact,
        default_regional_repair_efficiency,
        default_num_repair_orders,
        default_avg_repair_price,
        default_total_repair_cost,
        default_avg_length_of_employment,
        default_discounted_orders_rate,
        default_total_repair_order_discounts,
        default_avg_repair_order_discounts,
        default_avg_time_to_dispatch,
        default_repairs_cube,
    ]


async def test_roads_deployment(module__client, roads_nodes):
    response = await module__client.post(
        "/namespaces/base/deploy",
        json=DeploymentYAML(namespace="base", nodes=roads_nodes).dict(),
    )
    assert response.json() == {
        "links": 13,
        "namespace": "base",
        "nodes": [
            {
                "current_version": "v1.0",
                "name": "base.default.contractors",
            },
            {
                "current_version": "v1.0",
                "name": "base.default.hard_hats",
            },
            {
                "current_version": "v1.0",
                "name": "base.default.municipality",
            },
            {
                "current_version": "v1.0",
                "name": "base.default.repair_order_details",
            },
            {
                "current_version": "v1.0",
                "name": "base.default.repair_orders",
            },
            {
                "current_version": "v1.0",
                "name": "base.default.repair_type",
            },
            {
                "current_version": "v1.0",
                "name": "base.default.us_region",
            },
            {
                "current_version": "v1.0",
                "name": "base.default.us_states",
            },
            {
                "current_version": "v1.0",
                "name": "base.default.dispatchers",
            },
            {
                "current_version": "v1.0",
                "name": "base.default.hard_hat",
            },
            {
                "current_version": "v1.0",
                "name": "base.default.municipality_municipality_type",
            },
            {
                "current_version": "v1.0",
                "name": "base.default.municipality_type",
            },
            {
                "current_version": "v1.0",
                "name": "base.default.national_level_agg",
            },
            {
                "current_version": "v1.0",
                "name": "base.default.regional_level_agg",
            },
            {
                "current_version": "v1.0",
                "name": "base.default.repair_orders_fact",
            },
            {
                "current_version": "v1.0",
                "name": "base.default.avg_length_of_employment",
            },
            {
                "current_version": "v1.0",
                "name": "base.default.avg_repair_order_discounts",
            },
            {
                "current_version": "v1.0",
                "name": "base.default.avg_repair_price",
            },
            {
                "current_version": "v1.0",
                "name": "base.default.avg_time_to_dispatch",
            },
            {
                "current_version": "v1.0",
                "name": "base.default.contractor",
            },
            {
                "current_version": "v1.0",
                "name": "base.default.discounted_orders_rate",
            },
            {
                "current_version": "v1.0",
                "name": "base.default.dispatcher",
            },
            {
                "current_version": "v1.0",
                "name": "base.default.hard_hat_state",
            },
            {
                "current_version": "v1.0",
                "name": "base.default.municipality_dim",
            },
            {
                "current_version": "v1.0",
                "name": "base.default.num_repair_orders",
            },
            {
                "current_version": "v1.0",
                "name": "base.default.regional_repair_efficiency",
            },
            {
                "current_version": "v1.0",
                "name": "base.default.repair_order",
            },
            {
                "current_version": "v1.0",
                "name": "base.default.repair_orders_view",
            },
            {
                "current_version": "v1.0",
                "name": "base.default.total_repair_cost",
            },
            {
                "current_version": "v1.0",
                "name": "base.default.total_repair_order_discounts",
            },
            {
                "current_version": "v1.0",
                "name": "base.default.us_state",
            },
            {
                "current_version": "v1.0",
                "name": "base.default.repairs_cube",
            },
        ],
    }
    response = await module__client.get("/nodes?namespace=base")
    data = response.json()
    assert len(data) == len(roads_nodes)
