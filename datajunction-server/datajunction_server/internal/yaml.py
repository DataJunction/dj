import asyncio
import logging
from typing import Callable, cast
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from fastapi import Request, BackgroundTasks
from datajunction_server.database.user import User
from datajunction_server.database import Node
from datajunction_server.models import access
from sqlalchemy.ext.asyncio import AsyncSession
from datajunction_server.models.yaml import (
    CubeYAML,
    DeploymentInfo,
    DeploymentYAML,
    DimensionJoinLinkYAML,
    DimensionLinkYAML,
    DimensionReferenceLinkYAML,
    LinkableNodeYAML,
    MetricYAML,
    SourceYAML,
)
from datajunction_server.models.dimensionlink import (
    JoinLinkInput,
    LinkType,
)
from datajunction_server.models.node import (
    AttributeOutput,
    AttributeTypeName,
    CreateCubeNode,
    CreateNode,
    CreateSourceNode,
    MetricMetadataInput,
    NodeOutput,
    NodeType,
    SourceColumnOutput,
)
from datajunction_server.internal.nodes import (
    create_a_cube,
    create_a_node,
    create_a_source_node,
    upsert_reference_dimension_link,
    upsert_simple_dimension_link,
    upsert_complex_dimension_link,
)
from datajunction_server.models.yaml import NodeYAML
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.errors import (
    DJGraphCycleException,
    DJInvalidDeploymentConfig,
)
from datajunction_server.sql.parsing.backends.antlr4 import parse, ast
from datajunction_server.utils import SEPARATOR, session_context
from datajunction_server.internal.caching.interface import Cache
from datajunction_server.internal.namespaces import create_namespace

logger = logging.getLogger(__name__)


def extract_node_graph(nodes: list[NodeYAML], namespace: str) -> dict[str, list[str]]:
    """
    Extract the node graph from a list of nodes
        Scenario A: Fully self-contained deployment with all dependencies included
    """
    logger.info("Extracting node graph for %d nodes", len(nodes))

    def find_upstreams_for_node(node: NodeYAML) -> tuple[str, list[str]]:
        cls_name = node.__class__.__name__
        print("node.fully_qualified_name", node.fully_qualified_name)
        if (
            cls_name in ("MetricYAML", "TransformYAML", "DimensionYAML")
            and node.rendered_query
        ):
            # node.query = render_prefixes(node.query, node._namespace or namespace)
            query_ast = parse(node.rendered_query)
            cte_names = [cte.alias_or_name.identifier() for cte in query_ast.ctes]
            tables = {
                t.name.identifier()
                for t in query_ast.find_all(ast.Table)
                if t.name.identifier() not in cte_names
            }
            return node.fully_qualified_name, sorted(list(tables))
        if cls_name == "CubeYAML":
            dimension_nodes = [dim.rsplit(".", 1)[0] for dim in node.dimensions]
            return node.fully_qualified_name, node.metrics + dimension_nodes
        return node.fully_qualified_name, []

    dependencies_map: dict[str, list[str]] = {}
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(find_upstreams_for_node, node) for node in nodes]
        for future in as_completed(futures):
            name, deps = future.result()
            dependencies_map[name] = deps

    logger.info("Extracted node graph with %d entries", len(dependencies_map))
    return dependencies_map


def topological_levels(
    graph: dict[str, list[str]],
    ascending: bool = True,
) -> list[list[str]]:
    """
    Perform a topological sort on a directed acyclic graph (DAG) and
    return the nodes based on their levels.

    Args:
        graph (dict): A dictionary representing the DAG where keys are node names
                      and values are lists of upstream node names.

    Returns:
        list: A list of node names sorted in topological order.

    Raises:
        ValueError: If the graph contains a cycle.
    """
    # If there are any external dependencies, add them to the adjacency list
    for deps in list(graph.values()):
        for dep in deps:
            if dep not in graph:
                graph[dep] = []

    in_degree = defaultdict(int)
    for node in graph:
        in_degree[node] = 0
    for deps in graph.values():
        for dep in deps:
            in_degree[dep] += 1

    levels = []
    current = [n for n, d in in_degree.items() if d == 0]
    while current:
        levels.append(sorted(current))
        next_level = []
        for node in current:
            for dep in graph.get(node, []):
                in_degree[dep] -= 1
                if in_degree[dep] == 0:
                    next_level.append(dep)
        current = next_level

    if sum(in_degree.values()) != 0:
        raise DJGraphCycleException("The graph contains a cycle!")

    return levels if ascending else levels[::-1]


async def deploy_source_node_from_yaml(
    source_yaml: SourceYAML,
    session: AsyncSession,
    current_user: User,
    request: Request,
    query_service_client: QueryServiceClient,
    validate_access: access.ValidateAccessFn,
    background_tasks: BackgroundTasks,
    save_history: Callable,
) -> NodeOutput:
    """
    Deploy a source node from its YAML representation.
    """
    # TODO Handle case where there are no columns on source_yaml.columns and it's registering a table
    catalog, schema, table = source_yaml.table.split(".")
    node_output = await create_a_source_node(
        data=CreateSourceNode(
            name=source_yaml.fully_qualified_name,
            display_name=source_yaml.display_name,
            description=source_yaml.description,
            mode=source_yaml.mode,
            primary_key=source_yaml.primary_key,
            custom_metadata=source_yaml.custom_metadata,
            owners=source_yaml.owners,
            catalog=catalog,
            schema_=schema,
            table=table,
            columns=[
                SourceColumnOutput(
                    name=col.name,
                    type=col.type,
                    attributes=[
                        AttributeOutput(AttributeTypeName(name=attr))
                        for attr in col.attributes
                    ],
                )
                for col in source_yaml.columns or []
            ],
        ),
        session=session,
        current_user=current_user,
        request=request,
        query_service_client=query_service_client,
        validate_access=validate_access,
        background_tasks=background_tasks,
        save_history=save_history,
    )
    return node_output


async def deploy_dimension_link_from_yaml(
    namespace: str,
    node_yaml: NodeYAML,
    link_yaml: DimensionLinkYAML,
    request: Request,
    current_user: User,
    save_history: Callable,
):
    async with session_context(request) as session:
        user = cast(User, await User.get_by_username(session, current_user.username))
        if link_yaml.type == LinkType.JOIN:
            join_link = cast(DimensionJoinLinkYAML, link_yaml)
            if join_link.node_column:
                return await upsert_simple_dimension_link(
                    session,
                    node_yaml.fully_qualified_name,
                    join_link.rendered_dimension_node,
                    join_link.node_column,
                    None,
                    user,
                    save_history,
                )
            link_input = JoinLinkInput(
                dimension_node=join_link.rendered_dimension_node,
                join_type=join_link.join_type,
                join_on=join_link.rendered_join_on,
                role=join_link.role,
            )
            return await upsert_complex_dimension_link(
                session,
                node_yaml.fully_qualified_name,
                link_input,
                user,
                save_history,
            )
        reference_link = cast(DimensionReferenceLinkYAML, link_yaml)
        return await upsert_reference_dimension_link(
            session=session,
            node_name=node_yaml.fully_qualified_name,
            node_column=reference_link.node_column,
            dimension_node=reference_link.dimension_node,
            dimension_column=reference_link.dimension_attribute,
            role=reference_link.role,
            current_user=user,
            save_history=save_history,
        )


async def deploy_node_from_yaml(
    node_yaml: NodeYAML,
    current_user: User,
    request: Request,
    query_service_client: QueryServiceClient,
    validate_access: access.ValidateAccessFn,
    background_tasks: BackgroundTasks,
    save_history: Callable,
    cache: Cache,
) -> Node:
    """
    Deploy a node from its YAML representation.
    """
    async with session_context(request) as session:
        user = cast(User, await User.get_by_username(session, current_user.username))
        cls_name = node_yaml.__class__.__name__
        if cls_name == "SourceYAML":
            source_yaml = cast(SourceYAML, node_yaml)
            return await deploy_source_node_from_yaml(
                source_yaml=source_yaml,
                session=session,
                current_user=user,
                request=request,
                query_service_client=query_service_client,
                validate_access=validate_access,
                background_tasks=background_tasks,
                save_history=save_history,
            )
        if cls_name in ("TransformYAML", "DimensionYAML"):
            node_type = (
                NodeType.TRANSFORM
                if cls_name == "TransformYAML"
                else NodeType.DIMENSION
            )
            created_node = await create_a_node(
                data=CreateNode(
                    name=node_yaml.fully_qualified_name,
                    display_name=node_yaml.display_name,
                    description=node_yaml.description,
                    mode=node_yaml.mode,
                    primary_key=node_yaml.primary_key,
                    custom_metadata=node_yaml.custom_metadata,
                    owners=node_yaml.owners,
                    query=node_yaml.rendered_query,
                ),
                node_type=node_type,
                session=session,
                current_user=user,
                request=request,
                query_service_client=query_service_client,
                validate_access=validate_access,
                background_tasks=background_tasks,
                save_history=save_history,
                cache=cache,
            )
            return created_node
        if cls_name == "MetricYAML":
            metric_yaml = cast(MetricYAML, node_yaml)
            created_node = await create_a_node(
                data=CreateNode(
                    name=metric_yaml.fully_qualified_name,
                    display_name=metric_yaml.display_name,
                    description=metric_yaml.description,
                    mode=metric_yaml.mode,
                    custom_metadata=metric_yaml.custom_metadata,
                    owners=metric_yaml.owners,
                    query=metric_yaml.rendered_query,
                    required_dimensions=metric_yaml.required_dimensions,
                    metric_metadata=MetricMetadataInput(
                        direction=metric_yaml.direction,
                        unit=metric_yaml.unit,
                        significant_digits=metric_yaml.significant_digits,
                        min_decimal_exponent=metric_yaml.min_decimal_exponent,
                        max_decimal_exponent=metric_yaml.max_decimal_exponent,
                    )
                    if metric_yaml.direction
                    or metric_yaml.unit
                    or metric_yaml.significant_digits
                    else None,
                ),
                node_type=NodeType.METRIC,
                session=session,
                current_user=user,
                request=request,
                query_service_client=query_service_client,
                validate_access=validate_access,
                background_tasks=background_tasks,
                save_history=save_history,
                cache=cache,
            )
            return created_node
        if cls_name == "CubeYAML":
            cube_yaml = cast(CubeYAML, node_yaml)
            return await create_a_cube(
                data=CreateCubeNode(
                    name=cube_yaml.fully_qualified_name,
                    display_name=cube_yaml.display_name,
                    description=cube_yaml.description,
                    mode=cube_yaml.mode,
                    custom_metadata=cube_yaml.custom_metadata,
                    owners=cube_yaml.owners,
                    metrics=cube_yaml.rendered_metrics,
                    dimensions=cube_yaml.rendered_dimensions,
                    filters=cube_yaml.rendered_filters,
                ),
                request=request,
                session=session,
                current_user=user,
                query_service_client=query_service_client,
                background_tasks=background_tasks,
                validate_access=validate_access,
                save_history=save_history,
            )
    raise ValueError(f"Unknown node type: {cls_name}")  # pragma: no cover


def render_prefixes(parameterized_string: str, prefix: str):
    """
    Replaces ${prefix} in a string
    """
    return parameterized_string.replace("${prefix}", f"{prefix}.")


async def deploy_namespace(
    session: AsyncSession,
    namespace: str,
    deployment: DeploymentYAML,
    current_user: User,
    request: Request,
    query_service_client: QueryServiceClient,
    save_history: Callable,
    validate_access: access.ValidateAccessFn,
    background_tasks: BackgroundTasks,
    cache: Cache,
):
    """
    Deploy a namespace from its YAML representation.
    """
    logger.info(
        "Deploying namespace %s with %d nodes",
        namespace,
        len(deployment.nodes),
    )

    # Create necessary namespaces for the deployment
    namespaces = [namespace] + [
        f"{namespace}{SEPARATOR}{node.name.rsplit('.', 1)[0]}"
        for node in deployment.nodes
        if SEPARATOR in node.name
    ]
    logger.info("Creating namespaces if they do not exist: %s", set(namespaces))
    for nspace in set(namespaces):
        await create_namespace(
            session=session,
            namespace=nspace,
            current_user=current_user,
            save_history=save_history,
            include_parents=True,
        )

    # Figure out the deployment order (cubes should always be after everything else)
    node_graph = extract_node_graph(
        [node for node in deployment.nodes if not isinstance(node, CubeYAML)],
        namespace,
    )

    # Check for any dependencies that are not in the deployment
    deps_not_in_deployment = list(
        {dep for deps in node_graph.values() for dep in deps if dep not in node_graph},
    )
    if deps_not_in_deployment:
        logger.warning(
            "The following dependencies are not defined in the deployment: %s. "
            "They must pre-exist in the system before this deployment can succeed.",
            deps_not_in_deployment,
        )
        external_node_deps = await Node.get_by_names(session, deps_not_in_deployment)
        if len(external_node_deps) != len(deps_not_in_deployment):
            missing_nodes = set(deps_not_in_deployment) - {
                node.name for node in external_node_deps
            }
            raise DJInvalidDeploymentConfig(
                message=(
                    "The following dependencies are not in the deployment and do not"
                    " pre-exist in the system: " + ", ".join(missing_nodes)
                ),
            )
        logger.info(
            "All %d external dependencies pre-exist in the system",
            len(external_node_deps),
        )

    levels = topological_levels(node_graph, ascending=False)
    logger.info(
        "Deploying nodes in topological order with %d levels",
        len(levels),
    )
    name_to_node = {node.fully_qualified_name: node for node in deployment.nodes}
    deployed_nodes = []
    for level in levels:
        logger.info("Deploying level with %d nodes: %s", len(level), level)

        tasks = []
        for node_name in level:
            if node_name in deps_not_in_deployment:
                continue

            node_yaml = name_to_node[node_name]
            tasks.append(
                deploy_node_from_yaml(
                    node_yaml=node_yaml,
                    current_user=current_user,
                    request=request,
                    query_service_client=query_service_client,
                    validate_access=validate_access,
                    background_tasks=background_tasks,
                    save_history=save_history,
                    cache=cache,
                ),
            )
        deployed_level_nodes = await asyncio.gather(*tasks)
        deployed_nodes.extend(deployed_level_nodes)

    # Deploy dimension links
    link_tasks = []
    for node_yaml in deployment.nodes:
        if isinstance(node_yaml, LinkableNodeYAML):

            async def deploy_links_for_node(node_yaml):
                # run links sequentially for this node to avoid race conditions
                linkable_node_yaml = cast(LinkableNodeYAML, node_yaml)
                results = 0
                for link in linkable_node_yaml.dimension_links:
                    await deploy_dimension_link_from_yaml(
                        namespace=namespace,
                        node_yaml=linkable_node_yaml,
                        link_yaml=link,
                        request=request,
                        current_user=current_user,
                        save_history=save_history,
                    )
                    results += 1
                return results

            link_tasks.append(deploy_links_for_node(node_yaml=node_yaml))

    deployed_links = await asyncio.gather(*link_tasks)

    # deploy cubes
    cubes = [node for node in deployment.nodes if isinstance(node, CubeYAML)]
    cube_tasks = []
    for cube_yaml in cubes:
        cube_tasks.append(
            deploy_node_from_yaml(
                node_yaml=cube_yaml,
                current_user=current_user,
                request=request,
                query_service_client=query_service_client,
                validate_access=validate_access,
                background_tasks=background_tasks,
                save_history=save_history,
                cache=cache,
            ),
        )
    deployed_cubes = await asyncio.gather(*cube_tasks)
    logger.info("Finished deploying %d cubes", len(deployed_cubes))
    deployed_nodes.extend(deployed_cubes)
    logger.info("Finished deploying namespace %s", namespace)
    return DeploymentInfo(
        namespace=namespace,
        nodes=deployed_nodes,
        links=sum(deployed_links),
    )
