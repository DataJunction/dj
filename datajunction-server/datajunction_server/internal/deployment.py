import asyncio
from enum import Enum
import logging
from typing import Awaitable, Callable, cast
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from fastapi import Request, BackgroundTasks
from datajunction_server.database.user import User
from datajunction_server.database import Node
from datajunction_server.models import access
from sqlalchemy.ext.asyncio import AsyncSession
from datajunction_server.models.deployment import (
    CubeSpec,
    DeploymentInfo,
    DeploymentSpec,
    DimensionJoinLinkSpec,
    DimensionLinkSpec,
    DimensionReferenceLinkSpec,
    DimensionSpec,
    LinkableNodeSpec,
    MetricSpec,
    SourceSpec,
    NodeSpec,
    TransformSpec,
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
    UpdateNode,
)
from datajunction_server.internal.nodes import (
    create_a_cube,
    create_a_node,
    create_a_source_node,
    refresh_source,
    update_any_node,
    upsert_reference_dimension_link,
    upsert_simple_dimension_link,
    upsert_complex_dimension_link,
)
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


def extract_node_graph(nodes: list[NodeSpec]) -> dict[str, list[str]]:
    """
    Extract the node graph from a list of nodes
        Scenario A: Fully self-contained deployment with all dependencies included
    """
    logger.info("Extracting node graph for %d nodes", len(nodes))

    def find_upstreams_for_node(node: NodeSpec) -> tuple[str, list[str]]:
        cls_name = node.__class__.__name__
        print("node.fully_qualified_name", node.fully_qualified_name)
        if (
            cls_name in ("MetricSpec", "TransformSpec", "DimensionSpec")
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
        if cls_name == "CubeSpec":
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


async def deploy_source_node_from_spec(
    node_spec: SourceSpec,
    session: AsyncSession,
    current_user: User,
    request: Request,
    query_service_client: QueryServiceClient,
    validate_access: access.ValidateAccessFn,
    background_tasks: BackgroundTasks,
    save_history: Callable,
    existing: bool = False,
    **kwargs,
) -> NodeOutput:
    """
    Deploy a source node from its spec.
    """
    # TODO Handle case where there are no columns on source_spec.columns and it's registering a table
    catalog, schema, table = node_spec.table.split(".")
    if existing:
        return await refresh_source(  # type: ignore
            name=node_spec.fully_qualified_name,
            session=session,
            request=request,
            query_service_client=query_service_client,
            current_user=current_user,
            save_history=save_history,
        )
    node_output = await create_a_source_node(
        data=CreateSourceNode(
            name=node_spec.fully_qualified_name,
            display_name=node_spec.display_name,
            description=node_spec.description,
            mode=node_spec.mode,
            primary_key=node_spec.primary_key,
            custom_metadata=node_spec.custom_metadata,
            owners=node_spec.owners,
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
                for col in node_spec.columns or []
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


async def deploy_transform_dimension_node_from_spec(
    node_spec: TransformSpec | DimensionSpec,
    session: AsyncSession,
    current_user: User,
    request: Request,
    query_service_client: QueryServiceClient,
    validate_access: access.ValidateAccessFn,
    background_tasks: BackgroundTasks,
    save_history: Callable,
    cache: Cache,
    existing: bool = False,
) -> Node:
    """
    Deploy a source node from its spec.
    """
    if existing:
        request_headers = dict(request.headers)
        await update_any_node(
            node_spec.fully_qualified_name,
            data=UpdateNode(
                display_name=node_spec.display_name,
                description=node_spec.description,
                mode=node_spec.mode,
                primary_key=node_spec.primary_key,
                custom_metadata=node_spec.custom_metadata,
                owners=node_spec.owners,
                query=node_spec.rendered_query,
            ),
            session=session,
            query_service_client=query_service_client,
            current_user=current_user,
            background_tasks=background_tasks,
            validate_access=validate_access,
            request_headers=request_headers,
            save_history=save_history,
            refresh_materialization=True,
            cache=cache,
        )
        return await Node.get_by_name(  # type: ignore
            session,
            node_spec.fully_qualified_name,
            options=NodeOutput.load_options(),
            raise_if_not_exists=True,
        )
    created_node = await create_a_node(
        data=CreateNode(
            name=node_spec.fully_qualified_name,
            display_name=node_spec.display_name,
            description=node_spec.description,
            mode=node_spec.mode,
            primary_key=node_spec.primary_key,
            custom_metadata=node_spec.custom_metadata,
            owners=node_spec.owners,
            query=node_spec.rendered_query,
        ),
        node_type=node_spec.node_type,
        session=session,
        current_user=current_user,
        request=request,
        query_service_client=query_service_client,
        validate_access=validate_access,
        background_tasks=background_tasks,
        save_history=save_history,
        cache=cache,
    )
    return created_node


async def deploy_metric_node_from_spec(
    node_spec: MetricSpec,
    session: AsyncSession,
    current_user: User,
    request: Request,
    query_service_client: QueryServiceClient,
    validate_access: access.ValidateAccessFn,
    background_tasks: BackgroundTasks,
    save_history: Callable,
    cache: Cache,
    existing: bool = False,
) -> Node:
    """
    Deploy a metric node from its spec.
    """
    metric_metadata_input = (
        MetricMetadataInput(
            direction=node_spec.direction,
            unit=node_spec.unit,
            significant_digits=node_spec.significant_digits,
            min_decimal_exponent=node_spec.min_decimal_exponent,
            max_decimal_exponent=node_spec.max_decimal_exponent,
        )
        if node_spec.direction
        or node_spec.unit
        or node_spec.significant_digits
        or node_spec.min_decimal_exponent
        or node_spec.max_decimal_exponent
        else None
    )
    if existing:
        request_headers = dict(request.headers)
        await update_any_node(
            node_spec.fully_qualified_name,
            data=UpdateNode(
                display_name=node_spec.display_name,
                description=node_spec.description,
                mode=node_spec.mode,
                custom_metadata=node_spec.custom_metadata,
                owners=node_spec.owners,
                query=node_spec.rendered_query,
                required_dimensions=node_spec.required_dimensions,
                metric_metadata=metric_metadata_input,
            ),
            session=session,
            query_service_client=query_service_client,
            current_user=current_user,
            background_tasks=background_tasks,
            validate_access=validate_access,
            request_headers=request_headers,
            save_history=save_history,
            refresh_materialization=True,
            cache=cache,
        )
        return await Node.get_by_name(  # type: ignore
            session,
            node_spec.fully_qualified_name,
            options=NodeOutput.load_options(),
            raise_if_not_exists=True,
        )
    created_node = await create_a_node(
        data=CreateNode(
            name=node_spec.fully_qualified_name,
            display_name=node_spec.display_name,
            description=node_spec.description,
            mode=node_spec.mode,
            custom_metadata=node_spec.custom_metadata,
            owners=node_spec.owners,
            query=node_spec.rendered_query,
            required_dimensions=node_spec.required_dimensions,
            metric_metadata=metric_metadata_input,
        ),
        node_type=NodeType.METRIC,
        session=session,
        current_user=current_user,
        request=request,
        query_service_client=query_service_client,
        validate_access=validate_access,
        background_tasks=background_tasks,
        save_history=save_history,
        cache=cache,
    )
    return created_node


async def deploy_cube_node_from_spec(
    node_spec: CubeSpec,
    session: AsyncSession,
    current_user: User,
    request: Request,
    query_service_client: QueryServiceClient,
    validate_access: access.ValidateAccessFn,
    background_tasks: BackgroundTasks,
    save_history: Callable,
    cache: Cache,
    existing: bool = False,
    **kwargs,
) -> Node:
    if existing:
        logger.info("Updating cube node %s", node_spec.fully_qualified_name)
        request_headers = dict(request.headers)
        await update_any_node(
            node_spec.fully_qualified_name,
            data=UpdateNode(
                display_name=node_spec.display_name,
                description=node_spec.description,
                mode=node_spec.mode,
                custom_metadata=node_spec.custom_metadata,
                owners=node_spec.owners,
                metrics=node_spec.rendered_metrics,
                dimensions=node_spec.rendered_dimensions,
                filters=node_spec.rendered_filters,
            ),
            session=session,
            query_service_client=query_service_client,
            current_user=current_user,
            background_tasks=background_tasks,
            validate_access=validate_access,
            request_headers=request_headers,
            save_history=save_history,
            refresh_materialization=True,
            cache=cache,
        )
        return await Node.get_by_name(  # type: ignore
            session,
            node_spec.fully_qualified_name,
            options=NodeOutput.load_options(),
            raise_if_not_exists=True,
        )
    return await create_a_cube(
        data=CreateCubeNode(
            name=node_spec.fully_qualified_name,
            display_name=node_spec.display_name,
            description=node_spec.description,
            mode=node_spec.mode,
            custom_metadata=node_spec.custom_metadata,
            owners=node_spec.owners,
            metrics=node_spec.rendered_metrics,
            dimensions=node_spec.rendered_dimensions,
            filters=node_spec.rendered_filters,
        ),
        request=request,
        session=session,
        current_user=current_user,
        query_service_client=query_service_client,
        background_tasks=background_tasks,
        validate_access=validate_access,
        save_history=save_history,
    )


async def deploy_dimension_link_from_spec(
    node_spec: NodeSpec,
    link_spec: DimensionLinkSpec,
    request: Request,
    current_user: User,
    save_history: Callable,
):
    async with session_context(request) as session:
        user = cast(User, await User.get_by_username(session, current_user.username))
        if link_spec.type == LinkType.JOIN:
            join_link = cast(DimensionJoinLinkSpec, link_spec)
            if join_link.node_column:
                return await upsert_simple_dimension_link(
                    session,
                    node_spec.fully_qualified_name,
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
                node_spec.fully_qualified_name,
                link_input,
                user,
                save_history,
            )
        reference_link = cast(DimensionReferenceLinkSpec, link_spec)
        return await upsert_reference_dimension_link(
            session=session,
            node_name=node_spec.fully_qualified_name,
            node_column=reference_link.node_column,
            dimension_node=reference_link.dimension_node,
            dimension_column=reference_link.dimension_attribute,
            role=reference_link.role,
            current_user=user,
            save_history=save_history,
        )


async def deploy_node_from_spec(
    node_spec: NodeSpec,
    current_user: User,
    request: Request,
    query_service_client: QueryServiceClient,
    validate_access: access.ValidateAccessFn,
    background_tasks: BackgroundTasks,
    save_history: Callable,
    cache: Cache,
    existing: bool = False,
) -> Node:
    """
    Deploy a node from its specification.
    """
    node_deployers: dict[NodeType, Callable[..., Awaitable[Node]]] = {
        NodeType.SOURCE: deploy_source_node_from_spec,
        NodeType.TRANSFORM: deploy_transform_dimension_node_from_spec,
        NodeType.DIMENSION: deploy_transform_dimension_node_from_spec,
        NodeType.METRIC: deploy_metric_node_from_spec,
        NodeType.CUBE: deploy_cube_node_from_spec,
    }

    async with session_context(request) as session:
        user = cast(User, await User.get_by_username(session, current_user.username))
        deploy_fn = node_deployers.get(node_spec.node_type)
        if not deploy_fn:
            raise DJInvalidDeploymentConfig(f"Unknown node type: {node_spec.node_type}")
        return await deploy_fn(
            node_spec=node_spec,
            session=session,
            current_user=user,
            request=request,
            query_service_client=query_service_client,
            validate_access=validate_access,
            background_tasks=background_tasks,
            save_history=save_history,
            cache=cache,
            existing=existing,
        )


def render_prefixes(parameterized_string: str, prefix: str):
    """
    Replaces ${prefix} in a string
    """
    return parameterized_string.replace("${prefix}", f"{prefix}.")


async def create_deployment_namespaces(
    deployment: DeploymentSpec,
    session: AsyncSession,
    current_user: User,
    save_history: Callable,
):
    namespaces = [deployment.namespace] + [
        f"{deployment.namespace}{SEPARATOR}{node.name.rsplit('.', 1)[0]}"
        for node in deployment.nodes
        if SEPARATOR in node.name
    ]
    namespace_set = set(namespaces)
    logger.info("Creating namespaces if they do not exist: %s", namespace_set)
    for nspace in namespace_set:
        await create_namespace(
            session=session,
            namespace=nspace,
            current_user=current_user,
            save_history=save_history,
            include_parents=True,
        )


async def deploy_namespace(
    session: AsyncSession,
    namespace: str,
    deployment: DeploymentSpec,
    current_user: User,
    request: Request,
    query_service_client: QueryServiceClient,
    save_history: Callable,
    validate_access: access.ValidateAccessFn,
    background_tasks: BackgroundTasks,
    cache: Cache,
):
    """
    Deploy to a namespace based on the given deployment specification.
    """
    logger.info(
        "Deploying namespace %s with %d nodes and %d tags",
        namespace,
        len(deployment.nodes),
        len(deployment.tags),
    )

    class DeployAction(str, Enum):
        CREATE = "create"
        UPDATE = "update"

    # Track what gets deployed successfully
    deployed_nodes: list[Node] = []
    deployed_links: list[int] = []  # e.g., IDs of deployed links
    deployed_cubes: list[Node] = []

    # Snapshot existing state for rollback
    non_cubes = [
        n.fully_qualified_name for n in deployment.nodes if n.node_type != NodeType.CUBE
    ]
    cubes = [
        n.fully_qualified_name for n in deployment.nodes if n.node_type == NodeType.CUBE
    ]
    all_nodes = await Node.get_by_names(session, non_cubes) + [
        await Node.get_cube_by_name(session, cube) for cube in cubes
    ]
    existing_nodes_map: dict[str, NodeSpec] = {
        node.name: await node.to_spec(session) for node in all_nodes if node
    }
    nodes_to_deploy: list[NodeSpec] = []
    for node_spec in deployment.nodes:
        existing_spec = existing_nodes_map.get(node_spec.fully_qualified_name)
        if not existing_spec:
            logger.info(
                "Node %s does not exist, will create",
                node_spec.fully_qualified_name,
            )
            nodes_to_deploy.append(node_spec)
        elif node_spec != existing_spec:
            logger.info("Detected change for node %s", node_spec.fully_qualified_name)
            nodes_to_deploy.append(node_spec)
        else:
            logger.info(
                "Node %s is unchanged, skipping",
                node_spec.fully_qualified_name,
            )

    if not nodes_to_deploy:
        logger.info("No changes detected, skipping deployment")
        return DeploymentInfo(namespace=namespace, nodes=[], links=0)

    # TODO: Create tags

    # Create necessary namespaces for the deployment
    await create_deployment_namespaces(deployment, session, current_user, save_history)

    # Figure out the deployment order (cubes should always be after everything else)
    node_graph = extract_node_graph(
        [node for node in deployment.nodes if not isinstance(node, CubeSpec)],
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
    name_to_node = {node.fully_qualified_name: node for node in nodes_to_deploy}
    deployed_nodes = []
    for level in levels:
        logger.info("Deploying level with %d nodes: %s", len(level), level)

        tasks = []
        for node_name in level:
            if node_name in deps_not_in_deployment or node_name not in name_to_node:
                continue

            node_spec = name_to_node[node_name]
            tasks.append(
                deploy_node_from_spec(
                    node_spec=node_spec,
                    current_user=current_user,
                    request=request,
                    query_service_client=query_service_client,
                    validate_access=validate_access,
                    background_tasks=background_tasks,
                    save_history=save_history,
                    cache=cache,
                    existing=existing_nodes_map.get(node_spec.fully_qualified_name)
                    is not None,
                ),
            )
        deployed_level_nodes = await asyncio.gather(*tasks)
        deployed_nodes.extend(deployed_level_nodes)

    # Deploy dimension links
    link_tasks = []
    for node_spec in deployment.nodes:
        if isinstance(node_spec, LinkableNodeSpec):

            async def deploy_links_for_node(node_spec):
                # run links sequentially for this node to avoid race conditions
                linkable_node_spec = cast(LinkableNodeSpec, node_spec)
                results = 0
                for link in linkable_node_spec.dimension_links:
                    await deploy_dimension_link_from_spec(
                        node_spec=linkable_node_spec,
                        link_spec=link,
                        request=request,
                        current_user=current_user,
                        save_history=save_history,
                    )
                    results += 1
                return results

            link_tasks.append(deploy_links_for_node(node_spec=node_spec))

    deployed_links = await asyncio.gather(*link_tasks)

    # deploy cubes
    cube_tasks = []
    for cube_spec in [node for node in deployment.nodes if isinstance(node, CubeSpec)]:
        cube_tasks.append(
            deploy_node_from_spec(
                node_spec=cube_spec,
                current_user=current_user,
                request=request,
                query_service_client=query_service_client,
                validate_access=validate_access,
                background_tasks=background_tasks,
                save_history=save_history,
                cache=cache,
                existing=existing_nodes_map.get(node_spec.fully_qualified_name)
                is not None,
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
