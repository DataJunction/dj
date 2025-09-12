import asyncio
import logging
import time
from typing import Awaitable, Callable, Coroutine, cast
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from fastapi import Request, BackgroundTasks
from datajunction_server.database.user import User
from datajunction_server.database import Node
from datajunction_server.models import access
from sqlalchemy.ext.asyncio import AsyncSession
from datajunction_server.models.deployment import (
    CubeSpec,
    DeploymentResult,
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
    DJException,
    DJGraphCycleException,
    DJInvalidDeploymentConfig,
)
from datajunction_server.sql.parsing.backends.antlr4 import parse, ast
from datajunction_server.utils import SEPARATOR, get_settings, session_context
from datajunction_server.internal.caching.interface import Cache
from datajunction_server.internal.namespaces import create_namespace

settings = get_settings()
logger = logging.getLogger(__name__)


async def safe_task(
    name: str,
    deploy_type: DeploymentResult.Type,
    coroutine: Awaitable[DeploymentResult],
    semaphore: asyncio.Semaphore,
    timeout: int = 20,
) -> DeploymentResult:
    try:
        async with semaphore:
            return await asyncio.wait_for(coroutine, timeout)
    except asyncio.TimeoutError:
        return DeploymentResult(
            deploy_type=deploy_type,
            name=name,
            status=DeploymentResult.Status.FAILED,
            message=f"Task timed out after {timeout}s",
        )
    except Exception as exc:
        logger.exception("Error deploying %s %s: %s", deploy_type, name, exc)
        return DeploymentResult(
            deploy_type=deploy_type,
            name=name,
            status=DeploymentResult.Status.FAILED,
            message=str(exc),
        )


async def find_existing_nodes(
    session: AsyncSession,
    nodes: list[NodeSpec],
) -> dict[str, NodeSpec]:
    non_cubes = [n.rendered_name for n in nodes if n.node_type != NodeType.CUBE]
    cubes = [n.rendered_name for n in nodes if n.node_type == NodeType.CUBE]
    all_nodes = await Node.get_by_names(session, non_cubes) + [
        await Node.get_cube_by_name(session, cube) for cube in cubes
    ]
    return {node.name: await node.to_spec(session) for node in all_nodes if node}


async def deploy(
    deployment: DeploymentSpec,
    current_username: str,
    request: Request,
    query_service_client: QueryServiceClient,
    save_history: Callable,
    validate_access: access.ValidateAccessFn,
    background_tasks: BackgroundTasks,
    cache: Cache,
) -> list[DeploymentResult]:
    """
    Deploy to a namespace based on the given deployment specification.
    Profiled version that logs time elapsed for each step.
    """
    start_total = time.perf_counter()
    logger.info(
        "Starting deployment of %d nodes in namespace %s",
        len(deployment.nodes),
        deployment.namespace,
    )

    deployed_results: list[DeploymentResult] = []

    async with session_context(request) as session:
        existing = await find_existing_nodes(session, deployment.nodes)
        to_deploy, to_skip = filter_nodes_to_deploy(deployment.nodes, existing)
    deployed_results.extend(to_skip)
    if not to_deploy:
        logger.info(
            "No changes detected, skipping deployment. Total elapsed: %.3fs",
            time.perf_counter() - start_total,
        )
        return deployed_results

    logger.info(
        "Found %d nodes to deploy; skipped %d nodes",
        len(to_deploy),
        len(to_skip),
    )

    async with session_context(request) as session:
        current_user = cast(User, await User.get_by_username(session, current_username))
        await create_deployment_namespaces(
            deployment,
            session,
            current_user,
            save_history,
        )

    node_graph = extract_node_graph(
        [node for node in to_deploy if not isinstance(node, CubeSpec)],
    )

    # Check for any dependencies that are not in the deployment: they should pre-exist
    # in the system already or the deployment will fail
    external_deps = set()
    async with session_context(request) as session:
        external_deps = await check_external_deps(session, node_graph, deployment.nodes)

    logger.info("Starting deployment of %d nodes", len(to_deploy))
    deployed_nodes = await deploy_nodes_in_levels(
        nodes_to_deploy=to_deploy,
        node_graph=node_graph,
        current_username=current_username,
        request=request,
        query_service_client=query_service_client,
        validate_access=validate_access,
        background_tasks=background_tasks,
        save_history=save_history,
        cache=cache,
        existing_nodes_map=existing,
        external_deps=external_deps,
    )
    logger.info("Finished deploying %d non-cube nodes", len(deployed_nodes))
    deployed_results.extend(deployed_nodes)

    logger.info("Starting deployment of dimension links")
    deployed_links = await deploy_links_for_nodes(
        deployment_nodes=to_deploy,
        request=request,
        current_username=current_username,
        save_history=save_history,
    )
    logger.info("Finished deploying %d dimension links", len(deployed_links))
    deployed_results.extend(deployed_links)

    cubes_to_deploy = [node for node in to_deploy if isinstance(node, CubeSpec)]
    logger.info("Starting deployment of %d cubes", len(cubes_to_deploy))
    deployed_cubes = await deploy_cubes(
        to_deploy=cubes_to_deploy,
        current_username=current_username,
        request=request,
        query_service_client=query_service_client,
        validate_access=validate_access,
        background_tasks=background_tasks,
        save_history=save_history,
        cache=cache,
        existing_nodes_map=existing,
    )
    logger.info("Finished deploying %d cubes", len(deployed_cubes))
    deployed_results.extend(deployed_cubes)
    logger.info("Finished deploying namespace %s", deployment.namespace)
    return deployed_results


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


def extract_node_graph(nodes: list[NodeSpec]) -> dict[str, list[str]]:
    """
    Extract the node graph from a list of nodes
    """
    logger.info("Extracting node graph for %d nodes", len(nodes))
    dependencies_map: dict[str, list[str]] = {}
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(_find_upstreams_for_node, node) for node in nodes]
        for future in as_completed(futures):
            name, deps = future.result()
            dependencies_map[name] = deps

    logger.info("Extracted node graph with %d entries", len(dependencies_map))
    return dependencies_map


def _find_upstreams_for_node(node: NodeSpec) -> tuple[str, list[str]]:
    """
    Find the upstream dependencies for a given node.
    """
    if (
        isinstance(node, (TransformSpec, DimensionSpec, MetricSpec))
        and node.rendered_query
    ):
        query_ast = parse(node.rendered_query)
        cte_names = [cte.alias_or_name.identifier() for cte in query_ast.ctes]
        tables = {
            t.name.identifier()
            for t in query_ast.find_all(ast.Table)
            if t.name.identifier() not in cte_names
        }
        return node.rendered_name, sorted(list(tables))
    if isinstance(node, CubeSpec):
        dimension_nodes = [dim.rsplit(".", 1)[0] for dim in node.rendered_dimensions]
        return node.rendered_name, node.rendered_metrics + dimension_nodes
    return node.rendered_name, []


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


def filter_nodes_to_deploy(
    node_specs: list[NodeSpec],
    existing_nodes_map: dict[str, NodeSpec],
):
    to_create: list[NodeSpec] = []
    to_update: list[NodeSpec] = []
    to_skip: list[DeploymentResult] = []
    for node_spec in node_specs:
        existing_spec = existing_nodes_map.get(node_spec.rendered_name)
        if not existing_spec:
            to_create.append(node_spec)
        elif node_spec != existing_spec:
            to_update.append(node_spec)
        else:
            to_skip.append(
                DeploymentResult(
                    name=node_spec.rendered_name,
                    deploy_type=DeploymentResult.Type.NODE,
                    status=DeploymentResult.Status.NOOP,
                    message=f"Node {node_spec.rendered_name} is unchanged.",
                ),
            )

    logger.info(
        "Creating %d new nodes: %s",
        len(to_create),
        [node.rendered_name for node in to_create],
    )
    logger.info(
        "Updating %d existing nodes: %s",
        len(to_update),
        [node.rendered_name for node in to_update],
    )
    logger.info(
        "Skipping %d nodes as they are unchanged: %s",
        len(to_skip),
        [result.name for result in to_skip],
    )
    return to_create + to_update, to_skip


async def check_external_deps(
    session: AsyncSession,
    node_graph: dict[str, list[str]],
    deployment_nodes: list[NodeSpec],
) -> set[str]:
    """
    Find any dependencies that are not in the deployment but are already in the system.
    If any dependencies are not in the deployment and not in the system, raise an error.
    """
    dimension_link_deps = [
        link.rendered_dimension_node
        for node in deployment_nodes
        if isinstance(node, LinkableNodeSpec) and node.dimension_links
        for link in node.dimension_links
    ]

    deps_not_in_deployment = {
        dep
        for deps in list(node_graph.values())
        for dep in deps
        if dep not in node_graph
    }.union({dep for dep in dimension_link_deps if dep not in node_graph})
    if deps_not_in_deployment:
        logger.warning(
            "The following dependencies are not defined in the deployment: %s. "
            "They must pre-exist in the system before this deployment can succeed.",
            deps_not_in_deployment,
        )
        external_node_deps = await Node.get_by_names(
            session,
            list(deps_not_in_deployment),
        )
        if len(external_node_deps) != len(deps_not_in_deployment):
            missing_nodes = sorted(
                set(deps_not_in_deployment)
                - {node.name for node in external_node_deps},
            )
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
    return deps_not_in_deployment


async def deploy_nodes_in_levels(
    nodes_to_deploy: list[NodeSpec],
    node_graph: dict[str, list[str]],
    current_username: str,
    request: Request,
    query_service_client: QueryServiceClient,
    validate_access: Callable,
    background_tasks: BackgroundTasks,
    save_history: Callable,
    cache: Cache,
    existing_nodes_map: dict[str, NodeSpec],
    external_deps: set[str],
) -> list[DeploymentResult]:
    deployed_results = []
    levels = topological_levels(node_graph, ascending=False)
    logger.info(
        "Deploying nodes in topological order with %d levels",
        len(levels),
    )
    name_to_node = {node.rendered_name: node for node in nodes_to_deploy}
    for level in levels:
        logger.info("Deploying level with %d nodes: %s", len(level), level)

        node_tasks = []
        for node_name in level:
            if node_name in external_deps or node_name not in name_to_node:
                continue
            node_spec = name_to_node[node_name]
            node_tasks.append(
                deploy_node_from_spec(
                    node_spec=node_spec,
                    current_username=current_username,
                    request=request,
                    query_service_client=query_service_client,
                    validate_access=validate_access,
                    background_tasks=background_tasks,
                    save_history=save_history,
                    cache=cache,
                    existing=existing_nodes_map.get(node_spec.rendered_name)
                    is not None,
                ),
            )

        results_for_level = await run_tasks_with_semaphore(
            deploy_type=DeploymentResult.Type.NODE,
            task_names=level,
            task_coroutines=node_tasks,
        )
        deployed_results.extend(results_for_level)
    return deployed_results


async def deploy_links_for_nodes(
    deployment_nodes: list[NodeSpec],
    request: Request,
    current_username: str,
    save_history: Callable,
) -> list[DeploymentResult]:
    async def _deploy_links_for_node(node_spec: LinkableNodeSpec):
        # run links sequentially for this node to avoid race conditions
        linkable_node_spec = cast(LinkableNodeSpec, node_spec)
        results = []
        for link in linkable_node_spec.dimension_links or []:
            link_name = f"{node_spec.rendered_name} -> {link.rendered_dimension_node}"
            try:
                await deploy_dimension_link_from_spec(
                    node_spec=linkable_node_spec,
                    link_spec=link,
                    request=request,
                    current_username=current_username,
                    save_history=save_history,
                )
                results.append(
                    DeploymentResult(
                        deploy_type=DeploymentResult.Type.LINK,
                        name=link_name,
                        status=DeploymentResult.Status.SUCCESS,
                    ),
                )
            except Exception as exc:
                results.append(
                    DeploymentResult(
                        deploy_type=DeploymentResult.Type.LINK,
                        name=link_name,
                        status=DeploymentResult.Status.FAILED,
                        message=str(exc),
                    ),
                )
        return results

    # Deploy dimension links
    link_tasks = []
    for node_spec in deployment_nodes:
        if isinstance(node_spec, LinkableNodeSpec):
            link_tasks.append(_deploy_links_for_node(node_spec=node_spec))

    link_results = await run_tasks_with_semaphore(
        deploy_type=DeploymentResult.Type.LINK,
        task_names=[spec.name for spec in deployment_nodes],
        task_coroutines=link_tasks,
    )
    return [result for node_links in link_results for result in node_links]


async def deploy_cubes(
    to_deploy: list[NodeSpec],
    current_username: str,
    request: Request,
    query_service_client: QueryServiceClient,
    validate_access: Callable,
    save_history: Callable,
    cache: Cache,
    background_tasks: BackgroundTasks,
    existing_nodes_map: dict[str, NodeSpec],
) -> list[DeploymentResult]:
    cube_tasks = []
    cube_specs = [node for node in to_deploy if isinstance(node, CubeSpec)]
    for cube_spec in cube_specs:
        cube_tasks.append(
            deploy_node_from_spec(
                node_spec=cube_spec,
                current_username=current_username,
                request=request,
                query_service_client=query_service_client,
                validate_access=validate_access,
                background_tasks=background_tasks,
                save_history=save_history,
                cache=cache,
                existing=existing_nodes_map.get(cube_spec.rendered_name) is not None,
            ),
        )
    return await run_tasks_with_semaphore(
        deploy_type=DeploymentResult.Type.NODE,
        task_names=[spec.name for spec in cube_specs],
        task_coroutines=cube_tasks,
    )


async def run_tasks_with_semaphore(
    deploy_type: DeploymentResult.Type,
    task_names: list[str],
    task_coroutines: list[Coroutine],
) -> list[DeploymentResult]:
    semaphore = asyncio.Semaphore(settings.effective_writer_concurrency)
    logger.info(
        "Running %d tasks with concurrency %d",
        len(task_coroutines),
        settings.effective_writer_concurrency,
    )
    return await asyncio.gather(
        *[
            safe_task(
                name=name,
                deploy_type=deploy_type,
                coroutine=task,
                semaphore=semaphore,
            )
            for name, task in zip(task_names, task_coroutines)
        ],
        return_exceptions=True,
    )


async def deploy_node_from_spec(
    node_spec: NodeSpec,
    current_username: str,
    request: Request,
    query_service_client: QueryServiceClient,
    validate_access: access.ValidateAccessFn,
    background_tasks: BackgroundTasks = None,
    *,
    save_history: Callable,
    cache: Cache,
    existing: bool = False,
) -> DeploymentResult:
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

    deploy_fn = node_deployers.get(node_spec.node_type)
    if not deploy_fn:
        raise DJInvalidDeploymentConfig(f"Unknown node type: {node_spec.node_type}")
    try:
        node = await deploy_fn(
            node_spec=node_spec,
            current_username=current_username,
            request=request,
            query_service_client=query_service_client,
            validate_access=validate_access,
            background_tasks=background_tasks,
            save_history=save_history,
            cache=cache,
            existing=existing,
        )
    except DJException as exc:
        return DeploymentResult(
            deploy_type=DeploymentResult.Type.NODE,
            name=node_spec.rendered_name,
            status=DeploymentResult.Status.FAILED,
            message=str(exc),
        )

    return DeploymentResult(
        deploy_type=DeploymentResult.Type.NODE,
        name=node_spec.rendered_name,
        status=DeploymentResult.Status.SUCCESS
        if isinstance(node, Node)
        else DeploymentResult.Status.FAILED,
        message="",
    )


async def deploy_source_node_from_spec(
    node_spec: SourceSpec,
    current_username: str,
    request: Request,
    query_service_client: QueryServiceClient,
    validate_access: access.ValidateAccessFn,
    background_tasks: BackgroundTasks = None,
    *,
    save_history: Callable,
    existing: bool = False,
    **kwargs,
) -> NodeOutput:
    """
    Deploy a source node from its spec.
    """
    # TODO Handle case where there are no columns on source_spec.columns and it's registering a table

    catalog, schema, table = node_spec.table.split(".")

    async with session_context(request) as session:
        current_user = cast(User, await User.get_by_username(session, current_username))
        if existing:
            current_user = cast(
                User,
                await User.get_by_username(session, current_username),
            )
            return await refresh_source(  # type: ignore
                name=node_spec.rendered_name,
                session=session,
                request=request,
                query_service_client=query_service_client,
                current_user=current_user,
                save_history=save_history,
            )
        node_output = await create_a_source_node(
            data=CreateSourceNode(
                name=node_spec.rendered_name,
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
    current_username: str,
    request: Request,
    query_service_client: QueryServiceClient,
    validate_access: access.ValidateAccessFn,
    background_tasks: BackgroundTasks = None,
    *,
    save_history: Callable,
    cache: Cache,
    existing: bool = False,
) -> Node:
    """
    Deploy a transform or dimension node from its spec.
    """
    async with session_context(request) as session:
        current_user = cast(User, await User.get_by_username(session, current_username))
        if existing:
            request_headers = dict(request.headers)
            await update_any_node(
                node_spec.rendered_name,
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
                node_spec.rendered_name,
                options=NodeOutput.load_options(),
                raise_if_not_exists=True,
            )

        created_node = await create_a_node(
            data=CreateNode(
                name=node_spec.rendered_name,
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
    current_username: str,
    request: Request,
    query_service_client: QueryServiceClient,
    validate_access: access.ValidateAccessFn,
    background_tasks: BackgroundTasks = None,
    *,
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
    async with session_context(request) as session:
        current_user = cast(User, await User.get_by_username(session, current_username))
        if existing:
            request_headers = dict(request.headers)
            await update_any_node(
                node_spec.rendered_name,
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
                node_spec.rendered_name,
                options=NodeOutput.load_options(),
                raise_if_not_exists=True,
            )

        created_node = await create_a_node(
            data=CreateNode(
                name=node_spec.rendered_name,
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
    current_username: str,
    request: Request,
    query_service_client: QueryServiceClient,
    validate_access: access.ValidateAccessFn,
    background_tasks: BackgroundTasks = None,
    *,
    save_history: Callable,
    cache: Cache,
    existing: bool = False,
    **kwargs,
) -> Node:
    """
    Deploy a cube node from its spec.
    """
    async with session_context(request) as session:
        current_user = cast(User, await User.get_by_username(session, current_username))
        if existing:
            logger.info("Updating cube node %s", node_spec.rendered_name)
            request_headers = dict(request.headers)
            await update_any_node(
                node_spec.rendered_name,
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
                node_spec.rendered_name,
                options=NodeOutput.load_options(),
                raise_if_not_exists=True,
            )
        logger.info("Creating cube node %s", node_spec.rendered_name)
        return await create_a_cube(
            data=CreateCubeNode(
                name=node_spec.rendered_name,
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
    current_username: str,
    save_history: Callable,
):
    async with session_context(request) as session:
        current_user = cast(User, await User.get_by_username(session, current_username))
        if link_spec.type == LinkType.JOIN:
            join_link = cast(DimensionJoinLinkSpec, link_spec)
            if join_link.node_column:
                return await upsert_simple_dimension_link(
                    session,
                    node_spec.rendered_name,
                    join_link.rendered_dimension_node,
                    join_link.node_column,
                    None,
                    current_user,
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
                node_spec.rendered_name,
                link_input,
                current_user,
                save_history,
            )
        reference_link = cast(DimensionReferenceLinkSpec, link_spec)
        return await upsert_reference_dimension_link(
            session=session,
            node_name=node_spec.rendered_name,
            node_column=reference_link.node_column,
            dimension_node=reference_link.rendered_dimension_node,
            dimension_column=reference_link.dimension_attribute,
            role=reference_link.role,
            current_user=current_user,
            save_history=save_history,
        )


def render_prefixes(parameterized_string: str, prefix: str):
    """
    Replaces ${prefix} in a string
    """
    return parameterized_string.replace("${prefix}", f"{prefix}.")
