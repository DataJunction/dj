import asyncio
import logging
from typing import Awaitable, Callable, Coroutine, cast
from fastapi import Request, BackgroundTasks
from datajunction_server.internal.caching.interface import Cache
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.database.user import User
from datajunction_server.database import Node
from datajunction_server.models import access
from sqlalchemy.ext.asyncio import AsyncSession
from datajunction_server.api.tags import get_tags_by_name
from datajunction_server.models.base import labelize
from datajunction_server.internal.deployment.orchestrator import DeploymentOrchestrator
from datajunction_server.database.partition import Partition
from datajunction_server.models.attribute import AttributeTypeIdentifier
from datajunction_server.models.deployment import (
    CubeSpec,
    DeploymentResult,
    DeploymentSpec,
    NodeSpec,
)
from datajunction_server.models.node import (
    CreateCubeNode,
    NodeOutput,
    NodeType,
    UpdateNode,
)
from datajunction_server.internal.nodes import (
    create_a_cube,
    set_node_column_attributes,
    update_any_node,
)
from datajunction_server.internal.deployment.utils import DeploymentContext
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.errors import (
    DJException,
    DJInvalidDeploymentConfig,
)
from datajunction_server.utils import get_settings, session_context
from datajunction_server.internal.caching.interface import Cache

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
            operation=DeploymentResult.Operation.UNKNOWN,
            message=f"Task timed out after {timeout}s",
        )
    except Exception as exc:
        logger.exception("Error deploying %s %s: %s", deploy_type, name, exc)
        return DeploymentResult(
            deploy_type=deploy_type,
            name=name,
            status=DeploymentResult.Status.FAILED,
            operation=DeploymentResult.Operation.UNKNOWN,
            message=str(exc),
        )


async def deploy(
    session: AsyncSession,
    deployment_id: str,
    deployment: DeploymentSpec,
    context: DeploymentContext,
) -> list[DeploymentResult]:
    """
    Deploy to a namespace based on the given deployment specification.
    """
    orchestrator = DeploymentOrchestrator(
        deployment_id=deployment_id,
        deployment_spec=deployment,
        session=session,
        context=context,
    )
    return await orchestrator.execute()


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
                existing=existing_nodes_map.get(cube_spec.rendered_name),
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
    max_concurrency = min(settings.effective_writer_concurrency, len(task_coroutines))
    semaphore = asyncio.Semaphore(max_concurrency)
    logger.info(
        "Running %d tasks with concurrency %d",
        len(task_coroutines),
        max_concurrency,
    )
    if not task_coroutines:
        return []
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


async def deploy_node_tags(node_name: str, tag_names: list[str]) -> None:
    async with session_context() as session:
        node = await Node.get_by_name(session=session, name=node_name)
        tags = await get_tags_by_name(session, names=tag_names or [])
        node.tags = tags  # type: ignore
        session.add(node)
        await session.commit()
        await session.refresh(node)


async def deploy_column_properties(
    node_name: str,
    node_spec: NodeSpec,
    current_username: str,
    save_history: Callable,
) -> set[str]:
    changed_columns = set()
    async with session_context() as session:
        node = await Node.get_by_name(session=session, name=node_name)
        current_user = cast(User, await User.get_by_username(session, current_username))
        desired_column_state = {col.name: col for col in node_spec.columns or []}
        for col in node.current.columns:  # type: ignore
            if desired_col := desired_column_state.get(col.name):
                # Set column display name and description
                if (
                    col.display_name != desired_col.display_name
                    and desired_col.display_name is not None
                ):
                    col.display_name = desired_col.display_name
                    changed_columns.add(col.name)
                if col.description != desired_col.description:
                    col.description = desired_col.description
                    changed_columns.add(col.name)

                # Set column partition
                if desired_col.partition is None and col.partition:
                    await session.delete(col.partition)
                    changed_columns.add(col.name)
                elif col.partition is None and desired_col.partition:
                    partition = Partition(
                        column_id=col.id,
                        type_=desired_col.partition.type,
                        format=desired_col.partition.format,
                        granularity=desired_col.partition.granularity,
                    )
                    session.add(partition)
                    col.partition = partition
                    changed_columns.add(col.name)
                elif (
                    desired_col.partition
                    and col.partition
                    and desired_col.partition != col.partition.to_spec()
                ):
                    col.partition.type_ = desired_col.partition.type
                    col.partition.format = desired_col.partition.format
                    col.partition.granularity = desired_col.partition.granularity
                    session.add(col)
                    changed_columns.add(col.name)

                # Set column attributes
                if set(desired_col.attributes) != set(col.attribute_names()):
                    await set_node_column_attributes(
                        session=session,
                        node=node,  # type: ignore
                        column_name=col.name,
                        attributes=[
                            AttributeTypeIdentifier(name=attr)
                            for attr in desired_col.attributes
                        ]
                        + [
                            AttributeTypeIdentifier(name=attr)
                            for attr in col.attribute_names()
                            if attr == "primary_key"
                        ],
                        current_user=current_user,
                        save_history=save_history,
                    )
                    changed_columns.add(col.name)
            else:
                # If the column is not explicitly defined, reset it to default
                col.display_name = labelize(col.name)
                col.description = ""
                if col.partition:
                    await session.delete(col.partition)
                col.attributes = [
                    attr
                    for attr in col.attributes
                    if attr.attribute_type.name == "primary_key"
                ]

            session.add(col)
        await session.commit()
    return changed_columns


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
    existing: NodeSpec | None = None,
) -> DeploymentResult:
    """
    Deploy a node from its specification.
    """
    node_deployers: dict[NodeType, Callable[..., Awaitable[Node]]] = {
        NodeType.CUBE: deploy_cube_node_from_spec,
    }

    deploy_fn = node_deployers.get(node_spec.node_type)
    operation = (
        DeploymentResult.Operation.CREATE
        if not existing
        else DeploymentResult.Operation.UPDATE
    )
    changelog = []
    if not deploy_fn:  # pragma: no cover
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
        changed_fields = existing.diff(node_spec) if existing else []
        changelog.append(
            f"{operation.capitalize()}d {node_spec.node_type} ({node.current_version})",
        )
        changelog.append(
            ("└─ Updated " + ", ".join(changed_fields)),
        ) if changed_fields else ""

        if set(node_spec.tags) != set(  # pragma: no cover
            [tag.name for tag in node.tags],
        ):
            await deploy_node_tags(node_name=node.name, tag_names=node_spec.tags)
            tags_list = ", ".join([f"`{tag}`" for tag in node_spec.tags])
            changelog.append(f"└─ Set tags to {tags_list}.")
        if node.type in (  # pragma: no cover
            NodeType.SOURCE,
            NodeType.TRANSFORM,
            NodeType.DIMENSION,
            NodeType.CUBE,
        ):
            changed_columns = await deploy_column_properties(
                node_name=node.name,
                node_spec=node_spec,
                current_username=current_username,
                save_history=save_history,
            )
            if changed_columns and operation == DeploymentResult.Operation.UPDATE:
                changelog.append(  # pragma: no cover
                    f"└─ Set properties for {len(changed_columns)} columns",
                )
    except DJException as exc:
        return DeploymentResult(
            deploy_type=DeploymentResult.Type.NODE,
            name=node_spec.rendered_name,
            status=DeploymentResult.Status.FAILED,
            message="\n".join(changelog + [str(exc)]),
            operation=operation,
        )

    return DeploymentResult(
        deploy_type=DeploymentResult.Type.NODE,
        name=node_spec.rendered_name,
        status=DeploymentResult.Status.SUCCESS
        if isinstance(node, Node)
        else DeploymentResult.Status.FAILED,
        operation=operation,
        message="\n".join(changelog),
    )


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
        else:
            logger.info("Creating cube node %s", node_spec.rendered_name)
            await create_a_cube(
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

        return await Node.get_by_name(  # type: ignore
            session,
            node_spec.rendered_name,
            options=NodeOutput.load_options(),
            raise_if_not_exists=True,
        )
