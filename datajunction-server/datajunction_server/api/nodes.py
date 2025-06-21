"""
Node related APIs.
"""

import logging
import os
from http import HTTPStatus
from typing import Callable, List, Optional

from fastapi import BackgroundTasks, Depends, Query, Response
from fastapi.responses import JSONResponse
from fastapi_cache.decorator import cache
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload
from sqlalchemy.sql.operators import is_
from starlette.requests import Request

from datajunction_server.api.helpers import (
    get_catalog_by_name,
    get_column,
    get_node_by_name,
    get_node_namespace,
    raise_if_node_exists,
    get_save_history,
)
from datajunction_server.api.namespaces import create_node_namespace
from datajunction_server.api.tags import get_tags_by_name
from datajunction_server.database import DimensionLink
from datajunction_server.database.attributetype import ColumnAttribute
from datajunction_server.database.column import Column
from datajunction_server.database.history import History
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.partition import Partition
from datajunction_server.database.user import User
from datajunction_server.errors import (
    DJActionNotAllowedException,
    DJAlreadyExistsException,
    DJConfigurationException,
    DJDoesNotExistException,
    DJInvalidInputException,
    ErrorCode,
)
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.access.authorization import (
    validate_access,
    validate_access_requests,
)
from datajunction_server.internal.history import ActivityType, EntityType
from datajunction_server.internal.nodes import (
    activate_node,
    copy_to_new_node,
    create_cube_node_revision,
    create_node_from_inactive,
    create_node_revision,
    deactivate_node,
    get_column_level_lineage,
    get_node_column,
    hard_delete_node,
    remove_dimension_link,
    revalidate_node,
    save_column_level_lineage,
    save_node,
    set_node_column_attributes,
    update_any_node,
    upsert_complex_dimension_link,
)
from datajunction_server.internal.validation import validate_node_data
from datajunction_server.models import access
from datajunction_server.models.attribute import (
    AttributeTypeIdentifier,
    ColumnAttributes,
)
from datajunction_server.models.dimensionlink import (
    JoinLinkInput,
    JoinType,
    LinkDimensionIdentifier,
)
from datajunction_server.models.node import (
    ColumnOutput,
    CreateCubeNode,
    CreateNode,
    CreateSourceNode,
    DAGNodeOutput,
    DimensionAttributeOutput,
    LineageColumn,
    NodeIndexItem,
    NodeMode,
    NodeOutput,
    NodeRevisionBase,
    NodeRevisionOutput,
    NodeStatus,
    NodeStatusDetails,
    NodeValidation,
    NodeValidationError,
    UpdateNode,
)
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.partition import (
    Granularity,
    PartitionInput,
    PartitionType,
)
from datajunction_server.models.query import QueryCreate
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.sql.dag import (
    _node_output_options,
    get_dimensions,
    get_dimension_attributes,
    get_downstream_nodes,
    get_filter_only_dimensions,
    get_upstream_nodes,
)
from datajunction_server.sql.parsing.backends.antlr4 import parse, parse_rule
from datajunction_server.utils import (
    Version,
    get_and_update_current_user,
    get_current_user,
    get_namespace_from_name,
    get_query_service_client,
    get_session,
    get_settings,
)

_logger = logging.getLogger(__name__)
settings = get_settings()
router = SecureAPIRouter(tags=["nodes"])


@router.post("/nodes/validate/", response_model=NodeValidation)
async def validate_node(
    data: NodeRevisionBase,
    response: Response,
    session: AsyncSession = Depends(get_session),
) -> NodeValidation:
    """
    Determines whether the provided node is valid and returns metadata from node validation.
    """

    if data.type == NodeType.SOURCE:
        raise DJInvalidInputException(message="Source nodes cannot be validated")

    node_validator = await validate_node_data(data, session)
    if node_validator.errors:
        response.status_code = HTTPStatus.UNPROCESSABLE_ENTITY
    else:
        response.status_code = HTTPStatus.OK

    return NodeValidation(
        message=f"Node `{data.name}` is {node_validator.status}.",
        status=node_validator.status,
        columns=node_validator.columns,
        dependencies=set(node_validator.dependencies_map.keys()),
        errors=node_validator.errors,
        missing_parents=list(node_validator.missing_parents_map.keys()),
    )


@router.post("/nodes/{name}/validate/", response_model=NodeStatusDetails)
async def revalidate(
    name: str,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_and_update_current_user),
    save_history: Callable = Depends(get_save_history),
    *,
    background_tasks: BackgroundTasks,
) -> NodeStatusDetails:
    """
    Revalidate a single existing node and update its status appropriately
    """
    node_validator = await revalidate_node(
        name=name,
        session=session,
        current_user=current_user,
        background_tasks=background_tasks,
        save_history=save_history,
    )

    return NodeStatusDetails(
        status=node_validator.status,
        errors=[
            *[
                NodeValidationError(
                    type=ErrorCode.TYPE_INFERENCE.name,
                    message=failure,
                )
                for failure in node_validator.type_inference_failures
            ],
            *[
                NodeValidationError(
                    type=(
                        ErrorCode.TYPE_INFERENCE.name
                        if "Unable to infer type" in error.message
                        else error.code.name
                    ),
                    message=error.message,
                )
                for error in node_validator.errors
            ],
        ],
    )


@router.post(
    "/nodes/{node_name}/columns/{column_name}/attributes/",
    response_model=List[ColumnOutput],
    status_code=201,
)
async def set_column_attributes(
    node_name: str,
    column_name: str,
    attributes: List[AttributeTypeIdentifier],
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_and_update_current_user),
    save_history: Callable = Depends(get_save_history),
) -> List[ColumnOutput]:
    """
    Set column attributes for the node.
    """
    node = await Node.get_by_name(
        session,
        node_name,
        options=[
            joinedload(Node.current).options(*NodeRevision.default_load_options()),
        ],
    )
    columns = await set_node_column_attributes(
        session,
        node,  # type: ignore
        column_name,
        attributes,
        current_user=current_user,
        save_history=save_history,
    )
    return columns  # type: ignore


@router.get("/nodes/", response_model=List[str])
async def list_nodes(
    node_type: Optional[NodeType] = None,
    prefix: Optional[str] = None,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    validate_access: access.ValidateAccessFn = Depends(
        validate_access,
    ),
) -> List[str]:
    """
    List the available nodes.
    """
    nodes = await Node.find(session, prefix, node_type)  # type: ignore
    return [
        approval.access_object.name
        for approval in validate_access_requests(
            validate_access,
            current_user,
            [
                access.ResourceRequest(
                    verb=access.ResourceRequestVerb.BROWSE,
                    access_object=access.Resource.from_node(node),
                )
                for node in nodes
            ],
        )
    ]


@router.get("/nodes/details/", response_model=List[NodeIndexItem])
@cache(expire=settings.index_cache_expire)
async def list_all_nodes_with_details(
    node_type: Optional[NodeType] = None,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    validate_access: access.ValidateAccessFn = Depends(
        validate_access,
    ),
) -> List[NodeIndexItem]:
    """
    List the available nodes.
    """
    nodes_query = (
        select(
            NodeRevision.name,
            NodeRevision.display_name,
            NodeRevision.description,
            NodeRevision.type,
        )
        .where(
            Node.current_version == NodeRevision.version,
            Node.name == NodeRevision.name,
            Node.type == node_type if node_type else True,
            is_(Node.deactivated_at, None),
        )
        .order_by(NodeRevision.updated_at.desc())
        .limit(settings.node_list_max)
    )  # Very high limit as a safeguard
    results = [
        NodeIndexItem(name=row[0], display_name=row[1], description=row[2], type=row[3])
        for row in (await session.execute(nodes_query)).all()
    ]
    if len(results) == settings.node_list_max:  # pragma: no cover
        _logger.warning(
            "%s limit reached when returning all nodes, all nodes may not be captured in results",
            settings.node_list_max,
        )
    approvals = [
        approval.access_object.name
        for approval in validate_access_requests(
            validate_access,
            current_user,
            [
                access.ResourceRequest(
                    verb=access.ResourceRequestVerb.BROWSE,
                    access_object=access.Resource(
                        name=row.name,
                        resource_type=access.ResourceType.NODE,
                        owner="",
                    ),
                )
                for row in results
            ],
        )
    ]
    return [row for row in results if row.name in approvals]


@router.get("/nodes/{name}/", response_model=NodeOutput)
async def get_node(
    name: str,
    *,
    session: AsyncSession = Depends(get_session),
) -> NodeOutput:
    """
    Show the active version of the specified node.
    """
    node = await Node.get_by_name(
        session,
        name,
        options=NodeOutput.load_options(),
        raise_if_not_exists=True,
    )
    return NodeOutput.from_orm(node)


@router.delete("/nodes/{name}/")
async def delete_node(
    name: str,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_and_update_current_user),
    save_history: Callable = Depends(get_save_history),
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    background_tasks: BackgroundTasks,
    request: Request,
):
    """
    Delete (aka deactivate) the specified node.
    """
    await deactivate_node(
        session=session,
        name=name,
        current_user=current_user,
        save_history=save_history,
        query_service_client=query_service_client,
        background_tasks=background_tasks,
        request_headers=dict(request.headers),
    )
    return JSONResponse(
        status_code=HTTPStatus.OK,
        content={"message": f"Node `{name}` has been successfully deleted."},
    )


@router.delete("/nodes/{name}/hard/", name="Hard Delete a DJ Node")
async def hard_delete(
    name: str,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_and_update_current_user),
    save_history: Callable = Depends(get_save_history),
) -> JSONResponse:
    """
    Hard delete a node, destroying all links and invalidating all downstream nodes.
    This should be used with caution, deactivating a node is preferred.
    """
    impact = await hard_delete_node(
        name=name,
        session=session,
        current_user=current_user,
        save_history=save_history,
    )
    return JSONResponse(
        status_code=HTTPStatus.OK,
        content={
            "message": f"The node `{name}` has been completely removed.",
            "impact": impact,
        },
    )


@router.post("/nodes/{name}/restore/")
async def restore_node(
    name: str,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_and_update_current_user),
    save_history: Callable = Depends(get_save_history),
):
    """
    Restore (aka re-activate) the specified node.
    """
    await activate_node(
        session=session,
        name=name,
        current_user=current_user,
        save_history=save_history,
    )
    return JSONResponse(
        status_code=HTTPStatus.OK,
        content={"message": f"Node `{name}` has been successfully restored."},
    )


@router.get("/nodes/{name}/revisions/", response_model=List[NodeRevisionOutput])
async def list_node_revisions(
    name: str,
    *,
    session: AsyncSession = Depends(get_session),
) -> List[NodeRevisionOutput]:
    """
    List all revisions for the node.
    """
    node = await Node.get_by_name(
        session,
        name,
        options=[
            joinedload(Node.revisions).options(*NodeRevision.default_load_options()),
        ],
        raise_if_not_exists=True,
    )
    return node.revisions  # type: ignore


@router.post("/nodes/source/", response_model=NodeOutput, name="Create A Source Node")
async def create_source(
    data: CreateSourceNode,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_and_update_current_user),
    request: Request,
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    validate_access: access.ValidateAccessFn = Depends(
        validate_access,
    ),
    background_tasks: BackgroundTasks,
    save_history: Callable = Depends(get_save_history),
) -> NodeOutput:
    """
    Create a source node. If columns are not provided, the source node's schema
    will be inferred using the configured query service.
    """
    request_headers = dict(request.headers)
    await raise_if_node_exists(session, data.name)

    # if the node previously existed and now is inactive
    if recreated_node := await create_node_from_inactive(
        new_node_type=NodeType.SOURCE,
        data=data,
        session=session,
        current_user=current_user,
        request_headers=request_headers,
        query_service_client=query_service_client,
        validate_access=validate_access,
        background_tasks=background_tasks,
        save_history=save_history,
    ):
        return recreated_node

    namespace = get_namespace_from_name(data.name)
    await get_node_namespace(
        session=session,
        namespace=namespace,
    )  # Will return 404 if namespace doesn't exist
    data.namespace = namespace

    node = Node(
        name=data.name,
        namespace=data.namespace,
        display_name=data.display_name or f"{data.catalog}.{data.schema_}.{data.table}",
        type=NodeType.SOURCE,
        current_version=0,
        created_by_id=current_user.id,
    )
    catalog = await get_catalog_by_name(session=session, name=data.catalog)

    columns = [
        Column(
            name=column_data.name,
            type=column_data.type,
            dimension=(
                await get_node_by_name(
                    session,
                    name=column_data.dimension,
                    node_type=NodeType.DIMENSION,
                    raise_if_not_exists=False,
                )
            ),
            order=idx,
        )
        for idx, column_data in enumerate(data.columns)
    ]
    node_revision = NodeRevision(
        name=data.name,
        display_name=data.display_name or f"{catalog.name}.{data.schema_}.{data.table}",
        description=data.description,
        type=NodeType.SOURCE,
        status=NodeStatus.VALID,
        catalog_id=catalog.id,
        schema_=data.schema_,
        table=data.table,
        columns=columns,
        parents=[],
        created_by_id=current_user.id,
        query=data.query,
    )
    node.display_name = node_revision.display_name

    # Point the node to the new node revision.
    await save_node(
        session,
        node_revision,
        node,
        data.mode,
        current_user=current_user,
        save_history=save_history,
    )
    node = await Node.get_by_name(  # type: ignore
        session,
        node.name,
        options=[
            joinedload(Node.current).options(*NodeRevision.default_load_options()),
            joinedload(Node.tags),
        ],
    )
    return node


@router.post(
    "/nodes/transform/",
    response_model=NodeOutput,
    status_code=201,
    name="Create A Transform Node",
)
@router.post(
    "/nodes/dimension/",
    response_model=NodeOutput,
    status_code=201,
    name="Create A Dimension Node",
)
@router.post(
    "/nodes/metric/",
    response_model=NodeOutput,
    status_code=201,
    name="Create A Metric Node",
)
async def create_node(
    data: CreateNode,
    request: Request,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_and_update_current_user),
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    background_tasks: BackgroundTasks,
    validate_access: access.ValidateAccessFn = Depends(
        validate_access,
    ),
    save_history: Callable = Depends(get_save_history),
) -> NodeOutput:
    """
    Create a node.
    """
    request_headers = dict(request.headers)
    node_type = NodeType(os.path.basename(os.path.normpath(request.url.path)))

    if node_type == NodeType.DIMENSION and not data.primary_key:
        raise DJInvalidInputException("Dimension nodes must define a primary key!")

    await raise_if_node_exists(session, data.name)

    # if the node previously existed and now is inactive
    if recreated_node := await create_node_from_inactive(
        new_node_type=node_type,
        data=data,
        session=session,
        current_user=current_user,
        request_headers=request_headers,
        query_service_client=query_service_client,
        background_tasks=background_tasks,
        validate_access=validate_access,
        save_history=save_history,
    ):
        return recreated_node  # pragma: no cover

    namespace = get_namespace_from_name(data.name)
    await get_node_namespace(
        session=session,
        namespace=namespace,
    )  # Will return 404 if namespace doesn't exist
    data.namespace = namespace

    node = Node(
        name=data.name,
        namespace=data.namespace,
        type=NodeType(node_type),
        current_version=0,
        created_by_id=current_user.id,
    )
    node_revision = await create_node_revision(data, node_type, session, current_user)
    await save_node(
        session,
        node_revision,
        node,
        data.mode,
        current_user=current_user,
        save_history=save_history,
    )
    background_tasks.add_task(
        save_column_level_lineage,
        session=session,
        node_revision=node_revision,
    )

    node = await Node.get_by_name(  # type: ignore
        session,
        node.name,
        options=[
            joinedload(Node.current).options(*NodeRevision.default_load_options()),
        ],
    )
    node_revision = node.current
    column_names = {col.name for col in node_revision.columns}
    if data.primary_key and any(
        key_column not in column_names for key_column in data.primary_key
    ):
        raise DJInvalidInputException(
            f"Some columns in the primary key [{','.join(data.primary_key)}] "
            f"were not found in the list of available columns for the node {node.name}.",
        )
    if data.primary_key:
        for key_column in data.primary_key:
            if key_column in column_names:  # pragma: no cover
                await set_node_column_attributes(
                    session,
                    node,
                    key_column,
                    [
                        AttributeTypeIdentifier(
                            name=ColumnAttributes.PRIMARY_KEY.value,
                            namespace="system",
                        ),
                    ],
                    current_user=current_user,
                    save_history=save_history,
                )
    node = await Node.get_by_name(  # type: ignore
        session,
        node.name,
        options=[
            joinedload(Node.current).options(*NodeRevision.default_load_options()),
            joinedload(Node.tags),
        ],
    )
    return node


@router.post(
    "/nodes/cube/",
    response_model=NodeOutput,
    status_code=201,
    name="Create A Cube",
)
async def create_cube(
    data: CreateCubeNode,
    *,
    session: AsyncSession = Depends(get_session),
    request: Request,
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    current_user: User = Depends(get_and_update_current_user),
    background_tasks: BackgroundTasks,
    validate_access: access.ValidateAccessFn = Depends(
        validate_access,
    ),
    save_history: Callable = Depends(get_save_history),
) -> NodeOutput:
    """
    Create a cube node.
    """
    request_headers = dict(request.headers)
    await raise_if_node_exists(session, data.name)

    # if the node previously existed and now is inactive
    if recreated_node := await create_node_from_inactive(
        new_node_type=NodeType.CUBE,
        data=data,
        session=session,
        current_user=current_user,
        request_headers=request_headers,
        query_service_client=query_service_client,
        background_tasks=background_tasks,
        validate_access=validate_access,
        save_history=save_history,
    ):
        return recreated_node  # pragma: no cover

    namespace = get_namespace_from_name(data.name)
    await get_node_namespace(
        session=session,
        namespace=namespace,
    )
    data.namespace = namespace

    node = Node(
        name=data.name,
        namespace=data.namespace,
        type=NodeType.CUBE,
        current_version=0,
        created_by_id=current_user.id,
    )
    node_revision = await create_cube_node_revision(
        session=session,
        data=data,
        current_user=current_user,
    )
    await save_node(
        session,
        node_revision,
        node,
        data.mode,
        current_user=current_user,
        save_history=save_history,
    )
    node = await Node.get_by_name(session, data.name)  # type: ignore
    return node


@router.post(
    "/register/table/{catalog}/{schema_}/{table}/",
    response_model=NodeOutput,
    status_code=201,
)
async def register_table(
    catalog: str,
    schema_: str,
    table: str,
    *,
    session: AsyncSession = Depends(get_session),
    request: Request,
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    current_user: User = Depends(get_and_update_current_user),
    background_tasks: BackgroundTasks,
    save_history: Callable = Depends(get_save_history),
) -> NodeOutput:
    """
    Register a table. This creates a source node in the SOURCE_NODE_NAMESPACE and
    the source node's schema will be inferred using the configured query service.
    """
    request_headers = dict(request.headers)
    if not query_service_client:
        raise DJConfigurationException(
            message="Registering tables or views requires that a query "
            "service is configured for columns inference",
        )
    namespace = f"{settings.source_node_namespace}.{catalog}.{schema_}"
    name = f"{namespace}.{table}"
    await raise_if_node_exists(session, name)

    # Create the namespace if required (idempotent)
    await create_node_namespace(
        namespace=namespace,
        session=session,
        current_user=current_user,
        save_history=save_history,
    )

    # Use reflection to get column names and types
    _catalog = await get_catalog_by_name(session=session, name=catalog)
    columns = query_service_client.get_columns_for_table(
        _catalog.name,
        schema_,
        table,
        request_headers,
        _catalog.engines[0] if len(_catalog.engines) >= 1 else None,
    )

    return await create_source(
        data=CreateSourceNode(
            catalog=catalog,
            schema_=schema_,
            table=table,
            name=name,
            display_name=name,
            columns=[ColumnOutput.from_orm(col) for col in columns],
            description="This source node was automatically created as a registered table.",
            mode=NodeMode.PUBLISHED,
        ),
        session=session,
        current_user=current_user,
        background_tasks=background_tasks,
        save_history=save_history,
        request=request,
    )


@router.post(
    "/register/view/{catalog}/{schema_}/{view}/",
    response_model=NodeOutput,
    status_code=201,
)
async def register_view(
    catalog: str,
    schema_: str,
    view: str,
    query: str,
    replace: bool = False,
    *,
    session: AsyncSession = Depends(get_session),
    request: Request,
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    current_user: User = Depends(get_and_update_current_user),
    background_tasks: BackgroundTasks,
    save_history: Callable = Depends(get_save_history),
) -> NodeOutput:
    """
    Register a view by creating the view in the database and adding a source node for it.
    The source node is created in the SOURCE_NODE_NAMESPACE and
    its schema will be inferred using the configured query service.
    """
    request_headers = dict(request.headers)
    if not query_service_client:
        raise DJConfigurationException(
            message="Registering tables or views requires that a query "
            "service is configured for columns inference",
        )
    namespace = f"{settings.source_node_namespace}.{catalog}.{schema_}"
    node_name = f"{namespace}.{view}"
    view_name = f"{schema_}.{view}"
    await raise_if_node_exists(session, node_name)

    # Re-create the view in the database
    _catalog = await get_catalog_by_name(session=session, name=catalog)
    or_replace = "OR REPLACE" if replace else ""
    query = f"CREATE {or_replace} VIEW {view_name} AS {query}"
    query_create = QueryCreate(
        engine_name=_catalog.engines[0].name,
        catalog_name=_catalog.name,
        engine_version=_catalog.engines[0].version,
        submitted_query=query,
        async_=False,
    )
    query_service_client.create_view(
        view_name,
        query_create,
        request_headers,
    )

    # Use reflection to get column names and types
    columns = query_service_client.get_columns_for_table(
        _catalog.name,
        schema_,
        view,
        request_headers,
        _catalog.engines[0] if len(_catalog.engines) >= 1 else None,
    )

    # Create the namespace if required (idempotent)
    await create_node_namespace(
        namespace=namespace,
        session=session,
        current_user=current_user,
        save_history=save_history,
    )

    return await create_source(
        data=CreateSourceNode(
            catalog=catalog,
            schema_=schema_,
            table=view,
            name=node_name,
            display_name=node_name,
            columns=[ColumnOutput.from_orm(col) for col in columns],
            description="This source node was automatically created as a registered view.",
            mode=NodeMode.PUBLISHED,
            query=query,
        ),
        session=session,
        current_user=current_user,
        background_tasks=background_tasks,
        save_history=save_history,
        request=request,
    )


@router.post("/nodes/{name}/columns/{column}/", status_code=201)
async def link_dimension(
    name: str,
    column: str,
    dimension: str,
    dimension_column: Optional[str] = None,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_and_update_current_user),
    save_history: Callable = Depends(get_save_history),
) -> JSONResponse:
    """
    Add information to a node column
    """
    node = await Node.get_by_name(
        session,
        name,
        raise_if_not_exists=True,
    )
    dimension_node = await Node.get_by_name(
        session,
        dimension,
        raise_if_not_exists=True,
    )
    if dimension_node.type != NodeType.DIMENSION:  # type: ignore  # pragma: no cover
        # pragma: no cover
        raise DJInvalidInputException(f"Node {node.name} is not of type dimension!")  # type: ignore
    primary_key_columns = dimension_node.current.primary_key()  # type: ignore
    if len(primary_key_columns) > 1:
        raise DJActionNotAllowedException(  # pragma: no cover
            "Cannot use this endpoint to link a dimension with a compound primary key.",
        )

    target_column = await get_column(session, node.current, column)  # type: ignore
    if dimension_column:
        # Check that the dimension column exists
        column_from_dimension = await get_column(
            session,
            dimension_node.current,  # type: ignore
            dimension_column,
        )

        # Check the dimension column's type is compatible with the target column's type
        if not column_from_dimension.type.is_compatible(target_column.type):
            raise DJInvalidInputException(
                f"The column {target_column.name} has type {target_column.type} "
                f"and is being linked to the dimension {dimension} via the dimension"
                f" column {dimension_column}, which has type {column_from_dimension.type}."
                " These column types are incompatible and the dimension cannot be linked",
            )

    link_input = JoinLinkInput(
        dimension_node=dimension,
        join_type=JoinType.LEFT,
        join_on=(
            f"{name}.{column} = {dimension_node.name}.{primary_key_columns[0].name}"  # type: ignore
        ),
    )
    activity_type = await upsert_complex_dimension_link(
        session,
        name,
        link_input,
        current_user,
        save_history,
    )

    node = await Node.get_by_name(
        session,
        name,
        raise_if_not_exists=True,
    )
    return JSONResponse(
        status_code=201,
        content={
            "message": (
                f"Dimension node {dimension} has been successfully "
                f"linked to node {name} using column {column}."
            )
            if activity_type == ActivityType.CREATE
            else (
                f"The dimension link between {name} and {dimension} "
                "has been successfully updated."
            ),
        },
    )


@router.post("/nodes/{node_name}/columns/{node_column}/link", status_code=201)
async def add_reference_dimension_link(
    node_name: str,
    node_column: str,
    dimension_node: str,
    dimension_column: str,
    role: Optional[str] = None,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_and_update_current_user),
    save_history: Callable = Depends(get_save_history),
) -> JSONResponse:
    """
    Add reference dimension link to a node column
    """
    node = await Node.get_by_name(session, node_name, raise_if_not_exists=True)
    dim_node = await Node.get_by_name(session, dimension_node, raise_if_not_exists=True)
    if dim_node.type != NodeType.DIMENSION:  # type: ignore
        raise DJInvalidInputException(
            message=f"Node {node.name} is not of type dimension!",  # type: ignore
        )

    # The target and dimension columns should both exist
    target_column = await get_column(session, node.current, node_column)  # type: ignore
    dim_column = await get_column(session, dim_node.current, dimension_column)  # type: ignore

    # Check the dimension column's type is compatible with the target column's type
    if not dim_column.type.is_compatible(target_column.type):
        raise DJInvalidInputException(
            f"The column {target_column.name} has type {target_column.type} "
            f"and is being linked to the dimension {dimension_node} "
            f"via the dimension column {dimension_column}, which has "
            f"type {dim_column.type}. These column types are incompatible"
            " and the dimension cannot be linked",
        )

    activity_type = (
        ActivityType.UPDATE if target_column.dimension_column else ActivityType.CREATE
    )

    # Create the reference link
    target_column.dimension_id = dim_node.id  # type: ignore
    target_column.dimension_column = (
        f"{dimension_column}[{role}]" if role else dimension_column
    )
    session.add(target_column)
    await save_history(
        event=History(
            entity_type=EntityType.LINK,
            entity_name=node.name,  # type: ignore
            node=node.name,  # type: ignore
            activity_type=activity_type,
            details={
                "node_name": node_name,  # type: ignore
                "node_column": node_column,
                "dimension_node": dimension_node,
                "dimension_column": dimension_column,
                "role": role,
            },
            user=current_user.username,
        ),
        session=session,
    )
    await session.commit()
    return JSONResponse(
        status_code=201,
        content={
            "message": (
                f"{node_name}.{node_column} has been successfully "
                f"linked to {dimension_node}.{dimension_column}"
            ),
        },
    )


@router.delete("/nodes/{node_name}/columns/{node_column}/link", status_code=201)
async def remove_reference_dimension_link(
    node_name: str,
    node_column: str,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_and_update_current_user),
    save_history: Callable = Depends(get_save_history),
) -> JSONResponse:
    """
    Remove reference dimension link from a node column
    """
    node = await Node.get_by_name(session, node_name, raise_if_not_exists=True)
    target_column = await get_column(session, node.current, node_column)  # type: ignore
    if target_column.dimension_id or target_column.dimension_column:
        target_column.dimension_id = None
        target_column.dimension_column = None
        session.add(target_column)
        await save_history(
            event=History(
                entity_type=EntityType.LINK,
                entity_name=node.name,  # type: ignore
                node=node.name,  # type: ignore
                activity_type=ActivityType.DELETE,
                details={
                    "node_name": node_name,  # type: ignore
                    "node_column": node_column,
                },
                user=current_user.username,
            ),
            session=session,
        )
        session.commit()
        return JSONResponse(
            status_code=200,
            content={
                "message": (
                    f"The reference dimension link on {node_name}.{node_column} has been removed."
                ),
            },
        )
    return JSONResponse(
        status_code=200,
        content={
            "message": (
                f"There is no reference dimension link on {node_name}.{node_column}."
            ),
        },
    )


@router.post("/nodes/{node_name}/link/", status_code=201)
async def add_complex_dimension_link(
    node_name: str,
    link_input: JoinLinkInput,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_and_update_current_user),
    save_history: Callable = Depends(get_save_history),
) -> JSONResponse:
    """
    Links a source, dimension, or transform node to a dimension with a custom join query.
    If a link already exists, updates the link definition.
    """
    activity_type = await upsert_complex_dimension_link(
        session,
        node_name,
        link_input,
        current_user,
        save_history,
    )
    return JSONResponse(
        status_code=201,
        content={
            "message": (
                f"Dimension node {link_input.dimension_node} has been successfully "
                f"linked to node {node_name}."
            )
            if activity_type == ActivityType.CREATE
            else (
                f"The dimension link between {node_name} and {link_input.dimension_node} "
                "has been successfully updated."
            ),
        },
    )


@router.delete("/nodes/{node_name}/link/", status_code=201)
async def remove_complex_dimension_link(
    node_name: str,
    link_identifier: LinkDimensionIdentifier,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_and_update_current_user),
    save_history: Callable = Depends(get_save_history),
) -> JSONResponse:
    """
    Removes a complex dimension link based on the dimension node and its role (if any).
    """
    return await remove_dimension_link(
        session,
        node_name,
        link_identifier,
        current_user,
        save_history,
    )


@router.delete("/nodes/{name}/columns/{column}/", status_code=201)
async def delete_dimension_link(
    name: str,
    column: str,
    dimension: str,
    dimension_column: Optional[str] = None,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_and_update_current_user),
    save_history: Callable = Depends(get_save_history),
) -> JSONResponse:
    """
    Remove the link between a node column and a dimension node
    """
    return await remove_dimension_link(
        session,
        name,
        LinkDimensionIdentifier(dimension_node=dimension, role=None),
        current_user,
        save_history,
    )


@router.post(
    "/nodes/{name}/tags/",
    status_code=200,
    tags=["tags"],
    name="Update Tags on Node",
)
async def tags_node(
    name: str,
    tag_names: Optional[List[str]] = Query(default=None),
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_and_update_current_user),
    save_history: Callable = Depends(get_save_history),
) -> JSONResponse:
    """
    Add a tag to a node
    """
    node = await Node.get_by_name(session=session, name=name)
    if not tag_names:
        tag_names = []  # pragma: no cover
    tags = await get_tags_by_name(session, names=tag_names)
    node.tags = tags  # type: ignore

    session.add(node)
    await save_history(
        event=History(
            entity_type=EntityType.NODE,
            entity_name=node.name,  # type: ignore
            node=node.name,  # type: ignore
            activity_type=ActivityType.TAG,
            details={
                "tags": tag_names,
            },
            user=current_user.username,
        ),
        session=session,
    )
    await session.commit()
    await session.refresh(node)
    for tag in tags:
        await session.refresh(tag)

    return JSONResponse(
        status_code=200,
        content={
            "message": (
                f"Node `{name}` has been successfully updated with "
                f"the following tags: {', '.join(tag_names)}"
            ),
        },
    )


@router.post(
    "/nodes/{name}/refresh/",
    response_model=NodeOutput,
    status_code=201,
)
async def refresh_source_node(
    name: str,
    *,
    session: AsyncSession = Depends(get_session),
    request: Request,
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    current_user: User = Depends(get_and_update_current_user),
    save_history: Callable = Depends(get_save_history),
) -> NodeOutput:
    """
    Refresh a source node with the latest columns from the query service.
    """
    request_headers = dict(request.headers)
    source_node = await Node.get_by_name(
        session,
        name,
        options=[
            joinedload(Node.current).options(*NodeRevision.default_load_options()),
            joinedload(Node.tags),
        ],
    )
    current_revision = source_node.current  # type: ignore

    # If this is a view-based source node, let's rerun the create view
    new_query = None
    if current_revision.query:
        catalog = await get_catalog_by_name(
            session=session,
            name=current_revision.catalog.name,
        )
        query_create = QueryCreate(
            engine_name=catalog.engines[0].name,
            catalog_name=catalog.name,
            engine_version=catalog.engines[0].version,
            submitted_query=current_revision.query,
            async_=False,
        )
        query_service_client.create_view(
            view_name=current_revision.table,
            query_create=query_create,
            request_headers=request_headers,
        )
        new_query = current_revision.query

    # Get the latest columns for the source node's table from the query service
    new_columns = []
    try:
        new_columns = query_service_client.get_columns_for_table(
            current_revision.catalog.name,
            current_revision.schema_,  # type: ignore
            current_revision.table,  # type: ignore
            request_headers,
            current_revision.catalog.engines[0]
            if len(current_revision.catalog.engines) >= 1
            else None,
        )
    except DJDoesNotExistException:
        # continue with the update, if the table was not found
        pass

    refresh_details = {}
    if new_columns:
        # check if any of the columns have changed (only continue with update if they have)
        column_changes = {col.identifier() for col in current_revision.columns} != {
            (col.name, str(parse_rule(str(col.type), "dataType")))
            for col in new_columns
        }

        # if the columns haven't changed and the node has a table, we can skip the update
        if not column_changes:
            if not source_node.missing_table:  # type: ignore
                return source_node  # type: ignore
            # if the columns haven't changed but the node has a missing table, we should fix it
            source_node.missing_table = False  # type: ignore
            refresh_details["missing_table"] = "False"
    else:
        # since we don't see any columns, we assume the table is gone
        if source_node.missing_table:  # type: ignore
            # but if the node already has a missing table, we can skip the update
            return source_node  # type: ignore
        source_node.missing_table = True  # type: ignore
        new_columns = current_revision.columns
        refresh_details["missing_table"] = "True"

    # Create a new node revision with the updated columns and bump the version
    old_version = Version.parse(source_node.current_version)  # type: ignore
    new_revision = NodeRevision(
        name=current_revision.name,
        type=current_revision.type,
        node_id=current_revision.node_id,
        display_name=current_revision.display_name,
        description=current_revision.description,
        mode=current_revision.mode,
        catalog_id=current_revision.catalog_id,
        schema_=current_revision.schema_,
        table=current_revision.table,
        status=current_revision.status,
        dimension_links=[
            DimensionLink(
                dimension_id=link.dimension_id,
                join_sql=link.join_sql,
                join_type=link.join_type,
                join_cardinality=link.join_cardinality,
                materialization_conf=link.materialization_conf,
            )
            for link in current_revision.dimension_links
        ],
        created_by_id=current_user.id,
        query=new_query,
    )
    new_revision.version = str(old_version.next_major_version())
    new_revision.columns = [
        Column(
            name=column.name,
            type=column.type,
            node_revisions=[new_revision],
            order=idx,
        )
        for idx, column in enumerate(new_columns)
    ]

    # Keep the dimension links and attributes on the columns from the node's
    # last revision if any existed
    new_revision.copy_dimension_links_from_revision(current_revision)

    # Point the source node to the new revision
    source_node.current_version = new_revision.version  # type: ignore
    new_revision.extra_validation()

    session.add(new_revision)
    session.add(source_node)

    refresh_details["version"] = new_revision.version
    await save_history(
        event=History(
            entity_type=EntityType.NODE,
            entity_name=source_node.name,  # type: ignore
            node=source_node.name,  # type: ignore
            activity_type=ActivityType.REFRESH,
            details=refresh_details,
            user=current_user.username,
        ),
        session=session,
    )
    await session.commit()

    source_node = await Node.get_by_name(
        session,
        name,
        options=[
            joinedload(Node.current).options(*NodeRevision.default_load_options()),
            joinedload(Node.tags),
        ],
    )
    await session.refresh(source_node, ["current"])
    return source_node  # type: ignore


@router.patch("/nodes/{name}/", response_model=NodeOutput)
async def update_node(
    name: str,
    data: UpdateNode,
    *,
    session: AsyncSession = Depends(get_session),
    request: Request,
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    current_user: User = Depends(get_and_update_current_user),
    background_tasks: BackgroundTasks,
    validate_access: access.ValidateAccessFn = Depends(
        validate_access,
    ),
    save_history: Callable = Depends(get_save_history),
) -> NodeOutput:
    """
    Update a node.
    """
    request_headers = dict(request.headers)
    await update_any_node(
        name,
        data,
        session=session,
        query_service_client=query_service_client,
        current_user=current_user,
        background_tasks=background_tasks,
        validate_access=validate_access,
        request_headers=request_headers,
        save_history=save_history,
    )
    node = await Node.get_by_name(
        session,
        name,
        options=[
            joinedload(Node.current).options(*NodeRevision.default_load_options()),
            joinedload(Node.tags),
        ],
    )
    return node  # type: ignore


@router.get("/nodes/similarity/{node1_name}/{node2_name}")
async def calculate_node_similarity(
    node1_name: str,
    node2_name: str,
    *,
    session: AsyncSession = Depends(get_session),
) -> JSONResponse:
    """
    Compare two nodes by how similar their queries are
    """
    node1 = await Node.get_by_name(
        session,
        node1_name,
        options=[joinedload(Node.current)],
        raise_if_not_exists=True,
    )
    node2 = await Node.get_by_name(
        session,
        node2_name,
        options=[joinedload(Node.current)],
        raise_if_not_exists=True,
    )
    if NodeType.SOURCE in (node1.type, node2.type):  # type: ignore
        raise DJInvalidInputException(
            message="Cannot determine similarity of source nodes",
            http_status_code=HTTPStatus.CONFLICT,
        )
    node1_ast = parse(node1.current.query)  # type: ignore
    node2_ast = parse(node2.current.query)  # type: ignore
    similarity = node1_ast.similarity_score(node2_ast)
    return JSONResponse(status_code=200, content={"similarity": similarity})


@router.get(
    "/nodes/{name}/downstream/",
    response_model=List[DAGNodeOutput],
    name="List Downstream Nodes For A Node",
)
async def list_downstream_nodes(
    name: str,
    *,
    node_type: NodeType = None,
    depth: int = -1,
    session: AsyncSession = Depends(get_session),
) -> List[DAGNodeOutput]:
    """
    List all nodes that are downstream from the given node, filterable by type and max depth.
    Setting a max depth of -1 will include all downstream nodes.
    """
    return await get_downstream_nodes(
        session=session,
        node_name=name,
        node_type=node_type,
        include_deactivated=False,
        depth=depth,
    )


@router.get(
    "/nodes/{name}/upstream/",
    response_model=List[DAGNodeOutput],
    name="List Upstream Nodes For A Node",
)
async def list_upstream_nodes(
    name: str,
    *,
    node_type: NodeType = None,
    session: AsyncSession = Depends(get_session),
) -> List[DAGNodeOutput]:
    """
    List all nodes that are upstream from the given node, filterable by type.
    """
    return await get_upstream_nodes(session, name, node_type)


@router.get(
    "/nodes/{name}/dag/",
    name="List All Connected Nodes (Upstreams + Downstreams)",
)
async def list_node_dag(
    name: str,
    *,
    session: AsyncSession = Depends(get_session),
) -> List[DAGNodeOutput]:
    """
    List all nodes that are part of the DAG of the given node. This means getting all upstreams,
    downstreams, and linked dimension nodes.
    """
    node = await Node.get_by_name(
        session,
        name,
        options=_node_output_options(),
        raise_if_not_exists=True,
    )
    dimension_nodes = await get_dimensions(
        session,
        node.current.parents[0] if node.type == NodeType.METRIC else node,  # type: ignore
        with_attributes=False,
    )
    if node.type == NodeType.METRIC:  # type: ignore  # pragma: no cover
        dimension_nodes = dimension_nodes + node.current.parents  # type: ignore
    dimension_nodes += [node]  # type: ignore
    downstreams = await get_downstream_nodes(
        session,
        name,
        include_deactivated=False,
        include_cubes=False,
    )
    upstreams = await get_upstream_nodes(session, name, include_deactivated=False)
    dag_nodes = set(dimension_nodes + downstreams + upstreams)
    return list(dag_nodes)


@router.get(
    "/nodes/{name}/dimensions/",
    response_model=List[DimensionAttributeOutput],
    name="List All Dimension Attributes",
)
async def list_all_dimension_attributes(
    name: str,
    *,
    depth: int = 30,
    session: AsyncSession = Depends(get_session),
) -> list[DimensionAttributeOutput]:
    """
    List all available dimension attributes for the given node.
    """
    dimensions = await get_dimension_attributes(session, name)
    filter_only_dimensions = await get_filter_only_dimensions(session, name)
    return dimensions + filter_only_dimensions


@router.get(
    "/nodes/{name}/lineage/",
    response_model=List[LineageColumn],
    name="List column level lineage of node",
)
async def column_lineage(
    name: str,
    *,
    session: AsyncSession = Depends(get_session),
) -> List[LineageColumn]:
    """
    List column-level lineage of a node in a graph
    """

    node = await Node.get_by_name(
        session,
        name,
        options=[
            selectinload(Node.current).options(
                selectinload(NodeRevision.columns).options(
                    selectinload(Column.attributes).joinedload(
                        ColumnAttribute.attribute_type,
                    ),
                    selectinload(Column.dimension),
                    selectinload(Column.partition),
                ),
            ),
        ],
    )
    if node.current.lineage:  # type: ignore
        return node.current.lineage  # type: ignore
    return await get_column_level_lineage(session, node.current)  # type: ignore  # pragma: no cover


@router.patch(
    "/nodes/{node_name}/columns/{column_name}/",
    response_model=ColumnOutput,
    status_code=201,
)
async def set_column_display_name(
    node_name: str,
    column_name: str,
    display_name: str,
    current_user: User = Depends(get_and_update_current_user),
    save_history: Callable = Depends(get_save_history),
    *,
    session: AsyncSession = Depends(get_session),
) -> ColumnOutput:
    """
    Set column name for the node
    """
    node = await Node.get_by_name(
        session,
        node_name,
        options=[joinedload(Node.current)],
    )
    column = await get_column(session, node.current, column_name)  # type: ignore
    column.display_name = display_name
    session.add(column)
    await session.commit()
    await save_history(
        event=History(
            entity_type=EntityType.COLUMN_ATTRIBUTE,
            node=node.name,  # type: ignore
            activity_type=ActivityType.UPDATE,
            details={
                "column": column.name,
                "display_name": display_name,
            },
            user=current_user.username,
        ),
        session=session,
    )
    return column


@router.patch(
    "/nodes/{node_name}/columns/{column_name}/description",
    response_model=ColumnOutput,
    status_code=201,
)
async def set_column_description(
    node_name: str,
    column_name: str,
    description: str,
    current_user: User = Depends(get_and_update_current_user),
    save_history: Callable = Depends(get_save_history),
    *,
    session: AsyncSession = Depends(get_session),
) -> ColumnOutput:
    """
    Set column description for the node
    """
    node = await Node.get_by_name(
        session,
        node_name,
        options=[joinedload(Node.current)],
    )
    column = await get_column(session, node.current, column_name)  # type: ignore
    column.description = description
    session.add(column)
    await session.commit()
    await save_history(
        event=History(
            entity_type=EntityType.COLUMN_ATTRIBUTE,
            node=node.name,  # type: ignore
            activity_type=ActivityType.UPDATE,
            details={
                "column": column.name,
                "description": description,
            },
            user=current_user.username,
        ),
        session=session,
    )
    return column


@router.post(
    "/nodes/{node_name}/columns/{column_name}/partition",
    response_model=ColumnOutput,
    status_code=201,
    name="Set Node Column as Partition",
)
async def set_column_partition(
    node_name: str,
    column_name: str,
    input_partition: PartitionInput,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_and_update_current_user),
    save_history: Callable = Depends(get_save_history),
) -> ColumnOutput:
    """
    Add or update partition columns for the specified node.
    """
    node = await Node.get_by_name(
        session,
        node_name,
        options=[
            joinedload(Node.current).options(
                *NodeRevision.default_load_options(),
                joinedload(NodeRevision.cube_elements),
            ),
        ],
    )
    column = get_node_column(node, column_name)  # type: ignore
    upsert_partition_event = History(
        entity_type=EntityType.PARTITION,
        node=node_name,
        activity_type=ActivityType.CREATE,
        details={
            "column": column_name,
            "partition": input_partition.dict(),
        },
        user=current_user.username,
    )

    if input_partition.type_ == PartitionType.TEMPORAL:
        if input_partition.granularity is None:
            raise DJInvalidInputException(
                message=f"The granularity must be provided for temporal partitions. "
                f"One of: {[val.name for val in Granularity]}",
            )
        if input_partition.format is None:
            raise DJInvalidInputException(
                message="The temporal partition column's datetime format must be provided.",
            )

    if column.partition:
        column.partition.type_ = input_partition.type_
        column.partition.granularity = input_partition.granularity
        column.partition.format = input_partition.format
        session.add(column)
        upsert_partition_event.activity_type = ActivityType.UPDATE
        session.add(upsert_partition_event)
    else:
        partition = Partition(
            column=column,
            type_=input_partition.type_,
            granularity=input_partition.granularity,
            format=input_partition.format,
        )
        session.add(partition)
    await save_history(event=upsert_partition_event, session=session)
    await session.commit()
    await session.refresh(column)
    return column


@router.post(
    "/nodes/{node_name}/copy",
    response_model=DAGNodeOutput,
    name="Copy A Node",
)
async def copy_node(
    node_name: str,
    *,
    new_name: str,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_and_update_current_user),
    save_history: Callable = Depends(get_save_history),
) -> DAGNodeOutput:
    """
    Copy this node to a new name.
    """
    # Check to make sure that the new node's namespace exists
    new_node_namespace = ".".join(new_name.split(".")[:-1])
    await get_node_namespace(session, new_node_namespace, raise_if_not_exists=True)

    # Check if there is already a node with the new name
    existing_new_node = await get_node_by_name(
        session,
        new_name,
        raise_if_not_exists=False,
        include_inactive=True,
    )
    if existing_new_node:
        if existing_new_node.deactivated_at:
            await hard_delete_node(new_name, session, current_user, save_history)
        else:
            raise DJAlreadyExistsException(
                f"A node with name {new_name} already exists.",
            )

    # Copy existing node to the new name
    await copy_to_new_node(session, node_name, new_name, current_user, save_history)
    new_node = await Node.get_by_name(session, new_name)
    return new_node  # type: ignore
