# pylint: disable=too-many-lines,too-many-arguments
"""
Node related APIs.
"""
import logging
import os
from http import HTTPStatus
from typing import List, Optional

from fastapi import BackgroundTasks, Depends, Query, Response
from fastapi.responses import JSONResponse
from fastapi_cache.decorator import cache
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.sql.operators import is_
from starlette.requests import Request

from datajunction_server.api.catalogs import UNKNOWN_CATALOG_ID
from datajunction_server.api.helpers import (
    activate_node,
    deactivate_node,
    get_catalog_by_name,
    get_column,
    get_node_by_name,
    get_node_namespace,
    hard_delete_node,
    raise_if_node_exists,
    revalidate_node,
    validate_node_data,
)
from datajunction_server.api.namespaces import create_node_namespace
from datajunction_server.api.tags import get_tags_by_name
from datajunction_server.constants import NODE_LIST_MAX
from datajunction_server.database.column import Column
from datajunction_server.database.dimensionlink import DimensionLink
from datajunction_server.database.history import ActivityType, EntityType, History
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.partition import Partition
from datajunction_server.database.user import User
from datajunction_server.errors import (
    DJDoesNotExistException,
    DJException,
    DJInvalidInputException,
)
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.access.authorization import (
    validate_access,
    validate_access_requests,
)
from datajunction_server.internal.nodes import (
    _create_node_from_inactive,
    create_cube_node_revision,
    create_node_revision,
    get_column_level_lineage,
    get_node_column,
    save_column_level_lineage,
    save_node,
    set_node_column_attributes,
    update_any_node,
)
from datajunction_server.models import access
from datajunction_server.models.attribute import AttributeTypeIdentifier
from datajunction_server.models.dimensionlink import (
    LinkDimensionIdentifier,
    LinkDimensionInput,
)
from datajunction_server.models.history import status_change_history
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
    NodeValidation,
    UpdateNode,
)
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.partition import (
    Granularity,
    PartitionInput,
    PartitionType,
)
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.sql.dag import (
    get_dimensions,
    get_downstream_nodes,
    get_nodes_with_dimension,
    get_upstream_nodes,
)
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.utils import (
    Version,
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
def validate_node(
    data: NodeRevisionBase,
    response: Response,
    session: Session = Depends(get_session),
) -> NodeValidation:
    """
    Determines whether the provided node is valid and returns metadata from node validation.
    """

    if data.type == NodeType.SOURCE:
        raise DJException(message="Source nodes cannot be validated")

    node_validator = validate_node_data(data, session)
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


@router.post("/nodes/{name}/validate/", response_model=NodeValidation)
def revalidate(
    name: str,
    session: Session = Depends(get_session),
    current_user: Optional[User] = Depends(get_current_user),
) -> NodeValidation:
    """
    Revalidate a single existing node and update its status appropriately
    """
    status = revalidate_node(name=name, session=session, current_user=current_user)
    return JSONResponse(
        status_code=HTTPStatus.OK,
        content={
            "message": f"Node `{name}` has been set to {status}",
            "status": status,
        },
    )


@router.post(
    "/nodes/{node_name}/columns/{column_name}/attributes/",
    response_model=List[ColumnOutput],
    status_code=201,
)
def set_column_attributes(
    node_name: str,
    column_name: str,
    attributes: List[AttributeTypeIdentifier],
    *,
    session: Session = Depends(get_session),
    current_user: Optional[User] = Depends(get_current_user),
) -> List[ColumnOutput]:
    """
    Set column attributes for the node.
    """
    node = get_node_by_name(session, node_name)
    columns = set_node_column_attributes(
        session,
        node,
        column_name,
        attributes,
        current_user=current_user,
    )
    return columns  # type: ignore


@router.get("/nodes/", response_model=List[str])
def list_nodes(
    node_type: Optional[NodeType] = None,
    prefix: Optional[str] = None,
    *,
    session: Session = Depends(get_session),
    current_user: Optional[User] = Depends(get_current_user),
    validate_access: access.ValidateAccessFn = Depends(  # pylint: disable=W0621
        validate_access,
    ),
) -> List[str]:
    """
    List the available nodes.
    """
    statement = select(Node).where(is_(Node.deactivated_at, None))
    if prefix:
        statement = statement.where(
            Node.name.like(f"{prefix}%"),  # type: ignore  # pylint: disable=no-member
        )
    if node_type:
        statement = statement.where(Node.type == node_type)
    nodes = session.execute(statement).unique().scalars().all()
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
def list_all_nodes_with_details(
    node_type: Optional[NodeType] = None,
    *,
    session: Session = Depends(get_session),
    current_user: Optional[User] = Depends(get_current_user),
    validate_access: access.ValidateAccessFn = Depends(  # pylint: disable=W0621
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
        .limit(NODE_LIST_MAX)
    )  # Very high limit as a safeguard
    results = [
        NodeIndexItem(name=row[0], display_name=row[1], description=row[2], type=row[3])
        for row in session.execute(nodes_query).all()
    ]
    if len(results) == NODE_LIST_MAX:  # pragma: no cover
        _logger.warning(
            "%s limit reached when returning all nodes, all nodes may not be captured in results",
            NODE_LIST_MAX,
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


@router.get("/nodes/{name}/")
def get_node(name: str, *, session: Session = Depends(get_session)) -> NodeOutput:
    """
    Show the active version of the specified node.
    """
    node = get_node_by_name(session, name, with_current=True)
    return NodeOutput.from_orm(node)


@router.delete("/nodes/{name}/")
def delete_node(
    name: str,
    *,
    session: Session = Depends(get_session),
    current_user: Optional[User] = Depends(get_current_user),
):
    """
    Delete (aka deactivate) the specified node.
    """
    deactivate_node(session, name, current_user=current_user)
    return JSONResponse(
        status_code=HTTPStatus.OK,
        content={"message": f"Node `{name}` has been successfully deleted."},
    )


@router.delete("/nodes/{name}/hard/", name="Hard Delete a DJ Node")
def hard_delete(
    name: str,
    session: Session = Depends(get_session),
    current_user: Optional[User] = Depends(get_current_user),
) -> JSONResponse:
    """
    Hard delete a node, destroying all links and invalidating all downstream nodes.
    This should be used with caution, deactivating a node is preferred.
    """
    impact = hard_delete_node(name=name, session=session, current_user=current_user)
    return JSONResponse(
        status_code=HTTPStatus.OK,
        content={
            "message": f"The node `{name}` has been completely removed.",
            "impact": impact,
        },
    )


@router.post("/nodes/{name}/restore/")
def restore_node(
    name: str,
    *,
    session: Session = Depends(get_session),
    current_user: Optional[User] = Depends(get_current_user),
):
    """
    Restore (aka re-activate) the specified node.
    """
    activate_node(session, name, current_user=current_user)
    return JSONResponse(
        status_code=HTTPStatus.OK,
        content={"message": f"Node `{name}` has been successfully restored."},
    )


@router.get("/nodes/{name}/revisions/", response_model=List[NodeRevisionOutput])
def list_node_revisions(
    name: str, *, session: Session = Depends(get_session)
) -> List[NodeRevisionOutput]:
    """
    List all revisions for the node.
    """
    node = get_node_by_name(session, name, with_current=False)
    return node.revisions  # type: ignore


@router.post("/nodes/source/", response_model=NodeOutput, name="Create A Source Node")
def create_source(
    data: CreateSourceNode,
    session: Session = Depends(get_session),
    current_user: Optional[User] = Depends(get_current_user),
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    validate_access: access.ValidateAccessFn = Depends(  # pylint: disable=W0621
        validate_access,
    ),
) -> NodeOutput:
    """
    Create a source node. If columns are not provided, the source node's schema
    will be inferred using the configured query service.
    """
    raise_if_node_exists(session, data.name)

    # if the node previously existed and now is inactive
    if recreated_node := _create_node_from_inactive(
        new_node_type=NodeType.SOURCE,
        data=data,
        session=session,
        current_user=current_user,
        query_service_client=query_service_client,
        validate_access=validate_access,
    ):
        return recreated_node

    namespace = get_namespace_from_name(data.name)
    get_node_namespace(
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
    )
    catalog = get_catalog_by_name(session=session, name=data.catalog)

    columns = [
        Column(
            name=column_data.name,
            type=column_data.type,
            dimension=(
                get_node_by_name(
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
    )
    node.display_name = node_revision.display_name

    # Point the node to the new node revision.
    save_node(session, node_revision, node, data.mode, current_user=current_user)
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
def create_node(
    data: CreateNode,
    request: Request,
    *,
    session: Session = Depends(get_session),
    current_user: Optional[User] = Depends(get_current_user),
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    background_tasks: BackgroundTasks,
    validate_access: access.ValidateAccessFn = Depends(  # pylint: disable=W0621
        validate_access,
    ),
) -> NodeOutput:
    """
    Create a node.
    """
    node_type = NodeType(os.path.basename(os.path.normpath(request.url.path)))

    if node_type == NodeType.DIMENSION and not data.primary_key:
        raise DJInvalidInputException("Dimension nodes must define a primary key!")

    raise_if_node_exists(session, data.name)

    # if the node previously existed and now is inactive
    if recreated_node := _create_node_from_inactive(
        new_node_type=node_type,
        data=data,
        session=session,
        current_user=current_user,
        query_service_client=query_service_client,
        background_tasks=background_tasks,
        validate_access=validate_access,
    ):
        return recreated_node  # pragma: no cover

    namespace = get_namespace_from_name(data.name)
    get_node_namespace(
        session=session,
        namespace=namespace,
    )  # Will return 404 if namespace doesn't exist
    data.namespace = namespace

    node = Node(
        name=data.name,
        namespace=data.namespace,
        type=NodeType(node_type),
        current_version=0,
    )
    node_revision = create_node_revision(data, node_type, session)
    save_node(session, node_revision, node, data.mode, current_user=current_user)
    background_tasks.add_task(
        save_column_level_lineage,
        session=session,
        node_revision=node_revision,
    )
    session.refresh(node_revision)
    session.refresh(node)

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
                set_node_column_attributes(
                    session,
                    node,
                    key_column,
                    [AttributeTypeIdentifier(name="primary_key", namespace="system")],
                    current_user=current_user,
                )
    session.refresh(node)
    session.refresh(node.current)
    return node


@router.post(
    "/nodes/cube/",
    response_model=NodeOutput,
    status_code=201,
    name="Create A Cube",
)
def create_cube(
    data: CreateCubeNode,
    *,
    session: Session = Depends(get_session),
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    current_user: Optional[User] = Depends(get_current_user),
    background_tasks: BackgroundTasks,
    validate_access: access.ValidateAccessFn = Depends(  # pylint: disable=W0621
        validate_access,
    ),
) -> NodeOutput:
    """
    Create a cube node.
    """
    raise_if_node_exists(session, data.name)

    # if the node previously existed and now is inactive
    if recreated_node := _create_node_from_inactive(
        new_node_type=NodeType.CUBE,
        data=data,
        session=session,
        current_user=current_user,
        query_service_client=query_service_client,
        background_tasks=background_tasks,
        validate_access=validate_access,
    ):
        return recreated_node  # pragma: no cover

    namespace = get_namespace_from_name(data.name)
    get_node_namespace(
        session=session,
        namespace=namespace,
    )
    data.namespace = namespace

    node = Node(
        name=data.name,
        namespace=data.namespace,
        type=NodeType.CUBE,
        current_version=0,
    )
    node_revision = create_cube_node_revision(session=session, data=data)
    save_node(session, node_revision, node, data.mode, current_user=current_user)
    return node


@router.post(
    "/register/table/{catalog}/{schema_}/{table}/",
    response_model=NodeOutput,
    status_code=201,
)
def register_table(
    catalog: str,
    schema_: str,
    table: str,
    session: Session = Depends(get_session),
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    current_user: Optional[User] = Depends(get_current_user),
) -> NodeOutput:
    """
    Register a table. This creates a source node in the SOURCE_NODE_NAMESPACE and
    the source node's schema will be inferred using the configured query service.
    """
    if not query_service_client:
        raise DJException(
            message="Registering tables requires that a query "
            "service is configured for table columns inference",
        )
    namespace = f"{settings.source_node_namespace}.{catalog}.{schema_}"
    name = f"{namespace}.{table}"
    raise_if_node_exists(session, name)

    # Create the namespace if required (idempotent)
    create_node_namespace(
        namespace=namespace,
        session=session,
        current_user=current_user,
    )

    # Use reflection to get column names and types
    _catalog = get_catalog_by_name(session=session, name=catalog)
    columns = query_service_client.get_columns_for_table(
        _catalog.name,
        schema_,
        table,
        _catalog.engines[0] if len(_catalog.engines) >= 1 else None,
    )

    return create_source(
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
    )


@router.post("/nodes/{name}/columns/{column}/", status_code=201)
def link_dimension(
    name: str,
    column: str,
    dimension: str,
    dimension_column: Optional[str] = None,
    session: Session = Depends(get_session),
    current_user: Optional[User] = Depends(get_current_user),
) -> JSONResponse:
    """
    Add information to a node column
    """
    node = get_node_by_name(session=session, name=name)
    dimension_node = get_node_by_name(
        session=session,
        name=dimension,
        node_type=NodeType.DIMENSION,
    )
    if (
        dimension_node.current.catalog_id != UNKNOWN_CATALOG_ID
        and dimension_node.current.catalog is not None
        and node.current.catalog.name != dimension_node.current.catalog.name
    ):
        raise DJException(
            message=(
                "Cannot add dimension to column, because catalogs do not match: "
                f"{node.current.catalog.name}, {dimension_node.current.catalog.name}"
            ),
        )

    target_column = get_column(node.current, column)
    if dimension_column:
        # Check that the dimension column exists
        column_from_dimension = get_column(dimension_node.current, dimension_column)

        # Check the dimension column's type is compatible with the target column's type
        if not column_from_dimension.type.is_compatible(target_column.type):
            raise DJInvalidInputException(
                f"The column {target_column.name} has type {target_column.type} "
                f"and is being linked to the dimension {dimension} via the dimension"
                f" column {dimension_column}, which has type {column_from_dimension.type}."
                " These column types are incompatible and the dimension cannot be linked",
            )

    target_column.dimension = dimension_node
    target_column.dimension_id = dimension_node.id
    target_column.dimension_column = dimension_column

    session.add(node)
    session.add(
        History(
            entity_type=EntityType.LINK,
            entity_name=node.name,
            node=node.name,
            activity_type=ActivityType.CREATE,
            details={
                "column": target_column.name,
                "dimension": dimension_node.name,
                "dimension_column": dimension_column or "",
            },
            user=current_user.username if current_user else None,
        ),
    )
    session.commit()
    session.refresh(node)
    return JSONResponse(
        status_code=201,
        content={
            "message": (
                f"Dimension node {dimension} has been successfully "
                f"linked to column {column} on node {name}"
            ),
        },
    )


@router.post("/nodes/{node_name}/link/", status_code=201)
def add_complex_dimension_link(  # pylint: disable=too-many-locals
    node_name: str,
    link_input: LinkDimensionInput,
    session: Session = Depends(get_session),
    current_user: Optional[User] = Depends(get_current_user),
) -> JSONResponse:
    """
    Links a source, dimension, or transform node to a dimension with a custom join query.
    If a link already exists, updates the link definition.
    """
    node = get_node_by_name(session=session, name=node_name)
    if node.type not in (NodeType.SOURCE, NodeType.DIMENSION, NodeType.TRANSFORM):
        raise DJInvalidInputException(
            message=f"Cannot link dimension to a node of type {node.type}. "
            "Must be a source, dimension, or transform node.",
        )

    # Find the dimension node and check that the catalogs match
    dimension_node = get_node_by_name(
        session=session,
        name=link_input.dimension_node,
        node_type=NodeType.DIMENSION,
    )
    if (
        dimension_node.current.catalog_id != UNKNOWN_CATALOG_ID
        and dimension_node.current.catalog is not None
        and node.current.catalog.name != dimension_node.current.catalog.name
    ):
        raise DJException(  # pragma: no cover
            message=(
                "Cannot link dimension to node, because catalogs do not match: "
                f"{node.current.catalog.name}, {dimension_node.current.catalog.name}"
            ),
        )

    # Parse the join query and do some basic verification of its validity
    join_query = parse(
        f"SELECT 1 FROM {node_name} "
        f"{link_input.join_type} JOIN {link_input.dimension_node} "
        f"ON {link_input.join_on}",
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    join_query.compile(ctx)
    join_relation = join_query.select.from_.relations[0].extensions[0]  # type: ignore

    # Verify that the query references both the node and the dimension being joined
    expected_references = {node_name, link_input.dimension_node}
    references = {
        table.name.namespace.identifier()  # type: ignore
        for table in join_relation.criteria.on.find_all(ast.Column)  # type: ignore
    }
    if expected_references.difference(references):
        raise DJInvalidInputException(
            f"The join SQL provided does not reference both the origin node {node_name} and the "
            f"dimension node {link_input.dimension_node} that it's being joined to.",
        )

    # Verify that the columns in the ON clause exist on both nodes
    if ctx.exception.errors:
        raise DJException(
            message=f"Join query {link_input.join_on} is not valid",
            errors=ctx.exception.errors,
        )

    # Find an existing dimension link if there is already one defined for this node
    existing_link = [
        link
        for link in node.current.dimension_links
        if link.dimension_id == dimension_node.id and link.role == link_input.role
    ]
    is_update = False

    if existing_link:
        # Update the existing dimension link
        is_update = True
        dimension_link = existing_link[0]
        dimension_link.join_sql = link_input.join_on
        dimension_link.join_type = DimensionLink.parse_join_type(
            join_relation.join_type,
        )
        dimension_link.join_cardinality = link_input.join_cardinality
    else:
        # If there is no existing link, create new dimension link object
        dimension_link = DimensionLink(
            node_revision_id=node.current.id,
            dimension_id=dimension_node.id,
            join_sql=link_input.join_on,
            join_type=DimensionLink.parse_join_type(join_relation.join_type),
            join_cardinality=link_input.join_cardinality,
            role=link_input.role,
        )

    # Add/update the dimension link in the database
    session.add(dimension_link)
    session.add(
        History(
            entity_type=EntityType.LINK,
            entity_name=node.name,
            node=node.name,
            activity_type=ActivityType.CREATE if not is_update else ActivityType.UPDATE,
            details={
                "dimension": dimension_node.name,
                "join_sql": link_input.join_on,
                "join_cardinality": link_input.join_cardinality,
                "role": link_input.role,
            },
            user=current_user.username if current_user else None,
        ),
    )
    session.commit()
    session.refresh(node)
    return JSONResponse(
        status_code=201,
        content={
            "message": (
                f"Dimension node {dimension_node.name} has been successfully "
                f"linked to node {node_name}."
            )
            if not is_update
            else (
                f"The dimension link between {node_name} and {dimension_node.name} "
                "has been successfully updated."
            ),
        },
    )


@router.delete("/nodes/{node_name}/link/", status_code=201)
def remove_complex_dimension_link(  # pylint: disable=too-many-locals
    node_name: str,
    link_identifier: LinkDimensionIdentifier,
    session: Session = Depends(get_session),
    current_user: Optional[User] = Depends(get_current_user),
) -> JSONResponse:
    """
    Removes a complex dimension link based on the dimension node and its role (if any).
    """
    node = get_node_by_name(session=session, name=node_name)

    # Find the dimension node
    dimension_node = get_node_by_name(
        session=session,
        name=link_identifier.dimension_node,
        node_type=NodeType.DIMENSION,
    )
    removed = False

    # Delete the dimension link if one exists
    for link in node.current.dimension_links:
        if (
            link.dimension_id == dimension_node.id  # pragma: no cover
            and link.role == link_identifier.role  # pragma: no cover
        ):
            removed = True
            session.delete(link)
    if not removed:
        return JSONResponse(
            status_code=HTTPStatus.NOT_FOUND,
            content={
                "message": f"Dimension link to node {link_identifier.dimension_node} "
                + (f"with role {link_identifier.role} " if link_identifier.role else "")
                + "not found",
            },
        )
    session.add(
        History(
            entity_type=EntityType.LINK,
            entity_name=node.name,
            node=node.name,
            activity_type=ActivityType.DELETE,
            details={
                "dimension": dimension_node.name,
                "role": link_identifier.role,
            },
            user=current_user.username if current_user else None,
        ),
    )
    session.commit()
    session.refresh(node)
    return JSONResponse(
        status_code=201,
        content={
            "message": (
                f"Dimension link {dimension_node.name} "
                + (f"(role {link_identifier.role}) " if link_identifier.role else "")
                + f"to node {node_name} has been removed."
            )
            if removed
            else (
                f"Dimension link {dimension_node.name} "
                + (f"(role {link_identifier.role}) " if link_identifier.role else "")
                + f"to node {node_name} does not exist!"
            ),
        },
    )


@router.delete("/nodes/{name}/columns/{column}/", status_code=201)
def delete_dimension_link(
    name: str,
    column: str,
    dimension: str,
    dimension_column: Optional[str] = None,
    session: Session = Depends(get_session),
    current_user: Optional[User] = Depends(get_current_user),
) -> JSONResponse:
    """
    Remove the link between a node column and a dimension node
    """
    node = get_node_by_name(session=session, name=name)
    target_column = get_column(node.current, column)
    if (not target_column.dimension or target_column.dimension.name != dimension) and (
        not target_column.dimension_column
        or target_column.dimension_column != dimension_column
    ):
        return JSONResponse(
            status_code=304,
            content={
                "message": (
                    f"No change was made to {column} on node {name} as the "
                    f"specified dimension link to {dimension} on "
                    f"{dimension_column} was not found."
                ),
            },
        )

    # Find cubes that are affected by this dimension link removal and update their statuses
    affected_cubes = get_nodes_with_dimension(
        session,
        target_column.dimension,
        [NodeType.CUBE],
    )
    if affected_cubes:
        for cube in affected_cubes:
            if cube.status != NodeStatus.INVALID:  # pragma: no cover
                cube.status = NodeStatus.INVALID
                session.add(cube)
                session.add(
                    status_change_history(
                        node,
                        NodeStatus.VALID,
                        NodeStatus.INVALID,
                        current_user=current_user,
                    ),
                )

    target_column.dimension = None  # type: ignore
    target_column.dimension_id = None
    target_column.dimension_column = None
    session.add(node)
    session.add(
        History(
            entity_type=EntityType.LINK,
            entity_name=node.name,
            node=node.name,
            activity_type=ActivityType.DELETE,
            details={
                "column": column,
                "dimension": dimension,
                "dimension_column": dimension_column or "",
            },
            user=current_user.username if current_user else None,
        ),
    )
    session.commit()
    session.refresh(node)

    return JSONResponse(
        status_code=201,
        content={
            "message": (
                f"The dimension link on the node {name}'s {column} to "
                f"{dimension} has been successfully removed."
            ),
        },
    )


@router.post(
    "/nodes/{name}/tags/",
    status_code=200,
    tags=["tags"],
    name="Update Tags on Node",
)
def tags_node(
    name: str,
    tag_names: Optional[List[str]] = Query(default=None),
    *,
    session: Session = Depends(get_session),
    current_user: Optional[User] = Depends(get_current_user),
) -> JSONResponse:
    """
    Add a tag to a node
    """
    node = get_node_by_name(session=session, name=name)
    if not tag_names:
        tag_names = []  # pragma: no cover
    tags = get_tags_by_name(session, names=tag_names)
    node.tags = tags

    session.add(node)
    session.add(
        History(
            entity_type=EntityType.NODE,
            entity_name=node.name,
            node=node.name,
            activity_type=ActivityType.TAG,
            details={
                "tags": tag_names,
            },
            user=current_user.username if current_user else None,
        ),
    )
    session.commit()
    session.refresh(node)
    for tag in tags:
        session.refresh(tag)

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
def refresh_source_node(
    name: str,
    *,
    session: Session = Depends(get_session),
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    current_user: Optional[User] = Depends(get_current_user),
) -> NodeOutput:
    """
    Refresh a source node with the latest columns from the query service.
    """
    source_node = get_node_by_name(session, name, node_type=NodeType.SOURCE)
    current_revision = source_node.current

    # Get the latest columns for the source node's table from the query service
    new_columns = []
    try:
        new_columns = query_service_client.get_columns_for_table(
            current_revision.catalog.name,
            current_revision.schema_,  # type: ignore
            current_revision.table,  # type: ignore
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
            col.identifier() for col in new_columns
        }

        # FIXME: there is a bug with type translation (bigint != long) - fix it. # pylint: disable=fixme

        # if the columns haven't changed and the node has a table, we can skip the update
        if not column_changes:
            if not source_node.missing_table:
                return source_node
            # if the columns haven't changed but the node has a missing table, we should fix it
            source_node.missing_table = False
            refresh_details["missing_table"] = "False"
    else:
        # since we don't see any columns, we'll assume the table is gone
        source_node.missing_table = True
        new_columns = current_revision.columns
        refresh_details["missing_table"] = "True"

    # Create a new node revision with the updated columns and bump the version
    old_version = Version.parse(source_node.current_version)
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
    source_node.current_version = new_revision.version
    new_revision.extra_validation()

    session.add(new_revision)
    session.add(source_node)

    refresh_details["version"] = new_revision.version
    session.add(
        History(
            entity_type=EntityType.NODE,
            entity_name=source_node.name,
            node=source_node.name,
            activity_type=ActivityType.REFRESH,
            details=refresh_details,
            user=current_user.username if current_user else None,
        ),
    )
    session.commit()
    session.refresh(source_node.current)
    return source_node  # type: ignore


@router.patch("/nodes/{name}/", response_model=NodeOutput)
def update_node(
    name: str,
    data: UpdateNode,
    *,
    session: Session = Depends(get_session),
    query_service_client: QueryServiceClient = Depends(get_query_service_client),
    current_user: Optional[User] = Depends(get_current_user),
    background_tasks: BackgroundTasks,
    validate_access: access.ValidateAccessFn = Depends(  # pylint: disable=W0621
        validate_access,
    ),
) -> NodeOutput:
    """
    Update a node.
    """
    node = update_any_node(
        name,
        data,
        session=session,
        query_service_client=query_service_client,
        current_user=current_user,
        background_tasks=background_tasks,
        validate_access=validate_access,
    )
    return node  # type: ignore


@router.get("/nodes/similarity/{node1_name}/{node2_name}")
def calculate_node_similarity(
    node1_name: str, node2_name: str, *, session: Session = Depends(get_session)
) -> JSONResponse:
    """
    Compare two nodes by how similar their queries are
    """
    node1 = get_node_by_name(session=session, name=node1_name)
    node2 = get_node_by_name(session=session, name=node2_name)
    if NodeType.SOURCE in (node1.type, node2.type):
        raise DJException(
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
def list_downstream_nodes(
    name: str,
    *,
    node_type: NodeType = None,
    session: Session = Depends(get_session),
) -> List[DAGNodeOutput]:
    """
    List all nodes that are downstream from the given node, filterable by type.
    """
    return get_downstream_nodes(session, name, node_type)  # type: ignore


@router.get(
    "/nodes/{name}/upstream/",
    response_model=List[DAGNodeOutput],
    name="List Upstream Nodes For A Node",
)
def list_upstream_nodes(
    name: str,
    *,
    node_type: NodeType = None,
    session: Session = Depends(get_session),
) -> List[DAGNodeOutput]:
    """
    List all nodes that are upstream from the given node, filterable by type.
    """
    return get_upstream_nodes(session, name, node_type)


@router.get(
    "/nodes/{name}/dag/",
    name="List All Connected Nodes (Upstreams + Downstreams)",
)
def list_node_dag(
    name: str, *, session: Session = Depends(get_session)
) -> List[DAGNodeOutput]:
    """
    List all nodes that are part of the DAG of the given node. This means getting all upstreams,
    downstreams, and linked dimension nodes.
    """
    node = get_node_by_name(session, name, with_current=True)
    dimension_nodes = (
        get_dimensions(session, node.current.parents[0], with_attributes=False)
        + node.current.parents
        if node.type == NodeType.METRIC
        else get_dimensions(session, node, with_attributes=False)
    )
    dimension_nodes += [node]
    downstreams = get_downstream_nodes(
        session,
        name,
        include_deactivated=False,
        include_cubes=False,
    )
    upstreams = get_upstream_nodes(session, name, include_deactivated=False)
    dag_nodes = set(dimension_nodes + downstreams + upstreams)
    return [DAGNodeOutput.from_orm(node) for node in dag_nodes]


@router.get(
    "/nodes/{name}/dimensions/",
    response_model=List[DimensionAttributeOutput],
    name="List All Dimension Attributes",
)
def list_all_dimension_attributes(
    name: str, *, session: Session = Depends(get_session)
) -> List[DimensionAttributeOutput]:
    """
    List all available dimension attributes for the given node.
    """
    node = get_node_by_name(session, name)
    return get_dimensions(session, node, with_attributes=True)


@router.get(
    "/nodes/{name}/lineage/",
    response_model=List[LineageColumn],
    name="List column level lineage of node",
)
def column_lineage(
    name: str, *, session: Session = Depends(get_session)
) -> List[LineageColumn]:
    """
    List column-level lineage of a node in a graph
    """
    node = get_node_by_name(session, name)
    if node.current.lineage:
        return node.current.lineage  # type: ignore
    return get_column_level_lineage(session, node.current)  # pragma: no cover


@router.patch(
    "/nodes/{node_name}/columns/{column_name}/",
    response_model=ColumnOutput,
    status_code=201,
)
def set_column_display_name(
    node_name: str,
    column_name: str,
    display_name: str,
    current_user: Optional[User] = Depends(get_current_user),
    *,
    session: Session = Depends(get_session),
) -> ColumnOutput:
    """
    Set column name for the node
    """
    node = get_node_by_name(session, node_name)
    column = get_column(node.current, column_name)
    column.display_name = display_name
    session.add(column)
    session.add(
        History(
            entity_type=EntityType.COLUMN_ATTRIBUTE,
            node=node.name,
            activity_type=ActivityType.UPDATE,
            details={
                "column": column.name,
                "display_name": display_name,
            },
            user=current_user.username if current_user else None,
        ),
    )
    session.commit()
    session.refresh(column)
    session.refresh(node)
    session.refresh(node.current)
    return column


@router.post(
    "/nodes/{node_name}/columns/{column_name}/partition",
    response_model=ColumnOutput,
    status_code=201,
    name="Set Node Column as Partition",
)
def set_column_partition(  # pylint: disable=too-many-locals
    node_name: str,
    column_name: str,
    input_partition: PartitionInput,
    *,
    session: Session = Depends(get_session),
    current_user: Optional[User] = Depends(get_current_user),
) -> ColumnOutput:
    """
    Add or update partition columns for the specified node.
    """
    node = get_node_by_name(session, node_name)
    column = get_node_column(node, column_name)
    upsert_partition_event = History(
        entity_type=EntityType.PARTITION,
        node=node_name,
        activity_type=ActivityType.CREATE,
        details={
            "column": column_name,
            "partition": input_partition.dict(),
        },
        user=current_user.username if current_user else None,
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
        session.add(upsert_partition_event)

    session.commit()
    session.refresh(column)
    return column
