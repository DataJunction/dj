"""Nodes endpoint helper functions"""

import logging
from collections import defaultdict
from datetime import datetime, timezone
from http import HTTPStatus
from typing import Callable, Dict, List, Optional, Union, cast

from fastapi import BackgroundTasks, Request
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

from datajunction_server.internal.caching.interface import Cache
from datajunction_server.models.query import QueryCreate
from datajunction_server.api.helpers import (
    get_attribute_type,
    get_catalog_by_name,
    get_column,
    get_node_by_name,
    get_node_namespace,
    map_dimensions_to_roles,
    raise_if_node_exists,
    resolve_downstream_references,
    validate_cube,
)
from datajunction_server.construction.build_v2 import compile_node_ast
from datajunction_server.database.attributetype import AttributeType, ColumnAttribute
from datajunction_server.database.column import Column
from datajunction_server.database.catalog import Catalog
from datajunction_server.database.dimensionlink import DimensionLink
from datajunction_server.database.history import History
from datajunction_server.database.metricmetadata import MetricMetadata
from datajunction_server.database.node import MissingParent, Node, NodeRevision
from datajunction_server.database.partition import Partition
from datajunction_server.database.user import User
from datajunction_server.database.measure import FrozenMeasure
from datajunction_server.sql.decompose import MetricComponentExtractor
from datajunction_server.errors import (
    DJActionNotAllowedException,
    DJDoesNotExistException,
    DJError,
    DJException,
    DJInvalidInputException,
    DJNodeNotFound,
    ErrorCode,
)
from datajunction_server.internal.materializations import (
    create_new_materialization,
    schedule_materialization_jobs,
    schedule_materialization_jobs_bg,
)
from datajunction_server.internal.history import ActivityType, EntityType
from datajunction_server.internal.validation import NodeValidator, validate_node_data
from datajunction_server.models import access
from datajunction_server.models.attribute import (
    AttributeTypeIdentifier,
    ColumnAttributes,
    UniquenessScope,
)
from datajunction_server.models.base import labelize
from datajunction_server.models.cube import CubeRevisionMetadata
from datajunction_server.models.dimensionlink import (
    JoinLinkInput,
    JoinType,
    LinkDimensionIdentifier,
)
from datajunction_server.models.history import status_change_history
from datajunction_server.models.materialization import (
    MaterializationConfigOutput,
    MaterializationJobTypeEnum,
    UpsertMaterialization,
)
from datajunction_server.models.cube_materialization import UpsertCubeMaterialization
from datajunction_server.models.node import (
    DEFAULT_DRAFT_VERSION,
    DEFAULT_PUBLISHED_VERSION,
    CreateCubeNode,
    CreateNode,
    CreateSourceNode,
    LineageColumn,
    NodeMode,
    NodeOutput,
    NodeStatus,
    UpdateNode,
)
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.query import QueryCreate
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.sql.dag import (
    get_downstream_nodes,
    get_nodes_with_dimension,
    topological_sort,
)
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.ast import CompileContext
from datajunction_server.sql.parsing.backends.antlr4 import parse, parse_rule
from datajunction_server.typing import UTCDatetime
from datajunction_server.utils import (
    SEPARATOR,
    Version,
    VersionUpgrade,
    get_namespace_from_name,
    get_settings,
    session_context,
)

_logger = logging.getLogger(__name__)

settings = get_settings()


async def create_a_source_node(
    request: Request,
    session: AsyncSession,
    data: CreateSourceNode,
    current_user: User,
    query_service_client: QueryServiceClient,
    background_tasks: BackgroundTasks,
    validate_access: access.ValidateAccessFn,
    save_history: Callable,
):
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

    return await Node.get_by_name(  # type: ignore
        session,
        node.name,
        options=NodeOutput.load_options(),
    )


async def create_a_node(
    data: CreateNode,
    request: Request,
    node_type: NodeType,
    session: AsyncSession,
    current_user: User,
    query_service_client: QueryServiceClient,
    background_tasks: BackgroundTasks,
    validate_access: access.ValidateAccessFn,
    save_history: Callable,
    cache: Cache,
) -> Node:
    request_headers = dict(request.headers)
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
        cache=cache,
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

    column_names = {col.name: col for col in node_revision.columns}
    if data.primary_key:
        if any(key_column not in column_names for key_column in data.primary_key):
            raise DJInvalidInputException(
                f"Some columns in the primary key [{','.join(data.primary_key)}] "
                f"were not found in the list of available columns for the node {node.name}.",
            )
        pk_attribute = await get_attribute_type(
            session=session,
            name=ColumnAttributes.PRIMARY_KEY.value,
            namespace="system",
        )
        for key_column in data.primary_key:
            if key_column in column_names:  # pragma: no cover
                column_names[key_column].attributes.append(
                    ColumnAttribute(attribute_type=pk_attribute),
                )

    await save_node(
        session,
        node_revision,
        node,
        data.mode,
        current_user=current_user,
        save_history=save_history,
    )

    # For metric nodes, derive the referenced frozen measures and save them
    if node.type == NodeType.METRIC:
        background_tasks.add_task(derive_frozen_measures, node_revision.id)

    background_tasks.add_task(
        save_column_level_lineage,
        node_revision_id=node_revision.id,
    )

    return await Node.get_by_name(  # type: ignore
        session,
        node.name,
        options=NodeOutput.load_options(),
    )


async def create_a_cube(
    request: Request,
    session: AsyncSession,
    data: CreateCubeNode,
    current_user: User,
    query_service_client: QueryServiceClient,
    background_tasks: BackgroundTasks,
    validate_access: access.ValidateAccessFn,
    save_history: Callable,
) -> Node:
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
    return node


def get_node_column(node: Node, column_name: str) -> Column:
    """
    Gets the specified column on a node
    """
    column_map = {column.name: column for column in node.current.columns}
    if column_name not in column_map:
        raise DJDoesNotExistException(
            message=f"Column `{column_name}` does not exist on node `{node.name}`!",
        )
    column = column_map[column_name]
    return column


async def validate_and_build_attribute(
    session: AsyncSession,
    column: Column,
    attribute: AttributeTypeIdentifier,
    node: Node,
) -> ColumnAttribute:
    """
    Run some validation and build column attribute.
    """
    # Verify attribute type exists
    attribute_type = await get_attribute_type(
        session,
        attribute.name,
        attribute.namespace,
    )
    if not attribute_type:
        raise DJDoesNotExistException(
            message=f"Attribute type `{attribute.namespace}"
            f".{attribute.name}` "
            f"does not exist!",
        )

    # Verify that the attribute type is allowed for this node
    if node.type not in attribute_type.allowed_node_types:
        raise DJInvalidInputException(
            message=f"Attribute type `{attribute.namespace}.{attribute_type.name}` "
            f"not allowed on node type `{node.type}`!",
        )

    return ColumnAttribute(
        attribute_type=attribute_type,
        column=column,
    )


async def set_node_column_attributes(
    session: AsyncSession,
    node: Node,
    column_name: str,
    attributes: List[AttributeTypeIdentifier],
    current_user: User,
    save_history: Callable,
) -> List[Column]:
    """
    Sets the column attributes on the node if allowed.
    """
    column = get_node_column(node, column_name)
    all_columns_map = {column.name: column for column in node.current.columns}

    existing_attributes = column.attributes
    existing_attributes_map = {
        attr.attribute_type.name: attr for attr in existing_attributes
    }
    await session.refresh(column)
    column.attributes = []
    for attribute in attributes:
        if attribute.name in existing_attributes_map:
            column.attributes.append(existing_attributes_map[attribute.name])
        else:
            column.attributes.append(
                await validate_and_build_attribute(session, column, attribute, node),
            )

    # Validate column attributes by building mapping between
    # attribute scope and columns
    attributes_columns_map = defaultdict(set)
    all_columns = all_columns_map.values()

    for _col in all_columns:
        for attribute in _col.attributes:
            scopes_map = {
                UniquenessScope.NODE: attribute.attribute_type,
                UniquenessScope.COLUMN_TYPE: _col.type,
            }
            attributes_columns_map[
                (  # type: ignore
                    attribute.attribute_type,
                    tuple(
                        scopes_map[item]
                        for item in attribute.attribute_type.uniqueness_scope
                    ),
                )
            ].add(_col.name)

    for (attribute, _), columns in attributes_columns_map.items():
        if len(columns) > 1 and attribute.uniqueness_scope:
            column.attributes = existing_attributes
            raise DJInvalidInputException(
                message=f"The column attribute `{attribute.name}` is scoped to be "
                f"unique to the `{attribute.uniqueness_scope}` level, but there "
                "is more than one column tagged with it: "
                f"`{', '.join(sorted(list(columns)))}`",
            )

    session.add(column)
    await save_history(
        event=History(
            entity_type=EntityType.COLUMN_ATTRIBUTE,
            node=node.name,
            activity_type=ActivityType.SET_ATTRIBUTE,
            details={
                "column": column.name,
                "attributes": [attr.model_dump() for attr in attributes],
            },
            user=current_user.username,
        ),
        session=session,
    )
    await session.commit()
    await session.refresh(column)
    return [column]


async def create_node_revision(
    data: CreateNode,
    node_type: NodeType,
    session: AsyncSession,
    current_user: User,
) -> NodeRevision:
    """
    Create a non-source node revision.
    """
    node_revision = NodeRevision(
        name=data.name,
        display_name=(
            data.display_name
            if data.display_name
            else labelize(data.name.split(SEPARATOR)[-1])
        ),
        description=data.description,
        type=node_type,
        status=NodeStatus.VALID,
        query=data.query,
        mode=data.mode,
        required_dimensions=data.required_dimensions or [],
        created_by_id=current_user.id,
        custom_metadata=data.custom_metadata,
    )
    node_validator = await validate_node_data(node_revision, session)

    if node_validator.status == NodeStatus.INVALID:
        if node_revision.mode == NodeMode.DRAFT:
            node_revision.status = NodeStatus.INVALID
        else:
            raise DJException(
                http_status_code=HTTPStatus.BAD_REQUEST,
                errors=node_validator.errors,
            )
    else:
        node_revision.status = NodeStatus.VALID
    node_revision.missing_parents = [
        MissingParent(name=missing_parent)
        for missing_parent in node_validator.missing_parents_map
    ]
    node_revision.required_dimensions = node_validator.required_dimensions
    node_revision.metric_metadata = (
        MetricMetadata.from_input(data.metric_metadata)
        if node_type == NodeType.METRIC and data.metric_metadata is not None
        else None
    )
    new_parents = [node.name for node in node_validator.dependencies_map]
    catalog_ids = [
        node.catalog_id
        for node in node_validator.dependencies_map
        if node.catalog_id
        and node.catalog.name != settings.seed_setup.virtual_catalog_name
    ]
    if node_revision.mode == NodeMode.PUBLISHED and not len(set(catalog_ids)) <= 1:
        raise DJException(
            f"Cannot create nodes with multi-catalog dependencies: {set(catalog_ids)}",
        )
    catalog_id = next(
        iter(catalog_ids),
        (await Catalog.get_virtual_catalog(session)).id,
    )
    parent_refs = (
        (
            await session.execute(
                select(Node).where(
                    Node.name.in_(  # type: ignore
                        new_parents,
                    ),
                ),
            )
        )
        .unique()
        .scalars()
        .all()
    )
    node_revision.parents = parent_refs
    node_revision.columns = node_validator.columns or []
    if node_revision.type == NodeType.METRIC:
        if node_revision.columns:
            node_revision.columns[0].display_name = node_revision.display_name
    node_revision.catalog_id = catalog_id
    return node_revision


async def create_cube_node_revision(
    session: AsyncSession,
    data: CreateCubeNode,
    current_user: User,
) -> NodeRevision:
    """
    Create a cube node revision.
    """
    (
        metric_columns,
        metric_nodes,
        dimension_nodes,
        dimension_columns,
        catalog,
    ) = await validate_cube(
        session,
        data.metrics or [],
        data.dimensions or [],
        require_dimensions=False,
    )
    status = (
        NodeStatus.VALID
        if (
            all(metric.current.status == NodeStatus.VALID for metric in metric_nodes)
            and all(dim.current.status == NodeStatus.VALID for dim in dimension_nodes)
        )
        else NodeStatus.INVALID
    )

    # Build the "columns" for this node based on the cube elements. These are used
    # for marking partition columns when the cube gets materialized.
    node_columns = []
    dimension_to_roles_mapping = map_dimensions_to_roles(data.dimensions or [])
    for idx, col in enumerate(metric_columns + dimension_columns):
        await session.refresh(col, ["node_revision"])
        referenced_node = col.node_revision
        full_element_name = (
            referenced_node.name  # type: ignore
            if referenced_node.type == NodeType.METRIC  # type: ignore
            else f"{referenced_node.name}.{col.name}"  # type: ignore
        )
        node_column = Column(
            name=full_element_name,
            display_name=col.display_name,
            type=col.type,
            attributes=[
                ColumnAttribute(attribute_type_id=attr.attribute_type_id)
                for attr in col.attributes
            ],
            order=idx,
        )
        if full_element_name in dimension_to_roles_mapping:
            node_column.dimension_column = dimension_to_roles_mapping[full_element_name]

        node_columns.append(node_column)

    node_revision = NodeRevision(
        name=data.name,
        display_name=data.display_name or labelize(data.name.split(SEPARATOR)[-1]),
        description=data.description,
        type=NodeType.CUBE,
        query="",
        columns=node_columns,
        cube_elements=metric_columns + dimension_columns,
        parents=list(set(dimension_nodes + metric_nodes)),
        status=status,
        catalog=catalog,
        created_by_id=current_user.id,
    )
    return node_revision


async def derive_frozen_measures(node_revision_id: int) -> list[FrozenMeasure]:
    """
    Find or create frozen measures
    """
    async with session_context() as session:
        node_revision = cast(
            NodeRevision,
            await NodeRevision.get_by_id(
                session=session,
                node_revision_id=node_revision_id,
                options=[
                    joinedload(NodeRevision.parents).joinedload(Node.current),
                ],
            ),
        )
        if not node_revision:
            return []  # pragma: no cover
        extractor = MetricComponentExtractor.from_query_string(
            node_revision.query.lower(),
        )
        measures, derived_sql = extractor.extract()
        node_revision.derived_expression = str(derived_sql)

        frozen_measures: list[FrozenMeasure] = []
        if not node_revision.parents:
            return frozen_measures  # pragma: no cover

        await session.refresh(node_revision.parents[0], ["current"])
        for measure in measures:
            frozen_measure = await FrozenMeasure.get_by_name(
                session=session,
                name=measure.name,
            )
            if not frozen_measure and measure.aggregation:
                frozen_measure = FrozenMeasure(
                    name=measure.name,
                    upstream_revision_id=node_revision.parents[0].current.id,
                    expression=measure.expression,
                    aggregation=measure.aggregation,
                    rule=measure.rule,
                    used_by_node_revisions=[],
                )
                session.add(frozen_measure)
            if frozen_measure:
                frozen_measure.used_by_node_revisions.append(node_revision)
                frozen_measures.append(frozen_measure)
        await session.commit()
        return frozen_measures


async def save_node(
    session: AsyncSession,
    node_revision: NodeRevision,
    node: Node,
    node_mode: NodeMode,
    current_user: User,
    save_history: Callable,
):
    """
    Saves the newly created node revision
    Links the node and node revision together and saves them
    """
    node_revision.node = node
    node_revision.version = (
        str(DEFAULT_DRAFT_VERSION)
        if node_mode == NodeMode.DRAFT
        else str(DEFAULT_PUBLISHED_VERSION)
    )
    node.current_version = node_revision.version
    node_revision.extra_validation()
    node.owners.append(current_user)

    session.add(node)
    await save_history(
        event=History(
            node=node.name,
            entity_type=EntityType.NODE,
            entity_name=node.name,
            activity_type=ActivityType.CREATE,
            user=current_user.username,
        ),
        session=session,
    )
    await session.commit()
    await session.refresh(node, ["current"])
    newly_valid_nodes = await resolve_downstream_references(
        session=session,
        node_revision=node_revision,
        current_user=current_user,
        save_history=save_history,
    )
    await propagate_valid_status(
        session=session,
        valid_nodes=newly_valid_nodes,
        catalog_id=node.current.catalog_id,
        current_user=current_user,
        save_history=save_history,
    )
    await session.refresh(node.current)


async def copy_to_new_node(
    session: AsyncSession,
    existing_node_name: str,
    new_name: str,
    current_user: User,
    save_history: Callable,
) -> Node:
    """
    Copies the existing node to a new node with a new name.
    """
    node = await Node.get_by_name(
        session,
        existing_node_name,
        options=[
            joinedload(Node.current).options(
                *NodeRevision.default_load_options(),
                joinedload(NodeRevision.missing_parents),
            ),
            joinedload(Node.tags),
        ],
    )
    old_revision = node.current  # type: ignore
    new_node = Node(
        name=new_name,
        type=node.type,  # type: ignore
        display_name=node.display_name,  # type: ignore
        namespace=".".join(new_name.split(".")[:-1]),
        current_version=node.current_version,  # type: ignore
        created_at=node.created_at,  # type: ignore
        deactivated_at=node.deactivated_at,  # type: ignore
        tags=node.tags,  # type: ignore
        missing_table=node.missing_table,  # type: ignore
        created_by_id=current_user.id,
        owners=[current_user],
    )
    new_revision = NodeRevision(
        name=new_name,
        display_name=old_revision.display_name,
        type=old_revision.type,
        description=old_revision.description,
        query=old_revision.query,
        mode=old_revision.mode,
        version=old_revision.version,
        node=new_node,
        catalog=old_revision.catalog,
        schema_=old_revision.schema_,
        table=old_revision.table,
        required_dimensions=list(old_revision.required_dimensions),
        metric_metadata=old_revision.metric_metadata,
        cube_elements=list(old_revision.cube_elements),
        status=old_revision.status,
        parents=old_revision.parents,
        missing_parents=[
            MissingParent(name=missing_parent.name)
            for missing_parent in old_revision.missing_parents
        ],
        columns=[col.copy() for col in old_revision.columns],
        # TODO: availability and materializations are missing here
        lineage=old_revision.lineage,
        created_by_id=current_user.id,
        custom_metadata=old_revision.custom_metadata,
    )

    # Assemble new dimension links, where each link will need to have their join SQL rewritten
    new_dimension_links = []
    for link in old_revision.dimension_links:
        join_ast = link.join_sql_ast()
        for col in join_ast.find_all(ast.Column):
            if str(col.alias_or_name.namespace) == old_revision.name:
                col.alias_or_name.namespace = ast.Name(new_name)
        new_join_sql = str(
            join_ast.select.from_.relations[-1].extensions[-1].criteria.on,
        )
        new_dimension_links.append(
            DimensionLink(
                node_revision=new_revision,
                dimension_id=link.dimension_id,
                join_sql=new_join_sql,
                join_type=link.join_type,
                join_cardinality=link.join_cardinality,
                materialization_conf=link.materialization_conf,
            ),
        )
    new_revision.dimension_links = new_dimension_links

    # Reset the version of the new node
    new_revision.version = (
        str(DEFAULT_DRAFT_VERSION)
        if new_revision.mode == NodeMode.DRAFT
        else str(DEFAULT_PUBLISHED_VERSION)
    )
    new_node.current_version = new_revision.version
    session.add(new_revision)
    session.add(new_node)
    await session.commit()
    await save_history(
        event=History(
            node=new_node.name,
            entity_type=EntityType.NODE,
            entity_name=new_node.name,
            activity_type=ActivityType.CREATE,
            user=current_user.username,
            details={"copied_from": node.name},  # type: ignore
        ),
        session=session,
    )
    await session.refresh(new_node, ["current"])  # type: ignore

    # If the new node makes any downstream nodes valid, propagate
    newly_valid_nodes = await resolve_downstream_references(
        session=session,
        node_revision=new_revision,
        current_user=current_user,
        save_history=save_history,
    )
    await propagate_valid_status(
        session=session,
        valid_nodes=newly_valid_nodes,
        catalog_id=new_node.current.catalog_id,  # type: ignore
        current_user=current_user,
        save_history=save_history,
    )
    await session.refresh(new_node, ["current"])  # type: ignore
    return node  # type: ignore


async def update_any_node(
    name: str,
    data: UpdateNode,
    session: AsyncSession,
    request_headers: Dict[str, str],
    query_service_client: QueryServiceClient,
    current_user: User,
    save_history: Callable,
    background_tasks: BackgroundTasks = None,
    validate_access: access.ValidateAccessFn = None,
    refresh_materialization: bool = False,
    cache: Cache | None = None,
) -> Node:
    """
    Node update helper function that handles updating any node
    """
    node = await Node.get_by_name(
        session,
        name,
        for_update=True,
        include_inactive=True,
        options=[
            selectinload(Node.current).options(*NodeRevision.default_load_options()),
            selectinload(Node.owners),
        ],
        raise_if_not_exists=True,
    )
    node = cast(Node, node)

    # Check that the user has access to modify this node
    access_control = access.AccessControlStore(
        validate_access=validate_access,
        user=current_user,
        base_verb=access.ResourceRequestVerb.WRITE,
    )
    access_control.add_request_by_node(node)
    access_control.validate_and_raise()

    if data.owners and data.owners != [owner.username for owner in node.owners]:
        await update_owners(session, node, data.owners, current_user, save_history)

    if node.type == NodeType.CUBE:  # type: ignore
        node = await Node.get_cube_by_name(session, name)
        node_revision = await update_cube_node(
            session,
            node.current,  # type: ignore
            data,
            request_headers=request_headers,
            query_service_client=query_service_client,
            current_user=current_user,
            background_tasks=background_tasks,
            validate_access=validate_access,  # type: ignore
            save_history=save_history,
            refresh_materialization=refresh_materialization,
        )
        return node_revision.node if node_revision else node
    return await update_node_with_query(
        name,
        data,
        session,
        request_headers=request_headers,
        query_service_client=query_service_client,
        current_user=current_user,
        background_tasks=background_tasks,
        validate_access=validate_access,  # type: ignore
        save_history=save_history,
        cache=cache,
    )


async def update_node_with_query(
    name: str,
    data: UpdateNode,
    session: AsyncSession,
    *,
    request_headers: Dict[str, str],
    query_service_client: QueryServiceClient,
    current_user: User,
    background_tasks: BackgroundTasks,
    validate_access: access.ValidateAccessFn,
    save_history: Callable,
    cache: Cache,
) -> Node:
    """
    Update the named node with the changes defined in the UpdateNode object.
    Propagate these changes to all of the node's downstream children.

    Note: this function works for both source nodes and nodes with query (transforms,
    dimensions, metrics). We should update it to separate out the logic for source nodes
    """
    node = await Node.get_by_name(
        session,
        name,
        options=[
            joinedload(Node.current).options(*NodeRevision.default_load_options()),
            selectinload(Node.owners),
        ],
        include_inactive=True,
    )
    node = cast(Node, node)
    old_revision = node.current  # type: ignore
    new_revision = await create_new_revision_from_existing(
        session=session,
        old_revision=old_revision,
        node=node,  # type: ignore
        current_user=current_user,
        data=data,
    )

    if not new_revision:
        return node  # type: ignore

    node.current_version = new_revision.version  # type: ignore

    new_revision.extra_validation()

    session.add(new_revision)
    session.add(node)
    await save_history(
        event=node_update_history_event(new_revision, current_user),
        session=session,
    )

    if new_revision.status != old_revision.status:  # type: ignore
        await save_history(
            event=status_change_history(
                new_revision,  # type: ignore
                old_revision.status,
                new_revision.status,  # type: ignore
                current_user=current_user,
            ),
            session=session,
        )
    await session.commit()

    await session.refresh(new_revision)
    await session.refresh(node)

    # Handle materializations: Note that this must be done after we commit the new revision,
    # as otherwise the SQL build won't know about the new revision's query
    await session.refresh(old_revision, ["materializations"])
    await session.refresh(old_revision, ["columns"])
    active_materializations = [
        mat for mat in old_revision.materializations if not mat.deactivated_at
    ]

    await session.refresh(new_revision, ["materializations"])
    if active_materializations and new_revision.query != old_revision.query:
        for old in active_materializations:
            new_revision.materializations.append(
                await create_new_materialization(
                    session,
                    new_revision,
                    UpsertMaterialization(  # type: ignore
                        name=old.name,
                        config=old.config,
                        schedule=old.schedule,
                        strategy=old.strategy,
                        job=MaterializationJobTypeEnum.find_match(old.job).value.name,
                    )
                    if old.job != MaterializationJobTypeEnum.DRUID_CUBE.value.job_class
                    else (
                        UpsertCubeMaterialization(
                            job=MaterializationJobTypeEnum.find_match(
                                old.job,
                            ).value.name,
                            strategy=old.strategy,
                            schedule=old.schedule,
                            lookback_window=old.lookback_window,
                        )
                    ),
                    validate_access,
                    current_user=current_user,
                ),
            )
        background_tasks.add_task(
            schedule_materialization_jobs_bg,
            node_revision_id=node.current.id,  # type: ignore
            materialization_names=[
                mat.name
                for mat in node.current.materializations  # type: ignore
            ],
            query_service_client=query_service_client,
            request_headers=request_headers,
        )
        session.add(new_revision)
        await session.commit()

    if background_tasks:  # pragma: no cover
        background_tasks.add_task(
            save_column_level_lineage,
            node_revision_id=new_revision.id,
        )
        # TODO: Do not save this until:
        #   1. We get to the bottom of why there are query building discrepancies
        #   2. We audit our database calls to defer pulling the query_ast in most cases
        # background_tasks.add_task(
        #     save_query_ast,
        #     session=session,
        #     node_name=new_revision.name,
        # )

    history_events = {}
    old_columns_map = {col.name: col.type for col in old_revision.columns}

    await session.refresh(new_revision, ["columns"])
    await session.refresh(old_revision)
    history_events[node.name] = {  # type: ignore
        "name": node.name,  # type: ignore
        "current_version": node.current_version,  # type: ignore
        "previous_version": old_revision.version,
        "updated_columns": [
            col.name
            for col in new_revision.columns  # type: ignore
            if col.name not in old_columns_map or old_columns_map[col.name] != col.type
        ],
    }

    background_tasks.add_task(
        propagate_update_downstream,
        node,
        current_user=current_user,
        save_history=save_history,
        cache=cache,
    )
    await session.refresh(node, ["current"])
    await session.refresh(node.current, ["materializations"])  # type: ignore
    await session.refresh(node, ["owners"])  # type: ignore
    return node  # type: ignore


async def update_owners(
    session: AsyncSession,
    node: Node,
    new_owner_usernames: list[str],
    current_user: "User",
    save_history: Callable,
):
    """
    Update the owners of this node to match the given usernames.
    """
    from datajunction_server.internal.history import ActivityType, EntityType

    existing_owners = node.owners
    users = await User.get_by_usernames(session, usernames=new_owner_usernames)
    node.owners = users
    session.add(node)

    event = History(
        entity_type=EntityType.NODE,
        entity_name=node.name,
        node=node.name,
        activity_type=ActivityType.UPDATE,
        details={
            "version": node.current_version,
            "old_owners": [owner.username for owner in existing_owners],
            "new_owners": [owner.username for owner in node.owners],  # type: ignore
        },
        user=current_user.username,
    )
    await save_history(
        event=event,
        session=session,
    )
    await session.commit()


def has_minor_changes(
    old_revision: NodeRevision,
    data: UpdateNode,
):
    """
    Whether the node has minor changes
    """
    return (
        (
            data
            and data.description
            and old_revision.description
            and (old_revision.description != data.description)
        )
        or (data and data.mode and old_revision.mode != data.mode)
        or (
            data
            and data.display_name
            and old_revision.display_name != data.display_name
        )
    )


def node_update_history_event(new_revision: NodeRevision, current_user: User):
    """
    History event for node updates
    """
    return History(
        entity_type=EntityType.NODE,
        entity_name=new_revision.name,
        node=new_revision.name,
        activity_type=ActivityType.UPDATE,
        details={
            "version": new_revision.version,  # type: ignore
        },
        user=current_user.username,
    )


async def update_cube_node(
    session: AsyncSession,
    node_revision: NodeRevision,
    data: UpdateNode,
    *,
    request_headers: Dict[str, str],
    query_service_client: QueryServiceClient,
    current_user: User,
    background_tasks: BackgroundTasks = None,
    validate_access: access.ValidateAccessFn,
    save_history: Callable,
    refresh_materialization: bool = False,
) -> Optional[NodeRevision]:
    """
    Update cube node based on changes
    """
    node = await Node.get_cube_by_name(session, node_revision.name)
    node_revision = node.current  # type: ignore
    minor_changes = has_minor_changes(node_revision, data) or refresh_materialization
    old_metrics = [m.name for m in node_revision.cube_metrics()]
    old_dimensions = node_revision.cube_dimensions()
    major_changes = (data.metrics and data.metrics != old_metrics) or (
        data.dimensions and data.dimensions != old_dimensions
    )
    create_cube = CreateCubeNode(
        name=node_revision.name,
        display_name=data.display_name or node_revision.display_name,
        description=data.description or node_revision.description,
        metrics=data.metrics or old_metrics,
        dimensions=data.dimensions or old_dimensions,
        mode=data.mode or node_revision.mode,
        filters=data.filters or [],
        orderby=data.orderby or None,
        limit=data.limit or None,
    )
    if not major_changes and not minor_changes:
        return None

    new_cube_revision = await create_cube_node_revision(
        session,
        create_cube,
        current_user,
    )

    old_version = Version.parse(node_revision.version)
    if major_changes:
        new_cube_revision.version = str(old_version.next_major_version())
    elif minor_changes:  # pragma: no cover
        new_cube_revision.version = str(old_version.next_minor_version())
    new_cube_revision.node = node_revision.node
    new_cube_revision.node.current_version = new_cube_revision.version  # type: ignore

    await save_history(
        event=History(
            entity_type=EntityType.NODE,
            entity_name=new_cube_revision.name,
            node=new_cube_revision.name,
            activity_type=ActivityType.UPDATE,
            details={
                "version": new_cube_revision.version,  # type: ignore
            },
            pre={
                "metrics": old_metrics,
                "dimensions": old_dimensions,
            },
            post={
                "metrics": new_cube_revision.cube_node_metrics,
                "dimensions": new_cube_revision.cube_node_dimensions,
            },
            user=current_user.username,
        ),
        session=session,
    )

    # Bring over existing partition columns, if any
    new_columns_mapping = {col.name: col for col in new_cube_revision.columns}
    for col in node_revision.columns:
        new_col = new_columns_mapping.get(col.name)
        if col.partition and new_col:
            new_col.partition = Partition(
                column=new_col,
                type_=col.partition.type_,
                format=col.partition.format,
                granularity=col.partition.granularity,
            )

    # Update existing materializations
    active_materializations = [
        mat
        for mat in node_revision.materializations
        if not mat.deactivated_at and mat.name != "default"
    ]
    if active_materializations and (major_changes or refresh_materialization):
        for old in active_materializations:
            # Once we've migrated all materializations to the new format, we should only
            # be using UpsertCubeMaterialization for cube nodes
            job_type = MaterializationJobTypeEnum.find_match(old.job)
            materialization_upsert_class = UpsertMaterialization
            if job_type == MaterializationJobTypeEnum.DRUID_CUBE:
                materialization_upsert_class = UpsertCubeMaterialization
            new_cube_revision.materializations.append(
                await create_new_materialization(
                    session,
                    new_cube_revision,
                    materialization_upsert_class(
                        **MaterializationConfigOutput.model_validate(
                            old,
                        ).model_dump(
                            exclude={"job", "node_revision_id", "deactivated_at"},
                        ),
                        job=MaterializationJobTypeEnum.find_match(old.job).value.name,
                    ),
                    validate_access,
                    current_user=current_user,
                ),
            )
            await save_history(
                event=History(
                    entity_type=EntityType.MATERIALIZATION,
                    entity_name=old.name,
                    node=node_revision.name,
                    activity_type=ActivityType.UPDATE,
                    details={},
                    user=current_user.username,
                ),
                session=session,
            )
    session.add(new_cube_revision)
    session.add(new_cube_revision.node)
    await session.commit()

    await session.refresh(new_cube_revision, ["materializations"])
    if background_tasks:
        background_tasks.add_task(  # pragma: no cover
            schedule_materialization_jobs_bg,
            node_revision_id=new_cube_revision.id,
            materialization_names=[
                mat.name for mat in new_cube_revision.materializations
            ],
            query_service_client=query_service_client,
            request_headers=request_headers,
        )
    else:
        await schedule_materialization_jobs(  # pragma: no cover
            session=session,
            node_revision_id=new_cube_revision.id,
            materialization_names=[
                mat.name for mat in new_cube_revision.materializations
            ],
            query_service_client=query_service_client,
            request_headers=request_headers,
        )

    await session.refresh(new_cube_revision)
    await session.refresh(new_cube_revision.node)
    await session.refresh(new_cube_revision.node.current)
    return new_cube_revision


async def propagate_update_downstream(
    node: Node,
    current_user: User,
    save_history: Callable,
    cache: Cache | None = None,
):
    """
    Background task to propagate the updated node's changes to all of its downstream children.
    """
    async with session_context() as session:
        await _propagate_update_downstream(
            session=session,
            node=node,
            current_user=current_user,
            save_history=save_history,
            cache=cache,
        )


async def _propagate_update_downstream(
    session: AsyncSession,
    node: Node,
    current_user: User,
    save_history: Callable,
    cache: Cache | None = None,
):
    """
    Propagate the updated node's changes to all of its downstream children.
    Some potential changes to the upstream node and their effects on downstream nodes:
    - altered column names: may invalidate downstream nodes
    - altered column types: may invalidate downstream nodes
    - new columns: won't affect downstream nodes
    """
    _logger.info("Propagating update of node %s downstream", node.name)
    downstreams = await get_downstream_nodes(
        session,
        node.name,
        include_deactivated=False,
        include_cubes=False,
    )
    downstreams = topological_sort(downstreams)
    _logger.info(
        "Node %s updated — revalidating %s downstreams",
        node.name,
        len(downstreams),
    )

    # The downstreams need to be sorted topologically in order for the updates to be done
    # in the right order. Otherwise it is possible for a leaf node like a metric to be updated
    # before its upstreams are updated.
    for idx, downstream in enumerate(downstreams):
        original_node_revision = downstream.current
        previous_status = original_node_revision.status
        _logger.info(
            "[%s/%s] Revalidating downstream %s due to update of node %s",
            idx + 1,
            len(downstreams),
            downstream.name,
            node.name,
        )
        node_validator = await revalidate_node(
            downstream.name,
            session,
            current_user=current_user,
            save_history=save_history,
        )

        # Reset the upstreams DAG cache of any downstream nodes
        if cache:
            upstream_cache_key = downstream.upstream_cache_key()
            results = cache.get(upstream_cache_key)
            if results is not None:
                _logger.info(
                    "Clearing upstream cache for node %s due to update of node %s (cache key: %s)",
                    downstream.name,
                    node.name,
                    upstream_cache_key,
                )
                cache.delete(upstream_cache_key)

        # Record history event
        if (
            original_node_revision.version != downstream.current_version
            or previous_status != node_validator.status
        ):
            await save_history(
                event=History(
                    entity_type=EntityType.NODE,
                    entity_name=downstream.name,
                    node=downstream.name,
                    activity_type=ActivityType.UPDATE,
                    details={
                        "changes": {
                            "updated_columns": sorted(
                                list(node_validator.updated_columns),
                            ),
                        },
                        "upstream": {
                            "node": node.name,
                            "version": node.current_version,
                        },
                        "reason": f"Caused by update of `{node.name}` to "
                        f"{node.current_version}",
                    },
                    pre={
                        "status": previous_status,
                        "version": original_node_revision.version,
                    },
                    post={
                        "status": node_validator.status,
                        "version": downstream.current_version,
                    },
                    user=current_user.username,
                ),
                session=session,
            )
        await session.commit()


def copy_existing_node_revision(old_revision: NodeRevision, current_user: User):
    """
    Create an exact copy of the node revision
    """
    return NodeRevision(
        name=old_revision.name,
        version=old_revision.version,
        display_name=old_revision.display_name,
        description=old_revision.description,
        query=old_revision.query,
        type=old_revision.type,
        columns=[col.copy() for col in old_revision.columns],
        catalog=old_revision.catalog,
        schema_=old_revision.schema_,
        table=old_revision.table,
        parents=old_revision.parents,
        mode=old_revision.mode,
        materializations=old_revision.materializations,
        status=old_revision.status,
        required_dimensions=list(old_revision.required_dimensions),
        metric_metadata=old_revision.metric_metadata,
        dimension_links=[
            DimensionLink(
                dimension_id=link.dimension_id,
                join_sql=link.join_sql,
                join_type=link.join_type,
                join_cardinality=link.join_cardinality,
                role=link.role,
                materialization_conf=link.materialization_conf,
            )
            for link in old_revision.dimension_links
        ],
        created_by_id=current_user.id,
        custom_metadata=old_revision.custom_metadata,
    )


async def create_node_from_inactive(
    new_node_type: NodeType,
    data: Union[CreateSourceNode, CreateNode, CreateCubeNode],
    session: AsyncSession,
    *,
    current_user: User,
    request_headers: Dict[str, str],
    query_service_client: QueryServiceClient,
    save_history: Callable,
    background_tasks: BackgroundTasks = None,
    validate_access: access.ValidateAccessFn = None,
    cache: Cache | None = None,
) -> Optional[Node]:
    """
    If the node existed and is inactive the re-creation takes different steps than
    creating it from scratch.
    """
    previous_inactive_node = await Node.get_by_name(
        session,
        data.name,
        options=[
            joinedload(Node.current).options(*NodeRevision.default_load_options()),
        ],
        raise_if_not_exists=False,
        include_inactive=True,
    )
    if previous_inactive_node and previous_inactive_node.deactivated_at:
        if previous_inactive_node.type != new_node_type:
            raise DJInvalidInputException(  # pragma: no cover
                message=f"A node with name `{data.name}` of a `{previous_inactive_node.type.value}` "
                "type existed before. If you want to re-create it with a different type, "
                "you need to remove all traces of the previous node with a hard delete call: "
                "DELETE /nodes/{node_name}/hard",
                http_status_code=HTTPStatus.CONFLICT,
            )
        if new_node_type != NodeType.CUBE:
            update_node = UpdateNode(
                # MutableNodeFields
                display_name=data.display_name,
                description=data.description,
                mode=data.mode,
            )
            if isinstance(data, CreateSourceNode):
                update_node.catalog = data.catalog
                update_node.schema_ = data.schema_
                update_node.table = data.table
                update_node.columns = data.columns
                update_node.missing_table = data.missing_table

            if isinstance(data, CreateNode):
                update_node.query = data.query

            await update_node_with_query(
                name=data.name,
                data=update_node,
                session=session,
                request_headers=request_headers,
                query_service_client=query_service_client,
                current_user=current_user,
                background_tasks=background_tasks,
                validate_access=validate_access,  # type: ignore
                save_history=save_history,
                cache=cache,
            )
        else:
            await update_cube_node(
                session,
                previous_inactive_node.current,
                data,
                request_headers=request_headers,
                query_service_client=query_service_client,
                current_user=current_user,
                background_tasks=background_tasks,
                validate_access=validate_access,  # type: ignore
                save_history=save_history,
            )
        try:
            await activate_node(
                name=data.name,
                session=session,
                current_user=current_user,
                save_history=save_history,
            )
            return await get_node_by_name(session, data.name, with_current=True)
        except Exception as exc:  # pragma: no cover
            raise DJException(
                f"Restoring node `{data.name}` failed: {exc}",
            ) from exc

    return None


async def create_new_revision_from_existing(
    session: AsyncSession,
    old_revision: NodeRevision,
    node: Node,
    current_user: User,
    data: UpdateNode = None,
    version_upgrade: VersionUpgrade = None,
) -> Optional[NodeRevision]:
    """
    Creates a new revision from an existing node revision.
    """
    # minor changes
    minor_changes = (
        (data and data.description and old_revision.description != data.description)
        or (data and data.mode and old_revision.mode != data.mode)
        or (
            data
            and data.display_name
            and old_revision.display_name != data.display_name
        )
        or (
            data
            and data.metric_metadata
            and old_revision.metric_metadata != data.metric_metadata
        )
        or (
            data
            and data.custom_metadata is not None
            and old_revision.custom_metadata != data.custom_metadata
        )
    )

    # major changes
    query_changes = (
        old_revision.type != NodeType.SOURCE
        and data
        and data.query
        and old_revision.query != data.query
    )
    column_changes = (
        old_revision.type == NodeType.SOURCE
        and data
        and data.columns
        and ({col.identifier() for col in old_revision.columns} != data.columns)
    )
    pk_changes = data and (
        data.primary_key == []
        and len(old_revision.primary_key())
        or {col.name for col in old_revision.primary_key()}
        != set(data.primary_key or [])
    )
    required_dim_changes = (
        data
        and isinstance(data.required_dimensions, list)
        and {col.name for col in old_revision.required_dimensions}
        != set(data.required_dimensions)
    )
    major_changes = (
        query_changes or column_changes or pk_changes or required_dim_changes
    )

    # If nothing has changed, do not create the new node revision
    if not minor_changes and not major_changes and not version_upgrade:
        return None

    old_version = Version.parse(node.current_version)
    new_mode = data.mode if data and data.mode else old_revision.mode
    new_revision = NodeRevision(
        name=old_revision.name,
        node_id=node.id,
        version=str(
            old_version.next_major_version()
            if major_changes or version_upgrade == VersionUpgrade.MAJOR
            else old_version.next_minor_version(),
        ),
        display_name=(
            data.display_name
            if data and data.display_name
            else old_revision.display_name
        ),
        description=(
            data.description if data and data.description else old_revision.description
        ),
        query=(data.query if data and data.query else old_revision.query),
        type=old_revision.type,
        columns=[
            Column(
                name=column_data.name.lower()
                if node.type != NodeType.METRIC
                else column_data.name,
                type=column_data.type,
                dimension_column=column_data.dimension,
                attributes=column_data.attributes or [],
                order=idx,
            )
            for idx, column_data in enumerate(data.columns)
        ]
        if data and data.columns
        else [col.copy() for col in old_revision.columns],
        catalog=old_revision.catalog,
        schema_=old_revision.schema_,
        table=old_revision.table,
        parents=[],
        mode=new_mode,
        materializations=[],
        status=old_revision.status,
        metric_metadata=(
            MetricMetadata.from_input(data.metric_metadata)
            if data and data.metric_metadata
            else old_revision.metric_metadata
        ),
        dimension_links=[
            DimensionLink(
                dimension_id=link.dimension_id,
                join_sql=link.join_sql,
                join_type=link.join_type,
                join_cardinality=link.join_cardinality,
                materialization_conf=link.materialization_conf,
            )
            for link in old_revision.dimension_links
        ],
        created_by_id=current_user.id,
        custom_metadata=old_revision.custom_metadata,
    )

    # Set the node_revision relationship on copied columns
    for col in new_revision.columns:
        col.node_revision = new_revision

    if data and data.required_dimensions is not None:  # type: ignore
        new_revision.required_dimensions = data.required_dimensions  # type: ignore

    if data and data.custom_metadata is not None:  # type: ignore
        new_revision.custom_metadata = data.custom_metadata  # type: ignore

    # Link the new revision to its parents if a new revision was created and update its status
    if new_revision.type != NodeType.SOURCE:
        node_validator = await validate_node_data(new_revision, session)
        new_revision.columns = node_validator.columns
        new_revision.status = node_validator.status

        # Re-establish column relationships after validation overwrites them
        for col in new_revision.columns:
            col.node_revision = new_revision
        if node_validator.errors:
            if new_mode == NodeMode.DRAFT:
                new_revision.status = NodeStatus.INVALID
            else:
                raise DJException(
                    http_status_code=HTTPStatus.BAD_REQUEST,
                    errors=node_validator.errors,
                )

        # Update dimension links based on new columns
        new_column_names = {col.name for col in new_revision.columns}
        new_revision.dimension_links = [
            link
            for link in old_revision.dimension_links
            if link.foreign_key_column_names.intersection(new_column_names)
        ]

        new_parents = [n.name for n in node_validator.dependencies_map]
        parent_refs = (
            (
                await session.execute(
                    select(Node)
                    .where(
                        Node.name.in_(  # type: ignore
                            new_parents,
                        ),
                    )
                    .options(joinedload(Node.current)),
                )
            )
            .unique()
            .scalars()
            .all()
        )
        new_revision.parents = list(parent_refs)
        catalogs = [
            parent.current.catalog_id
            for parent in parent_refs
            if parent.current.catalog_id
        ]
        if catalogs:
            new_revision.catalog_id = catalogs[0]
        new_revision.columns = node_validator.columns or []
        if new_revision.type == NodeType.METRIC and new_revision.columns:
            new_revision.columns[0].display_name = new_revision.display_name

        # Update the primary key if one was set in the input
        if data is not None and (data.primary_key or data.primary_key == []):
            pk_attribute = (
                await session.execute(
                    select(AttributeType).where(
                        AttributeType.name == ColumnAttributes.PRIMARY_KEY.value,
                    ),
                )
            ).scalar_one()
            if set(data.primary_key) - set(col.name for col in new_revision.columns):
                raise DJInvalidInputException(  # pragma: no cover
                    f"Primary key {data.primary_key} does not exist on {new_revision.name}",
                )
            for col in new_revision.columns:
                # Remove the primary key attribute if it's not in the updated PK
                if col.has_primary_key_attribute() and col.name not in data.primary_key:
                    col.attributes = [
                        attr
                        for attr in col.attributes
                        if attr.attribute_type.name
                        != ColumnAttributes.PRIMARY_KEY.value
                    ]
                # Add (or keep) the primary key attribute if it is in the updated PK
                if col.name in data.primary_key and not col.has_primary_key_attribute():
                    col.attributes.append(
                        ColumnAttribute(column=col, attribute_type=pk_attribute),
                    )

        # Update the required dimensions if one was set in the input and the node is a metric
        if node_validator.required_dimensions and new_revision.type == NodeType.METRIC:
            new_revision.required_dimensions = node_validator.required_dimensions

        # Set the node's validity status
        invalid_primary_key = (
            new_revision.type == NodeType.DIMENSION and not new_revision.primary_key()
        )
        if invalid_primary_key:
            new_revision.status = NodeStatus.INVALID

        new_revision.missing_parents = [
            MissingParent(name=missing_parent)
            for missing_parent in node_validator.missing_parents_map
        ]
        _logger.info(
            "Parent nodes for %s (v%s): %s",
            new_revision.name,
            new_revision.version,
            [p.name for p in new_revision.parents],
        )
    return new_revision


async def save_column_level_lineage(node_revision_id: int):
    """
    Saves the column-level lineage for a node revision
    """
    async with session_context() as session:
        statement = (
            select(NodeRevision)
            .where(NodeRevision.id == node_revision_id)
            .options(
                selectinload(NodeRevision.columns),
            )
        )
        node_revision = (await session.execute(statement)).unique().scalar_one_or_none()
        if node_revision:  # pragma: no cover
            column_level_lineage = await get_column_level_lineage(
                session,
                node_revision,
            )
            node_revision.lineage = [
                lineage.model_dump() for lineage in column_level_lineage
            ]
            session.add(node_revision)
            await session.commit()


async def save_query_ast(  # pragma: no cover
    session: AsyncSession,
    node_name: str,
):
    """
    Compile and save query AST for a node
    """
    node = await Node.get_by_name(session, node_name)

    if (
        node
        and node.current.query
        and node.type
        in {
            NodeType.TRANSFORM,
            NodeType.DIMENSION,
        }
    ):
        node.current.query_ast = await compile_node_ast(session, node.current)

    session.add(node.current)  # type: ignore
    await session.commit()


async def get_column_level_lineage(
    session: AsyncSession,
    node_revision: NodeRevision,
) -> List[LineageColumn]:
    """
    Gets the column-level lineage for the node
    """
    if node_revision.status == NodeStatus.VALID and node_revision.type not in (
        NodeType.SOURCE,
        NodeType.CUBE,
    ):
        return [
            await column_lineage(
                session,
                node_revision,
                col.name,
            )
            for col in node_revision.columns
        ]
    return []


async def column_lineage(
    session: AsyncSession,
    node_rev: NodeRevision,
    column_name: str,
) -> LineageColumn:
    """
    Helper function to determine the lineage for a column on a node.
    """
    if node_rev.type == NodeType.SOURCE:
        return LineageColumn(
            node_name=node_rev.name,
            node_type=node_rev.type,
            display_name=node_rev.display_name,
            column_name=column_name,
            lineage=[],
        )

    ctx = CompileContext(session, DJException())
    query = (
        NodeRevision.format_metric_alias(
            node_rev.query,  # type: ignore
            node_rev.name,
        )
        if node_rev.type == NodeType.METRIC
        else node_rev.query
    )
    query_ast = parse(query)
    await query_ast.compile(ctx)
    query_ast.select.add_aliases_to_unnamed_columns()

    lineage_column = LineageColumn(
        column_name=column_name,
        node_name=node_rev.name,
        node_type=node_rev.type,
        display_name=node_rev.display_name,
        lineage=[],
    )

    # Find the expression AST for the column on the node
    column = [
        col
        for col in query_ast.select.projection
        if (  # pragma: no cover
            col != ast.Null() and col.alias_or_name.name.lower() == column_name.lower()  # type: ignore
        )
    ][0]
    column_or_child = column.child if isinstance(column, ast.Alias) else column  # type: ignore
    column_expr = (
        column_or_child.expression  # type: ignore
        if hasattr(column_or_child, "expression")
        else column_or_child
    )

    # At every layer, expand the lineage search tree with all columns referenced
    # by the current column's expression. If we reach an actual table with a DJ
    # node attached, save this to the lineage record. Otherwise, continue the search
    processed = list(column_expr.find_all(ast.Column)) if column_expr else []
    seen = set()
    while processed:
        current = processed.pop()
        if current in seen:
            continue
        if (
            hasattr(current, "table")
            and isinstance(current.table, ast.Table)
            and current.table.dj_node
        ):
            lineage_column.lineage.append(  # type: ignore
                await column_lineage(
                    session,
                    current.table.dj_node,
                    current.name.name
                    if not current.is_struct_ref
                    else current.struct_column_name,
                ),
            )
        else:
            expr_column_deps = (
                list(
                    current.expression.find_all(ast.Column),
                )
                if current.expression
                else []
            )
            for col_dep in expr_column_deps:
                processed.append(col_dep)
        seen.update({current})
    return lineage_column


async def get_single_cube_revision_metadata(
    session: AsyncSession,
    name: str,
    version: str | None = None,
) -> CubeRevisionMetadata:
    """
    Returns cube revision metadata for a single cube named `name`.
    """
    cube = await NodeRevision.get_cube_revision(session, name=name, version=version)
    if not cube:
        raise DJNodeNotFound(  # pragma: no cover
            message=(
                f"A cube node with name `{name}` does not exist."
                if not version
                else f"A cube node with name `{name}` and version `{version}` does not exist."
            ),
            http_status_code=404,
        )
    return CubeRevisionMetadata.from_cube_revision(cube)


async def get_all_cube_revisions_metadata(
    session: AsyncSession,
    catalog: str | None = None,
    page: int = 1,
    page_size: int = 10,
) -> list[CubeRevisionMetadata]:
    """
    Returns cube revision metadata for the latest version of all cubes, with pagination.
    Optionally filters by the catalog in which the cube is available.
    """
    cubes = await NodeRevision.get_cube_revisions(
        session=session,
        catalog=catalog,
        page=page,
        page_size=page_size,
    )
    return [CubeRevisionMetadata.from_cube_revision(cube) for cube in cubes]


async def upsert_complex_dimension_link(
    session: AsyncSession,
    node_name: str,
    link_input: JoinLinkInput,
    current_user: User,
    save_history: Callable,
) -> ActivityType:
    """
    Create or update a node-level dimension link.

    A dimension link is uniquely identified by the origin node, the dimension node being linked,
    and the role, if any. If an existing dimension link identified by those fields already exists,
    we'll update that dimension link. If no dimension link exists, we'll create a new one.
    """
    node = await Node.get_by_name(
        session,
        node_name,
        raise_if_not_exists=True,
    )
    if node.type not in (NodeType.SOURCE, NodeType.DIMENSION, NodeType.TRANSFORM):  # type: ignore
        raise DJInvalidInputException(
            message=f"Cannot link dimension to a node of type {node.type}. "  # type: ignore
            "Must be a source, dimension, or transform node.",
        )

    # Find the dimension node and check that the catalogs match
    dimension_node = await Node.get_by_name(
        session,
        link_input.dimension_node,
        raise_if_not_exists=True,
    )
    if (
        dimension_node.current.catalog.name != settings.seed_setup.virtual_catalog_name  # type: ignore
        and dimension_node.current.catalog is not None  # type: ignore
        and node.current.catalog.name != dimension_node.current.catalog.name  # type: ignore
    ):
        raise DJException(  # pragma: no cover
            message=(
                "Cannot link dimension to node, because catalogs do not match: "
                f"{node.current.catalog.name}, "  # type: ignore
                f"{dimension_node.current.catalog.name}"  # type: ignore
            ),
        )

    # Parse the join query and do some basic verification of its validity
    join_query = parse(
        f"SELECT 1 FROM {node_name} "
        f"{link_input.join_type} JOIN {link_input.dimension_node} "
        + (f"ON {link_input.join_on}" if link_input.join_on else ""),
    )
    exc = DJException()
    ctx = ast.CompileContext(session=session, exception=exc)
    await join_query.compile(ctx)
    join_relation = join_query.select.from_.relations[0].extensions[0]  # type: ignore

    # Verify that the query references both the node and the dimension being joined
    expected_references = {node_name, link_input.dimension_node}
    references = (
        {
            table.name.namespace.identifier()  # type: ignore
            for table in join_relation.criteria.on.find_all(ast.Column)  # type: ignore
        }
        if join_relation.criteria
        else {}
    )
    if (
        expected_references.difference(references)
        and link_input.join_type != JoinType.CROSS
    ):
        raise DJInvalidInputException(
            f"The join SQL provided does not reference both the origin node {node_name} and the "
            f"dimension node {link_input.dimension_node} that it's being joined to.",
        )

    # Verify that the columns in the ON clause exist on both nodes
    if ctx.exception.errors:
        raise DJInvalidInputException(
            message=f"Join query {link_input.join_on} is not valid",
            errors=ctx.exception.errors,
        )

    # Create a new revision for the node to capture the dimension link changes in a new version
    node = cast(Node, node)
    new_revision = await create_new_revision_for_dimension_link_update(
        session,
        node,
        current_user,
    )

    # Find an existing dimension link if there is already one defined for this node
    existing_link = [
        link  # type: ignore
        for link in new_revision.dimension_links  # type: ignore
        if link.dimension_id == dimension_node.id and link.role == link_input.role  # type: ignore
    ]
    activity_type = ActivityType.CREATE

    if existing_link:
        # Update the existing dimension link
        activity_type = ActivityType.UPDATE
        dimension_link = existing_link[0]
        dimension_link.join_sql = link_input.join_on
        dimension_link.join_type = DimensionLink.parse_join_type(
            join_relation.join_type,
        )
        dimension_link.join_cardinality = link_input.join_cardinality
    else:
        # If there is no existing link, create new dimension link object
        dimension_link = DimensionLink(
            node_revision_id=new_revision.id,  # type: ignore
            dimension_id=dimension_node.id,  # type: ignore
            join_sql=link_input.join_on,
            join_type=DimensionLink.parse_join_type(join_relation.join_type),
            join_cardinality=link_input.join_cardinality,
            role=link_input.role,
        )
        new_revision.dimension_links.append(dimension_link)  # type: ignore

    # Add/update the dimension link in the database
    session.add(dimension_link)
    session.add(new_revision)  # type: ignore
    await save_history(
        event=History(
            entity_type=EntityType.LINK,
            entity_name=node.name,  # type: ignore
            node=node.name,  # type: ignore
            activity_type=activity_type,
            details={
                "dimension": dimension_node.name,  # type: ignore
                "join_sql": link_input.join_on,
                "join_cardinality": link_input.join_cardinality,
                "role": link_input.role,
                "version": node.current_version,
            },
            user=current_user.username,
        ),
        session=session,
    )
    await session.commit()
    await session.refresh(node)
    return activity_type


async def upsert_simple_dimension_link(
    session: AsyncSession,
    name: str,
    dimension: str,
    column: str,
    dimension_column: str | None,
    current_user: User,
    save_history: Callable,
) -> ActivityType:
    """
    Create or update a simple node-level dimension link on a single column foreign key.
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
    return await upsert_complex_dimension_link(
        session,
        name,
        link_input,
        current_user,
        save_history,
    )


async def remove_dimension_link(
    session: AsyncSession,
    node_name: str,
    link_identifier: LinkDimensionIdentifier,
    current_user: User,
    save_history: Callable,
):
    """
    Removes the dimension link identified by the origin node, the dimension node, and its role.
    """
    node = await Node.get_by_name(session, node_name)

    # Find the dimension node
    dimension_node = await get_node_by_name(
        session=session,
        name=link_identifier.dimension_node,
        node_type=NodeType.DIMENSION,
        include_inactive=True,
    )
    removed = False

    # Find cubes that are affected by this dimension link removal and update their statuses
    downstream_cubes = await get_downstream_nodes(
        session,
        node_name,
        node_type=NodeType.CUBE,
    )
    for cube in downstream_cubes:
        cube = cast(Node, await Node.get_cube_by_name(session, cube.name))
        cube_dimension_nodes = [
            cube_elem_node.name
            for (element, cube_elem_node) in cube.current.cube_elements_with_nodes()
            if cube_elem_node.type == NodeType.DIMENSION
        ]
        if dimension_node.name in cube_dimension_nodes:
            cube.current.status = NodeStatus.INVALID
            session.add(cube)
            await save_history(
                event=status_change_history(
                    cube.current,  # type: ignore
                    NodeStatus.VALID,
                    NodeStatus.INVALID,
                    current_user=current_user,
                ),
                session=session,
            )
        await session.commit()

    # Create a new revision for dimension link removal
    node = cast(Node, node)
    new_revision = await create_new_revision_for_dimension_link_update(
        session,
        node,
        current_user,
    )

    # Delete the dimension link if one exists
    for link in new_revision.dimension_links:  # type: ignore
        if (
            link.dimension_id == dimension_node.id  # pragma: no cover
            and link.role == link_identifier.role  # pragma: no cover
        ):
            removed = True
            await session.delete(link)
    if not removed:
        return JSONResponse(
            status_code=HTTPStatus.NOT_FOUND,
            content={
                "message": f"Dimension link to node {link_identifier.dimension_node} "
                + (f"with role {link_identifier.role} " if link_identifier.role else "")
                + "not found",
            },
        )

    await save_history(
        event=History(
            entity_type=EntityType.LINK,
            entity_name=node.name,  # type: ignore
            node=node.name,  # type: ignore
            activity_type=ActivityType.DELETE,
            details={
                "version": node.current_version,
                "dimension": dimension_node.name,
                "role": link_identifier.role,
            },
            user=current_user.username,
        ),
        session=session,
    )
    await session.commit()
    await session.refresh(new_revision)  # type: ignore
    await session.refresh(node)
    return JSONResponse(
        status_code=201,
        content={
            "message": (
                f"Dimension link {link_identifier.dimension_node} "
                + (f"(role {link_identifier.role}) " if link_identifier.role else "")
                + f"to node {node_name} has been removed."
            )
            if removed
            else (
                f"Dimension link {link_identifier.dimension_node} "
                + (f"(role {link_identifier.role}) " if link_identifier.role else "")
                + f"to node {node_name} does not exist!"
            ),
        },
    )


async def upsert_reference_dimension_link(
    session: AsyncSession,
    node_name: str,
    node_column: str,
    dimension_node: str,
    dimension_column: str,
    role: str | None,
    current_user: User,
    save_history: Callable,
):
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


async def create_new_revision_for_dimension_link_update(
    session: AsyncSession,
    node: Node,
    current_user: User,
) -> NodeRevision:
    """
    Create a new revision for the node to capture the dimension link changes in a new version
    """
    new_revision = copy_existing_node_revision(node.current, current_user)
    new_revision.version = str(Version.parse(node.current_version).next_minor_version())
    node.current_version = new_revision.version
    new_revision.node = node

    session.add(new_revision)
    await session.commit()
    await session.refresh(new_revision)
    await session.refresh(new_revision, ["dimension_links"])

    return new_revision


async def propagate_valid_status(
    session: AsyncSession,
    valid_nodes: List[NodeRevision],
    catalog_id: int,
    current_user: User,
    save_history: Callable,
) -> None:
    """
    Propagate a valid status by revalidating all downstream nodes
    """
    while valid_nodes:
        resolved_nodes = []
        for node_revision in valid_nodes:
            if node_revision.status != NodeStatus.VALID:
                raise DJException(
                    f"Cannot propagate valid status: Node `{node_revision.name}` is not valid",
                )
            downstream_nodes = await get_downstream_nodes(
                session=session,
                node_name=node_revision.name,
            )
            newly_valid_nodes = []
            for node in downstream_nodes:
                node_validator = await validate_node_data(
                    data=node.current,
                    session=session,
                )
                node.current.status = node_validator.status
                if node_validator.status == NodeStatus.VALID:
                    node.current.columns = node_validator.columns or []
                    node.current.status = NodeStatus.VALID
                    node.current.catalog_id = catalog_id
                    await save_history(
                        event=status_change_history(
                            node.current,
                            NodeStatus.INVALID,
                            NodeStatus.VALID,
                            current_user=current_user,
                        ),
                        session=session,
                    )
                    newly_valid_nodes.append(node.current)
                session.add(node.current)
                await session.commit()
                await session.refresh(node.current)
            resolved_nodes.extend(newly_valid_nodes)
        valid_nodes = resolved_nodes


async def deactivate_node(
    session: AsyncSession,
    name: str,
    current_user: User,
    save_history: Callable,
    query_service_client: QueryServiceClient,
    background_tasks: BackgroundTasks,
    request_headers: Dict[str, str] = None,
    message: str = None,
):
    """
    Deactivates a node and propagates to all downstreams.
    """
    node = await Node.get_by_name(session, name)

    # Find all downstream nodes and mark them as invalid
    downstreams = await get_downstream_nodes(session, name)
    for downstream in downstreams:
        if downstream.current.status != NodeStatus.INVALID:
            downstream.current.status = NodeStatus.INVALID
            await save_history(
                event=status_change_history(
                    downstream.current,
                    NodeStatus.VALID,
                    NodeStatus.INVALID,
                    parent_node=name,
                    current_user=current_user,
                ),
                session=session,
            )
            session.add(downstream)

    # If the node has materializations, deactivate them
    for materialization in (
        node.current.materializations if node and node.current else []
    ):
        background_tasks.add_task(
            query_service_client.deactivate_materialization,
            node_name=name,
            materialization_name=materialization.name,
            request_headers=request_headers,
        )

    now = datetime.now(timezone.utc)
    node.deactivated_at = UTCDatetime(  # type: ignore
        year=now.year,
        month=now.month,
        day=now.day,
        hour=now.hour,
        minute=now.minute,
        second=now.second,
    )
    session.add(node)
    await save_history(
        event=History(
            entity_type=EntityType.NODE,
            entity_name=name,
            node=name,
            activity_type=ActivityType.DELETE,
            details={"message": message} if message else {},
            user=current_user.username,
        ),
        session=session,
    )
    await session.commit()
    await session.refresh(node, ["current"])


async def activate_node(
    session: AsyncSession,
    name: str,
    current_user: User,
    save_history: Callable,
    message: str = None,
):
    """Restores node and revalidate all downstreams."""
    node = await get_node_by_name(
        session,
        name,
        with_current=True,
        include_inactive=True,
    )
    if not node.deactivated_at:
        raise DJInvalidInputException(
            http_status_code=HTTPStatus.BAD_REQUEST,
            message=f"Cannot restore `{name}`, node already active.",
        )
    node.deactivated_at = None  # type: ignore

    # Find all downstream nodes and revalidate them
    downstreams = await get_downstream_nodes(session, node.name)
    for downstream in downstreams:
        old_status = downstream.current.status
        if downstream.type == NodeType.CUBE:
            downstream.current.status = NodeStatus.VALID
            for element in downstream.current.cube_elements:
                await session.refresh(element, ["node_revision"])
                if (
                    element.node_revision
                    and element.node_revision.status == NodeStatus.INVALID
                ):
                    downstream.current.status = NodeStatus.INVALID  # pragma: no cover
        else:
            # We should not fail node restoration just because of some nodes
            # that have been invalid already and stay that way.
            node_validator = await validate_node_data(downstream.current, session)
            downstream.current.status = node_validator.status
            if node_validator.errors:
                downstream.current.status = NodeStatus.INVALID
        session.add(downstream)
        if old_status != downstream.current.status:
            await save_history(
                event=status_change_history(
                    downstream.current,
                    old_status,
                    downstream.current.status,
                    parent_node=node.name,
                    current_user=current_user,
                ),
                session=session,
            )

    session.add(node)
    await save_history(
        event=History(
            entity_type=EntityType.NODE,
            entity_name=node.name,
            node=node.name,
            activity_type=ActivityType.RESTORE,
            details={"message": message} if message else {},
            user=current_user.username,
        ),
        session=session,
    )
    await session.commit()


async def revalidate_node(
    name: str,
    session: AsyncSession,
    current_user: User,
    save_history: Callable,
    update_query_ast: bool = False,
    background_tasks: BackgroundTasks = None,
) -> NodeValidator:
    """
    Revalidate a single existing node and update its status appropriately
    """
    node = await Node.get_by_name(
        session,
        name,
        options=[
            joinedload(Node.current).options(*NodeRevision.default_load_options()),
            joinedload(Node.tags),
        ],
        raise_if_not_exists=True,
    )
    current_node_revision = node.current  # type: ignore

    # Revalidate source node
    if current_node_revision.type == NodeType.SOURCE:
        if current_node_revision.status != NodeStatus.VALID:  # pragma: no cover
            current_node_revision.status = NodeStatus.VALID
            await save_history(
                event=status_change_history(
                    current_node_revision,
                    NodeStatus.INVALID,
                    NodeStatus.VALID,
                    current_user=current_user,
                ),
                session=session,
            )
            session.add(current_node_revision)
            await session.commit()
            await session.refresh(current_node_revision)
        return NodeValidator(
            status=node.current.status,  # type: ignore
            columns=node.current.columns,  # type: ignore
        )

    # Revalidate cube node
    if current_node_revision.type == NodeType.CUBE:
        cube_node = await Node.get_cube_by_name(session, name)
        current_node_revision = cube_node.current  # type: ignore
        cube_metrics = [metric.name for metric in current_node_revision.cube_metrics()]
        cube_dimensions = current_node_revision.cube_dimensions()
        errors = []
        try:
            await validate_cube(
                session,
                metric_names=cube_metrics,
                dimension_names=cube_dimensions,
                require_dimensions=False,
            )
            current_node_revision.status = NodeStatus.VALID
        except DJException as exc:  # pragma: no cover
            current_node_revision.status = NodeStatus.INVALID
            if exc.errors:
                errors.extend(exc.errors)
            else:
                errors.append(
                    DJError(code=ErrorCode.INVALID_DIMENSION, message=exc.message),
                )
        session.add(current_node_revision)
        await session.commit()
        return NodeValidator(
            status=current_node_revision.status,
            columns=current_node_revision.columns,
            errors=errors,
        )

    # Revalidate all other node types
    node_validator = await validate_node_data(current_node_revision, session)

    # Compile and save query AST
    if update_query_ast and background_tasks:
        background_tasks.add_task(  # pragma: no cover
            save_query_ast,
            session=session,
            node_name=node.name,  # type: ignore
        )

    # Update the status
    node.current.status = node_validator.status  # type: ignore

    existing_columns = {col.name: col for col in node.current.columns}  # type: ignore

    # Validate dimension links
    to_remove = set()
    for link in node.current.dimension_links:  # type: ignore
        if not link.foreign_key_column_names.intersection(set(existing_columns)):
            to_remove.add(link)  # pragma: no cover
            await session.delete(link)  # pragma: no cover

    # Check if any columns have been updated
    updated_columns = False
    for col in node_validator.columns:
        if existing_col := existing_columns.get(col.name):
            if existing_col.type != col.type:
                existing_col.type = col.type
                updated_columns = True
        else:
            node.current.columns.append(col)  # type: ignore  # pragma: no cover
            updated_columns = True  # pragma: no cover

    # Only create a new revision if the columns have been updated
    if updated_columns:  # type: ignore
        new_revision = copy_existing_node_revision(node.current, current_user)  # type: ignore
        new_revision.version = str(
            Version.parse(node.current.version).next_major_version(),  # type: ignore
        )

        new_revision.status = node_validator.status
        new_revision.lineage = [
            lineage.model_dump()
            for lineage in await get_column_level_lineage(session, new_revision)
        ]

        # Save which columns were modified and update the columns with the changes
        node_validator.updated_columns = node_validator.modified_columns(
            new_revision,  # type: ignore
        )
        new_revision.columns = node_validator.columns

        # Save the new revision of the child
        node.current_version = new_revision.version  # type: ignore
        new_revision.node_id = node.id  # type: ignore
        session.add(node)
        session.add(new_revision)
    await session.commit()
    await session.refresh(node.current)  # type: ignore
    await session.refresh(node, ["current"])
    return node_validator


async def hard_delete_node(
    name: str,
    session: AsyncSession,
    current_user: User,
    save_history: Callable,
):
    """
    Hard delete a node, destroying all links and invalidating all downstream nodes.
    This should be used with caution, deactivating a node is preferred.
    """
    node = await Node.get_by_name(
        session,
        name,
        options=[joinedload(Node.current), joinedload(Node.revisions)],
        include_inactive=True,
        raise_if_not_exists=True,
    )
    downstream_nodes = await get_downstream_nodes(session=session, node_name=name)

    linked_nodes = []
    if node.type == NodeType.DIMENSION:  # type: ignore
        linked_nodes = await get_nodes_with_dimension(
            session=session,
            dimension_node=node,  # type: ignore
        )

    await session.delete(node)
    await session.commit()
    impact = []  # Aggregate all impact of this deletion to include in response

    # Revalidate all downstream nodes
    for node in downstream_nodes:
        await save_history(  # Capture this in the downstream node's history
            event=History(
                entity_type=EntityType.DEPENDENCY,
                entity_name=name,
                node=node.name,
                activity_type=ActivityType.DELETE,
                user=current_user.username,
            ),
            session=session,
        )
        node_validator = await revalidate_node(
            name=node.name,
            session=session,
            current_user=current_user,
            save_history=save_history,
            update_query_ast=False,
        )
        impact.append(
            {
                "name": node.name,
                "status": node_validator.status if node_validator else "unknown",
                "effect": "downstream node is now invalid",
            },
        )

    # Revalidate all linked nodes
    for node in linked_nodes:
        await save_history(  # Capture this in the downstream node's history
            event=History(
                entity_type=EntityType.LINK,
                entity_name=name,
                node=node.name,
                activity_type=ActivityType.DELETE,
                user=current_user.username,
            ),
            session=session,
        )
        node_validator = await revalidate_node(
            name=node.name,
            session=session,
            current_user=current_user,
            save_history=save_history,
            update_query_ast=False,
        )
        impact.append(
            {
                "name": node.name,
                "status": node_validator.status,
                "effect": "broken link",
            },
        )
    await save_history(  # Capture this in the downstream node's history
        event=History(
            entity_type=EntityType.NODE,
            entity_name=name,
            node=node.name,
            activity_type=ActivityType.DELETE,
            details={
                "impact": impact,
            },
            user=current_user.username,
        ),
        session=session,
    )
    await session.commit()  # Commit the history events
    return impact


async def refresh_source(
    name: str,
    session: AsyncSession,
    current_user: User,
    save_history: Callable,
    query_service_client: QueryServiceClient,
    request: Request,
):
    request_headers = dict(request.headers)
    source_node = await Node.get_by_name(
        session,
        name,
        options=NodeOutput.load_options(),
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
            node_revision=new_revision,
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
        options=NodeOutput.load_options(),
    )
    await session.refresh(source_node, ["current"])
    return source_node
