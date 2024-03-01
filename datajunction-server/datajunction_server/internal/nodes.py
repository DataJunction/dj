# pylint: disable=too-many-lines,too-many-arguments
"""Nodes endpoint helper functions"""
import logging
from collections import defaultdict, deque
from http import HTTPStatus
from typing import Any, Dict, List, Optional, Union

from fastapi import BackgroundTasks
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from datajunction_server.api.catalogs import UNKNOWN_CATALOG_ID
from datajunction_server.api.helpers import (
    activate_node,
    get_attribute_type,
    get_node_by_name,
    map_dimensions_to_roles,
    propagate_valid_status,
    resolve_downstream_references,
    validate_cube,
    validate_node_data,
)
from datajunction_server.database.attributetype import AttributeType, ColumnAttribute
from datajunction_server.database.column import Column
from datajunction_server.database.dimensionlink import DimensionLink
from datajunction_server.database.history import ActivityType, EntityType, History
from datajunction_server.database.materialization import Materialization
from datajunction_server.database.metricmetadata import MetricMetadata
from datajunction_server.database.node import MissingParent, Node, NodeRevision
from datajunction_server.database.partition import Partition
from datajunction_server.database.user import User
from datajunction_server.errors import (
    DJDoesNotExistException,
    DJException,
    DJInvalidInputException,
    DJNodeNotFound,
)
from datajunction_server.internal.materializations import (
    create_new_materialization,
    schedule_materialization_jobs,
)
from datajunction_server.models import access
from datajunction_server.models.attribute import (
    AttributeTypeIdentifier,
    UniquenessScope,
)
from datajunction_server.models.base import labelize
from datajunction_server.models.cube import CubeRevisionMetadata
from datajunction_server.models.dimensionlink import (
    LinkDimensionIdentifier,
    LinkDimensionInput,
)
from datajunction_server.models.history import status_change_history
from datajunction_server.models.materialization import (
    MaterializationConfigOutput,
    MaterializationJobTypeEnum,
    UpsertMaterialization,
)
from datajunction_server.models.node import (
    DEFAULT_DRAFT_VERSION,
    DEFAULT_PUBLISHED_VERSION,
    CreateCubeNode,
    CreateNode,
    CreateSourceNode,
    LineageColumn,
    NodeMode,
    NodeStatus,
    UpdateNode,
)
from datajunction_server.models.node_type import NodeType
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.sql.dag import get_nodes_with_dimension
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.ast import CompileContext
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.utils import Version, VersionUpgrade

_logger = logging.getLogger(__name__)


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


def validate_and_build_attribute(
    session: Session,
    column: Column,
    attribute: AttributeTypeIdentifier,
    node: Node,
) -> ColumnAttribute:
    """
    Run some validation and build column attribute.
    """
    # Verify attribute type exists
    attribute_type = get_attribute_type(
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
        raise DJException(
            message=f"Attribute type `{attribute.namespace}.{attribute_type.name}` "
            f"not allowed on node type `{node.type}`!",
        )

    return ColumnAttribute(
        attribute_type=attribute_type,
        column=column,
    )


def set_node_column_attributes(
    session: Session,
    node: Node,
    column_name: str,
    attributes: List[AttributeTypeIdentifier],
    current_user: Optional[User] = None,
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
    column.attributes = []
    for attribute in attributes:
        if attribute.name in existing_attributes_map:
            column.attributes.append(existing_attributes_map[attribute.name])
        else:
            column.attributes.append(
                validate_and_build_attribute(session, column, attribute, node),
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
            raise DJException(
                message=f"The column attribute `{attribute.name}` is scoped to be "
                f"unique to the `{attribute.uniqueness_scope}` level, but there "
                "is more than one column tagged with it: "
                f"`{', '.join(sorted(list(columns)))}`",
            )

    session.add(column)
    session.add(
        History(
            entity_type=EntityType.COLUMN_ATTRIBUTE,
            node=node.name,
            activity_type=ActivityType.SET_ATTRIBUTE,
            details={
                "column": column.name,
                "attributes": [attr.dict() for attr in attributes],
            },
            user=current_user.username if current_user else None,
        ),
    )
    session.commit()
    session.refresh(column)

    session.refresh(node)
    session.refresh(node.current)
    return [column]


def create_node_revision(
    data: CreateNode,
    node_type: NodeType,
    session: Session,
) -> NodeRevision:
    """
    Create a non-source node revision.
    """
    node_revision = NodeRevision(
        name=data.name,
        display_name=data.display_name if data.display_name else labelize(data.name),
        description=data.description,
        type=node_type,
        status=NodeStatus.VALID,
        query=data.query,
        mode=data.mode,
        required_dimensions=data.required_dimensions or [],
    )
    node_validator = validate_node_data(node_revision, session)
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
        node.catalog_id for node in node_validator.dependencies_map if node.catalog_id
    ]
    if node_revision.mode == NodeMode.PUBLISHED and not len(set(catalog_ids)) <= 1:
        raise DJException(
            f"Cannot create nodes with multi-catalog dependencies: {set(catalog_ids)}",
        )
    catalog_id = next(iter(catalog_ids), 0)
    parent_refs = (
        session.execute(
            select(Node).where(
                # pylint: disable=no-member
                Node.name.in_(  # type: ignore
                    new_parents,
                ),
            ),
        )
        .scalars()
        .all()
    )
    node_revision.parents = parent_refs

    _logger.info(
        "Parent nodes for %s (%s): %s",
        data.name,
        node_revision.version,
        [p.name for p in node_revision.parents],
    )
    node_revision.columns = node_validator.columns or []
    if node_revision.type == NodeType.METRIC:
        if node_revision.columns:
            node_revision.columns[0].display_name = node_revision.display_name
    node_revision.catalog_id = catalog_id
    return node_revision


def create_cube_node_revision(  # pylint: disable=too-many-locals
    session: Session,
    data: CreateCubeNode,
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
    ) = validate_cube(
        session,
        data.metrics,
        data.dimensions,
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
    dimension_to_roles_mapping = map_dimensions_to_roles(data.dimensions)
    for idx, col in enumerate(metric_columns + dimension_columns):
        referenced_node = col.node_revision()
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
        display_name=data.display_name or labelize(data.name),
        description=data.description,
        type=NodeType.CUBE,
        query="",
        columns=node_columns,
        cube_elements=metric_columns + dimension_columns,
        parents=list(set(dimension_nodes + metric_nodes)),
        status=status,
        catalog=catalog,
    )
    return node_revision


def save_node(
    session: Session,
    node_revision: NodeRevision,
    node: Node,
    node_mode: NodeMode,
    current_user: Optional[User] = None,
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

    session.add(node)
    session.add(
        History(
            node=node.name,
            entity_type=EntityType.NODE,
            entity_name=node.name,
            activity_type=ActivityType.CREATE,
            user=current_user.username if current_user else None,
        ),
    )
    session.commit()

    newly_valid_nodes = resolve_downstream_references(
        session=session,
        node_revision=node_revision,
    )
    propagate_valid_status(
        session=session,
        valid_nodes=newly_valid_nodes,
        catalog_id=node.current.catalog_id,  # pylint: disable=no-member
        current_user=current_user,
    )
    session.refresh(node.current)


def copy_to_new_node(
    session: Session,
    node: Node,
    new_name: str,
    current_user: Optional[User] = None,
) -> Node:
    """
    Copies the existing node to a new node with a new name.
    """
    old_revision = node.current
    new_node = Node(
        name=new_name,
        type=node.type,
        display_name=node.display_name,
        namespace=".".join(new_name.split(".")[:-1]),
        current_version=node.current_version,
        created_at=node.created_at,
        deactivated_at=node.deactivated_at,
        tags=node.tags,
        missing_table=node.missing_table,
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
        required_dimensions=[col.copy() for col in old_revision.required_dimensions],
        metric_metadata=old_revision.metric_metadata,
        cube_elements=list(old_revision.cube_elements),
        status=old_revision.status,
        parents=old_revision.parents,
        missing_parents=[
            MissingParent(name=missing_parent.name)
            for missing_parent in old_revision.missing_parents
        ],
        columns=[col.copy() for col in old_revision.columns],
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
        # TODO: availability and materializations are missing here  # pylint: disable=fixme
        lineage=old_revision.lineage,
    )

    # Reset the version of the new node
    new_revision.version = (
        str(DEFAULT_DRAFT_VERSION)
        if new_revision.mode == NodeMode.DRAFT
        else str(DEFAULT_PUBLISHED_VERSION)
    )
    new_node.current_version = new_revision.version
    session.add(new_revision)
    session.add(new_node)

    # Add a history event recording the copy
    session.add(
        History(
            node=new_node.name,
            entity_type=EntityType.NODE,
            entity_name=new_node.name,
            activity_type=ActivityType.CREATE,
            user=current_user.username if current_user else None,
            details={"copied_from": node.name},
        ),
    )
    session.commit()

    # If the new node makes any downstream nodes valid, propagate
    newly_valid_nodes = resolve_downstream_references(
        session=session,
        node_revision=new_revision,
    )
    propagate_valid_status(
        session=session,
        valid_nodes=newly_valid_nodes,
        catalog_id=node.current.catalog_id,  # pylint: disable=no-member
        current_user=current_user,
    )
    session.refresh(node.current)
    return node


def update_any_node(
    name: str,
    data: UpdateNode,
    session: Session,
    query_service_client: QueryServiceClient,
    current_user: Optional[User] = None,
    background_tasks: BackgroundTasks = None,
    validate_access: access.ValidateAccessFn = None,
) -> Node:
    """
    Node update helper function that handles updating any node
    """
    node = get_node_by_name(session, name, for_update=True, include_inactive=True)
    if node.type == NodeType.CUBE:
        node_revision = update_cube_node(
            session,
            node.current,
            data,
            query_service_client=query_service_client,
            current_user=current_user,
            background_tasks=background_tasks,
            validate_access=validate_access,  # type: ignore
        )
        return node_revision.node if node_revision else node
    return update_node_with_query(
        name,
        data,
        session,
        query_service_client=query_service_client,
        current_user=current_user,
        background_tasks=background_tasks,
        validate_access=validate_access,  # type: ignore
    )


def update_node_with_query(
    name: str,
    data: UpdateNode,
    session: Session,
    *,
    query_service_client: QueryServiceClient,
    current_user: Optional[User] = None,
    background_tasks: BackgroundTasks,
    validate_access: access.ValidateAccessFn,
) -> Node:
    """
    Update the named node with the changes defined in the UpdateNode object.
    Propagate these changes to all of the node's downstream children.

    Note: this function works for both source nodes and nodes with query (transforms,
    dimensions, metrics). We should update it to separate out the logic for source nodes
    """
    node = get_node_by_name(session, name, for_update=True, include_inactive=True)
    old_revision = node.current
    new_revision = create_new_revision_from_existing(
        session,
        old_revision,
        node,
        data,
    )

    if not new_revision:
        return node  # type: ignore

    node.current_version = new_revision.version  # type: ignore

    new_revision.extra_validation()

    session.add(new_revision)
    session.add(node)
    session.add(node_update_history_event(new_revision, current_user))

    if new_revision.status != old_revision.status:  # type: ignore
        session.add(
            status_change_history(
                new_revision,  # type: ignore
                old_revision.status,
                new_revision.status,  # type: ignore
                current_user=current_user,
            ),
        )
    session.commit()

    session.refresh(new_revision)
    session.refresh(node)

    # Handle materializations: Note that this must be done after we commit the new revision,
    # as otherwise the SQL build won't know about the new revision's query
    active_materializations = [
        mat for mat in old_revision.materializations if not mat.deactivated_at
    ]
    if active_materializations and new_revision.query != old_revision.query:
        for old in active_materializations:
            new_revision.materializations.append(  # pylint: disable=no-member
                create_new_materialization(
                    session,
                    new_revision,
                    UpsertMaterialization(
                        name=old.name,
                        config=old.config,
                        schedule=old.schedule,
                        strategy=old.strategy,
                        job=MaterializationJobTypeEnum.find_match(old.job),
                    ),
                    validate_access,
                ),
            )
        background_tasks.add_task(
            schedule_materialization_jobs,
            materializations=node.current.materializations,
            query_service_client=query_service_client,
        )
        session.add(new_revision)
        session.commit()

    if background_tasks:
        background_tasks.add_task(
            save_column_level_lineage,
            session=session,
            node_revision=new_revision,
        )

    history_events = {}
    old_columns_map = {col.name: col.type for col in old_revision.columns}
    history_events[node.name] = {
        "name": node.name,
        "current_version": node.current_version,
        "previous_version": old_revision.version,
        "updated_columns": [
            col.name
            for col in new_revision.columns  # type: ignore
            if col.name not in old_columns_map or old_columns_map[col.name] != col.type
        ],
    }
    propagate_update_downstream(
        session,
        node,
        history_events,
        query_service_client=query_service_client,
        current_user=current_user,
        background_tasks=background_tasks,
        validate_access=validate_access,
    )

    session.refresh(node.current)
    return node


def has_minor_changes(
    old_revision: NodeRevision,
    data: UpdateNode,
):
    """
    Whether the node has minor changes
    """
    return (
        (data and data.description and old_revision.description != data.description)
        or (data and data.mode and old_revision.mode != data.mode)
        or (
            data
            and data.display_name
            and old_revision.display_name != data.display_name
        )
    )


def node_update_history_event(new_revision: NodeRevision, current_user: Optional[User]):
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
        user=current_user.username if current_user else None,
    )


def update_cube_node(  # pylint: disable=too-many-locals
    session: Session,
    node_revision: NodeRevision,
    data: UpdateNode,
    *,
    query_service_client: QueryServiceClient,
    current_user: Optional[User] = None,
    background_tasks: BackgroundTasks,
    validate_access: access.ValidateAccessFn,
) -> Optional[NodeRevision]:
    """
    Update cube node based on changes
    """
    minor_changes = has_minor_changes(node_revision, data)
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

    new_cube_revision = create_cube_node_revision(session, create_cube)

    old_version = Version.parse(node_revision.version)
    if major_changes:
        new_cube_revision.version = str(old_version.next_major_version())
    elif minor_changes:  # pragma: no cover
        new_cube_revision.version = str(old_version.next_minor_version())
    new_cube_revision.node = node_revision.node
    new_cube_revision.node.current_version = new_cube_revision.version  # type: ignore

    session.add(node_update_history_event(new_cube_revision, current_user))

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
    if major_changes and active_materializations:
        for old in active_materializations:
            new_cube_revision.materializations.append(  # pylint: disable=no-member
                create_new_materialization(
                    session,
                    new_cube_revision,
                    UpsertMaterialization(
                        **MaterializationConfigOutput.from_orm(old).dict(
                            exclude={"job"},
                        ),
                        job=MaterializationJobTypeEnum.find_match(old.job),
                    ),
                    validate_access,
                ),
            )
            session.add(
                History(
                    entity_type=EntityType.MATERIALIZATION,
                    entity_name=old.name,
                    node=node_revision.name,
                    activity_type=ActivityType.UPDATE,
                    details={},
                    user=current_user.username if current_user else None,
                ),
            )
        background_tasks.add_task(
            schedule_materialization_jobs,
            materializations=new_cube_revision.materializations,
            query_service_client=query_service_client,
        )
    session.add(new_cube_revision)
    session.add(new_cube_revision.node)
    session.commit()

    session.refresh(new_cube_revision)
    session.refresh(new_cube_revision.node)
    session.refresh(new_cube_revision.node.current)
    return new_cube_revision


def propagate_update_downstream(  # pylint: disable=too-many-locals
    session: Session,
    node: Node,
    history_events: Dict[str, Any],
    *,
    query_service_client: QueryServiceClient,
    current_user: Optional[User] = None,
    background_tasks: BackgroundTasks,
    validate_access: access.ValidateAccessFn,
):
    """
    Propagate the updated node's changes to all of its downstream children.
    Some potential changes to the upstream node and their effects on downstream nodes:
    - altered column names: may invalidate downstream nodes
    - altered column types: may invalidate downstream nodes
    - new columns: won't affect downstream nodes
    """
    processed = set()

    # Each entry being processed is a list that represents the changelog of affected nodes
    # The last entry in the list is the current node that's being processed
    to_process = deque([[node.current, child.node.current] for child in node.children])

    while to_process:
        changelog = to_process.popleft()
        child = changelog[-1]

        # Only process if it hasn't already been processed before
        if child.name not in processed:
            processed.add(child.name)

            if child.type == NodeType.CUBE:
                update_cube_node(
                    session,
                    child,
                    UpdateNode(
                        metrics=[metric.name for metric in child.cube_metrics()],
                        dimensions=child.cube_dimensions(),
                    ),
                    query_service_client=query_service_client,
                    background_tasks=background_tasks,
                    validate_access=validate_access,
                )
                continue

            node_validator = validate_node_data(
                data=child,
                session=session,
            )

            # Update the child with a new node revision if its columns or status have changed
            if node_validator.differs_from(child):
                new_revision = copy_existing_node_revision(child)
                new_revision.version = str(
                    Version.parse(child.version).next_major_version(),
                )

                new_revision.status = node_validator.status
                new_revision.lineage = [
                    lineage.dict()
                    for lineage in get_column_level_lineage(session, new_revision)
                ]

                # Save which columns were modified and update the columns with the changes
                updated_columns = node_validator.modified_columns(new_revision)
                new_revision.columns = node_validator.columns

                # Save the new revision of the child
                new_revision.node = child.node
                new_revision.node.current_version = new_revision.version
                session.add(new_revision)
                session.add(new_revision.node)

                # Add grandchildren for processing
                for grandchild in child.node.children:
                    new_changelog = changelog + [grandchild]
                    to_process.append(new_changelog)

                # Record history event
                history_events[child.name] = {
                    "name": child.name,
                    "current_version": new_revision.version,
                    "previous_version": child.version,
                    "updated_columns": sorted(list(updated_columns)),
                }
                event = History(
                    entity_type=EntityType.NODE,
                    entity_name=child.name,
                    node=child.name,
                    activity_type=ActivityType.STATUS_CHANGE,
                    details={
                        "upstreams": [
                            history_events[entry.name]
                            for entry in changelog
                            if entry.name in history_events
                        ],
                        "reason": f"Caused by update of `{node.name}` to "
                        f"{node.current_version}",
                    },
                    pre={"status": child.status},
                    post={"status": node_validator.status},
                    user=current_user.username if current_user else None,
                )
                session.add(event)
                session.commit()
                session.refresh(new_revision)
                session.refresh(new_revision.node)


def copy_existing_node_revision(old_revision: NodeRevision):
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
        columns=old_revision.columns,
        catalog=old_revision.catalog,
        schema_=old_revision.schema_,
        table=old_revision.table,
        parents=old_revision.parents,
        mode=old_revision.mode,
        materializations=old_revision.materializations,
        status=old_revision.status,
        required_dimensions=old_revision.required_dimensions,
        metric_metadata=old_revision.metric_metadata,
    )


def _create_node_from_inactive(
    new_node_type: NodeType,
    data: Union[CreateSourceNode, CreateNode, CreateCubeNode],
    session: Session,
    *,
    current_user: Optional[User] = None,
    query_service_client: QueryServiceClient,
    background_tasks: BackgroundTasks = None,
    validate_access: access.ValidateAccessFn = None,
) -> Optional[Node]:
    """
    If the node existed and is inactive the re-creation takes different steps than
    creating it from scratch.
    """
    previous_inactive_node = get_node_by_name(
        session,
        name=data.name,
        raise_if_not_exists=False,
        include_inactive=True,
    )
    if previous_inactive_node and previous_inactive_node.deactivated_at:
        if previous_inactive_node.type != new_node_type:
            raise DJException(  # pragma: no cover
                message=f"A node with name `{data.name}` of a `{previous_inactive_node.type.value}` "  # pylint: disable=line-too-long
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

            update_node_with_query(
                name=data.name,
                data=update_node,
                session=session,
                query_service_client=query_service_client,
                current_user=current_user,
                background_tasks=background_tasks,
                validate_access=validate_access,  # type: ignore
            )
        else:
            update_cube_node(
                session,
                previous_inactive_node.current,
                data,
                query_service_client=query_service_client,
                background_tasks=background_tasks,
                validate_access=validate_access,  # type: ignore
            )
        try:
            activate_node(name=data.name, session=session, current_user=current_user)
            return get_node_by_name(session, data.name, with_current=True)
        except Exception as exc:  # pragma: no cover
            raise DJException(
                f"Restoring node `{data.name}` failed: {exc}",
            ) from exc

    return None


def create_new_revision_from_existing(  # pylint: disable=too-many-locals,too-many-branches
    session: Session,
    old_revision: NodeRevision,
    node: Node,
    data: UpdateNode = None,
    version_upgrade: VersionUpgrade = None,
) -> Optional[NodeRevision]:
    """
    Creates a new revision from an existing node revision.
    """
    metadata_changes = data is not None and data.metric_metadata
    minor_changes = (
        (data and data.description and old_revision.description != data.description)
        or (data and data.mode and old_revision.mode != data.mode)
        or (
            data
            and data.display_name
            and old_revision.display_name != data.display_name
        )
        or metadata_changes
    )

    query_changes = (
        old_revision.type != NodeType.SOURCE
        and data
        and data.query
        and old_revision.query != data.query
    )
    column_changes = (
        old_revision.type == NodeType.SOURCE
        and data is not None
        and data.columns is not None
        and ({col.identifier() for col in old_revision.columns} != data.columns)
    )
    pk_changes = (
        data is not None
        and data.primary_key
        and {col.name for col in old_revision.primary_key()} != set(data.primary_key)
    )
    required_dim_changes = (
        data is not None
        and data.required_dimensions
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
                name=column_data.name,
                type=column_data.type,
                dimension_column=column_data.dimension,
                attributes=column_data.attributes or [],
                order=idx,
            )
            for idx, column_data in enumerate(data.columns)
        ]
        if data and data.columns
        else old_revision.columns,
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
    )
    if data.required_dimensions:  # type: ignore
        new_revision.required_dimensions = data.required_dimensions  # type: ignore

    # Link the new revision to its parents if a new revision was created and update its status
    if new_revision.type != NodeType.SOURCE:
        node_validator = validate_node_data(new_revision, session)
        new_revision.columns = node_validator.columns
        new_revision.status = node_validator.status
        if node_validator.errors:
            if new_mode == NodeMode.DRAFT:
                new_revision.status = NodeStatus.INVALID
            else:
                raise DJException(
                    http_status_code=HTTPStatus.BAD_REQUEST,
                    errors=node_validator.errors,
                )

        new_parents = [n.name for n in node_validator.dependencies_map]
        parent_refs = (
            session.execute(
                select(Node).where(
                    # pylint: disable=no-member
                    Node.name.in_(  # type: ignore
                        new_parents,
                    ),
                ),
            )
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
        if new_revision.type == NodeType.METRIC:
            new_revision.columns[0].display_name = new_revision.display_name

        # Update the primary key if one was set in the input
        if data is not None and data.primary_key:
            pk_attribute = session.execute(
                select(AttributeType).where(AttributeType.name == "primary_key"),
            ).scalar_one()
            if set(data.primary_key) - set(col.name for col in new_revision.columns):
                raise DJException(  # pragma: no cover
                    f"Primary key {data.primary_key} does not exist on {new_revision.name}",
                )
            for col in new_revision.columns:
                # Remove the primary key attribute if it's not in the updated PK
                if col.has_primary_key_attribute() and col.name not in data.primary_key:
                    col.attributes = [
                        attr
                        for attr in col.attributes
                        if attr.attribute_type.name != "primary_key"
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


def save_column_level_lineage(
    session: Session,
    node_revision: NodeRevision,
):
    """
    Saves the column-level lineage for a node
    """
    column_level_lineage = get_column_level_lineage(session, node_revision)
    node_revision.lineage = [lineage.dict() for lineage in column_level_lineage]
    session.add(node_revision)
    session.commit()


def get_column_level_lineage(
    session: Session,
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
            column_lineage(
                session,
                node_revision,
                col.name,
            )
            for col in node_revision.columns
        ]
    return []


def column_lineage(
    session: Session,
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
    query_ast.compile(ctx)
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
            col != ast.Null() and col.alias_or_name.name == column_name  # type: ignore
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
    processed = list(column_expr.find_all(ast.Column))
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
                column_lineage(
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


def get_cube_revision_metadata(session: Session, name: str):
    """
    Returns cube revision metadata for cube named `name`.
    """
    statement = (
        select(NodeRevision)
        .select_from(Node)
        .join(
            NodeRevision,
            (NodeRevision.name == Node.name)
            & (NodeRevision.version == Node.current_version),
        )
        .where(Node.name == name)
        .options(
            joinedload(NodeRevision.availability),
            joinedload(NodeRevision.materializations).joinedload(
                Materialization.backfills,
            ),
            joinedload(NodeRevision.cube_elements).joinedload(Column.node_revisions),
        )
    )
    result = session.execute(statement).unique().first()
    if not result:
        raise DJNodeNotFound(  # pragma: no cover
            message=f"A cube node with name `{name}` does not exist.",
            http_status_code=404,
        )
    cube = result[0]
    cube_metadata = CubeRevisionMetadata.from_orm(cube)
    cube_metadata.tags = cube.node.tags
    return cube_metadata


def upsert_complex_dimension_link(
    session: Session,
    node_name: str,
    link_input: LinkDimensionInput,
    current_user: Optional[User] = None,
) -> bool:
    """
    Create or update a node-level dimension link.

    A dimension link is uniquely identified by the origin node, the dimension node being linked,
    and the role, if any. If an existing dimension link identified by those fields already exists,
    we'll update that dimension link. If no dimension link exists, we'll create a new one.
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
    return is_update


def remove_dimension_link(
    session: Session,
    node_name: str,
    link_identifier: LinkDimensionIdentifier,
    current_user: Optional[User] = None,
):
    """
    Removes the dimension link identified by the origin node, the dimension node, and its role.
    """
    node = get_node_by_name(session=session, name=node_name)

    # Find the dimension node
    dimension_node = get_node_by_name(
        session=session,
        name=link_identifier.dimension_node,
        node_type=NodeType.DIMENSION,
    )
    removed = False

    # Find cubes that are affected by this dimension link removal and update their statuses
    affected_cubes = (
        get_nodes_with_dimension(
            session,
            dimension_node,
            [NodeType.CUBE],
        )
        or []
    )
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
