"""
Helper methods for namespaces endpoints.
"""

import os
import re
from datetime import datetime
from typing import Callable, Dict, List, Tuple

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from datajunction_server.api.helpers import get_node_namespace
from datajunction_server.database.history import History
from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.node import Column, Node, NodeRevision
from datajunction_server.database.user import User
from datajunction_server.errors import (
    DJActionNotAllowedException,
    DJDoesNotExistException,
    DJInvalidInputException,
)
from datajunction_server.internal.history import ActivityType, EntityType
from datajunction_server.internal.nodes import (
    get_single_cube_revision_metadata,
    hard_delete_node,
)
from datajunction_server.models.node import NodeMinimumDetail
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.dag import topological_sort
from datajunction_server.typing import UTCDatetime
from datajunction_server.utils import SEPARATOR

# A list of namespace names that cannot be used because they are
# part of a list of reserved SQL keywords
RESERVED_NAMESPACE_NAMES = [
    "user",
]


async def get_nodes_in_namespace(
    session: AsyncSession,
    namespace: str,
    node_type: NodeType = None,
    include_deactivated: bool = False,
) -> List[NodeMinimumDetail]:
    """
    Gets a list of node names in the namespace
    """
    return await NodeNamespace.list_nodes(
        session,
        namespace,
        node_type=node_type,
        include_deactivated=include_deactivated,
    )


async def get_nodes_in_namespace_detailed(
    session: AsyncSession,
    namespace: str,
    node_type: NodeType = None,
) -> List[Node]:
    """
    Gets a list of node names (w/ full details) in the namespace
    """
    await get_node_namespace(session, namespace)
    list_nodes_query = (
        select(Node)
        .where(
            or_(
                Node.namespace.like(f"{namespace}.%"),
                Node.namespace == namespace,
            ),
            Node.current_version == NodeRevision.version,
            Node.name == NodeRevision.name,
            Node.type == node_type if node_type else True,  # type: ignore
            Node.deactivated_at.is_(None),
        )
        .options(
            joinedload(Node.current).options(
                *NodeRevision.default_load_options(),
            ),
            joinedload(Node.tags),
        )
    )
    return (await session.execute(list_nodes_query)).unique().scalars().all()


async def list_namespaces_in_hierarchy(
    session: AsyncSession,
    namespace: str,
) -> List[NodeNamespace]:
    """
    Get all namespaces in hierarchy under the specified namespace
    """
    statement = select(NodeNamespace).where(
        or_(
            NodeNamespace.namespace.like(
                f"{namespace}.%",
            ),
            NodeNamespace.namespace == namespace,
        ),
    )
    namespaces = (await session.execute(statement)).scalars().all()
    if len(namespaces) == 0:
        raise DJDoesNotExistException(
            message=(f"Namespace `{namespace}` does not exist."),
            http_status_code=404,
        )
    return namespaces


async def mark_namespace_deactivated(
    session: AsyncSession,
    namespace: NodeNamespace,
    current_user: User,
    save_history: Callable,
    message: str = None,
):
    """
    Deactivates the node namespace and updates history indicating so
    """
    now = datetime.utcnow()
    namespace.deactivated_at = UTCDatetime(
        year=now.year,
        month=now.month,
        day=now.day,
        hour=now.hour,
        minute=now.minute,
        second=now.second,
    )
    await save_history(
        event=History(
            entity_type=EntityType.NAMESPACE,
            entity_name=namespace.namespace,
            node=None,
            activity_type=ActivityType.DELETE,
            details={"message": message or ""},
            user=current_user.username,
        ),
        session=session,
    )
    await session.commit()


async def mark_namespace_restored(
    session: AsyncSession,
    namespace: NodeNamespace,
    current_user: User,
    save_history: Callable,
    message: str = None,
):
    """
    Restores the node namespace and updates history indicating so
    """
    namespace.deactivated_at = None  # type: ignore
    await save_history(
        event=History(
            entity_type=EntityType.NAMESPACE,
            entity_name=namespace.namespace,
            node=None,
            activity_type=ActivityType.RESTORE,
            details={"message": message or ""},
            user=current_user.username,
        ),
        session=session,
    )
    await session.commit()


def validate_namespace(namespace: str):
    """
    Validate that the namespace parts are valid (i.e., cannot start with numbers or be empty)
    """
    parts = namespace.split(SEPARATOR)
    for part in parts:
        if (
            not part
            or not re.match("^[a-zA-Z][a-zA-Z0-9_]*$", part)
            or part in RESERVED_NAMESPACE_NAMES
        ):
            raise DJInvalidInputException(
                f"{namespace} is not a valid namespace. Namespace parts cannot start with numbers"
                f", be empty, or use the reserved keyword [{', '.join(RESERVED_NAMESPACE_NAMES)}]",
            )


def get_parent_namespaces(namespace: str):
    """
    Return a list of all parent namespaces
    """
    parts = namespace.split(SEPARATOR)
    return [SEPARATOR.join(parts[0:i]) for i in range(len(parts)) if parts[0:i]]


async def create_namespace(
    session: AsyncSession,
    namespace: str,
    current_user: User,
    save_history: Callable,
    include_parents: bool = True,
) -> List[str]:
    """
    Creates a namespace entry in the database table.
    """
    parents = (
        get_parent_namespaces(namespace) + [namespace]
        if include_parents
        else [namespace]
    )
    for parent_namespace in parents:
        if not await get_node_namespace(  # pragma: no cover
            session=session,
            namespace=parent_namespace,
            raise_if_not_exists=False,
        ):
            node_namespace = NodeNamespace(namespace=parent_namespace)
            session.add(node_namespace)
            await save_history(
                event=History(
                    entity_type=EntityType.NAMESPACE,
                    entity_name=namespace,
                    node=None,
                    activity_type=ActivityType.CREATE,
                    user=current_user.username,
                ),
                session=session,
            )
    await session.commit()
    return parents


async def hard_delete_namespace(
    session: AsyncSession,
    namespace: str,
    current_user: User,
    save_history: Callable,
    cascade: bool = False,
):
    """
    Hard delete a node namespace.
    """
    node_names = (
        (
            await session.execute(
                select(Node.name)
                .where(
                    or_(
                        Node.namespace.like(
                            f"{namespace}.%",
                        ),
                        Node.namespace == namespace,
                    ),
                )
                .order_by(Node.name),
            )
        )
        .scalars()
        .all()
    )

    if not cascade and node_names:
        raise DJActionNotAllowedException(
            message=(
                f"Cannot hard delete namespace `{namespace}` as there are still the "
                f"following nodes under it: `{node_names}`. Set `cascade` to true to "
                "additionally hard delete the above nodes in this namespace. WARNING:"
                " this action cannot be undone."
            ),
        )

    impacts = {}
    for node_name in node_names:
        impacts[node_name] = await hard_delete_node(
            node_name,
            session,
            current_user=current_user,
            save_history=save_history,
        )

    namespaces = await list_namespaces_in_hierarchy(session, namespace)
    for _namespace in namespaces:
        impacts[_namespace.namespace] = {
            "namespace": _namespace.namespace,
            "status": "deleted",
        }
        await session.delete(_namespace)
    await session.commit()
    return impacts


def _get_dir_and_filename(
    node_name: str,
    node_type: str,
    namespace_requested: str,
) -> Tuple[str, str, str]:
    """
    Get the directory, filename, and build name for a node
    """
    dot_split = node_name.replace(f"{namespace_requested}.", "").split(".")
    filename = f"{dot_split[-1]}.{node_type}.yaml"
    directory = os.path.sep.join(dot_split[:-1])
    build_name = (
        f"{SEPARATOR.join(dot_split[:-1])}.{dot_split[-1]}"
        if directory
        else dot_split[-1]
    )
    return filename, directory, build_name


def _non_primary_key_attributes(column: Column):
    """
    Returns all non-PK column attributes for a column
    """
    return [
        attr.attribute_type.name
        for attr in column.attributes
        if attr.attribute_type.name not in ("primary_key",)
    ]


def _attributes_config(column: Column):
    """
    Returns a project config definition for a partition on a column
    """
    non_pk_attributes = _non_primary_key_attributes(column)
    if non_pk_attributes:
        return {"attributes": _non_primary_key_attributes(column)}
    return {}


def _partition_config(column: Column):
    """
    Returns a project config definition for a partition on a column
    """
    if column.partition:
        return {
            "partition": {
                "format": column.partition.format,
                "granularity": column.partition.granularity,
                "type_": column.partition.type_,
            },
        }
    return {}


def _source_project_config(node: Node, namespace_requested: str) -> Dict:
    """
    Returns a project config definition for a source node
    """
    filename, directory, build_name = _get_dir_and_filename(
        node_name=node.name,
        node_type=node.type,
        namespace_requested=namespace_requested,
    )
    return {
        "filename": filename,
        "directory": directory,
        "build_name": build_name,
        "display_name": node.current.display_name,
        "description": node.current.description,
        "table": f"{node.current.catalog}.{node.current.schema_}.{node.current.table}",
        "columns": [
            {
                "name": column.name,
                "type": str(column.type),
                **_attributes_config(column),
                **_partition_config(column),
            }
            for column in node.current.columns
        ],
        "primary_key": [pk.name for pk in node.current.primary_key()],
        "dimension_links": _dimension_links_config(node),
        "tags": [tag.name for tag in node.tags],
    }


def _transform_project_config(node: Node, namespace_requested: str) -> Dict:
    """
    Returns a project config definition for a transform node
    """
    filename, directory, build_name = _get_dir_and_filename(
        node_name=node.name,
        node_type=node.type,
        namespace_requested=namespace_requested,
    )
    return {
        "filename": filename,
        "directory": directory,
        "build_name": build_name,
        "display_name": node.current.display_name,
        "description": node.current.description,
        "query": node.current.query,
        "columns": [
            {
                "name": column.name,
                **_attributes_config(column),
                **_partition_config(column),
            }
            for column in node.current.columns
            if _non_primary_key_attributes(column) or column.partition
        ],
        "primary_key": [pk.name for pk in node.current.primary_key()],
        "dimension_links": _dimension_links_config(node),
        "tags": [tag.name for tag in node.tags],
    }


def _dimension_project_config(node: Node, namespace_requested: str) -> Dict:
    """
    Returns a project config definition for a dimension node
    """
    filename, directory, build_name = _get_dir_and_filename(
        node_name=node.name,
        node_type=node.type,
        namespace_requested=namespace_requested,
    )
    return {
        "filename": filename,
        "directory": directory,
        "build_name": build_name,
        "display_name": node.current.display_name,
        "description": node.current.description,
        "query": node.current.query,
        "columns": [
            {
                "name": column.name,
                **_attributes_config(column),
                **_partition_config(column),
            }
            for column in node.current.columns
            if _non_primary_key_attributes(column) or column.partition
        ],
        "primary_key": [pk.name for pk in node.current.primary_key()],
        "dimension_links": _dimension_links_config(node),
        "tags": [tag.name for tag in node.tags],
    }


def _metric_project_config(node: Node, namespace_requested: str) -> Dict:
    """
    Returns a project config definition for a metric node
    """
    filename, directory, build_name = _get_dir_and_filename(
        node_name=node.name,
        node_type=node.type,
        namespace_requested=namespace_requested,
    )
    return {
        "filename": filename,
        "directory": directory,
        "build_name": build_name,
        "display_name": node.current.display_name,
        "description": node.current.description,
        "query": node.current.query,
        "tags": [tag.name for tag in node.tags],
        "required_dimensions": [dim.name for dim in node.current.required_dimensions],
        "direction": (
            node.current.metric_metadata.direction.name.lower()
            if node.current.metric_metadata and node.current.metric_metadata.direction
            else None
        ),
        "unit": (
            node.current.metric_metadata.unit.name.lower()
            if node.current.metric_metadata and node.current.metric_metadata.unit
            else None
        ),
        "significant_digits": (
            node.current.metric_metadata.significant_digits
            if node.current.metric_metadata
            and node.current.metric_metadata.significant_digits
            else None
        ),
        "min_decimal_exponent": (
            node.current.metric_metadata.min_decimal_exponent
            if node.current.metric_metadata
            and node.current.metric_metadata.min_decimal_exponent
            else None
        ),
        "max_decimal_exponent": (
            node.current.metric_metadata.max_decimal_exponent
            if node.current.metric_metadata
            and node.current.metric_metadata.max_decimal_exponent
            else None
        ),
    }


async def _cube_project_config(
    session: AsyncSession,
    node: Node,
    namespace_requested: str,
) -> Dict:
    """
    Returns a project config definition for a cube node
    """
    filename, directory, build_name = _get_dir_and_filename(
        node_name=node.name,
        node_type=NodeType.CUBE,
        namespace_requested=namespace_requested,
    )
    cube_revision = await get_single_cube_revision_metadata(session, node.name)
    metrics = []
    dimensions = []
    for element in cube_revision.cube_elements:
        if element.type == NodeType.METRIC:
            metrics.append(element.node_name)
        else:
            dimensions.append(f"{element.node_name}.{element.name}")
    return {
        "filename": filename,
        "directory": directory,
        "build_name": build_name,
        "display_name": cube_revision.display_name,
        "description": cube_revision.description,
        "metrics": metrics,
        "dimensions": dimensions,
        "columns": [
            {
                "name": column.name,
                **_partition_config(column),
            }
            for column in cube_revision.columns
            if column.partition
        ],
        "tags": [tag.name for tag in node.tags],
    }


def _dimension_links_config(node: Node):
    join_links = [
        {
            "type": "join",
            "dimension_node": link.dimension.name,
            "join_type": link.join_type,
            "join_on": link.join_sql,
            **({"role": link.role} if link.role else {}),
        }
        for link in node.current.dimension_links
    ]
    reference_links = [
        {
            "type": "reference",
            "node_column": column.name,
            "dimension": column.dimension.name
            + SEPARATOR
            + (column.dimension_column or ""),
        }
        for column in node.current.columns
        if column.dimension
    ]
    return join_links + reference_links


async def get_project_config(
    session: AsyncSession,
    nodes: List[Node],
    namespace_requested: str,
) -> List[Dict]:
    """
    Returns a project config definition
    """
    sorted_nodes = topological_sort(nodes)
    project_config_mapping = {
        NodeType.SOURCE: _source_project_config,
        NodeType.TRANSFORM: _transform_project_config,
        NodeType.DIMENSION: _dimension_project_config,
        NodeType.METRIC: _metric_project_config,
    }
    project_components = [
        project_config_mapping[node.type](
            node=node,
            namespace_requested=namespace_requested,
        )
        if node.type in project_config_mapping
        else await _cube_project_config(
            session=session,
            node=node,
            namespace_requested=namespace_requested,
        )
        for node in sorted_nodes
    ]
    return project_components
