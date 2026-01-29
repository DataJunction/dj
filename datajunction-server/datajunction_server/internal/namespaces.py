"""
Helper methods for namespaces endpoints.
"""

from collections import defaultdict
import logging
import os
import re
from datetime import datetime, timezone
from typing import Callable, Dict, List, Tuple

from sqlalchemy import or_, select, delete, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from datajunction_server.database.deployment import Deployment
from datajunction_server.models.deployment import (
    DeploymentSourceType,
    GitDeploymentSource,
    LocalDeploymentSource,
    NamespaceSourcesResponse,
)
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
from datajunction_server.models.namespace import (
    ImpactedNode,
    HardDeleteResponse,
    ImpactedNodes,
)
from datajunction_server.internal.history import ActivityType, EntityType
from datajunction_server.internal.nodes import (
    get_single_cube_revision_metadata,
)
from datajunction_server.models.node import NodeMinimumDetail
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.dag import (
    get_downstream_nodes,
    get_nodes_with_common_dimensions,
    topological_sort,
)
from datajunction_server.typing import UTCDatetime
from datajunction_server.utils import SEPARATOR

import logging
from typing import Callable, Dict, List, Optional, cast

import yaml
from sqlalchemy.ext.asyncio import AsyncSession
from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.node import Node
from datajunction_server.database.user import User
from datajunction_server.models.deployment import (
    CubeSpec,
    NamespaceSourcesResponse,
    NodeSpec,
)
from datajunction_server.models.dimensionlink import LinkType
from datajunction_server.models.node import NodeMinimumDetail
from datajunction_server.models.node_type import NodeType
from datajunction_server.utils import SEPARATOR

logger = logging.getLogger(__name__)

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
    now = datetime.now(timezone.utc)
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
    logger.info("Creating namespace `%s` and any parent namespaces", namespace)

    validate_namespace(namespace)
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
            logger.info("Created namespace `%s`", parent_namespace)
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
) -> HardDeleteResponse:
    """
    Hard delete a node namespace.
    """
    node_names = (
        (
            await session.execute(
                select(Node.name)
                .where(
                    or_(
                        Node.namespace.like(f"{namespace}.%"),
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
    impacts = {
        node_name: {"type": "node in namespace", "status": "hard deleted"}
        for node_name in node_names
    }

    # Track downstream nodes affected by deletions
    impacted_downstreams = defaultdict(list)
    impacted_links = defaultdict(list)
    nodes = await Node.get_by_names(session, node_names)
    for node in nodes:
        # Downstream links
        if node.type == NodeType.DIMENSION:
            for downstream_link in await get_nodes_with_common_dimensions(
                session,
                [node],
            ):
                if downstream_link.name not in node_names:
                    impacted_links[downstream_link.name].append(node.name)

        # Downstream query references
        for downstream_node in await get_downstream_nodes(
            session=session,
            node_name=node.name,
        ):
            if downstream_node.name not in node_names:
                impacted_downstreams[downstream_node.name].append(node.name)

    # Save history and update impacts for downstream nodes
    for impacted_node, causes in impacted_downstreams.items():
        await save_history(
            event=History(
                entity_type=EntityType.DEPENDENCY,
                entity_name=impacted_node,
                activity_type=ActivityType.DELETE,
                user=current_user.username,
                details={"caused_by": causes},
            ),
            session=session,
        )

    for impacted_node, causes in impacted_links.items():
        await save_history(
            event=History(
                entity_type=EntityType.DEPENDENCY,
                entity_name=impacted_node,
                activity_type=ActivityType.DELETE,
                user=current_user.username,
                details={"caused_by": causes},
            ),
            session=session,
        )

    # Delete the nodes
    await session.execute(delete(Node).where(Node.name.in_(node_names)))
    await session.commit()

    # Delete namespaces and record impact
    namespaces = await list_namespaces_in_hierarchy(session, namespace)
    deleted_namespaces = [ns.namespace for ns in namespaces]
    for _namespace in namespaces:
        impacts[_namespace.namespace] = {
            "type": "namespace",
            "status": "hard deleted",
        }
        await session.delete(_namespace)
    await session.commit()

    return HardDeleteResponse(
        deleted_namespaces=deleted_namespaces,
        deleted_nodes=node_names,
        impacted=ImpactedNodes(
            downstreams=[
                ImpactedNode(name=downstream, caused_by=caused_by)
                for downstream, caused_by in impacted_downstreams.items()
            ],
            links=[
                ImpactedNode(name=linked, caused_by=caused_by)
                for linked, caused_by in impacted_links.items()
            ],
        ),
    )


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


async def get_sources_for_namespace(
    session: AsyncSession,
    namespace: str,
) -> NamespaceSourcesResponse:
    """
    Helper to get deployment sources for a single namespace.
    Delegates to the bulk function for consistency.
    """
    results = await get_sources_for_namespaces_bulk(session, [namespace])
    return results.get(
        namespace,
        NamespaceSourcesResponse(
            namespace=namespace,
            primary_source=None,
            total_deployments=0,
        ),
    )


async def get_sources_for_namespaces_bulk(
    session: AsyncSession,
    namespaces: list[str],
) -> dict[str, NamespaceSourcesResponse]:
    """
    Get deployment sources for multiple namespaces in a single optimized query.

    Uses window functions to efficiently fetch the most recent 20 deployments
    per namespace in a single query, avoiding N database round trips.
    """
    if not namespaces:
        return {}

    # Get total counts per namespace in one query
    count_stmt = (
        select(
            Deployment.namespace,
            func.count().label("total"),
        )
        .where(Deployment.namespace.in_(namespaces))
        .group_by(Deployment.namespace)
    )
    count_result = await session.execute(count_stmt)
    counts_by_namespace = {row.namespace: row.total for row in count_result}

    # Get the most recent 20 deployments per namespace using window function
    # Create a subquery that ranks deployments within each namespace
    row_num = (
        func.row_number()
        .over(
            partition_by=Deployment.namespace,
            order_by=Deployment.created_at.desc(),
        )
        .label("rn")
    )

    # Include all deployments since failed deploys still indicate the source
    ranked_subquery = (
        select(
            Deployment.namespace,
            Deployment.spec,
            Deployment.created_at,
            row_num,
        ).where(Deployment.namespace.in_(namespaces))
    ).subquery()

    # Filter to only the top 20 per namespace
    recent_stmt = select(
        ranked_subquery.c.namespace,
        ranked_subquery.c.spec,
        ranked_subquery.c.created_at,
    ).where(ranked_subquery.c.rn <= 20)

    recent_result = await session.execute(recent_stmt)
    recent_rows = recent_result.all()

    # Process deployments and determine primary source for each namespace
    # Group deployments by namespace
    deployments_by_namespace: dict[str, list[dict]] = defaultdict(list)
    for row in recent_rows:
        deployments_by_namespace[row.namespace].append(
            {"spec": row.spec, "created_at": row.created_at},
        )

    # Build response for each namespace
    results: dict[str, NamespaceSourcesResponse] = {}
    for namespace in namespaces:
        total_deployments = counts_by_namespace.get(namespace, 0)

        if total_deployments == 0:
            results[namespace] = NamespaceSourcesResponse(
                namespace=namespace,
                primary_source=None,
                total_deployments=0,
            )
            continue

        recent_deployments = deployments_by_namespace.get(namespace, [])

        # Count source types among recent deployments to determine primary
        git_count = 0
        local_count = 0
        latest_git_source: GitDeploymentSource | None = None
        latest_local_source: LocalDeploymentSource | None = None

        for deployment in recent_deployments:
            source_data = (
                deployment["spec"].get("source") if deployment["spec"] else None
            )

            if source_data and source_data.get("type") == DeploymentSourceType.GIT:
                git_count += 1
                if latest_git_source is None:
                    latest_git_source = GitDeploymentSource(**source_data)
            elif source_data and source_data.get("type") == DeploymentSourceType.LOCAL:
                local_count += 1
                if latest_local_source is None:  # pragma: no branch
                    latest_local_source = LocalDeploymentSource(**source_data)
            else:
                # Legacy deployment without source info - treat as local
                local_count += 1
                if latest_local_source is None:  # pragma: no branch
                    latest_local_source = LocalDeploymentSource()

        # Primary source is the type with more deployments among recent 20
        # If tie, prefer git (managed) over local
        if git_count >= local_count and latest_git_source:
            primary_source: GitDeploymentSource | LocalDeploymentSource | None = (
                latest_git_source
            )
        elif latest_local_source:
            primary_source = latest_local_source
        else:
            primary_source = None  # pragma: no cover

        results[namespace] = NamespaceSourcesResponse(
            namespace=namespace,
            primary_source=primary_source,
            total_deployments=total_deployments,
        )

    return results


def inject_prefixes(unparameterized_string: str, prefix: str) -> str:
    """
    Replaces a namespace in a string with ${prefix}
    default.namespace.blah -> ${prefix}.blah
    default.namespace.blah.foo -> ${prefix}.blah.foo
    """
    return unparameterized_string.replace(f"{prefix}" + SEPARATOR, "${prefix}")


def _get_node_suffix(full_name: str, namespace_prefix: str) -> Optional[str]:
    """
    Extract the suffix of a node name after the namespace prefix.

    E.g., _get_node_suffix("demo.main.reports.revenue", "demo.main") -> "reports.revenue"
    """
    prefix_with_dot = namespace_prefix + SEPARATOR
    if full_name.startswith(prefix_with_dot):
        return full_name[len(prefix_with_dot) :]
    return None


def _inject_prefix_for_cube_ref(
    ref_name: str,
    namespace: str,
    parent_namespace: Optional[str],
    namespace_suffixes: set[str],
) -> str:
    """
    Inject ${prefix} for cube metric/dimension references.

    For cube references, we need to check if there's a matching node in the
    current namespace. A cube might reference nodes from a parent namespace
    (e.g., main), but if we've copied those nodes to the branch, we should
    use ${prefix} instead.

    Args:
        ref_name: Full reference name (e.g., "demo.main.reports.revenue")
        namespace: Current namespace being exported (e.g., "demo.feature_x")
        parent_namespace: Parent namespace if this is a branch (e.g., "demo.main")
        namespace_suffixes: Set of suffixes for nodes in the namespace
                           (e.g., {"my_metric", "reports.revenue"})

    Returns:
        Either "${prefix}<suffix>" if node exists in namespace,
        or the original ref_name if it's external
    """
    logger.info(
        "Cube ref injection: ref_name=%s, namespace=%s, parent_namespace=%s, "
        "namespace_suffixes=%s",
        ref_name,
        namespace,
        parent_namespace,
        namespace_suffixes,
    )

    # First try direct prefix injection (node is in current namespace)
    injected = inject_prefixes(ref_name, namespace)
    if injected != ref_name:
        logger.info("Cube ref: direct injection matched, %s -> %s", ref_name, injected)
        return injected

    # For branch namespaces, check if the reference is from parent namespace
    # and has been copied to this branch
    if parent_namespace:
        suffix = _get_node_suffix(ref_name, parent_namespace)
        logger.info(
            "Cube ref: checking parent namespace, suffix=%s, in_suffixes=%s",
            suffix,
            suffix in namespace_suffixes if suffix else False,
        )
        if suffix and suffix in namespace_suffixes:
            result = f"${{prefix}}{suffix}"
            logger.info("Cube ref: parent match, %s -> %s", ref_name, result)
            return result

    # External reference - keep as-is
    logger.info("Cube ref: external reference, keeping as-is: %s", ref_name)
    return ref_name


async def get_node_specs_for_export(
    session: AsyncSession,
    namespace: str,
) -> list[NodeSpec]:
    """
    Get node specs for a namespace with ${prefix} injection applied.

    This is shared between:
    - /namespaces/{namespace}/export/spec (JSON API)
    - /namespaces/{namespace}/export/yaml (ZIP download)
    """
    # Get the namespace object to find parent_namespace (for branch namespaces)
    namespace_obj = await NodeNamespace.get(
        session,
        namespace,
        raise_if_not_exists=False,
    )
    parent_namespace = namespace_obj.parent_namespace if namespace_obj else None

    nodes = await NodeNamespace.list_all_nodes(
        session,
        namespace,
        options=Node.cube_load_options(),
    )
    node_specs = [await node.to_spec(session) for node in nodes]

    # Build set of node suffixes in this namespace (for cube reference matching)
    # e.g., for "demo.feature_x.reports.revenue" with namespace "demo.feature_x",
    # the suffix is "reports.revenue"
    namespace_suffixes: set[str] = set()
    for node in nodes:
        suffix = _get_node_suffix(node.name, namespace)
        if suffix:
            namespace_suffixes.add(suffix)

    logger.info(
        "Export namespace '%s': parent_namespace=%s, namespace_suffixes=%s",
        namespace,
        parent_namespace,
        namespace_suffixes,
    )

    for node_spec in node_specs:
        logger.info(
            "Processing node_spec: name=%s, node_type=%s",
            node_spec.name,
            node_spec.node_type,
        )
        node_spec.name = inject_prefixes(node_spec.rendered_name, namespace)
        if node_spec.node_type in (
            NodeType.TRANSFORM,
            NodeType.DIMENSION,
            NodeType.METRIC,
        ):
            node_spec.query = inject_prefixes(node_spec.query, namespace)
        if node_spec.node_type in (
            NodeType.SOURCE,
            NodeType.TRANSFORM,
            NodeType.DIMENSION,
        ):
            for link in node_spec.dimension_links:
                if link.type == LinkType.JOIN:
                    link.dimension_node = inject_prefixes(
                        link.dimension_node,
                        namespace,
                    )
                    link.join_on = inject_prefixes(link.join_on, namespace)
                else:  # pragma: no cover
                    link.dimension = inject_prefixes(link.dimension, namespace)
        if node_spec.node_type == NodeType.CUBE:
            cube_spec = cast(CubeSpec, node_spec)
            logger.info(
                "Processing cube '%s': metrics=%s, dimensions=%s",
                node_spec.name,
                node_spec.metrics,
                node_spec.dimensions,
            )
            cube_spec.metrics = [
                _inject_prefix_for_cube_ref(
                    metric,
                    namespace,
                    parent_namespace,
                    namespace_suffixes,
                )
                for metric in node_spec.metrics
            ]
            cube_spec.dimensions = [
                _inject_prefix_for_cube_ref(
                    dim,
                    namespace,
                    parent_namespace,
                    namespace_suffixes,
                )
                for dim in node_spec.dimensions
            ]
            logger.info(
                "Cube '%s' after injection: metrics=%s, dimensions=%s",
                node_spec.name,
                cube_spec.metrics,
                cube_spec.dimensions,
            )

    return node_specs


class LiteralBlockString(str):
    """Marker class for strings that should always use literal block style (|-) in YAML."""

    pass


def _literal_block_representer(dumper, data):
    """Representer for LiteralBlockString - always uses |- style."""
    # Strip trailing whitespace so YAML uses |- (strip chomping)
    return dumper.represent_scalar("tag:yaml.org,2002:str", data.rstrip(), style="|")


def _multiline_str_representer(dumper, data):
    """
    Custom YAML representer that uses literal block style (|-) for multiline strings.
    Strips trailing newlines so YAML uses the strip chomping indicator (-).
    """
    if "\n" in data:
        # Strip trailing whitespace/newlines so YAML uses |- (strip chomping)
        data = data.rstrip()
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


def _get_yaml_dumper():
    """
    Get a YAML dumper configured for clean node export.
    Uses literal block style for multiline strings (like SQL queries).
    """

    class MultilineStrDumper(yaml.SafeDumper):
        pass

    MultilineStrDumper.add_representer(str, _multiline_str_representer)
    MultilineStrDumper.add_representer(LiteralBlockString, _literal_block_representer)
    return MultilineStrDumper


def _node_spec_to_yaml_dict(node_spec) -> dict:
    """
    Convert a NodeSpec to a dict suitable for YAML serialization.
    Excludes None values and empty lists for cleaner output.

    For columns:
    - Cubes: columns are always excluded (they're inferred from metrics/dimensions)
    - Other nodes: only includes columns with meaningful customizations
      (display_name different from name, attributes, description, or partition).
      Column types are excluded - let DJ infer them from the query/source.
    """
    # Use model_dump with mode="json" to convert Enums to strings
    # Note: Don't use exclude_unset=True as it would exclude discriminator fields
    # like 'type' on dimension_links which have default values
    data = node_spec.model_dump(
        mode="json",  # Converts Enums to strings, datetimes to ISO format, etc.
        exclude_none=True,
        exclude={"namespace"},  # namespace is part of file path, not content
    )

    # Cubes should never have columns in export - they're inferred from metrics/dimensions
    if data.get("node_type") == "cube":
        data.pop("columns", None)
    # For other nodes, filter columns to only include meaningful customizations
    elif "columns" in data and data["columns"]:
        filtered_columns = []
        for col in data["columns"]:
            # Check for meaningful customizations
            has_custom_display = col.get("display_name") and col.get(
                "display_name",
            ) != col.get("name")
            has_attributes = bool(col.get("attributes"))
            has_description = bool(col.get("description"))
            has_partition = bool(col.get("partition"))

            if has_custom_display or has_attributes or has_description or has_partition:
                # Include column but exclude type (let DJ infer)
                filtered_col = {
                    k: v
                    for k, v in col.items()
                    if k != "type" and v  # Exclude type and empty values
                }
                filtered_columns.append(filtered_col)

        if filtered_columns:
            data["columns"] = filtered_columns
        else:
            # Remove columns entirely if none have customizations
            del data["columns"]

    # Remove empty lists/dicts for cleaner YAML
    data = {k: v for k, v in data.items() if v or v == 0 or v is False}

    # Wrap query fields with LiteralBlockString so they always use |- style
    # Strip trailing whitespace from each line to ensure block style works
    if "query" in data and data["query"]:
        cleaned_query = "\n".join(line.rstrip() for line in data["query"].split("\n"))
        data["query"] = LiteralBlockString(cleaned_query)

    # Also wrap join_on in dimension_links
    if "dimension_links" in data:
        for link in data["dimension_links"]:
            if "join_on" in link and link["join_on"]:
                cleaned_join = "\n".join(
                    line.rstrip() for line in link["join_on"].split("\n")
                )
                link["join_on"] = LiteralBlockString(cleaned_join)

    return data


def node_spec_to_yaml(node_spec) -> str:
    """
    Convert a NodeSpec to formatted YAML string.

    Uses yamlfix to ensure consistent formatting.
    """
    from yamlfix import fix_code
    from yamlfix.model import YamlfixConfig

    yaml_dict = _node_spec_to_yaml_dict(node_spec)
    yaml_dumper = _get_yaml_dumper()
    yaml_content = yaml.dump(
        yaml_dict,
        Dumper=yaml_dumper,
        sort_keys=False,
        default_flow_style=False,
        width=200,  # Prevent premature line wrapping; let yamlfix handle it
    )

    # Configure yamlfix to match pre-commit hook defaults
    config = YamlfixConfig(
        explicit_start=False,
        line_length=120,  # Match typical yamlfix default for repos
    )

    # Format with yamlfix for consistent style
    try:
        yaml_content = fix_code(yaml_content, config=config)
    except Exception as e:
        # If yamlfix fails, log and return the original YAML
        logger.warning("yamlfix failed to format YAML: %s", e)

    return yaml_content
