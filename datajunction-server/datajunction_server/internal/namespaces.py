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
from io import StringIO

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedSeq
from ruamel.yaml.comments import CommentedMap
from ruamel.yaml.comments import Comment

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
    # First try direct prefix injection (node is in current namespace)
    injected = inject_prefixes(ref_name, namespace)
    if injected != ref_name:
        return injected

    # For branch namespaces, check if the reference is from parent namespace
    # and has been copied to this branch
    if parent_namespace:
        suffix = _get_node_suffix(ref_name, parent_namespace)
        if suffix and suffix in namespace_suffixes:
            return f"${{prefix}}{suffix}"

    # External reference - keep as-is
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
        if suffix:  # pragma: no branch
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
            # Apply the same parameterization logic to cube column names
            # Columns may reference metrics/dimensions from within or outside the deployment
            if cube_spec.columns:  # pragma: no branch
                for col_spec in cube_spec.columns:
                    col_spec.name = _inject_prefix_for_cube_ref(
                        col_spec.name,
                        namespace,
                        parent_namespace,
                        namespace_suffixes,
                    )

    return node_specs


def _get_yaml_handler():
    """
    Get a configured ruamel.yaml YAML handler for node export.
    Preserves comments and automatically uses literal block style for multiline strings.
    """
    # Use round-trip mode to preserve formatting and comments
    yaml_handler = YAML()
    yaml_handler.default_flow_style = False
    yaml_handler.width = 120
    # Don't set offset - it breaks list item indentation in ruamel.yaml
    yaml_handler.indent(mapping=2, sequence=4, offset=2)
    return yaml_handler


def _merge_list_with_key(existing_list, new_list, match_key="name"):
    """
    Merge a list of dicts by matching on a key (e.g., 'name').

    Preserves comments from existing items when they match new items.
    Handles additions, deletions, and reordering.

    Args:
        existing_list: Existing list (potentially CommentedSeq with comments)
        new_list: New list of dicts to merge
        match_key: Key to use for matching items (default: 'name')

    Returns:
        Merged list with comments preserved where items match
    """
    # Build lookup of existing items by match_key, including their index for comment preservation
    existing_by_key = {}
    for idx, item in enumerate(existing_list):
        if isinstance(item, dict) and match_key in item:  # pragma: no branch
            existing_by_key[item[match_key]] = (idx, item)

    # Build result list in the order of new_list
    result = CommentedSeq()

    # Check if existing list has comment info
    has_ca = isinstance(existing_list, CommentedSeq) and hasattr(existing_list, "ca")
    has_comment_list = (
        has_ca and hasattr(existing_list.ca, "comment") and existing_list.ca.comment
    )
    has_items_dict = (
        has_ca and hasattr(existing_list.ca, "items") and existing_list.ca.items
    )

    # Initialize comment attribute on result if we're going to preserve comments
    if has_comment_list or has_items_dict:
        if not hasattr(result, "ca"):
            result.ca = Comment()  # pragma: no cover
        # Initialize ca.comment as a list with None for the list-level comment
        if (
            not hasattr(result.ca, "comment") or result.ca.comment is None
        ):  # pragma: no branch
            result.ca.comment = [None]  # Index 0 is for comment before the list itself
        # Initialize ca.items if we have items with comments
        if has_items_dict and (  # pragma: no branch
            not hasattr(result.ca, "items") or result.ca.items is None
        ):
            result.ca.items = {}  # pragma: no cover

    for new_item in new_list:
        if not isinstance(new_item, dict) or match_key not in new_item:
            # Can't match this item, just use new value
            result.append(new_item)  # pragma: no cover
            continue  # pragma: no cover

        key_value = new_item[match_key]
        if key_value in existing_by_key:
            old_idx, existing_item = existing_by_key[key_value]
            new_idx = len(result)

            # Copy comment that appears BEFORE this list item in the existing list
            # Comments can be in two places:
            # 1. ca.comment[idx+1] - comment before item at idx (for multi-line block style)
            # 2. ca.items[idx] - comment associated with item at idx (for single-line flow style)

            # Check ca.comment first
            if has_comment_list:
                comment_idx = (
                    old_idx + 1
                )  # Comment for item at old_idx is at comment[old_idx+1]
                if (
                    len(existing_list.ca.comment) > comment_idx
                    and existing_list.ca.comment[comment_idx]
                ):
                    new_comment_idx = new_idx + 1
                    # Ensure result.ca.comment is long enough
                    while len(result.ca.comment) <= new_comment_idx:
                        result.ca.comment.append(None)
                    # Copy the comment
                    result.ca.comment[new_comment_idx] = existing_list.ca.comment[
                        comment_idx
                    ]

            # Also check ca.items for comments (used in flow style)
            if has_items_dict and old_idx in existing_list.ca.items:
                # ca.items[idx] is a list: [pre_comment, eol_comment, post_comment, pre_done_comment]
                # We're interested in index 1 (the comment before/with the item)
                item_comments = existing_list.ca.items[old_idx]
                if (
                    item_comments and len(item_comments) > 1 and item_comments[1]
                ):  # pragma: no branch
                    # If we didn't already get a comment from ca.comment, use this one
                    new_comment_idx = new_idx + 1
                    while len(result.ca.comment) <= new_comment_idx:
                        result.ca.comment.append(None)
                    if not result.ca.comment[new_comment_idx]:  # pragma: no branch
                        result.ca.comment[new_comment_idx] = item_comments[1]

            # Item exists - merge to preserve comments
            if isinstance(existing_item, CommentedMap):
                # Use existing_item directly to preserve all ca attributes (including ca.items)
                # Update with new values
                for k, v in new_item.items():
                    # Special handling for attributes: preserve order if the set is unchanged
                    if k == "attributes" and "attributes" in existing_item:
                        existing_attrs = (
                            set(existing_item["attributes"])
                            if isinstance(existing_item["attributes"], list)
                            else set()
                        )
                        new_attrs = set(v) if isinstance(v, list) else set()
                        if existing_attrs == new_attrs:  # pragma: no branch
                            # Same attributes, keep existing order
                            continue
                    existing_item[k] = v
                # Remove keys that no longer exist
                for k in list(existing_item.keys()):
                    if k not in new_item:
                        del existing_item[k]
                result.append(existing_item)
            else:  # pragma: no cover
                # Existing item has no comments, just use new
                result.append(new_item)  # pragma: no cover
        else:  # pragma: no cover
            # New item - add it
            result.append(new_item)  # pragma: no cover

    return result


def _merge_yaml_preserving_comments(existing, new_data, yaml_handler):
    """
    Merge new data into existing YAML structure while preserving comments.

    Strategy:
    - For simple values: update in place
    - For lists of dicts with 'name' key (columns): match by name and merge items (preserves comments)
    - For lists of dicts with 'dimension_node' key (dimension_links): match and merge
    - For other lists: replace entirely
    - For dicts: recursively merge keys
    - Preserve comments attached to keys that still exist
    - Remove keys that no longer exist in new_data

    Args:
        existing: Existing YAML data loaded with ruamel.yaml (has comment info)
        new_data: New data dict to merge in
        yaml_handler: YAML handler instance

    Returns:
        Merged data structure with comments preserved
    """
    # If existing isn't a CommentedMap (loaded from YAML), just return new_data
    if not isinstance(existing, (dict, CommentedMap)):
        return new_data

    # Start with existing to preserve comments
    result = existing if isinstance(existing, CommentedMap) else CommentedMap(existing)

    # Remove keys that no longer exist in new_data
    keys_to_remove = [k for k in result.keys() if k not in new_data]
    for key in keys_to_remove:
        del result[key]

    # Update/add keys from new_data
    for key, new_value in new_data.items():
        if key in result:
            old_value = result[key]
            # For dicts, recursively merge
            if isinstance(new_value, dict) and isinstance(
                old_value,
                (dict, CommentedMap),
            ):
                result[key] = _merge_yaml_preserving_comments(  # pragma: no cover
                    old_value,
                    new_value,
                    yaml_handler,
                )
            # For lists of dicts with identifiable keys, do smart merge
            elif isinstance(new_value, list) and isinstance(old_value, list):
                # Check if this is a list we can intelligently merge
                if (
                    key in ("columns", "dimension_links")
                    and new_value
                    and isinstance(new_value[0], dict)
                ):
                    # Determine match key based on list type
                    if key == "columns" and "name" in new_value[0]:
                        result[key] = _merge_list_with_key(
                            old_value,
                            new_value,
                            match_key="name",
                        )
                    elif (
                        key == "dimension_links" and "dimension_node" in new_value[0]
                    ):  # pragma: no cover
                        result[key] = _merge_list_with_key(  # pragma: no cover
                            old_value,
                            new_value,
                            match_key="dimension_node",
                        )
                    else:  # pragma: no cover
                        # Fallback: replace entirely
                        result[key] = new_value  # pragma: no cover
                elif key in ("metrics", "dimensions"):
                    # For cube metrics/dimensions, preserve order from existing YAML
                    # Only include items that are in the new list (in case some were removed)
                    new_value_set = set(new_value)
                    preserved_order = [v for v in old_value if v in new_value_set]
                    # Add any new items that weren't in the old list
                    result[key] = preserved_order + [
                        v for v in new_value if v not in preserved_order
                    ]
                else:  # pragma: no cover
                    # Other lists: replace entirely
                    result[key] = new_value  # pragma: no cover
            else:
                # Simple value or type mismatch: replace
                result[key] = new_value
        else:
            # New key - just add it
            result[key] = new_value

    return result


def _node_spec_to_yaml_dict(node_spec, include_all_columns=False) -> dict:
    """
    Convert a NodeSpec to a dict suitable for YAML serialization.
    Excludes None values and empty lists for cleaner output.

    Args:
        node_spec: The node specification to convert
        include_all_columns: If True, include all columns (for comment preservation when merging).
                           If False (default), only include columns with meaningful customizations.

    For columns:
    - Cubes: only export columns that have partitions set (all other columns are inferred)
    - Other nodes:
      - If include_all_columns=True: include all columns (but exclude types)
      - If include_all_columns=False: only includes columns with meaningful customizations
        (display_name different from default, attributes, description, or partition).
    - Column types and order are always excluded - let DJ infer them from the query/source.
    """
    # Use model_dump with mode="json" to convert Enums to strings
    # Note: Don't use exclude_unset=True as it would exclude discriminator fields
    # like 'type' on dimension_links which have default values
    data = node_spec.model_dump(
        mode="json",  # Converts Enums to strings, datetimes to ISO format, etc.
        exclude_none=True,
        exclude={"namespace"},  # namespace is part of file path, not content
    )

    # Filter columns to only include meaningful customizations
    # Special case for cubes: ALWAYS only export columns with partitions
    # For other nodes: respect include_all_columns flag to preserve comments
    if "columns" in data and data["columns"] is not None:
        from datajunction_server.models.base import labelize

        is_cube = data.get("node_type") == "cube"

        # Cubes: always filter to only partitions (even when preserving comments)
        # Other nodes: only filter when include_all_columns=False
        should_filter = is_cube or not include_all_columns

        if should_filter:
            filtered_columns = []

            for col in data["columns"]:
                # Check for meaningful customizations
                # For display_name, check if it's different from both the full name AND the default
                col_name = col.get("name", "")
                display_name = col.get("display_name")
                default_display = (
                    labelize(col_name.split(".")[-1]) if col_name else None
                )
                has_custom_display = (
                    display_name
                    and display_name != col_name
                    and display_name != default_display
                )
                has_attributes = bool(col.get("attributes"))
                has_description = bool(col.get("description"))
                has_partition = bool(col.get("partition"))

                # For cubes: only include columns with partitions
                # For other nodes: include columns with any customization
                if is_cube:
                    should_include = has_partition
                else:
                    should_include = (
                        has_custom_display
                        or has_attributes
                        or has_description
                        or has_partition
                    )

                if should_include:
                    # Include column but exclude type and order (let DJ infer)
                    filtered_col = {
                        k: v
                        for k, v in col.items()
                        if k not in ("type", "order")
                        and v is not None  # Exclude type, order and None values
                        and v != []  # Exclude empty lists
                        and v != {}  # Exclude empty dicts
                    }
                    filtered_columns.append(filtered_col)

            if filtered_columns:
                data["columns"] = filtered_columns
            else:
                # Remove columns entirely if none have customizations
                del data["columns"]
        else:
            # include_all_columns=True for non-cube nodes (preserve all columns for comment merging)
            data["columns"] = [
                {
                    k: v
                    for k, v in col.items()
                    if k not in ("type", "order")
                    and v is not None
                    and v != []  # Exclude empty lists
                    and v != {}  # Exclude empty dicts
                }
                for col in data["columns"]
            ]
    elif "columns" in data and data["columns"] is None:  # pragma: no cover
        # If columns is explicitly None, remove it
        del data["columns"]

    # Remove empty lists/dicts for cleaner YAML
    data = {k: v for k, v in data.items() if v or v == 0 or v is False}

    # Clean up multiline strings by stripping trailing whitespace from each line
    # Use LiteralScalarString to force literal block style (|) for multiline queries
    if "query" in data and data["query"]:
        from ruamel.yaml.scalarstring import LiteralScalarString

        cleaned_query = "\n".join(line.rstrip() for line in data["query"].split("\n"))
        # Strip any trailing newlines to ensure proper YAML parsing
        cleaned_query = cleaned_query.rstrip("\n")
        # Only use literal block style if it's actually multiline
        if "\n" in cleaned_query:
            data["query"] = LiteralScalarString(cleaned_query)
        else:
            data["query"] = cleaned_query

    # Also clean join_on in dimension_links
    if "dimension_links" in data:
        from ruamel.yaml.scalarstring import LiteralScalarString

        for link in data["dimension_links"]:
            if "join_on" in link and link["join_on"]:
                cleaned_join = "\n".join(
                    line.rstrip() for line in link["join_on"].split("\n")
                )
                # Strip any trailing newlines to ensure proper YAML parsing
                cleaned_join = cleaned_join.rstrip("\n")
                # Use literal block style for multiline join conditions
                if "\n" in cleaned_join:  # pragma: no cover
                    link["join_on"] = LiteralScalarString(cleaned_join)
                else:
                    link["join_on"] = cleaned_join

    return data


def node_spec_to_yaml(node_spec, existing_yaml: str | None = None) -> str:
    """
    Convert a NodeSpec to formatted YAML string.

    If existing_yaml is provided, merges the new data with existing content
    to preserve comments and ordering.

    Args:
        node_spec: The node specification to convert
        existing_yaml: Optional existing YAML content with comments to preserve

    Returns:
        Formatted YAML string with comments preserved (if existing_yaml provided)
    """
    # When merging with existing YAML, include all columns (even without customizations)
    # so we can match and preserve comments on them
    include_all_columns = existing_yaml is not None
    yaml_dict = _node_spec_to_yaml_dict(
        node_spec,
        include_all_columns=include_all_columns,
    )
    yaml_handler = _get_yaml_handler()

    # If existing YAML provided, load it and merge to preserve comments
    if existing_yaml:
        try:
            existing_data = yaml_handler.load(existing_yaml)
            if existing_data:  # pragma: no branch
                # Merge: update existing data with new values
                merged_data = _merge_yaml_preserving_comments(
                    existing_data,
                    yaml_dict,
                    yaml_handler,
                )
                yaml_dict = merged_data
        except Exception as e:  # pragma: no cover
            logger.warning("Failed to preserve comments from existing YAML: %s", e)
            # Fall through to export without comment preservation

    # Export to YAML
    stream = StringIO()
    yaml_handler.dump(yaml_dict, stream)
    return stream.getvalue()


# Git configuration validation helpers


async def resolve_git_config(
    session: AsyncSession,
    namespace: str,
    max_depth: int = 50,
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Resolve complete git configuration by walking up parent chain.

    Returns (github_repo_path, git_path, git_branch) tuple.
    - github_repo_path and git_path are resolved by walking up parents
    - git_branch is only taken from the current namespace (not inherited)

    This enables hierarchical configuration:
    - Parent namespace (e.g., "demo.metrics") has repo/path
    - Child namespaces (e.g., "demo.metrics.main") have branch + parent link

    Args:
        session: Database session
        namespace: Namespace to resolve config for
        max_depth: Maximum parent chain depth (prevents infinite loops)

    Returns:
        Tuple of (github_repo_path, git_path, git_branch)
        Any element can be None if not configured

    Raises:
        DJDoesNotExistException: If namespace doesn't exist
        DJInvalidInputException: If parent chain exceeds max_depth
    """
    from datajunction_server.api.helpers import get_node_namespace

    current_ns = await get_node_namespace(session, namespace)

    # git_branch is never inherited - only from current namespace
    git_branch = current_ns.git_branch

    # Start with current namespace's direct values
    github_repo_path = current_ns.github_repo_path
    git_path = current_ns.git_path

    # Walk up parent chain to resolve missing fields
    current = current_ns
    depth = 0

    while (not github_repo_path or git_path is None) and current.parent_namespace:
        if depth >= max_depth:
            raise DJInvalidInputException(  # pragma: no cover
                message=f"Parent chain exceeds maximum depth of {max_depth} "
                f"while resolving git config for '{namespace}'",
            )

        parent_ns = await get_node_namespace(session, current.parent_namespace)

        # Inherit missing fields from parent
        if not github_repo_path:  # pragma: no branch
            github_repo_path = parent_ns.github_repo_path
        if git_path is None:  # pragma: no branch
            git_path = parent_ns.git_path

        current = parent_ns
        depth += 1

    return github_repo_path, git_path, git_branch


def validate_sibling_relationship(
    child_namespace: str,
    parent_namespace: str,
) -> None:
    """
    Ensure parent-child namespace relationships are valid.

    Two valid patterns:
    1. Siblings: demo.main can have parent demo.feature (both have prefix "demo")
    2. Direct child: demo.main can have parent demo (child's prefix is "demo")

    Invalid: demo.feature cannot have parent team.main (different projects)
    """

    def get_prefix(ns: str) -> str:
        return ns.rsplit(".", 1)[0] if "." in ns else ""

    child_prefix = get_prefix(child_namespace)
    parent_prefix = get_prefix(parent_namespace)

    # Allow either:
    # 1. Siblings: child and parent have same prefix (e.g., demo.main + demo.feature)
    # 2. Direct child: child's prefix equals parent's namespace (e.g., demo.main -> demo)
    is_sibling = child_prefix == parent_prefix
    is_direct_child = child_prefix == parent_namespace

    if not (is_sibling or is_direct_child):
        raise DJInvalidInputException(
            message=(
                f"Namespace '{child_namespace}' (prefix: '{child_prefix}') "
                f"cannot have parent '{parent_namespace}'. "
                f"Expected parent to either be '{child_prefix}' (direct parent) "
                f"or have prefix '{child_prefix}' (sibling)."
            ),
        )


async def detect_parent_cycle(
    session: AsyncSession,
    child_namespace: str,
    new_parent: str,
    max_depth: int = 50,
) -> None:
    """
    Detect cycles in parent_namespace relationships.

    Prevents circular dependencies like: A -> B -> C -> A
    """
    visited = [child_namespace]
    visited_set = {child_namespace}
    current = new_parent
    depth = 0

    while current and depth < max_depth:
        if current in visited_set:
            raise DJInvalidInputException(
                message=f"Circular parent reference detected: {' -> '.join(visited)} -> {current}",
            )

        visited.append(current)
        visited_set.add(current)

        # Fetch parent of current
        stmt = select(NodeNamespace.parent_namespace).where(
            NodeNamespace.namespace == current,
        )
        result = await session.execute(stmt)
        current = result.scalar_one_or_none()
        depth += 1

    if depth >= max_depth:
        raise DJInvalidInputException(
            message=f"Parent chain exceeds maximum depth of {max_depth}",
        )


def validate_git_path(git_path: Optional[str]) -> None:
    """
    Ensure git_path doesn't escape repository boundaries.

    Blocks path traversal attacks (..) and absolute paths (/).
    """
    if not git_path:
        return

    # Normalize path
    normalized = git_path.strip()

    # Block path traversal
    if ".." in normalized:
        raise DJInvalidInputException(
            message="git_path cannot contain '..' (path traversal)",
        )

    # Block absolute paths
    if normalized.startswith("/"):
        raise DJInvalidInputException(
            message="git_path must be a relative path (cannot start with '/')",
        )
