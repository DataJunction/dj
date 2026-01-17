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
from datajunction_server.models.node import NodeMinimumDetail, NodeStatus
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.dag import (
    get_downstream_nodes,
    get_nodes_with_common_dimensions,
    topological_sort,
)
from datajunction_server.typing import UTCDatetime
from datajunction_server.utils import SEPARATOR

import logging
from typing import Callable, Dict, List, cast

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
from datajunction_server.models.impact import (
    NamespaceDiffResponse,
    ColumnChange,
    ColumnChangeType,
)

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
    nodes = await NodeNamespace.list_all_nodes(
        session,
        namespace,
        options=Node.cube_load_options(),
    )
    node_specs = [await node.to_spec(session) for node in nodes]

    for node_spec in node_specs:
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
            cube_spec.metrics = [
                inject_prefixes(metric, namespace) for metric in node_spec.metrics
            ]
            cube_spec.dimensions = [
                inject_prefixes(dimension, namespace)
                for dimension in node_spec.dimensions
            ]

    return node_specs


def _multiline_str_representer(dumper, data):
    """
    Custom YAML representer that uses literal block style (|) for multiline strings.
    """
    if "\n" in data:
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
    return {k: v for k, v in data.items() if v or v == 0 or v is False}


# =============================================================================
# Namespace Diff
# =============================================================================


async def compare_namespaces(
    session: AsyncSession,
    base_namespace: str,
    compare_namespace: str,
) -> NamespaceDiffResponse:
    """
    Compare two namespaces and return a diff showing:
    - Nodes added in compare (exist only in compare)
    - Nodes removed in compare (exist only in base)
    - Direct changes (user-provided fields differ)
    - Propagated changes (only system-derived fields differ: status, version)

    Args:
        session: Database session
        base_namespace: The "before" namespace (e.g., "dj.main")
        compare_namespace: The "after" namespace (e.g., "dj.feature-123")

    Returns:
        NamespaceDiffResponse with categorized changes
    """
    from datajunction_server.models.impact import (
        NamespaceDiffAddedNode,
        NamespaceDiffChangeType,
        NamespaceDiffNodeChange,
        NamespaceDiffRemovedNode,
        NamespaceDiffResponse,
    )

    # Fetch all nodes from both namespaces
    base_nodes = await NodeNamespace.list_all_nodes(
        session,
        base_namespace,
        options=Node.cube_load_options(),
    )
    compare_nodes = await NodeNamespace.list_all_nodes(
        session,
        compare_namespace,
        options=Node.cube_load_options(),
    )

    # Convert to specs for comparison
    base_specs = {node.name: await node.to_spec(session) for node in base_nodes}
    compare_specs = {node.name: await node.to_spec(session) for node in compare_nodes}

    # Create maps by node name (without namespace prefix) for matching
    base_nodes_map = {node.name: node for node in base_nodes}
    compare_nodes_map = {node.name: node for node in compare_nodes}

    def strip_namespace(full_name: str, namespace: str) -> str:
        """Strip namespace prefix from node name."""
        prefix = namespace + SEPARATOR
        if full_name.startswith(prefix):
            return full_name[len(prefix) :]
        return full_name

    # Map nodes by relative name (without namespace) for matching
    base_by_relative = {
        strip_namespace(name, base_namespace): (name, spec)
        for name, spec in base_specs.items()
    }
    compare_by_relative = {
        strip_namespace(name, compare_namespace): (name, spec)
        for name, spec in compare_specs.items()
    }

    # Find added, removed, and common nodes
    base_relative_names = set(base_by_relative.keys())
    compare_relative_names = set(compare_by_relative.keys())

    added_relative = compare_relative_names - base_relative_names
    removed_relative = base_relative_names - compare_relative_names
    common_relative = base_relative_names & compare_relative_names

    # Build response
    added: list[NamespaceDiffAddedNode] = []
    removed: list[NamespaceDiffRemovedNode] = []
    direct_changes: list[NamespaceDiffNodeChange] = []
    propagated_changes: list[NamespaceDiffNodeChange] = []
    unchanged_count = 0

    # Process added nodes
    for rel_name in sorted(added_relative):
        full_name, spec = compare_by_relative[rel_name]
        node = compare_nodes_map.get(full_name)
        added.append(
            NamespaceDiffAddedNode(
                name=rel_name,
                full_name=full_name,
                node_type=spec.node_type,
                display_name=spec.display_name,
                description=spec.description,
                status=node.current.status if node and node.current else None,
                version=node.current.version if node and node.current else None,
            ),
        )

    # Process removed nodes
    for rel_name in sorted(removed_relative):
        full_name, spec = base_by_relative[rel_name]
        node = base_nodes_map.get(full_name)
        removed.append(
            NamespaceDiffRemovedNode(
                name=rel_name,
                full_name=full_name,
                node_type=spec.node_type,
                display_name=spec.display_name,
                description=spec.description,
                status=node.current.status if node and node.current else None,
                version=node.current.version if node and node.current else None,
            ),
        )

    # Process common nodes - compare specs
    for rel_name in sorted(common_relative):
        base_full_name, base_spec = base_by_relative[rel_name]
        compare_full_name, compare_spec = compare_by_relative[rel_name]

        base_node = base_nodes_map.get(base_full_name)
        compare_node = compare_nodes_map.get(compare_full_name)

        # Get system-derived fields
        base_version = (
            base_node.current.version if base_node and base_node.current else None
        )
        compare_version = (
            compare_node.current.version
            if compare_node and compare_node.current
            else None
        )
        base_status = (
            base_node.current.status if base_node and base_node.current else None
        )
        compare_status = (
            compare_node.current.status
            if compare_node and compare_node.current
            else None
        )

        # Compare user-provided fields via spec comparison
        # The spec __eq__ compares user-provided fields (query, display_name, etc.)
        specs_equal = _compare_specs_for_diff(
            base_spec,
            compare_spec,
            base_namespace,
            compare_namespace,
        )

        if specs_equal:
            # User-provided fields are the same
            # Check if system-derived fields differ (propagated change)
            if base_version != compare_version or base_status != compare_status:
                # Propagated change - only system fields differ
                propagated_changes.append(
                    NamespaceDiffNodeChange(
                        name=rel_name,
                        full_name=compare_full_name,
                        node_type=compare_spec.node_type,
                        change_type=NamespaceDiffChangeType.PROPAGATED,
                        base_version=base_version,
                        compare_version=compare_version,
                        base_status=base_status,
                        compare_status=compare_status,
                        propagation_reason=_get_propagation_reason(
                            base_version,
                            compare_version,
                            base_status,
                            compare_status,
                        ),
                    ),
                )
            else:
                # Completely unchanged
                unchanged_count += 1
        else:
            # User-provided fields differ - direct change
            changed_fields = (
                base_spec.diff(compare_spec) if hasattr(base_spec, "diff") else []
            )
            column_changes = _detect_column_changes_for_diff(
                base_node,
                compare_node,
                base_namespace,
                compare_namespace,
            )

            direct_changes.append(
                NamespaceDiffNodeChange(
                    name=rel_name,
                    full_name=compare_full_name,
                    node_type=compare_spec.node_type,
                    change_type=NamespaceDiffChangeType.DIRECT,
                    base_version=base_version,
                    compare_version=compare_version,
                    base_status=base_status,
                    compare_status=compare_status,
                    changed_fields=changed_fields,
                    column_changes=column_changes,
                ),
            )

    # Try to trace causes for propagated changes
    # A propagated change is likely caused by a direct change in an upstream node
    direct_change_names = {dc.name for dc in direct_changes}
    for prop_change in propagated_changes:
        compare_full_name = prop_change.full_name
        compare_node = compare_nodes_map.get(compare_full_name)
        if compare_node and compare_node.current:
            # Check if any parent is in the direct changes
            parent_names = [p.name for p in compare_node.current.parents]
            for parent_name in parent_names:
                parent_rel_name = strip_namespace(parent_name, compare_namespace)
                if parent_rel_name in direct_change_names:
                    prop_change.caused_by.append(parent_rel_name)

    return NamespaceDiffResponse(
        base_namespace=base_namespace,
        compare_namespace=compare_namespace,
        added=added,
        removed=removed,
        direct_changes=direct_changes,
        propagated_changes=propagated_changes,
        unchanged_count=unchanged_count,
        added_count=len(added),
        removed_count=len(removed),
        direct_change_count=len(direct_changes),
        propagated_change_count=len(propagated_changes),
    )


def _strip_namespace_from_ref(ref: str, namespace: str) -> str:
    """
    Strip namespace prefix from a reference (node name, column ref, etc.).

    Handles both regular format (namespace.node) and amenable name format
    (namespace_DOT_node) where dots are replaced with _DOT_.
    """
    # Try regular format first: "namespace.node"
    prefix = namespace + SEPARATOR
    if ref.startswith(prefix):
        return ref[len(prefix) :]

    # Try amenable name format: "namespace_DOT_node"
    amenable_prefix = namespace.replace(".", "_DOT_") + "_DOT_"
    if ref.startswith(amenable_prefix):
        return ref[len(amenable_prefix) :]

    return ref


def _normalize_refs_in_set(refs: set[str], namespace: str) -> set[str]:
    """Normalize a set of references by stripping namespace prefix."""
    return {_strip_namespace_from_ref(r, namespace) for r in refs}


def _compare_specs_for_diff(
    base_spec: NodeSpec,
    compare_spec: NodeSpec,
    base_namespace: str,
    compare_namespace: str,
) -> bool:
    """
    Compare two specs for the namespace diff.

    This compares user-provided fields only, ignoring namespace differences.
    We need to normalize the specs to compare them fairly since they have
    different namespace prefixes.
    """
    # Node types must match
    if base_spec.node_type != compare_spec.node_type:
        return False

    # Compare user-provided metadata fields
    if base_spec.display_name != compare_spec.display_name:
        # Handle None vs empty string as equivalent
        if not (
            base_spec.display_name in (None, "")
            and compare_spec.display_name in (None, "")
        ):
            return False

    if base_spec.description != compare_spec.description:
        if not (
            base_spec.description in (None, "")
            and compare_spec.description in (None, "")
        ):
            return False

    if set(base_spec.owners or []) != set(compare_spec.owners or []):
        return False

    if set(base_spec.tags or []) != set(compare_spec.tags or []):
        return False

    if base_spec.mode != compare_spec.mode:
        return False

    if (base_spec.custom_metadata or {}) != (compare_spec.custom_metadata or {}):
        return False

    # Type-specific comparisons
    if base_spec.node_type == NodeType.SOURCE:
        # Compare catalog, schema, table
        if (
            base_spec.catalog != compare_spec.catalog
            or base_spec.schema_ != compare_spec.schema_
            or base_spec.table != compare_spec.table
        ):
            return False
        # Compare columns for sources (user-provided)
        if not _compare_columns_for_diff(
            base_spec.columns,
            compare_spec.columns,
            base_namespace,
            compare_namespace,
            compare_types=True,
        ):
            return False

    elif base_spec.node_type in (NodeType.TRANSFORM, NodeType.DIMENSION):
        # Compare query (normalize namespace references)
        if not _compare_queries_for_diff(
            base_spec.query,
            compare_spec.query,
            base_namespace,
            compare_namespace,
        ):
            return False
        # Compare columns (only user-provided metadata like display_name, attributes)
        if not _compare_columns_for_diff(
            base_spec.columns,
            compare_spec.columns,
            base_namespace,
            compare_namespace,
            compare_types=False,
        ):
            return False

    elif base_spec.node_type == NodeType.METRIC:
        # Compare query (normalize namespace references)
        if not _compare_queries_for_diff(
            base_spec.query,
            compare_spec.query,
            base_namespace,
            compare_namespace,
        ):
            return False
        # Compare required_dimensions (normalize namespace)
        base_req_dims = _normalize_refs_in_set(
            set(base_spec.required_dimensions or []),
            base_namespace,
        )
        compare_req_dims = _normalize_refs_in_set(
            set(compare_spec.required_dimensions or []),
            compare_namespace,
        )
        if base_req_dims != compare_req_dims:
            return False
        # Compare metric metadata
        if base_spec.direction != compare_spec.direction:
            return False
        # Compare unit_enum - handle None vs falsy cases
        base_unit = base_spec.unit_enum
        compare_unit = compare_spec.unit_enum
        if base_unit != compare_unit:
            # Both None is equal, handle value comparison
            if not (base_unit is None and compare_unit is None):
                return False

    elif base_spec.node_type == NodeType.CUBE:
        # Compare metrics and dimensions lists (normalize namespace)
        base_metrics = _normalize_refs_in_set(
            set(base_spec.metrics or []),
            base_namespace,
        )
        compare_metrics = _normalize_refs_in_set(
            set(compare_spec.metrics or []),
            compare_namespace,
        )
        if base_metrics != compare_metrics:
            return False

        base_dims = _normalize_refs_in_set(
            set(base_spec.dimensions or []),
            base_namespace,
        )
        compare_dims = _normalize_refs_in_set(
            set(compare_spec.dimensions or []),
            compare_namespace,
        )
        if base_dims != compare_dims:
            return False

        # Compare cube columns (normalize namespace in column names)
        if not _compare_cube_columns_for_diff(
            base_spec.columns,
            compare_spec.columns,
            base_namespace,
            compare_namespace,
        ):
            return False

    # Compare dimension links for linkable nodes
    if base_spec.node_type in (NodeType.SOURCE, NodeType.TRANSFORM, NodeType.DIMENSION):
        if not _compare_dimension_links_for_diff(
            base_spec.dimension_links or [],
            compare_spec.dimension_links or [],
            base_namespace,
            compare_namespace,
        ):
            return False

    return True


def _compare_queries_for_diff(
    base_query: str | None,
    compare_query: str | None,
    base_namespace: str,
    compare_namespace: str,
) -> bool:
    """
    Compare two SQL queries for semantic equivalence.

    Normalizes namespace references so that queries referencing nodes
    in their respective namespaces are compared as equivalent.
    """
    if base_query is None and compare_query is None:
        return True
    if base_query is None or compare_query is None:
        return False

    def normalize(q: str, namespace: str) -> str:
        # Normalize whitespace and case
        normalized = " ".join(q.split()).lower().strip()
        # Replace namespace prefix with a placeholder for comparison
        # This handles references like "namespace.node" -> "${ns}.node"
        ns_prefix = namespace.lower() + "."
        normalized = normalized.replace(ns_prefix, "${ns}.")
        return normalized

    return normalize(base_query, base_namespace) == normalize(
        compare_query,
        compare_namespace,
    )


def _compare_columns_for_diff(
    base_columns: list | None,
    compare_columns: list | None,
    base_namespace: str,
    compare_namespace: str,
    compare_types: bool = False,
) -> bool:
    """
    Compare column lists for equivalence.

    Args:
        base_columns: Columns from base spec
        compare_columns: Columns from compare spec
        base_namespace: Base namespace for normalizing references
        compare_namespace: Compare namespace for normalizing references
        compare_types: If True, compare column types (for SOURCE nodes)
    """
    if not base_columns and not compare_columns:
        return True
    if not base_columns or not compare_columns:
        # One is empty, the other is not
        return False

    if len(base_columns) != len(compare_columns):
        return False

    # Normalize column names by stripping namespace prefix
    base_by_name = {
        _strip_namespace_from_ref(c.name, base_namespace): c for c in base_columns
    }
    compare_by_name = {
        _strip_namespace_from_ref(c.name, compare_namespace): c for c in compare_columns
    }

    if set(base_by_name.keys()) != set(compare_by_name.keys()):
        return False

    for name, base_col in base_by_name.items():
        compare_col = compare_by_name[name]

        if compare_types and base_col.type != compare_col.type:
            return False

        # Compare user-provided metadata
        if base_col.display_name != compare_col.display_name:
            return False
        if base_col.description != compare_col.description:
            return False
        if set(base_col.attributes or []) != set(compare_col.attributes or []):
            return False

    return True


def _compare_cube_columns_for_diff(
    base_columns: list | None,
    compare_columns: list | None,
    base_namespace: str,
    compare_namespace: str,
) -> bool:
    """
    Compare cube column lists for equivalence.

    Cube columns have names that are fully qualified metric/dimension references
    (e.g., "namespace.metric_name"), so we need to normalize these.
    """
    if not base_columns and not compare_columns:
        return True
    if not base_columns or not compare_columns:
        return False

    if len(base_columns) != len(compare_columns):
        return False

    # Normalize column names by stripping namespace prefix
    base_by_name = {
        _strip_namespace_from_ref(c.name, base_namespace): c for c in base_columns
    }
    compare_by_name = {
        _strip_namespace_from_ref(c.name, compare_namespace): c for c in compare_columns
    }

    if set(base_by_name.keys()) != set(compare_by_name.keys()):
        return False

    # For cube columns, we mainly care about the names matching
    # Types are derived so we don't compare them strictly
    for name, base_col in base_by_name.items():
        compare_col = compare_by_name[name]

        # Compare user-provided metadata if present
        if base_col.display_name != compare_col.display_name:
            # Allow if both are None or match the normalized name
            base_display = base_col.display_name or ""
            compare_display = compare_col.display_name or ""
            if base_display and compare_display and base_display != compare_display:
                return False

        if base_col.description != compare_col.description:
            if base_col.description and compare_col.description:
                return False

        # Compare partition config if present
        base_partition = getattr(base_col, "partition", None)
        compare_partition = getattr(compare_col, "partition", None)
        if base_partition != compare_partition:
            return False

    return True


def _compare_dimension_links_for_diff(
    base_links: list,
    compare_links: list,
    base_namespace: str,
    compare_namespace: str,
) -> bool:
    """
    Compare dimension link lists for equivalence.

    Normalizes namespace references in dimension_node and join_on fields.
    """
    if len(base_links) != len(compare_links):
        return False

    # Sort by a consistent key for comparison (with normalized names)
    def link_key(link, namespace):
        if hasattr(link, "dimension_node"):
            dim = _strip_namespace_from_ref(link.dimension_node or "", namespace)
            return (link.type, dim, link.role or "")
        dim = _strip_namespace_from_ref(link.dimension or "", namespace)
        return (link.type, dim, link.role or "")

    base_sorted = sorted(base_links, key=lambda link: link_key(link, base_namespace))
    compare_sorted = sorted(
        compare_links,
        key=lambda link: link_key(link, compare_namespace),
    )

    for base_link, compare_link in zip(base_sorted, compare_sorted):
        if base_link.type != compare_link.type:
            return False
        if base_link.role != compare_link.role:
            return False

        if hasattr(base_link, "dimension_node"):
            # JOIN link - normalize dimension_node references
            base_dim = _strip_namespace_from_ref(
                base_link.dimension_node or "",
                base_namespace,
            )
            compare_dim = _strip_namespace_from_ref(
                compare_link.dimension_node or "",
                compare_namespace,
            )
            if base_dim != compare_dim:
                return False

            if base_link.join_type != compare_link.join_type:
                return False

            # Normalize join_on for comparison (whitespace and namespace)
            base_join = " ".join((base_link.join_on or "").split()).lower()
            compare_join = " ".join((compare_link.join_on or "").split()).lower()
            # Replace namespace references
            base_join = base_join.replace(base_namespace.lower() + ".", "${ns}.")
            compare_join = compare_join.replace(
                compare_namespace.lower() + ".",
                "${ns}.",
            )
            if base_join != compare_join:
                return False
        else:
            # REFERENCE link - normalize dimension column comparison
            base_dim = _strip_namespace_from_ref(
                base_link.dimension or "",
                base_namespace,
            )
            compare_dim = _strip_namespace_from_ref(
                compare_link.dimension or "",
                compare_namespace,
            )
            if base_dim != compare_dim:
                return False

    return True


def _detect_column_changes_for_diff(
    base_node: Node | None,
    compare_node: Node | None,
    base_namespace: str,
    compare_namespace: str,
) -> list[ColumnChange]:
    """
    Detect column changes between two nodes.

    Column names are normalized by stripping namespace prefixes to ensure
    columns like "ns1.metric_name" and "ns2.metric_name" are compared as equal.
    """
    changes: list[ColumnChange] = []

    if (
        not base_node
        or not base_node.current
        or not compare_node
        or not compare_node.current
    ):
        return changes

    # Normalize column names by stripping namespace prefix
    base_columns = {
        _strip_namespace_from_ref(col.name, base_namespace): col
        for col in base_node.current.columns
    }
    compare_columns = {
        _strip_namespace_from_ref(col.name, compare_namespace): col
        for col in compare_node.current.columns
    }

    # Removed columns
    for col_name in base_columns.keys() - compare_columns.keys():
        changes.append(
            ColumnChange(
                column=col_name,
                change_type=ColumnChangeType.REMOVED,
                old_type=str(base_columns[col_name].type),
            ),
        )

    # Added columns
    for col_name in compare_columns.keys() - base_columns.keys():
        changes.append(
            ColumnChange(
                column=col_name,
                change_type=ColumnChangeType.ADDED,
                new_type=str(compare_columns[col_name].type),
            ),
        )

    # Type changes
    for col_name in base_columns.keys() & compare_columns.keys():
        old_type = str(base_columns[col_name].type)
        new_type = str(compare_columns[col_name].type)
        if old_type != new_type:
            changes.append(
                ColumnChange(
                    column=col_name,
                    change_type=ColumnChangeType.TYPE_CHANGED,
                    old_type=old_type,
                    new_type=new_type,
                ),
            )

    return changes


def _get_propagation_reason(
    base_version: str | None,
    compare_version: str | None,
    base_status: NodeStatus | None,
    compare_status: NodeStatus | None,
) -> str:
    """
    Generate a human-readable reason for a propagated change.
    """
    reasons = []

    if base_version != compare_version:
        reasons.append(f"version changed from {base_version} to {compare_version}")

    if base_status != compare_status:
        base_status_str = base_status.value if base_status else "unknown"
        compare_status_str = compare_status.value if compare_status else "unknown"
        reasons.append(f"status changed from {base_status_str} to {compare_status_str}")

    return "; ".join(reasons) if reasons else "system-derived fields changed"
