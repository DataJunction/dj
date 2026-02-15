"""
Node resolvers
"""

from collections import OrderedDict
from typing import Any, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import defer, joinedload, load_only, noload, selectinload
from strawberry.types import Info

from datajunction_server.errors import DJNodeNotFound
from datajunction_server.api.graphql.scalars.node import NodeName, NodeSortField
from datajunction_server.api.graphql.scalars.sql import CubeDefinition
from datajunction_server.api.graphql.utils import dedupe_append, extract_fields
from datajunction_server.database.dimensionlink import DimensionLink
from datajunction_server.database.node import Column, ColumnAttribute
from datajunction_server.database.node import Node as DBNode
from datajunction_server.database.node import NodeRevision as DBNodeRevision
from datajunction_server.models.node import NodeMode, NodeStatus, NodeType


async def find_nodes_by(
    info: Info,
    names: Optional[List[str]] = None,
    fragment: Optional[str] = None,
    node_types: Optional[List[NodeType]] = None,
    tags: Optional[List[str]] = None,
    edited_by: Optional[str] = None,
    namespace: Optional[str] = None,
    limit: Optional[int] = 100,
    before: Optional[str] = None,
    after: Optional[str] = None,
    order_by: NodeSortField = NodeSortField.CREATED_AT,
    ascending: bool = False,
    mode: Optional[NodeMode] = None,
    owned_by: Optional[str] = None,
    missing_description: bool = False,
    missing_owner: bool = False,
    dimensions: Optional[List[str]] = None,
    statuses: Optional[List[NodeStatus]] = None,
    has_materialization: bool = False,
    orphaned_dimension: bool = False,
) -> List[DBNode]:
    """
    Finds nodes based on the search parameters. This function also tries to optimize
    the database query by only retrieving joined-in fields if they were requested.
    """
    session = info.context["session"]  # type: ignore
    fields = extract_fields(info)
    options = load_node_options(
        fields["nodes"]
        if "nodes" in fields
        else fields["edges"]["node"]
        if "edges" in fields
        else fields,
    )
    return await DBNode.find_by(
        session,
        names,
        fragment,
        node_types,
        tags,
        edited_by,
        namespace,
        limit,
        before,
        after,
        order_by=order_by.column,
        ascending=ascending,
        options=options,
        mode=mode,
        owned_by=owned_by,
        missing_description=missing_description,
        missing_owner=missing_owner,
        statuses=statuses,
        has_materialization=has_materialization,
        orphaned_dimension=orphaned_dimension,
        dimensions=dimensions,
    )


async def get_node_by_name(
    session: AsyncSession,
    fields: dict[str, Any] | None,
    name: str,
) -> DBNode | NodeName | None:
    """
    Retrieves a node by name. This function also tries to optimize the database
    query by only retrieving joined-in fields if they were requested.
    """
    if not fields:
        return None  # pragma: no cover
    if "name" in fields and len(fields) == 1:
        return NodeName(name=name)  # type: ignore

    options = load_node_options(
        fields["nodes"]
        if "nodes" in fields
        else fields["edges"]["node"]
        if "edges" in fields
        else fields,
    )
    return await DBNode.get_by_name(
        session,
        name=name,
        options=options,
    )


def load_node_options(fields):
    """
    Based on the GraphQL query input fields, builds a list of node load options.
    Uses noload() to prevent lazy loading for unrequested fields.
    """
    options = []

    if "revisions" in fields:
        node_revision_options = load_node_revision_options(fields["revisions"])
        options.append(joinedload(DBNode.revisions).options(*node_revision_options))
    else:
        options.append(noload(DBNode.revisions))

    if fields.get("current"):
        node_revision_options = load_node_revision_options(fields["current"])
        options.append(joinedload(DBNode.current).options(*node_revision_options))
    else:
        options.append(noload(DBNode.current))

    if "created_by" in fields:
        options.append(selectinload(DBNode.created_by))
    else:
        options.append(noload(DBNode.created_by))

    if "owners" in fields:
        options.append(selectinload(DBNode.owners))
    else:
        options.append(noload(DBNode.owners))

    if "edited_by" in fields:
        options.append(selectinload(DBNode.history))
    else:
        options.append(noload(DBNode.history))

    if "tags" in fields:
        options.append(selectinload(DBNode.tags))
    else:
        options.append(noload(DBNode.tags))

    if "git_info" in fields:
        from datajunction_server.database.namespace import NodeNamespace

        # Load namespace and its parent for git_info
        options.append(
            joinedload(DBNode.namespace_obj).joinedload(
                NodeNamespace.parent_namespace_obj,
            ),
        )
    else:
        options.append(noload(DBNode.namespace_obj))

    # Always noload children - not exposed in GraphQL
    options.append(noload(DBNode.children))

    return options


def build_cube_metrics_node_revision_options(requested_fields):
    """
    Build loading options for NodeRevision objects returned by cube_metrics.
    cube_metrics returns List[NodeRevision], so Strawberry will serialize them
    based on the requested subfields.
    """
    options = [
        defer(DBNodeRevision.query_ast),
        # Always noload these - not needed for cube_metrics NodeRevision
        noload(DBNodeRevision.node),
        noload(DBNodeRevision.created_by),
        noload(DBNodeRevision.missing_parents),
        noload(DBNodeRevision.cube_elements),
        noload(DBNodeRevision.required_dimensions),
        noload(DBNodeRevision.parents),
        noload(DBNodeRevision.dimension_links),
        noload(DBNodeRevision.availability),
        noload(DBNodeRevision.materializations),
    ]

    # Only load relationships if they're requested
    if requested_fields and "columns" in requested_fields:
        options.append(
            selectinload(DBNodeRevision.columns).options(
                joinedload(Column.attributes).joinedload(
                    ColumnAttribute.attribute_type,
                ),
                joinedload(Column.dimension),
                joinedload(Column.partition),
            ),
        )
    else:
        options.append(noload(DBNodeRevision.columns))

    if requested_fields and "catalog" in requested_fields:
        options.append(joinedload(DBNodeRevision.catalog))
    else:
        options.append(noload(DBNodeRevision.catalog))

    if requested_fields and "metric_metadata" in requested_fields:
        options.append(selectinload(DBNodeRevision.metric_metadata))
    else:
        options.append(noload(DBNodeRevision.metric_metadata))

    return options


def build_cube_dimensions_node_revision_options():
    """
    Build loading options for NodeRevision objects used by cube_dimensions.
    cube_dimensions returns List[DimensionAttribute] which is constructed manually,
    so we only need minimal NodeRevision fields (name, type).
    """
    return [
        # Only load what cube_dimensions needs: name, type
        load_only(DBNodeRevision.id, DBNodeRevision.name, DBNodeRevision.type),
        defer(DBNodeRevision.query_ast),
        # Noload all relationships - cube_dimensions builds DimensionAttribute manually
        noload(DBNodeRevision.node),
        noload(DBNodeRevision.created_by),
        noload(DBNodeRevision.missing_parents),
        noload(DBNodeRevision.cube_elements),
        noload(DBNodeRevision.required_dimensions),
        noload(DBNodeRevision.parents),
        noload(DBNodeRevision.dimension_links),
        noload(DBNodeRevision.availability),
        noload(DBNodeRevision.materializations),
        noload(DBNodeRevision.columns),
        noload(DBNodeRevision.catalog),
        noload(DBNodeRevision.metric_metadata),
    ]


def load_node_revision_options(node_revision_fields):
    """
    Based on the GraphQL query input fields, builds a list of node revision
    load options. Uses noload() to prevent lazy loading for unrequested fields.
    """
    options = [defer(DBNodeRevision.query_ast)]
    is_cube_request = (
        "cube_metrics" in node_revision_fields
        or "cube_dimensions" in node_revision_fields
    )

    # Get the subfields requested for cube_metrics/cube_dimensions
    cube_metric_fields = (
        node_revision_fields.get("cube_metrics") if node_revision_fields else None
    )
    cube_dimension_fields = (
        node_revision_fields.get("cube_dimensions") if node_revision_fields else None
    )

    # Handle columns
    if "columns" in node_revision_fields or "primary_key" in node_revision_fields:
        # Full columns with all relationships needed for columns/primary_key queries
        options.append(
            selectinload(DBNodeRevision.columns).options(
                joinedload(Column.attributes).joinedload(
                    ColumnAttribute.attribute_type,
                ),
                joinedload(Column.dimension),
                joinedload(Column.partition),
            ),
        )
    elif is_cube_request:
        # Minimal columns for cube_metrics/cube_dimensions
        # Only load the columns we need: name, order, dimension_column
        options.append(
            selectinload(DBNodeRevision.columns).options(
                load_only(
                    Column.id,
                    Column.name,
                    Column.order,
                    Column.dimension_column,
                ),
                noload(Column.attributes),
                noload(Column.dimension),
                noload(Column.partition),
            ),
        )
    else:
        options.append(noload(DBNodeRevision.columns))

    # Handle catalog - has lazy="joined" by default which we want to prevent
    if "catalog" in node_revision_fields:
        options.append(joinedload(DBNodeRevision.catalog))
    else:
        options.append(noload(DBNodeRevision.catalog))

    # Handle parents
    if "parents" in node_revision_fields:
        options.append(selectinload(DBNodeRevision.parents))
    else:
        options.append(noload(DBNodeRevision.parents))

    # Handle materializations
    if "materializations" in node_revision_fields:
        options.append(selectinload(DBNodeRevision.materializations))
    else:
        options.append(noload(DBNodeRevision.materializations))

    # Handle metric_metadata
    if "metric_metadata" in node_revision_fields:
        options.append(selectinload(DBNodeRevision.metric_metadata))
    else:
        options.append(noload(DBNodeRevision.metric_metadata))

    # Handle availability
    if "availability" in node_revision_fields:
        options.append(selectinload(DBNodeRevision.availability))
    else:
        options.append(noload(DBNodeRevision.availability))

    # Handle dimension_links
    if "dimension_links" in node_revision_fields:
        options.append(
            selectinload(DBNodeRevision.dimension_links).options(
                joinedload(DimensionLink.dimension).options(
                    selectinload(DBNode.current),
                ),
            ),
        )
    else:
        options.append(noload(DBNodeRevision.dimension_links))

    # Handle required_dimensions
    if "required_dimensions" in node_revision_fields:
        options.append(
            selectinload(DBNodeRevision.required_dimensions),
        )
    else:
        options.append(noload(DBNodeRevision.required_dimensions))

    # Handle cube_elements
    if "cube_elements" in node_revision_fields or is_cube_request:
        # Build optimized options for nested NodeRevision based on what's requested
        # cube_metrics returns List[NodeRevision] - needs full serialization based on requested fields
        # cube_dimensions returns List[DimensionAttribute] - only needs name, type from NodeRevision
        if cube_metric_fields:
            # cube_metrics needs full NodeRevision serialization
            nested_options = build_cube_metrics_node_revision_options(
                cube_metric_fields,
            )
        elif cube_dimension_fields:
            # cube_dimensions only needs minimal NodeRevision fields
            nested_options = build_cube_dimensions_node_revision_options()
        else:
            # cube_elements directly requested - load minimal
            nested_options = build_cube_dimensions_node_revision_options()

        options.append(
            selectinload(DBNodeRevision.cube_elements)
            .selectinload(Column.node_revision)
            .options(*nested_options),
        )
    else:
        options.append(noload(DBNodeRevision.cube_elements))

    # Noload relationships not exposed in GraphQL
    options.append(noload(DBNodeRevision.created_by))
    options.append(noload(DBNodeRevision.node))
    options.append(noload(DBNodeRevision.missing_parents))
    return options


async def resolve_metrics_and_dimensions(
    session: AsyncSession,
    cube_def: CubeDefinition,
) -> tuple[list[str], list[str]]:
    """
    Resolves the metrics and dimensions for a given cube definition.
    If a cube is specified, it retrieves the metrics and dimensions from the cube node.
    If no cube is specified, it uses the metrics and dimensions provided in the cube definition.
    """
    metrics = cube_def.metrics or []
    dimensions = cube_def.dimensions or []

    if cube_def.cube:
        cube_node = await DBNode.get_cube_by_name(session, cube_def.cube)
        if not cube_node:
            raise DJNodeNotFound(f"Cube '{cube_def.cube}' not found.")
        metrics = dedupe_append(cube_node.current.cube_node_metrics, metrics)
        dimensions = dedupe_append(cube_node.current.cube_node_dimensions, dimensions)

    metrics = list(OrderedDict.fromkeys(metrics))
    return metrics, dimensions


async def get_metrics(
    session: AsyncSession,
    metrics: list[str],
):
    return await DBNode.get_by_names(
        session,
        metrics,
        options=[
            joinedload(DBNode.current).options(
                selectinload(DBNodeRevision.columns),
                joinedload(DBNodeRevision.catalog),
                selectinload(DBNodeRevision.parents),
            ),
        ],
        include_inactive=False,
    )
