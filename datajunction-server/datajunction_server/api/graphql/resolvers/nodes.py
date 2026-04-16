"""
Node resolvers
"""

from collections import OrderedDict
from typing import Any, List, Optional

from sqlalchemy import select as sa_select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import defer, joinedload, load_only, noload, selectinload
from sqlalchemy.orm.attributes import set_committed_value
from strawberry.types import Info

from datajunction_server.errors import DJNodeNotFound
from datajunction_server.api.graphql.scalars.node import NodeName, NodeSortField
from datajunction_server.api.graphql.scalars.sql import CubeDefinition
from datajunction_server.api.graphql.utils import dedupe_append, extract_fields
from datajunction_server.database.dimensionlink import DimensionLink

from datajunction_server.database.node import Column, ColumnAttribute
from datajunction_server.database.node import Node as DBNode
from datajunction_server.database.node import NodeRevision as DBNodeRevision
from datajunction_server.api.graphql.resolvers.tags import tag_load_options
from datajunction_server.api.graphql.resolvers.users import user_load_options
from datajunction_server.models.node import NodeMode, NodeStatus, NodeType

# cubeMetrics/cubeDimensions fields derivable from the cube's own columns
# without loading cube_elements at all.
_CUBE_NAME_ONLY_FIELDS: frozenset[str] = frozenset({"name"})


def _is_name_only(fields) -> bool:
    """Check if requested fields are all derivable from column names alone."""
    return fields is not None and set(fields.keys()).issubset(_CUBE_NAME_ONLY_FIELDS)


class _RawColumn:
    """Lightweight stand-in for Column ORM objects in the name-only path."""

    __slots__ = ("name", "dimension_column", "type", "order")

    def __init__(self, name, dimension_column, col_type, order):
        self.name = name
        self.dimension_column = dimension_column
        self.type = col_type
        self.order = order


async def _attach_raw_columns(session, nodes):
    """
    Fetch column data and metric names as raw rows and inject into each
    cube node's current revision, bypassing ORM hydration.
    """
    from datajunction_server.database.node import CubeRelationship
    from datajunction_server.models.node import NodeType as NodeType_

    rev_ids = [
        node.current.id
        for node in nodes
        if node.current is not None and node.type == NodeType_.CUBE
    ]
    if not rev_ids:
        return

    # Fetch cube's own columns as raw tuples
    col_stmt = (
        sa_select(
            Column.node_revision_id,
            Column.name,
            Column.dimension_column,
            Column.type,
            Column.order,
        )
        .where(Column.node_revision_id.in_(rev_ids))
        .order_by(Column.node_revision_id, Column.order)
    )
    col_result = await session.execute(col_stmt)

    cols_by_rev: dict[int, list] = {}
    for rev_id, name, dim_col, col_type, order in col_result:
        cols_by_rev.setdefault(rev_id, []).append(
            _RawColumn(name, dim_col, col_type, order),
        )

    # Fetch metric element names per cube via the cube join table.
    # Metrics have _DOT_ in the element column name; dimensions don't.
    metric_stmt = (
        sa_select(CubeRelationship.cube_id, Column.name)
        .join(Column, Column.id == CubeRelationship.cube_element_id)
        .where(
            CubeRelationship.cube_id.in_(rev_ids),
            Column.name.like("%\\_DOT\\_%"),
        )
    )
    metric_result = await session.execute(metric_stmt)

    metric_names_by_rev: dict[int, set[str]] = {}
    for cube_id, elem_name in metric_result:
        metric_names_by_rev.setdefault(cube_id, set()).add(
            elem_name.replace("_DOT_", "."),
        )

    for node in nodes:
        if (
            node.current is not None and node.type == NodeType_.CUBE
        ):  # pragma: no branch
            rev_id = node.current.id
            set_committed_value(
                node.current,
                "columns",
                cols_by_rev.get(rev_id, []),
            )
            node.current._cube_metric_names = metric_names_by_rev.get(rev_id, set())


def _is_cube_name_only_request(current_fields: dict) -> bool:
    """Check if the current revision fields only need cube metric/dimension names."""
    cube_metric_fields = current_fields.get("cube_metrics")
    cube_dimension_fields = current_fields.get("cube_dimensions")
    is_cube = cube_metric_fields is not None or cube_dimension_fields is not None
    if not is_cube:
        return False
    return (cube_metric_fields is None or _is_name_only(cube_metric_fields)) and (
        cube_dimension_fields is None or _is_name_only(cube_dimension_fields)
    )


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
    node_fields = (
        fields["nodes"]
        if "nodes" in fields
        else fields["edges"]["node"]
        if "edges" in fields
        else fields
    )
    options = load_node_options(node_fields)

    # Signal to cube resolvers whether the name-only fast path is active
    current_fields = node_fields.get("current") or {}
    is_cube_name_only = _is_cube_name_only_request(current_fields)
    info.context["cube_name_only"] = is_cube_name_only  # type: ignore

    result = await DBNode.find_by(
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

    # For the name-only cube path, fetch column data as raw tuples instead
    # of ORM objects.  This avoids hydrating ~20k Column instances.
    if is_cube_name_only and result:
        await _attach_raw_columns(session, result)

    return result


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
        options.append(
            joinedload(DBNode.created_by).options(
                *user_load_options(fields["created_by"]),
            ),
        )
    else:
        options.append(noload(DBNode.created_by))

    if "owners" in fields:
        options.append(
            selectinload(DBNode.owners).options(
                *user_load_options(fields["owners"]),
            ),
        )
    else:
        options.append(noload(DBNode.owners))

    if "edited_by" in fields:
        options.append(selectinload(DBNode.history))
    else:
        options.append(noload(DBNode.history))

    if "tags" in fields:
        options.append(
            selectinload(DBNode.tags).options(*tag_load_options(fields["tags"])),
        )
    else:
        options.append(noload(DBNode.tags))

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
    # Defer heavy columns not needed for most GraphQL queries
    options = [
        defer(DBNodeRevision.query_ast),
        defer(DBNodeRevision.lineage),
    ]
    # query is also used by metric_metadata and extracted_measures resolvers
    needs_query = (
        "query" in node_revision_fields
        or "metric_metadata" in node_revision_fields
        or "extracted_measures" in node_revision_fields
    )
    if not needs_query:
        options.append(defer(DBNodeRevision.query))

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

    # When cubeMetrics/cubeDimensions only need "name", we can skip loading
    # both cube_elements AND columns as ORM objects — the resolver will use
    # raw column data fetched post-query instead.
    all_name_only = is_cube_request and (
        (cube_metric_fields is None or _is_name_only(cube_metric_fields))
        and (cube_dimension_fields is None or _is_name_only(cube_dimension_fields))
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
    elif is_cube_request and not all_name_only:
        # Minimal ORM columns for cube element resolution
        options.append(
            selectinload(DBNodeRevision.columns).options(
                load_only(
                    Column.id,
                    Column.name,
                    Column.type,
                    Column.order,
                    Column.dimension_column,
                ),
                noload(Column.attributes),
                noload(Column.dimension),
                noload(Column.partition),
            ),
        )
    else:
        # Name-only cube path: columns injected as raw tuples post-query
        # via _attach_raw_columns. No cube request: not needed.
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

    # Handle cube_elements (only needed when cubeMetrics/cubeDimensions request
    # fields beyond just "name")
    if is_cube_request:
        if all_name_only:
            # Name-only: metric names fetched via raw query in
            # _attach_raw_columns; no need to load cube_elements ORM objects.
            options.append(noload(DBNodeRevision.cube_elements))
        else:
            # Build cube element Column options
            cube_col_options = [
                load_only(
                    Column.id,
                    Column.name,
                    Column.type,
                    Column.order,
                    Column.dimension_column,
                    Column.node_revision_id,
                ),
                noload(Column.dimension),
                noload(Column.partition),
                noload(Column.measure),
            ]

            if cube_metric_fields and not _is_name_only(cube_metric_fields):
                nested_options = build_cube_metrics_node_revision_options(
                    cube_metric_fields,
                )
            else:
                nested_options = build_cube_dimensions_node_revision_options()
            cube_col_options.append(
                selectinload(Column.node_revision).options(*nested_options),
            )

            options.append(
                selectinload(DBNodeRevision.cube_elements).options(
                    *cube_col_options,
                ),
            )
    else:
        options.append(noload(DBNodeRevision.cube_elements))

    # Noload relationships not exposed in GraphQL
    options.append(noload(DBNodeRevision.created_by))
    options.append(noload(DBNodeRevision.node))
    options.append(noload(DBNodeRevision.missing_parents))
    options.append(noload(DBNodeRevision.frozen_measures))
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
