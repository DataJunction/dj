"""
Node resolvers
"""

from collections import OrderedDict
from typing import Any, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import defer, joinedload, noload, selectinload
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

    NOTE: We can only use noload() for:
    1. Nullable fields in the GraphQL schema
    2. @strawberry.field resolvers (only called when requested)
    3. Relationships not exposed in GraphQL

    For non-nullable plain attributes (current, revisions, created_by, owners, tags),
    we cannot use noload() because Strawberry validates the schema even for unrequested
    fields, and noload() returns None which violates the non-nullable constraint.
    """
    options = []

    # Handle revisions - NON-NULLABLE in schema, only load if requested
    if "revisions" in fields:
        node_revision_options = load_node_revision_options(fields["revisions"])
        options.append(joinedload(DBNode.revisions).options(*node_revision_options))
    # Cannot noload - revisions is non-nullable list in GraphQL schema

    # Handle current - NON-NULLABLE in schema, only load if requested
    if fields.get("current"):
        node_revision_options = load_node_revision_options(fields["current"])
        options.append(joinedload(DBNode.current).options(*node_revision_options))
    # Cannot noload - current is non-nullable in GraphQL schema

    # Handle created_by - NON-NULLABLE in schema
    if "created_by" in fields:
        options.append(selectinload(DBNode.created_by))
    # Cannot noload - created_by is non-nullable in GraphQL schema

    # Handle owners - NON-NULLABLE list in schema
    if "owners" in fields:
        options.append(selectinload(DBNode.owners))
    # Cannot noload - owners is non-nullable list in GraphQL schema

    # Handle edited_by (via history) - this is a @strawberry.field resolver, safe to noload
    if "edited_by" in fields:
        options.append(selectinload(DBNode.history))
    else:
        options.append(noload(DBNode.history))

    # Handle tags - NON-NULLABLE list in schema
    if "tags" in fields:
        options.append(selectinload(DBNode.tags))
    # Cannot noload - tags is non-nullable list in GraphQL schema

    # Always noload children - not exposed in GraphQL Node type
    options.append(noload(DBNode.children))

    return options


def load_node_revision_options(node_revision_fields):
    """
    Based on the GraphQL query input fields, builds a list of node revision
    load options.

    NOTE: We can only use noload() for:
    1. Nullable fields in the GraphQL schema
    2. @strawberry.field resolvers (only called when requested)
    3. Relationships not exposed in GraphQL

    For non-nullable plain attributes (parents), we cannot use noload().
    """
    options = [defer(DBNodeRevision.query_ast)]
    is_cube_request = (
        "cube_metrics" in node_revision_fields
        or "cube_dimensions" in node_revision_fields
    )

    # Handle columns - @strawberry.field resolver, safe to noload
    if (
        "columns" in node_revision_fields
        or is_cube_request
        or "primary_key" in node_revision_fields
    ):
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

    # Handle catalog - @strawberry.field resolver returning Optional, safe to noload
    # Also has lazy="joined" by default which we want to prevent
    if "catalog" in node_revision_fields:
        options.append(joinedload(DBNodeRevision.catalog))
    else:
        options.append(noload(DBNodeRevision.catalog))

    # Handle parents - NON-NULLABLE list in schema, cannot noload
    if "parents" in node_revision_fields:
        options.append(selectinload(DBNodeRevision.parents))
    # Cannot noload - parents is non-nullable list in GraphQL schema

    # Handle materializations - NULLABLE in schema, safe to noload
    if "materializations" in node_revision_fields:
        options.append(selectinload(DBNodeRevision.materializations))
    else:
        options.append(noload(DBNodeRevision.materializations))

    # Handle metric_metadata - @strawberry.field resolver, safe to noload
    if "metric_metadata" in node_revision_fields:
        options.append(selectinload(DBNodeRevision.metric_metadata))
    else:
        options.append(noload(DBNodeRevision.metric_metadata))

    # Handle availability - NULLABLE in schema, safe to noload
    if "availability" in node_revision_fields:
        options.append(selectinload(DBNodeRevision.availability))
    else:
        options.append(noload(DBNodeRevision.availability))

    # Handle dimension_links - @strawberry.field resolver, safe to noload
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

    # Handle required_dimensions - NULLABLE in schema, safe to noload
    if "required_dimensions" in node_revision_fields:
        options.append(
            selectinload(DBNodeRevision.required_dimensions),
        )
    else:
        options.append(noload(DBNodeRevision.required_dimensions))

    # Handle cube_elements - @strawberry.field resolver, safe to noload
    if "cube_elements" in node_revision_fields or is_cube_request:
        options.append(
            selectinload(DBNodeRevision.cube_elements)
            .selectinload(Column.node_revision)
            .options(
                selectinload(DBNodeRevision.node),
            ),
        )
    else:
        options.append(noload(DBNodeRevision.cube_elements))

    # Noload relationships not exposed in GraphQL schema - always safe
    # created_by on NodeRevision is not exposed as a plain attribute
    options.append(noload(DBNodeRevision.created_by))
    # node relationship is not exposed as a plain attribute
    options.append(noload(DBNodeRevision.node))
    # missing_parents is not exposed in GraphQL
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
