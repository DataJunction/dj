"""
Node resolvers
"""

from collections import OrderedDict
from typing import Any, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import defer, joinedload, selectinload
from strawberry.types import Info

from datajunction_server.errors import DJNodeNotFound
from datajunction_server.api.graphql.scalars.node import NodeName
from datajunction_server.api.graphql.scalars.sql import CubeDefinition
from datajunction_server.api.graphql.utils import dedupe_append, extract_fields
from datajunction_server.database.dimensionlink import DimensionLink
from datajunction_server.database.node import Column, ColumnAttribute
from datajunction_server.database.node import Node as DBNode
from datajunction_server.database.node import NodeRevision as DBNodeRevision
from datajunction_server.models.node import NodeType


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
        options=options,
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
    """
    options = []
    if "revisions" in fields:
        node_revision_options = load_node_revision_options(fields["revisions"])
        options.append(joinedload(DBNode.revisions).options(*node_revision_options))
    if fields.get("current"):
        node_revision_options = load_node_revision_options(fields["current"])
        options.append(joinedload(DBNode.current).options(*node_revision_options))
    if "created_by" in fields:
        options.append(joinedload(DBNode.created_by))
    if "edited_by" in fields:
        options.append(selectinload(DBNode.history))
    if "tags" in fields:
        options.append(selectinload(DBNode.tags))
    return options


def load_node_revision_options(node_revision_fields):
    """
    Based on the GraphQL query input fields, builds a list of node revision
    load options.
    """
    options = [defer(DBNodeRevision.query_ast)]
    is_cube_request = (
        "cube_metrics" in node_revision_fields
        or "cube_dimensions" in node_revision_fields
    )
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
    if "catalog" in node_revision_fields:
        options.append(joinedload(DBNodeRevision.catalog))
    if "parents" in node_revision_fields:
        options.append(selectinload(DBNodeRevision.parents))
    if "materializations" in node_revision_fields:
        options.append(selectinload(DBNodeRevision.materializations))
    if "metric_metadata" in node_revision_fields:
        options.append(selectinload(DBNodeRevision.metric_metadata))
    if "availability" in node_revision_fields:
        options.append(selectinload(DBNodeRevision.availability))
    if "dimension_links" in node_revision_fields:
        options.append(
            selectinload(DBNodeRevision.dimension_links).options(
                joinedload(DimensionLink.dimension).options(
                    selectinload(DBNode.current),
                ),
            ),
        )
    if "required_dimensions" in node_revision_fields:
        options.append(
            selectinload(DBNodeRevision.required_dimensions),
        )

    if "cube_elements" in node_revision_fields or is_cube_request:
        options.append(
            selectinload(DBNodeRevision.cube_elements)
            .selectinload(Column.node_revisions)
            .options(
                selectinload(DBNodeRevision.node),
            ),
        )
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
