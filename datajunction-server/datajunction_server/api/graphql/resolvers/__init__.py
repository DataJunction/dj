
from typing import List, Optional, Dict

from sqlalchemy.orm import selectinload, joinedload
from strawberry.types import Info

from datajunction_server.models.node import NodeType
from datajunction_server.database.node import Node as DBNode, NodeRevision as DBNodeRevision, Column, ColumnAttribute
from datajunction_server.database.dimensionlink import DimensionLink
from datajunction_server.api.graphql.utils import extract_fields


def get_sub_fields(fields):
    return {
        k: v for field in fields 
        if isinstance(field, Dict) for k, v in field.items()
    }


async def get_nodes(
    info: Info,
    names: Optional[List[str]] = None,
    fragment: Optional[str] = None,
    node_types: Optional[List[NodeType]] = None,
    tags: Optional[List[str]] = None,
) -> DBNode:
    """
    Finds nodes based on the search parameters. This function also tries to optimize
    the database query by only retrieving joined-in fields if they were requested.
    """
    session = info.context["session"]  # type: ignore
    fields = extract_fields(info)
    sub_fields = get_sub_fields(fields)
    options = []
    if sub_fields.get("current"):
        node_revision_options = []
        current_sub_fields = get_sub_fields(sub_fields["current"])
        is_cube_request = "cubeMetrics" in current_sub_fields or "cubeDimensions" in current_sub_fields
        if "columns" in current_sub_fields or is_cube_request:
            node_revision_options.append(
                selectinload(DBNodeRevision.columns).options(
                    joinedload(Column.attributes).joinedload(ColumnAttribute.attribute_type),
                    joinedload(Column.dimension),
                    joinedload(Column.partition),
                )
            )
        if "catalog" in current_sub_fields:
            node_revision_options.append(joinedload(DBNodeRevision.catalog))
        if "parents" in current_sub_fields:
            node_revision_options.append(selectinload(DBNodeRevision.parents))
        if "materializations" in current_sub_fields:
            node_revision_options.append(selectinload(DBNodeRevision.materializations))
        if "metric_metadata" in current_sub_fields:
            node_revision_options.append(selectinload(DBNodeRevision.metric_metadata))
        if "availability" in current_sub_fields:
            node_revision_options.append(selectinload(DBNodeRevision.availability))
        if "dimension_links" in current_sub_fields:
            node_revision_options.append(
                selectinload(DBNodeRevision.dimension_links).options(
                    joinedload(DimensionLink.dimension).options(
                        selectinload(DBNode.current),
                    ),
                )
            )
        if "required_dimensions" in current_sub_fields:
            node_revision_options.append(selectinload(DBNodeRevision.required_dimensions))

        if "cubeElements" in sub_fields["current"] or is_cube_request:
            node_revision_options.append(
                selectinload(DBNodeRevision.cube_elements)
                .selectinload(Column.node_revisions)
                .options(
                    selectinload(DBNodeRevision.node),
                )
            )

        options.append(joinedload(DBNode.current).options(*node_revision_options))

    if sub_fields.get("tags"):
        options.append(selectinload(DBNode.tags))

    return await DBNode.find_by(
        session,
        names,
        fragment,
        node_types,
        tags,
        *options,
    )
