"""
DAG related functions.
"""

import itertools
from typing import Dict, List, Optional, Set, Union, cast

from sqlalchemy import and_, func, join, literal, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased, contains_eager, joinedload, selectinload
from sqlalchemy.sql.operators import is_
from sqlalchemy.dialects.postgresql import array

from datajunction_server.database.attributetype import AttributeType, ColumnAttribute
from datajunction_server.database.column import Column
from datajunction_server.database.dimensionlink import DimensionLink
from datajunction_server.database.node import (
    Node,
    NodeColumns,
    NodeRelationship,
    NodeRevision,
)
from datajunction_server.errors import DJDoesNotExistException, DJGraphCycleException
from datajunction_server.models.attribute import ColumnAttributes
from datajunction_server.models.node import DimensionAttributeOutput
from datajunction_server.models.node_type import NodeType
from datajunction_server.utils import SEPARATOR, get_settings, refresh_if_needed

settings = get_settings()


def _node_output_options():
    """
    Statement options to retrieve all NodeOutput objects in one query
    """
    return [
        selectinload(Node.current).options(
            selectinload(NodeRevision.columns).options(
                selectinload(Column.attributes).joinedload(
                    ColumnAttribute.attribute_type,
                ),
                selectinload(Column.dimension),
                selectinload(Column.partition),
            ),
            selectinload(NodeRevision.catalog),
            selectinload(NodeRevision.parents),
            selectinload(NodeRevision.dimension_links).options(
                selectinload(DimensionLink.dimension).options(
                    selectinload(Node.current),
                ),
            ),
        ),
        selectinload(Node.tags),
    ]


async def get_downstream_nodes(
    session: AsyncSession,
    node_name: str,
    node_type: NodeType = None,
    include_deactivated: bool = True,
    include_cubes: bool = True,
    depth: int = -1,
) -> List[Node]:
    """
    Gets all downstream children of the given node, filterable by node type.
    Uses a recursive CTE query to build out all descendants from the node.
    """
    node = await Node.get_by_name(
        session,
        node_name,
        options=_node_output_options(),
    )
    if not node:
        return []
    initial_dag = (
        select(
            NodeRelationship.parent_id,
            NodeRevision.node_id,
            literal(0).label("depth"),
        )
        .where(NodeRelationship.parent_id == node.id)
        .join(NodeRevision, NodeRelationship.child_id == NodeRevision.id)
        .join(
            Node,
            (Node.id == NodeRevision.node_id)
            & (Node.current_version == NodeRevision.version),
        )
    )
    if not include_cubes:
        initial_dag = initial_dag.where((NodeRevision.type != NodeType.CUBE))
    dag = initial_dag.cte("downstreams", recursive=True).suffix_with(
        "CYCLE node_id SET is_cycle USING path",
    )

    next_layer = (
        select(
            dag.c.parent_id,
            NodeRevision.node_id,
            (dag.c.depth + literal(1)).label("depth"),
        )
        .join(NodeRelationship, dag.c.node_id == NodeRelationship.parent_id)
        .join(NodeRevision, NodeRelationship.child_id == NodeRevision.id)
        .join(Node, Node.id == NodeRevision.node_id)
    )
    if not include_cubes:
        next_layer = next_layer.where(NodeRevision.type != NodeType.CUBE)

    paths = dag.union_all(next_layer)

    # Calculate the maximum depth for each node
    max_depths = (
        select(
            paths.c.node_id,
            func.max(paths.c.depth).label("max_depth"),
        )
        .group_by(paths.c.node_id)
        .cte("max_depths")
    )

    # Select nodes with the maximum depth
    final_select = select(Node, max_depths.c.max_depth).join(
        max_depths,
        max_depths.c.node_id == Node.id,
    )

    if not include_deactivated:
        final_select = final_select.where(is_(Node.deactivated_at, None))

    # Add depth filter
    if depth > -1:
        final_select = final_select.where(max_depths.c.max_depth < depth)

    statement = final_select.order_by(max_depths.c.max_depth, Node.id).options(
        *_node_output_options(),
    )
    results = (await session.execute(statement)).unique().scalars().all()
    return [
        downstream
        for downstream in results
        if downstream.type == node_type or node_type is None
    ]


async def get_upstream_nodes(
    session: AsyncSession,
    node_name: str,
    node_type: NodeType = None,
    include_deactivated: bool = True,
) -> List[Node]:
    """
    Gets all upstreams of the given node, filterable by node type.
    Uses a recursive CTE query to build out all parents of the node.
    """
    node = (
        (
            await session.execute(
                select(Node)
                .where(
                    (Node.name == node_name) & (is_(Node.deactivated_at, None)),
                )
                .options(joinedload(Node.current)),
            )
        )
        .unique()
        .scalar()
    )
    if not node:
        raise DJDoesNotExistException(  # pragma: no cover
            message=f"Node with name {node_name} does not exist",
        )

    dag = (
        (
            select(
                NodeRelationship.child_id,
                NodeRevision.id,
                NodeRevision.node_id,
            )
            .where(NodeRelationship.child_id == node.current.id)
            .join(Node, NodeRelationship.parent_id == Node.id)
            .join(
                NodeRevision,
                (Node.id == NodeRevision.node_id)
                & (Node.current_version == NodeRevision.version),
            )
        )
        .cte("upstreams", recursive=True)
        .suffix_with(
            "CYCLE node_id SET is_cycle USING path",
        )
    )

    paths = dag.union_all(
        select(
            dag.c.child_id,
            NodeRevision.id,
            NodeRevision.node_id,
        )
        .join(NodeRelationship, dag.c.id == NodeRelationship.child_id)
        .join(Node, NodeRelationship.parent_id == Node.id)
        .join(
            NodeRevision,
            (Node.id == NodeRevision.node_id)
            & (Node.current_version == NodeRevision.version),
        ),
    )

    node_selector = select(Node)
    if not include_deactivated:
        node_selector = node_selector.where(is_(Node.deactivated_at, None))
    statement = (
        node_selector.join(paths, paths.c.node_id == Node.id)
        .join(
            NodeRevision,
            (Node.current_version == NodeRevision.version)
            & (Node.id == NodeRevision.node_id),
        )
        .options(*_node_output_options())
    )

    results = (await session.execute(statement)).unique().scalars().all()
    return [
        upstream
        for upstream in results
        if upstream.type == node_type or node_type is None
    ]


async def build_reference_link(
    session: AsyncSession,
    col: Column,
    path: list[str],
    role: list[str] | None = None,
) -> DimensionAttributeOutput | None:
    """
    Builds a reference link dimension attribute output for a column.
    """
    if not (col.dimension_id and col.dimension_column):
        return None  # pragma: no cover
    await session.refresh(col, ["dimension"])
    await session.refresh(col.dimension, ["current"])
    await session.refresh(col.dimension.current, ["columns"])

    dim_cols = col.dimension.current.columns
    if dim_col := next(
        (dc for dc in dim_cols if dc.name == col.dimension_column),
        None,
    ):
        return DimensionAttributeOutput(
            name=f"{col.dimension.name}.{col.dimension_column}"
            + (f"[{'->'.join(role)}]" if role else ""),
            node_name=col.dimension.name,
            node_display_name=col.dimension.current.display_name,
            properties=dim_col.attribute_names(),
            type=str(col.type),
            path=path,
        )
    return None  # pragma: no cover


async def get_dimension_attributes(
    session: AsyncSession,
    node_name: str,
    include_deactivated: bool = True,
):
    """
    Get all dimension attributes for a given node.
    """
    node = cast(
        Node,
        await Node.get_by_name(
            session,
            node_name,
            options=[joinedload(Node.current)],
        ),
    )
    if node.type == NodeType.METRIC:
        await refresh_if_needed(session, node.current, ["parents"])
        node = node.current.parents[0]
        await refresh_if_needed(session, node, ["current"])

    # Discover all dimension nodes in the given node's dimensions graph
    dimension_nodes_and_paths = await get_dimension_nodes(
        session,
        node,
        include_deactivated,
    )
    dimensions_map = {dim.id: dim for dim, _, _ in dimension_nodes_and_paths}

    # Add all reference links to the list of dimension attributes
    reference_links = []
    await refresh_if_needed(session, node.current, ["columns"])
    for col in node.current.columns:
        if col.dimension_id and col.dimension_column:
            await session.refresh(col, ["dimension"])
            if ref_link := await build_reference_link(  # pragma: no cover
                session,
                col,
                path=[f"{node.name}.{col.name}"],
            ):
                reference_links.append(ref_link)
    for dimension_node, path, role in dimension_nodes_and_paths:
        for col in dimension_node.current.columns:
            if col.dimension_id and col.dimension_column:
                join_path = (
                    [node.name] if dimension_node.name != node.name else []
                ) + [dimensions_map[int(node_id)].name for node_id in path]
                if ref_link := await build_reference_link(  # pragma: no cover
                    session,
                    col,
                    join_path,
                    role,
                ):
                    reference_links.append(ref_link)

    # Build all dimension attributes from the dimension nodes in the graph
    graph_dimensions = [
        DimensionAttributeOutput(
            name=f"{dim.name}.{col.name}" + (f"[{'->'.join(role)}]" if role else ""),
            node_name=dim.name,
            node_display_name=dim.current.display_name,
            properties=col.attribute_names(),
            type=str(col.type),
            path=[node_name] + [dimensions_map[int(node_id)].name for node_id in path],
        )
        for dim, path, role in dimension_nodes_and_paths
        for col in dim.current.columns
    ]

    # Build all local dimension attributes from the original node
    local_dimensions = [
        DimensionAttributeOutput(
            name=f"{node.name}.{col.name}",
            node_name=node.name,
            node_display_name=node.current.display_name,
            properties=col.attribute_names(),
            type=str(col.type),
            path=[],
        )
        for col in node.current.columns
    ]
    local_dimensions = [
        dim
        for dim in local_dimensions
        if "primary_key" in (dim.properties or [])
        or "dimension" in (dim.properties or [])
        or node.type == NodeType.DIMENSION
    ]
    return reference_links + graph_dimensions + local_dimensions


async def get_dimension_nodes(
    session: AsyncSession,
    node: Node,
    include_deactivated: bool = True,
) -> list[tuple[Node, list[str], list[str] | None]]:
    """
    Discovers all dimension nodes in the given node's dimensions graph using a recursive
    CTE query to build out the dimension links.
    """
    dag = (
        (
            select(
                DimensionLink.node_revision_id,
                NodeRevision.id,
                NodeRevision.node_id,
                array([NodeRevision.node_id]).label("join_path"),  # start path
                array([DimensionLink.role]).label("role"),
            )
            .where(DimensionLink.node_revision_id == node.current.id)
            .join(Node, DimensionLink.dimension_id == Node.id)
            .join(
                NodeRevision,
                (Node.id == NodeRevision.node_id)
                & (Node.current_version == NodeRevision.version),
            )
        )
        .cte("dimensions", recursive=True)
        .suffix_with(
            "CYCLE node_id SET is_cycle USING path",
        )
    )

    paths = dag.union_all(
        select(
            dag.c.node_revision_id,
            NodeRevision.id,
            NodeRevision.node_id,
            func.array_cat(dag.c.join_path, array([NodeRevision.node_id])).label(
                "join_path",
            ),
            func.array_cat(dag.c.role, array([DimensionLink.role])).label("role"),
        )
        .join(DimensionLink, dag.c.id == DimensionLink.node_revision_id)
        .join(Node, DimensionLink.dimension_id == Node.id)
        .join(
            NodeRevision,
            (Node.id == NodeRevision.node_id)
            & (Node.current_version == NodeRevision.version),
        ),
    )

    node_selector = select(Node, paths.c.join_path, paths.c.role)
    if not include_deactivated:
        node_selector = node_selector.where(  # pragma: no cover
            is_(Node.deactivated_at, None),
        )
    statement = (
        node_selector.join(paths, paths.c.node_id == Node.id)
        .join(
            NodeRevision,
            (Node.current_version == NodeRevision.version)
            & (Node.id == NodeRevision.node_id),
        )
        .options(*_node_output_options())
    )
    return [
        (node, path, [r for r in role if r])
        for node, path, role in (await session.execute(statement)).all()
    ]


async def get_dimensions_dag(
    session: AsyncSession,
    node_revision: NodeRevision,
    with_attributes: bool = True,
    depth: int = 30,
) -> List[Union[DimensionAttributeOutput, Node]]:
    """
    Gets the dimensions graph of the given node revision with a single recursive CTE query.
    This graph is split out into dimension attributes or dimension nodes depending on the
    `with_attributes` flag.
    """

    initial_node = aliased(NodeRevision, name="initial_node")
    dimension_node = aliased(Node, name="dimension_node")
    dimension_rev = aliased(NodeRevision, name="dimension_rev")
    current_node = aliased(Node, name="current_node")
    current_rev = aliased(NodeRevision, name="current_rev")
    next_node = aliased(Node, name="next_node")
    next_rev = aliased(NodeRevision, name="next_rev")
    column = aliased(Column, name="c")

    # Merge both branching points of the dimensions graph (the column -> dimension
    # branch and the node -> dimension branch) into a single CTE. We do this merge because
    # subqueries with UNION are not allowed in the recursive CTE.
    graph_branches = (
        (
            select(
                NodeColumns.node_id.label("node_revision_id"),
                Column.dimension_id,
                Column.name,
                Column.dimension_column,
            )
            .select_from(NodeColumns)
            .join(Column, NodeColumns.column_id == Column.id)
        )
        .union_all(
            select(
                DimensionLink.node_revision_id,
                DimensionLink.dimension_id,
                (
                    literal("[")
                    + func.coalesce(
                        DimensionLink.role,
                        literal(""),
                    )
                    + literal("]")
                ).label("name"),
                literal(None).label("dimension_column"),
            ).select_from(DimensionLink),
        )
        .cte("graph_branches")
    )

    # Recursive CTE
    dimensions_graph = (
        select(
            initial_node.id.label("path_start"),
            graph_branches.c.name.label("col_name"),
            graph_branches.c.dimension_column.label("dimension_column"),
            dimension_node.id.label("path_end"),
            (
                initial_node.name
                + "."
                + graph_branches.c.name
                + ","
                + dimension_node.name
            ).label(
                "join_path",
            ),
            dimension_node.name.label("node_name"),
            dimension_rev.id.label("node_revision_id"),
            dimension_rev.display_name.label("node_display_name"),
            literal(0).label("depth"),
        )
        .select_from(initial_node)
        .join(graph_branches, node_revision.id == graph_branches.c.node_revision_id)
        .join(
            dimension_node,
            (dimension_node.id == graph_branches.c.dimension_id)
            & (is_(dimension_node.deactivated_at, None)),
        )
        .join(
            dimension_rev,
            and_(
                dimension_rev.version == dimension_node.current_version,
                dimension_rev.node_id == dimension_node.id,
            ),
        )
        .where(initial_node.id == node_revision.id)
    ).cte("dimensions_graph", recursive=True)
    dimensions_graph = dimensions_graph.suffix_with(
        "CYCLE node_revision_id SET is_cycle USING path",
    )

    paths = dimensions_graph.union_all(
        select(
            dimensions_graph.c.path_start,
            graph_branches.c.name.label("col_name"),
            graph_branches.c.dimension_column.label("dimension_column"),
            next_node.id.label("path_end"),
            (
                dimensions_graph.c.join_path
                + "."
                + graph_branches.c.name
                + ","
                + next_node.name
            ).label(
                "join_path",
            ),
            next_node.name.label("node_name"),
            next_rev.id.label("node_revision_id"),
            next_rev.display_name.label("node_display_name"),
            (dimensions_graph.c.depth + literal(1)).label("depth"),
        )
        .select_from(
            dimensions_graph.join(
                current_node,
                (dimensions_graph.c.path_end == current_node.id),
            )
            .join(
                current_rev,
                and_(
                    current_rev.version == current_node.current_version,
                    current_rev.node_id == current_node.id,
                ),
            )
            .join(
                graph_branches,
                (current_rev.id == graph_branches.c.node_revision_id)
                & (is_(graph_branches.c.dimension_column, None)),
            )
            .join(
                next_node,
                (next_node.id == graph_branches.c.dimension_id)
                & (is_(graph_branches.c.dimension_column, None))
                & (is_(next_node.deactivated_at, None)),
            )
            .join(
                next_rev,
                and_(
                    next_rev.version == next_node.current_version,
                    next_rev.node_id == next_node.id,
                ),
            ),
        )
        .where(dimensions_graph.c.depth <= depth),
    )

    # Final SELECT statements
    # ----
    # If attributes was set to False, we only need to return the dimension nodes
    if not with_attributes:
        result = await session.execute(
            select(Node)
            .select_from(paths)
            .join(Node, paths.c.node_name == Node.name)
            .options(*_node_output_options()),
        )
        return result.unique().scalars().all()

    # Otherwise return the dimension attributes, which include both the dimension
    # attributes on the dimension nodes in the DAG as well as the local dimension
    # attributes on the initial node
    group_concat = (
        func.group_concat
        if session.bind.dialect.name in ("sqlite",)
        else func.string_agg
    )
    final_query = (
        select(
            paths.c.node_name,
            paths.c.node_display_name,
            column.name,
            column.type,
            group_concat(AttributeType.name, ",").label(
                "column_attribute_type_name",
            ),
            paths.c.join_path,
        )
        .select_from(paths)
        .join(NodeColumns, NodeColumns.node_id == paths.c.node_revision_id)
        .join(
            column,
            and_(
                NodeColumns.column_id == column.id,
                or_(
                    is_(paths.c.dimension_column, None),
                    paths.c.dimension_column == column.name,
                ),
            ),
        )
        .join(ColumnAttribute, column.id == ColumnAttribute.column_id, isouter=True)
        .join(
            AttributeType,
            ColumnAttribute.attribute_type_id == AttributeType.id,
            isouter=True,
        )
        .group_by(
            paths.c.node_name,
            paths.c.node_display_name,
            column.name,
            column.type,
            paths.c.join_path,
        )
        .union_all(
            select(
                NodeRevision.name,
                NodeRevision.display_name,
                Column.name,
                Column.type,
                group_concat(AttributeType.name, ",").label(
                    "column_attribute_type_name",
                ),
                literal("").label("join_path"),
            )
            .select_from(NodeRevision)
            .join(NodeColumns, NodeColumns.node_id == NodeRevision.id)
            .join(Column, NodeColumns.column_id == Column.id)
            .join(
                ColumnAttribute,
                Column.id == ColumnAttribute.column_id,
                isouter=True,
            )
            .join(
                AttributeType,
                ColumnAttribute.attribute_type_id == AttributeType.id,
                isouter=True,
            )
            .group_by(
                NodeRevision.name,
                NodeRevision.display_name,
                Column.name,
                Column.type,
                "join_path",
            )
            .where(NodeRevision.id == node_revision.id),
        )
    )

    def _extract_roles_from_path(join_path) -> str:
        """Extracts dimension roles from the query results' join path"""
        roles = [
            path.replace("[", "").replace("]", "").split(".")[-1]
            for path in join_path.split(",")
            if "[" in path  # this indicates that this a role
        ]
        non_empty_roles = [role for role in roles if role]
        return f"[{'->'.join(non_empty_roles)}]" if non_empty_roles else ""

    # Only include a given column it's an attribute on a dimension node or
    # if the column is tagged with the attribute type 'dimension'
    dimension_attributes = (await session.execute(final_query)).all()
    return sorted(
        [
            DimensionAttributeOutput(
                name=f"{node_name}.{column_name}{_extract_roles_from_path(join_path)}",
                node_name=node_name,
                node_display_name=node_display_name,
                properties=attribute_types.split(",") if attribute_types else [],
                type=str(column_type),
                path=[
                    (path.replace("[", "").replace("]", "")[:-1])
                    if path.replace("[", "").replace("]", "").endswith(".")
                    else path.replace("[", "").replace("]", "")
                    for path in join_path.split(",")[:-1]
                ]
                if join_path
                else [],
            )
            for (
                node_name,
                node_display_name,
                column_name,
                column_type,
                attribute_types,
                join_path,
            ) in dimension_attributes
            if (  # column has dimension attribute
                join_path == ""
                and attribute_types is not None
                and (
                    ColumnAttributes.DIMENSION.value in attribute_types
                    or ColumnAttributes.PRIMARY_KEY.value in attribute_types
                )
            )
            or (  # column is on dimension node
                join_path != ""
                or (
                    node_name == node_revision.name
                    and node_revision.type == NodeType.DIMENSION
                )
            )
        ],
        key=lambda x: (x.name, ",".join(x.path)),
    )


async def get_dimensions(
    session: AsyncSession,
    node: Node,
    with_attributes: bool = True,
    depth: int = 30,
) -> List[Union[DimensionAttributeOutput, Node]]:
    """
    Return all available dimensions for a given node.
    * Setting `attributes` to True will return a list of dimension attributes,
    * Setting `attributes` to False will return a list of dimension nodes
    """
    if node.type == NodeType.METRIC:
        dag = await get_dimensions_dag(
            session,
            node.current.parents[0].current,
            with_attributes,
            depth=depth,
        )
    else:
        await session.refresh(node, attribute_names=["current"])
        dag = await get_dimensions_dag(
            session,
            node.current,
            with_attributes,
            depth=depth,
        )
    return dag


async def get_filter_only_dimensions(
    session: AsyncSession,
    node_name: str,
):
    """
    Get dimensions for this node that can only be filtered by and cannot be grouped by
    or retrieved as a part of the node's SELECT clause.
    """
    filter_only_dimensions = []
    upstreams = await get_upstream_nodes(session, node_name, node_type=NodeType.SOURCE)
    for upstream in upstreams:
        await session.refresh(upstream.current, ["dimension_links"])
        for link in upstream.current.dimension_links:
            await session.refresh(link.dimension, ["current"])
            await session.refresh(link.dimension.current, ["columns"])
            column_mapping = {col.name: col for col in link.dimension.current.columns}
            filter_only_dimensions.extend(
                [
                    DimensionAttributeOutput(
                        name=dim,
                        node_name=link.dimension.name,
                        node_display_name=link.dimension.current.display_name,
                        type=str(column_mapping[dim.split(SEPARATOR)[-1]].type),
                        path=[upstream.name],
                        filter_only=True,
                        properties=column_mapping[
                            dim.split(SEPARATOR)[-1]
                        ].attribute_names(),
                    )
                    for dim in link.foreign_keys.values()
                ],
            )
    return filter_only_dimensions


async def group_dimensions_by_name(
    session: AsyncSession,
    node: Node,
) -> Dict[str, List[DimensionAttributeOutput]]:
    """
    Group the dimensions for the node by the dimension attribute name
    """
    return {
        k: list(v)
        for k, v in itertools.groupby(
            await get_dimensions(session, node),
            key=lambda dim: dim.name,
        )
    }


async def get_shared_dimensions(
    session: AsyncSession,
    metric_nodes: List[Node],
) -> List[DimensionAttributeOutput]:
    """
    Return a list of dimensions that are common between the metric nodes.
    """
    parents = await get_metric_parents(session, metric_nodes)
    return await get_common_dimensions(session, parents)


async def get_metric_parents(
    session: AsyncSession,
    metric_nodes: list[Node],
) -> list[Node]:
    """
    Return a list of parent nodes of the metrics
    """
    find_latest_node_revisions = [
        and_(
            NodeRevision.name == metric_node.name,
            NodeRevision.version == metric_node.current_version,
        )
        for metric_node in metric_nodes
    ]
    statement = (
        select(Node)
        .where(or_(*find_latest_node_revisions))
        .select_from(
            join(
                join(
                    NodeRevision,
                    NodeRelationship,
                ),
                Node,
                NodeRelationship.parent_id == Node.id,
            ),
        )
    )
    return list(set((await session.execute(statement)).scalars().all()))


async def get_common_dimensions(session: AsyncSession, nodes: list[Node]):
    """
    Return a list of dimensions that are common between the nodes.
    """
    metric_nodes = [node for node in nodes if node.type == NodeType.METRIC]
    other_nodes = [node for node in nodes if node.type != NodeType.METRIC]
    if metric_nodes:
        nodes = list(set(other_nodes + await get_metric_parents(session, metric_nodes)))

    common = await group_dimensions_by_name(session, nodes[0])
    for node in nodes[1:]:
        node_dimensions = await group_dimensions_by_name(session, node)

        # Merge each set of dimensions based on the name and path
        to_delete = set(common.keys() - node_dimensions.keys())
        common_dim_keys = common.keys() & list(node_dimensions.keys())
        if not common_dim_keys:
            return []
        for dim_key in to_delete:
            del common[dim_key]  # pragma: no cover
    return sorted(
        [y for x in common.values() for y in x],
        key=lambda x: (x.name, x.path),
    )


async def get_nodes_with_dimension(
    session: AsyncSession,
    dimension_node: Node,
    node_types: Optional[List[NodeType]] = None,
) -> List[NodeRevision]:
    """
    Find all nodes that can be joined to a given dimension
    """
    to_process = [dimension_node]
    processed: Set[str] = set()
    final_set: Set[NodeRevision] = set()
    while to_process:
        current_node = to_process.pop()
        processed.add(current_node.name)

        # Dimension nodes are used to expand the searchable graph by finding
        # the next layer of nodes that are linked to this dimension
        if current_node.type == NodeType.DIMENSION:
            statement = (
                select(NodeRevision)
                .join(
                    Node,
                    onclause=(
                        (NodeRevision.node_id == Node.id)
                        & (Node.current_version == NodeRevision.version)
                    ),
                )
                .join(
                    NodeColumns,
                    onclause=(NodeRevision.id == NodeColumns.node_id),
                )
                .join(
                    Column,
                    onclause=(NodeColumns.column_id == Column.id),
                )
                .where(
                    Column.dimension_id.in_(  # type: ignore
                        [current_node.id],
                    ),
                )
            )
            node_revisions = (
                (
                    await session.execute(
                        statement.options(contains_eager(NodeRevision.node)),
                    )
                )
                .unique()
                .scalars()
                .all()
            )

            dim_link_statement = (
                select(NodeRevision)
                .select_from(DimensionLink)
                .join(
                    NodeRevision,
                    onclause=(DimensionLink.node_revision_id == NodeRevision.id),
                )
                .join(
                    Node,
                    onclause=(
                        (NodeRevision.node_id == Node.id)
                        & (Node.current_version == NodeRevision.version)
                    ),
                )
                .where(DimensionLink.dimension_id.in_([current_node.id]))
            )
            nodes_via_dimension_link = (
                (
                    await session.execute(
                        dim_link_statement.options(contains_eager(NodeRevision.node)),
                    )
                )
                .unique()
                .scalars()
                .all()
            )
            for node_rev in node_revisions + nodes_via_dimension_link:
                if node_rev.name not in processed:  # pragma: no cover
                    to_process.append(node_rev.node)
        else:
            # All other nodes are added to the result set
            current_node = await Node.get_by_name(  # type: ignore
                session,
                current_node.name,
                options=[
                    joinedload(Node.current).options(
                        *NodeRevision.default_load_options(),
                    ),
                    selectinload(Node.children).options(
                        selectinload(NodeRevision.node),
                    ),
                ],
            )
            if current_node:
                final_set.add(current_node.current)
                for child in current_node.children:
                    if child.name not in processed:
                        to_process.append(child.node)
    if node_types:
        return [node for node in final_set if node.type in node_types]
    return list(final_set)


async def get_nodes_with_common_dimensions(
    session: AsyncSession,
    common_dimensions: List[Node],
    node_types: Optional[List[NodeType]] = None,
) -> List[NodeRevision]:
    """
    Find all nodes that share a list of common dimensions
    """
    nodes_that_share_dimensions = set()
    first = True
    for dimension in common_dimensions:
        new_nodes = await get_nodes_with_dimension(session, dimension, node_types)
        if first:
            nodes_that_share_dimensions = set(new_nodes)
            first = False
        else:
            nodes_that_share_dimensions = nodes_that_share_dimensions.intersection(
                set(new_nodes),
            )
            if not nodes_that_share_dimensions:
                break
    return list(nodes_that_share_dimensions)


def topological_sort(nodes: List[Node]) -> List[Node]:
    """
    Sort a list of nodes into topological order so that the nodes with the most dependencies
    are later in the list, and the nodes with the fewest dependencies are earlier.
    """
    all_nodes = {node.name: node for node in nodes}

    # Build adjacency list and calculate in-degrees
    adjacency_list: Dict[str, List[Node]] = {}
    in_degrees: Dict[str, int] = {}
    for node in nodes:
        adjacency_list[node.name] = [
            parent for parent in node.current.parents if parent.name in all_nodes
        ]
        in_degrees[node.name] = 0
    for parents in adjacency_list.values():
        for parent in parents:
            in_degrees[parent.name] += 1

    # Initialize queue with nodes having in-degree 0
    queue: List[Node] = [
        all_nodes[name] for name, degree in in_degrees.items() if degree == 0
    ]

    # Perform topological sort using Kahn's algorithm
    sorted_nodes: List[Node] = []
    while queue:
        current_node = queue.pop(0)
        sorted_nodes.append(current_node)
        for child in adjacency_list.get(current_node.name, []):
            in_degrees[child.name] -= 1
            if in_degrees[child.name] == 0:
                queue.append(child)

    # Check for cycles
    if len(sorted_nodes) != len(in_degrees):
        raise DJGraphCycleException("Graph has at least one cycle")

    return sorted_nodes[::-1]


async def get_dimension_dag_indegree(session, node_names: List[str]) -> Dict[str, int]:
    """
    For a given node, calculate the indegrees for its dimensions graph by finding the number
    of dimension links that reference this node. Non-dimension nodes will always have an
    indegree of 0.
    """
    nodes = await Node.get_by_names(session, node_names)
    dimension_ids = [node.id for node in nodes]
    statement = (
        select(
            DimensionLink.dimension_id,
            func.count(DimensionLink.id),
        )
        .where(DimensionLink.dimension_id.in_(dimension_ids))
        .group_by(DimensionLink.dimension_id)
    )
    result = await session.execute(statement)
    link_counts = {link[0]: link[1] for link in result.unique().all()}
    dimension_dag_indegree = {node.name: link_counts.get(node.id, 0) for node in nodes}
    return dimension_dag_indegree
