# pragma: no cover
"""
Functions for making queries directly against DJ
"""

from typing import Dict, List, Set, Tuple

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.construction.build import build_metric_nodes
from datajunction_server.construction.build_v2 import QueryBuilder
from datajunction_server.construction.utils import try_get_dj_node
from datajunction_server.database.node import Node
from datajunction_server.models.node_type import NodeType
from datajunction_server.naming import amenable_name
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse


def selects_only_metrics(select: ast.Select) -> bool:
    """
    Checks that a Select only has FROM metrics
    """

    return (
        select.from_ is not None
        and len(select.from_.relations) == 1
        and len(select.from_.relations[0].extensions) == 0
        and str(select.from_.relations[0].primary) == "metrics"
    )


async def resolve_metric_queries(
    session: AsyncSession,
    tree: ast.Query,
    ctx: ast.CompileContext,
    touched_nodes: Set[int],
    metrics: List[Node],
):
    """
    Find all Metric Node references
    ensure they belong to a query sourcing `metrics`
    build the queries as if they were API queries
    replace them in the original query
    """
    for col in tree.find_all(ast.Column):
        curr_cols = []
        metric_nodes = []
        if id(col) in touched_nodes:
            continue
        ident = col.identifier(False)
        if metric_node := await try_get_dj_node(session, ident, {NodeType.METRIC}):
            curr_cols.append((True, col))
            # if we found a metric node we need to check where it came from
            parent_select = col.get_nearest_parent_of_type(ast.Select)
            if not parent_select or not selects_only_metrics(parent_select):
                raise ast.DJParseException(
                    "Any SELECT referencing a Metric must source "
                    "from a single unaliased Table named `metrics`.",
                )
            metric_nodes.append(metric_node)
            metrics.append(metric_node)
            dimensions = [str(exp) for exp in parent_select.group_by]

            if any(
                (
                    parent_select.having,
                    parent_select.lateral_views,
                    parent_select.set_op,
                ),
            ):
                raise ast.DJParseException(
                    "HAVING, LATERAL VIEWS, and SET OPERATIONS "
                    "are not allowed on `metrics` queries.",
                )

            for sibling_col in parent_select.projection:
                if sibling_col is col:
                    continue
                if not isinstance(sibling_col, ast.Column):
                    raise ast.DJParseException(
                        "Only direct Columns are allowed in "
                        f"`metrics` queries, found `{sibling_col}`.",
                    )

                sibling_ident = sibling_col.identifier(False)

                sibling_node = await try_get_dj_node(
                    session,
                    sibling_ident,
                    {NodeType.METRIC},
                )

                if sibling_ident in dimensions:
                    curr_cols.append((False, sibling_col))
                elif sibling_node is not None:
                    curr_cols.append((True, sibling_col))
                else:
                    raise ast.DJParseException(
                        "You can only select direct METRIC nodes or a column "
                        f"from your GROUP BY on `metrics` queries, found `{sibling_col}`",
                    )

                if sibling_node:
                    metric_nodes.append(sibling_node)
                touched_nodes.add(id(sibling_col))

            filters = [str(parent_select.where)] if parent_select.where else []

            orderby = [
                str(sort)
                for sort in (
                    parent_select.organization.order + parent_select.organization.sort
                    if parent_select.organization
                    else []
                )
            ]
            limit = None
            if limit_exp := parent_select.limit:
                try:
                    limit = int(str(limit_exp))  # type: ignore
                except ValueError as exc:
                    raise ast.DJParseException(
                        f"LIMITs on `metrics` queries can only be integers not `{limit_exp}`.",
                    ) from exc
            touched_nodes.add(id(col))

            cte_name = ast.Name(f"metric_query_{len(tree.ctes)}")

            built = (
                (
                    await build_metric_nodes(
                        session,
                        metric_nodes,
                        filters,
                        dimensions,
                        orderby,
                        limit,
                    )
                )
                .bake_ctes()
                .set_alias(cte_name)
                .set_as(True)
            )
            built.parenthesized = True
            built.compile(ctx)
            for built_col in built.columns:  # pragma: no cover
                built_col.alias_or_name.namespace = None  # pragma: no cover
            for _, cur_col in curr_cols:
                name = amenable_name(cur_col.identifier(False))
                ref_type = [
                    col
                    for col in built.select.projection
                    if col.alias_or_name.name == name
                ][0].type

                swap_col = (
                    ast.Column(ast.Name(name), _type=ref_type, _table=built)
                    .set_alias(cur_col.alias and cur_col.alias.copy())
                    .set_as(True)
                )
                cur_col.swap(swap_col)

            swap_select = ast.Select(
                projection=parent_select.projection,
                from_=ast.From(
                    relations=[ast.Relation(primary=ast.Table(built.alias_or_name))],
                ),
            )
            parent_select.swap(swap_select)

            tree.ctes = tree.ctes + [built]
            touched_nodes.add(id(built))
            touched_nodes.add(id(swap_select))
    return tree


def find_all_other(
    node: ast.Node,
    touched_nodes: Set[int],
    node_map: Dict[str, List[ast.Column]],
):
    """
    Find all non-Metric DJ Node references
    """
    if id(node) in touched_nodes:
        return
    touched_nodes.add(id(node))
    if isinstance(node, ast.Column) and node.table is None:
        if namespace_name := node.name.namespace:
            namespace = namespace_name.identifier(False)
            if namespace in node_map:
                node_map[namespace].append(node)
            else:
                node_map[namespace] = [node]
    else:
        node.apply(lambda n: find_all_other(n, touched_nodes, node_map))


async def resolve_all(
    session: AsyncSession,
    ctx: ast.CompileContext,
    tree: ast.Query,
    dj_nodes: List[Node],
):
    """
    Resolve all references to DJ Nodes
    """
    touched_nodes: Set[int] = set()
    tree = await resolve_metric_queries(session, tree, ctx, touched_nodes, dj_nodes)
    node_map: Dict[str, List[ast.Column]] = {}
    find_all_other(tree, touched_nodes, node_map)
    for namespace, cols in node_map.items():
        if dj_node := await try_get_dj_node(  # pragma: no cover
            session,
            namespace,
            {NodeType.SOURCE, NodeType.TRANSFORM, NodeType.DIMENSION},
        ):
            dj_nodes.append(dj_node)
            cte_name = ast.Name(f"node_query_{len(tree.ctes)}")
            current_dj_node = dj_node.current
            query_builder = await QueryBuilder.create(session, current_dj_node)
            built = (await query_builder.build()).bake_ctes()
            built.alias = cte_name
            built.set_as(True)
            built.parenthesized = True
            await built.compile(ctx)
            for cur_col in cols:
                name = cur_col.name.name
                ref_col = [col for col in current_dj_node.columns if col.name == name][
                    0
                ]
                swap_col = (
                    ast.Column(ast.Name(name), _table=built, _type=ref_col.type)
                    .set_alias(cur_col.alias and cur_col.alias.copy())
                    .set_as(True)
                )
                cur_col.swap(swap_col)
            for tbl in tree.filter(
                lambda node: isinstance(node, ast.Table)
                and node.identifier(False) == dj_node.name,  # type: ignore
            ):
                tbl.name = cte_name
            tree.ctes = tree.ctes + [built]
            built.parent = tree
    return tree


async def build_dj_query(
    session: AsyncSession,
    query: str,
) -> Tuple[ast.Query, List[Node]]:
    """
    Build a sql query that refers to DJ Nodes
    """
    dj_nodes: List[Node] = []  # metrics first if any
    ctx = ast.CompileContext(session, ast.DJException())
    tree = parse(query).bake_ctes()
    tree = await resolve_all(session, ctx, tree, dj_nodes)
    await tree.compile(ctx)
    if not dj_nodes:
        raise ast.DJParseException(f"Found no dj nodes in query `{query}`.")
    return tree, dj_nodes
