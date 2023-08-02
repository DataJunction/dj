"""
Functions for making queries directly against DJ
"""
from typing import Dict, List, Optional, Set, Tuple

from sqlmodel import Session

from datajunction_server.construction.build import build_metric_nodes, build_node
from datajunction_server.construction.utils import DJErrorException, get_dj_node
from datajunction_server.models.node import Node, NodeType
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.utils import amenable_name


def try_get_dj_node(
    session: Session,
    name: str,
    kinds: Set[NodeType],
) -> Optional[Node]:
    "wraps get dj node to return None if no node is found"
    try:
        return get_dj_node(session, name, kinds, current=False)
    except DJErrorException:
        return None


def selects_only_metrics(select: ast.Select) -> bool:
    """
    Checks that a Select only has FROM metrics
    """
    if select.from_ is None:
        return False

    relations = select.from_.relations
    if len(relations) != 1:
        return False

    relation = relations[0]
    if len(relation.extensions) > 0:
        return False

    primary = relation.primary

    return str(primary) == "metrics"


def resolve_metric_queries(  # pylint: disable=R0914,R0912,R0915
    session: Session,
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
        curr_cols = [(True, col)]
        metric_nodes = []
        if id(col) in touched_nodes:
            continue
        ident = col.identifier(False)
        if metric_node := try_get_dj_node(session, ident, {NodeType.METRIC}):
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

                sibling_node = try_get_dj_node(
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
                build_metric_nodes(
                    session,
                    metric_nodes,
                    filters,
                    dimensions,
                    orderby,
                    limit,
                )
                .bake_ctes()
                .set_alias(cte_name)
                .set_as(True)
            )
            built.parenthesized = True
            built.compile(ctx)
            for built_col in built.columns:
                built_col.name.namespace = None
            for is_metric, cur_col in curr_cols:
                swap_col = (
                    ast.Column(
                        ast.Name(
                            amenable_name(cur_col.identifier(False))
                            if is_metric
                            else cur_col.name.name,
                            namespace=cte_name,
                        ),
                    )
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
    if isinstance(node, ast.Column):
        if namespace_name := node.name.namespace:
            namespace = namespace_name.identifier(False)
            if namespace in node_map:
                node_map[namespace].append(node)
            else:
                node_map[namespace] = [node]
    else:
        node.apply(lambda n: find_all_other(n, touched_nodes, node_map))


def resolve_all(
    session: Session,
    ctx: ast.CompileContext,
    tree: ast.Query,
    dj_nodes: List[Node],
):
    """
    Resolve all references to DJ Nodes
    """
    touched_nodes: Set[int] = set()
    tree = resolve_metric_queries(session, tree, ctx, touched_nodes, dj_nodes)
    node_map: Dict[str, List[ast.Column]] = {}
    find_all_other(tree, touched_nodes, node_map)
    for namespace, cols in node_map.items():
        if dj_node := try_get_dj_node(
            session,
            namespace,
            {NodeType.SOURCE, NodeType.TRANSFORM, NodeType.DIMENSION},
        ):
            dj_nodes.append(dj_node)
            cte_name = ast.Name(f"node_query_{len(tree.ctes)}")
            built = (
                build_node(
                    session,
                    dj_node.current,
                )
                .bake_ctes()
                .set_alias(cte_name)
                .set_as(True)
            )
            built.parenthesized = True
            built.compile(ctx)
            for cur_col in cols:
                swap_col = (
                    ast.Column(
                        ast.Name(
                            cur_col.name.name,
                            namespace=cte_name,
                        ),
                    )
                    .set_alias(cur_col.alias and cur_col.alias.copy())
                    .set_as(True)
                )
                cur_col.swap(swap_col)
            tree.ctes = tree.ctes + [built]
            built.parent = tree
    return tree


def build_dj_query(session: Session, query: str) -> Tuple[ast.Query, List[Node]]:
    """
    Build a sql query that refers to DJ Nodes
    """
    dj_nodes: List[Node] = []  # metrics first if any
    ctx = ast.CompileContext(session, ast.DJException())
    tree = parse(query)
    tree = resolve_all(session, ctx, tree, dj_nodes)
    if not dj_nodes:
        raise ast.DJParseException(f"Found no dj nodes in query `{query}`.")
    tree.compile(ctx)
    return tree, dj_nodes
