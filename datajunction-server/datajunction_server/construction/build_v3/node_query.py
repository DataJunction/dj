"""
Single-node SQL builder for non-metric / non-cube nodes (v3-style).

The metric-centric entrypoints (``build_metrics_sql`` / ``build_measures_sql``)
require at least one metric and decompose it into aggregation components. For
``/data/{node}`` and ``/sql/{node}`` requests against dimension / transform /
source nodes, we just need to render the node as executable SQL with any
non-source upstream parents inlined as CTEs and source parents inlined as
physical-table refs.

Implementation strategy: lean entirely on existing v3 primitives.

  1. ``find_upstream_node_names`` — pure parent-tracing (recursive CTE).
  2. ``batch_load_nodes_with_dependencies`` — shared eager-load tree.
  3. ``collect_node_ctes`` — compiles each node's query, rewrites parent refs
     (sources → physical, transforms → CTE name), and pushes ``PushdownFilters``
     into the CTEs whose columns the filters reference. Same primitive the
     measures path uses at ``measures.py:972``.

The starting node's body comes back in the same list as its ancestors. We
lift it out, attach the rest as CTEs, and apply outer-level concerns (LIMIT
in Phase 1; ORDER BY / WHERE / dim joins in Phase 2). No bespoke compilation,
no abuse of ``ctx.metrics``.

Phase 1 scope: no requested dimensions / filters / orderby. Limit only.
The Phase 2 surface is sketched in the docstring of ``build_node_sql_v3`` so
when we add dim-join + filter-pushdown we know exactly which v3 primitives
slot in: ``dimensions.resolve_dimensions``, ``dimensions.build_join_clause``,
and ``PushdownFilters`` for the existing ``collect_node_ctes`` call.
"""

import logging
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.construction.build_v3.builder import substitute_query_params
from datajunction_server.construction.build_v3.cte import collect_node_ctes
from datajunction_server.construction.build_v3.loaders import (
    batch_load_nodes_with_dependencies,
    find_upstream_node_names,
)
from datajunction_server.construction.build_v3.types import (
    BuildContext,
    ColumnMetadata,
    GeneratedSQL,
)
from datajunction_server.construction.build_v3.utils import get_cte_name
from datajunction_server.database.column import Column as DBColumn
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.dialect import Dialect
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.parsing import ast


logger = logging.getLogger(__name__)


async def build_node_sql_v3(
    session: AsyncSession,
    node_name: str,
    limit: int | None = None,
    dialect: Dialect = Dialect.SPARK,
    use_materialized: bool = True,
    query_parameters: dict[str, Any] | None = None,
) -> GeneratedSQL:
    """
    Build executable SQL for a single non-metric / non-cube node.

    Phase 1 (current): no requested dimensions / filters / orderby. Output
    is the starting node's compiled query body, with non-source upstream
    parents attached as CTEs and an optional LIMIT.

    Phase 2 (future): when dimensions / filters / orderby are added, this
    function will:
      - call ``dimensions.resolve_dimensions(ctx, starting)`` to find join
        paths to requested dimensions and add those dim nodes to the load set,
      - pass ``PushdownFilters`` to ``collect_node_ctes`` so user-supplied
        filters get pushed into upstream CTEs whose columns they reference,
      - apply ``dimensions.build_join_clause`` per resolved dimension to add
        JOIN clauses to the starting body,
      - apply non-pushdownable filters as outer WHERE / orderby on the
        starting body's Select.
    Until then, the routing layer (``internal/sql.py:build_node_sql``) falls
    through to v2 for those request shapes.
    """
    upstream_names, parent_map = await find_upstream_node_names(
        session,
        [node_name],
    )
    upstream_names.add(node_name)
    nodes = await batch_load_nodes_with_dependencies(session, upstream_names)
    nodes_dict = {node.name: node for node in nodes}

    starting = nodes_dict.get(node_name)
    if starting is None:  # pragma: no cover
        raise DJInvalidInputException(
            f"Node `{node_name}` does not exist or is deactivated.",
        )
    if starting.type in {NodeType.METRIC, NodeType.CUBE}:  # pragma: no cover
        raise DJInvalidInputException(
            f"build_node_sql_v3 expects a non-metric / non-cube node; "
            f"got {starting.type.value} for {node_name}",
        )

    ctx = BuildContext(
        session=session,
        metrics=[],
        dimensions=[],
        filters=[],
        dialect=dialect,
        use_materialized=use_materialized,
        nodes=nodes_dict,
        parent_map=parent_map,
    )

    starting_revision: NodeRevision = starting.current  # type: ignore[assignment]
    starting_columns: list[DBColumn] = list(starting_revision.columns)

    if starting.type == NodeType.SOURCE:
        # Sources don't get CTEs in v3 (collect_node_ctes skips them and
        # inlines them as physical-table refs in dependent CTEs). For a
        # source *as the starting node* there's nothing to delegate to, so
        # we build a trivial ``SELECT cols FROM <catalog>.<schema>.<table>``.
        table_ref = ".".join(
            part
            for part in (
                starting_revision.catalog.name if starting_revision.catalog else None,
                starting_revision.schema_,
                starting_revision.table,
            )
            if part
        )
        final_query = ast.Query(
            select=ast.Select(
                projection=[ast.Column(ast.Name(col.name)) for col in starting_columns],
                from_=ast.From(
                    relations=[
                        ast.Relation(primary=ast.Table(ast.Name(table_ref))),
                    ],
                ),
                limit=ast.Number(limit) if limit is not None else None,
            ),
        )
    else:
        final_query = _node_query_with_upstream_ctes(ctx, starting, limit)

    if query_parameters:  # pragma: no cover
        substitute_query_params(final_query, query_parameters)

    return GeneratedSQL(
        query=final_query,
        columns=[
            ColumnMetadata(
                name=col.name,
                semantic_name=f"{starting.name}.{col.name}",
                type=str(col.type),
                semantic_type="dimension",
            )
            for col in starting_columns
        ],
        dialect=dialect,
    )


def _node_query_with_upstream_ctes(
    ctx: BuildContext,
    starting: Node,
    limit: int | None,
) -> ast.Query:
    """
    Use the starting node's compiled query *as* the outer query, with any
    non-source upstream parents attached as CTEs.

    A parent-less node renders as just its own SELECT plus an optional LIMIT.
    A node with non-source upstream parents renders as ``WITH parent_a AS
    (...), parent_b AS (...) <starting_node_query>`` — exactly the CTEs
    needed to define the parents the body actually references, with no
    redundant ``WITH starting AS (...) SELECT * FROM starting`` wrapper.
    """
    # ``collect_node_ctes`` returns (cte_name, body) pairs for every non-source
    # node in the dependency chain — including the starting node itself, with
    # its parent table refs already rewritten by ``rewrite_table_references``
    # (sources → physical names, transforms → CTE names). Same primitive the
    # measures path uses at ``measures.py:972``.
    cte_pairs, _ = collect_node_ctes(ctx, [starting])
    starting_cte_name = get_cte_name(starting.name)

    starting_body: ast.Query | None = None
    parent_pairs: list[tuple[str, ast.Query]] = []
    for cte_name, cte_body in cte_pairs:
        if cte_name == starting_cte_name:
            starting_body = cte_body
        else:
            parent_pairs.append((cte_name, cte_body))

    if starting_body is None:  # pragma: no cover
        raise DJInvalidInputException(
            f"collect_node_ctes did not produce a body for {starting.name}",
        )

    # Promote upstream parents to proper CTEs (canonical pattern from
    # ``measures.py:1007-1013``) and attach to the starting query.
    parent_ctes: list[ast.Query] = []
    for parent_name, parent_body in parent_pairs:
        parent_body.to_cte(ast.Name(parent_name), starting_body)
        parent_ctes.append(parent_body)
    starting_body.ctes = parent_ctes

    if limit is not None:
        starting_body.select.limit = ast.Number(limit)

    return starting_body
