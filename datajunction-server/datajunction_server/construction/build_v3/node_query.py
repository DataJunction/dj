"""
Single-node SQL builder for non-metric / non-cube nodes (v3-style).

The metric-centric entrypoints (``build_metrics_sql`` / ``build_measures_sql``)
require at least one metric and decompose it into aggregation components. For
``/data/{node}`` and ``/sql/{node}`` requests against dimension / transform /
source nodes, we just need to render the node as executable SQL with any
non-source upstream parents inlined as CTEs and source parents inlined as
physical-table refs — and, when the request includes them, dim-link joins for
requested dimensions.

Implementation strategy: lean entirely on existing v3 primitives.

  - ``find_upstream_node_names`` — pure parent-tracing (recursive CTE).
  - ``batch_load_nodes_with_dependencies`` — shared eager-load tree.
  - ``collect_node_ctes`` — compiles each node's query, rewrites parent refs
    (sources → physical, transforms → CTE name), and pushes ``PushdownFilters``
    into the CTEs whose columns the filters reference.
  - ``resolve_dimensions`` — walks dim links from the starting node to find
    join paths to requested dimensions.
  - ``build_dimension_joins`` — multi-hop JOIN AST construction.
  - ``build_dimension_col_expr`` — alias-mapped projection of dim columns.

Two output shapes depending on whether dimensions are requested:

  *Simple shape* (no dims): the starting node's compiled query *is* the
  outer query. Parent-less node renders as just its own SELECT; node with
  non-source parents renders as ``WITH parent_a AS (...) <starting body>``.

  *Wrapped shape* (dims requested): the starting body becomes a CTE; a new
  outer SELECT projects the starting node's columns + aliased dim columns,
  and JOINs the resolved dim chains.
"""

import logging
from typing import Any, cast

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.construction.build_v3.builder import substitute_query_params
from datajunction_server.construction.build_v3.cte import collect_node_ctes
from datajunction_server.construction.build_v3.dimensions import (
    parse_dimension_ref,
    resolve_dimensions,
)
from datajunction_server.construction.build_v3.loaders import (
    batch_load_nodes_with_dependencies,
    find_upstream_node_names,
    load_post_preload_chain,
    preload_join_paths,
)
from datajunction_server.construction.build_v3.measures import (
    build_dimension_col_expr,
    build_dimension_joins,
    build_filter_column_aliases,
    build_outer_where,
    collect_cte_nodes_and_needed_columns,
)
from datajunction_server.construction.build_v3.types import (
    BuildContext,
    ColumnMetadata,
    GeneratedSQL,
    PushdownFilters,
    ResolvedDimension,
)
from datajunction_server.construction.build_v3.utils import (
    add_dimensions_from_filters,
    get_cte_name,
)
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
    dimensions: list[str] | None = None,
    filters: list[str] | None = None,
    limit: int | None = None,
    dialect: Dialect = Dialect.SPARK,
    use_materialized: bool = True,
    query_parameters: dict[str, Any] | None = None,
) -> GeneratedSQL:
    """
    Build executable SQL for a single non-metric / non-cube node.

    With no requested dimensions or filters, output is the starting node's
    compiled query body, with non-source upstream parents attached as CTEs
    and an optional LIMIT.

    With dimensions or filters, the starting body becomes a CTE and a new
    outer SELECT projects the starting node's columns plus the requested
    dim columns (alias-mapped via ``ctx.alias_registry``), with JOINs from
    ``build_dimension_joins`` for non-local dim links and a WHERE clause
    from ``build_outer_where`` for filters. Filters whose columns belong to
    upstream CTEs get pushed down via ``PushdownFilters`` for efficiency.

    Phase 2.3 (orderby) extends the outer SELECT with ORDER BY assembly.
    Until then, the routing layer falls through to v2 for orderby cases.
    """
    dim_list = list(dimensions or [])
    filter_list = list(filters or [])

    # Collect everything we need to load: starting node + its upstream chain
    # + every requested dim node + every dim referenced in filters.
    starting_set = {node_name}
    for dim_ref_str in dim_list:
        dim_ref = parse_dimension_ref(dim_ref_str)
        if dim_ref.node_name:
            starting_set.add(dim_ref.node_name)
    # Filters can reference dim nodes that aren't in ``dimensions`` — those
    # still need to be loaded so we can join + apply the filter. We handle
    # the dim-list expansion below via ``add_dimensions_from_filters``.

    upstream_names, parent_map = await find_upstream_node_names(
        session,
        list(starting_set),
    )
    upstream_names |= starting_set
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

    starting_revision: NodeRevision = starting.current  # type: ignore[assignment]
    starting_columns: list[DBColumn] = list(starting_revision.columns)

    ctx = BuildContext(
        session=session,
        metrics=[],
        dimensions=dim_list,
        filters=filter_list,
        dialect=dialect,
        use_materialized=use_materialized,
        nodes=nodes_dict,
        parent_map=parent_map,
    )

    # ``add_dimensions_from_filters`` parses ``ctx.filters`` for dim refs
    # and adds them to ``ctx.dimensions`` (and ``ctx.filter_dimensions`` so
    # they're excluded from the output projection). Dim refs from filters
    # are resolved + joined the same way user-requested dimensions are —
    # they just don't appear as projected columns.
    if filter_list:
        add_dimensions_from_filters(ctx)

    # Wrap path is required when anything beyond the bare node is requested
    # (dims, filters that pull in dim joins, etc). ``add_dimensions_from_filters``
    # may have grown ``ctx.dimensions`` so re-check after that.
    if ctx.dimensions or filter_list:
        # ``resolve_dimensions`` reads from ``ctx.join_paths`` to discover
        # links from the starting node to each requested dim. Without this
        # preload it returns "Cannot find join path" even when the link
        # exists in the database.
        target_dim_names = {
            parse_dimension_ref(d).node_name
            for d in ctx.dimensions
            if parse_dimension_ref(d).node_name
        }
        await preload_join_paths(
            ctx,
            {starting_revision.id},
            target_dim_names,
        )
        # Multi-hop join paths add intermediate dim nodes to ``ctx.nodes``
        # via ``preload_join_paths`` — but only with the limited eager-load
        # the dimension_links query gives them. Reload them with the full
        # tree so ``rewrite_table_references`` and ``get_parsed_query`` work.
        await load_post_preload_chain(ctx, baseline_node_names=upstream_names)
        resolved_dims = resolve_dimensions(ctx, starting)
        final_query = _build_with_dimensions(
            ctx,
            starting,
            starting_columns,
            resolved_dims,
            filter_list,
            limit,
        )
        output_columns = _columns_metadata_with_dims(
            starting,
            starting_columns,
            resolved_dims,
            ctx,
        )
    else:
        final_query = _build_no_dimensions(
            ctx,
            starting,
            starting_columns,
            limit,
        )
        output_columns = [
            ColumnMetadata(
                name=col.name,
                semantic_name=f"{starting.name}.{col.name}",
                type=str(col.type),
                semantic_type="dimension",
            )
            for col in starting_columns
        ]

    if query_parameters:  # pragma: no cover
        substitute_query_params(final_query, query_parameters)

    return GeneratedSQL(
        query=final_query,
        columns=output_columns,
        dialect=dialect,
    )


# ---------------------------------------------------------------------------
# No-dimensions path: starting body IS the outer query (no wrapping).
# ---------------------------------------------------------------------------


def _build_no_dimensions(
    ctx: BuildContext,
    starting: Node,
    starting_columns: list[DBColumn],
    limit: int | None,
) -> ast.Query:
    """
    No dimensions requested. For sources, emit ``SELECT cols FROM
    <physical_table>`` directly. For non-sources, lift the starting body out
    of ``collect_node_ctes`` and use it as the outer query, attaching any
    upstream non-source parents as CTEs.
    """
    if starting.type == NodeType.SOURCE:
        # Sources don't get CTEs in v3 (collect_node_ctes skips them); for a
        # source *as the starting node*, we have nothing to delegate to.
        revision: NodeRevision = starting.current  # type: ignore[assignment]
        table_ref = _physical_table_ref(revision)
        return ast.Query(
            select=ast.Select(
                projection=[ast.Column(ast.Name(col.name)) for col in starting_columns],
                from_=ast.From(
                    relations=[ast.Relation(primary=ast.Table(ast.Name(table_ref)))],
                ),
                limit=ast.Number(limit) if limit is not None else None,
            ),
        )

    # ``collect_node_ctes`` returns (cte_name, body) pairs for every non-source
    # node in the dependency chain — including the starting node itself, with
    # its parent table refs already rewritten by ``rewrite_table_references``.
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


# ---------------------------------------------------------------------------
# With-dimensions path: outer SELECT joins dim chains; starting body is a CTE.
# ---------------------------------------------------------------------------


def _build_with_dimensions(
    ctx: BuildContext,
    starting: Node,
    starting_columns: list[DBColumn],
    resolved_dims: list[ResolvedDimension],
    filter_list: list[str],
    limit: int | None,
) -> ast.Query:
    """
    Build the outer SELECT projecting starting cols + dim cols, with JOINs.

    Shape::

        WITH starting_cte AS (<starting body>),
             dim_a_cte AS (...),
             ...
        SELECT
          starting.col1, starting.col2, ...,                  -- node's own cols
          dim_a.country AS country,                           -- dim col, registry-aliased
          ...
        FROM <starting_cte | physical_table> starting
        LEFT JOIN dim_a_cte dim_a ON ...
        LEFT JOIN ...
        WHERE <filters>
        LIMIT N

    Filters whose columns belong to upstream CTEs get pushed down into
    those CTEs via ``PushdownFilters`` (handled inside ``collect_node_ctes``);
    everything else is applied at the outer ``WHERE``.
    """
    # ``collect_cte_nodes_and_needed_columns`` walks every link in every
    # resolved dim's ``join_path`` and adds each intermediate hop's
    # dimension to the CTE list — exactly what we need for multi-hop
    # chains where a dim link routes through an intermediate transform/dim.
    # Reused from measures.py so we don't drift.  We pass empty grain /
    # metric args because non-metric nodes don't decompose into components.
    nodes_for_ctes, _needed_columns = collect_cte_nodes_and_needed_columns(
        ctx,
        starting,
        resolved_dims,
        grain_col_specs=[],
        metric_expressions=[],
    )

    # Build the filter-column-alias map up front so ``PushdownFilters``
    # can resolve user filter refs to the right CTE columns.
    filter_column_aliases = (
        build_filter_column_aliases(ctx, resolved_dims, starting) if filter_list else {}
    )

    # ``collect_node_ctes`` skips sources (they get inlined as physical refs)
    # and produces bodies in dep order. We deliberately don't pass
    # ``needed_columns_by_node`` — the v3 metric path uses it for column
    # trimming, but for ``/sql/{node}`` we want each node's full projection
    # in the CTE so the user gets every column the node defines.
    cte_pairs, _ = collect_node_ctes(
        ctx,
        nodes_for_ctes,
        pushdown=PushdownFilters(
            filters=filter_list,
            column_aliases=filter_column_aliases,
        )
        if filter_list
        else None,
    )

    # Generate the alias for the starting (FROM) table — this is what
    # ``build_dimension_joins`` and ``build_dimension_col_expr`` use to qualify
    # column refs back to the starting node.
    main_alias = ctx.next_table_alias(starting.name)

    # Multi-hop joins (with dedup of shared sub-paths). Reused from measures.
    dim_aliases, joins = build_dimension_joins(ctx, resolved_dims, main_alias)

    # Outer projection: starting node's columns (qualified by main_alias) +
    # alias-registered dim columns from ``build_dimension_col_expr``.
    # Filter-only dimensions (those added by ``add_dimensions_from_filters``
    # but not user-requested) are excluded from the projection — they exist
    # to enable the filter, not to surface in the output.
    projection: list[Any] = [
        _qualified_column(col.name, main_alias) for col in starting_columns
    ]
    for resolved in resolved_dims:
        if resolved.original_ref in ctx.filter_dimensions:
            continue
        clean_alias = ctx.alias_registry.register(resolved.original_ref)
        projection.append(
            build_dimension_col_expr(resolved, main_alias, dim_aliases, clean_alias),
        )

    # FROM clause: source uses physical-table ref; non-source uses the
    # starting CTE's alias. In both cases we wrap with main_alias so the
    # qualified column refs above resolve.
    if starting.type == NodeType.SOURCE:
        revision: NodeRevision = starting.current  # type: ignore[assignment]
        from_table = ast.Table(ast.Name(_physical_table_ref(revision)))
    else:
        starting_cte_name = get_cte_name(starting.name)
        from_table = ast.Table(ast.Name(starting_cte_name))
    # Same pattern measures.py:935-940 uses for the FROM-side alias wrapper.
    primary = cast(
        ast.Expression,
        ast.Alias(child=from_table, alias=ast.Name(main_alias), as_=False),
    )

    # Outer WHERE: parse user filters and resolve column refs against the
    # starting node's main alias / dim CTE aliases. Filters that pushed
    # cleanly into upstream CTEs via ``PushdownFilters`` above will *also*
    # appear here at the outer level — that's the same shape measures.py
    # produces, and the database optimizer collapses the redundancy. The
    # WHERE is what guarantees correctness for filters whose columns aren't
    # in any pushdownable CTE (e.g. filters on a JOINed dim column).
    where_clause = (
        build_outer_where(
            filter_list,
            filter_column_aliases,
            resolved_dims,
            main_alias,
            dim_aliases,
            starting,
        )
        if filter_list
        else None
    )

    outer_query = ast.Query(
        select=ast.Select(
            projection=projection,
            from_=ast.From(
                relations=[ast.Relation(primary=primary, extensions=joins)],
            ),
            where=where_clause,
            limit=ast.Number(limit) if limit is not None else None,
        ),
    )

    # Promote each (cte_name, body) pair from collect_node_ctes to a real CTE
    # attached to the outer query. For non-source starting nodes, this also
    # promotes the starting body itself (sources are skipped by collect_node_ctes
    # so they're never present here).
    all_ctes: list[ast.Query] = []
    for cte_name, cte_body in cte_pairs:
        cte_body.to_cte(ast.Name(cte_name), outer_query)
        all_ctes.append(cte_body)
    outer_query.ctes = all_ctes

    return outer_query


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _physical_table_ref(revision: NodeRevision) -> str:
    """Build ``catalog.schema.table`` from a NodeRevision, skipping null parts."""
    return ".".join(
        part
        for part in (
            revision.catalog.name if revision.catalog else None,
            revision.schema_,
            revision.table,
        )
        if part
    )


def _qualified_column(col_name: str, table_alias: str) -> ast.Column:
    """``table_alias.col_name`` as an ast.Column."""
    return ast.Column(
        name=ast.Name(col_name, namespace=ast.Name(table_alias)),
    )


def _columns_metadata_with_dims(
    starting: Node,
    starting_columns: list[DBColumn],
    resolved_dims: list[ResolvedDimension],
    ctx: BuildContext,
) -> list[ColumnMetadata]:
    """Output column metadata for the dims-present path: node cols + dim cols."""
    columns = [
        ColumnMetadata(
            name=col.name,
            semantic_name=f"{starting.name}.{col.name}",
            type=str(col.type),
            semantic_type="dimension",
        )
        for col in starting_columns
    ]
    for resolved in resolved_dims:
        clean_alias = (
            ctx.alias_registry.get_alias(resolved.original_ref) or resolved.column_name
        )
        columns.append(
            ColumnMetadata(
                name=clean_alias,
                semantic_name=resolved.original_ref,
                type="string",  # type inference for joined dim cols is Phase 3
                semantic_type="dimension",
            ),
        )
    return columns
