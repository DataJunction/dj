"""
Server-side MCP tool implementations.

Each tool calls DJ internal services directly (no HTTP loop-through).
Tools read the per-request session from a ContextVar set by the HTTP
transport layer (see ``transport.py``).
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import mcp.types as types
from sqlalchemy import select
from sqlalchemy.orm import joinedload, selectinload
from sqlalchemy.sql.base import ExecutableOption

from datajunction_server.construction.build_v3.builder import (
    build_measures_sql,
    build_metrics_sql,
)
from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.internal.namespaces import get_git_info_for_namespace
from datajunction_server.mcp.context import get_mcp_session
from datajunction_server.mcp.formatters import (
    format_dimensions_compatibility,
    format_node_details,
    format_nodes_list,
)
from datajunction_server.models.dialect import Dialect
from datajunction_server.models.node import NodeMode, NodeStatus
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.dag import (
    get_common_dimensions,
    get_dimensions,
    get_downstream_nodes,
    get_nodes_with_common_dimensions,
    get_upstream_nodes,
)
from datajunction_server.utils import get_query_service_client, get_settings

logger = logging.getLogger(__name__)


async def _node_to_dict(node: Node, include_git: bool) -> Dict[str, Any]:
    """Adapt a SQLAlchemy ``Node`` into the dict shape ``format_nodes_list``
    and ``format_node_details`` expect (originally a GraphQL response).
    """
    current = node.current
    out: Dict[str, Any] = {
        "name": node.name,
        "type": node.type.value if hasattr(node.type, "value") else str(node.type),
        "current": (
            {
                "displayName": current.display_name,
                "description": current.description,
                "status": current.status.value
                if hasattr(current.status, "value")
                else str(current.status),
                "mode": current.mode.value
                if hasattr(current.mode, "value")
                else str(current.mode),
            }
            if current
            else {}
        ),
        "tags": [{"name": t.name, "tagType": t.tag_type} for t in (node.tags or [])],
        "owners": [
            {"username": o.username, "email": o.email} for o in (node.owners or [])
        ],
    }
    if include_git:
        session = get_mcp_session()
        git = await get_git_info_for_namespace(session, node.namespace)
        if git:
            out["gitInfo"] = {
                "repo": git.get("github_repo_path") or git.get("git_path"),
                "branch": git.get("git_branch"),
                "defaultBranch": git.get("default_branch"),
            }
    return out


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


async def list_namespaces() -> str:
    """List all namespaces with node counts."""
    from sqlalchemy import func

    session = get_mcp_session()
    statement = (
        select(NodeNamespace.namespace, func.count(Node.id).label("num_nodes"))
        .join(
            Node,
            onclause=NodeNamespace.namespace == Node.namespace,
            isouter=True,
        )
        .where(NodeNamespace.deactivated_at.is_(None))
        .group_by(NodeNamespace.namespace)
        .order_by(NodeNamespace.namespace)
    )
    result = await session.execute(statement)
    rows = result.all()

    if not rows:
        return "No namespaces found."

    lines = ["Available Namespaces:", ""]
    for namespace, num_nodes in rows:
        lines.append(f"  • {namespace} ({num_nodes or 0} nodes)")
    return "\n".join(lines)


async def search_nodes(
    query: str = "",
    node_type: Optional[str] = None,
    namespace: Optional[str] = None,
    tags: Optional[List[str]] = None,
    statuses: Optional[List[str]] = None,
    mode: Optional[str] = None,
    owned_by: Optional[str] = None,
    has_materialization: bool = False,
    limit: int = 25,
    prefer_main_branch: bool = True,
) -> str:
    """Search for nodes (metrics, dimensions, cubes, sources, transforms)."""
    session = get_mcp_session()

    # Resolve namespace → namespace.main when prefer_main_branch is set
    actual_namespace = namespace
    if (
        prefer_main_branch
        and namespace
        and namespace.split(".")[-1] not in ("main", "dev", "prod", "master")
    ):
        main_namespace = f"{namespace}.main"
        exists = await session.scalar(
            select(NodeNamespace.namespace).where(
                NodeNamespace.namespace == main_namespace,
                NodeNamespace.deactivated_at.is_(None),
            ),
        )
        if exists:
            actual_namespace = main_namespace
            logger.info("Resolved namespace '%s' to '%s'", namespace, actual_namespace)

    options: List[ExecutableOption] = [
        joinedload(Node.current),
        selectinload(Node.tags),
        selectinload(Node.owners),
    ]

    # Use the trigram-similarity ``search`` path rather than literal
    # ``fragment`` substring matching: natural-language queries like
    # "customer service volume" should rank ``cs.contact_volume_answered``
    # near the top by matching the description and overlapping tokens, not
    # require the user (or the LLM) to guess the substring shape.
    nodes = await Node.find_by(
        session,
        search=query or None,
        node_types=[NodeType(node_type)] if node_type else None,
        tags=tags or None,
        namespace=actual_namespace,
        mode=NodeMode(mode) if mode else None,
        owned_by=[owned_by] if owned_by else None,
        statuses=[NodeStatus(s) for s in statuses] if statuses else None,
        has_materialization=has_materialization,
        limit=limit,
        options=options,
    )

    nodes_dicts = [await _node_to_dict(n, include_git=True) for n in nodes]
    return format_nodes_list(nodes_dicts)


async def get_node_details(name: str) -> str:
    """Get detailed information about a specific node."""
    session = get_mcp_session()

    options: List[ExecutableOption] = [
        joinedload(Node.current).options(
            selectinload(NodeRevision.columns),
            selectinload(NodeRevision.parents),
            selectinload(NodeRevision.metric_metadata),
        ),
        selectinload(Node.tags),
        selectinload(Node.owners),
    ]
    node = await Node.get_by_name(session, name, options=options)
    if not node:
        return f"Node '{name}' not found."

    node_dict = await _node_to_dict(node, include_git=True)

    # Augment with the richer fields format_node_details renders.
    if node.current:
        node_dict["current"]["query"] = node.current.query
        node_dict["current"]["columns"] = [
            {"name": c.name, "type": str(c.type)} for c in (node.current.columns or [])
        ]
        node_dict["current"]["parents"] = [
            {
                "name": p.name,
                "type": (p.type.value if hasattr(p.type, "value") else str(p.type)),
            }
            for p in (node.current.parents or [])
        ]
        if node.current.metric_metadata:
            mm = node.current.metric_metadata
            unit = getattr(mm, "unit", None)
            # MetricMetadata.unit is a MetricUnit enum whose .value is a Unit
            # BaseModel; if a tool ever sees a raw Unit it still has .name/.label.
            unit_value = getattr(unit, "value", unit) if unit is not None else None
            node_dict["current"]["metricMetadata"] = {
                "direction": (
                    mm.direction.value
                    if mm.direction and hasattr(mm.direction, "value")
                    else None
                ),
                "unit": (
                    {
                        "name": getattr(unit_value, "name", None),
                        "label": getattr(unit_value, "label", None),
                    }
                    if unit_value is not None
                    else None
                ),
            }

    # Add dimensions for the node so format_node_details can list them.
    dim_dicts: List[Dict[str, Any]] = []
    if node.type in (
        NodeType.SOURCE,
        NodeType.TRANSFORM,
        NodeType.METRIC,
        NodeType.DIMENSION,
    ):
        dims = await get_dimensions(session, node, with_attributes=True)
        for d in dims:
            dim_dicts.append(
                {
                    "name": getattr(d, "name", str(d)),
                    "type": getattr(d, "type", None),
                },
            )

    return format_node_details(node_dict, dim_dicts or None)


async def get_common(
    metrics: Optional[List[str]] = None,
    dimensions: Optional[List[str]] = None,
) -> str:
    """Bidirectional metrics ↔ dimensions compatibility lookup."""
    if not metrics and not dimensions:
        return "Error: provide either 'metrics' or 'dimensions'."
    if metrics and dimensions:
        return "Error: provide either 'metrics' or 'dimensions', not both."

    session = get_mcp_session()

    if metrics:
        metric_nodes = []
        for name in metrics:
            n = await Node.get_by_name(
                session,
                name,
                options=[joinedload(Node.current)],
            )
            if n:
                metric_nodes.append(n)
        if not metric_nodes:
            return f"No metrics found among: {', '.join(metrics)}"

        common_dims = await get_common_dimensions(session, metric_nodes)
        # Adapt to the dict shape format_dimensions_compatibility expects.
        dim_dicts = [
            {"name": d.name, "type": getattr(d, "type", None)} for d in common_dims
        ]
        return format_dimensions_compatibility(metrics, dim_dicts)

    # dimensions branch
    dim_nodes = []
    for name in dimensions or []:
        n = await Node.get_by_name(session, name, options=[joinedload(Node.current)])
        if n:
            dim_nodes.append(n)
    if not dim_nodes:
        return f"No dimension nodes found among: {', '.join(dimensions or [])}"

    nodes = await get_nodes_with_common_dimensions(
        session,
        dim_nodes,
        node_types=[NodeType.METRIC],
    )
    lines = [
        "Metrics compatible with dimensions:",
        "=" * 60,
        "",
        f"Dimensions: {', '.join(dimensions or [])}",
        "",
    ]
    if not nodes:
        lines.append("No metrics found that share all specified dimensions.")
    else:
        lines.append(f"Found {len(nodes)} compatible metric(s):\n")
        for node in nodes:
            lines.append(f"  • {node.name}")
    return "\n".join(lines)


async def build_metric_sql(
    metrics: List[str],
    dimensions: Optional[List[str]] = None,
    filters: Optional[List[str]] = None,
    orderby: Optional[List[str]] = None,
    limit: Optional[int] = None,
    dialect: Optional[str] = None,
    use_materialized: bool = True,
) -> str:
    """Generate executable SQL for a set of metrics."""
    session = get_mcp_session()
    dialect_enum = Dialect(dialect) if dialect else None

    result = await build_metrics_sql(
        session=session,
        metrics=metrics,
        dimensions=dimensions or [],
        filters=filters or None,
        orderby=orderby or None,
        limit=limit,
        dialect=dialect_enum,
        use_materialized=use_materialized,
    )

    lines = [
        "Generated SQL Query:",
        "=" * 60,
        "",
        f"Dialect: {result.dialect.value if result.dialect else 'N/A'}",
        f"Cube: {result.cube_name or 'N/A'}",
        "",
        "SQL:",
        "-" * 60,
        result.sql,
        "",
        "Output Columns:",
        "-" * 60,
    ]
    for col in result.columns:
        semantic = f" (semantic: {col.semantic_name})" if col.semantic_name else ""
        lines.append(f"  • {col.name}: {col.type}{semantic}")
    return "\n".join(lines)


# Hard upper bound on ad-hoc scan size. Queries that would scan more than
# this much data refuse to execute and surface the estimate to the model so
# it can fall back to a coarser query, a smaller filter window, or recommend
# materializing a cube. Materialized queries (cube hit) bypass the check.
# 1 TB matches Trino's comfortable single-query ceiling; revisit if we see
# patterns that warrant tightening or per-engine overrides.
_AD_HOC_SCAN_LIMIT_BYTES = 1024**4


def _format_bytes(n: Optional[int]) -> str:
    if n is None:
        return "unknown"
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = float(n)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{n} B"  # pragma: no cover


async def _execute_metrics_query(
    metrics: List[str],
    dimensions: Optional[List[str]],
    filters: Optional[List[str]],
    orderby: Optional[List[str]],
    limit: Optional[int],
) -> tuple[Any, Any]:
    """Build SQL, run it, return (query_result, generated_sql).

    Policy:
      - Materialized cube → always run.
      - Ad-hoc Trino/etc → run if scan estimate is under the threshold.
      - No scan estimate available → run (we err on permissive; the model
        can learn from the actual query result faster than from a refusal).
      - Scan estimate over threshold → refuse with the estimate so the
        model can pick a smaller query.

    Raises ``ValueError`` (string-message) which the caller turns into an
    MCP-facing error block.
    """
    from datajunction_server.construction.build_v3.cube_matcher import (
        resolve_dialect_and_engine_for_metrics,
    )
    from datajunction_server.models.query import QueryCreate

    session = get_mcp_session()

    execution_ctx = await resolve_dialect_and_engine_for_metrics(
        session=session,
        metrics=metrics,
        dimensions=dimensions or [],
        use_materialized=True,
    )

    generated_sql = await build_metrics_sql(
        session=session,
        metrics=metrics,
        dimensions=dimensions or [],
        filters=filters or None,
        orderby=orderby or None,
        limit=limit,
        dialect=execution_ctx.dialect,
        use_materialized=True,
    )

    estimate = generated_sql.scan_estimate
    is_materialized = bool(estimate and estimate.has_materialization)
    total_bytes = estimate.total_bytes if estimate else None

    if (
        not is_materialized
        and total_bytes is not None
        and total_bytes > _AD_HOC_SCAN_LIMIT_BYTES
    ):
        raise ValueError(
            f"This query would scan ~{_format_bytes(total_bytes)} ad-hoc, "
            f"which is over the safety threshold ({_format_bytes(_AD_HOC_SCAN_LIMIT_BYTES)}). "
            f"Narrow the filters, request fewer dimensions, or — if this is a "
            f"frequent query — ask the data owner to materialize a cube for it.",
        )

    settings = get_settings()
    query_service_client = get_query_service_client(settings=settings)
    if query_service_client is None:
        raise ValueError(
            "Query service client is not configured on this DJ instance.",
        )

    query_create = QueryCreate(
        engine_name=execution_ctx.engine.name,
        catalog_name=execution_ctx.catalog_name,
        engine_version=execution_ctx.engine.version,
        submitted_query=generated_sql.sql,
        async_=False,
    )
    result = query_service_client.submit_query(query_create)
    if result.results.root:
        result.results.root[0].columns = generated_sql.columns or []  # type: ignore
    return result, generated_sql


async def get_metric_data(
    metrics: List[str],
    dimensions: Optional[List[str]] = None,
    filters: Optional[List[str]] = None,
    orderby: Optional[List[str]] = None,
    limit: Optional[int] = None,
) -> str:
    """Execute a metrics query and format rows.

    Runs against any available engine (cube → materialized fast path; else
    the catalog's default engine, typically Trino). Refuses up front if the
    scan estimate exceeds the safety threshold.
    """
    try:
        result, generated_sql = await _execute_metrics_query(
            metrics,
            dimensions,
            filters,
            orderby,
            limit,
        )
    except ValueError as exc:
        return f"⚠️  {exc}\n\nMetrics: {', '.join(metrics)}\nDimensions: {', '.join(dimensions or ['none'])}"

    lines = ["Query Results:", "=" * 60, ""]

    # Surface the scan estimate so the model knows whether it just hit a
    # cube or paid ad-hoc cost — useful context for the next query.
    estimate = generated_sql.scan_estimate
    if estimate is not None:
        if estimate.has_materialization:
            lines.append("Source: materialized cube")
        else:
            lines.append(
                f"Source: ad-hoc (scan ~{_format_bytes(estimate.total_bytes)})",
            )
        lines.append("")

    state = getattr(result, "state", None)
    lines.append(f"Query State: {state if state else 'unknown'}")
    if getattr(result, "id", None):
        lines.append(f"Query ID: {result.id}")

    results_root = getattr(getattr(result, "results", None), "root", None) or []
    rows: List[Dict[str, Any]] = []
    if results_root:
        first = results_root[0]
        column_names = [c.name for c in (first.columns or [])]
        for row in first.rows or []:
            rows.append(dict(zip(column_names, row)))

    if rows:
        lines.append(f"Row Count: {len(rows)}")
        lines.append("")
        lines.append("Data:")
        lines.append("-" * 60)
        for i, row in enumerate(rows[:10]):
            lines.append(f"Row {i + 1}:")
            for k, v in row.items():
                lines.append(f"  {k}: {v}")
            lines.append("")
        if len(rows) > 10:
            lines.append(f"... and {len(rows) - 10} more rows")
    else:
        lines.append("No results returned")

    errors = getattr(result, "errors", None)
    if errors:
        lines.append("")
        lines.append("Errors:")
        for e in errors:
            lines.append(f"  • {e}")

    return "\n".join(lines)


async def get_query_plan(
    metrics: List[str],
    dimensions: Optional[List[str]] = None,
    filters: Optional[List[str]] = None,
    dialect: Optional[str] = None,
    use_materialized: bool = True,
    include_temporal_filters: bool = False,
    lookback_window: Optional[str] = None,
) -> str:
    """Show how DJ decomposes the requested metrics into grain groups + components."""
    session = get_mcp_session()
    dialect_enum = Dialect(dialect) if dialect else Dialect.SPARK

    result = await build_measures_sql(
        session=session,
        metrics=metrics,
        dimensions=dimensions or [],
        filters=filters or None,
        dialect=dialect_enum,
        use_materialized=use_materialized,
        include_temporal_filters=include_temporal_filters,
        lookback_window=lookback_window,
    )

    lines = [
        "Query Execution Plan",
        "=" * 60,
        "",
        f"Dialect:    {result.dialect.value if result.dialect else 'N/A'}",
        f"Metrics:    {', '.join(metrics)}",
        f"Dimensions: {', '.join(result.requested_dimensions) if result.requested_dimensions else 'none'}",
        f"Grain Groups: {len(result.grain_groups)}",
        "",
    ]

    lines += ["Metric Formulas", "-" * 60]
    for metric_name, decomposed in result.decomposed_metrics.items():
        derived_tag = (
            " [derived]"
            if decomposed.is_derived_for_parents(
                result.ctx.parent_map.get(metric_name, []),
                result.ctx.nodes,
            )
            else ""
        )
        lines.append(f"  {metric_name}{derived_tag}")
        lines.append(f"    Original query:  {decomposed.metric_node.current.query}")
        components = [c.name for c in decomposed.components]
        if components:
            lines.append(f"    Components:      {', '.join(components)}")
        lines.append("")

    lines += ["Grain Groups", "-" * 60]
    for i, gg in enumerate(result.grain_groups, 1):
        lines.append(f"  Group {i}: {', '.join(gg.metrics)}")
        lines.append(
            f"    Grain:           {', '.join(gg.grain) if gg.grain else 'none'}",
        )
        agg = (
            gg.aggregability.value
            if hasattr(gg.aggregability, "value")
            else str(gg.aggregability)
        )
        lines.append(f"    Aggregability:   {agg}")
        lines.append(f"    Source node:     {gg.parent_name}")
        if gg.components:
            lines.append("    Components:")
            for comp in gg.components:
                lines.append(
                    f"      • {comp.name}: {comp.expression} "
                    f"(agg={comp.aggregation}, merge={comp.merge})",
                )
        lines.append("")
        lines.append("    SQL:")
        lines.append("    " + "-" * 56)
        for sql_line in gg.sql.splitlines():
            lines.append(f"    {sql_line}")
        lines.append("")

    return "\n".join(lines)


async def get_node_lineage(
    node_name: str,
    direction: str = "both",
    max_depth: Optional[int] = None,
) -> str:
    """Return upstream and/or downstream nodes for a given node."""
    session = get_mcp_session()
    depth = max_depth if max_depth is not None else -1

    lines = [f"Lineage for: {node_name}", "=" * 60, ""]

    def _format_section(title: str, nodes: List[Node]) -> List[str]:
        section = [f"{title} ({len(nodes)} nodes):", "-" * 60]
        if not nodes:
            section.append("  (none)")
        else:
            for n in nodes:
                t = n.type.value if hasattr(n.type, "value") else str(n.type)
                status = (
                    n.current.status.value
                    if n.current and hasattr(n.current.status, "value")
                    else "N/A"
                )
                section.append(f"  • {n.name} ({t}) - {status}")
        section.append("")
        return section

    if direction in ("upstream", "both"):
        # ``get_upstream_nodes`` does not support depth — mirrors the existing
        # /nodes/{name}/upstream/ API which also ignores depth.
        upstream = await get_upstream_nodes(session, node_name)
        lines += _format_section("Upstream Dependencies", upstream)

    if direction in ("downstream", "both"):
        downstream = await get_downstream_nodes(session, node_name, depth=depth)
        lines += _format_section("Downstream Dependencies", downstream)

    return "\n".join(lines)


async def get_node_dimensions(node_name: str) -> str:
    """List the dimensions available for a given node."""
    session = get_mcp_session()
    node = await Node.get_by_name(
        session,
        node_name,
        options=[joinedload(Node.current)],
    )
    if not node:
        return f"Node '{node_name}' not found."

    dims = await get_dimensions(session, node, with_attributes=True)

    lines = [
        f"Dimensions for: {node_name}",
        "=" * 60,
        "",
        f"Total: {len(dims)} dimensions",
        "",
        "Available Dimensions:",
        "-" * 60,
    ]
    if not dims:
        lines.append("  (no dimensions available)")
    else:
        for d in dims:
            line = f"  • {getattr(d, 'name', str(d))}"
            t = getattr(d, "type", None)
            if t:
                line += f" ({t})"
            path = getattr(d, "path", None)
            if path:
                line += f" - via: {' → '.join(path)}"
            lines.append(line)
    return "\n".join(lines)


async def visualize_metrics(
    metrics: List[str],
    dimensions: Optional[List[str]] = None,
    filters: Optional[List[str]] = None,
    orderby: Optional[List[str]] = None,
    limit: int = 100,
    chart_type: str = "line",
    title: Optional[str] = None,
    y_min: Optional[float] = None,
) -> List[types.TextContent]:
    """Render a text-based chart of the requested metrics.

    Optimised for terminal / chat MCP clients (Claude Code, Claude Desktop,
    Slack message blocks). For richer surfaces, fetch raw rows via
    ``get_metric_data`` and render client-side.
    """
    try:
        import plotext as plt  # type: ignore[import-untyped]
    except ImportError:  # pragma: no cover
        return [
            types.TextContent(
                type="text",
                text="plotext is not installed on the DJ server.",
            ),
        ]

    try:
        result, _ = await _execute_metrics_query(
            metrics,
            dimensions,
            filters,
            orderby,
            limit,
        )
    except ValueError as exc:
        return [
            types.TextContent(
                type="text",
                text=(
                    f"⚠️  {exc}\n\n"
                    f"Metrics: {', '.join(metrics)}\n"
                    f"Dimensions: {', '.join(dimensions or ['none'])}"
                ),
            ),
        ]

    results_root = getattr(getattr(result, "results", None), "root", None) or []
    if not results_root:
        return [
            types.TextContent(
                type="text",
                text="No results returned from query. Cannot generate visualization.",
            ),
        ]
    first = results_root[0]
    column_names = [c.name for c in (first.columns or [])]
    rows = [dict(zip(column_names, row)) for row in (first.rows or [])]
    if not rows:
        return [
            types.TextContent(
                type="text",
                text="No data rows returned from query. Cannot generate visualization.",
            ),
        ]

    # Pick x-axis: first dimension if provided, else first non-metric column.
    if dimensions:
        x_key = dimensions[0].split(".")[-1]
    else:
        metric_names = [m.split(".")[-1] for m in metrics]
        x_key = next(
            (k for k in rows[0] if k not in metric_names),
            list(rows[0].keys())[0],
        )

    x_values = [row.get(x_key, i) for i, row in enumerate(rows)]
    x_dates: List[Optional[datetime]] = []
    for x in x_values:
        try:
            if isinstance(x, str) and len(x) == 8:
                x_dates.append(datetime.strptime(x, "%Y%m%d"))
            elif isinstance(x, (int, float)) and x > 19000000:
                x_dates.append(datetime.strptime(str(int(x)), "%Y%m%d"))
            else:
                x_dates.append(None)
        except (ValueError, TypeError):
            x_dates.append(None)

    x_numeric: List[float] = []
    x_labels: List[str] = []
    is_categorical = False

    if x_dates and any(d is not None for d in x_dates):
        first_date = next(d for d in x_dates if d is not None)
        for i, d in enumerate(x_dates):
            if d is not None:
                x_numeric.append(float((d - first_date).days))
                x_labels.append(d.strftime("%Y-%m-%d"))
            else:
                x_numeric.append(float(i))
                x_labels.append(str(x_values[i]))
    else:
        try:
            x_numeric = [float(x) for x in x_values]
            x_labels = [str(x) for x in x_values]
        except (ValueError, TypeError):
            is_categorical = True
            x_numeric = list(range(len(x_values)))
            x_labels = [
                str(x)[:15] + ("…" if len(str(x)) > 15 else "") for x in x_values
            ]
            if chart_type == "line":
                chart_type = "bar"

    plt.clear_figure()
    for metric in metrics:
        metric_name = metric.split(".")[-1]
        y_numeric: List[Optional[float]] = []
        for row in rows:
            v = row.get(metric_name)
            if v is None:
                y_numeric.append(None)
            else:
                try:
                    y_numeric.append(float(v))
                except (ValueError, TypeError):
                    y_numeric.append(None)
        if not any(y is not None for y in y_numeric):
            continue
        if chart_type == "line":
            plt.plot(x_numeric, y_numeric, label=metric_name, marker="dot")
        elif chart_type == "bar":
            plt.bar(x_numeric, y_numeric, label=metric_name)
        else:  # scatter
            plt.scatter(x_numeric, y_numeric, label=metric_name, marker="dot")

    plt.title(title or ", ".join(m.split(".")[-1] for m in metrics))
    plt.xlabel(x_key)
    plt.ylabel("Value")
    if y_min is not None:
        plt.ylim(y_min, None)

    if is_categorical:
        plt.xticks(x_numeric, x_labels)
    elif len(x_numeric) > 10 and any(d is not None for d in x_dates):
        step = max(1, len(x_numeric) // 10)
        positions = [x_numeric[i] for i in range(0, len(x_numeric), step)]
        # Bind ``x_dates[i]`` to a local so mypy narrows the Optional[datetime].
        labels: List[str] = []
        for i in range(0, len(x_dates), step):
            d = x_dates[i]
            labels.append(d.strftime("%m/%d") if d is not None else str(x_values[i]))
        plt.xticks(positions, labels)

    # plotext renders the legend automatically when ``label=`` is passed
    # to plot()/bar()/scatter() — no separate ``legend()`` call needed
    # (and indeed it doesn't exist in plotext 5.x).
    plt.plot_size(100, 30)
    chart = plt.build()
    plt.clear_figure()

    return [
        types.TextContent(
            type="text",
            text=(
                f"{chart}\n\n"
                f"📊 {', '.join(m.split('.')[-1] for m in metrics)} | "
                f"{len(rows)} points | {chart_type}"
            ),
        ),
    ]
