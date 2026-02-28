"""
Formatters to convert GraphQL responses into AI-friendly formats
"""

from typing import Any, Dict, List


def format_nodes_list(nodes: List[Dict[str, Any]]) -> str:
    """
    Format a list of nodes for AI consumption with trust signals

    Args:
        nodes: List of node objects from GraphQL

    Returns:
        Formatted string representation
    """
    if not nodes:
        return "No nodes found."

    result = [f"Found {len(nodes)} nodes:\n"]

    for node in nodes:
        current = node.get("current", {})
        tags = node.get("tags", [])
        git_info = node.get("gitInfo", {})

        # Format node name with git branch info if available
        node_line = f"• {node['name']} ({node['type']})"
        if git_info and git_info.get("branch"):
            node_line += f" [git: {git_info['repo']} @ {git_info['branch']}]"
        result.append(node_line)

        if current.get("displayName"):
            result.append(f"  Display Name: {current['displayName']}")

        result.append(f"  Description: {current.get('description', 'No description')}")
        result.append(f"  Status: {current.get('status', 'unknown')}")
        result.append(f"  Mode: {current.get('mode', 'unknown')}")

        if tags:
            tag_names = [t["name"] for t in tags]
            result.append(f"  Tags: {', '.join(tag_names)}")

        if node.get("owners"):
            owners = [
                o.get("username", o.get("email", "unknown")) for o in node["owners"]
            ]
            result.append(f"  Owners: {', '.join(owners)}")

        result.append("")  # Blank line between nodes

    return "\n".join(result)


def format_node_details(
    node: Dict[str, Any],
    dimensions: List[Dict[str, Any]] = None,
) -> str:
    """
    Format detailed node information for AI

    Args:
        node: Node object from GraphQL
        dimensions: Optional list of available dimensions

    Returns:
        Formatted detailed description
    """
    current = node.get("current", {})
    result = [f"Node: {node['name']}", f"Type: {node['type']}", f"{'=' * 60}", ""]

    # Basic info
    if current.get("displayName"):
        result.append(f"Display Name: {current['displayName']}")

    result.append(f"Description: {current.get('description', 'No description')}")
    result.append(f"Status: {current.get('status', 'unknown')}")
    result.append(f"Mode: {current.get('mode', 'unknown')}")
    result.append("")

    # Metadata
    if node.get("tags"):
        tag_names = [t["name"] for t in node["tags"]]
        result.append(f"Tags: {', '.join(tag_names)}")

    if node.get("owners"):
        owners = [o.get("username", o.get("email", "unknown")) for o in node["owners"]]
        result.append(f"Owners: {', '.join(owners)}")

    result.append(f"Created: {node.get('createdAt', 'unknown')}")
    result.append("")

    # Git repository info (indicates if namespace is repo-backed)
    if node.get("gitInfo"):
        git_info = node["gitInfo"]
        result.append("Git Repository:")
        if git_info.get("repo"):  # pragma: no branch
            result.append(f"  Repo: {git_info['repo']}")
        if git_info.get("branch"):  # pragma: no branch
            result.append(f"  Branch: {git_info['branch']}")
        if git_info.get("defaultBranch"):  # pragma: no branch
            result.append(f"  Default Branch: {git_info['defaultBranch']}")
        result.append(
            "  → This namespace is repo-backed (use git workflow for changes)",
        )
        result.append("")

    # Metric-specific metadata
    if current.get("metricMetadata"):
        mm = current["metricMetadata"]
        result.append("Metric Metadata:")
        if mm.get("direction"):
            result.append(f"  Direction: {mm['direction']}")
        if mm.get("unit"):
            unit = mm["unit"]
            result.append(f"  Unit: {unit.get('label', unit.get('name', 'unknown'))}")
        result.append("")

    # SQL definition
    if current.get("query"):
        result.append("SQL Definition:")
        result.append(f"```sql\n{current['query']}\n```")
        result.append("")

    # Dimensions
    if dimensions:
        result.append(f"Available Dimensions ({len(dimensions)}):")
        for dim in dimensions:
            dim_name = dim.get("name", "unknown")
            result.append(f"  • {dim_name}")
            if dim.get("type"):
                result.append(f"    Type: {dim['type']}")
            if dim.get("dimensionNode"):
                dn = dim["dimensionNode"]
                if dn.get("current", {}).get("description"):
                    result.append(f"    Description: {dn['current']['description']}")
        result.append("")

    # Columns (if available)
    if current.get("columns"):
        cols = current["columns"]
        result.append(f"Columns ({len(cols)}):")
        for col in cols[:10]:  # Limit to first 10
            result.append(f"  • {col['name']} ({col.get('type', 'unknown')})")
        if len(cols) > 10:
            result.append(f"  ... and {len(cols) - 10} more columns")
        result.append("")

    # Lineage summary
    if node.get("current", {}).get("parents"):
        parents = node["current"]["parents"]
        result.append(f"Upstream Dependencies: {len(parents)}")
        for parent in parents[:5]:
            result.append(f"  • {parent.get('name', 'unknown')}")
        if len(parents) > 5:
            result.append(f"  ... and {len(parents) - 5} more")
        result.append("")

    return "\n".join(result)


def format_dimensions_compatibility(
    metrics: List[str],
    common_dims: List[Dict[str, Any]],
) -> str:
    """
    Format dimension compatibility information

    Args:
        metrics: List of metric names being analyzed
        common_dims: List of common dimension attributes

    Returns:
        Formatted compatibility report
    """
    result = [
        "Dimension Compatibility Analysis",
        f"Metrics: {', '.join(metrics)}",
        f"{'=' * 60}",
        "",
    ]

    if not common_dims:
        result.append("⚠️ No common dimensions found across these metrics.")
        result.append("These metrics cannot be queried together.")
        return "\n".join(result)

    result.append(f"✅ Found {len(common_dims)} common dimensions:\n")

    for dim in common_dims:
        dim_name = dim.get("name", "unknown")
        result.append(f"• {dim_name}")

        if dim.get("dimensionNode"):
            dn = dim["dimensionNode"]
            current = dn.get("current", {})
            if current.get("description"):
                result.append(f"  Description: {current['description']}")

        result.append("")

    result.append(
        "\nYou can query these metrics together using the dimensions listed above.",
    )

    return "\n".join(result)


def format_sql_response(sql_results: List[Dict[str, Any]]) -> str:
    """
    Format SQL generation results

    Args:
        sql_results: List of GeneratedSQL objects from GraphQL

    Returns:
        Formatted SQL with metadata
    """
    if not sql_results:
        return "No SQL generated."

    result = []

    for idx, sql_obj in enumerate(sql_results, 1):
        if len(sql_results) > 1:
            result.append(f"\n{'=' * 60}")
            result.append(f"Query {idx} of {len(sql_results)}")
            result.append(f"{'=' * 60}\n")

        # Node info
        node = sql_obj.get("node", {})
        result.append(f"Source Node: {node.get('name', 'unknown')}")
        result.append(f"Dialect: {sql_obj.get('dialect', 'unknown')}")
        result.append("")

        # SQL
        result.append("Generated SQL:")
        result.append("```sql")
        result.append(sql_obj.get("sql", ""))
        result.append("```")
        result.append("")

        # Columns
        columns = sql_obj.get("columns", [])
        if columns:
            result.append(f"Output Columns ({len(columns)}):")
            for col in columns:
                result.append(
                    f"  • {col.get('name', 'unknown')} ({col.get('type', 'unknown')})",
                )
            result.append("")

        # Upstream tables
        upstream = sql_obj.get("upstreamTables", [])
        if upstream:
            result.append(f"Upstream Tables: {', '.join(upstream)}")
            result.append("")

        # Errors/warnings
        errors = sql_obj.get("errors", [])
        if errors:
            result.append("⚠️ Warnings:")
            for error in errors:
                result.append(f"  • {error.get('message', 'Unknown error')}")
            result.append("")

    return "\n".join(result)


def format_error(error_message: str, context: str = None) -> str:
    """
    Format error message for AI

    Args:
        error_message: The error message
        context: Optional context about what was being attempted

    Returns:
        Formatted error message
    """
    result = ["❌ Error occurred"]

    if context:
        result.append(f"Context: {context}")

    result.append(f"Message: {error_message}")

    return "\n".join(result)
