"""
Formatters to convert GraphQL responses into AI-friendly formats
"""

from typing import Any, Dict, List, Optional


def format_nodes_list(nodes: List[Dict[str, Any]]) -> str:
    """
    Format a list of nodes for the search/discovery flow.

    Optimised for context economy: only fields that help the model
    *decide which node to inspect next*. Status, mode, and owners are
    available via ``get_node_details`` and intentionally omitted here.

    Per node, emits at most three lines:
      • <name> (<type>) [git: <repo> @ <branch>] — <displayName>
        <description (single line, trimmed)>
        tags: <tag1>, <tag2>
    """
    if not nodes:
        return "No nodes found."

    lines = [f"Found {len(nodes)} nodes:", ""]

    for node in nodes:
        current = node.get("current") or {}
        git_info = node.get("gitInfo") or {}
        tags = node.get("tags") or []

        head = f"• {node['name']} ({node['type']})"
        if git_info.get("branch"):
            repo = git_info.get("repo", "")
            head += f" [git: {repo} @ {git_info['branch']}]"
        if current.get("displayName"):
            head += f" — {current['displayName']}"
        lines.append(head)

        description = (current.get("description") or "").strip()
        if description:
            # Single-line, soft-trimmed so a multi-paragraph description
            # doesn't dominate the search response.
            collapsed = " ".join(description.split())
            if len(collapsed) > 160:
                collapsed = collapsed[:160].rstrip() + "…"
            lines.append(f"  {collapsed}")

        if tags:
            lines.append(f"  tags: {', '.join(t['name'] for t in tags)}")

        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def format_node_details(
    node: Dict[str, Any],
    dimensions: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """
    Format detailed node information for AI

    Args:
        node: Node object from GraphQL
        dimensions: Optional list of available dimensions

    Returns:
        Formatted detailed description
    """
    current = node.get("current") or {}
    result = [f"Node: {node['name']}", f"Type: {node['type']}", "=" * 60, ""]

    # Basic info — emit each line only if it has real content. The model
    # already has the name+type from the header; "Status: unknown" /
    # "Mode: unknown" / "Description: No description" filler is pure noise.
    if current.get("displayName"):
        result.append(f"Display Name: {current['displayName']}")
    if current.get("description"):
        result.append(f"Description: {current['description']}")
    if current.get("status"):
        result.append(f"Status: {current['status']}")
    if current.get("mode"):
        result.append(f"Mode: {current['mode']}")
    if result[-1] != "":
        result.append("")

    # Metadata
    if node.get("tags"):
        tag_names = [t["name"] for t in node["tags"]]
        result.append(f"Tags: {', '.join(tag_names)}")

    if node.get("owners"):
        owners = [o.get("username", o.get("email", "unknown")) for o in node["owners"]]
        result.append(f"Owners: {', '.join(owners)}")

    if node.get("createdAt"):
        result.append(f"Created: {node['createdAt']}")
    if result[-1] != "":
        result.append("")

    # Git repository info (indicates if namespace is repo-backed)
    git_info = node.get("gitInfo") or {}
    if any(git_info.get(k) for k in ("repo", "branch", "defaultBranch")):
        result.append("Git Repository:")
        if git_info.get("repo"):
            result.append(f"  Repo: {git_info['repo']}")
        if git_info.get("branch"):
            result.append(f"  Branch: {git_info['branch']}")
        if git_info.get("defaultBranch"):
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
