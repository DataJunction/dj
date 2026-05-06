"""
Unit tests for ``datajunction_server.mcp.formatters``.

The formatters drive every MCP tool's output — they're the single
biggest determinant of how much context a tool result consumes. These
tests pin down each branch with full-equality assertions so any change
to the formatting shape is intentional and visible in the diff.
"""

from typing import Any, Dict, List

from datajunction_server.mcp.formatters import (
    format_dimensions_compatibility,
    format_node_details,
    format_nodes_list,
)


# ---------------------------------------------------------------------------
# format_nodes_list
# ---------------------------------------------------------------------------


def test_format_nodes_list_empty() -> None:
    assert format_nodes_list([]) == "No nodes found."


def test_format_nodes_list_minimal_node() -> None:
    out = format_nodes_list([{"name": "n.foo", "type": "metric"}])
    assert out == "Found 1 nodes:\n\n• n.foo (metric)\n"


def test_format_nodes_list_full_node() -> None:
    nodes = [
        {
            "name": "n.foo",
            "type": "metric",
            "current": {
                "displayName": "Foo Metric",
                "description": "  Multi-line\n  description body  ",
            },
            "gitInfo": {"repo": "org/repo", "branch": "main"},
            "tags": [{"name": "core"}, {"name": "revenue"}],
        },
    ]
    expected = (
        "Found 1 nodes:\n"
        "\n"
        "• n.foo (metric) [git: org/repo @ main] — Foo Metric\n"
        "  Multi-line description body\n"
        "  tags: core, revenue\n"
    )
    assert format_nodes_list(nodes) == expected


def test_format_nodes_list_long_description_is_trimmed() -> None:
    long_desc = "x" * 200
    nodes = [
        {
            "name": "n.foo",
            "type": "metric",
            "current": {"description": long_desc},
        },
    ]
    out = format_nodes_list(nodes)
    # Description gets trimmed to 160 chars + ellipsis (single char).
    expected_body = "x" * 160 + "…"
    assert out == f"Found 1 nodes:\n\n• n.foo (metric)\n  {expected_body}\n"


def test_format_nodes_list_branch_without_repo() -> None:
    """Git block emits even if only ``branch`` is set; ``repo`` falls back to empty."""
    nodes = [
        {
            "name": "n.foo",
            "type": "metric",
            "gitInfo": {"branch": "feature1"},
        },
    ]
    assert (
        format_nodes_list(nodes)
        == "Found 1 nodes:\n\n• n.foo (metric) [git:  @ feature1]\n"
    )


# ---------------------------------------------------------------------------
# format_node_details
# ---------------------------------------------------------------------------


def test_format_node_details_minimal() -> None:
    out = format_node_details({"name": "n.foo", "type": "metric"})
    assert out == "Node: n.foo\nType: metric\n" + "=" * 60 + "\n"


def test_format_node_details_with_basic_metadata() -> None:
    node = {
        "name": "n.foo",
        "type": "metric",
        "current": {
            "displayName": "Foo",
            "description": "A metric",
            "status": "valid",
            "mode": "published",
        },
    }
    expected = (
        "Node: n.foo\n"
        "Type: metric\n" + "=" * 60 + "\n"
        "\n"
        "Display Name: Foo\n"
        "Description: A metric\n"
        "Status: valid\n"
        "Mode: published\n"
    )
    assert format_node_details(node) == expected


def test_format_node_details_with_tags_owners_created() -> None:
    node = {
        "name": "n.foo",
        "type": "metric",
        "tags": [{"name": "core"}, {"name": "revenue"}],
        "owners": [{"username": "alice"}, {"email": "bob@example.com"}],
        "createdAt": "2024-01-15",
    }
    expected = (
        "Node: n.foo\n"
        "Type: metric\n" + "=" * 60 + "\n"
        "\n"
        "Tags: core, revenue\n"
        "Owners: alice, bob@example.com\n"
        "Created: 2024-01-15\n"
    )
    assert format_node_details(node) == expected


def test_format_node_details_with_git_repo_block() -> None:
    node = {
        "name": "n.foo",
        "type": "metric",
        "gitInfo": {"repo": "org/repo", "branch": "main", "defaultBranch": "main"},
    }
    expected = (
        "Node: n.foo\n"
        "Type: metric\n" + "=" * 60 + "\n"
        "\n"
        "Git Repository:\n"
        "  Repo: org/repo\n"
        "  Branch: main\n"
        "  Default Branch: main\n"
        "  → This namespace is repo-backed (use git workflow for changes)\n"
    )
    assert format_node_details(node) == expected


def test_format_node_details_with_metric_metadata() -> None:
    node = {
        "name": "n.foo",
        "type": "metric",
        "current": {
            "metricMetadata": {
                "direction": "higher_is_better",
                "unit": {"name": "usd", "label": "US Dollars"},
            },
        },
    }
    expected = (
        "Node: n.foo\n"
        "Type: metric\n" + "=" * 60 + "\n"
        "\n"
        "Metric Metadata:\n"
        "  Direction: higher_is_better\n"
        "  Unit: US Dollars\n"
    )
    assert format_node_details(node) == expected


def test_format_node_details_with_metric_metadata_unit_name_fallback() -> None:
    """When ``unit.label`` is missing, fall back to ``unit.name``."""
    node = {
        "name": "n.foo",
        "type": "metric",
        "current": {"metricMetadata": {"unit": {"name": "usd"}}},
    }
    expected = (
        "Node: n.foo\nType: metric\n" + "=" * 60 + "\n\nMetric Metadata:\n  Unit: usd\n"
    )
    assert format_node_details(node) == expected


def test_format_node_details_with_query_definition() -> None:
    node = {
        "name": "n.foo",
        "type": "metric",
        "current": {"query": "SELECT 1"},
    }
    expected = (
        "Node: n.foo\n"
        "Type: metric\n" + "=" * 60 + "\n"
        "\n"
        "SQL Definition:\n"
        "```sql\nSELECT 1\n```\n"
    )
    assert format_node_details(node) == expected


def test_format_node_details_with_dimensions_arg() -> None:
    node = {"name": "n.foo", "type": "metric"}
    dimensions: List[Dict[str, Any]] = [
        {"name": "d.country", "type": "string"},
        {
            "name": "d.region",
            "dimensionNode": {"current": {"description": "geo region"}},
        },
    ]
    expected = (
        "Node: n.foo\n"
        "Type: metric\n" + "=" * 60 + "\n"
        "\n"
        "Available Dimensions (2):\n"
        "  • d.country\n"
        "    Type: string\n"
        "  • d.region\n"
        "    Description: geo region\n"
    )
    assert format_node_details(node, dimensions) == expected


def test_format_node_details_with_columns_truncates_at_ten() -> None:
    cols = [{"name": f"c{i}", "type": "int"} for i in range(13)]
    node = {"name": "n.foo", "type": "metric", "current": {"columns": cols}}
    expected = (
        "Node: n.foo\n"
        "Type: metric\n" + "=" * 60 + "\n"
        "\n"
        "Columns (13):\n"
        "  • c0 (int)\n"
        "  • c1 (int)\n"
        "  • c2 (int)\n"
        "  • c3 (int)\n"
        "  • c4 (int)\n"
        "  • c5 (int)\n"
        "  • c6 (int)\n"
        "  • c7 (int)\n"
        "  • c8 (int)\n"
        "  • c9 (int)\n"
        "  ... and 3 more columns\n"
    )
    assert format_node_details(node) == expected


def test_format_node_details_with_parents_truncates_at_five() -> None:
    parents = [{"name": f"p{i}"} for i in range(8)]
    node = {"name": "n.foo", "type": "metric", "current": {"parents": parents}}
    expected = (
        "Node: n.foo\n"
        "Type: metric\n" + "=" * 60 + "\n"
        "\n"
        "Upstream Dependencies: 8\n"
        "  • p0\n"
        "  • p1\n"
        "  • p2\n"
        "  • p3\n"
        "  • p4\n"
        "  ... and 3 more\n"
    )
    assert format_node_details(node) == expected


# ---------------------------------------------------------------------------
# format_dimensions_compatibility
# ---------------------------------------------------------------------------


def test_format_dimensions_compatibility_no_common_dims() -> None:
    expected = (
        "Dimension Compatibility Analysis\n"
        "Metrics: m1, m2\n" + "=" * 60 + "\n"
        "\n"
        "⚠️ No common dimensions found across these metrics.\n"
        "These metrics cannot be queried together."
    )
    assert format_dimensions_compatibility(["m1", "m2"], []) == expected


def test_format_node_details_with_partial_git_info_branch_only() -> None:
    """Git block fires with only ``branch`` set — exercises the
    ``if repo / if branch / if defaultBranch`` False/True branch combinations.
    """
    node = {
        "name": "n.foo",
        "type": "metric",
        "gitInfo": {"branch": "feature1"},
    }
    expected = (
        "Node: n.foo\n"
        "Type: metric\n" + "=" * 60 + "\n"
        "\n"
        "Git Repository:\n"
        "  Branch: feature1\n"
        "  → This namespace is repo-backed (use git workflow for changes)\n"
    )
    assert format_node_details(node) == expected


def test_format_node_details_with_partial_git_info_default_only() -> None:
    """Git block fires with only ``defaultBranch`` set."""
    node = {
        "name": "n.foo",
        "type": "metric",
        "gitInfo": {"defaultBranch": "main"},
    }
    expected = (
        "Node: n.foo\n"
        "Type: metric\n" + "=" * 60 + "\n"
        "\n"
        "Git Repository:\n"
        "  Default Branch: main\n"
        "  → This namespace is repo-backed (use git workflow for changes)\n"
    )
    assert format_node_details(node) == expected


def test_format_node_details_metric_metadata_direction_only_no_unit() -> None:
    """Direction set, unit absent — exercises the ``if mm.get("unit")`` False branch."""
    node = {
        "name": "n.foo",
        "type": "metric",
        "current": {"metricMetadata": {"direction": "higher_is_better"}},
    }
    expected = (
        "Node: n.foo\n"
        "Type: metric\n" + "=" * 60 + "\n"
        "\n"
        "Metric Metadata:\n"
        "  Direction: higher_is_better\n"
    )
    assert format_node_details(node) == expected


def test_format_node_details_dimension_with_dimension_node_no_description() -> None:
    """A dimension with a ``dimensionNode`` but no description on its
    current revision skips the Description line — covers the false branch
    of the inner ``if dn.get("current", {}).get("description")`` check."""
    node = {"name": "n.foo", "type": "metric"}
    dimensions: List[Dict[str, Any]] = [
        {"name": "d.country", "dimensionNode": {"current": {}}},
    ]
    expected = (
        "Node: n.foo\n"
        "Type: metric\n" + "=" * 60 + "\n"
        "\n"
        "Available Dimensions (1):\n"
        "  • d.country\n"
    )
    assert format_node_details(node, dimensions) == expected


def test_format_node_details_metric_metadata_unit_only_no_direction() -> None:
    """Metric metadata block with unit set but no direction — direction
    branch is False, unit branch is True."""
    node = {
        "name": "n.foo",
        "type": "metric",
        "current": {"metricMetadata": {"unit": {"name": "usd", "label": "USD"}}},
    }
    expected = (
        "Node: n.foo\nType: metric\n" + "=" * 60 + "\n\nMetric Metadata:\n  Unit: USD\n"
    )
    assert format_node_details(node) == expected


def test_format_node_details_dimension_without_dimension_node() -> None:
    """A dimension without a ``dimensionNode`` skips the description line —
    covers the ``if dim.get("dimensionNode")`` False branch in the loop."""
    node = {"name": "n.foo", "type": "metric"}
    dimensions: List[Dict[str, Any]] = [{"name": "d.country"}]
    expected = (
        "Node: n.foo\n"
        "Type: metric\n" + "=" * 60 + "\n"
        "\n"
        "Available Dimensions (1):\n"
        "  • d.country\n"
    )
    assert format_node_details(node, dimensions) == expected


def test_format_dimensions_compatibility_dim_without_dimension_node() -> None:
    """A common dimension without a ``dimensionNode`` is rendered with
    just its name — no Description: line."""
    dims: List[Dict[str, Any]] = [{"name": "d.country"}]
    expected = (
        "Dimension Compatibility Analysis\n"
        "Metrics: m1\n" + "=" * 60 + "\n"
        "\n"
        "✅ Found 1 common dimensions:\n"
        "\n"
        "• d.country\n"
        "\n"
        "\n"
        "You can query these metrics together using the dimensions listed above."
    )
    assert format_dimensions_compatibility(["m1"], dims) == expected


def test_format_dimensions_compatibility_dim_with_dimension_node_no_description() -> (
    None
):
    """A dimension with a ``dimensionNode`` but no description still
    renders just the name. Covers the inner ``if current.get("description")``
    False branch."""
    dims: List[Dict[str, Any]] = [
        {"name": "d.country", "dimensionNode": {"current": {}}},
    ]
    expected = (
        "Dimension Compatibility Analysis\n"
        "Metrics: m1\n" + "=" * 60 + "\n"
        "\n"
        "✅ Found 1 common dimensions:\n"
        "\n"
        "• d.country\n"
        "\n"
        "\n"
        "You can query these metrics together using the dimensions listed above."
    )
    assert format_dimensions_compatibility(["m1"], dims) == expected


def test_format_dimensions_compatibility_with_dims() -> None:
    dims: List[Dict[str, Any]] = [
        {"name": "d.date", "dimensionNode": {"current": {"description": "calendar"}}},
        {"name": "d.country"},
    ]
    expected = (
        "Dimension Compatibility Analysis\n"
        "Metrics: m1\n" + "=" * 60 + "\n"
        "\n"
        "✅ Found 2 common dimensions:\n"
        "\n"
        "• d.date\n"
        "  Description: calendar\n"
        "\n"
        "• d.country\n"
        "\n"
        "\n"
        "You can query these metrics together using the dimensions listed above."
    )
    assert format_dimensions_compatibility(["m1"], dims) == expected
