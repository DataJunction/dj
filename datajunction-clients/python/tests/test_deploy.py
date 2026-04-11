import importlib.metadata
import io
from pathlib import Path
import time
from unittest import mock
import pytest
from unittest.mock import MagicMock, patch
from datajunction.deployment import DeploymentService
from datajunction.exceptions import DJClientException, DJDeploymentFailure
from datajunction.models import DeploymentInfo
from datajunction.rendering import (
    _render_error_bullets,
    _strip_summary_lines,
    print_deployment_header,
    print_results,
)
import yaml
from rich.console import Console


def test_clean_dict_removes_nones_and_empty():
    dirty = {
        "a": None,
        "b": [],
        "c": {},
        "d": {"x": None, "y": {"z": []}, "k": "keep"},
        "e": [1, 2],
    }
    cleaned = DeploymentService.clean_dict(dirty)
    assert cleaned == {"d": {"k": "keep"}, "e": [1, 2]}


def test_filter_node_for_export_removes_columns_without_customizations():
    """Columns without meaningful customizations should be removed."""
    node = {
        "name": "test.node",
        "query": "SELECT * FROM foo",
        "columns": [
            # Should be kept: has custom display_name
            {"name": "user_id", "type": "INT", "display_name": "User ID"},
            # Should be removed: display_name same as name
            {"name": "created_at", "type": "TIMESTAMP", "display_name": "created_at"},
            # Should be kept: has attributes
            {"name": "id", "type": "BIGINT", "attributes": ["primary_key"]},
            # Should be removed: no customizations
            {"name": "plain_col", "type": "VARCHAR"},
            # Should be kept: has description
            {"name": "desc_col", "type": "TEXT", "description": "A useful column"},
        ],
    }

    filtered = DeploymentService.filter_node_for_export(node)

    # Only columns with customizations should remain
    assert len(filtered["columns"]) == 3

    # Type should be excluded from all columns
    for col in filtered["columns"]:
        assert "type" not in col

    # Check correct columns were kept
    col_names = [c["name"] for c in filtered["columns"]]
    assert "user_id" in col_names
    assert "id" in col_names
    assert "desc_col" in col_names
    assert "created_at" not in col_names
    assert "plain_col" not in col_names


def test_filter_node_for_export_removes_columns_key_when_empty():
    """If no columns have customizations, the columns key should be removed."""
    node = {
        "name": "test.node",
        "query": "SELECT * FROM foo",
        "columns": [
            {"name": "a", "type": "INT"},
            {"name": "b", "type": "VARCHAR", "display_name": "b"},  # same as name
        ],
    }

    filtered = DeploymentService.filter_node_for_export(node)

    assert "columns" not in filtered


def test_filter_node_for_export_always_removes_columns_for_cubes():
    """Cube columns should always be removed - they're inferred from metrics/dimensions."""
    node = {
        "name": "test.cube",
        "node_type": "cube",
        "metrics": ["test.metric1", "test.metric2"],
        "dimensions": ["test.dim1"],
        "columns": [
            {"name": "metric1", "type": "BIGINT"},
            {"name": "dim1", "type": "VARCHAR", "display_name": "Dimension 1"},
        ],
    }

    filtered = DeploymentService.filter_node_for_export(node)

    # Columns should be removed regardless of customizations
    assert "columns" not in filtered
    # Other fields should remain
    assert filtered["metrics"] == ["test.metric1", "test.metric2"]
    assert filtered["dimensions"] == ["test.dim1"]


def test_pull_writes_yaml_files(tmp_path):
    # fake client returning a minimal deployment spec
    client = MagicMock()
    client._export_namespace_spec.return_value = {
        "namespace": "foo.bar",
        "nodes": [
            {"name": "foo.bar.baz", "query": "SELECT 1"},
            {"name": "foo.bar.qux", "query": "SELECT 2"},
        ],
    }
    svc = DeploymentService(client)

    svc.pull("foo.bar", tmp_path)

    # project-level yaml
    project_yaml = yaml.safe_load((tmp_path / "dj.yaml").read_text())
    assert project_yaml["namespace"] == "foo.bar"

    # node files
    baz_file = tmp_path / "foo" / "bar" / "baz.yaml"
    assert baz_file.exists()
    assert yaml.safe_load(baz_file.read_text())["query"] == "SELECT 1"

    qux_file = tmp_path / "foo" / "bar" / "qux.yaml"
    assert qux_file.exists()


def test_pull_raises_if_target_not_empty(tmp_path):
    (tmp_path / "something.txt").write_text("not empty")
    client = MagicMock()
    svc = DeploymentService(client)
    with pytest.raises(DJClientException):
        svc.pull("ns", tmp_path)


def test_print_results_success():
    out = io.StringIO()
    print_results(
        "abc-123",
        DeploymentInfo.from_dict(
            {
                "namespace": "some.namespace",
                "status": "success",
                "results": [
                    {
                        "name": "some.random.node",
                        "operation": "create",
                        "status": "success",
                        "message": "ok",
                    },
                ],
            },
        ),
        Console(file=out, no_color=True, width=80),
    )
    assert out.getvalue() == (
        "\n"
        "╭─ abc-123  ·  some.namespace ─────────────────────────────────────────────────╮\n"
        "│   ✓  create    some.random.node                                              │\n"
        "│ ──────────────────────────────────────────────────────────────────────────── │\n"
        "│ ✓ 1 succeeded                                                                │\n"
        "╰──────────────────────────────────────────────────────────────────────────────╯\n"
    )


def test_print_results_hides_noops_by_default():
    out = io.StringIO()
    print_results(
        "uuid-1",
        DeploymentInfo.from_dict(
            {
                "namespace": "ns",
                "status": "success",
                "results": [
                    {"name": "ns.node_a", "operation": "create", "status": "success"},
                    {"name": "ns.node_b", "operation": "noop", "status": "success"},
                ],
            },
        ),
        Console(file=out, no_color=True, width=80),
    )
    assert out.getvalue() == (
        "\n"
        "╭─ uuid-1  ·  ns ──────────────────────────────────────────────────────────────╮\n"
        "│   ✓  create    ns.node_a                                                     │\n"
        "│ ──────────────────────────────────────────────────────────────────────────── │\n"
        "│ ✓ 2 succeeded                                                                │\n"
        "╰──────────────────────────────────────────────────────────────────────────────╯\n"
    )


def test_print_results_shows_noops_when_verbose():
    out = io.StringIO()
    print_results(
        "uuid-1",
        DeploymentInfo.from_dict(
            {
                "namespace": "ns",
                "status": "success",
                "results": [
                    {"name": "ns.node_a", "operation": "create", "status": "success"},
                    {"name": "ns.node_b", "operation": "noop", "status": "success"},
                ],
            },
        ),
        Console(file=out, no_color=True, width=80),
        verbose=True,
    )
    assert out.getvalue() == (
        "\n"
        "╭─ uuid-1  ·  ns ──────────────────────────────────────────────────────────────╮\n"
        "│   ✓  create    ns.node_a                                                     │\n"
        "│   ✓  noop      ns.node_b                                                     │\n"
        "│ ──────────────────────────────────────────────────────────────────────────── │\n"
        "│ ✓ 2 succeeded                                                                │\n"
        "╰──────────────────────────────────────────────────────────────────────────────╯\n"
    )


def test_print_results_summary_includes_skipped_and_noop_counts():
    out = io.StringIO()
    print_results(
        "uuid-2",
        DeploymentInfo.from_dict(
            {
                "namespace": "ns",
                "status": "success",
                "results": [
                    {"name": "ns.a", "operation": "create", "status": "success"},
                    {"name": "ns.b", "operation": "skip", "status": "skipped"},
                    {"name": "ns.c", "operation": "noop", "status": "noop"},
                ],
            },
        ),
        Console(file=out, no_color=True, width=80),
        verbose=False,
    )
    assert out.getvalue() == (
        "\n"
        "╭─ uuid-2  ·  ns ──────────────────────────────────────────────────────────────╮\n"
        "│   ✓  create    ns.a                                                          │\n"
        "│   –  skip      ns.b                                                          │\n"
        "│ ──────────────────────────────────────────────────────────────────────────── │\n"
        "│ ✓ 1 succeeded  – 1 skipped  1 noop                                           │\n"
        "╰──────────────────────────────────────────────────────────────────────────────╯\n"
    )


def test_render_error_bullets_single():
    text = _render_error_bullets("Column `x` does not exist")
    assert "Column" in text.plain
    assert "x" in text.plain


def test_render_error_bullets_multiple_semicolons():
    text = _render_error_bullets(
        "Missing `a`; Invalid type for `b`; Unknown column `c`",
    )
    plain = text.plain
    assert "Missing" in plain
    assert "Invalid type" in plain
    assert "Unknown column" in plain
    # Three bullets means two newlines were inserted between them
    assert plain.count("•") == 3


def test_render_error_bullets_multiline_indents_continuation():
    """Embedded newlines in a bullet are indented so they align under the bullet text."""
    text = _render_error_bullets(
        "First line\nSecond line\nThird line",
    )
    plain = text.plain
    assert plain.count("•") == 1
    assert "First line" in plain
    assert "Second line" in plain
    assert "Third line" in plain


def test_strip_summary_lines_removes_update_headers():
    msg = "Updated transform (v2.0)\n└─ Updated query, dimension_links\n[invalid] join_on error"
    result = _strip_summary_lines(msg)
    assert "Updated transform" not in result
    assert "└─" not in result
    assert "[invalid] join_on error" in result


def test_strip_summary_lines_preserves_non_summary_content():
    msg = "Column `x` does not exist; Invalid type"
    assert _strip_summary_lines(msg) == msg


def test_print_results_invalid_status_shown_as_error():
    """Results with status='invalid' show ✗ and are counted in the summary."""
    out = io.StringIO()
    print_results(
        "uuid-err",
        DeploymentInfo.from_dict(
            {
                "namespace": "ns",
                "status": "success",
                "results": [
                    {
                        "name": "ns.bad_node",
                        "operation": "update",
                        "status": "invalid",
                        "message": "join_on references unknown node",
                    },
                    {
                        "name": "ns.good_node",
                        "operation": "create",
                        "status": "success",
                    },
                ],
            },
        ),
        Console(file=out, no_color=True, width=80),
    )
    assert out.getvalue() == (
        "\n"
        "╭─ uuid-err  ·  ns ────────────────────────────────────────────────────────────╮\n"
        "│   ✗  update    ns.bad_node                                                   │\n"
        "│      • join_on references unknown node                                       │\n"
        "│   ✓  create    ns.good_node                                                  │\n"
        "│ ──────────────────────────────────────────────────────────────────────────── │\n"
        "│ ✓ 1 succeeded  ✗ 1 invalid                                                   │\n"
        "╰──────────────────────────────────────────────────────────────────────────────╯\n"
    )


def test_print_results_unified_summary_combines_direct_and_downstream():
    """Summary shows total invalid count with (N direct, N downstream) breakdown."""
    out = io.StringIO()
    print_results(
        "uuid-combined",
        DeploymentInfo.from_dict(
            {
                "namespace": "ns",
                "status": "success",
                "results": [
                    {
                        "name": "ns.a",
                        "operation": "update",
                        "status": "invalid",
                        "message": "some error",
                    },
                ],
                "downstream_impacts": [
                    {
                        "name": "ns.b",
                        "node_type": "metric",
                        "predicted_status": "invalid",
                        "caused_by": ["ns.a"],
                    },
                ],
            },
        ),
        Console(file=out, no_color=True, width=80),
    )
    assert out.getvalue() == (
        "\n"
        "╭─ uuid-combined  ·  ns ───────────────────────────────────────────────────────╮\n"
        "│   ✗  update    ns.a                                                          │\n"
        "│      • some error                                                            │\n"
        "│ ──────────────────────────────────────────────────────────────────────────── │\n"
        "│   Downstream Impacts                                                         │\n"
        "│   from a                                                                     │\n"
        "│   └ ✗ metric b  → invalid                                                    │\n"
        "│ ──────────────────────────────────────────────────────────────────────────── │\n"
        "│ ✗ 2 invalid  (1 direct, 1 downstream)                                        │\n"
        "╰──────────────────────────────────────────────────────────────────────────────╯\n"
    )


def test_print_results_downstream_only_invalid_no_breakdown():
    """When only downstream nodes are invalid (no direct errors), no breakdown parenthetical."""
    out = io.StringIO()
    print_results(
        "uuid-downstream-only",
        DeploymentInfo.from_dict(
            {
                "namespace": "ns",
                "status": "success",
                "results": [
                    {"name": "ns.a", "operation": "update", "status": "success"},
                ],
                "downstream_impacts": [
                    {
                        "name": "ns.b",
                        "node_type": "metric",
                        "predicted_status": "invalid",
                        "caused_by": ["ns.a"],
                    },
                ],
            },
        ),
        Console(file=out, no_color=True, width=80),
    )
    assert out.getvalue() == (
        "\n"
        "╭─ uuid-downstream-only  ·  ns ────────────────────────────────────────────────╮\n"
        "│   ✓  update    ns.a                                                          │\n"
        "│ ──────────────────────────────────────────────────────────────────────────── │\n"
        "│   Downstream Impacts                                                         │\n"
        "│   from a                                                                     │\n"
        "│   └ ✗ metric b  → invalid                                                    │\n"
        "│ ──────────────────────────────────────────────────────────────────────────── │\n"
        "│ ✓ 1 succeeded  ✗ 1 invalid  (downstream)                                     │\n"
        "╰──────────────────────────────────────────────────────────────────────────────╯\n"
    )


def test_print_results_dimension_links_grouped_under_parent():
    """Dimension link results (name contains ' -> ') nest under their parent node row."""
    out = io.StringIO()
    print_results(
        "uuid-dimlink",
        DeploymentInfo.from_dict(
            {
                "namespace": "ns",
                "status": "failed",
                "results": [
                    {
                        "name": "ns.my_transform",
                        "operation": "update",
                        "status": "invalid",
                        "message": "join_on error",
                        "changed_fields": ["dimension_links"],
                    },
                    {
                        "name": "ns.my_transform -> ns.dim_date",
                        "operation": "create",
                        "status": "failed",
                        "message": "node does not exist",
                    },
                ],
            },
        ),
        Console(file=out, no_color=True, width=80),
    )
    assert out.getvalue() == (
        "\n"
        "╭─ uuid-dimlink  ·  ns ────────────────────────────────────────────────────────╮\n"
        "│   ✗  update    ns.my_transform  [dimension_links]                            │\n"
        "│      • join_on error                                                         │\n"
        "│      dimension links                                                         │\n"
        "│      └─ ✗  create    → ns.dim_date                                           │\n"
        "│         • node does not exist                                                │\n"
        "│ ──────────────────────────────────────────────────────────────────────────── │\n"
        "│ ✗ 2 invalid                                                                  │\n"
        "╰──────────────────────────────────────────────────────────────────────────────╯\n"
    )


def test_print_results_dim_link_child_shows_changed_fields():
    """Dim link child with changed_fields displays the field list (line 165)."""
    out = io.StringIO()
    print_results(
        "uuid-dl-cf",
        DeploymentInfo.from_dict(
            {
                "namespace": "ns",
                "status": "success",
                "results": [
                    {"name": "ns.parent", "operation": "update", "status": "success"},
                    {
                        "name": "ns.parent -> ns.dim",
                        "operation": "update",
                        "status": "noop",
                        "changed_fields": ["query"],
                    },
                ],
            },
        ),
        Console(file=out, no_color=True, width=80),
        verbose=True,
    )
    assert out.getvalue() == (
        "\n"
        "╭─ uuid-dl-cf  ·  ns ──────────────────────────────────────────────────────────╮\n"
        "│   ✓  update    ns.parent                                                     │\n"
        "│      dimension links                                                         │\n"
        "│      └─ ·  update    → ns.dim  [query]                                       │\n"
        "│ ──────────────────────────────────────────────────────────────────────────── │\n"
        "│ ✓ 1 succeeded                                                                │\n"
        "╰──────────────────────────────────────────────────────────────────────────────╯\n"
    )


def test_print_results_error_message_fully_stripped():
    """When _strip_summary_lines removes all content, no bullet block is rendered (line 195->198)."""
    out = io.StringIO()
    print_results(
        "uuid-stripped",
        DeploymentInfo.from_dict(
            {
                "namespace": "ns",
                "status": "failed",
                "results": [
                    {
                        "name": "ns.node",
                        "operation": "update",
                        "status": "invalid",
                        "message": "Updated transform (v2.0)\n└─ Updated query, dimension_links",
                    },
                ],
            },
        ),
        Console(file=out, no_color=True, width=80),
    )
    assert out.getvalue() == (
        "\n"
        "╭─ uuid-stripped  ·  ns ───────────────────────────────────────────────────────╮\n"
        "│   ✗  update    ns.node                                                       │\n"
        "│ ──────────────────────────────────────────────────────────────────────────── │\n"
        "│ ✗ 1 invalid                                                                  │\n"
        "╰──────────────────────────────────────────────────────────────────────────────╯\n"
    )


def test_print_results_downstream_tree_with_cube_and_recursion():
    """
    Covers:
      - lines 210-211: cube annotation in _impact_annotation (via: a, b)
      - line 247: cycle detection in _collect_transitive_cubes
      - line 250: cube collected in _collect_transitive_cubes
      - line 284: recursive _collect_impact_lines (b → c)
      - line 330: root ns.b has empty items after ns.a rendered everything
    """
    out = io.StringIO()
    print_results(
        "uuid-tree",
        DeploymentInfo.from_dict(
            {
                "namespace": "ns",
                "status": "success",
                "results": [
                    {"name": "ns.a", "operation": "update", "status": "success"},
                ],
                "downstream_impacts": [
                    {
                        "name": "ns.b",
                        "node_type": "transform",
                        "predicted_status": "invalid",
                        "caused_by": ["ns.a"],
                    },
                    {
                        "name": "ns.c",
                        "node_type": "transform",
                        "predicted_status": "invalid",
                        "caused_by": ["ns.b"],
                    },
                    {
                        "name": "ns.cube1",
                        "node_type": "cube",
                        "predicted_status": "invalid",
                        "caused_by": ["ns.a", "ns.b"],
                    },
                ],
            },
        ),
        Console(file=out, no_color=True, width=80),
    )
    assert out.getvalue() == (
        "\n"
        "╭─ uuid-tree  ·  ns ───────────────────────────────────────────────────────────╮\n"
        "│   ✓  update    ns.a                                                          │\n"
        "│ ──────────────────────────────────────────────────────────────────────────── │\n"
        "│   Downstream Impacts                                                         │\n"
        "│   from a                                                                     │\n"
        "│   ├ ✗ transform b  → invalid                                                 │\n"
        "│   │  └ ✗ transform c  → invalid                                              │\n"
        "│   └ ✗ cube cube1  → invalid  (via: a, b)                                     │\n"
        "│ ──────────────────────────────────────────────────────────────────────────── │\n"
        "│ ✓ 1 succeeded  ✗ 3 invalid  (downstream)                                     │\n"
        "╰──────────────────────────────────────────────────────────────────────────────╯\n"
    )


def test_print_results_downstream_impact_with_no_caused_by_hides_tree():
    """Invalidated downstream node with empty caused_by produces no tree section (line 348)."""
    out = io.StringIO()
    print_results(
        "uuid-nocause",
        DeploymentInfo.from_dict(
            {
                "namespace": "ns",
                "status": "success",
                "results": [
                    {"name": "ns.a", "operation": "update", "status": "success"},
                ],
                "downstream_impacts": [
                    {
                        "name": "ns.b",
                        "node_type": "metric",
                        "predicted_status": "invalid",
                        "caused_by": [],
                    },
                ],
            },
        ),
        Console(file=out, no_color=True, width=80),
    )
    assert out.getvalue() == (
        "\n"
        "╭─ uuid-nocause  ·  ns ────────────────────────────────────────────────────────╮\n"
        "│   ✓  update    ns.a                                                          │\n"
        "│ ──────────────────────────────────────────────────────────────────────────── │\n"
        "│ ✓ 1 succeeded  ✗ 1 invalid  (downstream)                                     │\n"
        "╰──────────────────────────────────────────────────────────────────────────────╯\n"
    )


def test_print_deployment_header_with_client_version():
    """Header includes client version row when package metadata is available (lines 414-415)."""
    out = io.StringIO()
    with patch("importlib.metadata.version", return_value="1.2.3"):
        print_deployment_header(
            "push",
            "ads.moon",
            Console(file=out, no_color=True, width=80),
            repo="https://git.example.com/org/repo.git",
            branch="main",
        )
    assert out.getvalue() == (
        "\n"
        "───────────────────────────────────── push ─────────────────────────────────────\n"
        "  namespace  ads.moon\n"
        "  repo       https://git.example.com/org/repo.git\n"
        "  branch     main\n"
        "  client     datajunction 1.2.3\n"
        "────────────────────────────────────────────────────────────────────────────────\n"
    )


def test_print_deployment_header_without_client_version():
    """Header omits client row when package metadata is unavailable (line 422->425)."""
    out = io.StringIO()
    with patch(
        "importlib.metadata.version",
        side_effect=importlib.metadata.PackageNotFoundError,
    ):
        print_deployment_header(
            "dry run",
            "ads.moon",
            Console(file=out, no_color=True, width=80),
            repo="https://git.example.com/org/repo.git",
            branch="main",
        )
    assert out.getvalue() == (
        "\n"
        "─────────────────────────────────── dry run ────────────────────────────────────\n"
        "  namespace  ads.moon\n"
        "  repo       https://git.example.com/org/repo.git\n"
        "  branch     main\n"
        "────────────────────────────────────────────────────────────────────────────────\n"
    )


def test_reconstruct_deployment_spec(tmp_path):
    # set up a fake exported project
    (tmp_path / "dj.yaml").write_text(
        yaml.safe_dump({"namespace": "foo", "tags": ["t1"]}),
    )
    node_dir = tmp_path / "foo"
    node_dir.mkdir()
    node_file = node_dir / "bar.yaml"
    node_file.write_text(yaml.safe_dump({"name": "foo.bar", "query": "SELECT 1"}))

    svc = DeploymentService(MagicMock())
    spec, warnings = svc._reconstruct_deployment_spec(tmp_path)
    assert spec["namespace"] == "foo"
    assert spec["tags"] == ["t1"]
    assert spec["nodes"][0]["name"] == "foo.bar"


@pytest.mark.timeout(2)
def test_push_waits_until_success(monkeypatch, tmp_path):
    # Create a fake project structure so _reconstruct_deployment_spec returns something
    (tmp_path / "dj.yaml").write_text(yaml.safe_dump({"namespace": "foo"}))
    (tmp_path / "bar.yaml").write_text(yaml.safe_dump({"name": "foo.bar"}))

    # Fake client that returns "pending" once then "success"
    client = MagicMock()
    responses = [
        {"uuid": "123", "status": "pending", "results": [], "namespace": "foo"},
        {"uuid": "123", "status": "success", "results": [], "namespace": "foo"},
    ]
    client.deploy.return_value = responses[0]
    client.check_deployment.side_effect = responses[1:]

    svc = DeploymentService(client, console=Console(file=io.StringIO()))
    monkeypatch.setattr(time, "sleep", lambda _: None)

    svc.push(tmp_path)  # should not raise

    client.deploy.assert_called_once()
    client.check_deployment.assert_called()


def test_push_force_sets_flag_in_spec(monkeypatch, tmp_path):
    """push(force=True) must include {"force": True} in the spec sent to client.deploy."""
    (tmp_path / "dj.yaml").write_text(yaml.safe_dump({"namespace": "foo"}))
    (tmp_path / "bar.yaml").write_text(yaml.safe_dump({"name": "foo.bar"}))

    client = MagicMock()
    client.deploy.return_value = {
        "uuid": "abc",
        "status": "success",
        "results": [],
        "namespace": "foo",
    }

    svc = DeploymentService(client, console=Console(file=io.StringIO()))
    monkeypatch.setattr(time, "sleep", lambda _: None)

    svc.push(tmp_path, force=True)

    spec_sent = client.deploy.call_args[0][0]
    assert spec_sent.get("force") is True


def test_push_without_force_does_not_set_flag(monkeypatch, tmp_path):
    """push() without force must not include force in the spec (server default handles it)."""
    (tmp_path / "dj.yaml").write_text(yaml.safe_dump({"namespace": "foo"}))
    (tmp_path / "bar.yaml").write_text(yaml.safe_dump({"name": "foo.bar"}))

    client = MagicMock()
    client.deploy.return_value = {
        "uuid": "abc",
        "status": "success",
        "results": [],
        "namespace": "foo",
    }

    svc = DeploymentService(client, console=Console(file=io.StringIO()))
    monkeypatch.setattr(time, "sleep", lambda _: None)

    svc.push(tmp_path)

    spec_sent = client.deploy.call_args[0][0]
    assert "force" not in spec_sent


def test_push_times_out(monkeypatch, tmp_path):
    # minimal project structure so _reconstruct_deployment_spec works
    (tmp_path / "dj.yaml").write_text(yaml.safe_dump({"namespace": "foo"}))
    (tmp_path / "bar.yaml").write_text(yaml.safe_dump({"name": "foo.bar"}))

    # Fake client: deploy returns a uuid, check_deployment always 'pending'
    client = MagicMock()
    client.deploy.return_value = {
        "uuid": "123",
        "status": "pending",
        "results": [],
        "namespace": "foo",
    }
    client.check_deployment.return_value = {
        "uuid": "123",
        "status": "pending",
        "results": [],
        "namespace": "foo",
    }

    svc = DeploymentService(client, console=Console(file=io.StringIO()))

    # Patch time.sleep to skip waiting
    monkeypatch.setattr(time, "sleep", lambda _: None)

    # Simulate time moving past the timeout on the second call
    start_time = 1_000_000
    times = [start_time, start_time + 301]  # second call is > 5 minutes later
    monkeypatch.setattr(time, "time", lambda: times.pop(0))

    with pytest.raises(DJClientException, match="Deployment timed out"):
        svc.push(tmp_path)

    client.deploy.assert_called_once()
    client.check_deployment.assert_called()


@pytest.mark.timeout(2)
def test_push_raises_on_failed_deployment(monkeypatch, tmp_path):
    (tmp_path / "dj.yaml").write_text(yaml.safe_dump({"namespace": "foo"}))
    (tmp_path / "bar.yaml").write_text(yaml.safe_dump({"name": "foo.bar"}))

    failed_results = [
        {
            "deploy_type": "node",
            "name": "foo.bar",
            "operation": "create",
            "status": "failed",
            "message": "Column `x` does not exist",
        },
    ]
    client = MagicMock()
    client.deploy.return_value = {
        "uuid": "456",
        "status": "failed",
        "results": failed_results,
        "namespace": "foo",
    }

    svc = DeploymentService(client, console=Console(file=io.StringIO()))
    monkeypatch.setattr(time, "sleep", lambda _: None)

    with pytest.raises(DJDeploymentFailure) as exc_info:
        svc.push(tmp_path)

    assert "foo" in exc_info.value.message
    assert len(exc_info.value.errors) == 1
    assert exc_info.value.errors[0]["name"] == "foo.bar"


@pytest.mark.timeout(2)
def test_push_raises_after_polling_to_failure(monkeypatch, tmp_path):
    (tmp_path / "dj.yaml").write_text(yaml.safe_dump({"namespace": "ns"}))
    (tmp_path / "ns.yaml").write_text(yaml.safe_dump({"name": "ns.node"}))

    client = MagicMock()
    client.deploy.return_value = {
        "uuid": "789",
        "status": "pending",
        "results": [],
        "namespace": "ns",
    }
    client.check_deployment.side_effect = [
        {"uuid": "789", "status": "pending", "results": [], "namespace": "ns"},
        {
            "uuid": "789",
            "status": "failed",
            "results": [
                {
                    "deploy_type": "general",
                    "name": "DJInvalidDeploymentConfig",
                    "operation": "unknown",
                    "status": "failed",
                    "message": "Missing dependencies: source.foo",
                },
            ],
            "namespace": "ns",
        },
    ]

    svc = DeploymentService(client, console=Console(file=io.StringIO()))
    monkeypatch.setattr(time, "sleep", lambda _: None)

    with pytest.raises(DJDeploymentFailure) as exc_info:
        svc.push(tmp_path)

    assert exc_info.value.errors[0]["message"] == "Missing dependencies: source.foo"
    assert client.check_deployment.call_count == 2


def test_read_project_yaml_returns_empty(tmp_path: Path):
    """
    Verify that when dj.yaml is missing, _read_project_yaml returns an empty dict.
    """
    # Create a directory without dj.yaml
    project_dir = tmp_path
    client = mock.MagicMock()
    svc = DeploymentService(client, console=Console(file=io.StringIO()))
    result = svc._read_project_yaml(project_dir)
    assert result == {}


class TestBuildDeploymentSource:
    """Tests for _build_deployment_source method."""

    def test_no_git_env_vars_returns_local_source(self, monkeypatch):
        """When no DJ_DEPLOY_REPO is set, returns local source with hostname."""
        # Ensure none of the git-related env vars are set
        monkeypatch.delenv("DJ_DEPLOY_REPO", raising=False)
        monkeypatch.delenv("DJ_DEPLOY_BRANCH", raising=False)
        monkeypatch.delenv("DJ_DEPLOY_COMMIT", raising=False)
        monkeypatch.delenv("DJ_DEPLOY_CI_SYSTEM", raising=False)
        monkeypatch.delenv("DJ_DEPLOY_CI_RUN_URL", raising=False)
        monkeypatch.setattr(
            DeploymentService,
            "_detect_git_repo",
            staticmethod(lambda cwd=None: None),
        )
        monkeypatch.setattr(
            DeploymentService,
            "_detect_git_branch",
            staticmethod(lambda cwd=None: None),
        )

        result = DeploymentService._build_deployment_source()

        # Now we always track local deploys
        assert result is not None
        assert result["type"] == "local"
        assert "hostname" in result

    def test_git_source_with_all_fields(self, monkeypatch):
        """When DJ_DEPLOY_REPO is set, returns git source with all fields."""
        monkeypatch.setenv("DJ_DEPLOY_REPO", "github.com/org/repo")
        monkeypatch.setenv("DJ_DEPLOY_BRANCH", "main")
        monkeypatch.setenv("DJ_DEPLOY_COMMIT", "abc123")
        monkeypatch.setenv("DJ_DEPLOY_CI_SYSTEM", "github-actions")
        monkeypatch.setenv(
            "DJ_DEPLOY_CI_RUN_URL",
            "https://github.com/org/repo/actions/runs/123",
        )

        result = DeploymentService._build_deployment_source()

        assert result is not None
        assert result["type"] == "git"
        assert result["repository"] == "github.com/org/repo"
        assert result["branch"] == "main"
        assert result["commit_sha"] == "abc123"
        assert result["ci_system"] == "github-actions"
        assert result["ci_run_url"] == "https://github.com/org/repo/actions/runs/123"

    def test_git_source_with_only_repo(self, monkeypatch):
        """When only DJ_DEPLOY_REPO is set, other fields are omitted."""
        monkeypatch.setenv("DJ_DEPLOY_REPO", "github.com/org/repo")
        monkeypatch.delenv("DJ_DEPLOY_BRANCH", raising=False)
        monkeypatch.delenv("DJ_DEPLOY_COMMIT", raising=False)
        monkeypatch.delenv("DJ_DEPLOY_CI_SYSTEM", raising=False)
        monkeypatch.delenv("DJ_DEPLOY_CI_RUN_URL", raising=False)
        import subprocess as sp

        monkeypatch.setattr(
            "datajunction.deployment.subprocess.run",
            mock.Mock(side_effect=sp.CalledProcessError(128, "git")),
        )

        result = DeploymentService._build_deployment_source()

        assert result is not None
        assert result["type"] == "git"
        assert result["repository"] == "github.com/org/repo"
        assert "branch" not in result
        assert "commit_sha" not in result
        assert "ci_system" not in result
        assert "ci_run_url" not in result

    def test_local_source_when_track_local_true(self, monkeypatch):
        """When DJ_DEPLOY_TRACK_LOCAL=true, returns local source."""
        monkeypatch.delenv("DJ_DEPLOY_REPO", raising=False)
        monkeypatch.setenv("DJ_DEPLOY_TRACK_LOCAL", "true")
        monkeypatch.setenv("DJ_DEPLOY_REASON", "testing locally")
        monkeypatch.setattr(
            DeploymentService,
            "_detect_git_repo",
            staticmethod(lambda cwd=None: None),
        )
        monkeypatch.setattr(
            DeploymentService,
            "_detect_git_branch",
            staticmethod(lambda cwd=None: None),
        )

        result = DeploymentService._build_deployment_source()

        assert result is not None
        assert result["type"] == "local"
        assert "hostname" in result  # should be set to socket.gethostname()
        assert result["reason"] == "testing locally"

    def test_local_source_track_local_case_insensitive(self, monkeypatch):
        """DJ_DEPLOY_TRACK_LOCAL should be case-insensitive."""
        monkeypatch.delenv("DJ_DEPLOY_REPO", raising=False)
        monkeypatch.setenv("DJ_DEPLOY_TRACK_LOCAL", "TRUE")
        monkeypatch.delenv("DJ_DEPLOY_REASON", raising=False)
        monkeypatch.setattr(
            DeploymentService,
            "_detect_git_repo",
            staticmethod(lambda cwd=None: None),
        )
        monkeypatch.setattr(
            DeploymentService,
            "_detect_git_branch",
            staticmethod(lambda cwd=None: None),
        )

        result = DeploymentService._build_deployment_source()

        assert result is not None
        assert result["type"] == "local"

    def test_git_takes_precedence_over_local(self, monkeypatch):
        """When both DJ_DEPLOY_REPO and DJ_DEPLOY_TRACK_LOCAL are set, git wins."""
        monkeypatch.setenv("DJ_DEPLOY_REPO", "github.com/org/repo")
        monkeypatch.setenv("DJ_DEPLOY_TRACK_LOCAL", "true")

        result = DeploymentService._build_deployment_source()

        assert result is not None
        assert result["type"] == "git"

    def test_reconstruct_deployment_spec_includes_source(self, tmp_path, monkeypatch):
        """_reconstruct_deployment_spec should include source when env vars are set."""
        # Set up project files
        (tmp_path / "dj.yaml").write_text(yaml.safe_dump({"namespace": "test"}))
        (tmp_path / "node.yaml").write_text(yaml.safe_dump({"name": "test.node"}))

        # Set env vars for git source
        monkeypatch.setenv("DJ_DEPLOY_REPO", "github.com/test/repo")
        monkeypatch.setenv("DJ_DEPLOY_BRANCH", "feature-branch")

        svc = DeploymentService(MagicMock())
        spec, warnings = svc._reconstruct_deployment_spec(tmp_path)

        assert "source" in spec
        assert spec["source"]["type"] == "git"
        assert spec["source"]["repository"] == "github.com/test/repo"
        assert spec["source"]["branch"] == "feature-branch"

    def test_reconstruct_deployment_spec_local_source_without_repo_env_var(
        self,
        tmp_path,
        monkeypatch,
    ):
        """_reconstruct_deployment_spec should include local source when no repo env var."""
        # Set up project files
        (tmp_path / "dj.yaml").write_text(yaml.safe_dump({"namespace": "test"}))
        (tmp_path / "node.yaml").write_text(yaml.safe_dump({"name": "test.node"}))

        # Ensure no git env vars are set
        monkeypatch.delenv("DJ_DEPLOY_REPO", raising=False)

        svc = DeploymentService(MagicMock())
        spec, warnings = svc._reconstruct_deployment_spec(tmp_path)

        # Now we always track local deploys
        assert "source" in spec
        assert spec["source"]["type"] == "local"
        assert "hostname" in spec["source"]

    def test_git_source_includes_author_from_git_log(self, monkeypatch):
        """Commit author is auto-detected from git log when no env vars are set."""
        monkeypatch.setenv("DJ_DEPLOY_REPO", "github.com/org/repo")
        monkeypatch.delenv("DJ_DEPLOY_AUTHOR_EMAIL", raising=False)
        monkeypatch.delenv("DJ_DEPLOY_AUTHOR_NAME", raising=False)
        monkeypatch.setattr(
            DeploymentService,
            "_detect_git_commit_author",
            staticmethod(lambda cwd=None: ("alice@example.com", "Alice Smith")),
        )
        monkeypatch.setattr(
            DeploymentService,
            "_detect_git_branch",
            staticmethod(lambda cwd=None: None),
        )

        result = DeploymentService._build_deployment_source()

        assert result["type"] == "git"
        assert result["commit_author_email"] == "alice@example.com"
        assert result["commit_author_name"] == "Alice Smith"

    def test_git_source_author_env_vars_take_precedence(self, monkeypatch):
        """DJ_DEPLOY_AUTHOR_EMAIL/NAME override git log author."""
        monkeypatch.setenv("DJ_DEPLOY_REPO", "github.com/org/repo")
        monkeypatch.setenv("DJ_DEPLOY_AUTHOR_EMAIL", "ci-override@example.com")
        monkeypatch.setenv("DJ_DEPLOY_AUTHOR_NAME", "CI Override")
        monkeypatch.setattr(
            DeploymentService,
            "_detect_git_commit_author",
            staticmethod(lambda cwd=None: ("git-author@example.com", "Git Author")),
        )
        monkeypatch.setattr(
            DeploymentService,
            "_detect_git_branch",
            staticmethod(lambda cwd=None: None),
        )

        result = DeploymentService._build_deployment_source()

        assert result["commit_author_email"] == "ci-override@example.com"
        assert result["commit_author_name"] == "CI Override"

    def test_git_source_omits_author_when_unavailable(self, monkeypatch):
        """Author fields are omitted when git log returns nothing."""
        monkeypatch.setenv("DJ_DEPLOY_REPO", "github.com/org/repo")
        monkeypatch.delenv("DJ_DEPLOY_AUTHOR_EMAIL", raising=False)
        monkeypatch.delenv("DJ_DEPLOY_AUTHOR_NAME", raising=False)
        monkeypatch.setattr(
            DeploymentService,
            "_detect_git_commit_author",
            staticmethod(lambda cwd=None: (None, None)),
        )
        monkeypatch.setattr(
            DeploymentService,
            "_detect_git_branch",
            staticmethod(lambda cwd=None: None),
        )

        result = DeploymentService._build_deployment_source()

        assert "commit_author_email" not in result
        assert "commit_author_name" not in result


class TestGetImpact:
    """Tests for the get_impact method."""

    def test_get_impact_calls_api(self, tmp_path, monkeypatch):
        """get_impact should call the deployment impact API."""
        # Set up project files
        (tmp_path / "dj.yaml").write_text(yaml.safe_dump({"namespace": "test.ns"}))
        (tmp_path / "node.yaml").write_text(
            yaml.safe_dump({"name": "test.ns.my_node", "node_type": "source"}),
        )

        # Ensure no git env vars are set for cleaner test
        monkeypatch.delenv("DJ_DEPLOY_REPO", raising=False)

        # Create mock client
        mock_client = MagicMock()
        mock_client.get_deployment_impact.return_value = {
            "uuid": "dry_run",
            "namespace": "test.ns",
            "status": "success",
            "results": [
                {
                    "name": "test.ns.my_node",
                    "operation": "noop",
                    "status": "success",
                    "message": "",
                },
            ],
            "downstream_impacts": [],
        }

        svc = DeploymentService(mock_client)
        result = svc.get_impact(tmp_path)

        # Verify the API was called
        mock_client.get_deployment_impact.assert_called_once()
        call_args = mock_client.get_deployment_impact.call_args[0][0]
        assert call_args["namespace"] == "test.ns"
        assert "nodes" in call_args

        # Verify the result is returned
        assert result["namespace"] == "test.ns"
        assert result["uuid"] == "dry_run"

    def test_get_impact_with_namespace_override(self, tmp_path, monkeypatch):
        """get_impact should respect namespace override."""
        # Set up project files
        (tmp_path / "dj.yaml").write_text(yaml.safe_dump({"namespace": "original.ns"}))
        (tmp_path / "node.yaml").write_text(yaml.safe_dump({"name": "test.node"}))

        monkeypatch.delenv("DJ_DEPLOY_REPO", raising=False)

        mock_client = MagicMock()
        mock_client.get_deployment_impact.return_value = {"namespace": "override.ns"}

        svc = DeploymentService(mock_client)
        svc.get_impact(tmp_path, namespace="override.ns")

        # Verify the namespace was overridden in the API call
        call_args = mock_client.get_deployment_impact.call_args[0][0]
        assert call_args["namespace"] == "override.ns"


class TestDetectGitBranch:
    """Tests for _detect_git_branch."""

    def test_returns_current_branch(self, monkeypatch):
        monkeypatch.setattr(
            "datajunction.deployment.subprocess.run",
            lambda *a, **kw: mock.MagicMock(stdout="feature/my-metric\n"),
        )
        assert DeploymentService._detect_git_branch() == "feature/my-metric"

    def test_returns_none_when_not_in_git_repo(self, monkeypatch):
        import subprocess as sp

        monkeypatch.setattr(
            "datajunction.deployment.subprocess.run",
            mock.Mock(side_effect=sp.CalledProcessError(128, "git")),
        )
        assert DeploymentService._detect_git_branch() is None

    def test_returns_none_when_git_not_installed(self, monkeypatch):
        monkeypatch.setattr(
            "datajunction.deployment.subprocess.run",
            mock.Mock(side_effect=FileNotFoundError),
        )
        assert DeploymentService._detect_git_branch() is None

    @pytest.mark.parametrize(
        "remote_output,expected",
        [
            ("  origin/my-feature\n", "my-feature"),
            ("  upstream/my-feature\n", "my-feature"),
            ("  fork/my-feature\n", "my-feature"),
        ],
    )
    def test_detached_head_falls_back_to_remote_ref(
        self,
        monkeypatch,
        remote_output,
        expected,
    ):
        calls = iter(
            [
                mock.MagicMock(stdout="HEAD\n"),  # git rev-parse --abbrev-ref HEAD
                mock.MagicMock(stdout=remote_output),  # git branch -r --points-at HEAD
            ],
        )
        monkeypatch.setattr(
            "datajunction.deployment.subprocess.run",
            lambda *a, **kw: next(calls),
        )
        assert DeploymentService._detect_git_branch() == expected

    def test_detached_head_returns_none_when_no_remote_ref(self, monkeypatch):
        calls = iter(
            [
                mock.MagicMock(stdout="HEAD\n"),  # git rev-parse --abbrev-ref HEAD
                mock.MagicMock(
                    stdout="",
                ),  # git branch -r --points-at HEAD — no results
            ],
        )
        monkeypatch.setattr(
            "datajunction.deployment.subprocess.run",
            lambda *a, **kw: next(calls),
        )
        assert DeploymentService._detect_git_branch() is None


class TestDetectGitCommitAuthor:
    """Tests for _detect_git_commit_author."""

    def test_returns_email_and_name(self, monkeypatch):
        monkeypatch.setattr(
            "datajunction.deployment.subprocess.run",
            lambda *a, **kw: mock.MagicMock(stdout="alice@example.com|Alice Smith\n"),
        )
        email, name = DeploymentService._detect_git_commit_author()
        assert email == "alice@example.com"
        assert name == "Alice Smith"

    def test_returns_none_when_not_in_git_repo(self, monkeypatch):
        import subprocess as sp

        monkeypatch.setattr(
            "datajunction.deployment.subprocess.run",
            mock.Mock(side_effect=sp.CalledProcessError(128, "git")),
        )
        assert DeploymentService._detect_git_commit_author() == (None, None)

    def test_returns_none_when_git_not_installed(self, monkeypatch):
        monkeypatch.setattr(
            "datajunction.deployment.subprocess.run",
            mock.Mock(side_effect=FileNotFoundError),
        )
        assert DeploymentService._detect_git_commit_author() == (None, None)

    def test_handles_empty_name(self, monkeypatch):
        monkeypatch.setattr(
            "datajunction.deployment.subprocess.run",
            lambda *a, **kw: mock.MagicMock(stdout="alice@example.com|\n"),
        )
        email, name = DeploymentService._detect_git_commit_author()
        assert email == "alice@example.com"
        assert name is None


class TestBranchToNamespaceSuffix:
    """Tests for _branch_to_namespace_suffix."""

    def test_simple_branch(self):
        assert DeploymentService._branch_to_namespace_suffix("main") == "main"

    def test_slash_and_hyphen_replaced_with_underscore(self):
        assert (
            DeploymentService._branch_to_namespace_suffix("feature/my-metric")
            == "feature_my_metric"
        )

    def test_underscores_preserved(self):
        assert (
            DeploymentService._branch_to_namespace_suffix("fix_some_thing")
            == "fix_some_thing"
        )

    def test_consecutive_special_chars(self):
        assert (
            DeploymentService._branch_to_namespace_suffix("feat//weird--branch")
            == "feat_weird_branch"
        )


class TestDeriveNamespace:
    """Tests for _derive_namespace."""

    def test_replaces_last_segment_with_branch(self):
        assert (
            DeploymentService._derive_namespace("project.main", "feature/my-metric")
            == "project.feature_my_metric"
        )

    def test_main_branch_preserves_namespace(self):
        assert (
            DeploymentService._derive_namespace("project.main", "main")
            == "project.main"
        )

    def test_single_segment_base_appends_branch(self):
        assert (
            DeploymentService._derive_namespace("project", "feature-x")
            == "project.feature_x"
        )

    def test_deep_namespace(self):
        assert (
            DeploymentService._derive_namespace("org.team.main", "my-branch")
            == "org.team.my_branch"
        )


class TestPushBranchDetection:
    """Tests for push() branch auto-detection."""

    def test_push_derives_namespace_from_git_branch(self, monkeypatch, tmp_path):
        """When no namespace is passed, push() derives it from the git branch."""
        (tmp_path / "dj.yaml").write_text(yaml.safe_dump({"namespace": "project.main"}))
        (tmp_path / "my_node.yaml").write_text(
            yaml.safe_dump({"name": "project.my_node"}),
        )

        monkeypatch.delenv("DJ_DEPLOY_REPO", raising=False)
        monkeypatch.setattr(
            "datajunction.deployment.subprocess.run",
            lambda *a, **kw: mock.MagicMock(stdout="feature/new-metric\n"),
        )
        monkeypatch.setattr(time, "sleep", lambda _: None)

        client = MagicMock()
        client.deploy.return_value = {"uuid": "abc", "status": "success", "results": []}
        client.check_deployment.return_value = {
            "uuid": "abc",
            "status": "success",
            "results": [],
        }

        svc = DeploymentService(client, console=Console(file=io.StringIO()))
        svc.push(tmp_path)

        deployed_namespace = client.deploy.call_args[0][0]["namespace"]
        assert deployed_namespace == "project.feature_new_metric"

    def test_push_explicit_namespace_overrides_git(self, monkeypatch, tmp_path):
        """An explicit namespace argument always wins over git detection."""
        (tmp_path / "dj.yaml").write_text(yaml.safe_dump({"namespace": "project.main"}))
        (tmp_path / "my_node.yaml").write_text(
            yaml.safe_dump({"name": "project.my_node"}),
        )

        monkeypatch.delenv("DJ_DEPLOY_REPO", raising=False)
        monkeypatch.setattr(
            "datajunction.deployment.subprocess.run",
            lambda *a, **kw: mock.MagicMock(stdout="feature/new-metric\n"),
        )
        monkeypatch.setattr(time, "sleep", lambda _: None)

        client = MagicMock()
        client.deploy.return_value = {"uuid": "abc", "status": "success", "results": []}

        svc = DeploymentService(client, console=Console(file=io.StringIO()))
        svc.push(tmp_path, namespace="project.explicit-override")

        deployed_namespace = client.deploy.call_args[0][0]["namespace"]
        assert deployed_namespace == "project.explicit-override"

    def test_push_no_git_repo_uses_dj_yaml_namespace(self, monkeypatch, tmp_path):
        """When git is unavailable, falls back to the namespace in dj.yaml."""
        (tmp_path / "dj.yaml").write_text(yaml.safe_dump({"namespace": "project.main"}))
        (tmp_path / "my_node.yaml").write_text(
            yaml.safe_dump({"name": "project.my_node"}),
        )

        monkeypatch.delenv("DJ_DEPLOY_REPO", raising=False)
        import subprocess as sp

        monkeypatch.setattr(
            "datajunction.deployment.subprocess.run",
            mock.Mock(side_effect=sp.CalledProcessError(128, "git")),
        )
        monkeypatch.setattr(time, "sleep", lambda _: None)

        client = MagicMock()
        client.deploy.return_value = {"uuid": "abc", "status": "success", "results": []}

        svc = DeploymentService(client, console=Console(file=io.StringIO()))
        svc.push(tmp_path)

        deployed_namespace = client.deploy.call_args[0][0]["namespace"]
        assert deployed_namespace == "project.main"

    def test_git_source_includes_branch_when_no_env_var(self, monkeypatch):
        """_build_deployment_source falls back to git detection for branch."""
        monkeypatch.setenv("DJ_DEPLOY_REPO", "github.com/org/repo")
        monkeypatch.delenv("DJ_DEPLOY_BRANCH", raising=False)
        monkeypatch.delenv("DJ_DEPLOY_COMMIT", raising=False)
        monkeypatch.delenv("DJ_DEPLOY_CI_SYSTEM", raising=False)
        monkeypatch.delenv("DJ_DEPLOY_CI_RUN_URL", raising=False)
        monkeypatch.setattr(
            "datajunction.deployment.subprocess.run",
            lambda *a, **kw: mock.MagicMock(stdout="feature/auto-detected\n"),
        )

        result = DeploymentService._build_deployment_source()

        assert result["branch"] == "feature/auto-detected"

    def test_push_git_config_failure_is_warned_not_raised(self, monkeypatch, tmp_path):
        """When _set_namespace_git_config raises, push() prints a warning and continues."""
        (tmp_path / "dj.yaml").write_text(yaml.safe_dump({"namespace": "project.main"}))
        (tmp_path / "my_node.yaml").write_text(
            yaml.safe_dump({"name": "project.my_node"}),
        )

        monkeypatch.delenv("DJ_DEPLOY_REPO", raising=False)
        monkeypatch.setattr(
            DeploymentService,
            "_detect_git_branch",
            staticmethod(lambda cwd=None: "main"),
        )
        monkeypatch.setattr(
            DeploymentService,
            "_detect_git_repo",
            staticmethod(lambda cwd=None: None),
        )
        monkeypatch.setattr(time, "sleep", lambda _: None)

        client = MagicMock()
        client._set_namespace_git_config.side_effect = Exception("network error")
        client.deploy.return_value = {"uuid": "abc", "status": "success", "results": []}
        client.check_deployment.return_value = {
            "uuid": "abc",
            "status": "success",
            "results": [],
        }

        out = io.StringIO()
        svc = DeploymentService(client, console=Console(file=out))
        svc.push(tmp_path)

        assert "Warning" in out.getvalue()
        client.deploy.assert_called_once()


def test_djdeploymentfailure_str_with_errors():
    exc = DJDeploymentFailure(
        project_name="my.namespace",
        errors=[
            {"name": "my.namespace.node_a", "message": "Column `x` does not exist"},
            {"name": "my.namespace.node_b", "message": None},
        ],
    )
    rendered = str(exc)
    assert "my.namespace" in rendered
    assert "my.namespace.node_a" in rendered
    assert "Column `x` does not exist" in rendered
    assert "my.namespace.node_b" in rendered
    assert "(no message)" in rendered


def test_djdeploymentfailure_str_with_no_errors():
    exc = DJDeploymentFailure(project_name="my.namespace", errors=[])
    assert str(exc) == exc.message
    assert "my.namespace" in str(exc)
