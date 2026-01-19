"""Tests for models."""

from datajunction.models import (
    ColumnChange,
    NamespaceDiff,
    NamespaceDiffAddedNode,
    NamespaceDiffChangeType,
    NamespaceDiffNodeChange,
    NamespaceDiffRemovedNode,
    QueryState,
)


def test_enum_list():
    """
    Check list of query states works
    """
    assert QueryState.list() == [
        "UNKNOWN",
        "ACCEPTED",
        "SCHEDULED",
        "RUNNING",
        "FINISHED",
        "CANCELED",
        "FAILED",
    ]


# =============================================================================
# Namespace Diff Models Tests
# =============================================================================


class TestNamespaceDiffChangeType:
    """Tests for NamespaceDiffChangeType enum."""

    def test_direct_value(self):
        """Test DIRECT enum value."""
        assert NamespaceDiffChangeType.DIRECT.value == "direct"

    def test_propagated_value(self):
        """Test PROPAGATED enum value."""
        assert NamespaceDiffChangeType.PROPAGATED.value == "propagated"


class TestColumnChange:
    """Tests for ColumnChange dataclass."""

    def test_added_column(self):
        """Test creating an added column change."""
        change = ColumnChange(
            column="new_col",
            change_type="added",
            new_type="string",
        )
        assert change.column == "new_col"
        assert change.change_type == "added"
        assert change.new_type == "string"
        assert change.old_type is None

    def test_removed_column(self):
        """Test creating a removed column change."""
        change = ColumnChange(
            column="old_col",
            change_type="removed",
            old_type="int",
        )
        assert change.column == "old_col"
        assert change.change_type == "removed"
        assert change.old_type == "int"
        assert change.new_type is None

    def test_type_changed_column(self):
        """Test creating a type changed column change."""
        change = ColumnChange(
            column="col",
            change_type="type_changed",
            old_type="int",
            new_type="bigint",
        )
        assert change.column == "col"
        assert change.change_type == "type_changed"
        assert change.old_type == "int"
        assert change.new_type == "bigint"


class TestNamespaceDiffNodeChange:
    """Tests for NamespaceDiffNodeChange dataclass."""

    def test_from_dict_direct_change(self):
        """Test creating a direct change from dict."""
        data = {
            "name": "my_node",
            "full_name": "ns.my_node",
            "node_type": "transform",
            "change_type": "direct",
            "base_version": "v1.0",
            "compare_version": "v2.0",
            "base_status": "valid",
            "compare_status": "valid",
            "changed_fields": ["query", "description"],
        }
        change = NamespaceDiffNodeChange.from_dict(None, data)
        assert change.name == "my_node"
        assert change.full_name == "ns.my_node"
        assert change.node_type == "transform"
        assert change.change_type == NamespaceDiffChangeType.DIRECT
        assert change.base_version == "v1.0"
        assert change.compare_version == "v2.0"
        assert change.changed_fields == ["query", "description"]

    def test_from_dict_propagated_change(self):
        """Test creating a propagated change from dict."""
        data = {
            "name": "downstream_node",
            "full_name": "ns.downstream_node",
            "node_type": "metric",
            "change_type": "propagated",
            "base_version": "v1.0",
            "compare_version": "v1.1",
            "base_status": "valid",
            "compare_status": "invalid",
            "propagation_reason": "version_changed",
            "caused_by": ["upstream_node"],
        }
        change = NamespaceDiffNodeChange.from_dict(None, data)
        assert change.change_type == NamespaceDiffChangeType.PROPAGATED
        assert change.propagation_reason == "version_changed"
        assert change.caused_by == ["upstream_node"]

    def test_from_dict_with_column_changes(self):
        """Test creating a change with column changes."""
        data = {
            "name": "source_node",
            "full_name": "ns.source_node",
            "node_type": "source",
            "change_type": "direct",
            "changed_fields": ["columns"],
            "column_changes": [
                {"column": "new_col", "change_type": "added", "new_type": "string"},
                {"column": "old_col", "change_type": "removed", "old_type": "int"},
                {
                    "column": "mod_col",
                    "change_type": "type_changed",
                    "old_type": "int",
                    "new_type": "bigint",
                },
            ],
        }
        change = NamespaceDiffNodeChange.from_dict(None, data)
        assert len(change.column_changes) == 3
        assert change.column_changes[0].column == "new_col"
        assert change.column_changes[0].change_type == "added"
        assert change.column_changes[1].column == "old_col"
        assert change.column_changes[1].change_type == "removed"
        assert change.column_changes[2].column == "mod_col"
        assert change.column_changes[2].change_type == "type_changed"


class TestNamespaceDiffAddedNode:
    """Tests for NamespaceDiffAddedNode dataclass."""

    def test_from_dict(self):
        """Test creating an added node from dict."""
        data = {
            "name": "new_node",
            "full_name": "ns.new_node",
            "node_type": "transform",
            "display_name": "New Node",
            "description": "A new transform node",
            "status": "valid",
            "version": "v1.0",
        }
        node = NamespaceDiffAddedNode.from_dict(None, data)
        assert node.name == "new_node"
        assert node.full_name == "ns.new_node"
        assert node.node_type == "transform"
        assert node.display_name == "New Node"
        assert node.description == "A new transform node"
        assert node.status == "valid"
        assert node.version == "v1.0"

    def test_from_dict_minimal(self):
        """Test creating an added node with minimal fields."""
        data = {
            "name": "node",
            "full_name": "ns.node",
            "node_type": "source",
        }
        node = NamespaceDiffAddedNode.from_dict(None, data)
        assert node.name == "node"
        assert node.display_name is None
        assert node.description is None


class TestNamespaceDiffRemovedNode:
    """Tests for NamespaceDiffRemovedNode dataclass."""

    def test_from_dict(self):
        """Test creating a removed node from dict."""
        data = {
            "name": "old_node",
            "full_name": "ns.old_node",
            "node_type": "dimension",
            "display_name": "Old Node",
            "description": "A removed dimension node",
            "status": "valid",
            "version": "v1.0",
        }
        node = NamespaceDiffRemovedNode.from_dict(None, data)
        assert node.name == "old_node"
        assert node.full_name == "ns.old_node"
        assert node.node_type == "dimension"


class TestNamespaceDiff:
    """Tests for NamespaceDiff dataclass."""

    def test_from_dict_full(self):
        """Test creating a NamespaceDiff from dict with all fields."""
        data = {
            "base_namespace": "dj.main",
            "compare_namespace": "dj.feature",
            "added": [
                {
                    "name": "new_node",
                    "full_name": "dj.feature.new_node",
                    "node_type": "transform",
                },
            ],
            "removed": [
                {
                    "name": "old_node",
                    "full_name": "dj.main.old_node",
                    "node_type": "source",
                },
            ],
            "direct_changes": [
                {
                    "name": "changed_node",
                    "full_name": "dj.feature.changed_node",
                    "node_type": "metric",
                    "change_type": "direct",
                    "changed_fields": ["query"],
                },
            ],
            "propagated_changes": [
                {
                    "name": "prop_node",
                    "full_name": "dj.feature.prop_node",
                    "node_type": "transform",
                    "change_type": "propagated",
                    "caused_by": ["changed_node"],
                },
            ],
            "unchanged_count": 10,
            "added_count": 1,
            "removed_count": 1,
            "direct_change_count": 1,
            "propagated_change_count": 1,
        }
        diff = NamespaceDiff.from_dict(None, data)
        assert diff.base_namespace == "dj.main"
        assert diff.compare_namespace == "dj.feature"
        assert len(diff.added) == 1
        assert len(diff.removed) == 1
        assert len(diff.direct_changes) == 1
        assert len(diff.propagated_changes) == 1
        assert diff.unchanged_count == 10
        assert diff.added_count == 1

    def test_from_dict_empty(self):
        """Test creating a NamespaceDiff with no changes."""
        data = {
            "base_namespace": "dj.main",
            "compare_namespace": "dj.feature",
            "added": [],
            "removed": [],
            "direct_changes": [],
            "propagated_changes": [],
            "unchanged_count": 20,
            "added_count": 0,
            "removed_count": 0,
            "direct_change_count": 0,
            "propagated_change_count": 0,
        }
        diff = NamespaceDiff.from_dict(None, data)
        assert not diff.has_changes()
        assert diff.unchanged_count == 20

    def test_has_changes_with_added(self):
        """Test has_changes returns True when nodes are added."""
        diff = NamespaceDiff(
            base_namespace="a",
            compare_namespace="b",
            added=[],
            removed=[],
            direct_changes=[],
            propagated_changes=[],
            unchanged_count=0,
            added_count=1,
            removed_count=0,
            direct_change_count=0,
            propagated_change_count=0,
        )
        assert diff.has_changes()

    def test_has_changes_with_removed(self):
        """Test has_changes returns True when nodes are removed."""
        diff = NamespaceDiff(
            base_namespace="a",
            compare_namespace="b",
            added=[],
            removed=[],
            direct_changes=[],
            propagated_changes=[],
            unchanged_count=0,
            added_count=0,
            removed_count=1,
            direct_change_count=0,
            propagated_change_count=0,
        )
        assert diff.has_changes()

    def test_has_changes_with_direct_changes(self):
        """Test has_changes returns True with direct changes."""
        diff = NamespaceDiff(
            base_namespace="a",
            compare_namespace="b",
            added=[],
            removed=[],
            direct_changes=[],
            propagated_changes=[],
            unchanged_count=0,
            added_count=0,
            removed_count=0,
            direct_change_count=1,
            propagated_change_count=0,
        )
        assert diff.has_changes()

    def test_has_changes_false(self):
        """Test has_changes returns False when no changes."""
        diff = NamespaceDiff(
            base_namespace="a",
            compare_namespace="b",
            added=[],
            removed=[],
            direct_changes=[],
            propagated_changes=[],
            unchanged_count=5,
            added_count=0,
            removed_count=0,
            direct_change_count=0,
            propagated_change_count=0,
        )
        assert not diff.has_changes()

    def test_summary_no_changes(self):
        """Test summary returns 'No changes' when nothing changed."""
        diff = NamespaceDiff(
            base_namespace="a",
            compare_namespace="b",
            added=[],
            removed=[],
            direct_changes=[],
            propagated_changes=[],
            unchanged_count=5,
            added_count=0,
            removed_count=0,
            direct_change_count=0,
            propagated_change_count=0,
        )
        assert diff.summary() == "No changes"

    def test_summary_with_changes(self):
        """Test summary includes all change types."""
        diff = NamespaceDiff(
            base_namespace="a",
            compare_namespace="b",
            added=[],
            removed=[],
            direct_changes=[],
            propagated_changes=[],
            unchanged_count=5,
            added_count=2,
            removed_count=1,
            direct_change_count=3,
            propagated_change_count=4,
        )
        summary = diff.summary()
        assert "+2 added" in summary
        assert "-1 removed" in summary
        assert "~3 direct changes" in summary
        assert "~4 propagated" in summary

    def test_to_markdown_header(self):
        """Test to_markdown includes header with namespace names."""
        diff = NamespaceDiff(
            base_namespace="dj.main",
            compare_namespace="dj.feature",
            added=[],
            removed=[],
            direct_changes=[],
            propagated_changes=[],
            unchanged_count=0,
            added_count=0,
            removed_count=0,
            direct_change_count=0,
            propagated_change_count=0,
        )
        md = diff.to_markdown()
        assert "## Namespace Diff: `dj.feature` vs `dj.main`" in md

    def test_to_markdown_summary_table(self):
        """Test to_markdown includes summary table."""
        diff = NamespaceDiff(
            base_namespace="a",
            compare_namespace="b",
            added=[],
            removed=[],
            direct_changes=[],
            propagated_changes=[],
            unchanged_count=10,
            added_count=2,
            removed_count=1,
            direct_change_count=3,
            propagated_change_count=4,
        )
        md = diff.to_markdown()
        assert "### Summary" in md
        assert "| Added | 2 |" in md
        assert "| Removed | 1 |" in md
        assert "| Direct Changes | 3 |" in md
        assert "| Propagated Changes | 4 |" in md
        assert "| Unchanged | 10 |" in md

    def test_to_markdown_added_nodes(self):
        """Test to_markdown includes added nodes section."""
        added_node = NamespaceDiffAddedNode(
            name="new_metric",
            full_name="dj.feature.new_metric",
            node_type="metric",
        )
        diff = NamespaceDiff(
            base_namespace="a",
            compare_namespace="b",
            added=[added_node],
            removed=[],
            direct_changes=[],
            propagated_changes=[],
            unchanged_count=0,
            added_count=1,
            removed_count=0,
            direct_change_count=0,
            propagated_change_count=0,
        )
        md = diff.to_markdown()
        assert "### Added Nodes" in md
        assert "| `new_metric` | metric |" in md

    def test_to_markdown_removed_nodes(self):
        """Test to_markdown includes removed nodes section."""
        removed_node = NamespaceDiffRemovedNode(
            name="old_source",
            full_name="dj.main.old_source",
            node_type="source",
        )
        diff = NamespaceDiff(
            base_namespace="a",
            compare_namespace="b",
            added=[],
            removed=[removed_node],
            direct_changes=[],
            propagated_changes=[],
            unchanged_count=0,
            added_count=0,
            removed_count=1,
            direct_change_count=0,
            propagated_change_count=0,
        )
        md = diff.to_markdown()
        assert "### Removed Nodes" in md
        assert "| `old_source` | source |" in md

    def test_to_markdown_direct_changes(self):
        """Test to_markdown includes direct changes section."""
        direct_change = NamespaceDiffNodeChange(
            name="modified_transform",
            full_name="dj.feature.modified_transform",
            node_type="transform",
            change_type=NamespaceDiffChangeType.DIRECT,
            changed_fields=["query", "description"],
        )
        diff = NamespaceDiff(
            base_namespace="a",
            compare_namespace="b",
            added=[],
            removed=[],
            direct_changes=[direct_change],
            propagated_changes=[],
            unchanged_count=0,
            added_count=0,
            removed_count=0,
            direct_change_count=1,
            propagated_change_count=0,
        )
        md = diff.to_markdown()
        assert "### Direct Changes" in md
        assert "| `modified_transform` | transform | query, description |" in md

    def test_to_markdown_direct_changes_with_columns(self):
        """Test to_markdown includes column changes detail."""
        col_changes = [
            ColumnChange(column="new_col", change_type="added", new_type="string"),
            ColumnChange(column="old_col", change_type="removed", old_type="int"),
            ColumnChange(
                column="mod_col",
                change_type="type_changed",
                old_type="int",
                new_type="bigint",
            ),
        ]
        direct_change = NamespaceDiffNodeChange(
            name="source_node",
            full_name="dj.feature.source_node",
            node_type="source",
            change_type=NamespaceDiffChangeType.DIRECT,
            changed_fields=["columns"],
            column_changes=col_changes,
        )
        diff = NamespaceDiff(
            base_namespace="a",
            compare_namespace="b",
            added=[],
            removed=[],
            direct_changes=[direct_change],
            propagated_changes=[],
            unchanged_count=0,
            added_count=0,
            removed_count=0,
            direct_change_count=1,
            propagated_change_count=0,
        )
        md = diff.to_markdown()
        assert "#### Column Changes" in md
        assert "**source_node**:" in md
        assert "Added: `new_col` (string)" in md
        assert "Removed: `old_col` (int)" in md
        assert "Type changed: `mod_col` (int -> bigint)" in md

    def test_to_markdown_propagated_changes(self):
        """Test to_markdown includes propagated changes section."""
        prop_change = NamespaceDiffNodeChange(
            name="downstream_metric",
            full_name="dj.feature.downstream_metric",
            node_type="metric",
            change_type=NamespaceDiffChangeType.PROPAGATED,
            base_status="valid",
            compare_status="invalid",
            caused_by=["upstream_transform"],
        )
        diff = NamespaceDiff(
            base_namespace="a",
            compare_namespace="b",
            added=[],
            removed=[],
            direct_changes=[],
            propagated_changes=[prop_change],
            unchanged_count=0,
            added_count=0,
            removed_count=0,
            direct_change_count=0,
            propagated_change_count=1,
        )
        md = diff.to_markdown()
        assert "### Propagated Changes" in md
        assert (
            "| `downstream_metric` | metric | valid -> invalid | `upstream_transform` |"
            in md
        )

    def test_to_markdown_no_changes_message(self):
        """Test to_markdown shows no changes message when empty."""
        diff = NamespaceDiff(
            base_namespace="a",
            compare_namespace="b",
            added=[],
            removed=[],
            direct_changes=[],
            propagated_changes=[],
            unchanged_count=5,
            added_count=0,
            removed_count=0,
            direct_change_count=0,
            propagated_change_count=0,
        )
        md = diff.to_markdown()
        assert "*No changes detected between namespaces.*" in md
