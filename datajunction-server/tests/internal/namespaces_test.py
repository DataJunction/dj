"""
Tests for internal namespace functions
"""

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq

from datajunction_server.internal.namespaces import (
    _merge_list_with_key,
    _merge_yaml_preserving_comments,
)


class TestMergeListWithKey:
    """Tests for _merge_list_with_key function"""

    def test_merge_yaml_list_preserves_attribute_order_when_unchanged(self):
        """Test that attribute order is preserved when the attribute set hasn't changed"""
        from ruamel.yaml import YAML

        yaml = YAML()

        # Create existing list by parsing YAML to get proper CommentedMap structure
        existing_yaml = """
- name: col1
  type: int
  attributes:
    - primary_key
    - dimension
"""
        existing = yaml.load(existing_yaml)

        # Create new list with same attributes but different order
        new_yaml = """
- name: col1
  type: int
  attributes:
    - dimension
    - primary_key
"""
        new_list = yaml.load(new_yaml)

        result = _merge_list_with_key(existing, new_list, "name")

        # Should preserve the original attribute order since the set is unchanged
        assert result[0]["attributes"] == ["primary_key", "dimension"]

    def test_merge_yaml_list_updates_attributes_when_changed(self):
        """Test that attributes are updated when the set changes"""
        # Create existing list with specific attribute order
        existing = CommentedSeq(
            [
                CommentedMap(
                    {
                        "name": "col1",
                        "type": "int",
                        "attributes": ["primary_key", "dimension"],
                    },
                ),
            ],
        )

        # Create new list with different attributes
        new_list = CommentedSeq(
            [
                CommentedMap(
                    {
                        "name": "col1",
                        "type": "int",
                        "attributes": ["primary_key"],  # Removed "dimension"
                    },
                ),
            ],
        )

        result = _merge_list_with_key(existing, new_list, "name")

        # Should update to new attributes since the set changed
        assert result[0]["attributes"] == ["primary_key"]

    def test_merge_yaml_list_handles_non_list_attributes(self):
        """Test that non-list attributes in existing item don't cause issues"""
        # Create existing list with non-list attributes value
        existing = CommentedSeq(
            [
                CommentedMap(
                    {
                        "name": "col1",
                        "type": "int",
                        "attributes": "not_a_list",  # Not a list
                    },
                ),
            ],
        )

        # Create new list with proper list attributes
        new_list = CommentedSeq(
            [
                CommentedMap(
                    {
                        "name": "col1",
                        "type": "int",
                        "attributes": ["primary_key"],
                    },
                ),
            ],
        )

        result = _merge_list_with_key(existing, new_list, "name")

        # Should update since existing wasn't a list
        assert result[0]["attributes"] == ["primary_key"]

    def test_merge_yaml_list_adds_attributes_when_missing_in_existing(self):
        """Test that attributes are added when they don't exist in existing item"""
        # Create existing list without attributes
        existing = CommentedSeq(
            [
                CommentedMap(
                    {
                        "name": "col1",
                        "type": "int",
                    },
                ),
            ],
        )

        # Create new list with attributes
        new_list = CommentedSeq(
            [
                CommentedMap(
                    {
                        "name": "col1",
                        "type": "int",
                        "attributes": ["primary_key"],
                    },
                ),
            ],
        )

        result = _merge_list_with_key(existing, new_list, "name")

        # Should add the new attributes
        assert result[0]["attributes"] == ["primary_key"]

    def test_merge_via_yaml_preserving_comments_with_unchanged_attributes(self):
        """Test attribute order preservation through the full YAML merge flow"""
        yaml = YAML()

        # Create existing YAML with columns that have specific attribute order
        existing_yaml = """
name: test_node
type: transform
columns:
  - name: col1
    type: int
    attributes:
      - primary_key
      - dimension
  - name: col2
    type: string
"""
        existing = yaml.load(existing_yaml)

        # Create new YAML with same attributes but different order
        new_yaml = """
name: test_node
type: transform
columns:
  - name: col1
    type: int
    attributes:
      - dimension
      - primary_key
  - name: col2
    type: string
"""
        new_data = yaml.load(new_yaml)

        # Merge the YAML structures
        result = _merge_yaml_preserving_comments(existing, new_data, yaml)

        # Should preserve the original attribute order since the set is unchanged
        assert result["columns"][0]["attributes"] == ["primary_key", "dimension"]

    def test_merge_yaml_preserves_cube_metrics_dimensions_order(self):
        """Test that cube metrics and dimensions order is preserved from existing YAML"""
        yaml = YAML()

        # Create existing cube YAML with specific order
        existing_yaml = """
node_type: cube
name: my_cube
description: A cube
metrics:
  - metric_z
  - metric_a
  - metric_m
dimensions:
  - dim_y
  - dim_b
  - dim_x
"""
        existing = yaml.load(existing_yaml)

        # Create new YAML with same items but different order (simulating DB order)
        new_yaml = """
node_type: cube
name: my_cube
description: A cube updated
metrics:
  - metric_a
  - metric_m
  - metric_z
dimensions:
  - dim_b
  - dim_x
  - dim_y
"""
        new_data = yaml.load(new_yaml)

        # Merge the YAML structures
        result = _merge_yaml_preserving_comments(existing, new_data, yaml)

        # Should preserve the original order from existing YAML
        assert result["metrics"] == ["metric_z", "metric_a", "metric_m"]
        assert result["dimensions"] == ["dim_y", "dim_b", "dim_x"]
        # Description should be updated though
        assert result["description"] == "A cube updated"

    def test_merge_yaml_cube_adds_new_metrics_at_end(self):
        """Test that new metrics/dimensions are added at the end when preserving order"""
        yaml = YAML()

        # Create existing cube YAML
        existing_yaml = """
node_type: cube
name: my_cube
metrics:
  - metric_a
  - metric_b
dimensions:
  - dim_x
"""
        existing = yaml.load(existing_yaml)

        # Create new YAML with additional items
        new_yaml = """
node_type: cube
name: my_cube
metrics:
  - metric_a
  - metric_b
  - metric_c
dimensions:
  - dim_x
  - dim_y
"""
        new_data = yaml.load(new_yaml)

        # Merge the YAML structures
        result = _merge_yaml_preserving_comments(existing, new_data, yaml)

        # Should preserve existing order and add new items at end
        assert result["metrics"] == ["metric_a", "metric_b", "metric_c"]
        assert result["dimensions"] == ["dim_x", "dim_y"]

    def test_merge_yaml_cube_removes_deleted_metrics(self):
        """Test that removed metrics/dimensions are not included in result"""
        yaml = YAML()

        # Create existing cube YAML
        existing_yaml = """
node_type: cube
name: my_cube
metrics:
  - metric_a
  - metric_b
  - metric_c
dimensions:
  - dim_x
  - dim_y
"""
        existing = yaml.load(existing_yaml)

        # Create new YAML with some items removed
        new_yaml = """
node_type: cube
name: my_cube
metrics:
  - metric_a
  - metric_c
dimensions:
  - dim_x
"""
        new_data = yaml.load(new_yaml)

        # Merge the YAML structures
        result = _merge_yaml_preserving_comments(existing, new_data, yaml)

        # Should only include items that are in new data, in original order
        assert result["metrics"] == ["metric_a", "metric_c"]
        assert result["dimensions"] == ["dim_x"]
