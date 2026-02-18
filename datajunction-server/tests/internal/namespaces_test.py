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
