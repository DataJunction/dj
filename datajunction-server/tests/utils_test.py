"""
Tests for utility functions.
"""

from datajunction_server.utils import deep_merge


class TestDeepMerge:
    """Tests for the deep_merge utility function."""

    def test_deep_merge_simple(self):
        """Test basic deep merge of two dicts."""
        base = {"a": 1, "b": 2}
        overrides = {"b": 3, "c": 4}
        result = deep_merge(base, overrides)
        assert result == {"a": 1, "b": 3, "c": 4}

    def test_deep_merge_nested(self):
        """Test deep merge with nested dictionaries."""
        base = {
            "a": 1,
            "b": {
                "c": 2,
                "d": 3,
            },
        }
        overrides = {
            "b": {
                "c": 10,
                "e": 5,
            },
        }
        result = deep_merge(base, overrides)
        assert result == {
            "a": 1,
            "b": {
                "c": 10,
                "d": 3,
                "e": 5,
            },
        }

    def test_deep_merge_deeply_nested(self):
        """Test deep merge with multiple levels of nesting."""
        base = {
            "level1": {
                "level2": {
                    "level3": {
                        "a": 1,
                        "b": 2,
                    },
                },
            },
        }
        overrides = {
            "level1": {
                "level2": {
                    "level3": {
                        "b": 20,
                        "c": 30,
                    },
                },
            },
        }
        result = deep_merge(base, overrides)
        assert result == {
            "level1": {
                "level2": {
                    "level3": {
                        "a": 1,
                        "b": 20,
                        "c": 30,
                    },
                },
            },
        }

    def test_deep_merge_override_replaces_non_dict(self):
        """Test that override replaces non-dict value with dict."""
        base = {"a": 1}
        overrides = {"a": {"nested": "value"}}
        result = deep_merge(base, overrides)
        assert result == {"a": {"nested": "value"}}

    def test_deep_merge_non_dict_replaces_dict(self):
        """Test that non-dict override replaces dict value."""
        base = {"a": {"nested": "value"}}
        overrides = {"a": 1}
        result = deep_merge(base, overrides)
        assert result == {"a": 1}

    def test_deep_merge_empty_base(self):
        """Test deep merge with empty base dict."""
        base = {}
        overrides = {"a": 1, "b": {"c": 2}}
        result = deep_merge(base, overrides)
        assert result == {"a": 1, "b": {"c": 2}}

    def test_deep_merge_empty_overrides(self):
        """Test deep merge with empty overrides dict."""
        base = {"a": 1, "b": {"c": 2}}
        overrides = {}
        result = deep_merge(base, overrides)
        assert result == {"a": 1, "b": {"c": 2}}

    def test_deep_merge_does_not_mutate_inputs(self):
        """Test that deep_merge does not modify the input dicts."""
        base = {"a": 1, "b": {"c": 2}}
        overrides = {"b": {"c": 10, "d": 3}}

        # Store original values
        base_original = {"a": 1, "b": {"c": 2}}
        overrides_original = {"b": {"c": 10, "d": 3}}

        result = deep_merge(base, overrides)

        # Verify inputs are unchanged
        assert base == base_original
        assert overrides == overrides_original

        # Verify result is independent
        result["b"]["c"] = 999
        assert base["b"]["c"] == 2
        assert overrides["b"]["c"] == 10

    def test_deep_merge_with_lists(self):
        """Test that lists are replaced, not merged."""
        base = {"a": [1, 2, 3]}
        overrides = {"a": [4, 5]}
        result = deep_merge(base, overrides)
        assert result == {"a": [4, 5]}

    def test_deep_merge_druid_spec_example(self):
        """Test deep_merge with a realistic Druid spec override scenario."""
        base = {
            "dataSchema": {
                "dataSource": "my_cube",
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": "DAY",
                    "intervals": [],
                },
            },
            "tuningConfig": {
                "partitionsSpec": {
                    "targetPartitionSize": 5000000,
                    "type": "hashed",
                },
                "useCombiner": True,
                "type": "hadoop",
            },
        }

        overrides = {
            "tuningConfig": {
                "partitionsSpec": {
                    "targetRowsPerSegment": 1000000,
                    "type": "single_dim",
                    "partitionDimension": "date_id",
                },
                "maxNumConcurrentSubTasks": 20,
            },
            "dataSchema": {
                "granularitySpec": {
                    "queryGranularity": "HOUR",
                },
            },
        }

        result = deep_merge(base, overrides)

        # Check tuningConfig
        assert (
            result["tuningConfig"]["partitionsSpec"]["targetRowsPerSegment"] == 1000000
        )
        assert result["tuningConfig"]["partitionsSpec"]["type"] == "single_dim"
        assert (
            result["tuningConfig"]["partitionsSpec"]["partitionDimension"] == "date_id"
        )
        assert result["tuningConfig"]["maxNumConcurrentSubTasks"] == 20
        # Original values preserved
        assert result["tuningConfig"]["useCombiner"] is True
        assert result["tuningConfig"]["type"] == "hadoop"
        # Note: targetPartitionSize was in base but not in overrides' partitionsSpec,
        # and since partitionsSpec is a dict, it gets deep-merged
        assert (
            result["tuningConfig"]["partitionsSpec"]["targetPartitionSize"] == 5000000
        )

        # Check dataSchema
        assert result["dataSchema"]["dataSource"] == "my_cube"
        assert result["dataSchema"]["granularitySpec"]["queryGranularity"] == "HOUR"
        assert result["dataSchema"]["granularitySpec"]["type"] == "uniform"
        assert result["dataSchema"]["granularitySpec"]["segmentGranularity"] == "DAY"
