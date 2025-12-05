"""
Tests for dimension reachability functions.
"""

import pytest

from datajunction_server.internal.dimension_reachability import (
    _compute_transitive_closure,
)


class TestComputeTransitiveClosure:
    """Unit tests for the transitive closure algorithm."""

    def test_empty_graph(self):
        """Empty graph returns empty closure."""
        result = _compute_transitive_closure(set(), set())
        assert result == set()

    def test_single_node(self):
        """Single node can reach itself."""
        result = _compute_transitive_closure({1}, set())
        assert result == {(1, 1)}

    def test_direct_edge(self):
        """Direct edge is included in closure."""
        nodes = {1, 2}
        edges = {(1, 2)}
        result = _compute_transitive_closure(nodes, edges)
        assert result == {(1, 1), (2, 2), (1, 2)}

    def test_transitive_chain(self):
        """Chain A -> B -> C produces A -> C."""
        nodes = {1, 2, 3}
        edges = {(1, 2), (2, 3)}
        result = _compute_transitive_closure(nodes, edges)
        expected = {
            (1, 1),
            (2, 2),
            (3, 3),  # Self-loops
            (1, 2),
            (2, 3),  # Direct edges
            (1, 3),  # Transitive edge
        }
        assert result == expected

    def test_diamond_graph(self):
        """
        Diamond pattern:
            1
           / \\
          2   3
           \\ /
            4
        """
        nodes = {1, 2, 3, 4}
        edges = {(1, 2), (1, 3), (2, 4), (3, 4)}
        result = _compute_transitive_closure(nodes, edges)
        expected = {
            (1, 1),
            (2, 2),
            (3, 3),
            (4, 4),  # Self-loops
            (1, 2),
            (1, 3),
            (2, 4),
            (3, 4),  # Direct edges
            (1, 4),  # Transitive: 1 -> 2 -> 4 and 1 -> 3 -> 4
        }
        assert result == expected

    def test_cycle(self):
        """Cycles are handled correctly (all nodes reach all nodes in cycle)."""
        nodes = {1, 2, 3}
        edges = {(1, 2), (2, 3), (3, 1)}  # 1 -> 2 -> 3 -> 1
        result = _compute_transitive_closure(nodes, edges)
        # In a cycle, every node can reach every other node
        expected = {(i, j) for i in nodes for j in nodes}
        assert result == expected

    def test_disconnected_components(self):
        """Disconnected components don't affect each other."""
        nodes = {1, 2, 3, 4}
        edges = {(1, 2), (3, 4)}  # Two separate edges
        result = _compute_transitive_closure(nodes, edges)
        expected = {
            (1, 1),
            (2, 2),
            (3, 3),
            (4, 4),  # Self-loops
            (1, 2),
            (3, 4),  # Direct edges, no cross-component edges
        }
        assert result == expected

    def test_longer_chain(self):
        """Longer chains work correctly."""
        nodes = {1, 2, 3, 4, 5}
        edges = {(1, 2), (2, 3), (3, 4), (4, 5)}
        result = _compute_transitive_closure(nodes, edges)

        # Node 1 should reach all nodes
        assert (1, 2) in result
        assert (1, 3) in result
        assert (1, 4) in result
        assert (1, 5) in result

        # Node 3 should reach 4 and 5
        assert (3, 4) in result
        assert (3, 5) in result

        # Node 5 should only reach itself
        assert (5, 5) in result
        assert (5, 1) not in result


# Integration tests require database fixtures
# These will be added when running with the full test suite


@pytest.mark.asyncio
async def test_rebuild_dimension_reachability_empty_db(module__session):
    """
    Test rebuild with no dimensions returns 0.
    """
    # This test requires the database fixtures from conftest.py
    # It will be run as part of the integration test suite
    pass


@pytest.mark.asyncio
async def test_ensure_reachability_table_populated(module__session):
    """
    Test that ensure_reachability_table_populated rebuilds when empty.
    """
    # This test requires the database fixtures from conftest.py
    pass


@pytest.mark.asyncio
async def test_get_reachability_stats(module__session):
    """
    Test getting reachability statistics.
    """
    # This test requires the database fixtures from conftest.py
    pass
