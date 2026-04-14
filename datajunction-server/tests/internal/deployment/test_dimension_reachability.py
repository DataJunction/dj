"""
Tests for DimensionReachability — batched dimension reachability lookups.
"""

import pytest
from unittest.mock import AsyncMock, patch

from datajunction_server.internal.deployment.dimension_reachability import (
    DimensionReachability,
)


class TestDimensionReachabilityInMemory:
    """Pure in-memory tests — no DB, no BFS, just the lookup logic."""

    def test_empty_paths(self):
        r = DimensionReachability({})
        assert r.reachable_from(1) == set()
        assert not r.is_reachable(1, "dim.a")
        assert r.shared_dimensions(set()) == set()
        assert r.shared_dimensions({1}) == set()
        assert r.unreachable_dimensions({1}, {"dim.a"}) == {"dim.a": {1}}

    def test_single_source_single_dim(self):
        paths = {(10, "dim.country", ""): [100]}
        r = DimensionReachability(paths)
        assert r.is_reachable(10, "dim.country")
        assert not r.is_reachable(10, "dim.city")
        assert r.reachable_from(10) == {"dim.country"}

    def test_single_source_multiple_dims(self):
        paths = {
            (10, "dim.country", ""): [100],
            (10, "dim.city", ""): [101],
            (10, "dim.date", ""): [102],
        }
        r = DimensionReachability(paths)
        assert r.reachable_from(10) == {"dim.country", "dim.city", "dim.date"}

    def test_multiple_sources_shared(self):
        paths = {
            (10, "dim.country", ""): [100],
            (10, "dim.date", ""): [101],
            (20, "dim.country", ""): [200],
            (20, "dim.date", ""): [201],
            (20, "dim.city", ""): [202],
        }
        r = DimensionReachability(paths)
        # country and date are shared; city is only reachable from 20
        assert r.shared_dimensions({10, 20}) == {"dim.country", "dim.date"}

    def test_unreachable_dimensions(self):
        paths = {
            (10, "dim.country", ""): [100],
            (20, "dim.country", ""): [200],
            (20, "dim.date", ""): [201],
        }
        r = DimensionReachability(paths)
        missing = r.unreachable_dimensions({10, 20}, {"dim.country", "dim.date"})
        # date is only missing from source 10
        assert missing == {"dim.date": {10}}

    def test_unreachable_all_present(self):
        paths = {
            (10, "dim.a", ""): [1],
            (20, "dim.a", ""): [2],
        }
        r = DimensionReachability(paths)
        assert r.unreachable_dimensions({10, 20}, {"dim.a"}) == {}

    def test_roles_are_tracked(self):
        """Different roles for the same dim are both reachable."""
        paths = {
            (10, "dim.date", ""): [100],
            (10, "dim.date", "order"): [101],
        }
        r = DimensionReachability(paths)
        # dim.date is reachable (role doesn't affect node-level reachability)
        assert r.is_reachable(10, "dim.date")

    def test_local_names(self):
        """Local names make each source reachable from itself."""
        paths = {(10, "dim.country", ""): [100]}
        local = {10: "node.fact_table", 20: "node.other_table"}
        r = DimensionReachability(paths, local_names=local)
        # 10 can reach dim.country (via BFS) and node.fact_table (local)
        assert r.is_reachable(10, "dim.country")
        assert r.is_reachable(10, "node.fact_table")
        # 20 can only reach node.other_table (local), nothing via BFS
        assert r.is_reachable(20, "node.other_table")
        assert not r.is_reachable(20, "dim.country")

    def test_local_names_in_shared(self):
        """Local dimensions participate in shared_dimensions."""
        paths = {}
        local = {10: "node.same", 20: "node.same"}
        r = DimensionReachability(paths, local_names=local)
        # Both sources are "node.same" → it's shared
        assert r.shared_dimensions({10, 20}) == {"node.same"}

    def test_local_names_not_shared(self):
        """Different local names are not shared."""
        paths = {}
        local = {10: "node.a", 20: "node.b"}
        r = DimensionReachability(paths, local_names=local)
        assert r.shared_dimensions({10, 20}) == set()


class TestDimensionReachabilityBuild:
    """Tests for the async build method using mocked find_join_paths_batch."""

    @pytest.mark.asyncio
    async def test_build_empty(self):
        r = await DimensionReachability.build(
            session=AsyncMock(),
            source_revision_ids=set(),
            target_dimension_names=set(),
        )
        assert r.reachable_from(1) == set()

    @pytest.mark.asyncio
    async def test_build_delegates_to_find_join_paths_batch(self):
        mock_paths = {
            (10, "dim.country", ""): [100],
            (20, "dim.country", ""): [200],
        }
        with patch(
            "datajunction_server.internal.deployment.dimension_reachability.find_join_paths_batch",
            new_callable=AsyncMock,
            return_value=mock_paths,
        ) as mock_bfs:
            r = await DimensionReachability.build(
                session=AsyncMock(),
                source_revision_ids={10, 20},
                target_dimension_names={"dim.country"},
                local_names={10: "node.fact"},
            )
            mock_bfs.assert_called_once()

        assert r.is_reachable(10, "dim.country")
        assert r.is_reachable(10, "node.fact")  # local
        assert r.is_reachable(20, "dim.country")

    @pytest.mark.asyncio
    async def test_build_with_local_names_no_targets(self):
        """Empty targets → no BFS, but local names still work."""
        r = await DimensionReachability.build(
            session=AsyncMock(),
            source_revision_ids={10},
            target_dimension_names=set(),
            local_names={10: "node.self"},
        )
        assert r.is_reachable(10, "node.self")
        assert not r.is_reachable(10, "dim.other")
