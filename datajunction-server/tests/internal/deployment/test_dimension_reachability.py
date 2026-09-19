"""
Tests for DimensionReachability — batched dimension reachability lookups.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from datajunction_server.internal.deployment.dimension_reachability import (
    DimensionReachability,
    find_reference_dimensions_batch,
)
from datajunction_server.internal.deployment.utils import (
    extract_dimension_refs_from_filters,
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

    def test_role_aware_reachability(self):
        """Role-aware check: bare matches only role-less paths; a role matches
        only that exact role path."""
        paths = {
            (10, "dim.date", ""): [100],
            (10, "dim.date", "order"): [101],
            (10, "dim.geo", "signup"): [102],
        }
        r = DimensionReachability(paths)
        # Bare request: matches only the role-less path.
        assert r.is_reachable_under_role(10, "dim.date", None)
        # Role request: matches only that exact role.
        assert r.is_reachable_under_role(10, "dim.date", "order")
        assert not r.is_reachable_under_role(10, "dim.date", "ship")
        # dim.geo is only reachable under the `signup` role, not bare.
        assert not r.is_reachable_under_role(10, "dim.geo", None)
        assert r.is_reachable_under_role(10, "dim.geo", "signup")
        # The node-level check stays role-agnostic.
        assert r.is_reachable(10, "dim.geo")

    def test_unreachable_dimension_roles(self):
        """Bare reference to a role-only-linked dim is reported unreachable,
        while the role-qualified reference is reachable."""
        paths = {
            (10, "dim.geo", "signup"): [100],
            (20, "dim.geo", "signup"): [200],
        }
        r = DimensionReachability(paths)
        # Bare request is unreachable from both sources.
        assert r.unreachable_dimension_roles(
            {10, 20},
            {("dim.geo", None)},
        ) == {("dim.geo", None): {10, 20}}
        # Role-qualified request is reachable from both.
        assert r.unreachable_dimension_roles({10, 20}, {("dim.geo", "signup")}) == {}

    def test_local_names_are_role_less(self):
        """Local dimensions are role-less (bare) reachable."""
        r = DimensionReachability({}, local_names={10: "node.fact"})
        assert r.is_reachable_under_role(10, "node.fact", None)
        assert not r.is_reachable_under_role(10, "node.fact", "x")

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


class TestReferenceDimensions:
    """A reference-linked column makes its dimension reachable without a join."""

    def test_reference_dimension_is_reachable(self):
        r = DimensionReachability(
            {},
            local_names={10: "orders"},
            reference_dims={10: {("dim.customer", "")}},
        )
        assert r.is_reachable(10, "dim.customer")
        assert r.is_reachable_under_role(10, "dim.customer", None)
        assert r.reachable_from(10) == {"orders", "dim.customer"}

    def test_reference_dimension_role_matches_only_its_role(self):
        r = DimensionReachability({}, reference_dims={10: {("dim.date", "ordered")}})
        assert r.is_reachable_under_role(10, "dim.date", "ordered")
        assert not r.is_reachable_under_role(10, "dim.date", "shipped")
        # A bare reference does not match a role-only reference link.
        assert not r.is_reachable_under_role(10, "dim.date", None)
        # The node-level check stays role-agnostic.
        assert r.is_reachable(10, "dim.date")

    def test_reference_dimensions_do_not_disturb_join_paths(self):
        paths = {(10, "dim.product", ""): [100], (10, "dim.date", "ordered"): [101]}
        r = DimensionReachability(paths, reference_dims={10: {("dim.customer", "")}})
        assert r.is_reachable_under_role(10, "dim.product", None)
        assert r.is_reachable_under_role(10, "dim.date", "ordered")
        assert not r.is_reachable_under_role(10, "dim.date", None)
        assert r.unreachable_dimension_roles({10}, {("dim.customer", None)}) == {}

    def test_reference_dimensions_participate_in_shared(self):
        r = DimensionReachability(
            {},
            reference_dims={10: {("dim.customer", "")}, 20: {("dim.customer", "")}},
        )
        assert r.shared_dimensions({10, 20}) == {"dim.customer"}

    @pytest.mark.asyncio
    async def test_find_reference_dimensions_batch_parses_roles(self):
        """The role travels inline on dimension_column: `column[role]`."""
        session = AsyncMock()
        session.execute.return_value = MagicMock(
            all=MagicMock(
                return_value=[
                    (10, "dim.customer", "customer_id"),
                    (10, "dim.date", "dateint[ordered]"),
                    (20, "dim.product", None),
                ],
            ),
        )
        reference_dims = await find_reference_dimensions_batch(
            session,
            {10, 20},
            {"dim.customer", "dim.date", "dim.product"},
        )
        assert reference_dims == {
            10: {("dim.customer", ""), ("dim.date", "ordered")},
            20: {("dim.product", "")},
        }


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
        with (
            patch(
                "datajunction_server.internal.deployment.dimension_reachability.find_join_paths_batch",
                new_callable=AsyncMock,
                return_value=mock_paths,
            ) as mock_bfs,
            patch(
                "datajunction_server.internal.deployment.dimension_reachability."
                "find_reference_dimensions_batch",
                new_callable=AsyncMock,
                return_value={20: {("dim.customer", "")}},
            ) as mock_refs,
        ):
            r = await DimensionReachability.build(
                session=AsyncMock(),
                source_revision_ids={10, 20},
                target_dimension_names={"dim.country"},
                local_names={10: "node.fact"},
            )
            mock_bfs.assert_called_once()
            mock_refs.assert_called_once()

        assert r.is_reachable(10, "dim.country")
        assert r.is_reachable(10, "node.fact")  # local
        assert r.is_reachable(20, "dim.country")
        assert r.is_reachable(20, "dim.customer")  # reference link
        assert not r.is_reachable(10, "dim.customer")

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


class TestExtractDimensionRefsFromFilters:
    """Tests for extract_dimension_refs_from_filters."""

    def test_single_filter(self):
        result = extract_dimension_refs_from_filters(
            ["ns.hard_hat.state = 'CA'"],
        )
        assert result == [("ns.hard_hat", "state")]

    def test_multiple_filters(self):
        result = extract_dimension_refs_from_filters(
            ["ns.hard_hat.state = 'CA'", "ns.date_dim.year > 2020"],
        )
        assert sorted(result) == [("ns.date_dim", "year"), ("ns.hard_hat", "state")]

    def test_empty_filters(self):
        assert extract_dimension_refs_from_filters([]) == []

    def test_unparseable_filter(self):
        result = extract_dimension_refs_from_filters(["not valid sql !!!"])
        assert result == []

    def test_filter_with_no_namespace(self):
        result = extract_dimension_refs_from_filters(["x > 5"])
        assert result == []

    def test_filter_with_single_namespace_segment(self):
        result = extract_dimension_refs_from_filters(["hard_hat.state = 'CA'"])
        assert result == []

    def test_filter_with_multiple_refs_in_one_expression(self):
        result = extract_dimension_refs_from_filters(
            ["ns.dim_a.col1 > 5 AND ns.dim_b.col2 = 'x'"],
        )
        assert sorted(result) == [("ns.dim_a", "col1"), ("ns.dim_b", "col2")]
