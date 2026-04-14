"""
Batched dimension reachability for cube validation and impact propagation.

Uses a single BFS query (via find_join_paths_batch) to determine which
dimensions are reachable from a set of source nodes through the dimension
link graph.  All subsequent lookups are pure in-memory.

Used by:
  - Cube validation: are all requested dimensions reachable from every metric?
  - Dimension link propagation: which cubes lost dimension access after a link change?
  - Cube filter validation: are filter dimensions reachable?
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.construction.build_v3.loaders import find_join_paths_batch


class DimensionReachability:
    """Batched dimension reachability — one BFS query, in-memory lookups."""

    def __init__(
        self,
        paths: dict[tuple[int, str, str], list[int]],
        local_names: dict[int, str] | None = None,
    ):
        self._paths = paths
        # Fast lookup: source_rev_id → set of reachable dimension node names
        self._reachable: dict[int, set[str]] = {}
        for (src_id, dim_name, _role), _links in paths.items():
            self._reachable.setdefault(src_id, set()).add(dim_name)
        # Local dimensions: a node can always reach itself (its own columns
        # are "local dimensions" that don't need a join path).
        if local_names:
            for rev_id, node_name in local_names.items():
                self._reachable.setdefault(rev_id, set()).add(node_name)

    @classmethod
    async def build(
        cls,
        session: AsyncSession,
        source_revision_ids: set[int],
        target_dimension_names: set[str],
        local_names: dict[int, str] | None = None,
    ) -> DimensionReachability:
        """One batched BFS query to build the reachability map.

        Args:
            source_revision_ids: Revision IDs of the source nodes to BFS from.
            target_dimension_names: Dimension node names to search for.
            local_names: Optional mapping of rev_id → node_name.  Each source
                node is always reachable from itself (local dimensions).
        """
        if not source_revision_ids or not target_dimension_names:
            return cls({}, local_names)
        paths = await find_join_paths_batch(
            session,
            source_revision_ids,
            target_dimension_names,
        )
        return cls(paths, local_names)

    def is_reachable(self, source_rev_id: int, dim_name: str) -> bool:
        """Check if a dimension is reachable from a source node."""
        return dim_name in self._reachable.get(source_rev_id, set())

    def reachable_from(self, source_rev_id: int) -> set[str]:
        """All dimension names reachable from a source node."""
        return self._reachable.get(source_rev_id, set())

    def shared_dimensions(self, source_rev_ids: set[int]) -> set[str]:
        """Intersection of reachable dimensions across all source nodes."""
        if not source_rev_ids:
            return set()
        sets = [self.reachable_from(sid) for sid in source_rev_ids]
        return set.intersection(*sets) if sets else set()

    def unreachable_dimensions(
        self,
        source_rev_ids: set[int],
        requested: set[str],
    ) -> dict[str, set[int]]:
        """For each requested dim, which source_rev_ids can't reach it?

        Returns a dict mapping dim_name → set of source_rev_ids that lack
        a path to it.  Empty dict means all dimensions are reachable from
        all sources.
        """
        missing: dict[str, set[int]] = {}
        for dim in requested:
            unreachable_from = {
                sid for sid in source_rev_ids if not self.is_reachable(sid, dim)
            }
            if unreachable_from:
                missing[dim] = unreachable_from
        return missing
