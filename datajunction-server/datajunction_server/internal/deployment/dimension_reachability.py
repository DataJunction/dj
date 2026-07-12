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

# Sentinel for the `role` argument meaning "any role" (node-level check,
# role-agnostic). Distinct from `None`, which means a *bare* (role-less)
# reference that must match only a role-less join path.
_ANY_ROLE = object()


class DimensionReachability:
    """Batched dimension reachability — one BFS query, in-memory lookups.

    Reachability is tracked at two granularities:

    - Node-level (role-agnostic): "is dimension node X reachable at all?"
      Used by impact propagation.
    - Role-aware: "is dimension node X reachable under role R (or bare)?"
      Used by cube deploy-time validation, which must match the role-aware
      availability check that revalidation performs — a *bare* dimension
      reference must only be considered reachable when a *role-less* join path
      exists, not when the sole path carries a role.
    """

    def __init__(
        self,
        paths: dict[tuple[int, str, str], list[int]],
        local_names: dict[int, str] | None = None,
    ):
        self._paths = paths
        # Fast lookup: source_rev_id → set of reachable dimension node names
        self._reachable: dict[int, set[str]] = {}
        # Role-aware lookup: source_rev_id → set of (dim_node_name, role_path).
        # role_path is the "->" joined chain of link roles ("" for a role-less
        # path), exactly as produced by find_join_paths_batch and as suffixed
        # onto dimension attribute names by get_dimensions.
        self._reachable_roles: dict[int, set[tuple[str, str]]] = {}
        for (src_id, dim_name, role_path), _links in paths.items():
            self._reachable.setdefault(src_id, set()).add(dim_name)
            self._reachable_roles.setdefault(src_id, set()).add(
                (dim_name, role_path),
            )
        # Local dimensions: a node can always reach itself (its own columns
        # are "local dimensions" that don't need a join path). Local dimensions
        # carry no role (role-less).
        if local_names:
            for rev_id, node_name in local_names.items():
                self._reachable.setdefault(rev_id, set()).add(node_name)
                self._reachable_roles.setdefault(rev_id, set()).add(
                    (node_name, ""),
                )

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

    def is_reachable(
        self,
        source_rev_id: int,
        dim_name: str,
        role: object = _ANY_ROLE,
    ) -> bool:
        """Check if a dimension is reachable from a source node.

        Args:
            source_rev_id: Revision ID of the source node.
            dim_name: Dimension node name.
            role: If left as the default (`_ANY_ROLE`), performs a role-agnostic
                node-level check. Pass `None` for a *bare* reference (matches
                only a role-less join path) or a role string (e.g.
                ``"order_date"`` / ``"a->b"`` for multi-hop) to match only that
                exact role path.
        """
        if role is _ANY_ROLE:
            return dim_name in self._reachable.get(source_rev_id, set())
        role_path = role or ""
        return (dim_name, role_path) in self._reachable_roles.get(
            source_rev_id,
            set(),
        )

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

    def unreachable_dimension_roles(
        self,
        source_rev_ids: set[int],
        requested: set[tuple[str, str | None]],
    ) -> dict[tuple[str, str | None], set[int]]:
        """Role-aware variant of :meth:`unreachable_dimensions`.

        Args:
            source_rev_ids: Source node revision IDs.
            requested: Set of ``(dim_node_name, role)`` pairs, where ``role`` is
                ``None`` for a bare reference or a role string. A bare reference
                is unreachable unless a role-less join path exists; a role-played
                reference is unreachable unless a path with that exact role
                exists.

        Returns a dict mapping ``(dim_node_name, role)`` → set of source_rev_ids
        that lack a matching path. Empty dict means all are reachable.
        """
        missing: dict[tuple[str, str | None], set[int]] = {}
        for dim_name, role in requested:
            unreachable_from = {
                sid
                for sid in source_rev_ids
                if not self.is_reachable(sid, dim_name, role)
            }
            if unreachable_from:
                missing[(dim_name, role)] = unreachable_from
        return missing
