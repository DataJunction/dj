"""
Batched dimension reachability for cube validation and impact propagation.

Two batched queries determine which dimensions are reachable from a set of
source nodes: a BFS over the dimension link graph (via find_join_paths_batch)
and a lookup of column-level reference dimensions.  All subsequent lookups are
pure in-memory.

Used by:
  - Cube validation: are all requested dimensions reachable from every metric?
  - Dimension link propagation: which cubes lost dimension access after a link change?
  - Cube filter validation: are filter dimensions reachable?
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.construction.build_v3.loaders import find_join_paths_batch
from datajunction_server.database.column import Column
from datajunction_server.database.node import Node


async def find_reference_dimensions_batch(
    session: AsyncSession,
    source_revision_ids: set[int],
    target_dimension_names: set[str],
) -> dict[int, set[tuple[str, str]]]:
    """Reference-linked dimensions per source revision, as (dim_name, role) pairs.

    A reference link is not a graph edge — it lives on ``Column.dimension_id``
    with the role inline in ``dimension_column`` — so the BFS cannot see it.
    """
    statement = (
        select(Column.node_revision_id, Node.name, Column.dimension_column)
        .join(Node, Node.id == Column.dimension_id)
        .where(Column.node_revision_id.in_(source_revision_ids))
        .where(Node.name.in_(target_dimension_names))
    )
    rows = (await session.execute(statement)).all()
    reference_dims: dict[int, set[tuple[str, str]]] = {}
    for rev_id, dim_name, dimension_column in rows:
        role = ""
        if dimension_column and "[" in dimension_column:
            role = dimension_column.split("[", 1)[1].rstrip("]")
        reference_dims.setdefault(rev_id, set()).add((dim_name, role))
    return reference_dims


class DimensionReachability:
    """Batched dimension reachability — two batched queries, in-memory lookups.

    Tracks reachability node-level (role-agnostic, for impact propagation) and
    role-aware (for cube deploy validation). See `is_reachable_under_role` for
    the bare-vs-role matching rule.
    """

    def __init__(
        self,
        paths: dict[tuple[int, str, str], list[int]],
        local_names: dict[int, str] | None = None,
        reference_dims: dict[int, set[tuple[str, str]]] | None = None,
    ):
        self._paths = paths
        # Reachable (dimension, role) pairs for each source node; role is "" for
        # a role-less path. Keying on role — not just the dimension — is what
        # rejects a bare reference that's only reachable under a role.
        self._reachable_roles: dict[int, set[tuple[str, str]]] = {}
        for (src_id, dim_name, role_path), _links in paths.items():
            self._reachable_roles.setdefault(src_id, set()).add(
                (dim_name, role_path),
            )
        # A node always reaches its own columns ("local dimensions"), role-less.
        for rev_id, node_name in (local_names or {}).items():
            self._reachable_roles.setdefault(rev_id, set()).add((node_name, ""))
        # Reference-linked dimensions are local too — the column already holds the
        # value — but they carry the role declared on the column.
        for rev_id, dim_roles in (reference_dims or {}).items():
            self._reachable_roles.setdefault(rev_id, set()).update(dim_roles)
        # Node-level (role-agnostic) view, derived once for O(1) lookups.
        self._reachable: dict[int, set[str]] = {
            sid: {name for name, _role in roles}
            for sid, roles in self._reachable_roles.items()
        }

    @classmethod
    async def build(
        cls,
        session: AsyncSession,
        source_revision_ids: set[int],
        target_dimension_names: set[str],
        local_names: dict[int, str] | None = None,
    ) -> DimensionReachability:
        """Batched queries (join-path BFS plus reference links) to build the map.

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
        reference_dims = await find_reference_dimensions_batch(
            session,
            source_revision_ids,
            target_dimension_names,
        )
        return cls(paths, local_names, reference_dims)

    def is_reachable(self, source_rev_id: int, dim_name: str) -> bool:
        """Node-level (role-agnostic) check: is the dimension reachable at all?"""
        return dim_name in self._reachable.get(source_rev_id, set())

    def is_reachable_under_role(
        self,
        source_rev_id: int,
        dim_name: str,
        role: str | None,
    ) -> bool:
        """Role-aware check. A bare reference (role=None) matches only a
        role-less join path; a role string matches only that exact role path
        (e.g. "order_date", or "a->b" for multi-hop).
        """
        return (dim_name, role or "") in self._reachable_roles.get(
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
        """Role-aware variant of unreachable_dimensions. `requested` is a set of
        (dim_node_name, role) pairs (role=None for bare); see
        is_reachable_under_role for matching. Returns {(dim, role): set of source
        rev ids that lack a path}.
        """
        missing: dict[tuple[str, str | None], set[int]] = {}
        for dim_name, role in requested:
            unreachable_from = {
                sid
                for sid in source_rev_ids
                if not self.is_reachable_under_role(sid, dim_name, role)
            }
            if unreachable_from:
                missing[(dim_name, role)] = unreachable_from
        return missing
