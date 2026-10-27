"""
Helpers for API endpoints
"""

import asyncio
import http.client
import json
import logging
import re
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from itertools import count
from http import HTTPStatus

from fastapi import Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased, defer, joinedload, selectinload
from sqlalchemy.sql.operators import and_, is_

from datajunction_server.api.notifications import get_notifier
from datajunction_server.construction.build import (
    validate_shared_dimensions,
)
from datajunction_server.construction.build_v2 import FullColumnName
from datajunction_server.construction.build_v3.dimension_refs import (
    split_dimension_ref,
)
from datajunction_server.construction.build_v3.cte import strip_role_suffix
from datajunction_server.construction.build_v3.dimensions import parse_dimension_ref
from datajunction_server.construction.build_v3.loaders import (
    JoinLinkRow,
    JoinPathState,
    advance_join_paths,
    find_upstream_node_names,
)
from datajunction_server.database.attributetype import AttributeType
from datajunction_server.database.catalog import Catalog
from datajunction_server.database.column import Column
from datajunction_server.database.dimensionlink import DimensionLink
from datajunction_server.database.engine import Engine
from datajunction_server.database.history import History
from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.node import (
    MissingParent,
    Node,
    NodeMissingParents,
    NodeRevision,
    RequiredDimension,
)
from datajunction_server.database.user import User
from datajunction_server.errors import (
    DJAlreadyExistsException,
    DJDoesNotExistException,
    DJError,
    DJInvalidInputException,
    DJNodeNotFound,
    DJWarning,
    ErrorCode,
)
from datajunction_server.internal.engines import get_engine
from datajunction_server.internal.history import EntityType
from datajunction_server.models.attribute import RESERVED_ATTRIBUTE_NAMESPACE
from datajunction_server.models.dimensionlink import JoinType
from datajunction_server.models.engine import Dialect
from datajunction_server.models.history import status_change_history
from datajunction_server.models.materialization import (
    MaterializationConfigInfoUnified,
    MaterializationConfigOutput,
    MaterializationStrategy,
)
from datajunction_server.models.node import NodeStatus
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.query import ColumnMetadata, QueryWithResults
from datajunction_server.naming import from_amenable_name
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.sql.parsing import ast
from datajunction_server.typing import END_JOB_STATES
from datajunction_server.utils import SEPARATOR

_logger = logging.getLogger(__name__)

# The role group must accept `-` and `>` so that chained roles like
# `[customer->registration]`, produced by any multi-hop FK path, parse here the same
# way they do in `FullColumnName.role` (see `construction/build_v2.py`). Keep the two
# in sync: a role this regex cannot match is silently dropped by its callers, which
# strip roles with this pattern and then look the column up by name.
COLUMN_NAME_REGEX = r"([A-Za-z0-9_\.]+)(\[[A-Za-z0-9_\-\>]+\])?"


async def get_node_namespace(
    session: AsyncSession,
    namespace: str,
    raise_if_not_exists: bool = True,
) -> NodeNamespace:
    """
    Get a node namespace
    """
    statement = select(NodeNamespace).where(NodeNamespace.namespace == namespace)
    node_namespace = (await session.execute(statement)).scalar_one_or_none()
    if raise_if_not_exists:  # pragma: no cover
        if not node_namespace:
            raise DJDoesNotExistException(
                message=(f"node namespace `{namespace}` does not exist."),
                http_status_code=404,
            )
    return node_namespace


async def check_namespace_not_git_only(
    session: AsyncSession,
    namespace: str,
    *,
    remedy: str = "Node changes must be deployed from git via the /deployments API.",
) -> None:
    """
    Check that a namespace (or any of its ancestors) is not git-managed.

    The check cascades up the dotted namespace hierarchy so that locking
    ``foo.main`` also locks ``foo.main.metrics``, ``foo.main.dimensions``,
    etc. Without that, a flagged parent is trivially bypassable by mutating
    in a child namespace.

    Blocks mutations when any ancestor (or the namespace itself):
    - Has ``git_only=True`` (explicitly locked), OR
    - Is a repo owner (``github_repo_path`` set and no ``parent_namespace``).

    A repo owner covers both a git root (spawns ``<ns>.<branch>`` children) and
    a flat git-backed namespace (tracks a branch directly). Branch namespaces
    (children with a ``parent_namespace``) are intentionally allowed — edits
    there are valid and get synced back to the git repo, so they don't trip
    this check.

    ``remedy`` is the sentence appended to the error. It defaults to the node
    wording because that is what almost every caller is guarding; anything else
    governed by the same flag should say where *its* change belongs instead.

    Implementation: namespace names contain their parent path (``a.b.c``),
    so we materialize every prefix and fire a single ``IN`` query. One round
    trip regardless of hierarchy depth.
    """
    parts = namespace.split(".")
    candidates = [".".join(parts[: i + 1]) for i in range(len(parts))]
    matches = (
        (
            await session.execute(
                select(NodeNamespace).where(NodeNamespace.namespace.in_(candidates)),
            )
        )
        .scalars()
        .all()
    )

    # Walk deepest-first so the most specific locked ancestor (the one the
    # user can most usefully act on) wins the error message. ``is_repo_owner``
    # only applies to the namespace itself — its branch children are
    # intentionally editable — so don't propagate that flag up the chain.
    for ns in sorted(matches, key=lambda n: len(n.namespace), reverse=True):
        is_self = ns.namespace == namespace
        is_repo_owner = (
            is_self and ns.github_repo_path is not None and ns.parent_namespace is None
        )
        if ns.git_only or is_repo_owner:
            scope_msg = (
                f"'{namespace}'"
                if is_self
                else f"'{namespace}' (locked via ancestor '{ns.namespace}')"
            )
            raise DJInvalidInputException(
                message=f"Namespace {scope_msg} is git-managed. {remedy}",
            )


async def get_node_by_name(
    session: AsyncSession,
    name: str | None,
    node_type: NodeType | None = None,
    with_current: bool = False,
    raise_if_not_exists: bool = True,
    include_inactive: bool = False,
) -> Node:
    """
    Get a node by name
    """
    from datajunction_server.models.node import NodeOutput

    statement = select(Node).where(Node.name == name)
    if not include_inactive:
        statement = statement.where(is_(Node.deactivated_at, None))
    if node_type:
        statement = statement.where(Node.type == node_type)
    if with_current:
        # Use full NodeOutput load options to ensure all required fields
        # (like dimension_links) are eagerly loaded for serialization
        statement = statement.options(*NodeOutput.load_options())
        result = await session.execute(statement)
        node = result.unique().scalar_one_or_none()
    else:
        result = await session.execute(statement)
        node = result.unique().scalar_one_or_none()
    if raise_if_not_exists:
        if not node:
            raise DJNodeNotFound(
                message=(
                    f"A {'' if not node_type else node_type + ' '}"
                    f"node with name `{name}` does not exist."
                ),
                http_status_code=404,
            )
    return node


async def raise_if_node_exists(session: AsyncSession, name: str) -> None:
    """
    Raise an error if the node with the given name already exists.
    """
    node = await Node.get_by_name(session, name, raise_if_not_exists=False)
    if node:
        raise DJAlreadyExistsException(
            message=f"A node with name `{name}` already exists.",
            http_status_code=HTTPStatus.CONFLICT,
        )


async def get_column(
    session: AsyncSession,
    node: NodeRevision,
    column_name: str,
) -> Column:
    """
    Get a column from a node revision
    """
    await session.refresh(node, ["columns"])
    return resolve_column(
        node.columns,
        column_name,
        node.name,
        node.type,
        missing_message=f"Column {column_name} does not exist on node {node.name}",
    )


def resolve_column(
    columns: list[Column],
    column_name: str,
    node_name: str,
    node_type: NodeType,
    missing_message: str | None = None,
) -> Column:
    """Resolve a node column, including role-qualified cube identities."""
    if node_type == NodeType.CUBE:
        role_qualified = {column.cube_element_name: column for column in columns}
        if column_name in role_qualified:
            return role_qualified[column_name]

        same_name = [column for column in columns if column.name == column_name]
        if len(same_name) > 1:
            options = sorted(column.cube_element_name for column in same_name)
            raise DJInvalidInputException(
                message=(
                    f"Column `{column_name}` on node `{node_name}` is ambiguous across "
                    f"roles. Use one of: {', '.join(options)}"
                ),
            )
        if same_name:
            return same_name[0]
    else:
        requested_column = next(
            (column for column in columns if column.name == column_name),
            None,
        )
        if requested_column:
            return requested_column

    raise DJDoesNotExistException(
        message=missing_message
        or f"Column `{column_name}` does not exist on node `{node_name}`!",
        http_status_code=404,
    )


async def get_attribute_type(
    session: AsyncSession,
    name: str,
    namespace: str | None = RESERVED_ATTRIBUTE_NAMESPACE,
) -> AttributeType | None:
    """
    Gets an attribute type by name.
    """
    statement = (
        select(AttributeType)
        .where(AttributeType.name == name)
        .where(AttributeType.namespace == namespace)
    )
    return (await session.execute(statement)).scalar_one_or_none()


async def get_catalog_by_name(session: AsyncSession, name: str) -> Catalog:
    """
    Get a catalog by name
    """
    statement = (
        select(Catalog).where(Catalog.name == name).options(joinedload(Catalog.engines))
    )
    catalog = (await session.execute(statement)).scalar()
    if not catalog:
        raise DJDoesNotExistException(
            message=f"Catalog with name `{name}` does not exist.",
            http_status_code=404,
        )
    return catalog


def _resolve_required_dimensions(
    required_dimensions: list[str],
    parent_columns: list[Column],
    dim_nodes: dict[str, "Node"],
    parent_dimension_links: list[DimensionLink] | None = None,
    pending_link_roles: set[tuple[str, str | None]] | None = None,
    filter_parents: set[str] | None = None,
    filter_graph: "FilterGraph | None" = None,
) -> tuple[set[str], list[RequiredDimension]]:
    """
    Pure resolution of required_dimensions strings against pre-fetched nodes.

    Required dimensions can be specified as:
    - Full path: "dimensions.date.dateint"            -> look up in dim_nodes, find column
    - Role-qualified full path: "dimensions.date.dateint[created_date]"
      -> same, plus the role must resolve to a dimension link reachable
         from one of the node's direct parents
    - Short name: "status"                            -> find in parent_columns

    Called by find_required_dimensions (after its DB fetch) and by the bulk
    deployment validator (with its batch-prefetched _all_dim_nodes cache).

    `pending_link_roles` holds (dimension node name, role) pairs for links a
    deployment declares but has not saved yet; they count as reachable too.

    Role validation only checks reachability from a direct (one-hop) parent.
    When `filter_graph` is given, a full path must instead pass
    can_filter_on from `filter_parents`.

    Returns:
        Tuple of (invalid dimension paths, resolved RequiredDimension rows)
    """
    invalid_required_dimensions: set[str] = set()
    resolved: list[RequiredDimension] = []

    parent_cols_by_name: dict[str, list[Column]] = {}
    for col in parent_columns:
        parent_cols_by_name.setdefault(col.name, []).append(col)

    parent_dimension_links = parent_dimension_links or []

    # Separate full paths from short names.
    # full_paths: {dim_node_name: [(full_path, col_name, role), ...]}
    full_paths: dict[str, list[tuple[str, str, str | None]]] = {}
    short_names: list[str] = []

    for required_dim in required_dimensions:
        # A role can itself contain `.` (e.g. a multi-hop role path), so the
        # full-path/short-name split is decided on the ref with any bracketed
        # role stripped off first -- testing the raw string would misroute a
        # bare short name like `status[a.b]` into the full-path branch.
        dim_part, _ = split_dimension_ref(required_dim)
        if SEPARATOR in dim_part:
            dim_ref = parse_dimension_ref(required_dim)
            full_paths.setdefault(dim_ref.node_name, []).append(
                (required_dim, dim_ref.column_name, dim_ref.role),
            )
        else:
            short_names.append(required_dim)

    for short_name in short_names:
        matches = parent_cols_by_name.get(short_name, [])
        if len(matches) == 1:
            # Resolves locally against metric's own parent.
            resolved.append(RequiredDimension(ref=short_name, dimension_id=None))
        else:
            # No match, or the same short name exists on more than one direct
            # parent -- ambiguous, so it must be qualified as `node.column`.
            invalid_required_dimensions.add(short_name)

    for dim_node_name, entries in full_paths.items():
        dim_node = dim_nodes.get(dim_node_name)
        if not dim_node or not dim_node.current:  # pragma: no cover
            for full_path, _, _ in entries:
                invalid_required_dimensions.add(full_path)
            continue

        dim_col_map = {col.name: col for col in dim_node.current.columns}
        # Roles that a direct parent can reach this dimension node through
        # (one hop only -- see docstring above).
        reachable_roles = {
            link.role
            for link in parent_dimension_links
            if link.dimension and link.dimension.name == dim_node_name
        } | {
            role
            for linked_name, role in (pending_link_roles or set())
            if linked_name == dim_node_name
        }
        for full_path, col_name, role in entries:
            if col_name not in dim_col_map:
                invalid_required_dimensions.add(full_path)
                continue
            if filter_graph is not None:
                if not can_filter_on(full_path, filter_parents or set(), filter_graph):
                    invalid_required_dimensions.add(full_path)
                    continue
            elif role is not None and role not in reachable_roles:
                # No link with this role reaches this node.
                invalid_required_dimensions.add(full_path)
                continue
            resolved.append(
                RequiredDimension(ref=full_path, dimension_id=dim_node.id),
            )

    return invalid_required_dimensions, resolved


async def find_required_dimensions(
    session: AsyncSession,
    required_dimensions: list[str],
    parent_columns: list[Column],
    parent_dimension_links: list[DimensionLink] | None = None,
    parent_names: set[str] | None = None,
) -> tuple[set[str], list[RequiredDimension]]:
    """
    Resolve required dimension paths into RequiredDimension rows.

    Fetches all needed dimension nodes in a single DB query, then delegates
    resolution to _resolve_required_dimensions. With `parent_names` (the
    nodes the metric reads from), each attribute must pass can_filter_on.

    Returns:
        Tuple of (invalid dimension paths, resolved RequiredDimension rows)
    """
    # Collect dim node names from full-path entries so we can batch-fetch them.
    # Strip any bracketed role first -- it can itself contain `.`, so testing
    # the raw string would misroute a bare short name like `status[a.b]` into
    # a (bogus) full-path node name.
    dim_node_names: set[str] = set()
    for required_dim in required_dimensions:
        dim_part, _ = split_dimension_ref(required_dim)
        if SEPARATOR in dim_part:
            dim_node_names.add(dim_part.rsplit(SEPARATOR, 1)[0])

    dim_nodes: dict[str, Node] = {}
    if dim_node_names:
        result = await session.execute(
            select(Node)
            .filter(Node.name.in_(dim_node_names))
            .options(
                selectinload(Node.current).options(
                    selectinload(NodeRevision.columns),
                ),
            ),
        )
        dim_nodes = {node.name: node for node in result.scalars().all()}

    filter_graph = None
    if parent_names is not None:
        _, parent_map = await find_upstream_node_names(session, sorted(parent_names))
        filter_graph = await load_filter_graph(
            session,
            {
                name: nodes_upstream(name, lambda child: parent_map.get(child, []))
                for name in parent_names
            },
            attribute_nodes(required_dimensions),
        )

    return _resolve_required_dimensions(
        required_dimensions,
        parent_columns,
        dim_nodes,
        parent_dimension_links,
        filter_parents=parent_names,
        filter_graph=filter_graph,
    )


def attribute_nodes(attributes: list[str]) -> set[str]:
    """Node names of `node.column` attributes."""
    names: set[str] = set()
    for attribute in attributes:
        dim_part, _ = split_dimension_ref(attribute)
        if SEPARATOR in dim_part:
            names.add(dim_part.rsplit(SEPARATOR, 1)[0])
    return names


def nodes_upstream(name: str, parents: Callable[[str], list[str]]) -> set[str]:
    """Every node upstream of `name`."""
    seen: set[str] = set()
    stack = [name]
    while stack:
        for parent in parents(stack.pop()):
            if parent not in seen:
                seen.add(parent)
                stack.append(parent)
    return seen


@dataclass(frozen=True)
class FilterLink:
    """A join link from a node to a dimension node."""

    dimension: str
    role: str | None
    # Dimension columns the join keys match.
    keys: frozenset[str] = frozenset()


@dataclass
class FilterGraph:
    """The links and columns SQL builds use to filter a metric's parents."""

    # Parent name -> every node upstream of it.
    upstream: dict[str, set[str]] = field(default_factory=dict)
    # Node name -> its join links.
    links: dict[str, set[FilterLink]] = field(default_factory=dict)
    # Node name -> referenced (dimension, column) pairs.
    references: dict[str, set[tuple[str, str]]] = field(default_factory=dict)


def can_filter_on(attribute: str, parents: set[str], graph: FilterGraph) -> bool:
    """
    Whether SQL builds can filter every parent on a `node.column[role]`.

    For each parent, in the order resolve_dimensions tries them:
    1. The parent's own column, without a role.
    2. Join links from the parent to the node, then between dimension
       nodes, never returning to a node already on the path. Paths are found
       with advance_join_paths, the step the builder's path loader uses.
       Without a role any path counts. With a role, the roles along one path,
       joined by `->`, must equal it; links without a role are skipped.
       Otherwise SQL builds use another path.
    3. A parent column that references the attribute, under any role.
    4. A join link on a node upstream of the parent whose keys include the
       attribute, without a role. SQL builds then filter on the key column
       upstream.
    """
    dim_part, role = split_dimension_ref(attribute)
    node, column = dim_part.rsplit(SEPARATOR, 1)
    return bool(parents) and all(
        _parent_can_filter(parent, node, column, role, graph) for parent in parents
    )


def _parent_can_filter(
    parent: str,
    node: str,
    column: str,
    role: str | None,
    graph: FilterGraph,
) -> bool:
    """can_filter_on for one parent."""
    if node == parent and role is None:
        return True
    roles = join_role_paths(graph.links, parent, node)
    if roles:
        return role is None or role in roles
    if (node, column) in graph.references.get(parent, set()):
        return True
    return role is None and any(
        link.dimension == node and f"{node}{SEPARATOR}{column}" in link.keys
        for name in graph.upstream.get(parent, set())
        for link in graph.links.get(name, set())
    )


def join_role_paths(
    links: dict[str, set[FilterLink]],
    start: str,
    target: str,
) -> set[str]:
    """
    Role paths of the join paths from `start` to `target`, written as SQL builds
    write them. Uses the same search step as the builder's path loader.
    """
    names = [start]
    ids = {start: 0}

    def node_id(name: str) -> int:
        if name not in ids:
            ids[name] = len(names)
            names.append(name)
        return ids[name]

    found: dict[tuple[int, str, str], list[int]] = {}
    next_link_id = count()
    frontier = [JoinPathState(0, 0, [], "", [])]
    while frontier:
        rows = [
            JoinLinkRow(
                current,
                next(next_link_id),
                node_id(link.dimension),
                link.dimension,
                link.role or "",
            )
            for current in dict.fromkeys(item.node_id for item in frontier)
            for link in links.get(names[current], set())
        ]
        frontier = advance_join_paths(frontier, rows, {target}, found)
    return {role for _, _, role in found}


def link_keys(
    node: str,
    dimension: str,
    join_sql: str | None,
    join_type: JoinType | None,
) -> frozenset[str]:
    """Dimension columns a link's join keys match, as `node.column`."""
    mapping = DimensionLink.build_foreign_key_mapping(
        join_sql,  # type: ignore
        join_type or JoinType.LEFT,
        node,
        dimension,
    )
    return frozenset(column.identifier() for column in mapping)


async def load_filter_graph(
    session: AsyncSession,
    upstream: dict[str, set[str]],
    targets: set[str],
    declared: FilterGraph | None = None,
) -> FilterGraph:
    """
    Load what can_filter_on reads for these parents.

    `upstream` maps each parent to every node upstream of it, and `targets`
    are the attribute nodes. Saved links and references are merged with
    `declared`, which holds links not saved yet. Join keys are read only
    for upstream links to a target. Links load one query per hop.
    """
    declared = declared or FilterGraph()
    graph = FilterGraph(
        upstream=upstream,
        links={name: set(links) for name, links in declared.links.items()},
        references={name: set(refs) for name, refs in declared.references.items()},
    )
    parents = set(upstream)
    ancestors = {name for names in upstream.values() for name in names}
    for name, refs in (await column_references(session, parents)).items():
        graph.references.setdefault(name, set()).update(refs)

    loaded: set[str] = set()
    walked: set[str] = set()
    frontier = parents
    # Upstream links load but are not walked.
    unloaded = parents | ancestors
    while frontier:
        if unloaded:
            saved = await _saved_filter_links(session, unloaded, ancestors, targets)
            for name, links in saved.items():
                graph.links.setdefault(name, set()).update(links)
            loaded |= unloaded
        walked |= frontier
        frontier = {
            link.dimension for name in frontier for link in graph.links.get(name, set())
        } - walked
        unloaded = frontier - loaded
    return graph


async def column_references(
    session: AsyncSession,
    node_names: set[str],
) -> dict[str, set[tuple[str, str]]]:
    """(dimension node, column) pairs that each node's current columns reference."""
    dimension = aliased(Node)
    rows = await session.execute(
        select(Node.name, dimension.name, Column.name, Column.dimension_column)
        .join(
            NodeRevision,
            and_(
                NodeRevision.node_id == Node.id,
                NodeRevision.version == Node.current_version,
            ),
        )
        .join(Column, Column.node_revision_id == NodeRevision.id)
        .join(dimension, dimension.id == Column.dimension_id)
        .where(Node.name.in_(node_names)),
    )
    references: dict[str, set[tuple[str, str]]] = {}
    for node_name, dimension_name, column_name, dimension_column in rows:
        references.setdefault(node_name, set()).add(
            (dimension_name, strip_role_suffix(dimension_column or column_name)),
        )
    return references


async def _saved_filter_links(
    session: AsyncSession,
    node_names: set[str],
    ancestors: set[str],
    targets: set[str],
) -> dict[str, set[FilterLink]]:
    """Each node's current join links."""
    dimension = aliased(Node)
    rows = await session.execute(
        select(
            Node.name,
            dimension.name,
            DimensionLink.role,
            DimensionLink.join_sql,
            DimensionLink.join_type,
        )
        .join(
            NodeRevision,
            and_(
                NodeRevision.node_id == Node.id,
                NodeRevision.version == Node.current_version,
            ),
        )
        .join(DimensionLink, DimensionLink.node_revision_id == NodeRevision.id)
        .join(dimension, dimension.id == DimensionLink.dimension_id)
        .where(Node.name.in_(node_names)),
    )
    links: dict[str, set[FilterLink]] = {}
    for node_name, dimension_name, role, join_sql, join_type in rows:
        keys: frozenset[str] = frozenset()
        if node_name in ancestors and dimension_name in targets:
            keys = link_keys(node_name, dimension_name, join_sql, join_type)
        links.setdefault(node_name, set()).add(
            FilterLink(dimension_name, role or None, keys),
        )
    return links


async def resolve_downstream_references(
    session: AsyncSession,
    node_revision: NodeRevision,
    current_user: User,
    save_history: Callable,
) -> list[NodeRevision]:
    """
    Find all node revisions with missing parent references to `node` and resolve them
    """
    from datajunction_server.internal.validation import validate_node_data

    missing_parents = (
        (
            await session.execute(
                select(MissingParent).where(MissingParent.name == node_revision.name),
            )
        )
        .scalars()
        .all()
    )
    newly_valid_nodes = []
    for missing_parent in missing_parents:
        missing_parent_links = (
            (
                await session.execute(
                    select(NodeMissingParents).where(
                        NodeMissingParents.missing_parent_id == missing_parent.id,
                    ),
                )
            )
            .scalars()
            .all()
        )
        for (
            link
        ) in missing_parent_links:  # Remove from missing parents and add to parents
            downstream_node_id = link.referencing_node_id
            downstream_node_revision = (
                (
                    await session.execute(
                        select(NodeRevision)
                        .where(NodeRevision.id == downstream_node_id)
                        .options(
                            joinedload(NodeRevision.missing_parents),
                            joinedload(NodeRevision.parents),
                        ),
                    )
                )
                .unique()
                .scalar_one()
            )
            await session.refresh(node_revision, ["node"])
            await session.refresh(
                downstream_node_revision,
                ["parents", "missing_parents"],
            )
            # Compare by id, not Python identity: session.refresh returns a
            # different ORM instance than `node_revision.node` even when they
            # represent the same row, so an identity-based `in` check would
            # wrongly append and stage a duplicate NodeRelationship INSERT.
            if node_revision.node.id not in {  # pragma: no branch
                p.id for p in downstream_node_revision.parents
            }:
                downstream_node_revision.parents.append(node_revision.node)
            downstream_node_revision.missing_parents.remove(missing_parent)
            node_validator = await validate_node_data(
                data=downstream_node_revision,
                session=session,
            )
            event = None
            if downstream_node_revision.status != node_validator.status:
                event = status_change_history(
                    downstream_node_revision,
                    downstream_node_revision.status,
                    node_validator.status,
                    parent_node=node_revision.name,
                    current_user=current_user,
                )

            downstream_node_revision.status = node_validator.status

            await session.refresh(downstream_node_revision, ["columns"])
            downstream_node_revision.columns = node_validator.columns
            if node_validator.status == NodeStatus.VALID:
                newly_valid_nodes.append(downstream_node_revision)
            session.add(downstream_node_revision)
            if event:
                await save_history(event=event, session=session)
            await session.commit()
            await session.refresh(downstream_node_revision)

        await session.delete(missing_parent)  # Remove missing parent reference to node
    return newly_valid_nodes


def dedupe_cube_elements(columns: list[Column]) -> list[Column]:
    """
    Dedupe cube element columns by column id.

    `cube_elements` is the many-to-many between a cube revision and the columns
    it references (its metric columns and dimension attributes), keyed by
    `(cube_id, column.id)`. A dimension attribute referenced under multiple roles
    resolves to the same Column object, so appending it once per role would
    violate `pk_cube`. Roles live on the cube's node_columns, so deduping here is
    lossless.
    """
    deduped: dict = {}
    for col in columns:
        deduped.setdefault(col.id if col.id is not None else id(col), col)
    return list(deduped.values())


async def validate_cube(
    session: AsyncSession,
    metric_names: list[str],
    dimension_names: list[str],
    require_dimensions: bool = False,
) -> tuple[
    list[Column],
    list[Node],
    list[Node],
    list[Column],
    list[str | None],
    Catalog | None,
]:
    """
    Validate that a set of metrics and dimensions can be built together.

    Returns (in order): metric columns, metric nodes, dimension nodes, resolved
    dimension columns, the "[role]"-or-None suffix for each resolved dimension
    column (kept strictly 1:1 with it), and the shared catalog. The roles are
    returned alongside their columns rather than recomputed by position because
    unresolvable references are skipped below — a position-based alignment would
    drop or mis-bind roles on cubes that reach one dimension via two FK roles.
    """
    metric_nodes = await check_metrics_exist(session, metric_names)
    catalogs = [metric.current.catalog for metric in metric_nodes]
    catalog = catalogs[0] if catalogs else None

    # Verify that the provided metrics are metric nodes
    metrics: list[Column] = [metric.current.columns[0] for metric in metric_nodes]
    for metric in metrics:
        await session.refresh(metric, ["node_revision"])
    if not metrics:
        raise DJInvalidInputException(
            message=("At least one metric is required"),
            http_status_code=http.client.UNPROCESSABLE_ENTITY,
        )
    non_metrics = [metric for metric in metric_nodes if metric.type != NodeType.METRIC]
    if non_metrics:
        message = (
            f"Node {non_metrics[0].name} of type {non_metrics[0].type} "  # type: ignore
            f"cannot be added to a cube."
            + " Did you mean to add a dimension attribute?"
            if non_metrics[0].type == NodeType.DIMENSION  # type: ignore
            else ""
        )
        raise DJInvalidInputException(
            message=message,
            errors=[DJError(code=ErrorCode.NODE_TYPE_ERROR, message=message)],
            http_status_code=http.client.UNPROCESSABLE_ENTITY,
        )

    dimension_attributes, dimension_nodes = await check_dimension_attributes_exist(
        session,
        dimension_names,
    )
    dimension_mapping: dict[str, Node] = {
        f"{attr.node_name}{SEPARATOR}{attr.column_name}": dimension_nodes[
            attr.node_name
        ]
        for attr in dimension_attributes
    }
    dimensions: list[Column] = []
    dimension_roles: list[str | None] = []
    for attr in dimension_attributes:
        dimension_node = dimension_mapping[
            f"{attr.node_name}{SEPARATOR}{attr.column_name}"
        ]
        columns = {col.name: col for col in dimension_node.current.columns}  # type: ignore

        column_name_without_role = attr.column_name
        match = re.fullmatch(COLUMN_NAME_REGEX, attr.column_name)
        if match:
            column_name_without_role = match.groups()[0]

        if column_name_without_role in columns:  # pragma: no cover
            dimensions.append(columns[column_name_without_role])
            dimension_roles.append(f"[{attr.role}]" if attr.role else None)

    if require_dimensions and not dimensions:
        raise DJInvalidInputException(  # pragma: no cover
            message="At least one dimension is required",
        )

    if len(set(catalogs)) > 1:
        raise DJInvalidInputException(
            message=(
                f"Metrics and dimensions cannot be from multiple catalogs: {catalogs}"
            ),
        )

    if len(set(catalogs)) < 1:  # pragma: no cover
        raise DJInvalidInputException(
            message=("Metrics and dimensions must be part of a common catalog"),
        )

    # Only validate shared dimensions if dimensions were actually requested
    # This avoids expensive dimension graph loading when dimensions=[]
    if dimension_names:
        await validate_shared_dimensions(
            session,
            metric_nodes,
            dimension_names,
        )
    return (
        metrics,
        metric_nodes,
        list(dimension_nodes.values()),
        dimensions,
        dimension_roles,
        catalog,
    )


async def check_metrics_exist(session: AsyncSession, metrics: list[str]) -> list[Node]:
    """
    Check that the list of metrics are valid metric nodes and return them.
    """
    metrics_sorting_order = {val: idx for idx, val in enumerate(metrics)}
    metric_nodes: list[Node] = sorted(
        await Node.get_by_names(
            session,
            metrics,
            options=[
                joinedload(Node.current).options(
                    selectinload(NodeRevision.columns),
                    joinedload(NodeRevision.catalog),
                    selectinload(NodeRevision.parents),
                ),
            ],
            include_inactive=False,
        ),
        key=lambda x: metrics_sorting_order.get(x.name, 0),
    )
    if len(metric_nodes) != len(metrics):
        not_found = set(metrics) - {metric.name for metric in metric_nodes}
        message = f"The following metric nodes were not found: {', '.join(not_found)}"
        raise DJNodeNotFound(
            message,
            errors=[DJError(code=ErrorCode.UNKNOWN_NODE, message=message)],
        )
    return metric_nodes


async def check_dimension_attributes_exist(
    session: AsyncSession,
    dimensions: list[str],
) -> tuple[list[FullColumnName], dict[str, Node]]:
    """
    Verify that the provided dimension attributes exist
    """
    dimension_attributes: list[FullColumnName] = [
        FullColumnName(dimension_attribute) for dimension_attribute in dimensions
    ]
    dimension_node_names = [attr.node_name for attr in dimension_attributes]
    dimension_nodes: dict[str, Node] = {
        node.name: node
        for node in await Node.get_by_names(
            session,
            dimension_node_names,
            options=[
                joinedload(Node.current).options(
                    selectinload(NodeRevision.columns).options(
                        selectinload(Column.node_revision),
                    ),
                    defer(NodeRevision.query_ast),
                ),
            ],
        )
    }
    missing_dimensions = set(dimension_node_names) - set(dimension_nodes)
    if missing_dimensions:  # pragma: no cover
        missing_dimension_attributes = ", ".join(  # pragma: no cover
            [
                attr.name
                for attr in dimension_attributes
                if attr.node_name in missing_dimensions
            ],
        )
        message = (
            f"Please make sure that `{missing_dimension_attributes}` "
            "is a dimensional attribute."
        )
        raise DJInvalidInputException(  # pragma: no cover
            message,
            errors=[DJError(code=ErrorCode.INVALID_DIMENSION, message=message)],
        )
    return dimension_attributes, dimension_nodes


async def get_history(
    session: AsyncSession,
    entity_type: EntityType,
    entity_name: str,
    offset: int,
    limit: int,
):
    """
    Get the history for a given entity type and name
    """
    return (
        (
            await session.execute(
                select(History)
                .where(History.entity_type == entity_type)
                .where(History.entity_name == entity_name)
                .offset(offset)
                .limit(limit)
                .order_by(History.created_at.desc()),
            )
        )
        .scalars()
        .all()
    )


def validate_orderby(
    orderby: list[str],
    metrics: list[str],
    dimension_attributes: list[str],
):
    """
    Validate that all elements in an order by match a metric or dimension attribute
    """
    invalid_orderbys = []
    for orderby_element in orderby:
        if orderby_element.split(" ")[0] not in metrics + dimension_attributes:
            invalid_orderbys.append(orderby_element)
    if invalid_orderbys:
        raise DJInvalidInputException(
            message=(
                f"Columns {invalid_orderbys} in order by clause must also be "
                "specified in the metrics or dimensions"
            ),
        )


async def find_existing_cube(
    session: AsyncSession,
    metric_columns: list[Column],
    dimension_columns: list[Column],
    dimension_roles: list[str | None] | None = None,
    materialized: bool = True,
) -> NodeRevision | None:
    """
    Find an existing cube with these metrics and dimensions, if any.
    If `materialized` is set, it will only look for materialized cubes.
    """
    element_names = [col.name for col in (metric_columns + dimension_columns)]
    required_dimension_refs = (
        {
            col.full_name() + (role or "")
            for col, role in zip(dimension_columns, dimension_roles)
        }
        if dimension_roles is not None
        else set()
    )
    statement = (
        select(Node)
        .join(
            NodeRevision,
            onclause=(
                and_(
                    (Node.id == NodeRevision.node_id),
                    (Node.current_version == NodeRevision.version),
                )
            ),
        )
        .options(
            joinedload(Node.current).options(
                joinedload(NodeRevision.materializations),
                joinedload(NodeRevision.availability),
                selectinload(NodeRevision.columns),
            ),
        )
    )
    for name in element_names:
        statement = statement.filter(
            NodeRevision.cube_elements.any(Column.name == name),  # type: ignore
        )

    existing_cubes = (await session.execute(statement)).unique().scalars().all()
    for cube in existing_cubes:
        if required_dimension_refs:
            cube_element_refs = {col.cube_element_name for col in cube.current.columns}
            if not required_dimension_refs.issubset(cube_element_refs):
                continue
        if not materialized or (  # pragma: no cover
            materialized and cube.current.materializations and cube.current.availability
        ):
            return cube.current

    return None


async def resolve_engine(
    session: AsyncSession,
    node: Node,
    engine_name: str | None = None,
    engine_version: str | None = None,
    dialect: Dialect | None = None,
) -> Engine:
    """
    Resolve which engine should be used to execute node SQL.
    The engine is determined in the following order:
      1. If an explicit engine name and version are provided, fetch that engine
         from the database.
      2. Otherwise, fall back to the first engine associated with the node's
         catalog that matches the requested dialect.
      3. Validate that the chosen engine is available for the given node.
    """
    available_engines = [
        eng
        for eng in node.current.catalog.engines
        if not dialect or eng.dialect == dialect
    ]
    engine = (
        await get_engine(session, engine_name, engine_version)  # type: ignore
        if engine_name
        else available_engines[0]
    )
    if engine not in available_engines:
        raise DJInvalidInputException(  # pragma: no cover
            f"The selected engine is not available for the node {node.name}. "
            f"Available engines include: {', '.join(engine.name for engine in available_engines)}",
        )
    return engine


async def query_event_stream(
    query: QueryWithResults,
    request_headers: dict[str, str] | None,
    query_service_client: QueryServiceClient,
    columns: list[Column],
    request,
    timeout: float = 0.0,
    stream_delay: float = 0.5,
    retry_timeout: int = 5000,
    warnings: list[DJWarning] | None = None,
):
    """
    A generator of events from a query submitted to the query service.

    Build warnings, if any, come first as a `warnings` event.
    """
    starting_time = time.time()
    # Start with query and query_next as the initial state of the query
    query_prev = query_next = query
    query_id = query_prev.id
    if warnings:
        yield {
            "event": "warnings",
            "id": uuid.uuid4(),
            "retry": retry_timeout,
            "data": json.dumps(
                [warning.model_dump(mode="json") for warning in warnings],
            ),
        }
    _logger.info("sending initial event to the client for query %s", query_id)
    yield {
        "event": "message",
        "id": uuid.uuid4(),
        "retry": retry_timeout,
        "data": json.dumps(query.json()),
    }
    # Continuously check the query until it's complete
    while not timeout or (time.time() - starting_time < timeout):
        # Check if the client closed the connection
        if await request.is_disconnected():  # pragma: no cover
            _logger.error("connection closed by the client")
            break

        # Check the current state of the query
        query_next = await query_service_client.get_query(  # type: ignore # pragma: no cover
            query_id=query_id,
            request_headers=request_headers,
        )
        if query_next.state in END_JOB_STATES:  # pragma: no cover
            _logger.info(  # pragma: no cover
                "query end state detected (%s), sending final event to the client",
                query_next.state,
            )
            if query_next.results.root:  # pragma: no cover
                query_next.results.root[0].columns = columns or []
            yield {
                "event": "message",
                "id": uuid.uuid4(),
                "retry": retry_timeout,
                "data": json.dumps(query_next.json()),
            }
            _logger.info("connection closed by the server")
            break
        if query_prev != query_next:  # pragma: no cover
            _logger.info(
                "query information has changed, sending an event to the client",
            )
            yield {
                "event": "message",
                "id": uuid.uuid4(),
                "retry": retry_timeout,
                "data": json.dumps(query_next.json()),
            }

            query = query_next
        await asyncio.sleep(stream_delay)  # pragma: no cover


def assemble_column_metadata(
    column: ast.Column,
    use_semantic_metadata: bool = False,
) -> ColumnMetadata:
    """
    Extract column metadata from AST
    """
    has_semantic_entity = hasattr(column, "semantic_entity") and column.semantic_entity

    if use_semantic_metadata and has_semantic_entity:
        column_name = column.semantic_entity.split(SEPARATOR)[-1]  # type: ignore
        node_name = SEPARATOR.join(column.semantic_entity.split(SEPARATOR)[:-1])  # type: ignore
    else:
        column_name = getattr(column.name, "name", None)
        node_name = (
            from_amenable_name(column.table.alias_or_name.name)  # type: ignore
            if hasattr(column, "table") and column.table
            else None
        )

    metadata = ColumnMetadata(
        name=column.alias_or_name.name,
        type=str(column.type),
        column=column_name,
        node=node_name,
        semantic_entity=column.semantic_entity
        if hasattr(column, "semantic_entity")
        else None,
        semantic_type=column.semantic_type
        if hasattr(column, "semantic_type")
        else None,
    )
    return metadata


async def save_history(
    event: History,
    session: AsyncSession,
    _notify: Callable[
        [History],
        None,
    ],  # To use a different notify function, inject a get_notifier dependency
) -> None:
    """
    Save a history event to the database and process notifications
    """
    session.add(event)
    await session.commit()
    _notify(event)


async def get_save_history(notify: Callable = Depends(get_notifier)) -> Callable:
    """
    Dependency provider for save history function
    """

    async def save_history_with_notify(event, session):
        await save_history(event, session, notify)

    return save_history_with_notify


def get_materialization_info(
    query_service_client: QueryServiceClient,
    node: Node,
    include_all_revisions: bool,
    show_inactive: bool,
    request_headers: dict[str, str] | None = None,
):
    """Get materializations for a node

    If include_all_revisions=true this pulls materializations for each node revision and
    combines them into a single list. Otherwise it just pulls materializations for the
    current node revision.

    If show_inactive=true this also returns materializations with a deactivated_at timestamp
    """
    return (
        [
            materialization
            for node_revision in node.revisions  # type: ignore
            for materialization in get_node_revision_materialization(
                query_service_client=query_service_client,
                node_revision=node_revision,
                show_inactive=show_inactive,
                request_headers=request_headers,
            )
        ]
        if include_all_revisions
        else get_node_revision_materialization(
            query_service_client=query_service_client,
            node_revision=node.current,  # type: ignore
            show_inactive=show_inactive,
            request_headers=request_headers,
        )
    )


def get_node_revision_materialization(
    query_service_client: QueryServiceClient,
    node_revision: NodeRevision,
    show_inactive: bool,
    request_headers: dict[str, str] | None = None,
) -> list[MaterializationConfigInfoUnified]:
    """Merge in materialization info from the query service for a node revision"""
    materializations = []
    for materialization in node_revision.materializations:
        if not materialization.deactivated_at or show_inactive:
            info = query_service_client.get_materialization_info(
                node_revision.name,
                node_revision.version,
                node_revision.type,
                materialization.name,
                request_headers=request_headers,
            )
            if materialization.strategy != MaterializationStrategy.INCREMENTAL_TIME:
                info.urls = [info.urls[0]] if info.urls else []
            materialization_config_output = MaterializationConfigOutput.model_validate(
                materialization,
            )
            # Use workflow_urls from V3 config if available, otherwise fall back to
            # query service urls
            config_dict = materialization_config_output.config
            if config_dict.get("workflow_urls"):  # pragma: no cover
                info.urls = config_dict["workflow_urls"]
            materializations.append(
                MaterializationConfigInfoUnified(
                    **materialization_config_output.model_dump(),
                    **info.model_dump(),
                    node_version=node_revision.version,
                ),
            )
    return materializations
