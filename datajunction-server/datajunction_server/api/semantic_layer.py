"""
Semantic-layer REST API implementing the sl-db-engine-spec over DataJunction
cubes (each cube = a "view").

Spec: https://github.com/betodealmeida/sl-db-engine-spec/blob/main/server/SPEC.md

DJ generates SQL; the client executes it. This router does NOT run queries —
``/views/{view}/sql`` returns physical SQL + dialect for the client to run;
``/views/list`` and ``/views/{view}`` are metadata. Mounted at ``/semantic``.
"""

import logging
from typing import Any, cast

from fastapi import Body, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field  # pylint: disable=no-name-in-module
from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.user import User
from datajunction_server.errors import DJException
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.access.authorization import (
    AccessChecker,
    get_access_checker,
)
from datajunction_server.internal.namespaces import (
    get_git_info_for_namespace,
    get_parent_namespaces,
    resolve_git_info_from_map,
)
from datajunction_server.internal.sql import (
    build_row_count_sql,
    generate_dimensions_sql,
    generate_metrics_sql,
)
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.access import ResourceAction, ResourceType
from datajunction_server.utils import get_current_user, get_session

logger = logging.getLogger(__name__)

# SecureAPIRouter (vs a plain APIRouter) attaches the DJHTTPBearer dependency,
# which validates the JWT/cookie and populates ``request.state.user`` so that
# ``get_current_user`` resolves — matching every other secured router
# (cubes, nodes, …).
router = SecureAPIRouter(prefix="/semantic", tags=["semantic-layer"])

# Default row limit applied when the caller doesn't specify one, to bound result size.
DEFAULT_ROW_LIMIT = 10000
# An explicit limit above this is rejected (400).
MAX_ROW_LIMIT = 100000

DIMENSION_FALLBACK_ARROW_TYPE_NAME = "utf8"
METRIC_FALLBACK_ARROW_TYPE_NAME = "floating"

DJ_TO_ARROW_TYPE_NAMES = {
    "array": "list",
    "bigint": "int",
    "binary": "binary",
    "boolean": "bool",
    "char": "utf8",
    "date": "date",
    "decimal": "decimal",
    "double": "floating",
    "float": "floating",
    "int": "int",
    "integer": "int",
    "list": "list",
    "long": "int",
    "map": "map",
    "smallint": "int",
    "string": "utf8",
    "struct": "struct",
    "time": "time",
    "timestamp": "timestamp",
    "timestamptz": "timestamp",
    "tinyint": "int",
    "varchar": "utf8",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _problem(status_code: int, detail: str) -> JSONResponse:
    """Build an error response matching the spec's ``{status_code, detail}`` shape."""
    return JSONResponse(
        status_code=status_code,
        content={"status_code": status_code, "detail": detail},
    )


def _arrow_type_name(dj_type: Any) -> str | None:
    """Map a DJ column type to the Arrow JSON type object's ``name`` value."""
    if not dj_type:
        return None

    type_name = str(dj_type).lower().split("(", maxsplit=1)[0].strip()
    type_name = type_name.split("<", maxsplit=1)[0].strip()

    return DJ_TO_ARROW_TYPE_NAMES.get(type_name)


def _cube_column_type_map(cube: NodeRevision) -> dict[str, str | None]:
    """Return Arrow type names for the metric/dimension ids exposed by a cube."""
    return {
        column.cube_element_name: _arrow_type_name(column.type)
        for column in cube.columns
    }


def _cube_metadata_map(cube: NodeRevision) -> dict[str, dict[str, str | None]]:
    """Return column metadata for the metric/dimension ids exposed by a cube."""
    return {
        column.cube_element_name: {"display_name": column.display_name}
        for column in cube.columns
    }


def _generated_column_arrow_type_name(column: Any) -> str:
    """Return the semantic-layer Arrow type name for a generated SQL column."""
    arrow_type = _arrow_type_name(getattr(column, "type", None))
    if arrow_type:
        return arrow_type

    semantic_type = str(getattr(column, "semantic_type", "") or "").lower()
    if semantic_type == "dimension":
        return DIMENSION_FALLBACK_ARROW_TYPE_NAME
    return METRIC_FALLBACK_ARROW_TYPE_NAME


def _metrics_payload(cube: NodeRevision) -> list["MetricInfo"]:
    """Spec ``metrics`` list. ``definition`` is display-only."""
    type_by_name = _cube_column_type_map(cube)
    metadata_by_name = _cube_metadata_map(cube)
    return [
        MetricInfo(
            id=metric_name,
            name=metric_name.split(".")[-1],
            type=type_by_name.get(metric_name) or METRIC_FALLBACK_ARROW_TYPE_NAME,
            definition=metric_name,
            description=None,
            aggregation="OTHER",
            metadata=MetricsMetadata(
                display_name=metadata_by_name.get(metric_name, {}).get(
                    "display_name",
                    "",
                ),
            ),
        )
        for metric_name in cube.cube_node_metrics
    ]


def _dimensions_payload(cube: NodeRevision) -> list["DimensionInfo"]:
    """Spec ``dimensions`` list. Grain detection is deferred."""
    type_by_name = _cube_column_type_map(cube)
    metadata_by_name = _cube_metadata_map(cube)
    return [
        DimensionInfo(
            id=dim_ref,
            name=dim_ref.split(".")[-1],
            type=type_by_name.get(dim_ref) or DIMENSION_FALLBACK_ARROW_TYPE_NAME,
            definition=dim_ref,
            description=None,
            grain=None,
            metadata=DimensionMetadata(
                display_name=metadata_by_name.get(dim_ref, {}).get("display_name", ""),
            ),
        )
        for dim_ref in cube.cube_node_dimensions
    ]


def _view_payload(cube: NodeRevision) -> "ViewDetail":
    """Serialize a cube as a full semantic view (spec ``/views/{view}`` detail).

    Reads ``cube_node_metrics``/``cube_node_dimensions`` (metric names + dimension
    refs) off the cube's current revision — the only cube data the spec payload
    needs. Everything else is a coarse default filled in above.
    """
    return ViewDetail(
        name=cube.name,
        display_name=cube.display_name or None,
        uid=cube.name,
        features=[],  # no optional spec features for now
        dimensions=_dimensions_payload(cube),
        metrics=_metrics_payload(cube),
    )


# ---------------------------------------------------------------------------
# Filter / order translation (spec QueryPayload -> DJ build_metrics_sql args)
# ---------------------------------------------------------------------------

# Comparison operators accepted for filters. These cover the portable operators
# used by the semantic-layer spec; pattern matching remains unsupported.
_ALLOWED_OPERATORS = frozenset(
    {
        "=",
        "!=",
        ">",
        "<",
        ">=",
        "<=",
        "IN",
        "NOT IN",
        "BETWEEN",
        "IS NULL",
        "IS NOT NULL",
    },
)
_NULLARY_OPERATORS = frozenset({"IS NULL", "IS NOT NULL"})
_COLLECTION_OPERATORS = frozenset({"IN", "NOT IN"})


def _quote_value(value: Any) -> str:
    """Render a scalar Python value as a SQL literal via sqlglot, which handles
    quote-escaping and numeric/bool/NULL rendering for the scalars the client
    sends (strings, numbers, bools, null)."""
    # Imported lazily so the optional ``sqlglot`` extra isn't required at app
    # import time — the rest of the codebase keeps sqlglot off the eager import
    # path (e.g. transpilation.py imports it inside functions).
    import sqlglot

    return sqlglot.exp.convert(value).sql(dialect="spark")


def _filter_to_sql(flt: "FilterPayload") -> str:
    """Spec FilterPayload -> ``<id> <op> <literal>``. Column is validated against
    the cube by the caller; value is quoted. No arbitrary SQL reaches DJ."""
    op = flt.operator.upper()
    if op not in _ALLOWED_OPERATORS:
        raise DJException(
            message=f"Unsupported filter operator: {flt.operator}",
            http_status_code=400,
        )
    if not flt.column:
        raise DJException(
            message=f"Filter with operator {flt.operator} requires a column",
            http_status_code=400,
        )
    if op in _NULLARY_OPERATORS:
        return f"{flt.column} {op}"
    if op in _COLLECTION_OPERATORS:
        if not isinstance(flt.value, (list, tuple, set, frozenset)) or not flt.value:
            raise DJException(
                message=f"Filter operator {flt.operator} requires a non-empty list",
                http_status_code=400,
            )
        values = ", ".join(_quote_value(value) for value in flt.value)
        return f"{flt.column} {op} ({values})"
    if op == "BETWEEN":
        if not isinstance(flt.value, (list, tuple)) or len(flt.value) != 2:
            raise DJException(
                message="Filter operator BETWEEN requires exactly two values",
                http_status_code=400,
            )
        start, end = (_quote_value(value) for value in flt.value)
        return f"{flt.column} BETWEEN {start} AND {end}"
    return f"{flt.column} {op} {_quote_value(flt.value)}"


# ---------------------------------------------------------------------------
# Request schemas
# ---------------------------------------------------------------------------


class OrderPayload(BaseModel):
    """A single ORDER BY clause."""

    by: str
    direction: str = "ASC"


class FilterPayload(BaseModel):
    """A WHERE/HAVING filter."""

    type: str = "WHERE"
    column: str | None = None
    operator: str = "="
    value: Any = None


class QueryPayload(BaseModel):
    """The query specification."""

    metrics: list[str] = Field(default_factory=list)
    dimensions: list[str] = Field(default_factory=list)
    filters: list[FilterPayload] = Field(default_factory=list)
    order: list[OrderPayload] = Field(default_factory=list)
    limit: int | None = None
    # Accepted so we can explicitly reject it (DJ SQL generation has no offset);
    # silently ignoring it would return page 1 for every page.
    offset: int | None = None


class QueryRequest(BaseModel):
    """Body for POST /views/{view_name}/sql (backs the spec's /query)."""

    additional_configuration: dict[str, Any] = Field(default_factory=dict)
    query: QueryPayload


class ListViewsRequest(BaseModel):
    """Discovery options shared by the semantic listing endpoints."""

    runtime_configuration: dict[str, Any] = Field(default_factory=dict)
    path: list[str | None] | None = None


class DiscoverRequest(BaseModel):
    """A positional selection in the view discovery hierarchy."""

    runtime_configuration: dict[str, Any] = Field(default_factory=dict)
    path: list[str | None] = Field(default_factory=list)


class DiscoverLevel(BaseModel):
    id: str
    display_name: str


class DiscoverResponse(BaseModel):
    level: DiscoverLevel
    values: list[str | None]
    next_level: DiscoverLevel | None = None


class ValuesRequest(BaseModel):
    """Body for POST /views/{view_name}/values."""

    additional_configuration: dict[str, Any] = Field(default_factory=dict)
    dimension: str
    filters: list[FilterPayload] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------------------


class MetricsMetadata(BaseModel):
    """Metric-specific field metadata"""

    display_name: str


class MetricInfo(BaseModel):
    """A metric exposed by a semantic view."""

    id: str
    name: str
    type: str
    definition: str
    description: str | None
    aggregation: str
    metadata: MetricsMetadata


class DimensionMetadata(BaseModel):
    """Dimension-specific field metadata"""

    display_name: str


class DimensionInfo(BaseModel):
    """A dimension exposed by a semantic view."""

    id: str
    name: str
    type: str
    definition: str
    description: str | None
    grain: str | None
    metadata: DimensionMetadata


class ViewSummary(BaseModel):
    """Summary entry returned by ``/views/`` and ``/views/list``."""

    name: str
    display_name: str | None = None
    uid: str
    features: list[str]
    namespace: str | None = None
    branch: str | None = None


class ViewDetail(ViewSummary):
    """Full semantic view returned by ``/views/{view}``."""

    dimensions: list[DimensionInfo]
    metrics: list[MetricInfo]


class ColumnInfo(BaseModel):
    """A single output column of the generated SQL."""

    name: str
    type: str


class GeneratedSQLResponse(BaseModel):
    """Physical SQL + metadata returned by ``/views/{view}/sql``."""

    sql: str
    dialect: str
    columns: list[ColumnInfo]
    cube_name: str | None


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


async def _active_git_namespaces(
    session: AsyncSession,
    root: str | None = None,
) -> dict[str, NodeNamespace]:
    """Load location metadata, following Git links outside a root's name prefix."""
    active = NodeNamespace.deactivated_at.is_(None)
    if root is None:
        rows = await session.execute(select(NodeNamespace).where(active))
        return {namespace.namespace: namespace for namespace in rows.scalars()}

    namespaces: dict[str, NodeNamespace] = {}
    pending_prefixes = {root}
    pending_parents = {root}
    checked_prefixes: set[str] = set()
    checked_parents: set[str] = set()
    while pending_prefixes or pending_parents:
        predicates = [
            condition
            for prefix in pending_prefixes
            for condition in (
                NodeNamespace.namespace == prefix,
                NodeNamespace.namespace.startswith(f"{prefix}.", autoescape=True),
            )
        ]
        if pending_parents:
            predicates.append(NodeNamespace.parent_namespace.in_(pending_parents))
        rows = await session.execute(
            select(NodeNamespace).where(active, or_(*predicates)),
        )
        checked_prefixes.update(pending_prefixes)
        checked_parents.update(pending_parents)
        new_rows = [row for row in rows.scalars() if row.namespace not in namespaces]
        namespaces.update({row.namespace: row for row in new_rows})
        pending_parents = {
            row.namespace
            for row in new_rows
            if (row.git_branch or row.github_repo_path)
            and row.namespace not in checked_parents
        }
        pending_prefixes = {
            row.namespace
            for row in new_rows
            if row.namespace != root
            and not row.namespace.startswith(f"{root}.")
            and row.namespace not in checked_prefixes
        }

    # Git resolution also reads string ancestors and the direct FK parent of a
    # branch namespace. Load these as metadata after traversal so they cannot
    # expand the selected subtree or pull in unrelated cubes.
    ancestor_names = {
        ancestor for name in namespaces for ancestor in get_parent_namespaces(name)
    } - namespaces.keys()
    if ancestor_names:
        rows = await session.execute(
            select(NodeNamespace).where(
                active,
                NodeNamespace.namespace.in_(ancestor_names),
            ),
        )
        namespaces.update({row.namespace: row for row in rows.scalars()})

    parent_names = {
        row.parent_namespace
        for row in namespaces.values()
        if row.git_branch and row.parent_namespace
    } - namespaces.keys()
    if parent_names:
        rows = await session.execute(
            select(NodeNamespace).where(
                active,
                NodeNamespace.namespace.in_(parent_names),
            ),
        )
        namespaces.update({row.namespace: row for row in rows.scalars()})
    return namespaces


def _git_location(
    node_namespace: str,
    namespaces: dict[str, NodeNamespace],
) -> tuple[str, str] | None:
    """Resolve the repository owner and effective branch for a node namespace."""
    location = _git_view_location(node_namespace, namespaces)
    return (location[0], location[1]) if location else None


def _git_view_location(
    node_namespace: str,
    namespaces: dict[str, NodeNamespace],
) -> tuple[str, str, str | None] | None:
    """Resolve a cube's Git root, branch, and branch namespace."""
    info = resolve_git_info_from_map(node_namespace, namespaces)
    if not info:
        return None
    root = info["git_root_namespace"]
    branch = info["branch"] or info["default_branch"]
    if not root or not branch:
        return None
    return root, branch, info["branch_namespace"]


def _discovery_filter(configuration: dict[str, Any]) -> tuple[str | None, str | None]:
    """Validate the selectors while leaving unrelated runtime options alone."""
    namespace = configuration.get("namespace")
    branch = configuration.get("branch")
    for key, value in (("namespace", namespace), ("branch", branch)):
        if value is not None and (not isinstance(value, str) or not value):
            raise DJException(
                message=f"runtime_configuration.{key} must be a non-empty string",
                http_status_code=400,
            )
    if branch is not None and namespace is None:
        raise DJException(
            message="runtime_configuration.branch requires namespace",
            http_status_code=400,
        )
    return namespace, branch


def _path_filter(path: list[str | None]) -> tuple[str | None, str | None]:
    if (
        len(path) > 2
        or (path and (not isinstance(path[0], str) or not path[0]))
        or (
            len(path) == 2
            and path[1] is not None
            and (not isinstance(path[1], str) or not path[1])
        )
    ):
        raise DJException(message="Invalid view discovery path", http_status_code=400)
    return (path[0] if path else None, path[1] if len(path) == 2 else None)


async def _approved_resources(
    access_checker: AccessChecker,
) -> set[tuple[ResourceType, str]]:
    """Keep resource types when comparing authorization decisions by name."""
    return {
        (
            decision.request.access_object.resource_type,
            decision.request.access_object.name,
        )
        for decision in await access_checker.check()
    }


async def _cube_summaries(
    session: AsyncSession,
    scopes: set[str] | None,
    exact_namespace: str | None = None,
) -> list[tuple[str, str | None, str]]:
    """Select only the columns needed for discovery, scoped in SQL when possible."""
    if scopes is not None and not scopes:
        return []
    statement = (
        select(Node.name, NodeRevision.display_name, Node.namespace)
        .join(
            NodeRevision,
            and_(
                NodeRevision.node_id == Node.id,
                NodeRevision.version == Node.current_version,
            ),
        )
        .where(Node.deactivated_at.is_(None), Node.type == NodeType.CUBE)
    )
    if exact_namespace is not None:
        statement = statement.where(Node.namespace == exact_namespace)
    elif scopes is not None:
        statement = statement.where(
            or_(
                *(
                    condition
                    for scope in scopes
                    for condition in (
                        Node.namespace == scope,
                        Node.namespace.startswith(f"{scope}.", autoescape=True),
                    )
                ),
            ),
        )
    return list((await session.execute(statement)).all())


async def _selected_namespace(
    session: AsyncSession,
    namespace: str,
) -> tuple[NodeNamespace, bool] | None:
    """Return a selectable namespace and whether it owns a Git repository."""
    row = await NodeNamespace.get(
        session,
        namespace,
        raise_if_not_exists=False,
    )
    if not row or row.deactivated_at is not None:
        return None
    if row.github_repo_path:
        return row, True
    if await get_git_info_for_namespace(session, namespace) is not None:
        return None
    return row, False


def _view_scopes(
    root: str | None,
    branch: str | None,
    namespaces: dict[str, NodeNamespace],
) -> set[str] | None:
    """Limit the cube query to the root and matching branch namespace trees."""
    if root is None:
        return None
    scopes = set()
    root_location = _git_location(root, namespaces)
    if branch is None or root_location == (root, branch):
        scopes.add(root)
    for candidate in namespaces.values():
        if candidate.namespace == root:
            continue
        location = _git_location(candidate.namespace, namespaces)
        if location is None or location[0] != root:
            continue
        if branch is not None and location[1] != branch:
            continue
        if (
            branch is not None and candidate.git_branch
        ) or not candidate.namespace.startswith(f"{root}."):
            scopes.add(candidate.namespace)
    return scopes


async def _discoverable_namespaces(
    session: AsyncSession,
    access_checker: AccessChecker,
) -> list[str]:
    """List readable Git roots and plain namespaces, including empty ones."""
    namespaces = await _active_git_namespaces(session)
    values = sorted(
        name
        for name, row in namespaces.items()
        if row.github_repo_path or resolve_git_info_from_map(name, namespaces) is None
    )
    access_checker.add_namespaces(values, ResourceAction.READ)
    approved = await _approved_resources(access_checker)
    return [name for name in values if (ResourceType.NAMESPACE, name) in approved]


def _branch_namespaces(
    root: str,
    namespaces: dict[str, NodeNamespace],
) -> list[tuple[str, str]]:
    """Return branch namespace names and branch values for one Git root."""
    branches = []
    for candidate in namespaces.values():
        if candidate.namespace == root or not candidate.git_branch:
            continue
        location = _git_location(candidate.namespace, namespaces)
        if location is not None and location[0] == root:
            branches.append((candidate.namespace, location[1]))
    return branches


@router.post(
    "/discover",
    response_model=DiscoverResponse,
    response_model_exclude_none=True,
)
async def discover_views(
    request: DiscoverRequest,
    session: AsyncSession = Depends(get_session),
    access_checker: AccessChecker = Depends(get_access_checker),
) -> DiscoverResponse | JSONResponse:
    """Browse Git branches and non-Git cube namespaces in two levels."""
    try:
        if (
            request.runtime_configuration.get("namespace") is not None
            or request.runtime_configuration.get("branch") is not None
        ):
            raise DJException(
                message="Use path for view discovery",
                http_status_code=400,
            )
        _path_filter(request.path)
        if len(request.path) == 2:
            raise DJException(
                message="No hierarchy level follows this path",
                http_status_code=400,
            )
    except DJException as exc:
        return _problem(exc.http_status_code or 400, exc.message)
    if not request.path:
        return DiscoverResponse(
            level=DiscoverLevel(id="namespace", display_name="Namespace"),
            values=await _discoverable_namespaces(session, access_checker),
            next_level=DiscoverLevel(id="branch", display_name="Branch"),
        )
    namespace = cast(str, request.path[0])  # _path_filter validated the path.
    selected = await _selected_namespace(session, namespace)
    if selected is None:
        return _problem(404, f"Namespace `{namespace}` does not exist.")
    if not selected[1]:
        access_checker.add_namespace(namespace, ResourceAction.READ)
        if (ResourceType.NAMESPACE, namespace) not in await _approved_resources(
            access_checker,
        ):
            return _problem(404, f"Namespace `{namespace}` does not exist.")
        return DiscoverResponse(
            level=DiscoverLevel(id="branch", display_name="Branch"),
            values=[None],
        )
    branch_values = await _list_semantic_branches(namespace, session, access_checker)
    if isinstance(branch_values, JSONResponse):
        return branch_values
    return DiscoverResponse(
        level=DiscoverLevel(id="branch", display_name="Branch"),
        values=branch_values,
    )


async def _list_semantic_branches(
    namespace: str,
    session: AsyncSession,
    access_checker: AccessChecker,
) -> list[str] | JSONResponse:
    """List actual Git branch names for one repository root."""
    namespaces = await _active_git_namespaces(session, namespace)
    root = namespaces.get(namespace)
    if not root or not root.github_repo_path:
        return _problem(404, f"Git namespace `{namespace}` does not exist.")

    root_location = _git_location(namespace, namespaces)
    branch_namespaces = _branch_namespaces(namespace, namespaces)

    access_checker.add_namespace(namespace, ResourceAction.READ)
    access_checker.add_namespaces(
        [name for name, _ in branch_namespaces],
        ResourceAction.READ,
    )
    approved = await _approved_resources(access_checker)
    if (ResourceType.NAMESPACE, namespace) not in approved:
        return _problem(404, f"Git namespace `{namespace}` does not exist.")

    branches = {root_location[1]} if root_location else set()
    branches.update(
        branch
        for name, branch in branch_namespaces
        if (ResourceType.NAMESPACE, name) in approved
    )
    return sorted(branches)


@router.post(
    "/views/",
    response_model=list[ViewSummary],
    response_model_exclude_unset=True,
)
@router.post(
    "/views/list",
    response_model=list[ViewSummary],
    response_model_exclude_unset=True,
)
async def list_views(
    request: ListViewsRequest | None = Body(default=None),
    session: AsyncSession = Depends(get_session),
    access_checker: AccessChecker = Depends(get_access_checker),
) -> list[ViewSummary] | JSONResponse:
    """List the semantic views (DJ cubes) available to the caller.

    The spec's ``/views/`` returns summaries only;
    full metrics/dimensions are fetched per-view via ``/views/{view}``.
    """
    try:
        configuration = request.runtime_configuration if request else {}
        path = request.path if request else None
        if path is not None:
            if "namespace" in configuration or "branch" in configuration:
                raise DJException(
                    message="Use path or runtime_configuration selectors",
                    http_status_code=400,
                )
            namespace, branch = _path_filter(path)
        else:
            namespace, branch = _discovery_filter(configuration)

        selected = await _selected_namespace(session, namespace) if namespace else None
        if namespace and selected is None:
            return _problem(404, "Invalid view discovery path")

        branch_namespaces: list[tuple[str, str]] = []
        if selected and not selected[1]:
            if branch is not None:
                return _problem(404, "Invalid view discovery path")
            namespaces = {}
            cubes = await _cube_summaries(session, None, exact_namespace=namespace)
        else:
            namespaces = await _active_git_namespaces(session, namespace)
            if namespace:
                branch_namespaces = _branch_namespaces(namespace, namespaces)
                root_location = _git_location(namespace, namespaces)
                valid_branches = {value for _, value in branch_namespaces}
                if root_location:
                    valid_branches.add(root_location[1])
                if path is not None and len(path) == 2 and branch is None:
                    return _problem(404, "Invalid view discovery path")
                if branch is not None and branch not in valid_branches:
                    return _problem(404, "Invalid view discovery path")
            cubes = await _cube_summaries(
                session,
                _view_scopes(namespace, branch, namespaces),
            )
    except DJException as exc:
        return _problem(exc.http_status_code or 400, exc.message)

    required_namespaces: set[str] = set()
    if namespace:
        required_namespaces.add(namespace)
    if branch is not None:
        required_namespaces.update(
            name for name, value in branch_namespaces if value == branch
        )
    candidates = []
    for cube_name, display_name, node_namespace in cubes:
        location = _git_view_location(node_namespace, namespaces)
        if selected and selected[1]:
            if location is None or location[0] != namespace:
                continue
            if branch is not None and location[1] != branch:
                continue
        needed = {location[0]} if location else {node_namespace}
        if location and location[2]:
            needed.add(location[2])
        required_namespaces.update(needed)
        candidates.append((cube_name, display_name, location, needed))
        access_checker.add_request_by_node_name(cube_name, ResourceAction.READ)

    access_checker.add_namespaces(sorted(required_namespaces), ResourceAction.READ)
    approved = await _approved_resources(access_checker)
    if namespace and (ResourceType.NAMESPACE, namespace) not in approved:
        return _problem(404, "Invalid view discovery path")
    if namespace is not None and branch is not None and branch_namespaces:
        root_branch = _git_location(namespace, namespaces)
        if root_branch != (namespace, branch) and not any(
            value == branch and (ResourceType.NAMESPACE, name) in approved
            for name, value in branch_namespaces
        ):
            return _problem(404, "Invalid view discovery path")

    summaries = []
    for cube_name, display_name, location, needed in candidates:
        if (ResourceType.NODE, cube_name) not in approved or any(
            (ResourceType.NAMESPACE, name) not in approved for name in needed
        ):
            continue
        summary = ViewSummary(
            name=cube_name,
            display_name=display_name or None,
            uid=cube_name,
            features=[],
        )
        if location:
            summary.namespace, summary.branch = location[:2]
        summaries.append(summary)
    return summaries


@router.post(
    "/views/{view_name}",
    response_model=ViewDetail,
    response_model_exclude_unset=True,
)
async def get_view(
    view_name: str,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> ViewDetail | JSONResponse:
    """Describe a single semantic view: its metrics and dimensions."""
    try:
        cube_node = await Node.get_cube_by_name(
            session,
            view_name,
            for_measures_sql=True,  # load only what we need to build the metrics
        )
    except DJException as exc:
        return _problem(exc.http_status_code or 404, exc.message)
    if cube_node is None or cube_node.current is None:
        return _problem(404, f"View `{view_name}` does not exist.")
    return _view_payload(cube_node.current)


async def _generate_sql(
    session: AsyncSession,
    payload: QueryPayload,
    cube_rev: NodeRevision,
    row_count: bool = False,
) -> GeneratedSQLResponse:
    """Generate physical SQL pinned to this cube (``matched_cube=cube_rev``), so
    we don't let ``find_matching_cube`` pick a different/differently-filtered
    materialization.
    """
    orderby = (
        [f"{o.by} {o.direction.upper()}" for o in payload.order]
        if payload.order
        else None
    )
    limit = payload.limit if payload.limit is not None else DEFAULT_ROW_LIMIT

    request_filters = [_filter_to_sql(f) for f in payload.filters]

    if payload.metrics:
        generated_sql = await generate_metrics_sql(
            session,
            metrics=payload.metrics,
            dimensions=payload.dimensions,
            filters=request_filters,
            matched_cube=cube_rev,
            orderby=orderby,
            limit=limit,
            endpoint="/semantic-layer/views/sql",
        )
    else:
        generated_sql = await generate_dimensions_sql(
            session,
            dimensions=payload.dimensions,
            filters=request_filters,
            matched_cube=cube_rev,
            orderby=orderby,
            limit=limit,
            endpoint="/semantic-layer/views/sql",
        )
    if row_count:
        generated_sql = build_row_count_sql(generated_sql)
    # ``generated_sql.sql`` renders via ``to_sql(query, dialect)``, which already
    # applies DJ's dialect rules and transpiles to the resolved dialect, so it is
    # execution-ready for the caller (which runs it directly). We return the
    # dialect so the caller knows which engine to run it on.
    columns = generated_sql.columns or []
    return GeneratedSQLResponse(
        sql=generated_sql.sql,
        dialect=generated_sql.dialect.value,
        columns=[
            ColumnInfo(name=col.name, type=_generated_column_arrow_type_name(col))
            for col in columns
        ],
        cube_name=generated_sql.cube_name,
    )


async def _generate_view_sql(
    view_name: str,
    payload: QueryPayload,
    session: AsyncSession,
    row_count: bool = False,
) -> GeneratedSQLResponse | JSONResponse:
    """Validate and generate SQL for a semantic view query."""
    if payload.offset:
        return _problem(
            400,
            "Pagination via `offset` is not supported yet.",
        )
    if payload.limit is not None and payload.limit < 0:
        return _problem(400, "`limit` must be non-negative.")
    if payload.limit is not None and payload.limit > MAX_ROW_LIMIT:
        return _problem(
            400,
            f"`limit` {payload.limit} exceeds the maximum of {MAX_ROW_LIMIT}.",
        )
    if not payload.metrics and not payload.dimensions:
        return _problem(400, "A query must request at least one metric or dimension.")
    try:
        cube_node = await Node.get_cube_by_name(session, view_name)
        if cube_node is None or cube_node.current is None:
            return _problem(404, f"View `{view_name}` does not exist.")
        cube_rev = cube_node.current
        # Everything referenced must belong to this cube (full refs incl. role
        # suffixes), else one view could compute another's metrics.
        allowed_metrics = set(cube_rev.cube_node_metrics)
        allowed_dims = set(cube_rev.cube_node_dimensions)
        allowed = allowed_metrics | allowed_dims
        bad_metrics = [m for m in payload.metrics if m not in allowed_metrics]
        bad_dims = [d for d in payload.dimensions if d not in allowed_dims]
        bad_filters = [
            f.column for f in payload.filters if f.column and f.column not in allowed
        ]
        if bad_metrics or bad_dims or bad_filters:
            return _problem(
                400,
                f"View `{view_name}` does not contain: "
                f"metrics={bad_metrics} dimensions={bad_dims} "
                f"filter_columns={bad_filters}",
            )
        result = await _generate_sql(session, payload, cube_rev, row_count=row_count)
    except DJException as exc:
        return _problem(exc.http_status_code or 400, exc.message)
    return result


@router.post("/views/{view_name}/sql", response_model=GeneratedSQLResponse)
async def generate_query_sql(
    view_name: str,
    body: QueryRequest,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> GeneratedSQLResponse | JSONResponse:
    """Generate physical SQL for a view query."""
    return await _generate_view_sql(view_name, body.query, session)


@router.post("/views/{view_name}/row-count", response_model=GeneratedSQLResponse)
async def generate_row_count_sql(
    view_name: str,
    body: QueryRequest,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> GeneratedSQLResponse | JSONResponse:
    """Generate SQL that counts the rows returned by a semantic query."""
    return await _generate_view_sql(view_name, body.query, session, row_count=True)


@router.post("/views/{view_name}/values", response_model=GeneratedSQLResponse)
async def generate_values_sql(
    view_name: str,
    body: ValuesRequest,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> GeneratedSQLResponse | JSONResponse:
    """Generate SQL for a dimension's distinct values, optionally filtered."""
    try:
        cube_node = await Node.get_cube_by_name(session, view_name)
    except DJException as exc:
        return _problem(exc.http_status_code or 400, exc.message)
    if cube_node is None or cube_node.current is None:
        return _problem(404, f"View `{view_name}` does not exist.")

    cube_rev = cube_node.current
    if body.dimension not in cube_rev.cube_node_dimensions:
        return _problem(
            400,
            f"View `{view_name}` does not contain dimension `{body.dimension}`.",
        )

    bad_filters = [
        flt.column
        for flt in body.filters
        if flt.column and flt.column not in cube_rev.cube_node_dimensions
    ]
    if bad_filters:
        return _problem(
            400,
            f"View `{view_name}` does not contain filter dimensions: {bad_filters}",
        )

    query = QueryPayload(
        dimensions=[body.dimension],
        filters=body.filters,
    )
    try:
        return await _generate_sql(session, query, cube_rev)
    except DJException as exc:
        return _problem(exc.http_status_code or 400, exc.message)
