"""
Semantic-layer REST API implementing the sl-db-engine-spec over DataJunction
cubes (each cube = a "view").

Spec: https://github.com/betodealmeida/sl-db-engine-spec/blob/main/server/SPEC.md

DJ generates SQL; the client executes it. This router does NOT run queries —
``/views/{view}/sql`` returns physical SQL + dialect for the client to run;
``/views/list`` and ``/views/{view}`` are metadata. Mounted at ``/semantic``.
"""

import logging
from typing import Any

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
    """Summary entry returned by ``/views/list``."""

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
    return namespaces


def _git_location(
    node_namespace: str,
    namespaces: dict[str, NodeNamespace],
) -> tuple[str, str] | None:
    """Resolve the repository owner and effective branch for a node namespace."""
    info = resolve_git_info_from_map(node_namespace, namespaces)
    if not info:
        return None
    root = info["git_root_namespace"]
    branch = info["branch"] or info["default_branch"]
    if not root or not branch:
        return None
    return root, branch


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


async def _plain_cube_summaries(
    session: AsyncSession,
    namespace: str,
    access_checker: AccessChecker,
) -> list[tuple[str, str | None, str]] | None:
    """Return readable cubes in an exact non-Git namespace, or None for Git."""
    namespace_row = await NodeNamespace.get(
        session,
        namespace,
        raise_if_not_exists=False,
    )
    if not namespace_row or namespace_row.deactivated_at is not None:
        return []
    if await get_git_info_for_namespace(session, namespace) is not None:
        return None
    access_checker.add_namespace(namespace, ResourceAction.READ)
    cubes = await _cube_summaries(session, None, exact_namespace=namespace)
    for name, _, _ in cubes:
        access_checker.add_request_by_node_name(name, ResourceAction.READ)
    decisions = await access_checker.check()
    approved_namespaces = {
        decision.request.access_object.name
        for decision in decisions
        if decision.request.access_object.resource_type == ResourceType.NAMESPACE
    }
    approved_cubes = {
        decision.request.access_object.name
        for decision in decisions
        if decision.request.access_object.resource_type == ResourceType.NODE
    }
    if namespace not in approved_namespaces:
        return []
    return [cube for cube in cubes if cube[0] in approved_cubes]


async def _namespace_map_for_cubes(
    session: AsyncSession,
    cube_namespaces: set[str],
) -> dict[str, NodeNamespace]:
    """Load cube namespaces, their string ancestors, and linked Git parents."""
    if not cube_namespaces:
        return {}
    wanted = cube_namespaces | {
        parent
        for namespace in cube_namespaces
        for parent in get_parent_namespaces(namespace)
    }
    active = NodeNamespace.deactivated_at.is_(None)
    rows = await session.execute(
        select(NodeNamespace).where(active, NodeNamespace.namespace.in_(wanted)),
    )
    namespaces = {row.namespace: row for row in rows.scalars()}
    linked_parents = {
        row.parent_namespace
        for row in namespaces.values()
        if row.parent_namespace and row.parent_namespace not in namespaces
    }
    if linked_parents:
        rows = await session.execute(
            select(NodeNamespace).where(
                active,
                NodeNamespace.namespace.in_(linked_parents),
            ),
        )
        namespaces.update({row.namespace: row for row in rows.scalars()})
    return namespaces


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


@router.post("/namespaces/", response_model=list[str])
async def list_semantic_namespaces(
    session: AsyncSession = Depends(get_session),
    access_checker: AccessChecker = Depends(get_access_checker),
) -> list[str]:
    """List active Git repository roots available for semantic browsing."""
    rows = await session.execute(
        select(NodeNamespace.namespace).where(
            NodeNamespace.deactivated_at.is_(None),
            NodeNamespace.github_repo_path.is_not(None),
            NodeNamespace.github_repo_path != "",
        ),
    )
    roots = sorted(rows.scalars())
    access_checker.add_namespaces(roots, ResourceAction.READ)
    approved = set(await access_checker.approved_resource_names())
    return [root for root in roots if root in approved]


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
        values = set(await list_semantic_namespaces(session, access_checker))
        rows = await session.execute(
            select(Node.name, Node.namespace).where(
                Node.deactivated_at.is_(None),
                Node.type == NodeType.CUBE,
            ),
        )
        cubes = list(rows.all())
        namespaces = await _namespace_map_for_cubes(
            session,
            {namespace for _, namespace in cubes},
        )
        unversioned_cubes = [
            (name, namespace)
            for name, namespace in cubes
            if namespace in namespaces
            and resolve_git_info_from_map(namespace, namespaces) is None
        ]
        access_checker.add_namespaces(
            sorted({namespace for _, namespace in unversioned_cubes}),
            ResourceAction.READ,
        )
        for name, _ in unversioned_cubes:
            access_checker.add_request_by_node_name(name, ResourceAction.READ)
        decisions = await access_checker.check()
        approved_namespaces = {
            decision.request.access_object.name
            for decision in decisions
            if decision.request.access_object.resource_type == ResourceType.NAMESPACE
        }
        approved_cubes = {
            decision.request.access_object.name
            for decision in decisions
            if decision.request.access_object.resource_type == ResourceType.NODE
        }
        values.update(
            namespace
            for name, namespace in unversioned_cubes
            if namespace in approved_namespaces and name in approved_cubes
        )
        return DiscoverResponse(
            level=DiscoverLevel(id="namespace", display_name="Namespace"),
            values=sorted(values),
            next_level=DiscoverLevel(id="branch", display_name="Branch"),
        )
    namespace = request.path[0]
    assert isinstance(namespace, str)
    plain_cubes = await _plain_cube_summaries(session, namespace, access_checker)
    if plain_cubes is not None:
        if not plain_cubes:
            return _problem(404, f"Namespace `{namespace}` does not exist.")
        return DiscoverResponse(
            level=DiscoverLevel(id="branch", display_name="Branch"),
            values=[None],
        )
    values = await list_semantic_branches(namespace, session, access_checker)
    if isinstance(values, JSONResponse):
        return values
    return DiscoverResponse(
        level=DiscoverLevel(id="branch", display_name="Branch"),
        values=values,
    )


@router.post("/namespaces/{namespace}/branches/", response_model=list[str])
async def list_semantic_branches(
    namespace: str,
    session: AsyncSession = Depends(get_session),
    access_checker: AccessChecker = Depends(get_access_checker),
) -> list[str] | JSONResponse:
    """List actual Git branch names for one repository root."""
    namespaces = await _active_git_namespaces(session, namespace)
    root = namespaces.get(namespace)
    if not root or not root.github_repo_path:
        return _problem(404, f"Git namespace `{namespace}` does not exist.")

    access_checker.add_namespace(namespace, ResourceAction.READ)
    if namespace not in await access_checker.approved_resource_names():
        return _problem(404, f"Git namespace `{namespace}` does not exist.")

    root_location = _git_location(namespace, namespaces)
    branches = {root_location[1]} if root_location else set()
    branch_namespaces = [
        (candidate.namespace, location[1])
        for candidate in namespaces.values()
        if candidate.namespace != namespace
        and candidate.git_branch
        and (location := _git_location(candidate.namespace, namespaces)) is not None
        and location[0] == namespace
    ]
    access_checker.add_namespaces(
        [name for name, _ in branch_namespaces],
        ResourceAction.READ,
    )
    approved = set(await access_checker.approved_resource_names())
    branches.update(branch for name, branch in branch_namespaces if name in approved)
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
        if request and request.path is not None:
            if "namespace" in configuration or "branch" in configuration:
                raise DJException(
                    message="Use path or runtime_configuration selectors",
                    http_status_code=400,
                )
            namespace, branch = _path_filter(request.path)
            if namespace is not None:
                plain_cubes = await _plain_cube_summaries(
                    session,
                    namespace,
                    access_checker,
                )
                if plain_cubes is not None:
                    if (
                        len(request.path) == 2 and branch is not None
                    ) or not plain_cubes:
                        return _problem(404, "Invalid view discovery path")
                    return [
                        ViewSummary(
                            name=name,
                            display_name=display_name or None,
                            uid=name,
                            features=[],
                        )
                        for name, display_name, _ in plain_cubes
                    ]
                if len(request.path) == 2 and branch is None:
                    raise DJException(
                        message="Invalid view discovery path",
                        http_status_code=404,
                    )
                branches = await list_semantic_branches(
                    namespace,
                    session,
                    access_checker,
                )
                if isinstance(branches, JSONResponse):
                    return branches
                if branch is not None and branch not in branches:
                    raise DJException(
                        message="Invalid view discovery path",
                        http_status_code=404,
                    )
        else:
            namespace, branch = _discovery_filter(configuration)
        namespaces = await _active_git_namespaces(session, namespace)
        cubes = await _cube_summaries(
            session,
            _view_scopes(namespace, branch, namespaces),
        )
    except DJException as exc:
        return _problem(exc.http_status_code or 400, exc.message)
    for cube_name, _, _ in cubes:
        access_checker.add_request_by_node_name(cube_name, ResourceAction.READ)
    approved = set(await access_checker.approved_resource_names())
    summaries = []
    for cube_name, display_name, node_namespace in cubes:
        if cube_name not in approved:
            continue
        location = _git_location(node_namespace, namespaces)
        if namespace is not None and (location is None or location[0] != namespace):
            continue
        if branch is not None and (location is None or location[1] != branch):
            continue
        summary = ViewSummary(
            name=cube_name,
            display_name=display_name or None,
            uid=cube_name,
            features=[],
        )
        if location:
            summary.namespace, summary.branch = location
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
