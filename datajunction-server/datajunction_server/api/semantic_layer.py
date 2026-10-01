"""
Semantic-layer REST API implementing the sl-db-engine-spec over DataJunction
cubes (each cube = a "view").

Spec: https://github.com/betodealmeida/sl-db-engine-spec/blob/main/server/SPEC.md

DJ generates SQL; the client executes it. This router does NOT run queries —
``/views/{view}/sql`` returns physical SQL + dialect for the client to run;
``/views/list`` and ``/views/{view}`` are metadata. Mounted at ``/semantic``.
"""

import logging
from copy import deepcopy
from typing import Any

from fastapi import Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field  # pylint: disable=no-name-in-module
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.user import User
from datajunction_server.errors import DJException
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.sql import (
    build_row_count_sql,
    generate_dimensions_sql,
    generate_metrics_sql,
)
from datajunction_server.construction.build_v3.builder import apply_orderby_limit
from datajunction_server.construction.build_v3.types import (
    GeneratedSQL as V3GeneratedSQL,
)
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.parsing import ast
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
SEMANTIC_VIEW_FEATURES = ["GROUP_LIMIT"]

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
        features=SEMANTIC_VIEW_FEATURES,
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


class GroupLimitPayload(BaseModel):
    """Top or bottom groups selected independently of the displayed rows."""

    dimensions: list[str]
    top: int
    metric: str | None = None
    direction: str = "DESC"
    group_others: bool = False
    # None inherits the outer filters; [] deliberately ranks without them.
    filters: list[FilterPayload] | None = None


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
    group_limit: GroupLimitPayload | None = None


class QueryRequest(BaseModel):
    """Body for POST /views/{view_name}/sql (backs the spec's /query)."""

    additional_configuration: dict[str, Any] = Field(default_factory=dict)
    query: QueryPayload


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


@router.post("/views/list", response_model=list[ViewSummary])
async def list_views(
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> list[ViewSummary] | JSONResponse:
    """List the semantic views (DJ cubes) available to the caller.

    The spec's ``/views/list`` returns summaries only;
    full metrics/dimensions are fetched per-view via ``/views/{view}``.
    """
    try:
        cubes = await Node.find_names_and_display_names(
            session,
            node_type=NodeType.CUBE,
        )
    except DJException as exc:
        return _problem(exc.http_status_code or 400, exc.message)
    return [
        ViewSummary(
            name=cube_name,
            display_name=display_name or None,
            uid=cube_name,
            features=SEMANTIC_VIEW_FEATURES,
        )
        for cube_name, display_name in cubes
    ]


@router.post("/views/{view_name}", response_model=ViewDetail)
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


def _limit_to_ranked_groups(
    main: V3GeneratedSQL,
    ranked: V3GeneratedSQL,
    dimensions: list[str],
    orderby: list[str] | None,
    limit: int,
) -> V3GeneratedSQL:
    """Keep main-query rows whose dimension tuple occurs in the top-N query."""
    main_columns = {column.semantic_name: column.name for column in main.columns}
    ranked_columns = {column.semantic_name: column.name for column in ranked.columns}
    main_alias = "semantic_query"
    ranked_alias = "ranked_groups"

    def column(name: str, alias: str) -> ast.Column:
        return ast.Column(name=ast.Name(name), _table=ast.Table(ast.Name(alias)))

    conditions: list[ast.Expression] = []
    for dimension in dimensions:
        left = column(main_columns[dimension], main_alias)
        right = column(ranked_columns[dimension], ranked_alias)
        condition = ast.BinaryOp(
            left=ast.BinaryOp(left=left, right=right, op=ast.BinaryOpKind.Eq),
            right=ast.BinaryOp(
                left=ast.IsNull(expr=column(main_columns[dimension], main_alias)),
                right=ast.IsNull(expr=column(ranked_columns[dimension], ranked_alias)),
                op=ast.BinaryOpKind.And,
            ),
            op=ast.BinaryOpKind.Or,
        )
        condition.parenthesized = True
        conditions.append(condition)

    main_query = deepcopy(main.query)
    main_query.parenthesized = True
    main_query.alias = ast.Name(main_alias)
    main_query.as_ = True
    ranked_query = deepcopy(ranked.query)
    ranked_query.parenthesized = True
    ranked_query.alias = ast.Name(ranked_alias)
    ranked_query.as_ = True

    query = ast.Query(
        select=ast.Select(
            projection=[
                column(col.name, main_alias).set_alias(ast.Name(col.name)).set_as(True)
                for col in main.columns
            ],
            from_=ast.From(
                relations=[
                    ast.Relation(
                        primary=main_query,
                        extensions=[
                            ast.Join(
                                join_type="INNER",
                                right=ranked_query,
                                criteria=ast.JoinCriteria(
                                    on=ast.BinaryOp.And(*conditions),
                                ),
                            ),
                        ],
                    ),
                ],
            ),
        ),
    )
    result = V3GeneratedSQL(
        query=query,
        columns=main.columns,
        dialect=main.dialect,
        cube_name=main.cube_name,
        scan_estimate=main.scan_estimate,
        warnings=main.warnings + ranked.warnings,
    )
    return apply_orderby_limit(result, orderby, limit)


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
            # A group limit adds a subquery join with null-safe tuple matching.
            # Build both sides from live sources so an available Druid cube
            # cannot route this SQL to Druid's restricted join planner.
            use_materialized=payload.group_limit is None,
            orderby=None if payload.group_limit else orderby,
            limit=None if payload.group_limit else limit,
            endpoint="/semantic-layer/views/sql",
        )
    else:
        generated_sql = await generate_dimensions_sql(
            session,
            dimensions=payload.dimensions,
            filters=request_filters,
            matched_cube=cube_rev,
            use_materialized=payload.group_limit is None,
            orderby=None if payload.group_limit else orderby,
            limit=None if payload.group_limit else limit,
            endpoint="/semantic-layer/views/sql",
        )
    if payload.group_limit:
        group_limit = payload.group_limit
        metric = group_limit.metric or payload.metrics[0]
        ranking_filters = (
            payload.filters if group_limit.filters is None else group_limit.filters
        )
        ranked_sql = await generate_metrics_sql(
            session,
            metrics=[metric],
            dimensions=group_limit.dimensions,
            filters=[_filter_to_sql(f) for f in ranking_filters],
            matched_cube=cube_rev,
            use_materialized=False,
            dialect=generated_sql.dialect,
            orderby=[f"{metric} {group_limit.direction.upper()}"]
            + [f"{dim} ASC" for dim in group_limit.dimensions],
            limit=group_limit.top,
            endpoint="/semantic-layer/views/sql/group-limit",
        )
        generated_sql = _limit_to_ranked_groups(
            generated_sql,
            ranked_sql,
            group_limit.dimensions,
            orderby,
            limit,
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
        if group_limit := payload.group_limit:
            if group_limit.group_others:
                return _problem(400, "`group_others` is not supported yet.")
            if not payload.metrics and not group_limit.metric:
                return _problem(400, "`group_limit` requires a ranking metric.")
            if not group_limit.dimensions or len(set(group_limit.dimensions)) != len(
                group_limit.dimensions,
            ):
                return _problem(
                    400,
                    "`group_limit.dimensions` must be non-empty and unique.",
                )
            if not set(group_limit.dimensions).issubset(payload.dimensions):
                return _problem(
                    400,
                    "`group_limit.dimensions` must be selected dimensions.",
                )
            if group_limit.metric and group_limit.metric not in allowed_metrics:
                return _problem(400, "`group_limit.metric` must belong to the view.")
            if group_limit.top < 1 or group_limit.top > MAX_ROW_LIMIT:
                return _problem(
                    400,
                    f"`group_limit.top` must be between 1 and {MAX_ROW_LIMIT}.",
                )
            if group_limit.direction.upper() not in {"ASC", "DESC"}:
                return _problem(400, "`group_limit.direction` must be ASC or DESC.")
            bad_group_filters = [
                f.column for f in group_limit.filters or [] if f.column not in allowed
            ]
            if bad_group_filters:
                return _problem(
                    400,
                    f"View `{view_name}` does not contain group-limit filter columns: "
                    f"{bad_group_filters}",
                )
            for flt in group_limit.filters or []:
                filter_type = flt.type.upper()
                if filter_type not in {"WHERE", "HAVING"}:
                    return _problem(
                        400,
                        "`group_limit.filters.type` must be WHERE or HAVING.",
                    )
                valid_columns = (
                    allowed_dims if filter_type == "WHERE" else allowed_metrics
                )
                if flt.column not in valid_columns:
                    return _problem(
                        400,
                        f"`group_limit.filters` {filter_type} columns must be "
                        f"{'dimensions' if filter_type == 'WHERE' else 'metrics'}.",
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
