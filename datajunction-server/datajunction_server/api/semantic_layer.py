"""
Semantic-layer REST API implementing the sl-db-engine-spec over DataJunction
cubes (each cube = a "view").

Spec: https://github.com/betodealmeida/sl-db-engine-spec/blob/main/server/SPEC.md

DJ generates SQL; the client executes it. This router does NOT run queries —
``/views/{view}/sql`` returns physical SQL + dialect for the client to run;
``/views/list`` and ``/views/{view}`` are metadata. Mounted at ``/semantic``.
"""

import logging
from collections.abc import Mapping
from typing import Any, Literal

from fastapi import Depends
from fastapi.responses import JSONResponse
from pydantic import (  # pylint: disable=no-name-in-module
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    ValidationError,
)
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.user import User
from datajunction_server.errors import DJException
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.sql import generate_metrics_sql
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.unit import Unit, unit_to_dict
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

_UNIT_ADAPTER = TypeAdapter(Unit)


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


def _semantic_type(
    column: Any,
    arrow_type: str,
    *,
    is_metric: bool,
) -> str:
    """Derive the portable business type from DJ's structured column fields."""
    unit = unit_to_dict(getattr(column, "unit", None))
    if unit and "kind" in unit:
        unit_kind = unit["kind"]
        if unit_kind == "time":
            return "duration"
        if unit_kind in {
            "currency",
            "percentage",
            "proportion",
            "count",
            "duration",
            "data_size",
        }:
            return unit_kind

    attributes = (
        set(column.attribute_names()) if hasattr(column, "attribute_names") else set()
    )
    if "primary_key" in attributes:
        return "identifier"
    if arrow_type == "date":
        return "date"
    if arrow_type == "timestamp":
        return "timestamp"
    if arrow_type == "bool":
        return "boolean"
    if arrow_type in {"int", "floating", "decimal"} or is_metric:
        return "number"
    return "category"


def _format_metadata(column: Any) -> "FormatMetadata | None":
    """Translate units with unambiguous display semantics to portable hints."""
    unit = unit_to_dict(getattr(column, "unit", None))
    if not unit or "kind" not in unit:
        return None
    preset_by_kind = {
        "currency": "currency",
        "percentage": "percentage",
        "time": "duration",
        "data_size": "data_size",
    }
    preset = preset_by_kind.get(unit["kind"])
    return FormatMetadata(preset=preset) if preset else None


def _validated_unit(raw_unit: Any) -> Unit | None:
    """Validate custom metadata units without letting bad annotations fail a view."""
    if raw_unit is None:
        return None
    try:
        return _UNIT_ADAPTER.validate_python(raw_unit)
    except ValidationError:
        return None


_METADATA_KEYS = {
    "display_name",
    "semantic_type",
    "unit",
    "attributes",
    "format",
    "filter",
    "extensions",
}


def _raw_column_metadata(cube: NodeRevision, column_id: str) -> dict[str, Any]:
    """Find semantic metadata declared in an element node's custom metadata.

    Metric nodes describe one value, so their metadata may be declared directly.
    Dimension nodes can expose several values and use a ``columns`` mapping.
    An optional ``semantic_layer`` wrapper keeps this contract separate from other
    DJ custom metadata.
    """
    for element in getattr(cube, "cube_elements", []):
        revision = element.node_revision
        if revision is None:
            continue
        if revision.type == NodeType.METRIC:
            if revision.name != column_id:
                continue
            raw = revision.custom_metadata or {}
        else:
            base_id = f"{revision.name}.{element.name}"
            if column_id != base_id and not column_id.startswith(f"{base_id}["):
                continue
            raw = revision.custom_metadata or {}
            if isinstance(raw, Mapping) and isinstance(
                raw.get("semantic_layer"),
                Mapping,
            ):
                raw = raw["semantic_layer"]
            raw = raw.get("columns", {}).get(element.name, {})

        if isinstance(raw, Mapping) and isinstance(raw.get("semantic_layer"), Mapping):
            raw = raw["semantic_layer"]
        return dict(raw) if isinstance(raw, Mapping) else {}
    return {}


def _column_metadata(
    column: Any,
    *,
    is_metric: bool,
    raw_metadata: Mapping[str, Any] | None = None,
) -> "ColumnMetadata | None":
    """Build strict portable metadata from a DJ cube column."""
    raw_metadata = raw_metadata or {}
    arrow_type = _arrow_type_name(getattr(column, "type", None)) or (
        METRIC_FALLBACK_ARROW_TYPE_NAME
        if is_metric
        else DIMENSION_FALLBACK_ARROW_TYPE_NAME
    )
    attributes = column.attribute_names() if hasattr(column, "attribute_names") else []
    extensions = (
        {
            str(namespace): dict(values)
            for namespace, values in raw_metadata.get("extensions", {}).items()
            if isinstance(values, Mapping)
        }
        if isinstance(raw_metadata.get("extensions"), Mapping)
        else {}
    )
    producer_metadata = {
        key: value for key, value in raw_metadata.items() if key not in _METADATA_KEYS
    }
    if producer_metadata:
        extensions["datajunction"] = {
            **producer_metadata,
            **extensions.get("datajunction", {}),
        }

    raw_format = raw_metadata.get("format")
    format_metadata = None
    if isinstance(raw_format, Mapping):
        raw_preset = raw_format.get("preset")
        format_metadata = FormatMetadata(
            preset=(
                raw_preset
                if isinstance(raw_preset, str)
                and raw_preset in FormatMetadata.allowed_presets()
                else None
            ),
            precision=(
                raw_format.get("precision")
                if isinstance(raw_format.get("precision"), int)
                and not isinstance(raw_format.get("precision"), bool)
                and raw_format["precision"] >= 0
                else None
            ),
            scale=(
                raw_format.get("scale")
                if isinstance(raw_format.get("scale"), (int, float))
                and not isinstance(raw_format.get("scale"), bool)
                else None
            ),
        )
        if not format_metadata.model_dump(exclude_none=True):
            format_metadata = None

    raw_unit = raw_metadata.get("unit", unit_to_dict(getattr(column, "unit", None)))
    unit = _validated_unit(raw_unit)
    raw_semantic_type = raw_metadata.get("semantic_type")
    metadata = ColumnMetadata(
        display_name=(
            raw_metadata.get("display_name")
            if isinstance(raw_metadata.get("display_name"), str)
            else getattr(column, "display_name", None)
        ),
        semantic_type=(
            raw_semantic_type
            if isinstance(raw_semantic_type, str)
            and raw_semantic_type in ColumnMetadata.allowed_semantic_types()
            else _semantic_type(column, arrow_type, is_metric=is_metric)
        ),
        unit=unit,
        attributes=(
            list(dict.fromkeys(raw_metadata["attributes"]))
            if isinstance(raw_metadata.get("attributes"), list)
            and all(isinstance(value, str) for value in raw_metadata["attributes"])
            else attributes or None
        ),
        format=format_metadata or _format_metadata(column),
        filter=(
            FilterMetadata.from_mapping(raw_metadata["filter"])
            if isinstance(raw_metadata.get("filter"), Mapping)
            else None
        ),
        extensions=extensions or None,
    )
    return metadata if metadata.model_dump(exclude_none=True) else None


def _cube_column_map(cube: NodeRevision) -> dict[str, Any]:
    """Return role-aware cube columns keyed by semantic-layer id."""
    return {column.cube_element_name: column for column in cube.columns}


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
    columns_by_name = _cube_column_map(cube)
    return [
        MetricInfo(
            id=metric_name,
            name=metric_name.split(".")[-1],
            type=type_by_name.get(metric_name) or METRIC_FALLBACK_ARROW_TYPE_NAME,
            definition=metric_name,
            description=getattr(columns_by_name.get(metric_name), "description", None),
            aggregation="OTHER",
            metadata=(
                _column_metadata(
                    columns_by_name[metric_name],
                    is_metric=True,
                    raw_metadata=_raw_column_metadata(cube, metric_name),
                )
                if metric_name in columns_by_name
                else None
            ),
        )
        for metric_name in cube.cube_node_metrics
    ]


def _dimensions_payload(cube: NodeRevision) -> list["DimensionInfo"]:
    """Spec ``dimensions`` list. Grain detection is deferred."""
    type_by_name = _cube_column_type_map(cube)
    columns_by_name = _cube_column_map(cube)
    return [
        DimensionInfo(
            id=dim_ref,
            name=dim_ref.split(".")[-1],
            type=type_by_name.get(dim_ref) or DIMENSION_FALLBACK_ARROW_TYPE_NAME,
            definition=dim_ref,
            description=getattr(columns_by_name.get(dim_ref), "description", None),
            grain=None,
            metadata=(
                _column_metadata(
                    columns_by_name[dim_ref],
                    is_metric=False,
                    raw_metadata=_raw_column_metadata(cube, dim_ref),
                )
                if dim_ref in columns_by_name
                else None
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
        display_name=cube.display_name,
        uid=cube.name,
        features=[],  # no optional spec features for now
        dimensions=_dimensions_payload(cube),
        metrics=_metrics_payload(cube),
    )


# ---------------------------------------------------------------------------
# Filter / order translation (spec QueryPayload -> DJ build_metrics_sql args)
# ---------------------------------------------------------------------------

# Comparison operators accepted for filters; anything else (IN/LIKE/…) is rejected.
_ALLOWED_OPERATORS = frozenset(
    {"=", "!=", ">", "<", ">=", "<=", "IS NULL", "IS NOT NULL"},
)
_NULLARY_OPERATORS = frozenset({"IS NULL", "IS NOT NULL"})


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

    query: QueryPayload


# ---------------------------------------------------------------------------
# Response schemas
# ---------------------------------------------------------------------------


class FormatMetadata(BaseModel):
    """Portable presentation hints from the semantic-layer specification."""

    preset: (
        Literal[
            "smart_number",
            "number",
            "currency",
            "percentage",
            "duration",
            "data_size",
        ]
        | None
    ) = None
    precision: int | None = Field(default=None, ge=0)
    scale: float | None = None

    model_config = ConfigDict(extra="forbid")

    @staticmethod
    def allowed_presets() -> set[str]:
        """Values accepted by the portable metadata contract."""
        return {
            "smart_number",
            "number",
            "currency",
            "percentage",
            "duration",
            "data_size",
        }


class FilterMetadata(BaseModel):
    """Portable query-builder hints from the semantic-layer specification."""

    kind: (
        Literal[
            "text",
            "number",
            "range",
            "date",
            "datetime",
            "boolean",
            "select",
        ]
        | None
    ) = None
    operators: list[str] | None = None
    default_operator: str | None = None
    multi: bool | None = None

    model_config = ConfigDict(extra="forbid")

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "FilterMetadata | None":
        """Sanitize free-form DJ custom metadata into the strict contract."""
        allowed_operators = {
            "=",
            "!=",
            ">",
            ">=",
            "<",
            "<=",
            "IN",
            "NOT IN",
            "IS NULL",
            "IS NOT NULL",
            "between",
            "contains",
            "starts_with",
            "ends_with",
        }
        operators = raw.get("operators")
        clean_operators = (
            list(dict.fromkeys(operators))
            if isinstance(operators, list)
            and all(
                isinstance(operator, str) and operator in allowed_operators
                for operator in operators
            )
            else None
        )
        default_operator = raw.get("default_operator")
        if (
            not isinstance(default_operator, str)
            or default_operator not in allowed_operators
            or (clean_operators is not None and default_operator not in clean_operators)
        ):
            default_operator = None
        metadata = cls(
            kind=(
                raw.get("kind")
                if raw.get("kind")
                in {"text", "number", "range", "date", "datetime", "boolean", "select"}
                else None
            ),
            operators=clean_operators,
            default_operator=default_operator,
            multi=raw.get("multi") if isinstance(raw.get("multi"), bool) else None,
        )
        return metadata if metadata.model_dump(exclude_none=True) else None


class ColumnMetadata(BaseModel):
    """Portable metadata shared by semantic dimensions and metrics."""

    display_name: str | None = None
    semantic_type: (
        Literal[
            "currency",
            "percentage",
            "proportion",
            "count",
            "duration",
            "data_size",
            "date",
            "timestamp",
            "identifier",
            "category",
            "url",
            "boolean",
            "number",
            "string",
        ]
        | None
    ) = None
    unit: Unit | None = None
    attributes: list[str] | None = None
    format: FormatMetadata | None = None
    filter: FilterMetadata | None = None
    extensions: dict[str, dict[str, Any]] | None = None

    model_config = ConfigDict(extra="forbid")

    @staticmethod
    def allowed_semantic_types() -> set[str]:
        """Values accepted by the portable metadata contract."""
        return {
            "currency",
            "percentage",
            "proportion",
            "count",
            "duration",
            "data_size",
            "date",
            "timestamp",
            "identifier",
            "category",
            "url",
            "boolean",
            "number",
            "string",
        }


class MetricInfo(BaseModel):
    """A metric exposed by a semantic view."""

    id: str
    name: str
    type: str
    definition: str
    description: str | None
    aggregation: str
    metadata: ColumnMetadata | None = None


class DimensionInfo(BaseModel):
    """A dimension exposed by a semantic view."""

    id: str
    name: str
    type: str
    definition: str
    description: str | None
    grain: str | None
    metadata: ColumnMetadata | None = None


class ViewSummary(BaseModel):
    """Summary entry returned by ``/views/list`` (``{name, uid, features}``)."""

    name: str
    display_name: str | None = None
    uid: str
    features: list[str]


class ViewDetail(BaseModel):
    """Full semantic view returned by ``/views/{view}``."""

    name: str
    display_name: str | None = None
    uid: str
    features: list[str]
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


@router.post(
    "/views/list",
    response_model=list[ViewSummary],
    response_model_exclude_none=True,
)
async def list_views(
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> list[ViewSummary] | JSONResponse:
    """List the semantic views (DJ cubes) available to the caller.

    The spec's ``/views/list`` returns summaries only (``{name, uid, features}``);
    full metrics/dimensions are fetched per-view via ``/views/{view}``.
    """
    try:
        cubes = await Node.find_names_with_display_names(
            session,
            node_type=NodeType.CUBE,
        )
    except DJException as exc:
        return _problem(exc.http_status_code or 400, exc.message)
    return [
        ViewSummary(
            name=cube_name,
            display_name=display_name,
            uid=cube_name,
            features=[],
        )
        for cube_name, display_name in cubes
    ]


@router.post(
    "/views/{view_name}",
    response_model=ViewDetail,
    response_model_exclude_none=True,
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

    # Delegate to the shared metrics-SQL core (the same helper ``/sql/metrics/v3``
    # uses). Pinning the view's cube via ``matched_cube`` makes the dialect
    # resolve from that cube's own availability and applies its stored
    # ``cube_filters``; ``dialect=None`` lets the helper do that resolution.
    generated_sql = await generate_metrics_sql(
        session,
        metrics=payload.metrics,
        dimensions=payload.dimensions,
        filters=request_filters,
        matched_cube=cube_rev,
        orderby=orderby,
        limit=limit,
        use_materialized=True,
        dialect=None,
        endpoint="/semantic-layer/views/sql",
    )
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


@router.post("/views/{view_name}/sql", response_model=GeneratedSQLResponse)
async def generate_query_sql(
    view_name: str,
    body: QueryRequest,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> GeneratedSQLResponse | JSONResponse:
    """Generate physical SQL for a view query."""
    payload = body.query
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
    if not payload.metrics:
        return _problem(
            400,
            "A query must request at least one metric. Dimension-only queries "
            "(distinct values) are not supported on this endpoint.",
        )
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
        result = await _generate_sql(session, payload, cube_rev)
    except DJException as exc:
        return _problem(exc.http_status_code or 400, exc.message)
    return result
