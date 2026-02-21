"""
Models for generated SQL
"""

from typing import List, Optional

from datajunction_server.transpilation import transpile_sql
from pydantic import field_validator
from pydantic.main import BaseModel

from datajunction_server.errors import DJQueryBuildError
from datajunction_server.models.cube_materialization import MetricComponent
from datajunction_server.models.engine import Dialect
from datajunction_server.models.node_type import NodeNameVersion
from datajunction_server.models.query import ColumnMetadata, V3ColumnMetadata


class SourceScanInfo(BaseModel):
    """
    Information about a source table scan.

    Provides table-level scan metrics. All size/partition fields are optional
    and only included when availability metadata is available.
    """

    source_name: str  # Node name (e.g., "source.sales_fact")

    # Physical table location (from availability state)
    catalog: Optional[str] = None
    schema_: Optional[str] = None  # underscore to avoid Python keyword
    table: Optional[str] = None

    # Table size (None if no availability metadata)
    total_bytes: Optional[int] = None  # Total table size (all partitions)

    # Partition information (None if no availability metadata)
    partition_columns: List[
        str
    ] = []  # Partition columns (e.g., ["utc_date", "region"])
    total_partition_count: Optional[int] = None  # Total partitions in table

    # Filter-based estimates (optional - only included when accurate)
    scan_bytes: Optional[int] = None  # Bytes that will be scanned (after filters)
    scan_percentage: Optional[float] = None  # scan_bytes / total_bytes
    scanned_partition_count: Optional[int] = None  # Partitions that will be scanned


class ScanEstimate(BaseModel):
    """
    Estimate of data scan size for a query.

    total_bytes is None if no sources have size metadata available.
    """

    total_bytes: Optional[int]  # None if no size metadata available for any source
    sources: List[SourceScanInfo]
    has_materialization: bool = False  # Whether materialized tables are being used


class TranspiledSQL(BaseModel):
    """
    Generated SQL for a given node, the output of a QueryBuilder(...).build() call.
    """

    sql: str
    dialect: Optional[Dialect] = None

    @classmethod
    def create(cls, *, dialect, **kwargs):
        sql = transpile_sql(kwargs["sql"], dialect)
        return cls(
            sql=sql,
            dialect=dialect,
            **{k: v for k, v in kwargs.items() if k not in {"sql", "dialect"}},
        )

    @field_validator("dialect", mode="before")
    def validate_dialect(cls, v):
        if v is None:
            return None
        return Dialect(v)


class GeneratedSQL(TranspiledSQL):
    """
    Generated SQL for a given node, the output of a QueryBuilder(...).build() call.
    """

    node: NodeNameVersion
    sql: str
    columns: Optional[List[ColumnMetadata]] = None  # pragma: no-cover
    grain: list[str] | None = None
    dialect: Optional[Dialect] = None
    upstream_tables: Optional[List[str]] = None
    metrics: dict[str, tuple[list[MetricComponent], str]] | None = None
    spark_conf: dict[str, str] | None = None
    errors: Optional[List[DJQueryBuildError]] = None
    scan_estimate: Optional[ScanEstimate] = None


class ComponentResponse(BaseModel):
    """Response model for a metric component in measures SQL."""

    name: str  # Component name (e.g., "unit_price_sum")
    expression: str  # The raw SQL expression (e.g., "unit_price")
    aggregation: Optional[str] = None  # Phase 1: "SUM", "COUNT", etc.
    merge: Optional[str] = (
        None  # Phase 2 (re-aggregation): "SUM", "COUNT_DISTINCT", etc.
    )
    aggregability: str  # "FULL", "LIMITED", or "NONE"


class MetricFormulaResponse(BaseModel):
    """Response model for a metric's combiner formula."""

    name: str  # Full metric name (e.g., "v3.avg_unit_price")
    short_name: str  # Short name (e.g., "avg_unit_price")
    query: str  # Original metric query (e.g., "SELECT AVG(unit_price) FROM ...")
    combiner: str  # Formula combining components (e.g., "SUM(unit_price_sum) / SUM(unit_price_count)")
    components: List[str]  # Component names used in this metric
    is_derived: bool  # True if metric is derived from other metrics
    parent_name: Optional[str] = None  # Source fact/transform node name


class GrainGroupResponse(BaseModel):
    """Response model for a single grain group in measures SQL."""

    sql: str
    columns: List[V3ColumnMetadata]  # Clean V3 column metadata
    grain: List[str]
    aggregability: str
    metrics: List[str]
    components: List[
        ComponentResponse
    ]  # Metric components for materialization planning
    parent_name: str  # Source fact/transform node name
    scan_estimate: Optional[ScanEstimate] = None  # Per-query scan estimate


class MeasuresSQLResponse(BaseModel):
    """Response model for V3 measures SQL with multiple grain groups."""

    grain_groups: List[GrainGroupResponse]
    metric_formulas: List[MetricFormulaResponse]  # How metrics combine components
    dialect: Optional[str] = None
    requested_dimensions: List[str]


class CombinedMeasuresSQLResponse(BaseModel):
    """
    Response model for combined measures SQL.

    This endpoint combines multiple grain groups into a single SQL query
    using FULL OUTER JOIN on shared dimensions with COALESCE for dimension columns.
    """

    sql: str  # Combined SQL query
    columns: List[V3ColumnMetadata]  # Output columns with semantic metadata
    grain: List[str]  # Shared grain columns (dimensions)
    grain_groups_combined: int  # Number of grain groups that were combined
    dialect: Optional[str] = None
    use_preagg_tables: (
        bool  # If True, data is read from pre-agg tables; if False, from source tables
    )
    source_tables: List[str]  # Tables being read (pre-agg tables or source tables)
