"""
Models for metrics.
"""

from __future__ import annotations

from pydantic.main import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.node import Node
from datajunction_server.errors import DJWarning
from datajunction_server.models.cube_materialization import MetricComponent
from datajunction_server.models.engine import Dialect
from datajunction_server.models.node import (
    DimensionAttributeOutput,
    MetricMetadataOutput,
)
from datajunction_server.models.query import ColumnMetadata, V3ColumnMetadata
from datajunction_server.models.semiadditive import SemiAdditiveSpec
from datajunction_server.models.sql import ScanEstimate, TranspiledSQL
from datajunction_server.models.unit import unit_to_dict
from datajunction_server.sql.decompose import MetricComponentExtractor
from datajunction_server.sql.parsing.backends.antlr4 import ast, parse
from datajunction_server.transpilation import transpile_sql
from datajunction_server.typing import UTCDatetime


class Metric(BaseModel):
    """
    Class for a metric.
    """

    id: int
    name: str
    display_name: str
    current_version: str
    description: str = ""

    created_at: UTCDatetime
    updated_at: UTCDatetime

    query: str
    upstream_node: str | None = None
    expression: str

    dimensions: list[DimensionAttributeOutput]
    metric_metadata: MetricMetadataOutput | None = None
    # Structured metric-level unit, derived from the metric's output column.
    # `metric_metadata.unit` remains the legacy flat-string field for
    # back-compat; `unit` is the structured shape and is the source of truth
    # going forward. `None` when no unit is set, regardless of input shape.
    unit: dict | None = None
    required_dimensions: list[str]
    semi_additive: SemiAdditiveSpec | None = None

    # Whether the metric is a single aggregation call (a "measure") that can map
    # 1:1 to a column in an externally-built pre-aggregation table. Derived/ratio
    # metrics are not measures. Computed on the fly from the metric's expression.
    is_measure: bool

    incompatible_druid_functions: list[str]

    measures: list[MetricComponent]
    derived_query: str
    derived_expression: str

    custom_metadata: dict | None = None

    @classmethod
    async def parse_node(
        cls,
        node: Node,
        dims: list[DimensionAttributeOutput],
        session: AsyncSession,
    ) -> Metric:
        """
        Parses a node into a metric.
        """
        query_ast = parse(node.current.query)
        functions = [func.function() for func in query_ast.find_all(ast.Function)]
        incompatible_druid_functions = [
            func.__name__.upper()
            for func in functions
            if Dialect.DRUID not in func.dialects
        ]
        extractor = MetricComponentExtractor(node.current.id)
        measures, derived_sql = await extractor.extract(session)
        return cls(
            id=node.id,
            name=node.name,
            display_name=node.current.display_name,  # type: ignore
            current_version=node.current_version,
            description=node.current.description,
            created_at=node.created_at,
            updated_at=node.current.updated_at,
            query=node.current.query,  # type: ignore
            upstream_node=(
                node.current.non_metric_parents[0].name
                if node.current.non_metric_parents
                else None
            ),
            expression=str(query_ast.select.projection[0]),
            dimensions=dims,
            metric_metadata=node.current.metric_metadata,
            unit=unit_to_dict(
                node.current.columns[0].unit if node.current.columns else None,
            ),
            required_dimensions=[dim.name for dim in node.current.required_dimensions],
            semi_additive=node.current.semi_additive,
            is_measure=node.current.is_measure,
            incompatible_druid_functions=incompatible_druid_functions,
            measures=measures,
            derived_query=str(derived_sql).strip(),
            derived_expression=str(derived_sql.select.projection[0]).strip(),
            custom_metadata=node.current.custom_metadata,
        )


class TranslatedSQL(TranspiledSQL):
    """
    Class for SQL generated from a given metric.
    """

    # TODO: once type-inference is added to /query/ endpoint
    # columns attribute can be required
    sql: str
    columns: list[ColumnMetadata] | None = None  # pragma: no-cover
    dialect: Dialect | None = None
    upstream_tables: list[str] | None = None
    scan_estimate: ScanEstimate | None = None
    # Populated on the v3 build path only; the v2 builder raises no warnings.
    warnings: list[DJWarning] = []

    @classmethod
    def create(cls, *, dialect: Dialect | None = None, **kwargs):
        sql = transpile_sql(kwargs["sql"], dialect)
        return cls(
            sql=sql,
            dialect=dialect,
            **{k: v for k, v in kwargs.items() if k not in {"sql", "dialect"}},
        )


class V3TranslatedSQL(BaseModel):
    """
    SQL response model for V3 SQL generation endpoints.

    This is a cleaner response model specifically for V3 that:
    - Uses V3ColumnMetadata (no legacy column/node fields)
    - Has required fields (not optional like legacy TranslatedSQL)
    """

    sql: str
    columns: list[V3ColumnMetadata]
    dialect: Dialect

    # If a cube was used, contains cube name (fetch details via /cubes/{name}/)
    cube_name: str | None = None

    # Scan estimate (aggregated from all grain groups)
    scan_estimate: ScanEstimate | None = None

    warnings: list[DJWarning] = []
