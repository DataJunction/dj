"""
Models for metrics.
"""

from typing import List, Optional

from pydantic.main import BaseModel

from datajunction_server.database.node import Node
from datajunction_server.models.cube_materialization import MetricComponent
from datajunction_server.models.engine import Dialect
from datajunction_server.models.node import (
    DimensionAttributeOutput,
    MetricMetadataOutput,
)
from datajunction_server.models.query import ColumnMetadata
from datajunction_server.models.sql import TranspiledSQL
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
    upstream_node: str
    expression: str

    dimensions: List[DimensionAttributeOutput]
    metric_metadata: Optional[MetricMetadataOutput] = None
    required_dimensions: List[str]

    incompatible_druid_functions: List[str]

    measures: List[MetricComponent]
    derived_query: str
    derived_expression: str

    @classmethod
    def parse_node(cls, node: Node, dims: List[DimensionAttributeOutput]) -> "Metric":
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
        extractor = MetricComponentExtractor.from_query_string(node.current.query)
        measures, derived_sql = extractor.extract()
        return cls(
            id=node.id,
            name=node.name,
            display_name=node.current.display_name,
            current_version=node.current_version,
            description=node.current.description,
            created_at=node.created_at,
            updated_at=node.current.updated_at,
            query=node.current.query,
            upstream_node=node.current.parents[0].name,
            expression=str(query_ast.select.projection[0]),
            dimensions=dims,
            metric_metadata=node.current.metric_metadata,
            required_dimensions=[dim.name for dim in node.current.required_dimensions],
            incompatible_druid_functions=incompatible_druid_functions,
            measures=measures,
            derived_query=str(derived_sql).strip(),
            derived_expression=str(derived_sql.select.projection[0]).strip(),
        )


class TranslatedSQL(TranspiledSQL):
    """
    Class for SQL generated from a given metric.
    """

    # TODO: once type-inference is added to /query/ endpoint
    # columns attribute can be required
    sql: str
    columns: Optional[List[ColumnMetadata]] = None  # pragma: no-cover
    dialect: Optional[Dialect] = None
    upstream_tables: Optional[List[str]] = None

    @classmethod
    def create(cls, *, dialect: Dialect | None = None, **kwargs):
        sql = transpile_sql(kwargs["sql"], dialect)
        return cls(
            sql=sql,
            dialect=dialect,
            **{k: v for k, v in kwargs.items() if k not in {"sql", "dialect"}},
        )
