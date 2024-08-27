"""
Models for metrics.
"""
from typing import List, Optional

from pydantic.class_validators import root_validator
from pydantic.main import BaseModel

from datajunction_server.database.node import Node
from datajunction_server.models.engine import Dialect
from datajunction_server.models.node import (
    DimensionAttributeOutput,
    MetricMetadataOutput,
)
from datajunction_server.models.query import ColumnMetadata
from datajunction_server.sql.parsing.backends.antlr4 import ast, parse
from datajunction_server.transpilation import get_transpilation_plugin
from datajunction_server.typing import UTCDatetime
from datajunction_server.utils import get_settings


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
        )


class TranslatedSQL(BaseModel):
    """
    Class for SQL generated from a given metric.
    """

    # TODO: once type-inference is added to /query/ endpoint  # pylint: disable=fixme
    # columns attribute can be required
    sql: str
    columns: Optional[List[ColumnMetadata]] = None  # pragma: no-cover
    dialect: Optional[Dialect] = None
    upstream_tables: Optional[List[str]] = None

    @root_validator(pre=False)
    def transpile_sql(  # pylint: disable=no-self-argument
        cls,
        values,
    ) -> "TranslatedSQL":
        """
        Transpiles SQL to the specified dialect with the configured transpilation plugin.
        If no plugin is configured, it will just return the original generated query.
        """
        settings = get_settings()
        if settings.sql_transpilation_library:  # pragma: no cover
            plugin = get_transpilation_plugin(settings.sql_transpilation_library)
            values["sql"] = plugin.transpile_sql(
                values["sql"],
                input_dialect=Dialect.SPARK,
                output_dialect=values["dialect"],
            )
        return values
