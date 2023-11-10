"""
Models for metrics.
"""
from typing import List, Optional

from pydantic.class_validators import root_validator
from sqlmodel import SQLModel

from datajunction_server.models.engine import Dialect
from datajunction_server.models.node import (
    DimensionAttributeOutput,
    MetricMetadataOutput,
    Node,
)
from datajunction_server.models.query import ColumnMetadata
from datajunction_server.sql.dag import get_dimensions
from datajunction_server.transpilation import get_transpilation_plugin
from datajunction_server.typing import UTCDatetime
from datajunction_server.utils import get_settings


class Metric(SQLModel):
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

    dimensions: List[DimensionAttributeOutput]
    metric_metadata: Optional[MetricMetadataOutput] = None

    @classmethod
    def parse_node(cls, node: Node) -> "Metric":
        """
        Parses a node into a metric.
        """

        return cls(
            **node.dict(),
            description=node.current.description,
            updated_at=node.current.updated_at,
            query=node.current.query,
            dimensions=get_dimensions(node),
            metric_metadata=node.current.metric_metadata,
        )


class TranslatedSQL(SQLModel):
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
        if settings.sql_transpilation_library:
            plugin = get_transpilation_plugin(settings.sql_transpilation_library)
            values["sql"] = plugin.transpile_sql(
                values["sql"],
                input_dialect=Dialect.SPARK,
                output_dialect=values["dialect"],
            )
        return values
