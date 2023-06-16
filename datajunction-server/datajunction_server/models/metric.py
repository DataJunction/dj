"""
Models for metrics.
"""
from typing import List, Optional

from sqlmodel import SQLModel

from datajunction_server.models.engine import Dialect
from datajunction_server.models.node import DimensionAttributeOutput, Node
from datajunction_server.models.query import ColumnMetadata
from datajunction_server.sql.dag import get_dimensions
from datajunction_server.typing import UTCDatetime


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
