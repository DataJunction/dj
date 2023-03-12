"""
Models for metrics.
"""
from typing import List

from sqlmodel import SQLModel

from dj.models.node import Node
from dj.sql.dag import get_dimensions
from dj.typing import UTCDatetime


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

    dimensions: List[str]

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

    sql: str
