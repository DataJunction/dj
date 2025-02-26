"""Node type"""

from pydantic import BaseModel

from datajunction_server.enum import StrEnum


class NodeType(StrEnum):
    """
    Node type.

    A node can have 4 types, currently:

    1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.
    2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.
    3. METRIC nodes are leaves in the DAG, and have a single aggregation query.
    4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.
    5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.
    """

    SOURCE = "source"
    TRANSFORM = "transform"
    METRIC = "metric"
    DIMENSION = "dimension"
    CUBE = "cube"


class NodeNameOutput(BaseModel):
    """
    Node name only
    """

    name: str

    class Config:
        orm_mode = True


class NodeNameVersion(BaseModel):
    """
    Node name and version
    """

    name: str
    version: str
    display_name: str | None

    class Config:
        orm_mode = True
