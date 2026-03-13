"""Node type"""

from pydantic import BaseModel, ConfigDict

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

    model_config = ConfigDict(from_attributes=True)


class NodeTypeDisplay(BaseModel):
    """
    Minimal node info needed to render a node card in the dimensions DAG.
    """

    name: str
    display_name: str | None = None
    type: NodeType | None = None

    model_config = ConfigDict(from_attributes=True)


class DimensionDAGEdge(BaseModel):
    """A directed edge in the dimension DAG: source node links to target dimension."""

    source: str
    target: str


class DimensionDAGOutput(BaseModel):
    """
    Response for GET /dimensions/{name}/dag.

    inbound            - nodes whose dimension links point to this dimension (consumers).
    inbound_edges      – edges between inbound nodes (source links to target), preserving
                         the multi-level graph structure discovered by BFS.
    outbound           – dimension nodes that this node links to (its own dimension_links).
    outbound_edges     – edges between outbound dimension nodes (source links to target).
    """

    inbound: list[NodeTypeDisplay]
    inbound_edges: list[DimensionDAGEdge]
    outbound: list[NodeTypeDisplay]
    outbound_edges: list[DimensionDAGEdge]


class NodeNameVersion(BaseModel):
    """
    Node name and version
    """

    name: str
    version: str
    display_name: str | None = None

    model_config = ConfigDict(from_attributes=True)
