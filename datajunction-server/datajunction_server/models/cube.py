"""
Models for cubes.
"""

from typing import List, Optional

from pydantic import Field, root_validator
from pydantic.main import BaseModel

from datajunction_server.naming import SEPARATOR, from_amenable_name, amenable_name
from datajunction_server.models.materialization import MaterializationConfigOutput
from datajunction_server.models.measure import (
    FrozenMeasureKey,
    NodeRevisionNameVersion,
)
from datajunction_server.models.node import (
    AvailabilityStateBase,
    ColumnOutput,
    NodeMode,
    NodeStatus,
)
from datajunction_server.database.node import NodeRevision
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.partition import PartitionOutput
from datajunction_server.models.tag import TagOutput
from datajunction_server.typing import UTCDatetime


class MetricMeasures(BaseModel):
    metric: NodeRevisionNameVersion
    frozen_measures: list[FrozenMeasureKey]


class CubeElementMetadata(BaseModel):
    """
    Metadata for an element in a cube
    """

    name: str
    display_name: str
    node_name: str
    type: str
    partition: Optional[PartitionOutput]

    @root_validator(pre=True)
    def type_string(cls, values):
        """
        Extracts the type as a string
        """
        values = dict(values)
        if "node_revisions" in values:
            values["node_name"] = values["node_revisions"][0].name
            values["type"] = (
                values["node_revisions"][0].type
                if values["node_revisions"][0].type == NodeType.METRIC
                else NodeType.DIMENSION
            )
        return values

    class Config:
        orm_mode = True

    def derive_sql_column(self) -> ColumnOutput:
        """
        Derives the column name in the generated Cube SQL based on the CubeElement
        """
        query_column_name = (
            self.name
            if self.type == "metric"
            else amenable_name(
                f"{self.node_name}{SEPARATOR}{self.name}",
            )
        )
        return ColumnOutput(
            name=query_column_name,
            display_name=self.display_name,
            type=self.type,
        )


class CubeRevisionMetadata(BaseModel):
    """
    Metadata for a cube node
    """

    id: int = Field(alias="node_revision_id")
    node_id: int
    type: NodeType
    name: str
    display_name: str
    version: str
    status: NodeStatus
    mode: NodeMode
    description: str = ""
    availability: Optional[AvailabilityStateBase] = None
    cube_elements: List[CubeElementMetadata]
    cube_node_metrics: List[str]
    cube_node_dimensions: List[str]
    query: Optional[str]
    columns: List[ColumnOutput]
    sql_columns: Optional[List[ColumnOutput]]
    updated_at: UTCDatetime
    materializations: List[MaterializationConfigOutput]
    tags: Optional[List[TagOutput]]
    measures: list[MetricMeasures] | None = None

    class Config:
        allow_population_by_field_name = True
        orm_mode = True

    @classmethod
    def parse_obj(cls, cube: "NodeRevision"):
        """
        Converts a cube node revision into a cube revision metadata object
        """
        # Preserve the ordering of elements
        element_ordering = {col.name: col.order for col in cube.columns}
        cube.cube_elements = sorted(
            cube.cube_elements,
            key=lambda elem: element_ordering.get(from_amenable_name(elem.name), 0),
        )

        # Parse the database object into a pydantic object
        cube_metadata = cls.from_orm(cube)

        # Populate metric measures
        cube_metadata.measures = []
        for node_revision in cube.metric_node_revisions():
            if node_revision:  # pragma: no cover
                cube_metadata.measures.append(
                    MetricMeasures(
                        metric=node_revision,
                        frozen_measures=node_revision.frozen_measures,
                    ),
                )

        cube_metadata.tags = cube.node.tags
        cube_metadata.sql_columns = [
            element.derive_sql_column() for element in cube_metadata.cube_elements
        ]
        return cube_metadata


class DimensionValue(BaseModel):
    """
    Dimension value and count
    """

    value: List[str]
    count: Optional[int]


class DimensionValues(BaseModel):
    """
    Dimension values
    """

    dimensions: List[str]
    values: List[DimensionValue]
    cardinality: int
