"""
Models for cubes.
"""

from typing import List, Optional

from pydantic import Field, model_validator, ConfigDict
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
    partition: Optional[PartitionOutput] = None

    @model_validator(mode="before")
    def type_string(cls, values):
        """
        Extracts the type as a string
        """
        if isinstance(values, dict):
            return values

        # Create a new dict, don't modify the original object or it could
        # overwrite the SQLAlchemy model
        data = {}
        if hasattr(values, "__dict__"):  # pragma: no cover
            data.update(values.__dict__)

        if hasattr(values, "node_revision"):  # pragma: no cover
            data["node_name"] = values.node_revision.name
            data["type"] = (
                values.node_revision.type
                if values.node_revision.type == NodeType.METRIC
                else NodeType.DIMENSION
            )
        return data

    model_config = ConfigDict(from_attributes=True)

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
    query: Optional[str] = None
    columns: List[ColumnOutput]
    sql_columns: Optional[List[ColumnOutput]] = None
    updated_at: UTCDatetime
    materializations: List[MaterializationConfigOutput]
    tags: Optional[List[TagOutput]] = None
    measures: list[MetricMeasures] | None = None

    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True,
    )

    @classmethod
    def from_cube_revision(cls, cube: "NodeRevision"):
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
        cube_metadata = cls.model_validate(cube)

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
