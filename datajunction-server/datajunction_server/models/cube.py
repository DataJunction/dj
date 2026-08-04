"""
Models for cubes.
"""

from pydantic import ConfigDict, Field, model_validator
from pydantic.main import BaseModel

from datajunction_server.database.node import NodeRevision
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
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.partition import PartitionOutput
from datajunction_server.models.tag import TagOutput
from datajunction_server.naming import SEPARATOR, amenable_name
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
    partition: PartitionOutput | None = None
    role: str | None = None

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
        if self.type == "metric":
            query_column_name = self.name
        else:
            full_name = f"{self.node_name}{SEPARATOR}{self.name}"
            # Include the role so two roles on one column get distinct names.
            if self.role:
                full_name = f"{full_name}[{self.role}]"
            query_column_name = amenable_name(full_name)
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
    availability: AvailabilityStateBase | None = None
    cube_elements: list[CubeElementMetadata]
    cube_node_metrics: list[str]
    cube_node_dimensions: list[str]
    cube_filters: list[str] | None = None
    query: str | None = None
    columns: list[ColumnOutput]
    sql_columns: list[ColumnOutput] | None = None
    updated_at: UTCDatetime
    materializations: list[MaterializationConfigOutput]
    tags: list[TagOutput] | None = None
    custom_metadata: dict | None = None
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
        # Parse the database object into a pydantic object
        cube_metadata = cls.model_validate(cube)

        # Cube columns are the ordered, role-aware source of truth. The
        # cube_elements many-to-many intentionally stores each referenced DB
        # column once, so reconstruct one metadata entry per cube column.
        elements_by_column_name = {
            (
                element.node_name
                if element.type == "metric"
                else f"{element.node_name}{SEPARATOR}{element.name}"
            ): element
            for element in cube_metadata.cube_elements
        }
        expanded_elements: list[CubeElementMetadata] = []
        for column in cube.columns:
            element = elements_by_column_name.get(column.name)
            if element is None:  # pragma: no cover - database invariant
                continue
            if element.type == "metric":
                expanded_elements.append(element)
                continue
            role = (
                column.dimension_column.strip("[]") if column.dimension_column else None
            )
            expanded_elements.append(element.model_copy(update={"role": role}))
        cube_metadata.cube_elements = expanded_elements

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


class ViewDDLDialect(BaseModel):
    """DDL for a single dialect (Spark or Trino)."""

    versioned_view_name: str
    unversioned_view_name: str
    versioned_ddl: str
    unversioned_ddl: str


class ViewDDLResponse(BaseModel):
    """
    Response for the view DDL endpoint.
    Contains CREATE OR REPLACE VIEW statements for both Spark and Trino dialects.
    """

    spark: ViewDDLDialect
    trino: ViewDDLDialect
    is_materialized: bool


class DimensionValue(BaseModel):
    """
    Dimension value and count
    """

    value: list[str]
    count: int | None


class DimensionValues(BaseModel):
    """
    Dimension values
    """

    dimensions: list[str]
    values: list[DimensionValue]
    cardinality: int
