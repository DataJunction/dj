"""
Models for measures.
"""

from typing import TYPE_CHECKING, List, Optional

from pydantic.main import BaseModel
from pydantic import ConfigDict, Field, model_validator

from datajunction_server.enum import StrEnum
from datajunction_server.models.cube_materialization import (
    AggregationRule as MeasureAggregationRule,
)


if TYPE_CHECKING:
    pass


class AggregationRule(StrEnum):
    """
    Type of allowed aggregation for a given measure.
    """

    ADDITIVE = "additive"
    NON_ADDITIVE = "non-additive"
    SEMI_ADDITIVE = "semi-additive"


class NodeColumn(BaseModel):
    """
    Defines a column on a node
    """

    node: str
    column: str


class CreateMeasure(BaseModel):
    """
    Input for creating a measure
    """

    name: str
    display_name: Optional[str] = None
    description: Optional[str] = None
    columns: List[NodeColumn] = Field(default_factory=list)
    additive: AggregationRule = AggregationRule.NON_ADDITIVE


class EditMeasure(BaseModel):
    """
    Editable fields on a measure
    """

    display_name: Optional[str] = None
    description: Optional[str] = None
    columns: Optional[List[NodeColumn]] = None
    additive: Optional[AggregationRule] = None


class ColumnOutput(BaseModel):
    """
    A simplified column schema, without ID or dimensions.
    """

    name: str
    type: str
    node: str

    @model_validator(mode="before")
    def transform(cls, column):
        """
        Transforms the values for output
        """
        if isinstance(column, dict):
            return {  # pragma: no cover
                "name": column.get("name"),
                "type": str(column.get("type")),
                "node": column.get("node_revisions")[0].name,
            }
        return {
            "name": column.name,
            "type": str(column.type),
            "node": column.node_revision.name,
        }

    model_config = ConfigDict(from_attributes=True)


class MeasureOutput(BaseModel):
    """
    Output model for measures
    """

    name: str
    display_name: Optional[str] = None
    description: Optional[str] = None
    columns: List[ColumnOutput] = Field(default_factory=list)
    additive: AggregationRule

    model_config = ConfigDict(from_attributes=True)


class NodeRevisionNameVersion(BaseModel):
    """
    Node name and version
    """

    name: str
    version: str

    model_config = ConfigDict(from_attributes=True)


class FrozenMeasureOutput(BaseModel):
    """
    The output fields when listing frozen measure metadata
    """

    name: str
    expression: str
    aggregation: str
    rule: MeasureAggregationRule
    upstream_revision: NodeRevisionNameVersion
    used_by_node_revisions: list[NodeRevisionNameVersion]

    model_config = ConfigDict(from_attributes=True)


class FrozenMeasureKey(BaseModel):
    """
    Base frozen measure fields.
    """

    name: str
    expression: str
    aggregation: str
    rule: MeasureAggregationRule
    upstream_revision: NodeRevisionNameVersion

    model_config = ConfigDict(from_attributes=True)
