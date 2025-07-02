"""
Models for measures.
"""

from typing import TYPE_CHECKING, List, Optional

from pydantic.class_validators import root_validator
from pydantic.main import BaseModel

from datajunction_server.enum import StrEnum
from pydantic import model_validator, ConfigDict

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
    columns: List[NodeColumn]
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
    @classmethod
    def transform(cls, values):
        """
        Transforms the values for output
        """
        return {
            "name": values.get("name"),
            "type": str(values.get("type")),
            "node": values.get("node_revisions")[0].name,
        }
    model_config = ConfigDict(from_attributes=True)


class MeasureOutput(BaseModel):
    """
    Output model for measures
    """

    name: str
    display_name: Optional[str] = None
    description: Optional[str] = None
    columns: List[ColumnOutput]
    additive: AggregationRule
    model_config = ConfigDict(from_attributes=True)
