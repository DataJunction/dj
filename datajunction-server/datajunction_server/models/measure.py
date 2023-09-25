"""
Models for measures.
"""
import enum
from typing import TYPE_CHECKING, List, Optional

from pydantic.class_validators import root_validator
from sqlalchemy.sql.schema import Column as SqlaColumn
from sqlalchemy.types import Enum, String
from sqlmodel import Field, Relationship

from datajunction_server.models.base import BaseSQLModel, generate_display_name

if TYPE_CHECKING:
    from datajunction_server.models import Column


class AggregationRule(str, enum.Enum):
    """
    Type of allowed aggregation for a given measure.
    """

    ADDITIVE = "additive"
    NON_ADDITIVE = "non-additive"
    SEMI_ADDITIVE = "semi-additive"


class NodeColumn(BaseSQLModel):
    """
    Defines a column on a node
    """

    node: str
    column: str


class CreateMeasure(BaseSQLModel):
    """
    Input for creating a measure
    """

    name: str
    display_name: Optional[str]
    description: Optional[str]
    columns: List[NodeColumn]
    additive: AggregationRule = AggregationRule.NON_ADDITIVE


class EditMeasure(BaseSQLModel):
    """
    Editable fields on a measure
    """

    display_name: Optional[str]
    description: Optional[str]
    columns: Optional[List[NodeColumn]]
    additive: Optional[AggregationRule]


class Measure(BaseSQLModel, table=True):  # type: ignore
    """
    Measure class.

    Measure is a basic data modelling concept that helps with making Metric nodes portable,
    that is, so they can be computed on various DJ nodes using the same Metric definitions.

    By default, if a node column is not a Dimension or Dimension attribute then it should
    be a Measure.
    """

    __tablename__ = "measures"

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(unique=True)
    display_name: Optional[str] = Field(
        sa_column=SqlaColumn(
            "display_name",
            String,
            default=generate_display_name("name"),
        ),
    )
    description: Optional[str]
    columns: List["Column"] = Relationship(
        back_populates="measure",
        sa_relationship_kwargs={
            "lazy": "joined",
        },
    )
    additive: AggregationRule = Field(
        default=AggregationRule.NON_ADDITIVE,
        sa_column=SqlaColumn(Enum(AggregationRule)),
    )


class ColumnOutput(BaseSQLModel):
    """
    A simplified column schema, without ID or dimensions.
    """

    name: str
    type: str
    node: str

    @root_validator(pre=True)
    def transform(cls, values):  # pylint: disable=no-self-argument
        """
        Transforms the values for output
        """
        return {
            "name": values.get("name"),
            "type": str(values.get("type")),
            "node": values.get("node_revisions")[0].name,
        }


class MeasureOutput(BaseSQLModel):
    """
    Output model for measures
    """

    name: str
    display_name: Optional[str]
    description: Optional[str]
    columns: List[ColumnOutput]
    additive: AggregationRule
