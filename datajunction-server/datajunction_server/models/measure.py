"""
Models for measures.
"""
from typing import TYPE_CHECKING, List, Optional

from pydantic.class_validators import root_validator
from pydantic.main import BaseModel
from sqlalchemy import BigInteger, Integer
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import Enum, String

from datajunction_server.database.connection import Base
from datajunction_server.enum import StrEnum
from datajunction_server.models.base import labelize

if TYPE_CHECKING:
    from datajunction_server.models import Column


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
    display_name: Optional[str]
    description: Optional[str]
    columns: List[NodeColumn]
    additive: AggregationRule = AggregationRule.NON_ADDITIVE


class EditMeasure(BaseModel):
    """
    Editable fields on a measure
    """

    display_name: Optional[str]
    description: Optional[str]
    columns: Optional[List[NodeColumn]]
    additive: Optional[AggregationRule]


class Measure(Base):  # type: ignore  # pylint: disable=too-few-public-methods
    """
    Measure class.

    Measure is a basic data modelling concept that helps with making Metric nodes portable,
    that is, so they can be computed on various DJ nodes using the same Metric definitions.

    By default, if a node column is not a Dimension or Dimension attribute then it should
    be a Measure.
    """

    __tablename__ = "measures"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
    )
    name: Mapped[str] = mapped_column(unique=True)
    display_name: Mapped[str] = mapped_column(
        String,
        insert_default=lambda context: labelize(context.current_parameters.get("name")),
    )
    description: Mapped[Optional[str]]
    columns: Mapped[List["Column"]] = relationship(
        back_populates="measure",
        lazy="joined",
    )
    additive: Mapped[AggregationRule] = mapped_column(
        Enum(AggregationRule),
        default=AggregationRule.NON_ADDITIVE,
    )


class ColumnOutput(BaseModel):
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

    class Config:  # pylint: disable=missing-class-docstring, too-few-public-methods
        orm_mode = True


class MeasureOutput(BaseModel):
    """
    Output model for measures
    """

    name: str
    display_name: Optional[str]
    description: Optional[str]
    columns: List[ColumnOutput]
    additive: AggregationRule

    class Config:  # pylint: disable=missing-class-docstring, too-few-public-methods
        orm_mode = True
