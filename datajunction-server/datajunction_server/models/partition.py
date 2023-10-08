"""Partition-related models."""
import enum
from typing import TYPE_CHECKING, List, Optional

from pydantic.class_validators import validator
from pydantic.main import BaseModel
from sqlalchemy import JSON
from sqlalchemy import Column as SqlaColumn
from sqlmodel import Field, Relationship, SQLModel

from datajunction_server.models.base import BaseSQLModel
from datajunction_server.models.column import Column

if TYPE_CHECKING:
    from datajunction_server.models.materialization import Materialization


class PartitionType(str, enum.Enum):
    """
    Partition type.

    A partition can be temporal or categorical
    """

    TEMPORAL = "temporal"
    CATEGORICAL = "categorical"


class Partition(BaseSQLModel, table=True):
    """
    A partition specification consists of a reference to a partition column and a partition type
    (either temporal or categorical). Both partition types indicate how to partition the
    materialized dataset, which the configured materializations will use when building
    materialization jobs. The temporal partition additionally tells us how to incrementally
    materialize the node, with the ongoing materialization job operating on the latest partitions.

    An expression can be optionally provided for temporal partitions, which evaluates to the
    temporal partition for scheduled runs. This is typically used to configure a specific timestamp
    format for the partition column, i.e., CAST(FORMAT(DJ_LOGICAL_TIMESTAMP(), "yyyyMMdd") AS INT)
    would yield a date integer from the current processing partition.
    """

    id: Optional[int] = Field(default=None, primary_key=True)

    # The column reference that this partition is defined on
    column_id: int = Field(foreign_key="column.id")
    column: Column = Relationship(
        back_populates="partition",
        sa_relationship_kwargs={
            "primaryjoin": "Column.id==Partition.column_id",
        },
    )

    # This expression evaluates to the temporal partition value for scheduled runs
    # defaults to CAST(FORMAT(DJ_LOGICAL_TIMESTAMP(), "yyyyMMdd") AS INT)
    expression: Optional[str]
    type_: PartitionType


class PartitionInput(BaseSQLModel):
    """
    Used for setting partition columns on a node
    """

    expression: Optional[str]
    type_: PartitionType


class PartitionBackfill(BaseModel):
    """
    Used for setting backfilled values
    """

    column_name: str

    # Backfilled values and range. Most temporal partitions will just use `range`, but some may
    # optionally use `values` to specify specific values
    # Ex: values: [20230901]
    #     range: [20230901, 20231001]
    values: Optional[List]
    range: Optional[List]


class PartitionOutput(SQLModel):
    """
    Output for partition
    """

    type_: PartitionType
    expression: Optional[str]


class Backfill(BaseSQLModel, table=True):
    """
    A backfill run is linked to a materialization config, where users provide the range
    (of a temporal partition) to backfill for the node.
    """

    id: Optional[int] = Field(default=None, primary_key=True)

    # The column reference that this partition is defined on
    materialization_id: int = Field(foreign_key="materialization.id")
    materialization: "Materialization" = Relationship(
        back_populates="backfills",
        sa_relationship_kwargs={
            "primaryjoin": "Materialization.id==Backfill.materialization_id",
        },
    )

    # Backfilled values and range
    spec: Optional[PartitionBackfill] = Field(
        default={},
        sa_column=SqlaColumn(JSON),
    )

    urls: Optional[List[str]] = Field(
        default=[],
        sa_column=SqlaColumn(JSON),
    )

    @validator("spec")
    def val_spec(cls, val):
        return val.dict()


class BackfillOutput(BaseModel):
    """
    Output model for backfills
    """

    spec: Optional[PartitionBackfill]
    urls: Optional[List[str]]
