# Column.update_forward_refs(Partition=Partition)
import enum
from typing import Optional, List

from sqlalchemy import Column as SqlaColumn, JSON
from sqlmodel import Field, Relationship, SQLModel

from datajunction_server.models.column import Column
from datajunction_server.models.base import BaseSQLModel


class PartitionType(str, enum.Enum):
    """
    Partition type.

    A partition can be temporal or categorical
    """

    TEMPORAL = "temporal"
    CATEGORICAL = "categorical"


class Partition(BaseSQLModel, table=True):
    """
    A partition specification tells the ongoing and backfill materialization jobs how to partition
    the materialized dataset and which partition values (a list or range of values) have been backfilled.
    Partitions may be temporal or categorical and will be handled differently depending on the type.

    For temporal partition types, the ongoing materialization job will continue to operate on the
    latest partitions and the partition values specified by `values` and `range` are only relevant
    to the backfill job.

    Examples:
        This will tell DJ to backfill for all values of the dateint partition:
          Partition(name=“dateint”, type="temporal", values=[], range=())
        This will tell DJ to backfill just 20230601 and 20230605:
          Partition(name=“dateint”, type="temporal", values=[20230601, 20230605], range=())
        This will tell DJ to backfill 20230601 and between 20220101 and 20230101:
          Partition(name=“dateint”, type="temporal", values=[20230601], range=(20220101, 20230101))

        For categorical partition types, the ongoing materialization job will *only* operate on the
        specified partition values in `values` and `range`:
            Partition(name=“group_id”, type="categorical", values=["a", "b", "c"], range=())
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
    # defaults to CAST(FORMAT(NOW(), "yyyyMMdd") AS INT)
    expression: Optional[str]

    # Backfilled values and range
    values: Optional[List[str]] = Field(
        default=[],
        sa_column=SqlaColumn(JSON),
    )
    range: Optional[List[str]] = Field(
        default=[],
        sa_column=SqlaColumn(JSON),
    )

    type_: PartitionType


class PartitionInput(BaseSQLModel):
    """
    Used for setting partition columns on a node
    """
    expression: Optional[str]
    type_: PartitionType


class PartitionBackfill(BaseSQLModel):
    """
    Used for setting backfilled values
    """
    column_name: str

    # Backfilled values and range
    values: Optional[List]
    range: Optional[List]


class PartitionOutput(SQLModel):
    """
    Output for partition
    """
    type_: PartitionType
    expression: Optional[str]
