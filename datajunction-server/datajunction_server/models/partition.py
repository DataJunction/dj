"""Partition-related models."""
from typing import TYPE_CHECKING, List, Optional

from pydantic.main import BaseModel
from sqlalchemy import JSON, BigInteger, Enum, Integer
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql.schema import ForeignKey

from datajunction_server.database.connection import Base
from datajunction_server.enum import StrEnum
from datajunction_server.models.column import Column
from datajunction_server.sql.parsing.types import TimestampType

if TYPE_CHECKING:
    from datajunction_server.models.materialization import Materialization


class PartitionType(StrEnum):
    """
    Partition type.

    A partition can be temporal or categorical
    """

    TEMPORAL = "temporal"
    CATEGORICAL = "categorical"


class Granularity(StrEnum):
    """
    Time dimension granularity.
    """

    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"


class PartitionInput(BaseModel):
    """
    Expected settings for specifying a partition column
    """

    type_: PartitionType

    #
    # Temporal partitions will additionally have the following properties:
    #
    # Timestamp granularity
    granularity: Optional[Granularity]
    # Timestamp format
    format: Optional[str]


class Partition(Base):  # type: ignore  # pylint: disable=too-few-public-methods
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

    __tablename__ = "partition"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
    )

    type_: Mapped[PartitionType] = mapped_column(Enum(PartitionType))

    #
    # Temporal partitions will additionally have the following properties:
    #
    # Timestamp granularity
    granularity: Mapped[Optional[Granularity]] = mapped_column(Enum(Granularity))
    # Timestamp format
    format: Mapped[Optional[str]]

    # The column reference that this partition is defined on
    column_id: Mapped[int] = mapped_column(ForeignKey("column.id"))
    column: Mapped[Column] = relationship(
        back_populates="partition",
        primaryjoin="Column.id==Partition.column_id",
    )

    def temporal_expression(self, interval: Optional[str] = None):
        """
        This expression evaluates to the temporal partition value for scheduled runs. Defaults to
        CAST(FORMAT(DJ_LOGICAL_TIMESTAMP(), 'yyyyMMdd') AS <column type>). Includes the interval
        offset in the expression if provided.
        """
        from datajunction_server.sql.parsing import (  # pylint: disable=import-outside-toplevel
            ast,
        )

        # pylint: disable=import-outside-toplevel
        from datajunction_server.sql.parsing.backends.antlr4 import parse

        timestamp_expression = ast.Cast(
            expression=ast.Function(
                ast.Name("DJ_LOGICAL_TIMESTAMP"),
                args=[],
            ),
            data_type=TimestampType(),
        )
        if interval:
            interval_ast = parse(f"SELECT INTERVAL {interval}")
            timestamp_expression = ast.BinaryOp(  # type: ignore
                left=timestamp_expression,
                right=interval_ast.select.projection[0],  # type: ignore
                op=ast.BinaryOpKind.Minus,
            )

        if self.type_ == PartitionType.TEMPORAL:
            return ast.Cast(
                expression=ast.Function(
                    ast.Name("DATE_FORMAT"),
                    args=[
                        timestamp_expression,
                        ast.String(f"'{self.format}'"),
                    ],
                ),
                data_type=self.column.type,  # pylint: disable=no-member
            )
        return None  # pragma: no cover


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

    class Config:  # pylint: disable=missing-class-docstring, too-few-public-methods
        orm_mode = True


class PartitionOutput(BaseModel):
    """
    Output for partition
    """

    type_: PartitionType
    format: Optional[str]
    granularity: Optional[str]
    expression: Optional[str]

    class Config:  # pylint: disable=missing-class-docstring, too-few-public-methods
        orm_mode = True


class PartitionColumnOutput(BaseModel):
    """
    Output for partition columns
    """

    name: str
    type_: PartitionType
    format: Optional[str]
    expression: Optional[str]


class Backfill(Base):  # type: ignore  # pylint: disable=too-few-public-methods
    """
    A backfill run is linked to a materialization config, where users provide the range
    (of a temporal partition) to backfill for the node.
    """

    __tablename__ = "backfill"

    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
    )

    # The column reference that this partition is defined on
    materialization_id: Mapped[int] = mapped_column(ForeignKey("materialization.id"))
    materialization: Mapped["Materialization"] = relationship(
        back_populates="backfills",
        primaryjoin="Materialization.id==Backfill.materialization_id",
    )

    # Backfilled values and range
    spec: Mapped[Optional[PartitionBackfill]] = mapped_column(
        JSON,
        default={},
    )

    urls: Mapped[Optional[List[str]]] = mapped_column(
        JSON,
        default=[],
    )
    #
    # @validator("spec")
    # def val_spec(
    #     cls,
    #     val,
    # ):  # pylint: disable=missing-function-docstring,no-self-argument
    #     return val.dict()


class BackfillOutput(BaseModel):
    """
    Output model for backfills
    """

    spec: Optional[PartitionBackfill]
    urls: Optional[List[str]]
