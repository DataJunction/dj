"""Partition database schema."""
import re
from typing import Optional

from sqlalchemy import BigInteger, Enum, ForeignKey, Integer
from sqlalchemy.orm import Mapped, mapped_column, relationship

from datajunction_server.database.base import Base
from datajunction_server.database.column import Column
from datajunction_server.models.partition import Granularity, PartitionType
from datajunction_server.naming import amenable_name
from datajunction_server.sql.functions import Function, function_registry
from datajunction_server.sql.parsing.types import TimestampType


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
    column_id: Mapped[int] = mapped_column(
        ForeignKey(
            "column.id",
            name="fk_partition_column_id_column",
            ondelete="CASCADE",
        ),
    )
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

    def categorical_expression(self):
        """
        Expression for the categorical partition
        """
        from datajunction_server.sql.parsing import (  # pylint: disable=import-outside-toplevel
            ast,
        )

        # Register a DJ function (inherits the `datajunction_server.sql.functions.Function` class)
        # that has the partition column name as the function name. This will be substituted at
        # runtime with the partition column name
        amenable_partition_name = amenable_name(self.column.name)
        clazz = type(
            amenable_partition_name,
            (Function,),
            {
                "is_runtime": True,
                "substitute": staticmethod(lambda: f"${{{amenable_partition_name}}}"),
            },
        )
        snake_cased = re.sub(r"(?<!^)(?=[A-Z])", "_", clazz.__name__)
        function_registry[clazz.__name__.upper()] = clazz
        function_registry[snake_cased.upper()] = clazz

        return ast.Cast(
            expression=ast.Function(
                name=ast.Name(amenable_partition_name),
                args=[],
            ),
            data_type=self.column.type,
        )
