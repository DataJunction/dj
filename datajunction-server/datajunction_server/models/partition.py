"""Partition-related models."""

from pydantic import ConfigDict
from pydantic.main import BaseModel

from datajunction_server.enum import StrEnum


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
    granularity: Granularity | None = None
    # Timestamp format
    format: str | None = None


class PartitionBackfill(BaseModel):
    """
    Used for setting backfilled values
    """

    column_name: str

    # Backfilled values and range. Most temporal partitions will just use `range`, but some may
    # optionally use `values` to specify specific values
    # Ex: values: [20230901]
    #     range: [20230901, 20231001]
    values: list | None = None
    range: list | None = None

    model_config = ConfigDict(from_attributes=True)


class PartitionOutput(BaseModel):
    """
    Output for partition
    """

    type_: PartitionType
    format: str | None = None
    granularity: str | None = None
    expression: str | None = None

    model_config = ConfigDict(from_attributes=True)


class PartitionColumnOutput(BaseModel):
    """
    Output for partition columns
    """

    name: str
    type_: PartitionType
    format: str | None = None
    expression: str | None = None


class BackfillOutput(BaseModel):
    """
    Output model for backfills
    """

    spec: list[PartitionBackfill] | None = None
    urls: list[str] | None = None

    model_config = ConfigDict(from_attributes=True)
