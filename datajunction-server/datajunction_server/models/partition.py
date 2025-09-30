"""Partition-related models."""

from typing import TYPE_CHECKING, List, Optional

from pydantic.main import BaseModel
from pydantic import ConfigDict

from datajunction_server.enum import StrEnum

if TYPE_CHECKING:
    pass


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
    granularity: Optional[Granularity] = None
    # Timestamp format
    format: Optional[str] = None


class PartitionBackfill(BaseModel):
    """
    Used for setting backfilled values
    """

    column_name: str

    # Backfilled values and range. Most temporal partitions will just use `range`, but some may
    # optionally use `values` to specify specific values
    # Ex: values: [20230901]
    #     range: [20230901, 20231001]
    values: Optional[List] = None
    range: Optional[List] = None

    model_config = ConfigDict(from_attributes=True)


class PartitionOutput(BaseModel):
    """
    Output for partition
    """

    type_: PartitionType
    format: Optional[str] = None
    granularity: Optional[str] = None
    expression: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class PartitionColumnOutput(BaseModel):
    """
    Output for partition columns
    """

    name: str
    type_: PartitionType
    format: Optional[str] = None
    expression: Optional[str] = None


class BackfillOutput(BaseModel):
    """
    Output model for backfills
    """

    spec: Optional[List[PartitionBackfill]] = None
    urls: Optional[List[str]] = None

    model_config = ConfigDict(from_attributes=True)
