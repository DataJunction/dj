"""Partition-related models."""

import calendar
import re
from datetime import datetime, timedelta

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


# Java-style temporal format tokens, most to least significant. A format built
# only from these, in this order, renders to an integer whose ordering is
# chronological -- which is what makes comparing two such values meaningful.
PARTITION_FORMAT_TOKENS = {
    "yyyy": "%Y",
    "MM": "%m",
    "dd": "%d",
    "HH": "%H",
    "mm": "%M",
    "ss": "%S",
}
_TOKEN_PATTERN = re.compile("|".join(PARTITION_FORMAT_TOKENS))


def render_partition_value(moment: datetime, format_: str | None) -> int | None:
    """
    Render a point in time as an integer in a partition format.

    Only a format built purely from tokens in descending order of significance
    renders: anything with separators (``yyyy-MM-dd``) can't produce an integer,
    and anything out of order (``ddMMyyyy``) produces one whose ordering isn't
    chronological. Both yield None so the caller declines to judge.
    """
    if not format_:
        return None
    tokens = _TOKEN_PATTERN.findall(format_)
    if _TOKEN_PATTERN.sub("", format_) != "":
        return None
    order = list(PARTITION_FORMAT_TOKENS)
    if [order.index(token) for token in tokens] != sorted(
        order.index(token) for token in tokens
    ):
        return None
    return int(
        moment.strftime(
            _TOKEN_PATTERN.sub(lambda m: PARTITION_FORMAT_TOKENS[m.group()], format_),
        ),
    )


def strftime_format(format_: str | None) -> str | None:
    """
    Translate a Java-style partition format into a strftime pattern.

    Separators survive as literals, so ``yyyy-MM-dd`` renders. A format with no
    recognized token yields None, since a constant cannot order or step.
    """
    if not format_ or not _TOKEN_PATTERN.search(format_):
        return None
    return _TOKEN_PATTERN.sub(lambda m: PARTITION_FORMAT_TOKENS[m.group()], format_)


# Grains that step by a fixed amount of time
_GRAIN_DELTAS = {
    Granularity.SECOND: timedelta(seconds=1),
    Granularity.MINUTE: timedelta(minutes=1),
    Granularity.HOUR: timedelta(hours=1),
    Granularity.DAY: timedelta(days=1),
    Granularity.WEEK: timedelta(weeks=1),
}

# Grains that step by whole months
_GRAIN_MONTHS = {
    Granularity.MONTH: 1,
    Granularity.QUARTER: 3,
    Granularity.YEAR: 12,
}


def _add_months(moment: datetime, count: int) -> datetime:
    """
    Move a moment forward by whole months, clamping the day.
    """
    index = moment.month - 1 + count
    year = moment.year + index // 12
    month = index % 12 + 1
    return moment.replace(
        year=year,
        month=month,
        day=min(moment.day, calendar.monthrange(year, month)[1]),
    )


class PartitionGrain(BaseModel):
    """
    The formats and grain of a node's temporal partition columns.

    The formats are ordered to match a partition value like ``["20240101", "00"]``,
    and the grain is the finest one, which is the step between two values.
    """

    formats: list[str]
    granularity: Granularity

    def next_value(self, value: list) -> list[str] | None:
        """
        Return the partition value one grain after this one.
        """
        patterns = [strftime_format(format_) for format_ in self.formats]
        if len(value) != len(patterns) or not all(patterns):
            return None
        try:
            moment = datetime.strptime(
                "".join(str(part) for part in value),
                "".join(patterns),  # type: ignore
            )
        except ValueError:
            return None
        months = _GRAIN_MONTHS.get(self.granularity)
        moment = (
            _add_months(moment, months)
            if months
            else moment + _GRAIN_DELTAS[self.granularity]
        )
        return [moment.strftime(pattern) for pattern in patterns]  # type: ignore
