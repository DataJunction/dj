"""Materialization scalars"""

import strawberry
from strawberry.scalars import JSON


@strawberry.type
class PartitionBackfill:
    """
    Used for setting backfilled values
    """

    column_name: str

    # Backfilled values and range. Most temporal partitions will just use `range`, but some may
    # optionally use `values` to specify specific values
    # Ex: values: [20230901]
    #     range: [20230901, 20231001]
    values: list[str] | None
    range: list[str] | None


@strawberry.type
class Backfill:
    """
    Materialization job backfill
    """

    spec: list[PartitionBackfill] | None
    urls: list[str] | None


@strawberry.type
class MaterializationConfig:
    """
    Materialization config
    """

    name: str | None
    config: JSON
    schedule: str
    job: str | None
    backfills: list[Backfill]
    strategy: str | None
