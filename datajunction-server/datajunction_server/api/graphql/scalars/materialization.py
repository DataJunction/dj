"""Materialization scalars"""

from typing import List, Optional

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
    values: Optional[List[str]]
    range: Optional[List[str]]


@strawberry.type
class Backfill:
    """
    Materialization job backfill
    """

    spec: Optional[List[PartitionBackfill]]
    urls: Optional[List[str]]


@strawberry.type
class MaterializationConfig:
    """
    Materialization config
    """

    name: Optional[str]
    config: JSON
    schedule: str
    job: Optional[str]
    backfills: List[Backfill]
    strategy: Optional[str]
