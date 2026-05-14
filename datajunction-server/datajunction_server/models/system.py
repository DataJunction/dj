from typing import Any
from pydantic import BaseModel


class SystemMetricData(BaseModel):
    """
    Output for a /system/data/{metric} query.

    ``columns`` is the ordered list of column names (dimension semantic entities
    plus the metric node name). ``rows`` is a 2-D list of values aligned to
    ``columns``. This shape mirrors the ``/data`` endpoint's row/column layout
    and is ~5-10x smaller than the legacy per-cell envelope.
    """

    columns: list[str]
    rows: list[list[Any]]


class DimensionStats(BaseModel):
    """
    Output model for dimension statistics.
    """

    name: str
    indegree: int = 0
    cube_count: int
