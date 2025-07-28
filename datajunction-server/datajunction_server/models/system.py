from typing import Any
from pydantic import BaseModel


class RowOutput(BaseModel):
    """
    Output model for node counts.
    """

    value: Any
    col: str


class DimensionStats(BaseModel):
    """
    Output model for dimension statistics.
    """

    name: str
    indegree: int = 0
    cube_count: int
