"""
Models for generated SQL
"""

from typing import List, Optional

from datajunction_server.transpilation import transpile_sql
from pydantic import validator
from pydantic.main import BaseModel

from datajunction_server.errors import DJQueryBuildError
from datajunction_server.models.cube_materialization import MetricComponent
from datajunction_server.models.engine import Dialect
from datajunction_server.models.node_type import NodeNameVersion
from datajunction_server.models.query import ColumnMetadata


class TranspiledSQL(BaseModel):
    """
    Generated SQL for a given node, the output of a QueryBuilder(...).build() call.
    """

    sql: str
    dialect: Optional[Dialect] = None

    @classmethod
    def create(cls, *, dialect, **kwargs):
        sql = transpile_sql(kwargs["sql"], dialect)
        return cls(
            sql=sql,
            dialect=dialect,
            **{k: v for k, v in kwargs.items() if k not in {"sql", "dialect"}},
        )

    @validator("dialect", pre=True)
    def validate_dialect(cls, v):
        if v is None:
            return None
        return Dialect(v)


class GeneratedSQL(TranspiledSQL):
    """
    Generated SQL for a given node, the output of a QueryBuilder(...).build() call.
    """

    node: NodeNameVersion
    sql: str
    columns: Optional[List[ColumnMetadata]] = None  # pragma: no-cover
    grain: list[str] | None = None
    dialect: Optional[Dialect] = None
    upstream_tables: Optional[List[str]] = None
    metrics: dict[str, tuple[list[MetricComponent], str]] | None = None
    spark_conf: dict[str, str] | None = None
    errors: Optional[List[DJQueryBuildError]] = None
