"""
Models for generated SQL
"""

from typing import List, Optional

from pydantic.class_validators import root_validator
from pydantic.main import BaseModel

from datajunction_server.errors import DJQueryBuildError
from datajunction_server.models.cube_materialization import Measure
from datajunction_server.models.engine import Dialect
from datajunction_server.models.node_type import NodeNameVersion
from datajunction_server.models.query import ColumnMetadata
from datajunction_server.transpilation import get_transpilation_plugin


class GeneratedSQL(BaseModel):
    """
    Generated SQL for a given node, the output of a QueryBuilder(...).build() call.
    """

    node: NodeNameVersion
    sql: str
    sql_transpilation_library: Optional[str] = None
    columns: Optional[List[ColumnMetadata]] = None  # pragma: no-cover
    grain: list[str] | None = None
    dialect: Optional[Dialect] = None
    upstream_tables: Optional[List[str]] = None
    metrics: dict[str, tuple[list[Measure], str]] | None = None
    spark_conf: dict[str, str] | None = None
    errors: Optional[List[DJQueryBuildError]] = None

    @root_validator(pre=False)
    def transpile_sql(
        cls,
        values,
    ):
        """
        Transpiles SQL to the specified dialect with the configured transpilation plugin.
        If no plugin is configured, it will just return the original generated query.
        """
        if values.get("sql_transpilation_library"):
            plugin = get_transpilation_plugin(  # pragma: no cover
                values.get("sql_transpilation_library"),
            )
            values["sql"] = plugin.transpile_sql(  # pragma: no cover
                values["sql"],
                input_dialect=Dialect.SPARK,
                output_dialect=values["dialect"],
            )
        return values
