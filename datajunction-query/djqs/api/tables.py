"""
Table related APIs.
"""

from typing import Optional

from fastapi import APIRouter, Path, Query

from djqs.api.helpers import get_columns
from djqs.exceptions import DJInvalidTableRef
from djqs.models.table import TableInfo
from djqs.utils import get_settings

router = APIRouter(tags=["Table Reflection"])


@router.get("/table/{table}/columns/", response_model=TableInfo)
def table_columns(
    table: str = Path(..., example="tpch.sf1.customer"),
    engine: Optional[str] = Query(None, example="trino"),
    engine_version: Optional[str] = Query(None, example="451"),
) -> TableInfo:
    """
    Get column information for a table
    """
    table_parts = table.split(".")
    if len(table_parts) != 3:
        raise DJInvalidTableRef(
            http_status_code=422,
            message=f"The provided table value `{table}` is invalid. A valid value "
            f"for `table` must be in the format `<catalog>.<schema>.<table>`",
        )
    settings = get_settings()

    if engine and engine_version:
        engine_config = settings.find_engine(
            engine_name=engine,
            engine_version=engine_version or settings.default_engine_version,
        )
    else:
        engine_config = settings.find_engine(
            engine_name=settings.default_engine,
            engine_version=engine_version or settings.default_engine_version,
        )
    external_columns = get_columns(
        uri=engine_config.uri,
        extra_params=engine_config.extra_params,
        catalog=table_parts[0],
        schema=table_parts[1],
        table=table_parts[2],
    )
    return TableInfo(
        name=table,
        columns=external_columns,
    )
