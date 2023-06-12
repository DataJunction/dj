"""
Table related APIs.
"""
from typing import Optional

from fastapi import APIRouter, Depends
from sqlmodel import Session

from djqs.api.helpers import get_columns, get_engine
from djqs.exceptions import DJInvalidTableRef
from djqs.models.table import TableInfo
from djqs.utils import get_session, get_settings

router = APIRouter(tags=["Table Reflection"])


@router.get("/table/{table}/columns/", response_model=TableInfo)
def table_columns(
    table: str,
    engine: Optional[str] = None,
    engine_version: Optional[str] = None,
    *,
    session: Session = Depends(get_session),
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
    engine = get_engine(
        session=session,
        name=engine or settings.default_reflection_engine,
        version=engine_version or settings.default_reflection_engine_version,
    )
    external_columns = get_columns(
        uri=engine.uri,
        extra_params={},
        catalog=table_parts[0],
        schema=table_parts[1],
        table=table_parts[2],
    )
    return TableInfo(
        name=table,
        columns=external_columns,
    )
