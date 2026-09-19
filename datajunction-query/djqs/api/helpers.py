"""
Helper functions for API
"""

from typing import Any

from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import NoSuchTableError, OperationalError

from djqs.exceptions import DJException, DJTableNotFound


def get_columns(
    table: str,
    schema: str | None,
    catalog: str | None,
    uri: str | None,
    extra_params: dict[str, Any] | None,
) -> list[dict[str, str]]:  # pragma: no cover
    """
    Return all columns in a given table.
    """
    if not uri:
        raise DJException("Cannot retrieve columns without a uri")

    engine = create_engine(uri, connect_args=extra_params)
    try:
        inspector = inspect(engine)
        column_metadata = inspector.get_columns(
            table,
            schema=schema,
        )
    except NoSuchTableError as exc:  # pylint: disable=broad-except
        raise DJTableNotFound(
            message=f"No such table `{table}` in schema `{schema}` in catalog `{catalog}`",
            http_status_code=404,
        ) from exc
    except OperationalError as exc:
        if "unknown database" in str(exc):
            raise DJException(message=f"No such schema `{schema}`") from exc
        raise

    return [
        {"name": column["name"], "type": column["type"].python_type.__name__.upper()}
        for column in column_metadata
    ]
