"""
A DB engine spec for Superset.
"""

import re
from datetime import timedelta
from typing import TYPE_CHECKING, Any, List, Optional, Set, TypedDict

import requests
from sqlalchemy.engine.reflection import Inspector

try:
    from superset.db_engine_specs.base import BaseEngineSpec
except ImportError:  # pragma: no cover
    # we don't really need the base class, so we can just mock it if Apache Superset is
    # not installed
    BaseEngineSpec = object

if TYPE_CHECKING:
    from superset.models.core import Database


SELECT_STAR_MESSAGE = (
    "DJ does not support data preview, since the `metrics` table is a virtual table "
    "representing the whole repository of metrics. An administrator should configure the "
    "DJ database with the `disable_data_preview` attribute set to `true` in the `extra` "
    "field."
)
GET_METRICS_TIMEOUT = timedelta(seconds=60)


class MetricType(TypedDict, total=False):
    """
    Type for metrics return by `get_metrics`.
    """

    metric_name: str
    expression: str
    verbose_name: Optional[str]
    metric_type: Optional[str]
    description: Optional[str]
    d3format: Optional[str]
    warning_text: Optional[str]
    extra: Optional[str]


class DJEngineSpec(BaseEngineSpec):
    """
    Engine spec for the DataJunction metric repository

    See https://github.com/DataJunction/dj for more information.
    """

    engine = "dj"
    engine_name = "DJ"

    sqlalchemy_uri_placeholder = "dj://host:port/database_id"

    _time_grain_expressions = {
        None: "{col}",
        "PT1S": "DATE_TRUNC('second', {col})",
        "PT1M": "DATE_TRUNC('minute', {col})",
        "PT1H": "DATE_TRUNC('hour', {col})",
        "P1D": "DATE_TRUNC('day', {col})",
        "P1W": "DATE_TRUNC('week', {col})",
        "P1M": "DATE_TRUNC('month', {col})",
        "P3M": "DATE_TRUNC('quarter', {col})",
        "P1Y": "DATE_TRUNC('year', {col})",
    }

    @classmethod
    def select_star(
        cls,
        *args: Any,
        **kwargs: Any,
    ) -> str:
        """
        Return a ``SELECT *`` query.

        Since DJ doesn't have tables per se, a ``SELECT *`` query doesn't make sense.
        """
        message = SELECT_STAR_MESSAGE.replace("'", "''")
        return f"SELECT '{message}' AS warning"

    @classmethod
    def get_metrics(
        cls,
        database: "Database",
        inspector: Inspector,
        table_name: str,
        schema: Optional[str],
    ) -> List[MetricType]:
        """
        Get all metrics from a given schema and table.
        """
        with database.get_sqla_engine_with_context() as engine:
            base_url = engine.connect().connection.base_url

        response = requests.get(
            base_url / "metrics/",
            timeout=GET_METRICS_TIMEOUT.total_seconds(),
        )
        payload = response.json()
        return [
            {
                "metric_name": metric_name,
                "expression": f'"{metric_name}"',
                "description": "",
            }
            for metric_name in payload
        ]

    @classmethod
    def execute(
        cls,
        cursor: Any,
        query: str,
        **kwargs: Any,
    ) -> None:
        """
        Quote ``__timestamp`` and other identifiers starting with an underscore.
        """
        query = re.sub(r" AS (_.*?)(\b|$)", r' AS "\1"', query)

        return super().execute(cursor, query, **kwargs)

    @classmethod
    def get_view_names(
        cls,
        database: "Database",
        inspector: Inspector,
        schema: Optional[str],
    ) -> Set[str]:
        """
        Return all views.
        """
        return set()
