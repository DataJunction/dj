"""
A DB engine spec for Apache Superset.
"""

import re
from typing import Any, List, Optional, TYPE_CHECKING, TypedDict

import requests
from marshmallow import fields, Schema
from marshmallow.validate import Range
from sqlalchemy.engine.reflection import Inspector
from superset.db_engine_specs.base import BaseEngineSpec

if TYPE_CHECKING:
    from superset.models.core import Database


SELECT_STAR_MESSAGE = (
    "DJ does not support data preview, since the `metrics` table is a virtual table "
    "representing the whole metric repository. An administrator should configure the "
    "DJ database with the `disable_data_preview` attribute set to `true` in the `extra` "
    "field."
)


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


class DJParametersSchema(Schema):  # pylint: disable=too-few-public-methods
    """
    Schema for configuring the database in Apache Superset.
    """

    host = fields.String(required=True, description="Hostname or IP address")
    port = fields.Integer(
        required=True,
        description="Database port",
        validate=Range(min=0, max=2 ** 16, max_inclusive=False),
    )
    database = fields.Integer(required=True, default=0, description="Database number")


class DJEngineSpec(BaseEngineSpec):  # pylint: disable=abstract-method
    """
    Engine spec for the DataJunction metric repository

    See https://github.com/DataJunction/datajunction for more information.
    """

    engine = "dj"
    engine_name = "DJ"

    default_driver = ""
    sqlalchemy_uri_placeholder = "dj://host:port/database_id"
    parameters_schema = DJParametersSchema()

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
    def select_star(  # pylint: disable=unused-argument
        cls,
        *args: Any,
        **kwargs: Any,
    ) -> str:
        """
        Return a ``SELECT *`` query.

        Since DJ doesn't have tables per se, a ``SELECT *`` query doesn't make sense.
        """
        message = SELECT_STAR_MESSAGE.replace("'", "''")
        return f"SELECT '{message}' AS message"

    @classmethod
    def get_metrics(  # pylint: disable=unused-argument
        cls,
        database: "Database",
        inspector: Inspector,
        table_name: str,
        schema: Optional[str],
    ) -> List[MetricType]:
        """
        Get all metrics from a given schema and table.
        """
        engine = database.get_sqla_engine()
        base_url = engine.connect().connection.base_url

        response = requests.get(base_url / "metrics/")
        payload = response.json()
        return [
            {
                "metric_name": metric["name"],
                "expression": f'"{metric["name"]}"',
                "description": metric["description"],
            }
            for metric in payload
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
        query = re.sub(r" AS (_.*)(\b|$)", r' AS "\1"', query)

        return super().execute(cursor, query, **kwargs)
