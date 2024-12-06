"""Metric metadata scalars"""

from typing import Optional

import strawberry

from datajunction_server.models.node import MetricDirection as MetricDirection_
from datajunction_server.sql.decompose import Aggregability as Aggregability_

MetricDirection = strawberry.enum(MetricDirection_)
Aggregability = strawberry.enum(Aggregability_)


@strawberry.type
class Unit:  # pylint: disable=too-few-public-methods
    """
    Metric unit
    """

    name: str
    label: Optional[str]
    category: Optional[str]
    abbreviation: Optional[str]


@strawberry.type
class AggregationRule:
    """
    The aggregation rule for the measure.
    """

    type: Aggregability = Aggregability.NONE  # type: ignore
    level: list[str] | None = None


@strawberry.type
class Measure:  # pylint: disable=too-few-public-methods
    """
    Measure output
    """

    name: str
    expression: str  # A SQL expression for defining the measure
    aggregation: str
    rule: AggregationRule


@strawberry.type
class ExtractedMeasures:  # pylint: disable=too-few-public-methods
    """
    extracted measures from metric
    """

    measures: list[Measure]
    derived_sql: str


@strawberry.type
class MetricMetadata:  # pylint: disable=too-few-public-methods
    """
    Metric metadata output
    """

    direction: Optional[MetricDirection]  # type: ignore
    unit: Optional[Unit]
