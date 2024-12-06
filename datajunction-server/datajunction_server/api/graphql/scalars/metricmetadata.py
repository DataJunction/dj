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
    The aggregation rule for the measure. If the Aggregability type is LIMITED, the `level` should
    be specified to highlight the level at which the measure needs to be aggregated in order to
    support the specified aggregation function.

    For example, consider a metric like COUNT(DISTINCT user_id). It can be decomposed into a
    single measure with LIMITED aggregability, i.e., it is only aggregatable if the measure is
    calculated at the `user_id` level:
    - name: num_users
      expression: DISTINCT user_id
      aggregation: COUNT
      rule:
        type: LIMITED
        level: ["user_id"]
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
