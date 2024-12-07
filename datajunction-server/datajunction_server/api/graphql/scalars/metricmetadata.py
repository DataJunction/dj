"""Metric metadata scalars"""

from typing import Optional

import strawberry

from datajunction_server.models.node import MetricDirection as MetricDirection_
from datajunction_server.sql.decompose import Aggregability as Aggregability_
from datajunction_server.sql.decompose import AggregationRule as AggregationRule_
from datajunction_server.sql.decompose import Measure as Measure_

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


@strawberry.experimental.pydantic.type(model=AggregationRule_, all_fields=True)
class AggregationRule:  # pylint: disable=missing-class-docstring,too-few-public-methods
    ...


@strawberry.experimental.pydantic.type(model=Measure_, all_fields=True)
class Measure:  # pylint: disable=missing-class-docstring,too-few-public-methods
    ...


@strawberry.type
class ExtractedMeasures:  # pylint: disable=too-few-public-methods
    """
    extracted measures from metric
    """

    measures: list[Measure]
    derived_query: str
    derived_expression: str


@strawberry.type
class MetricMetadata:  # pylint: disable=too-few-public-methods
    """
    Metric metadata output
    """

    direction: Optional[MetricDirection]  # type: ignore
    unit: Optional[Unit]
