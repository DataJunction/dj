"""Metric metadata scalars"""

from typing import Optional

import strawberry

from datajunction_server.models.cube_materialization import (
    Aggregability as Aggregability_,
)
from datajunction_server.models.cube_materialization import (
    AggregationRule as AggregationRule_,
)
from datajunction_server.models.cube_materialization import Measure as Measure_
from datajunction_server.models.node import MetricDirection as MetricDirection_

MetricDirection = strawberry.enum(MetricDirection_)
Aggregability = strawberry.enum(Aggregability_)


@strawberry.type
class Unit:
    """
    Metric unit
    """

    name: str
    label: Optional[str]
    category: Optional[str]
    abbreviation: Optional[str]


@strawberry.experimental.pydantic.type(model=AggregationRule_, all_fields=True)
class AggregationRule: ...


@strawberry.experimental.pydantic.type(model=Measure_, all_fields=True)
class Measure: ...


@strawberry.type
class ExtractedMeasures:
    """
    extracted measures from metric
    """

    measures: list[Measure]
    derived_query: str
    derived_expression: str


@strawberry.type
class MetricMetadata:
    """
    Metric metadata output
    """

    direction: MetricDirection | None  # type: ignore
    unit: Unit | None
    significant_digits: int | None
    min_decimal_exponent: int | None
    max_decimal_exponent: int | None
    expression: str
    incompatible_druid_functions: list[str]
