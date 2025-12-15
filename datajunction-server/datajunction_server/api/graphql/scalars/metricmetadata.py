"""Metric metadata scalars"""

from typing import Optional

import strawberry

from datajunction_server.models.cube_materialization import (
    Aggregability as Aggregability_,
)
from datajunction_server.models.cube_materialization import (
    AggregationRule as AggregationRule_,
)
from datajunction_server.models.cube_materialization import (
    MetricComponent as MetricComponent_,
)
from datajunction_server.models.cube_materialization import (
    DecomposedMetric as DecomposedMetric_,
)
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


@strawberry.experimental.pydantic.type(model=MetricComponent_, all_fields=True)
class MetricComponent: ...


@strawberry.experimental.pydantic.type(model=DecomposedMetric_, all_fields=True)
class DecomposedMetric:
    """
    Decomposed metric, which includes its components and combining expression.

    - components: The individual measures that make up this metric
    - combiner: Expression combining merged components into the final value
    - derived_query: The full derived query AST as a string
    - derived_expression: Alias for combiner (backward compatibility)
    """

    @strawberry.field
    def derived_expression(self) -> str:
        """Alias for combiner, for backward compatibility with existing GraphQL queries."""
        return self.combiner  # type: ignore


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
