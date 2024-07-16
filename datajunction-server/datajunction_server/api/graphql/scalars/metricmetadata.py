"""Metric metadata scalars"""

from typing import Optional

import strawberry

from datajunction_server.models.node import MetricDirection as MetricDirection_

MetricDirection = strawberry.enum(MetricDirection_)


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
class MetricMetadata:  # pylint: disable=too-few-public-methods
    """
    Metric metadata output
    """

    direction: Optional[MetricDirection]  # type: ignore
    unit: Optional[Unit]
