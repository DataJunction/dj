"""SQL-related scalars."""

from typing import Annotated

import strawberry

from datajunction_server.api.graphql.scalars.metricmetadata import MetricComponent
from datajunction_server.database.queryrequest import QueryBuildType as QueryBuildType_
from datajunction_server.models.column import SemanticType as SemanticType_
from datajunction_server.models.engine import Dialect as Dialect_

SemanticType = strawberry.enum(SemanticType_)
Dialect = strawberry.enum(Dialect_)
QueryBuildType = strawberry.enum(QueryBuildType_)


@strawberry.input
class CubeDefinition:
    """
    The cube definition for the query
    """

    cube: Annotated[
        str | None,
        strawberry.argument(
            description="The name of the cube to query",
        ),
    ] = None  # type: ignore
    metrics: Annotated[
        list[str] | None,
        strawberry.argument(
            description="A list of metric node names",
        ),
    ] = None  # type: ignore
    dimensions: Annotated[
        list[str] | None,
        strawberry.argument(
            description="A list of dimension attribute names",
        ),
    ] = None
    filters: Annotated[
        list[str] | None,
        strawberry.argument(
            description="A list of filter SQL clauses",
        ),
    ] = None
    orderby: Annotated[
        list[str] | None,
        strawberry.argument(
            description="A list of order by clauses",
        ),
    ] = None


@strawberry.type
class VersionedRef:
    """
    Versioned reference
    """

    name: str
    version: str


@strawberry.type
class MaterializationUnit:
    """ """

    upstream: VersionedRef
    grain_dimensions: list[VersionedRef]
    measures: list[MetricComponent]
    filter_refs: list[VersionedRef]
    filters: list[str]


@strawberry.type
class MaterializationPlan:
    """ """

    units: list[MaterializationUnit]
