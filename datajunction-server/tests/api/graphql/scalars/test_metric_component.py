"""GraphQL exposes the same sketch component metadata as REST."""

import strawberry

from datajunction_server.api.graphql.scalars.metricmetadata import (
    MetricComponent as GraphQLMetricComponent,
)
from datajunction_server.models.decompose import AggregationRule, MetricComponent
from datajunction_server.models.materialization import MaterializationTarget


def test_sketch_component_fields_are_queryable():
    component = MetricComponent(
        name="latency_digest",
        expression="latency_ms",
        aggregation="build_digest(latency_ms, 200)",
        merge="merge_digest",
        merge_args=["200"],
        serialize="digest_to_bytes({})",
        serialize_targets=[MaterializationTarget.DRUID],
        serialize_type="binary",
        rule=AggregationRule(),
    )

    @strawberry.type
    class Query:
        @strawberry.field
        def metric_component(self) -> GraphQLMetricComponent:
            return GraphQLMetricComponent.from_pydantic(component)  # type: ignore[attr-defined]

    result = strawberry.Schema(query=Query).execute_sync(
        "{ metricComponent { mergeArgs serialize serializeTargets serializeType } }",
    )

    assert result.errors is None
    assert result.data == {
        "metricComponent": {
            "mergeArgs": ["200"],
            "serialize": "digest_to_bytes({})",
            "serializeTargets": ["DRUID"],
            "serializeType": "binary",
        },
    }
