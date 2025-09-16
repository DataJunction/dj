import pytest
from datajunction_server.models.deployment import SourceSpec, MetricSpec
from datajunction_server.models.node import MetricUnit
from datajunction_server.errors import DJInvalidInputException


def test_source_spec():
    source_spec = SourceSpec(
        name="test_source",
        table="public.test_db.test_table",
    )
    assert source_spec.rendered_name == "test_source"
    assert source_spec.rendered_query is None

    with pytest.raises(DJInvalidInputException):
        source_spec = SourceSpec(
            name="test_source",
            table="test_db.test_table",
        )


def test_metric_spec():
    metric_spec = MetricSpec(
        name="test_metric",
        query="SELECT 1 AS value",
        unit=MetricUnit.DAY,
        description="A test metric",
    )
    other_metric_spec = MetricSpec(
        name="other_metric",
        query="SELECT 2 AS value",
        unit=MetricUnit.DOLLAR,
        description="Another test metric",
    )
    assert metric_spec.rendered_name == "test_metric"
    assert metric_spec.rendered_query == "SELECT 1 AS value"
    assert metric_spec.query_ast is not None
    assert metric_spec.__eq__(metric_spec)
    assert not metric_spec.__eq__(object())
    assert not metric_spec.__eq__(other_metric_spec)
