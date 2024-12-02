"""
Tests for ``datajunction_server.sql.decompose``.
"""
import pytest

from datajunction_server.sql.decompose import (
    Aggregability,
    AggregationRule,
    Measure,
    extractor,
)
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.sql.parsing.backends.exceptions import DJParseException


def test_simple_sum():
    """
    Test decomposition for a metric definition that is a simple sum.
    """
    measures, derived_sql = extractor.extract_measures(
        "SELECT SUM(sales_amount) FROM parent_node",
    )
    expected_measures = [
        Measure(
            name="sales_amount_sum_0",
            expression="sales_amount",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT SUM(sales_amount_sum_0) FROM parent_node"),
    )


def test_sum_with_cast():
    """
    Test decomposition for a metric definition that has a sum with a cast.
    """
    measures, derived_sql = extractor.extract_measures(
        "SELECT CAST(SUM(sales_amount) AS DOUBLE) * 100.0 FROM parent_node",
    )
    expected_measures = [
        Measure(
            name="sales_amount_sum_0",
            expression="sales_amount",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT CAST(SUM(sales_amount_sum_0) AS DOUBLE) * 100.0 FROM parent_node",
        ),
    )

    measures, derived_sql = extractor.extract_measures(
        "SELECT 100.0 * SUM(sales_amount) FROM parent_node",
    )
    expected_measures = [
        Measure(
            name="sales_amount_sum_0",
            expression="sales_amount",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT 100.0 * SUM(sales_amount_sum_0) FROM parent_node"),
    )


def test_sum_with_coalesce():
    """
    Test decomposition for a metric definition that has a sum with coalesce.
    """
    measures, derived_sql = extractor.extract_measures(
        "SELECT COALESCE(SUM(sales_amount), 0) FROM parent_node",
    )
    expected_measures = [
        Measure(
            name="sales_amount_sum_1",
            expression="sales_amount",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT COALESCE(SUM(sales_amount_sum_1), 0) FROM parent_node"),
    )


def test_multiple_sums():
    """
    Test decomposition for a metric definition that has multiple sums.
    """
    measures, derived_sql = extractor.extract_measures(
        "SELECT SUM(sales_amount) + SUM(fraud_sales) FROM parent_node",
    )
    expected_measures = [
        Measure(
            name="sales_amount_sum_0",
            expression="sales_amount",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        Measure(
            name="fraud_sales_sum_1",
            expression="fraud_sales",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT SUM(sales_amount_sum_0) + SUM(fraud_sales_sum_1) FROM parent_node",
        ),
    )

    measures, derived_sql = extractor.extract_measures(
        "SELECT SUM(sales_amount) - SUM(fraud_sales) FROM parent_node",
    )
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT SUM(sales_amount_sum_0) - SUM(fraud_sales_sum_1) FROM parent_node",
        ),
    )


def test_nested_functions():
    """
    Test behavior with deeply nested functions inside aggregations.
    """
    measures, derived_sql = extractor.extract_measures(
        "SELECT SUM(ROUND(COALESCE(sales_amount, 0) * 1.1)) FROM parent_node",
    )
    expected_measures = [
        Measure(
            name="sales_amount_sum_0",
            expression="ROUND(COALESCE(sales_amount, 0) * 1.1)",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT SUM(sales_amount_sum_0) FROM parent_node"),
    )

    measures, derived_sql = extractor.extract_measures(
        "SELECT LN(SUM(COALESCE(sales_amount, 0)) + 1) FROM parent_node",
    )
    expected_measures = [
        Measure(
            name="sales_amount_sum_1",
            expression="COALESCE(sales_amount, 0)",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT LN(SUM(sales_amount_sum_1) + 1) FROM parent_node"),
    )


def test_average():
    """
    Test decomposition for a metric definition that uses AVG.
    """
    measures, derived_sql = extractor.extract_measures(
        "SELECT AVG(sales_amount) FROM parent_node",
    )

    expected_measures = [
        Measure(
            name="sales_amount_sum_0",
            expression="sales_amount",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        Measure(
            name="count",
            expression="1",
            aggregation="COUNT",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT SUM(sales_amount_sum_0) / COUNT(count) FROM parent_node"),
    )


def test_rate():
    """
    Test decomposition for a rate metric definition.
    """
    measures, derived_sql = extractor.extract_measures(
        "SELECT SUM(clicks) / SUM(impressions) FROM parent_node",
    )
    expected_measures0 = [
        Measure(
            name="clicks_sum_0",
            expression="clicks",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        Measure(
            name="impressions_sum_1",
            expression="impressions",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures0
    assert str(derived_sql) == str(
        parse("SELECT SUM(clicks_sum_0) / SUM(impressions_sum_1) FROM parent_node"),
    )

    measures, derived_sql = extractor.extract_measures(
        "SELECT 1.0 * SUM(clicks) / NULLIF(SUM(impressions), 0) FROM parent_node",
    )
    expected_measures = [
        Measure(
            name="clicks_sum_0",
            expression="clicks",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        Measure(
            name="impressions_sum_2",
            expression="impressions",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT 1.0 * SUM(clicks_sum_0) / NULLIF(SUM(impressions_sum_2), 0) FROM parent_node",
        ),
    )

    measures, derived_sql = extractor.extract_measures(
        "SELECT CAST(CAST(SUM(clicks) AS INT) AS DOUBLE) / "
        "CAST(SUM(impressions) AS DOUBLE) FROM parent_node",
    )
    assert measures == expected_measures0
    assert str(derived_sql) == str(
        parse(
            "SELECT CAST(CAST(SUM(clicks_sum_0) AS INT) AS DOUBLE) / "
            "CAST(SUM(impressions_sum_1) AS DOUBLE) FROM parent_node",
        ),
    )

    measures, derived_sql = extractor.extract_measures(
        "SELECT COALESCE(SUM(clicks) / SUM(impressions), 0) FROM parent_node",
    )
    expected_measures = [
        Measure(
            name="clicks_sum_1",
            expression="clicks",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        Measure(
            name="impressions_sum_2",
            expression="impressions",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT COALESCE(SUM(clicks_sum_1) / SUM(impressions_sum_2), 0) FROM parent_node",
        ),
    )

    measures, derived_sql = extractor.extract_measures(
        "SELECT IF(SUM(clicks) > 0, CAST(SUM(impressions) AS DOUBLE) "
        "/ CAST(SUM(clicks) AS DOUBLE), NULL) FROM parent_node",
    )
    expected_measures = [
        Measure(
            name="clicks_sum_1",
            expression="clicks",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        Measure(
            name="impressions_sum_2",
            expression="impressions",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        Measure(
            name="clicks_sum_3",
            expression="clicks",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT IF(SUM(clicks_sum_1) > 0, CAST(SUM(impressions_sum_2) AS DOUBLE)"
            " / CAST(SUM(clicks_sum_3) AS DOUBLE), NULL) FROM parent_node",
        ),
    )

    measures, derived_sql = extractor.extract_measures(
        "SELECT ln(sum(clicks) + 1) / sum(views) FROM parent_node",
    )
    expected_measures = [
        Measure(
            name="clicks_sum_1",
            expression="clicks",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        Measure(
            name="views_sum_2",
            expression="views",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT ln(sum(clicks_sum_1) + 1) / sum(views_sum_2) FROM parent_node"),
    )


def test_has_ever():
    """
    Test decomposition for a metric definition that uses MAX.
    """
    measures, derived_sql = extractor.extract_measures(
        "SELECT MAX(IF(condition, 1, 0)) FROM parent_node",
    )
    expected_measures = [
        Measure(
            name="condition_max_0",
            expression="IF(condition, 1, 0)",
            aggregation="MAX",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT MAX(condition_max_0) FROM parent_node"),
    )


def test_fraction_with_if():
    """
    Test decomposition for a rate metric with complex numerator and denominators.
    """
    measures, derived_sql = extractor.extract_measures(
        "SELECT IF(SUM(COALESCE(action, 0)) > 0, "
        "CAST(SUM(COALESCE(action_two, 0)) AS DOUBLE) / "
        "CAST(SUM(COALESCE(action, 0)) AS DOUBLE), NULL) FROM parent_node",
    )

    expected_measures = [
        Measure(
            name="action_sum_1",
            expression="COALESCE(action, 0)",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        Measure(
            name="action_two_sum_2",
            expression="COALESCE(action_two, 0)",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        Measure(
            name="action_sum_3",
            expression="COALESCE(action, 0)",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT IF(SUM(action_sum_1) > 0, "
            "CAST(SUM(action_two_sum_2) AS DOUBLE) / "
            "CAST(SUM(action_sum_3) AS DOUBLE), NULL) FROM parent_node",
        ),
    )


def test_count():
    """
    Test decomposition for a count metric.
    """
    measures, derived_sql = extractor.extract_measures(
        "SELECT COUNT(IF(action = 1, action_event_ts, 0)) FROM parent_node",
    )
    expected_measures = [
        Measure(
            name="action_action_event_ts_count_0",
            expression="IF(action = 1, action_event_ts, 0)",
            aggregation="COUNT",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT SUM(action_action_event_ts_count_0) FROM parent_node"),
    )


def test_count_distinct_rate():
    """
    Test decomposition for a metric that uses count distinct.
    """
    measures, derived_sql = extractor.extract_measures(
        "SELECT COUNT(DISTINCT user_id) / COUNT(action) FROM parent_node",
    )
    expected_measures = [
        Measure(
            name="user_id_count_0",
            expression="DISTINCT user_id",
            aggregation="COUNT",
            rule=AggregationRule(type=Aggregability.LIMITED),
        ),
        Measure(
            name="action_count_1",
            expression="action",
            aggregation="COUNT",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT COUNT( DISTINCT user_id_count_0) / SUM(action_count_1) FROM parent_node",
        ),
    )


def test_no_aggregation():
    """
    Test behavior when there is no aggregation function in the metric query.
    """
    measures, derived_sql = extractor.extract_measures(
        "SELECT sales_amount FROM parent_node",
    )
    expected_measures = []
    assert measures == expected_measures
    assert str(derived_sql) == str(parse("SELECT sales_amount FROM parent_node"))


def test_multiple_aggregations_with_conditions():
    """
    Test behavior with conditional aggregations in the metric query.
    """
    measures, derived_sql = extractor.extract_measures(
        "SELECT SUM(IF(region = 'US', sales_amount, 0)) + "
        "COUNT(DISTINCT IF(region = 'US', account_id, NULL)) FROM parent_node",
    )
    expected_measures = [
        Measure(
            name="region_sales_amount_sum_0",
            expression="IF(region = 'US', sales_amount, 0)",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        Measure(
            name="region_account_id_count_1",
            expression="DISTINCT IF(region = 'US', account_id, NULL)",
            aggregation="COUNT",
            rule=AggregationRule(type=Aggregability.LIMITED),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT  SUM(region_sales_amount_sum_0) + COUNT("
            "DISTINCT region_account_id_count_1) FROM parent_node",
        ),
    )

    measures, derived_sql = extractor.extract_measures(
        "SELECT cast(coalesce(max(a), max(b), 0) as double) + "
        "cast(coalesce(max(a), max(b)) as double) FROM parent_node",
    )
    expected_measures = [
        Measure(
            name="a_max_1",
            expression="a",
            aggregation="MAX",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        Measure(
            name="b_max_2",
            expression="b",
            aggregation="MAX",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        Measure(
            name="a_max_4",
            expression="a",
            aggregation="MAX",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        Measure(
            name="b_max_5",
            expression="b",
            aggregation="MAX",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT CAST(coalesce(max(a_max_1), max(b_max_2), 0) AS DOUBLE) + "
            "CAST(coalesce(max(a_max_4), max(b_max_5)) AS DOUBLE) FROM parent_node",
        ),
    )


def test_empty_query():
    """
    Test behavior when the metric query is empty.
    """
    with pytest.raises(DJParseException, match="Empty query provided!"):
        extractor.extract_measures("")


def test_unsupported_aggregation_function():
    """
    Test behavior when the query contains unsupported aggregation functions. We just return an
    empty list of measures in this case, because there are no pre-aggregatable measures.
    """
    measures, derived_sql = extractor.extract_measures(
        "SELECT MEDIAN(sales_amount) FROM parent_node",
    )
    expected_measures = []
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT MEDIAN(sales_amount) FROM parent_node"),
    )

    measures, derived_sql = extractor.extract_measures(
        "SELECT approx_percentile(duration_ms, 1.0, 0.9) / 1000 FROM parent_node",
    )
    expected_measures = []
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT approx_percentile(duration_ms, 1.0, 0.9) / 1000 FROM parent_node",
        ),
    )
