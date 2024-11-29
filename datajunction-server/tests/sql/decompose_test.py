"""
Tests for ``datajunction_server.sql.decompose``.
"""
import pytest

from datajunction_server.sql.decompose import Measure, extractor
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
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT SUM(sales_amount_sum_0) FROM parent_node"),
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
        ),
        Measure(name="count", expression="1", aggregation="COUNT"),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT SUM(sales_amount_sum_0) / COUNT(count) FROM parent_node"),
    )


def test_rate():
    """
    Test decomposition for a metric definition that is a ratio
    """
    measures, derived_sql = extractor.extract_measures(
        "SELECT SUM(clicks) / SUM(impressions) FROM parent_node",
    )
    expected_measures = [
        Measure(name="clicks_sum_0", expression="clicks", aggregation="SUM"),
        Measure(name="impressions_sum_1", expression="impressions", aggregation="SUM"),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT SUM(clicks_sum_0) / SUM(impressions_sum_1) FROM parent_node"),
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
        ),
        Measure(
            name="action_two_sum_2",
            expression="COALESCE(action_two, 0)",
            aggregation="SUM",
        ),
        Measure(
            name="action_sum_3",
            expression="COALESCE(action, 0)",
            aggregation="SUM",
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
        ),
        Measure(name="action_count_1", expression="action", aggregation="COUNT"),
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
        ),
        Measure(
            name="region_account_id_count_1",
            expression="DISTINCT IF(region = 'US', account_id, NULL)",
            aggregation="COUNT",
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT  SUM(region_sales_amount_sum_0) + COUNT("
            "DISTINCT region_account_id_count_1) FROM parent_node",
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
    Test behavior when the query contains unsupported aggregation functions.
    """
    measures, derived_sql = extractor.extract_measures(
        "SELECT MEDIAN(sales_amount) FROM parent_node",
    )
    expected_measures = []
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT MEDIAN(sales_amount) FROM parent_node"),
    )
