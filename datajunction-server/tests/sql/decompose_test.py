"""
Tests for ``datajunction_server.sql.decompose``.
"""

import pytest

from datajunction_server.models.cube_materialization import (
    Aggregability,
    AggregationRule,
    MetricComponent,
)
from datajunction_server.sql.decompose import MetricComponentExtractor
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.sql.parsing.backends.exceptions import DJParseException


def test_simple_sum():
    """
    Test decomposition for a metric definition that is a simple sum.
    """
    extractor = MetricComponentExtractor.from_query_string(
        "SELECT SUM(sales_amount) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="sales_amount_sum_b5a3cefe",
            expression="sales_amount",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT SUM(sales_amount_sum_b5a3cefe) FROM parent_node"),
    )


def test_sum_with_cast():
    """
    Test decomposition for a metric definition that has a sum with a cast.
    """
    extractor = MetricComponentExtractor.from_query_string(
        "SELECT CAST(SUM(sales_amount) AS DOUBLE) * 100.0 FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="sales_amount_sum_b5a3cefe",
            expression="sales_amount",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT CAST(SUM(sales_amount_sum_b5a3cefe) AS DOUBLE) * 100.0 FROM parent_node",
        ),
    )

    extractor = MetricComponentExtractor.from_query_string(
        "SELECT 100.0 * SUM(sales_amount) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="sales_amount_sum_b5a3cefe",
            expression="sales_amount",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT 100.0 * SUM(sales_amount_sum_b5a3cefe) FROM parent_node"),
    )


def test_sum_with_coalesce():
    """
    Test decomposition for a metric definition that has a sum with coalesce.
    """
    extractor = MetricComponentExtractor.from_query_string(
        "SELECT COALESCE(SUM(sales_amount), 0) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="sales_amount_sum_b5a3cefe",
            expression="sales_amount",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT COALESCE(SUM(sales_amount_sum_b5a3cefe), 0) FROM parent_node"),
    )

    extractor = MetricComponentExtractor.from_query_string(
        "SELECT SUM(COALESCE(sales_amount, 0)) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="sales_amount_sum_65a3b528",
            expression="COALESCE(sales_amount, 0)",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT SUM(sales_amount_sum_65a3b528) FROM parent_node"),
    )


def test_multiple_sums():
    """
    Test decomposition for a metric definition that has multiple sums.
    """
    extractor = MetricComponentExtractor.from_query_string(
        "SELECT SUM(sales_amount) + SUM(fraud_sales) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="sales_amount_sum_b5a3cefe",
            expression="sales_amount",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="fraud_sales_sum_0e1bc4a2",
            expression="fraud_sales",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT SUM(sales_amount_sum_b5a3cefe) + "
            "SUM(fraud_sales_sum_0e1bc4a2) FROM parent_node",
        ),
    )

    extractor = MetricComponentExtractor.from_query_string(
        "SELECT SUM(sales_amount) - SUM(fraud_sales) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="sales_amount_sum_b5a3cefe",
            expression="sales_amount",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="fraud_sales_sum_0e1bc4a2",
            expression="fraud_sales",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT SUM(sales_amount_sum_b5a3cefe) - "
            "SUM(fraud_sales_sum_0e1bc4a2) FROM parent_node",
        ),
    )


def test_nested_functions():
    """
    Test behavior with deeply nested functions inside aggregations.
    """
    extractor = MetricComponentExtractor.from_query_string(
        "SELECT SUM(ROUND(COALESCE(sales_amount, 0) * 1.1)) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="sales_amount_sum_090066cf",
            expression="ROUND(COALESCE(sales_amount, 0) * 1.1)",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT SUM(sales_amount_sum_090066cf) FROM parent_node"),
    )

    extractor = MetricComponentExtractor.from_query_string(
        "SELECT LN(SUM(COALESCE(sales_amount, 0)) + 1) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="sales_amount_sum_65a3b528",
            expression="COALESCE(sales_amount, 0)",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT LN(SUM(sales_amount_sum_65a3b528) + 1) FROM parent_node"),
    )


def test_average():
    """
    Test decomposition for a metric definition that uses AVG.
    """
    extractor = MetricComponentExtractor.from_query_string(
        "SELECT AVG(sales_amount) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()

    expected_measures = [
        MetricComponent(
            name="sales_amount_count_b5a3cefe",
            expression="sales_amount",
            aggregation="COUNT",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="sales_amount_sum_b5a3cefe",
            expression="sales_amount",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT SUM(sales_amount_sum_b5a3cefe) / "
            "SUM(sales_amount_count_b5a3cefe) FROM parent_node",
        ),
    )


def test_rate():
    """
    Test decomposition for a rate metric definition.
    """
    extractor = MetricComponentExtractor.from_query_string(
        "SELECT SUM(clicks) / SUM(impressions) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures0 = [
        MetricComponent(
            name="clicks_sum_c45fd8cf",
            expression="clicks",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="impressions_sum_3be0a0e7",
            expression="impressions",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures0
    assert str(derived_sql) == str(
        parse(
            "SELECT SUM(clicks_sum_c45fd8cf) / SUM(impressions_sum_3be0a0e7) FROM parent_node",
        ),
    )

    extractor = MetricComponentExtractor.from_query_string(
        "SELECT 1.0 * SUM(clicks) / NULLIF(SUM(impressions), 0) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="clicks_sum_c45fd8cf",
            expression="clicks",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="impressions_sum_3be0a0e7",
            expression="impressions",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT 1.0 * SUM(clicks_sum_c45fd8cf) / "
            "NULLIF(SUM(impressions_sum_3be0a0e7), 0) FROM parent_node",
        ),
    )

    extractor = MetricComponentExtractor.from_query_string(
        "SELECT CAST(CAST(SUM(clicks) AS INT) AS DOUBLE) / "
        "CAST(SUM(impressions) AS DOUBLE) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    assert measures == expected_measures0
    assert str(derived_sql) == str(
        parse(
            "SELECT CAST(CAST(SUM(clicks_sum_c45fd8cf) AS INT) AS DOUBLE) / "
            "CAST(SUM(impressions_sum_3be0a0e7) AS DOUBLE) FROM parent_node",
        ),
    )

    extractor = MetricComponentExtractor.from_query_string(
        "SELECT COALESCE(SUM(clicks) / SUM(impressions), 0) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="clicks_sum_c45fd8cf",
            expression="clicks",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="impressions_sum_3be0a0e7",
            expression="impressions",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT COALESCE(SUM(clicks_sum_c45fd8cf) / "
            "SUM(impressions_sum_3be0a0e7), 0) FROM parent_node",
        ),
    )

    extractor = MetricComponentExtractor.from_query_string(
        "SELECT IF(SUM(clicks) > 0, CAST(SUM(impressions) AS DOUBLE) "
        "/ CAST(SUM(clicks) AS DOUBLE), NULL) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="clicks_sum_c45fd8cf",
            expression="clicks",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="impressions_sum_3be0a0e7",
            expression="impressions",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT IF(SUM(clicks_sum_c45fd8cf) > 0, CAST(SUM(impressions_sum_3be0a0e7) AS DOUBLE)"
            " / CAST(SUM(clicks_sum_c45fd8cf) AS DOUBLE), NULL) FROM parent_node",
        ),
    )

    extractor = MetricComponentExtractor.from_query_string(
        "SELECT ln(sum(clicks) + 1) / sum(views) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="clicks_sum_c45fd8cf",
            expression="clicks",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="views_sum_d8e39817",
            expression="views",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT ln(sum(clicks_sum_c45fd8cf) + 1) / sum(views_sum_d8e39817) FROM parent_node",
        ),
    )


def test_max_if():
    """
    Test decomposition for a metric definition that uses MAX.
    """
    extractor = MetricComponentExtractor.from_query_string(
        "SELECT MAX(IF(condition, 1, 0)) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="condition_max_f04b0c57",
            expression="IF(condition, 1, 0)",
            aggregation="MAX",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT MAX(condition_max_f04b0c57) FROM parent_node"),
    )


def test_fraction_with_if():
    """
    Test decomposition for a rate metric with complex numerator and denominators.
    """
    extractor = MetricComponentExtractor.from_query_string(
        "SELECT IF(SUM(COALESCE(action, 0)) > 0, "
        "CAST(SUM(COALESCE(action_two, 0)) AS DOUBLE) / "
        "CAST(SUM(COALESCE(action, 0)) AS DOUBLE), NULL) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()

    expected_measures = [
        MetricComponent(
            name="action_sum_c9802ccb",
            expression="COALESCE(action, 0)",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="action_two_sum_05d921a8",
            expression="COALESCE(action_two, 0)",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT IF(SUM(action_sum_c9802ccb) > 0, "
            "CAST(SUM(action_two_sum_05d921a8) AS DOUBLE) / "
            "CAST(SUM(action_sum_c9802ccb) AS DOUBLE), NULL) FROM parent_node",
        ),
    )


def test_count():
    """
    Test decomposition for a count metric.
    """
    extractor = MetricComponentExtractor.from_query_string(
        "SELECT COUNT(IF(action = 1, action_event_ts, 0)) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="action_action_event_ts_count_7d582e65",
            expression="IF(action = 1, action_event_ts, 0)",
            aggregation="COUNT",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT SUM(action_action_event_ts_count_7d582e65) FROM parent_node"),
    )


def test_count_distinct_rate():
    """
    Test decomposition for a metric that uses count distinct.
    """
    extractor = MetricComponentExtractor.from_query_string(
        "SELECT COUNT(DISTINCT user_id) / COUNT(action) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="user_id_distinct_7f092f23",
            expression="user_id",
            aggregation=None,
            rule=AggregationRule(
                type=Aggregability.LIMITED,
                level=["user_id"],
            ),
        ),
        MetricComponent(
            name="action_count_50d753fd",
            expression="action",
            aggregation="COUNT",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT COUNT( DISTINCT user_id_distinct_7f092f23) / "
            "SUM(action_count_50d753fd) FROM parent_node",
        ),
    )


def test_any_value():
    """
    Test decomposition for a metric definition that has ANY_VALUE as the agg function
    """
    extractor = MetricComponentExtractor.from_query_string(
        "SELECT ANY_VALUE(sales_amount) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="sales_amount_any_value_b5a3cefe",
            expression="sales_amount",
            aggregation="ANY_VALUE",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT ANY_VALUE(sales_amount_any_value_b5a3cefe) FROM parent_node"),
    )


def test_no_aggregation():
    """
    Test behavior when there is no aggregation function in the metric query.
    """
    extractor = MetricComponentExtractor.from_query_string(
        "SELECT sales_amount FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = []
    assert measures == expected_measures
    assert str(derived_sql) == str(parse("SELECT sales_amount FROM parent_node"))


def test_multiple_aggregations_with_conditions():
    """
    Test behavior with conditional aggregations in the metric query.
    """
    extractor = MetricComponentExtractor.from_query_string(
        "SELECT SUM(IF(region = 'US', sales_amount, 0)) + "
        "COUNT(DISTINCT IF(region = 'US', account_id, NULL)) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="region_sales_amount_sum_5467b14a",
            expression="IF(region = 'US', sales_amount, 0)",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="region_account_id_distinct_ee608f27",
            expression="IF(region = 'US', account_id, NULL)",
            aggregation=None,
            rule=AggregationRule(
                type=Aggregability.LIMITED,
                level=["IF(region = 'US', account_id, NULL)"],
            ),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT SUM(region_sales_amount_sum_5467b14a) + "
            "COUNT(DISTINCT region_account_id_distinct_ee608f27) FROM parent_node",
        ),
    )

    extractor = MetricComponentExtractor.from_query_string(
        "SELECT cast(coalesce(max(a), max(b), 0) as double) + "
        "cast(coalesce(max(a), max(b)) as double) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="a_max_0f00346b",
            expression="a",
            aggregation="MAX",
            rule=AggregationRule(type=Aggregability.FULL, level=None),
        ),
        MetricComponent(
            name="b_max_6d64a2e5",
            expression="b",
            aggregation="MAX",
            rule=AggregationRule(type=Aggregability.FULL, level=None),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT CAST(coalesce(max(a_max_0f00346b), max(b_max_6d64a2e5), 0) AS DOUBLE) + "
            "CAST(coalesce(max(a_max_0f00346b), max(b_max_6d64a2e5)) AS DOUBLE) FROM parent_node",
        ),
    )


def test_empty_query():
    """
    Test behavior when the metric query is empty.
    """
    with pytest.raises(DJParseException, match="Empty query provided!"):
        extractor = MetricComponentExtractor.from_query_string("")
        extractor.extract()


def test_unsupported_aggregation_function():
    """
    Test behavior when the query contains unsupported aggregation functions. We just return an
    empty list of measures in this case, because there are no pre-aggregatable measures.
    """
    extractor = MetricComponentExtractor.from_query_string(
        "SELECT MEDIAN(sales_amount) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = []
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT MEDIAN(sales_amount) FROM parent_node"),
    )

    extractor = MetricComponentExtractor.from_query_string(
        "SELECT approx_percentile(duration_ms, 1.0, 0.9) / 1000 FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = []
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT approx_percentile(duration_ms, 1.0, 0.9) / 1000 FROM parent_node",
        ),
    )


def test_count_if():
    """
    Test decomposition for count_if.
    """
    extractor = MetricComponentExtractor.from_query_string(
        "SELECT CAST(COUNT_IF(ARRAY_CONTAINS(field_a, 'xyz')) AS FLOAT) / COUNT(*) "
        "FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="field_a_count_if_3979ffbd",
            expression="ARRAY_CONTAINS(field_a, 'xyz')",
            aggregation="COUNT_IF",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="count_58ac32c5",
            expression="*",
            aggregation="COUNT",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT  CAST(SUM(field_a_count_if_3979ffbd) AS FLOAT) / SUM(count_58ac32c5) "
            "FROM parent_node",
        ),
    )


def test_metric_query_with_aliases():
    """
    Test behavior when the query contains unsupported aggregation functions. We just return an
    empty list of measures in this case, because there are no pre-aggregatable measures.
    """
    extractor = MetricComponentExtractor.from_query_string(
        "SELECT avg(cast(repair_orders_fact.time_to_dispatch as int)) "
        "FROM default.repair_orders_fact repair_orders_fact",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="time_to_dispatch_count_3bc9baed",
            expression="CAST(time_to_dispatch AS INT)",
            aggregation="COUNT",
            rule=AggregationRule(type=Aggregability.FULL, level=None),
        ),
        MetricComponent(
            name="time_to_dispatch_sum_3bc9baed",
            expression="CAST(time_to_dispatch AS INT)",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL, level=None),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT SUM(time_to_dispatch_sum_3bc9baed) / "
            "SUM(time_to_dispatch_count_3bc9baed) FROM default.repair_orders_fact",
        ),
    )


def test_max_by():
    """
    Test decomposition for a metric that uses MAX_BY.
    """
    extractor = MetricComponentExtractor.from_query_string(
        "SELECT MAX_BY(IF(condition, 1, 0), dimension) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="condition_max_by_da873133",
            expression="IF(condition, 1, 0)",
            aggregation="MAX",
            rule=AggregationRule(type=Aggregability.LIMITED, level=["dimension"]),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT MAX_BY(condition_max_by_da873133, dimension) FROM parent_node"),
    )


def test_min_by():
    """
    Test decomposition for a metric that uses MIN_BY.
    """
    extractor = MetricComponentExtractor.from_query_string(
        "SELECT MIN_BY(IF(condition, 1, 0), dimension) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="condition_min_by_da873133",
            expression="IF(condition, 1, 0)",
            aggregation="MIN",
            rule=AggregationRule(type=Aggregability.LIMITED, level=["dimension"]),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT MIN_BY(condition_min_by_da873133, dimension) FROM parent_node"),
    )
