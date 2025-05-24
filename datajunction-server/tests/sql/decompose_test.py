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
            name="sales_amount_sum_a1b27bc7",
            expression="sales_amount",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT SUM(sales_amount_sum_a1b27bc7) FROM parent_node"),
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
            name="sales_amount_sum_a1b27bc7",
            expression="sales_amount",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT CAST(SUM(sales_amount_sum_a1b27bc7) AS DOUBLE) * 100.0 FROM parent_node",
        ),
    )

    extractor = MetricComponentExtractor.from_query_string(
        "SELECT 100.0 * SUM(sales_amount) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="sales_amount_sum_a1b27bc7",
            expression="sales_amount",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT 100.0 * SUM(sales_amount_sum_a1b27bc7) FROM parent_node"),
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
            name="sales_amount_sum_a1b27bc7",
            expression="sales_amount",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT COALESCE(SUM(sales_amount_sum_a1b27bc7), 0) FROM parent_node"),
    )

    extractor = MetricComponentExtractor.from_query_string(
        "SELECT SUM(COALESCE(sales_amount, 0)) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="sales_amount_sum_bc5c6414",
            expression="COALESCE(sales_amount, 0)",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT SUM(sales_amount_sum_bc5c6414) FROM parent_node"),
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
            name="sales_amount_sum_a1b27bc7",
            expression="sales_amount",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="fraud_sales_sum_0a5d6799",
            expression="fraud_sales",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT SUM(sales_amount_sum_a1b27bc7) + "
            "SUM(fraud_sales_sum_0a5d6799) FROM parent_node",
        ),
    )

    extractor = MetricComponentExtractor.from_query_string(
        "SELECT SUM(sales_amount) - SUM(fraud_sales) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT SUM(sales_amount_sum_a1b27bc7) - "
            "SUM(fraud_sales_sum_0a5d6799) FROM parent_node",
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
            name="sales_amount_sum_d511860a",
            expression="ROUND(COALESCE(sales_amount, 0) * 1.1)",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT SUM(sales_amount_sum_d511860a) FROM parent_node"),
    )

    extractor = MetricComponentExtractor.from_query_string(
        "SELECT LN(SUM(COALESCE(sales_amount, 0)) + 1) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="sales_amount_sum_bc5c6414",
            expression="COALESCE(sales_amount, 0)",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT LN(SUM(sales_amount_sum_bc5c6414) + 1) FROM parent_node"),
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
            name="sales_amount_count_a1b27bc7",
            expression="sales_amount",
            aggregation="COUNT",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="sales_amount_sum_a1b27bc7",
            expression="sales_amount",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT SUM(sales_amount_sum_a1b27bc7) / "
            "SUM(sales_amount_count_a1b27bc7) FROM parent_node",
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
            name="clicks_sum_c9e9e0fc",
            expression="clicks",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="impressions_sum_87e980e6",
            expression="impressions",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures0
    assert str(derived_sql) == str(
        parse(
            "SELECT SUM(clicks_sum_c9e9e0fc) / SUM(impressions_sum_87e980e6) FROM parent_node",
        ),
    )

    extractor = MetricComponentExtractor.from_query_string(
        "SELECT 1.0 * SUM(clicks) / NULLIF(SUM(impressions), 0) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="clicks_sum_c9e9e0fc",
            expression="clicks",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="impressions_sum_87e980e6",
            expression="impressions",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT 1.0 * SUM(clicks_sum_c9e9e0fc) / "
            "NULLIF(SUM(impressions_sum_87e980e6), 0) FROM parent_node",
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
            "SELECT CAST(CAST(SUM(clicks_sum_c9e9e0fc) AS INT) AS DOUBLE) / "
            "CAST(SUM(impressions_sum_87e980e6) AS DOUBLE) FROM parent_node",
        ),
    )

    extractor = MetricComponentExtractor.from_query_string(
        "SELECT COALESCE(SUM(clicks) / SUM(impressions), 0) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="clicks_sum_c9e9e0fc",
            expression="clicks",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="impressions_sum_87e980e6",
            expression="impressions",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT COALESCE(SUM(clicks_sum_c9e9e0fc) / "
            "SUM(impressions_sum_87e980e6), 0) FROM parent_node",
        ),
    )

    extractor = MetricComponentExtractor.from_query_string(
        "SELECT IF(SUM(clicks) > 0, CAST(SUM(impressions) AS DOUBLE) "
        "/ CAST(SUM(clicks) AS DOUBLE), NULL) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="clicks_sum_c9e9e0fc",
            expression="clicks",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="impressions_sum_87e980e6",
            expression="impressions",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT IF(SUM(clicks_sum_c9e9e0fc) > 0, CAST(SUM(impressions_sum_87e980e6) AS DOUBLE)"
            " / CAST(SUM(clicks_sum_c9e9e0fc) AS DOUBLE), NULL) FROM parent_node",
        ),
    )

    extractor = MetricComponentExtractor.from_query_string(
        "SELECT ln(sum(clicks) + 1) / sum(views) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="clicks_sum_c9e9e0fc",
            expression="clicks",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="views_sum_59a14a57",
            expression="views",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT ln(sum(clicks_sum_c9e9e0fc) + 1) / sum(views_sum_59a14a57) FROM parent_node",
        ),
    )


def test_has_ever():
    """
    Test decomposition for a metric definition that uses MAX.
    """
    extractor = MetricComponentExtractor.from_query_string(
        "SELECT MAX(IF(condition, 1, 0)) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="condition_max_98f2d913",
            expression="IF(condition, 1, 0)",
            aggregation="MAX",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT MAX(condition_max_98f2d913) FROM parent_node"),
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
            name="action_sum_d0b4f8e5",
            expression="COALESCE(action, 0)",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="action_two_sum_0c8945fc",
            expression="COALESCE(action_two, 0)",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT IF(SUM(action_sum_d0b4f8e5) > 0, "
            "CAST(SUM(action_two_sum_0c8945fc) AS DOUBLE) / "
            "CAST(SUM(action_sum_d0b4f8e5) AS DOUBLE), NULL) FROM parent_node",
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
            name="action_action_event_ts_count_59b28b54",
            expression="IF(action = 1, action_event_ts, 0)",
            aggregation="COUNT",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT SUM(action_action_event_ts_count_59b28b54) FROM parent_node"),
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
            name="user_id_distinct_e8701ad4",
            expression="user_id",
            aggregation=None,
            rule=AggregationRule(
                type=Aggregability.LIMITED,
                level=["user_id"],
            ),
        ),
        MetricComponent(
            name="action_count_418c5509",
            expression="action",
            aggregation="COUNT",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT COUNT( DISTINCT user_id_distinct_e8701ad4) / "
            "SUM(action_count_418c5509) FROM parent_node",
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
            name="sales_amount_any_value_a1b27bc7",
            expression="sales_amount",
            aggregation="ANY_VALUE",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT ANY_VALUE(sales_amount_any_value_a1b27bc7) FROM parent_node"),
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
            name="region_sales_amount_sum_55eb544e",
            expression="IF(region = 'US', sales_amount, 0)",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="region_account_id_distinct_11ca6967",
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
            "SELECT SUM(region_sales_amount_sum_55eb544e) + "
            "COUNT(DISTINCT region_account_id_distinct_11ca6967) FROM parent_node",
        ),
    )

    extractor = MetricComponentExtractor.from_query_string(
        "SELECT cast(coalesce(max(a), max(b), 0) as double) + "
        "cast(coalesce(max(a), max(b)) as double) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="a_max_0cc175b9",
            expression="a",
            aggregation="MAX",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="b_max_92eb5ffe",
            expression="b",
            aggregation="MAX",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT CAST(coalesce(max(a_max_0cc175b9), max(b_max_92eb5ffe), 0) AS DOUBLE) + "
            "CAST(coalesce(max(a_max_0cc175b9), max(b_max_92eb5ffe)) AS DOUBLE) FROM parent_node",
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
            name="field_a_count_if_c1f2ed10",
            expression="ARRAY_CONTAINS(field_a, 'xyz')",
            aggregation="COUNT_IF",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="count_3389dae3",
            expression="*",
            aggregation="COUNT",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT  CAST(SUM(field_a_count_if_c1f2ed10) AS FLOAT) / SUM(count_3389dae3) "
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
            name="time_to_dispatch_count_bf99afd6",
            expression="CAST(time_to_dispatch AS INT)",
            aggregation="COUNT",
            rule=AggregationRule(type=Aggregability.FULL, level=None),
        ),
        MetricComponent(
            name="time_to_dispatch_sum_bf99afd6",
            expression="CAST(time_to_dispatch AS INT)",
            aggregation="SUM",
            rule=AggregationRule(type=Aggregability.FULL, level=None),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT SUM(time_to_dispatch_sum_bf99afd6) / "
            "SUM(time_to_dispatch_count_bf99afd6) FROM default.repair_orders_fact",
        ),
    )
