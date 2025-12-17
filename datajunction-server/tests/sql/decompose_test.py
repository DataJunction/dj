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
            merge="SUM",  # SUM merges as SUM
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
            merge="SUM",
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
            merge="SUM",
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
            merge="SUM",
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
            merge="SUM",
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
            merge="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="fraud_sales_sum_0e1bc4a2",
            expression="fraud_sales",
            aggregation="SUM",
            merge="SUM",
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
            merge="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="fraud_sales_sum_0e1bc4a2",
            expression="fraud_sales",
            aggregation="SUM",
            merge="SUM",
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
            merge="SUM",
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
            merge="SUM",
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

    AVG decomposes into two components:
    - SUM: aggregation=SUM, merge=SUM
    - COUNT: aggregation=COUNT, merge=SUM (counts roll up as sums)

    The combiner is: SUM(sum_col) / SUM(count_col)
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
            merge="SUM",  # COUNT merges as SUM
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="sales_amount_sum_b5a3cefe",
            expression="sales_amount",
            aggregation="SUM",
            merge="SUM",
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
            merge="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="impressions_sum_3be0a0e7",
            expression="impressions",
            aggregation="SUM",
            merge="SUM",
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
            merge="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="impressions_sum_3be0a0e7",
            expression="impressions",
            aggregation="SUM",
            merge="SUM",
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
            merge="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="impressions_sum_3be0a0e7",
            expression="impressions",
            aggregation="SUM",
            merge="SUM",
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
            merge="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="impressions_sum_3be0a0e7",
            expression="impressions",
            aggregation="SUM",
            merge="SUM",
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
            merge="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="views_sum_d8e39817",
            expression="views",
            aggregation="SUM",
            merge="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT ln(SUM(clicks_sum_c45fd8cf) + 1) / SUM(views_sum_d8e39817) FROM parent_node",
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
            merge="MAX",
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
            merge="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="action_two_sum_05d921a8",
            expression="COALESCE(action_two, 0)",
            aggregation="SUM",
            merge="SUM",
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

    COUNT aggregation merges as SUM (sum up the counts).
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
            merge="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
    ]
    assert measures == expected_measures
    # Verify COUNT merges as SUM
    assert measures[0].merge == "SUM"
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
            merge=None,
            rule=AggregationRule(
                type=Aggregability.LIMITED,
                level=["user_id"],
            ),
        ),
        MetricComponent(
            name="action_count_50d753fd",
            expression="action",
            aggregation="COUNT",
            merge="SUM",
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
            merge="ANY_VALUE",
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
            merge="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="region_account_id_distinct_ee608f27",
            expression="IF(region = 'US', account_id, NULL)",
            aggregation=None,
            merge=None,
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
            merge="MAX",
            rule=AggregationRule(type=Aggregability.FULL, level=None),
        ),
        MetricComponent(
            name="b_max_6d64a2e5",
            expression="b",
            aggregation="MAX",
            merge="MAX",
            rule=AggregationRule(type=Aggregability.FULL, level=None),
        ),
    ]
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse(
            "SELECT CAST(coalesce(MAX(a_max_0f00346b), MAX(b_max_6d64a2e5), 0) AS DOUBLE) + "
            "CAST(coalesce(MAX(a_max_0f00346b), MAX(b_max_6d64a2e5)) AS DOUBLE) FROM parent_node",
        ),
    )


def test_min_agg():
    extractor = MetricComponentExtractor.from_query_string("SELECT MIN(a) FROM parent")
    measures, derived_sql = extractor.extract()
    assert measures == [
        MetricComponent(
            name="a_min_3cf406a5",
            expression="a",
            aggregation="MIN",
            merge="MIN",
            rule=AggregationRule(type=Aggregability.FULL, level=None),
        ),
    ]
    assert str(derived_sql) == str(parse("SELECT MIN(a_min_3cf406a5) FROM parent"))


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
            merge="SUM",
            rule=AggregationRule(type=Aggregability.FULL),
        ),
        MetricComponent(
            name="count_58ac32c5",
            expression="*",
            aggregation="COUNT",
            merge="SUM",
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
            merge="SUM",
            rule=AggregationRule(type=Aggregability.FULL, level=None),
        ),
        MetricComponent(
            name="time_to_dispatch_sum_3bc9baed",
            expression="CAST(time_to_dispatch AS INT)",
            aggregation="SUM",
            merge="SUM",
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
    expected_measures = []
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT MAX_BY(IF(condition, 1, 0), dimension) FROM parent_node"),
    )


def test_min_by():
    """
    Test decomposition for a metric that uses MIN_BY.
    """
    extractor = MetricComponentExtractor.from_query_string(
        "SELECT MIN_BY(IF(condition, 1, 0), dimension) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = []
    assert measures == expected_measures
    assert str(derived_sql) == str(
        parse("SELECT MIN_BY(IF(condition, 1, 0), dimension) FROM parent_node"),
    )


def test_approx_count_distinct():
    """
    Test decomposition for an approximate count distinct metric.

    APPROX_COUNT_DISTINCT decomposes to HLL sketch operations using Spark functions:
    - aggregation: hll_sketch_agg (Spark's function for building HLL sketch)
    - merge: hll_union (Spark's function for combining sketches)
    - The combiner uses hll_sketch_estimate(hll_union(...)) to get the final count

    Translation to other dialects (Druid, Trino) happens in the transpilation layer.
    """
    extractor = MetricComponentExtractor.from_query_string(
        "SELECT APPROX_COUNT_DISTINCT(user_id) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()
    expected_measures = [
        MetricComponent(
            name="user_id_hll_7f092f23",
            expression="user_id",
            aggregation="hll_sketch_agg",  # Spark's HLL accumulate
            merge="hll_union",  # Spark's HLL merge
            rule=AggregationRule(type=Aggregability.FULL, level=None),
        ),
    ]
    assert measures == expected_measures
    # Verify the derived SQL uses Spark HLL functions
    assert str(derived_sql) == str(
        parse(
            "SELECT hll_sketch_estimate(hll_union(user_id_hll_7f092f23)) FROM parent_node",
        ),
    )


def test_approx_count_distinct_with_expression():
    """
    Test decomposition for APPROX_COUNT_DISTINCT with a complex expression.
    """
    extractor = MetricComponentExtractor.from_query_string(
        "SELECT APPROX_COUNT_DISTINCT(COALESCE(user_id, 'unknown')) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()

    # Verify structure of the measure (hash value may vary)
    assert len(measures) == 1
    measure = measures[0]
    assert measure.expression == "COALESCE(user_id, 'unknown')"
    assert measure.aggregation == "hll_sketch_agg"
    assert measure.merge == "hll_union"
    assert measure.rule.type == Aggregability.FULL

    # Verify derived SQL contains the HLL functions
    derived_str = str(derived_sql)
    assert "hll_sketch_estimate" in derived_str
    assert "hll_union" in derived_str
    assert "_hll_" in derived_str


def test_approx_count_distinct_with_conditional():
    """
    Test decomposition for APPROX_COUNT_DISTINCT with IF condition.
    """
    extractor = MetricComponentExtractor.from_query_string(
        "SELECT APPROX_COUNT_DISTINCT(IF(active = 1, user_id, NULL)) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()

    # Verify structure of the measure
    assert len(measures) == 1
    measure = measures[0]
    assert measure.expression == "IF(active = 1, user_id, NULL)"
    assert measure.aggregation == "hll_sketch_agg"
    assert measure.merge == "hll_union"
    assert measure.rule.type == Aggregability.FULL

    # Verify derived SQL contains the HLL functions
    derived_str = str(derived_sql)
    assert "hll_sketch_estimate" in derived_str
    assert "hll_union" in derived_str


def test_approx_count_distinct_combined_with_sum():
    """
    Test decomposition for a metric combining APPROX_COUNT_DISTINCT with SUM.
    """
    extractor = MetricComponentExtractor.from_query_string(
        "SELECT SUM(revenue) / APPROX_COUNT_DISTINCT(user_id) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()

    # Should have two measures: one for SUM, one for HLL
    assert len(measures) == 2

    # Find SUM measure
    sum_measures = [m for m in measures if m.aggregation == "SUM"]
    assert len(sum_measures) == 1
    assert sum_measures[0].expression == "revenue"
    assert sum_measures[0].merge == "SUM"

    # Find HLL measure
    hll_measures = [m for m in measures if m.aggregation == "hll_sketch_agg"]
    assert len(hll_measures) == 1
    assert hll_measures[0].expression == "user_id"
    assert hll_measures[0].merge == "hll_union"

    # Verify derived SQL has both
    derived_str = str(derived_sql)
    assert "SUM(" in derived_str
    assert "hll_sketch_estimate" in derived_str
    assert "hll_union" in derived_str


def test_approx_count_distinct_multiple():
    """
    Test decomposition with multiple APPROX_COUNT_DISTINCT on different columns.
    """
    extractor = MetricComponentExtractor.from_query_string(
        "SELECT APPROX_COUNT_DISTINCT(user_id) + APPROX_COUNT_DISTINCT(session_id) "
        "FROM parent_node",
    )
    measures, derived_sql = extractor.extract()

    # Should have two HLL measures
    assert len(measures) == 2
    assert all(m.aggregation == "hll_sketch_agg" for m in measures)
    assert all(m.merge == "hll_union" for m in measures)

    # Verify both expressions are present
    expressions = {m.expression for m in measures}
    assert expressions == {"user_id", "session_id"}

    # Verify derived SQL has both HLL estimate calls
    derived_str = str(derived_sql)
    assert derived_str.count("hll_sketch_estimate") == 2
    assert derived_str.count("hll_union") == 2


def test_approx_count_distinct_rate():
    """
    Test decomposition for a rate metric using APPROX_COUNT_DISTINCT.

    Example: unique users who clicked / unique users who viewed
    """
    extractor = MetricComponentExtractor.from_query_string(
        "SELECT CAST(APPROX_COUNT_DISTINCT(IF(clicked = 1, user_id, NULL)) AS DOUBLE) / "
        "CAST(APPROX_COUNT_DISTINCT(user_id) AS DOUBLE) FROM parent_node",
    )
    measures, derived_sql = extractor.extract()

    # Should have two HLL measures
    assert len(measures) == 2
    assert all(m.aggregation == "hll_sketch_agg" for m in measures)
    assert all(m.merge == "hll_union" for m in measures)

    # Verify expressions
    expressions = {m.expression for m in measures}
    assert "user_id" in expressions
    assert "IF(clicked = 1, user_id, NULL)" in expressions

    # Verify derived SQL structure
    derived_str = str(derived_sql)
    assert "CAST(" in derived_str
    assert "hll_sketch_estimate" in derived_str
    assert "hll_union" in derived_str


def test_approx_count_distinct_dialect_translation():
    """
    Test that the decomposed HLL SQL can be translated to different dialects.

    This is an integration test showing the full flow:
    1. Decompose APPROX_COUNT_DISTINCT -> Spark HLL functions
    2. Translate Spark HLL functions -> target dialect
    """
    from datajunction_server.models.engine import Dialect
    from datajunction_server.sql.translation import translate_sql

    extractor = MetricComponentExtractor.from_query_string(
        "SELECT APPROX_COUNT_DISTINCT(user_id) FROM parent_node",
    )
    _, derived_sql = extractor.extract()
    spark_sql = str(derived_sql)

    # The decomposed SQL uses Spark HLL functions
    assert "hll_sketch_estimate" in spark_sql
    assert "hll_union" in spark_sql

    # Translate to Druid
    druid_sql = translate_sql(spark_sql, Dialect.DRUID)
    assert "APPROX_COUNT_DISTINCT_DS_HLL" in druid_sql
    assert "DS_HLL" in druid_sql
    assert "hll_sketch_estimate" not in druid_sql
    assert "hll_union" not in druid_sql

    # Translate to Trino
    trino_sql = translate_sql(spark_sql, Dialect.TRINO)
    assert "cardinality" in trino_sql
    assert "merge" in trino_sql
    assert "hll_sketch_estimate" not in trino_sql
    assert "hll_union" not in trino_sql

    # Spark to Spark is identity
    spark_spark_sql = translate_sql(spark_sql, Dialect.SPARK)
    assert spark_spark_sql == spark_sql


def test_approx_count_distinct_combined_metrics_dialect_translation():
    """
    Test dialect translation for a complex metric combining HLL with other aggregations.
    """
    from datajunction_server.models.engine import Dialect
    from datajunction_server.sql.translation import translate_sql

    extractor = MetricComponentExtractor.from_query_string(
        "SELECT SUM(revenue) / APPROX_COUNT_DISTINCT(user_id) AS revenue_per_user "
        "FROM parent_node",
    )
    _, derived_sql = extractor.extract()
    spark_sql = str(derived_sql)

    # Verify Spark SQL structure - contains both SUM and HLL
    assert "SUM(" in spark_sql
    assert "hll_sketch_estimate(hll_union(" in spark_sql

    # Translate to Druid - should preserve SUM but translate HLL
    druid_sql = translate_sql(spark_sql, Dialect.DRUID)
    assert "SUM(" in druid_sql
    assert "APPROX_COUNT_DISTINCT_DS_HLL(DS_HLL(" in druid_sql
    assert "hll_sketch_estimate" not in druid_sql

    # Translate to Trino
    trino_sql = translate_sql(spark_sql, Dialect.TRINO)
    assert "SUM(" in trino_sql
    assert "cardinality(merge(" in trino_sql
    assert "hll_sketch_estimate" not in trino_sql
