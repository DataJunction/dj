"""
Tests for ``datajunction_server.sql.decompose``.
"""

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from tests.construction.build_v3 import assert_sql_equal
from datajunction_server.database.node import Node, NodeRelationship, NodeRevision
from datajunction_server.models.cube_materialization import (
    Aggregability,
    AggregationRule,
    MetricComponent,
)
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.decompose import MetricComponentExtractor
from datajunction_server.sql.parsing.backends.exceptions import DJParseException
from datajunction_server.models.engine import Dialect
from datajunction_server.sql.parsing.ast import to_sql


@pytest_asyncio.fixture
async def parent_node(clean_session: AsyncSession, clean_current_user):
    """Create a parent source node called 'parent_node'."""
    session = clean_session
    current_user = clean_current_user
    node = Node(
        name="parent_node",
        type=NodeType.SOURCE,
        current_version="v1.0",
        created_by_id=current_user.id,
    )
    session.add(node)
    await session.flush()

    revision = NodeRevision(
        node_id=node.id,
        version="v1.0",
        name="parent_node",
        type=NodeType.SOURCE,
        created_by_id=current_user.id,
    )
    session.add(revision)
    await session.flush()
    return node


@pytest_asyncio.fixture
async def create_metric(session: AsyncSession, current_user, parent_node):
    """Fixture to create a metric node with a query."""
    created_metrics: list[NodeRevision] = []

    async def _create(query: str, name: str = None, parent=None):
        parent_to_use = parent if parent else parent_node
        metric_name = name or f"test_metric_{len(created_metrics)}"

        metric_node = Node(
            name=metric_name,
            type=NodeType.METRIC,
            current_version="v1.0",
            created_by_id=current_user.id,
        )
        session.add(metric_node)
        await session.flush()

        metric_rev = NodeRevision(
            node_id=metric_node.id,
            version="v1.0",
            name=metric_name,
            type=NodeType.METRIC,
            query=query,
            created_by_id=current_user.id,
        )
        session.add(metric_rev)
        await session.flush()

        rel = NodeRelationship(parent_id=parent_to_use.id, child_id=metric_rev.id)
        session.add(rel)
        await session.flush()

        created_metrics.append(metric_rev)
        return metric_rev

    return _create


@pytest.mark.asyncio
async def test_simple_sum(session: AsyncSession, create_metric):
    """
    Test decomposition for a metric definition that is a simple sum.
    """
    metric_rev = await create_metric("SELECT SUM(sales_amount) FROM parent_node")
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)
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
    assert_sql_equal(
        str(derived_sql),
        "SELECT SUM(sales_amount_sum_b5a3cefe) FROM parent_node",
    )


@pytest.mark.asyncio
async def test_sum_with_cast(session: AsyncSession, create_metric):
    """
    Test decomposition for a metric definition that has a sum with a cast.
    """
    metric_rev = await create_metric(
        "SELECT CAST(SUM(sales_amount) AS DOUBLE) * 100.0 FROM parent_node",
    )
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)
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
    assert_sql_equal(
        str(derived_sql),
        "SELECT CAST(SUM(sales_amount_sum_b5a3cefe) AS DOUBLE) * 100.0 FROM parent_node",
    )

    metric_rev2 = await create_metric(
        "SELECT 100.0 * SUM(sales_amount) FROM parent_node",
    )
    extractor2 = MetricComponentExtractor(metric_rev2.id)
    measures, derived_sql = await extractor2.extract(session)
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
    assert_sql_equal(
        str(derived_sql),
        "SELECT 100.0 * SUM(sales_amount_sum_b5a3cefe) FROM parent_node",
    )


@pytest.mark.asyncio
async def test_sum_with_coalesce(session: AsyncSession, create_metric):
    """
    Test decomposition for a metric definition that has a sum with coalesce.
    """
    metric_rev = await create_metric(
        "SELECT COALESCE(SUM(sales_amount), 0) FROM parent_node",
    )
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)
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
    assert_sql_equal(
        str(derived_sql),
        "SELECT COALESCE(SUM(sales_amount_sum_b5a3cefe), 0) FROM parent_node",
    )

    metric_rev2 = await create_metric(
        "SELECT SUM(COALESCE(sales_amount, 0)) FROM parent_node",
    )
    extractor2 = MetricComponentExtractor(metric_rev2.id)
    measures, derived_sql = await extractor2.extract(session)
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
    assert_sql_equal(
        str(derived_sql),
        "SELECT SUM(sales_amount_sum_65a3b528) FROM parent_node",
    )


@pytest.mark.asyncio
async def test_multiple_sums(session: AsyncSession, create_metric):
    """
    Test decomposition for a metric definition that has multiple sums.
    """
    metric_rev = await create_metric(
        "SELECT SUM(sales_amount) + SUM(fraud_sales) FROM parent_node",
    )
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)
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
    assert_sql_equal(
        str(derived_sql),
        "SELECT SUM(sales_amount_sum_b5a3cefe) + "
        "SUM(fraud_sales_sum_0e1bc4a2) FROM parent_node",
    )

    metric_rev2 = await create_metric(
        "SELECT SUM(sales_amount) - SUM(fraud_sales) FROM parent_node",
    )
    extractor2 = MetricComponentExtractor(metric_rev2.id)
    measures, derived_sql = await extractor2.extract(session)
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
    assert_sql_equal(
        str(derived_sql),
        "SELECT SUM(sales_amount_sum_b5a3cefe) - "
        "SUM(fraud_sales_sum_0e1bc4a2) FROM parent_node",
    )


@pytest.mark.asyncio
async def test_nested_functions(session: AsyncSession, create_metric):
    """
    Test behavior with deeply nested functions inside aggregations.
    """
    metric_rev = await create_metric(
        "SELECT SUM(ROUND(COALESCE(sales_amount, 0) * 1.1)) FROM parent_node",
    )
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)
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
    assert_sql_equal(
        str(derived_sql),
        "SELECT SUM(sales_amount_sum_090066cf) FROM parent_node",
    )

    metric_rev2 = await create_metric(
        "SELECT LN(SUM(COALESCE(sales_amount, 0)) + 1) FROM parent_node",
    )
    extractor2 = MetricComponentExtractor(metric_rev2.id)
    measures, derived_sql = await extractor2.extract(session)
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
    assert_sql_equal(
        str(derived_sql),
        "SELECT LN(SUM(sales_amount_sum_65a3b528) + 1) FROM parent_node",
    )


@pytest.mark.asyncio
async def test_average(session: AsyncSession, create_metric):
    """
    Test decomposition for a metric definition that uses AVG.

    AVG decomposes into two components:
    - SUM: aggregation=SUM, merge=SUM
    - COUNT: aggregation=COUNT, merge=SUM (counts roll up as sums)

    The combiner is: SUM(sum_col) / SUM(count_col)
    """
    metric_rev = await create_metric("SELECT AVG(sales_amount) FROM parent_node")
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)

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
    assert_sql_equal(
        str(derived_sql),
        "SELECT SUM(sales_amount_sum_b5a3cefe) / "
        "SUM(sales_amount_count_b5a3cefe) FROM parent_node",
    )


@pytest.mark.asyncio
async def test_rate(session: AsyncSession, create_metric):
    """
    Test decomposition for a rate metric definition.
    """
    metric_rev = await create_metric(
        "SELECT SUM(clicks) / SUM(impressions) FROM parent_node",
    )
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)
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
    assert_sql_equal(
        str(derived_sql),
        "SELECT SUM(clicks_sum_c45fd8cf) / SUM(impressions_sum_3be0a0e7) FROM parent_node",
    )

    metric_rev2 = await create_metric(
        "SELECT 1.0 * SUM(clicks) / NULLIF(SUM(impressions), 0) FROM parent_node",
    )
    extractor2 = MetricComponentExtractor(metric_rev2.id)
    measures, derived_sql = await extractor2.extract(session)
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
    assert_sql_equal(
        str(derived_sql),
        "SELECT 1.0 * SUM(clicks_sum_c45fd8cf) / "
        "NULLIF(SUM(impressions_sum_3be0a0e7), 0) FROM parent_node",
    )

    metric_rev3 = await create_metric(
        "SELECT CAST(CAST(SUM(clicks) AS INT) AS DOUBLE) / "
        "CAST(SUM(impressions) AS DOUBLE) FROM parent_node",
    )
    extractor3 = MetricComponentExtractor(metric_rev3.id)
    measures, derived_sql = await extractor3.extract(session)
    assert measures == expected_measures0
    assert_sql_equal(
        str(derived_sql),
        "SELECT CAST(CAST(SUM(clicks_sum_c45fd8cf) AS INT) AS DOUBLE) / "
        "CAST(SUM(impressions_sum_3be0a0e7) AS DOUBLE) FROM parent_node",
    )

    metric_rev4 = await create_metric(
        "SELECT COALESCE(SUM(clicks) / SUM(impressions), 0) FROM parent_node",
    )
    extractor4 = MetricComponentExtractor(metric_rev4.id)
    measures, derived_sql = await extractor4.extract(session)
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
    assert_sql_equal(
        str(derived_sql),
        "SELECT COALESCE(SUM(clicks_sum_c45fd8cf) / "
        "SUM(impressions_sum_3be0a0e7), 0) FROM parent_node",
    )

    metric_rev5 = await create_metric(
        "SELECT IF(SUM(clicks) > 0, CAST(SUM(impressions) AS DOUBLE) "
        "/ CAST(SUM(clicks) AS DOUBLE), NULL) FROM parent_node",
    )
    extractor5 = MetricComponentExtractor(metric_rev5.id)
    measures, derived_sql = await extractor5.extract(session)
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
    assert_sql_equal(
        str(derived_sql),
        "SELECT IF(SUM(clicks_sum_c45fd8cf) > 0, CAST(SUM(impressions_sum_3be0a0e7) AS DOUBLE)"
        " / CAST(SUM(clicks_sum_c45fd8cf) AS DOUBLE), NULL) FROM parent_node",
    )

    metric_rev6 = await create_metric(
        "SELECT ln(sum(clicks) + 1) / sum(views) FROM parent_node",
    )
    extractor6 = MetricComponentExtractor(metric_rev6.id)
    measures, derived_sql = await extractor6.extract(session)
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
    assert_sql_equal(
        str(derived_sql),
        "SELECT ln(SUM(clicks_sum_c45fd8cf) + 1) / SUM(views_sum_d8e39817) FROM parent_node",
    )


@pytest.mark.asyncio
async def test_max_if(session: AsyncSession, create_metric):
    """
    Test decomposition for a metric definition that uses MAX.
    """
    metric_rev = await create_metric("SELECT MAX(IF(condition, 1, 0)) FROM parent_node")
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)
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
    assert_sql_equal(
        str(derived_sql),
        "SELECT MAX(condition_max_f04b0c57) FROM parent_node",
    )


@pytest.mark.asyncio
async def test_fraction_with_if(session: AsyncSession, create_metric):
    """
    Test decomposition for a rate metric with complex numerator and denominators.
    """
    metric_rev = await create_metric(
        "SELECT IF(SUM(COALESCE(action, 0)) > 0, "
        "CAST(SUM(COALESCE(action_two, 0)) AS DOUBLE) / "
        "CAST(SUM(COALESCE(action, 0)) AS DOUBLE), NULL) FROM parent_node",
    )
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)

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
    assert_sql_equal(
        str(derived_sql),
        "SELECT IF(SUM(action_sum_c9802ccb) > 0, "
        "CAST(SUM(action_two_sum_05d921a8) AS DOUBLE) / "
        "CAST(SUM(action_sum_c9802ccb) AS DOUBLE), NULL) FROM parent_node",
    )


@pytest.mark.asyncio
async def test_count(session: AsyncSession, create_metric):
    """
    Test decomposition for a count metric.

    COUNT aggregation merges as SUM (sum up the counts).
    """
    metric_rev = await create_metric(
        "SELECT COUNT(IF(action = 1, action_event_ts, 0)) FROM parent_node",
    )
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)
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
    assert_sql_equal(
        str(derived_sql),
        "SELECT SUM(action_action_event_ts_count_7d582e65) FROM parent_node",
    )


@pytest.mark.asyncio
async def test_count_distinct_rate(session: AsyncSession, create_metric):
    """
    Test decomposition for a metric that uses count distinct.
    """
    metric_rev = await create_metric(
        "SELECT COUNT(DISTINCT user_id) / COUNT(action) FROM parent_node",
    )
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)
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
    assert_sql_equal(
        str(derived_sql),
        "SELECT COUNT( DISTINCT user_id_distinct_7f092f23) / "
        "SUM(action_count_50d753fd) FROM parent_node",
    )


@pytest.mark.asyncio
async def test_any_value(session: AsyncSession, create_metric):
    """
    Test decomposition for a metric definition that has ANY_VALUE as the agg function
    """
    metric_rev = await create_metric("SELECT ANY_VALUE(sales_amount) FROM parent_node")
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)
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
    assert_sql_equal(
        str(derived_sql),
        "SELECT ANY_VALUE(sales_amount_any_value_b5a3cefe) FROM parent_node",
    )


@pytest.mark.asyncio
async def test_no_aggregation(session: AsyncSession, create_metric):
    """
    Test behavior when there is no aggregation function in the metric query.
    """
    metric_rev = await create_metric("SELECT sales_amount FROM parent_node")
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)
    assert measures == []
    assert_sql_equal(str(derived_sql), "SELECT sales_amount FROM parent_node")


@pytest.mark.asyncio
async def test_multiple_aggregations_with_conditions(
    session: AsyncSession,
    create_metric,
):
    """
    Test behavior with conditional aggregations in the metric query.
    """
    metric_rev = await create_metric(
        "SELECT SUM(IF(region = 'US', sales_amount, 0)) + "
        "COUNT(DISTINCT IF(region = 'US', account_id, NULL)) FROM parent_node",
    )
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)
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
    assert_sql_equal(
        str(derived_sql),
        "SELECT SUM(region_sales_amount_sum_5467b14a) + "
        "COUNT(DISTINCT region_account_id_distinct_ee608f27) FROM parent_node",
    )

    metric_rev2 = await create_metric(
        "SELECT cast(coalesce(max(a), max(b), 0) as double) + "
        "cast(coalesce(max(a), max(b)) as double) FROM parent_node",
    )
    extractor2 = MetricComponentExtractor(metric_rev2.id)
    measures, derived_sql = await extractor2.extract(session)
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
    assert_sql_equal(
        str(derived_sql),
        "SELECT CAST(coalesce(MAX(a_max_0f00346b), MAX(b_max_6d64a2e5), 0) AS DOUBLE) + "
        "CAST(coalesce(MAX(a_max_0f00346b), MAX(b_max_6d64a2e5)) AS DOUBLE) FROM parent_node",
    )


@pytest.mark.asyncio
async def test_min_agg(session: AsyncSession, create_metric):
    metric_rev = await create_metric("SELECT MIN(a) FROM parent")
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)
    assert measures == [
        MetricComponent(
            name="a_min_3cf406a5",
            expression="a",
            aggregation="MIN",
            merge="MIN",
            rule=AggregationRule(type=Aggregability.FULL, level=None),
        ),
    ]
    assert_sql_equal(str(derived_sql), "SELECT MIN(a_min_3cf406a5) FROM parent")


@pytest.mark.asyncio
async def test_empty_query(session: AsyncSession, create_metric):
    """
    Test behavior when the metric query is empty.
    """
    metric_rev = await create_metric("")
    extractor = MetricComponentExtractor(metric_rev.id)
    with pytest.raises(DJParseException, match="Empty query provided!"):
        await extractor.extract(session)


@pytest.mark.asyncio
async def test_unsupported_aggregation_function(session: AsyncSession, create_metric):
    """
    Test behavior when the query contains unsupported aggregation functions. We just return an
    empty list of measures in this case, because there are no pre-aggregatable measures.
    """
    metric_rev = await create_metric("SELECT MEDIAN(sales_amount) FROM parent_node")
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)
    assert measures == []
    assert_sql_equal(str(derived_sql), "SELECT MEDIAN(sales_amount) FROM parent_node")

    metric_rev2 = await create_metric(
        "SELECT approx_percentile(duration_ms, 1.0, 0.9) / 1000 FROM parent_node",
    )
    extractor2 = MetricComponentExtractor(metric_rev2.id)
    measures, derived_sql = await extractor2.extract(session)
    assert measures == []
    assert_sql_equal(
        str(derived_sql),
        "SELECT approx_percentile(duration_ms, 1.0, 0.9) / 1000 FROM parent_node",
    )


@pytest.mark.asyncio
async def test_count_if(session: AsyncSession, create_metric):
    """
    Test decomposition for count_if.
    """
    metric_rev = await create_metric(
        "SELECT CAST(COUNT_IF(ARRAY_CONTAINS(field_a, 'xyz')) AS FLOAT) / COUNT(*) "
        "FROM parent_node",
    )
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)
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
    assert_sql_equal(
        str(derived_sql),
        "SELECT  CAST(SUM(field_a_count_if_3979ffbd) AS FLOAT) / SUM(count_58ac32c5) "
        "FROM parent_node",
    )


@pytest.mark.asyncio
async def test_metric_query_with_aliases(session: AsyncSession, create_metric):
    """
    Test behavior when the query contains unsupported aggregation functions. We just return an
    empty list of measures in this case, because there are no pre-aggregatable measures.
    """
    metric_rev = await create_metric(
        "SELECT avg(cast(repair_orders_fact.time_to_dispatch as int)) "
        "FROM default.repair_orders_fact repair_orders_fact",
    )
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)
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
    assert_sql_equal(
        str(derived_sql),
        "SELECT SUM(time_to_dispatch_sum_3bc9baed) / "
        "SUM(time_to_dispatch_count_3bc9baed) FROM default.repair_orders_fact",
    )


@pytest.mark.asyncio
async def test_max_by(session: AsyncSession, create_metric):
    """
    Test decomposition for a metric that uses MAX_BY.
    """
    metric_rev = await create_metric(
        "SELECT MAX_BY(IF(condition, 1, 0), dimension) FROM parent_node",
    )
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)
    assert measures == []
    assert_sql_equal(
        str(derived_sql),
        "SELECT MAX_BY(IF(condition, 1, 0), dimension) FROM parent_node",
    )


@pytest.mark.asyncio
async def test_min_by(session: AsyncSession, create_metric):
    """
    Test decomposition for a metric that uses MIN_BY.
    """
    metric_rev = await create_metric(
        "SELECT MIN_BY(IF(condition, 1, 0), dimension) FROM parent_node",
    )
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)
    assert measures == []
    assert_sql_equal(
        str(derived_sql),
        "SELECT MIN_BY(IF(condition, 1, 0), dimension) FROM parent_node",
    )


@pytest.mark.asyncio
async def test_approx_count_distinct(session: AsyncSession, create_metric):
    """
    Test decomposition for an approximate count distinct metric.

    APPROX_COUNT_DISTINCT decomposes to HLL sketch operations using Spark functions:
    - aggregation: hll_sketch_agg (Spark's function for building HLL sketch)
    - merge: hll_union (Spark's function for combining sketches)
    - The combiner uses hll_sketch_estimate(hll_union(...)) to get the final count

    Translation to other dialects (Druid, Trino) happens in the transpilation layer.
    """
    metric_rev = await create_metric(
        "SELECT APPROX_COUNT_DISTINCT(user_id) FROM parent_node",
    )
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)
    expected_measures = [
        MetricComponent(
            name="user_id_hll_7f092f23",
            expression="user_id",
            aggregation="hll_sketch_agg",  # Spark's HLL accumulate
            merge="hll_union_agg",  # Spark's HLL merge
            rule=AggregationRule(type=Aggregability.FULL, level=None),
        ),
    ]
    assert measures == expected_measures
    # Verify the derived SQL uses Spark HLL functions
    assert_sql_equal(
        str(derived_sql),
        "SELECT hll_sketch_estimate(hll_union_agg(user_id_hll_7f092f23)) FROM parent_node",
    )


@pytest.mark.asyncio
async def test_approx_count_distinct_with_expression(
    session: AsyncSession,
    create_metric,
):
    """
    Test decomposition for APPROX_COUNT_DISTINCT with a complex expression.
    """
    metric_rev = await create_metric(
        "SELECT APPROX_COUNT_DISTINCT(COALESCE(user_id, 'unknown')) FROM parent_node",
    )
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)

    # Verify structure of the measure (hash value may vary)
    assert len(measures) == 1
    measure = measures[0]
    assert measure.expression == "COALESCE(user_id, 'unknown')"
    assert measure.aggregation == "hll_sketch_agg"
    assert measure.merge == "hll_union_agg"
    assert measure.rule.type == Aggregability.FULL

    # Verify derived SQL contains the HLL functions
    derived_str = str(derived_sql)
    assert "hll_sketch_estimate" in derived_str
    assert "hll_union_agg" in derived_str
    assert "_hll_" in derived_str


@pytest.mark.asyncio
async def test_approx_count_distinct_with_conditional(
    session: AsyncSession,
    create_metric,
):
    """
    Test decomposition for APPROX_COUNT_DISTINCT with IF condition.
    """
    metric_rev = await create_metric(
        "SELECT APPROX_COUNT_DISTINCT(IF(active = 1, user_id, NULL)) FROM parent_node",
    )
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)

    # Verify structure of the measure
    assert len(measures) == 1
    measure = measures[0]
    assert measure.expression == "IF(active = 1, user_id, NULL)"
    assert measure.aggregation == "hll_sketch_agg"
    assert measure.merge == "hll_union_agg"
    assert measure.rule.type == Aggregability.FULL

    # Verify derived SQL contains the HLL functions
    derived_str = str(derived_sql)
    assert "hll_sketch_estimate" in derived_str
    assert "hll_union_agg" in derived_str


@pytest.mark.asyncio
async def test_approx_count_distinct_combined_with_sum(
    session: AsyncSession,
    create_metric,
):
    """
    Test decomposition for a metric combining APPROX_COUNT_DISTINCT with SUM.
    """
    metric_rev = await create_metric(
        "SELECT SUM(revenue) / APPROX_COUNT_DISTINCT(user_id) FROM parent_node",
    )
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)

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
    assert hll_measures[0].merge == "hll_union_agg"

    # Verify derived SQL has both
    derived_str = str(derived_sql)
    assert "SUM(" in derived_str
    assert "hll_sketch_estimate" in derived_str
    assert "hll_union_agg" in derived_str


@pytest.mark.asyncio
async def test_approx_count_distinct_multiple(session: AsyncSession, create_metric):
    """
    Test decomposition with multiple APPROX_COUNT_DISTINCT on different columns.
    """
    metric_rev = await create_metric(
        "SELECT APPROX_COUNT_DISTINCT(user_id) + APPROX_COUNT_DISTINCT(session_id) "
        "FROM parent_node",
    )
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)

    # Should have two HLL measures
    assert len(measures) == 2
    assert all(m.aggregation == "hll_sketch_agg" for m in measures)
    assert all(m.merge == "hll_union_agg" for m in measures)

    # Verify both expressions are present
    expressions = {m.expression for m in measures}
    assert expressions == {"user_id", "session_id"}

    # Verify derived SQL has both HLL estimate calls
    derived_str = str(derived_sql)
    assert derived_str.count("hll_sketch_estimate") == 2
    assert derived_str.count("hll_union_agg") == 2


@pytest.mark.asyncio
async def test_approx_count_distinct_rate(session: AsyncSession, create_metric):
    """
    Test decomposition for a rate metric using APPROX_COUNT_DISTINCT.

    Example: unique users who clicked / unique users who viewed
    """
    metric_rev = await create_metric(
        "SELECT CAST(APPROX_COUNT_DISTINCT(IF(clicked = 1, user_id, NULL)) AS DOUBLE) / "
        "CAST(APPROX_COUNT_DISTINCT(user_id) AS DOUBLE) FROM parent_node",
    )
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)

    # Should have two HLL measures
    assert len(measures) == 2
    assert all(m.aggregation == "hll_sketch_agg" for m in measures)
    assert all(m.merge == "hll_union_agg" for m in measures)

    # Verify expressions
    expressions = {m.expression for m in measures}
    assert "user_id" in expressions
    assert "IF(clicked = 1, user_id, NULL)" in expressions

    # Verify derived SQL structure
    derived_str = str(derived_sql)
    assert_sql_equal(
        derived_str,
        "SELECT  CAST(hll_sketch_estimate(hll_union_agg(clicked_user_id_hll_f3824813)) AS DOUBLE) / CAST(hll_sketch_estimate(hll_union_agg(user_id_hll_7f092f23)) AS DOUBLE) FROM parent_node",
    )


@pytest.mark.asyncio
async def test_approx_count_distinct_dialect_translation(
    session: AsyncSession,
    create_metric,
):
    """
    Test that the decomposed HLL SQL can be translated to different dialects.

    This is an integration test showing the full flow:
    1. Decompose APPROX_COUNT_DISTINCT -> Spark HLL functions
    2. Translate Spark HLL functions -> target dialect
    """
    metric_rev = await create_metric(
        "SELECT APPROX_COUNT_DISTINCT(user_id) FROM parent_node",
    )
    extractor = MetricComponentExtractor(metric_rev.id)
    _, derived_sql = await extractor.extract(session)
    spark_sql = str(derived_sql)

    # The decomposed SQL uses Spark HLL functions
    assert_sql_equal(
        spark_sql,
        "SELECT hll_sketch_estimate(hll_union_agg(user_id_hll_7f092f23)) FROM parent_node",
    )

    # Translate to Druid
    druid_sql = to_sql(derived_sql, Dialect.DRUID)
    assert_sql_equal(
        druid_sql,
        "SELECT hll_sketch_estimate(ds_hll(user_id_hll_7f092f23)) FROM parent_node",
    )

    # Translate to Trino
    trino_sql = to_sql(derived_sql, Dialect.TRINO)
    assert_sql_equal(
        trino_sql,
        "SELECT cardinality(merge(user_id_hll_7f092f23)) FROM parent_node",
    )

    # Spark to Spark is identity
    spark_spark_sql = to_sql(derived_sql, Dialect.SPARK)
    assert spark_spark_sql == spark_sql


@pytest.mark.asyncio
async def test_approx_count_distinct_combined_metrics_dialect_translation(
    session: AsyncSession,
    create_metric,
):
    """
    Test dialect translation for a complex metric combining HLL with other aggregations.
    """
    metric_rev = await create_metric(
        "SELECT SUM(revenue) / APPROX_COUNT_DISTINCT(user_id) AS revenue_per_user "
        "FROM parent_node",
    )
    extractor = MetricComponentExtractor(metric_rev.id)
    _, derived_sql = await extractor.extract(session)
    spark_sql = str(derived_sql)

    # Verify Spark SQL structure - contains both SUM and HLL
    assert_sql_equal(
        spark_sql,
        "SELECT SUM(revenue_sum_60e4d31f) / hll_sketch_estimate(hll_union_agg(user_id_hll_7f092f23)) AS revenue_per_user FROM parent_node",
    )

    # Translate to Druid - should preserve SUM but translate HLL
    druid_sql = to_sql(derived_sql, Dialect.DRUID)
    assert_sql_equal(
        druid_sql,
        "SELECT SAFE_DIVIDE(SUM(revenue_sum_60e4d31f), hll_sketch_estimate(ds_hll(user_id_hll_7f092f23))) AS revenue_per_user FROM parent_node",
    )

    # Translate to Trino
    trino_sql = to_sql(derived_sql, Dialect.TRINO)
    assert_sql_equal(
        trino_sql,
        "SELECT SUM(revenue_sum_60e4d31f) / cardinality(merge(user_id_hll_7f092f23)) AS revenue_per_user FROM parent_node",
    )


@pytest.mark.asyncio
async def test_var_pop(session: AsyncSession, create_metric):
    """
    Test decomposition for a population variance metric.

    VAR_POP decomposes into three components:
    - SUM(x): to compute mean
    - SUM(POWER(x, 2)): sum of squares (uses template syntax)
    - COUNT(x): for normalization

    Formula: E[X²] - E[X]² = (sum_sq/n) - (sum/n)²
    """
    metric_rev = await create_metric("SELECT VAR_POP(price) FROM parent_node")
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)

    # Verify we have exactly 3 components
    assert len(measures) == 3

    # Build a lookup by aggregation type for order-independent assertions
    by_agg = {m.aggregation: m for m in measures}

    # Check SUM component
    assert "SUM" in by_agg
    assert by_agg["SUM"].expression == "price"
    assert by_agg["SUM"].merge == "SUM"
    assert "_sum_" in by_agg["SUM"].name

    # Check SUM(POWER(price, 2)) component - expanded template for sum of squares
    assert "SUM(POWER(price, 2))" in by_agg
    assert by_agg["SUM(POWER(price, 2))"].expression == "price"
    assert by_agg["SUM(POWER(price, 2))"].merge == "SUM"
    assert "_sum_sq_" in by_agg["SUM(POWER(price, 2))"].name

    # Check COUNT component
    assert "COUNT" in by_agg
    assert by_agg["COUNT"].expression == "price"
    assert by_agg["COUNT"].merge == "SUM"
    assert "_count_" in by_agg["COUNT"].name

    # The derived SQL should reference all components
    derived_str = str(derived_sql)
    assert_sql_equal(
        derived_str,
        """
      SELECT
        SUM(price_sum_sq_726db899) / SUM(price_count_726db899) -
        POWER(SUM(price_sum_726db899) / SUM(price_count_726db899), 2)
      FROM parent_node""",
    )


@pytest.mark.asyncio
async def test_var_samp(session: AsyncSession, create_metric):
    """
    Test decomposition for a sample variance metric.

    VAR_SAMP uses n-1 in the denominator (Bessel's correction).
    """
    metric_rev = await create_metric("SELECT VAR_SAMP(price) FROM parent_node")
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)

    # Same components as VAR_POP
    assert len(measures) == 3
    agg_types = {m.aggregation for m in measures}
    assert agg_types == {"SUM", "SUM(POWER(price, 2))", "COUNT"}
    assert_sql_equal(
        str(derived_sql),
        """
      SELECT
        SUM(price_count_726db899) * SUM(price_sum_sq_726db899) - POWER(SUM(price_sum_726db899), 2) / SUM(price_count_726db899) * SUM(price_count_726db899) - 1
      FROM parent_node""",
    )


@pytest.mark.asyncio
async def test_stddev_pop(session: AsyncSession, create_metric):
    """
    Test decomposition for population standard deviation.

    STDDEV_POP = SQRT(VAR_POP), so it uses the same components as variance.
    """
    metric_rev = await create_metric("SELECT STDDEV_POP(price) FROM parent_node")
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)

    # Same components as VAR_POP
    assert len(measures) == 3
    agg_types = {m.aggregation for m in measures}
    assert agg_types == {"SUM", "SUM(POWER(price, 2))", "COUNT"}

    # Derived SQL should include SQRT for standard deviation
    derived_str = str(derived_sql)
    assert_sql_equal(
        derived_str,
        """SELECT
        SQRT(
          SUM(price_sum_sq_726db899) / SUM(price_count_726db899) -
          POWER(SUM(price_sum_726db899) / SUM(price_count_726db899), 2)
        )
      FROM parent_node""",
    )


@pytest.mark.asyncio
async def test_stddev_samp(session: AsyncSession, create_metric):
    """
    Test decomposition for sample standard deviation.

    STDDEV_SAMP = SQRT(VAR_SAMP).
    """
    metric_rev = await create_metric("SELECT STDDEV_SAMP(price) FROM parent_node")
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)

    # Same components as VAR_SAMP
    assert len(measures) == 3
    agg_types = {m.aggregation for m in measures}
    assert agg_types == {"SUM", "SUM(POWER(price, 2))", "COUNT"}

    # Derived SQL should include SQRT
    derived_str = str(derived_sql)
    assert_sql_equal(
        derived_str,
        """
      SELECT
        SQRT(
          SUM(price_count_726db899) * SUM(price_sum_sq_726db899) -
          POWER(SUM(price_sum_726db899), 2) /
          SUM(price_count_726db899) * SUM(price_count_726db899) - 1
        )
      FROM parent_node""",
    )


@pytest.mark.asyncio
async def test_covar_pop(session: AsyncSession, create_metric):
    """Test COVAR_POP decomposition - population covariance."""
    metric_rev = await create_metric(
        "SELECT COVAR_POP(price, quantity) FROM parent_node",
    )
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)

    # Should have 4 components: sum_x, sum_y, sum_xy, count
    assert len(measures) == 4
    agg_types = {m.aggregation for m in measures}
    assert agg_types == {
        "SUM(price)",
        "SUM(quantity)",
        "SUM(price * quantity)",
        "COUNT(price)",
    }

    # All components should merge via SUM
    for m in measures:
        assert m.merge == "SUM"

    # Check component naming
    by_agg = {m.aggregation: m for m in measures}
    assert "_sum_x_" in by_agg["SUM(price)"].name
    assert "_sum_y_" in by_agg["SUM(quantity)"].name
    assert "_sum_xy_" in by_agg["SUM(price * quantity)"].name
    assert "_count_" in by_agg["COUNT(price)"].name


@pytest.mark.asyncio
async def test_covar_samp(session: AsyncSession, create_metric):
    """Test COVAR_SAMP decomposition - sample covariance with Bessel's correction."""
    metric_rev = await create_metric("SELECT COVAR_SAMP(revenue, cost) FROM sales")
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)

    # Same 4 components as COVAR_POP
    assert len(measures) == 4
    agg_types = {m.aggregation for m in measures}
    assert agg_types == {
        "SUM(revenue)",
        "SUM(cost)",
        "SUM(revenue * cost)",
        "COUNT(revenue)",
    }


@pytest.mark.asyncio
async def test_corr(session: AsyncSession, create_metric):
    """Test CORR decomposition - Pearson correlation coefficient."""
    metric_rev = await create_metric("SELECT CORR(x, y) FROM data")
    extractor = MetricComponentExtractor(metric_rev.id)
    measures, derived_sql = await extractor.extract(session)

    # Should have 6 components: sum_x, sum_y, sum_x_sq, sum_y_sq, sum_xy, count
    assert len(measures) == 6
    agg_types = {m.aggregation for m in measures}
    assert agg_types == {
        "SUM(x)",
        "SUM(y)",
        "SUM(POWER(x, 2))",
        "SUM(POWER(y, 2))",
        "SUM(x * y)",
        "COUNT(x)",
    }

    # All components should merge via SUM
    for m in measures:
        assert m.merge == "SUM"

    # Check component naming includes both columns where appropriate
    by_agg = {m.aggregation: m for m in measures}
    assert "_sum_x_" in by_agg["SUM(x)"].name
    assert "_sum_y_" in by_agg["SUM(y)"].name
    assert "_sum_x_sq_" in by_agg["SUM(POWER(x, 2))"].name
    assert "_sum_y_sq_" in by_agg["SUM(POWER(y, 2))"].name
    assert "_sum_xy_" in by_agg["SUM(x * y)"].name
    assert "_count_" in by_agg["COUNT(x)"].name


# =============================================================================
# Tests for extract_from_base_metrics (derived metrics)
# =============================================================================


@pytest_asyncio.fixture
async def create_base_metric(
    clean_session: AsyncSession,
    clean_current_user,
    parent_node,
):
    """Fixture to create a base metric node with a query (has non-metric parent)."""
    session = clean_session
    current_user = clean_current_user
    created_metrics = {}

    async def _create(name: str, query: str):
        metric_node = Node(
            name=name,
            type=NodeType.METRIC,
            current_version="v1.0",
            created_by_id=current_user.id,
        )
        session.add(metric_node)
        await session.flush()

        metric_rev = NodeRevision(
            node_id=metric_node.id,
            version="v1.0",
            name=name,
            type=NodeType.METRIC,
            query=query,
            created_by_id=current_user.id,
        )
        session.add(metric_rev)
        await session.flush()

        # Base metric has source node as parent (not another metric)
        rel = NodeRelationship(parent_id=parent_node.id, child_id=metric_rev.id)
        session.add(rel)
        await session.flush()

        created_metrics[name] = (metric_node, metric_rev)
        return metric_node, metric_rev

    return _create


@pytest_asyncio.fixture
async def create_derived_metric(clean_session: AsyncSession, clean_current_user):
    """Fixture to create a derived metric that references base metrics."""
    session = clean_session
    current_user = clean_current_user

    async def _create(name: str, query: str, base_metric_nodes: list[Node]):
        metric_node = Node(
            name=name,
            type=NodeType.METRIC,
            current_version="v1.0",
            created_by_id=current_user.id,
        )
        session.add(metric_node)
        await session.flush()

        metric_rev = NodeRevision(
            node_id=metric_node.id,
            version="v1.0",
            name=name,
            type=NodeType.METRIC,
            query=query,
            created_by_id=current_user.id,
        )
        session.add(metric_rev)
        await session.flush()

        # Derived metric has base metrics as parents
        for base_node in base_metric_nodes:
            rel = NodeRelationship(parent_id=base_node.id, child_id=metric_rev.id)
            session.add(rel)

        await session.flush()
        return metric_node, metric_rev

    return _create


@pytest.mark.asyncio
async def test_extract_derived_metric_revenue_per_order(
    clean_session: AsyncSession,
    create_base_metric,
    create_derived_metric,
):
    """
    Test derived metric: revenue_per_order = revenue / orders

    This tests the "same parent" pattern where both base metrics come from the
    same fact table (orders_source). The derived metric references both by name.
    """
    session = clean_session
    # Create base metrics (both from same "orders" fact)
    revenue_node, _ = await create_base_metric(
        "default.revenue",
        "SELECT SUM(amount) FROM default.orders_source",
    )
    orders_node, _ = await create_base_metric(
        "default.orders",
        "SELECT COUNT(*) FROM default.orders_source",
    )

    # Create derived metric: revenue per order with NULLIF for divide-by-zero protection
    _, derived_rev = await create_derived_metric(
        "default.revenue_per_order",
        "SELECT default.revenue / NULLIF(default.orders, 0)",
        [revenue_node, orders_node],
    )

    extractor = MetricComponentExtractor(derived_rev.id)
    components, derived_ast = await extractor.extract(session)
    derived_sql = str(derived_ast)

    # Should collect components from both base metrics:
    # - revenue: SUM(amount) -> amount_sum component
    # - orders: COUNT(*) -> count component
    assert len(components) == 2

    # Verify component types
    agg_types = {c.aggregation for c in components}
    assert "SUM" in agg_types
    assert "COUNT" in agg_types

    # Derived SQL should have metric references substituted
    assert "default.revenue" not in derived_sql
    assert "default.orders" not in derived_sql
    # Should contain the combiner expressions
    assert "SUM(" in derived_sql
    assert "NULLIF(" in derived_sql


@pytest.mark.asyncio
async def test_extract_derived_metric_cross_fact_ratio(
    clean_session: AsyncSession,
    create_base_metric,
    create_derived_metric,
):
    """
    Test derived metric: revenue_per_page_view = revenue / page_views

    This tests the "cross-fact" pattern where base metrics come from different
    fact tables (orders_source and events_source) that share dimensions.
    """
    session = clean_session
    # Create base metrics from different facts
    revenue_node, _ = await create_base_metric(
        "default.revenue",
        "SELECT SUM(amount) FROM default.orders_source",
    )
    page_views_node, _ = await create_base_metric(
        "default.page_views",
        "SELECT SUM(page_views) FROM default.events_source",
    )

    # Create cross-fact derived metric
    _, derived_rev = await create_derived_metric(
        "default.revenue_per_page_view",
        "SELECT default.revenue / NULLIF(default.page_views, 0)",
        [revenue_node, page_views_node],
    )

    extractor = MetricComponentExtractor(derived_rev.id)
    components, derived_ast = await extractor.extract(session)
    derived_sql = str(derived_ast)

    # Should collect SUM components from both base metrics
    assert len(components) == 2
    assert all(c.aggregation == "SUM" for c in components)

    # Verify expressions reference different columns
    expressions = {c.expression for c in components}
    assert "amount" in expressions
    assert "page_views" in expressions

    # Derived SQL should have metric references substituted
    assert "default.revenue" not in derived_sql
    assert "default.page_views" not in derived_sql


@pytest.mark.asyncio
async def test_extract_derived_metric_shared_components(
    clean_session: AsyncSession,
    create_base_metric,
    create_derived_metric,
):
    """
    Test component deduplication when base metrics share the same aggregation.

    When two base metrics have identical aggregations (same expression + function),
    they produce the same component hash and should be deduplicated.
    """
    session = clean_session
    # Two base metrics that both aggregate "amount" with SUM
    # They'll produce the same component: amount_sum_<hash>
    gross_revenue_node, _ = await create_base_metric(
        "default.gross_revenue",
        "SELECT SUM(amount) FROM default.orders_source",
    )
    net_revenue_node, _ = await create_base_metric(
        "default.net_revenue",
        "SELECT SUM(amount) - SUM(discount) FROM default.orders_source",
    )

    # Derived metric that uses both
    _, derived_rev = await create_derived_metric(
        "default.revenue_ratio",
        "SELECT default.gross_revenue / NULLIF(default.net_revenue, 0)",
        [gross_revenue_node, net_revenue_node],
    )

    extractor = MetricComponentExtractor(derived_rev.id)
    components, _ = await extractor.extract(session)

    # Should have 2 unique components:
    # - amount_sum (shared between both metrics - deduplicated)
    # - discount_sum (only in net_revenue)
    assert len(components) == 2

    # Verify the amount component appears only once
    amount_components = [c for c in components if c.expression == "amount"]
    assert len(amount_components) == 1


# =============================================================================
# Tests for Nested Derived Metrics (derived metrics referencing derived metrics)
# =============================================================================


@pytest.mark.asyncio
async def test_extract_nested_derived_metric_two_levels(
    clean_session: AsyncSession,
    create_base_metric,
    create_derived_metric,
):
    """
    Test nested derived metric: growth_index = derived_metric / constant

    Structure:
    - base_metric: SUM(amount) -> revenue
    - derived_metric_1: revenue * 1.1 (references base_metric)
    - nested_derived: derived_metric_1 / 100 (references derived_metric_1)

    The nested derived should decompose all the way to the base components.
    """
    session = clean_session
    # Level 0: Base metric
    revenue_node, _ = await create_base_metric(
        "default.revenue",
        "SELECT SUM(amount) FROM default.orders_source",
    )

    # Level 1: Derived metric referencing base metric
    adjusted_revenue_node, _ = await create_derived_metric(
        "default.adjusted_revenue",
        "SELECT default.revenue * 1.1",
        [revenue_node],
    )

    # Level 2: Nested derived metric referencing level 1 derived metric
    _, growth_index_rev = await create_derived_metric(
        "default.growth_index",
        "SELECT default.adjusted_revenue / 100",
        [adjusted_revenue_node],
    )

    extractor = MetricComponentExtractor(growth_index_rev.id)
    components, derived_ast = await extractor.extract(session)
    derived_sql = str(derived_ast)

    # Should decompose all the way to base components
    assert len(components) == 1
    assert components[0].aggregation == "SUM"
    assert components[0].expression == "amount"

    # Derived SQL should NOT contain any intermediate metric references
    assert "default.revenue" not in derived_sql
    assert "default.adjusted_revenue" not in derived_sql

    # Should contain the fully expanded combiner expression
    assert "SUM(" in derived_sql
    assert "1.1" in derived_sql
    assert "100" in derived_sql


@pytest.mark.asyncio
async def test_extract_nested_derived_metric_three_levels(
    clean_session: AsyncSession,
    create_base_metric,
    create_derived_metric,
):
    """
    Test 3-level nested derived metric decomposition.

    Structure:
    - L0: total_orders = COUNT(*) FROM orders
    - L1: orders_per_day = total_orders / 30
    - L2: weekly_avg = orders_per_day * 7
    - L3: monthly_projection = weekly_avg * 4.3

    Should decompose to the COUNT(*) component.
    """
    session = clean_session
    # Level 0
    total_orders_node, _ = await create_base_metric(
        "default.total_orders",
        "SELECT COUNT(*) FROM default.orders_source",
    )

    # Level 1
    orders_per_day_node, _ = await create_derived_metric(
        "default.orders_per_day",
        "SELECT default.total_orders / 30",
        [total_orders_node],
    )

    # Level 2
    weekly_avg_node, _ = await create_derived_metric(
        "default.weekly_avg",
        "SELECT default.orders_per_day * 7",
        [orders_per_day_node],
    )

    # Level 3
    _, monthly_projection_rev = await create_derived_metric(
        "default.monthly_projection",
        "SELECT default.weekly_avg * 4.3",
        [weekly_avg_node],
    )

    extractor = MetricComponentExtractor(monthly_projection_rev.id)
    components, derived_ast = await extractor.extract(session)
    derived_sql = str(derived_ast)

    # Should decompose all the way to COUNT(*) component
    assert len(components) == 1
    assert components[0].aggregation == "COUNT"
    assert components[0].merge == "SUM"  # COUNT merges as SUM

    # Should not contain intermediate metric references
    assert "default.total_orders" not in derived_sql
    assert "default.orders_per_day" not in derived_sql
    assert "default.weekly_avg" not in derived_sql


@pytest.mark.asyncio
async def test_extract_nested_derived_metric_multiple_base_metrics(
    clean_session: AsyncSession,
    create_base_metric,
    create_derived_metric,
):
    """
    Test nested derived metric that ultimately depends on multiple base metrics.

    Structure:
    - L0: revenue = SUM(amount)
    - L0: order_count = COUNT(*)
    - L1: aov = revenue / order_count (derived from 2 base metrics)
    - L2: aov_index = aov * 100 (nested derived)

    Should decompose to both base metric components.
    """
    session = clean_session
    # Level 0: Two base metrics
    revenue_node, _ = await create_base_metric(
        "default.revenue",
        "SELECT SUM(amount) FROM default.orders_source",
    )
    order_count_node, _ = await create_base_metric(
        "default.order_count",
        "SELECT COUNT(*) FROM default.orders_source",
    )

    # Level 1: Derived metric from both base metrics
    aov_node, _ = await create_derived_metric(
        "default.aov",
        "SELECT default.revenue / NULLIF(default.order_count, 0)",
        [revenue_node, order_count_node],
    )

    # Level 2: Nested derived
    _, aov_index_rev = await create_derived_metric(
        "default.aov_index",
        "SELECT default.aov * 100",
        [aov_node],
    )

    extractor = MetricComponentExtractor(aov_index_rev.id)
    components, derived_ast = await extractor.extract(session)
    derived_sql = str(derived_ast)

    # Should have components from both base metrics
    assert len(components) == 2
    agg_types = {c.aggregation for c in components}
    assert agg_types == {"SUM", "COUNT"}

    # Should not contain any metric references
    assert "default.revenue" not in derived_sql
    assert "default.order_count" not in derived_sql
    assert "default.aov" not in derived_sql


@pytest.mark.asyncio
async def test_extract_nested_derived_circular_reference_detection(
    clean_session: AsyncSession,
    clean_current_user,
):
    """
    Test that circular references in nested derived metrics are detected.

    Structure:
    - metric_a references metric_b
    - metric_b references metric_a (circular!)

    Should raise ValueError with cycle detection message.
    """
    session = clean_session
    current_user = clean_current_user

    # Create two metric nodes that will reference each other
    metric_a_node = Node(
        name="default.metric_a",
        type=NodeType.METRIC,
        current_version="v1.0",
        created_by_id=current_user.id,
    )
    session.add(metric_a_node)
    await session.flush()

    metric_b_node = Node(
        name="default.metric_b",
        type=NodeType.METRIC,
        current_version="v1.0",
        created_by_id=current_user.id,
    )
    session.add(metric_b_node)
    await session.flush()

    # Create revisions
    metric_a_rev = NodeRevision(
        node_id=metric_a_node.id,
        version="v1.0",
        name="default.metric_a",
        type=NodeType.METRIC,
        query="SELECT default.metric_b * 2",
        created_by_id=current_user.id,
    )
    session.add(metric_a_rev)
    await session.flush()

    metric_b_rev = NodeRevision(
        node_id=metric_b_node.id,
        version="v1.0",
        name="default.metric_b",
        type=NodeType.METRIC,
        query="SELECT default.metric_a / 2",
        created_by_id=current_user.id,
    )
    session.add(metric_b_rev)
    await session.flush()

    # Create circular relationships
    rel_a = NodeRelationship(parent_id=metric_b_node.id, child_id=metric_a_rev.id)
    rel_b = NodeRelationship(parent_id=metric_a_node.id, child_id=metric_b_rev.id)
    session.add(rel_a)
    session.add(rel_b)
    await session.flush()

    # Build cache for cache-based extraction
    nodes_cache = {
        "default.metric_a": metric_a_node,
        "default.metric_b": metric_b_node,
    }
    # Need to set current revision on nodes for cache-based extraction
    metric_a_node.current = metric_a_rev
    metric_b_node.current = metric_b_rev

    parent_map = {
        "default.metric_a": ["default.metric_b"],
        "default.metric_b": ["default.metric_a"],
    }

    extractor = MetricComponentExtractor(metric_a_rev.id)

    # Should detect the circular reference
    with pytest.raises(ValueError, match="Circular metric reference detected"):
        await extractor.extract(
            session,
            nodes_cache=nodes_cache,
            parent_map=parent_map,
            metric_node=metric_a_node,
        )


@pytest.mark.asyncio
async def test_extract_with_cache_basic(
    clean_session: AsyncSession,
    create_base_metric,
):
    """
    Test extraction using the cache-based path (nodes_cache, parent_map).

    This is the path used by build_v3 for efficiency.
    """
    session = clean_session
    # Create a base metric
    revenue_node, revenue_rev = await create_base_metric(
        "default.revenue",
        "SELECT SUM(amount) FROM default.orders_source",
    )

    # Set current revision on node
    revenue_node.current = revenue_rev

    # Build cache
    nodes_cache = {"default.revenue": revenue_node}
    parent_map: dict[str, list[str]] = {
        "default.revenue": [],
    }  # No metric parents (it's a base metric)

    extractor = MetricComponentExtractor(revenue_rev.id)
    components, derived_ast = await extractor.extract(
        session,
        nodes_cache=nodes_cache,
        parent_map=parent_map,
        metric_node=revenue_node,
    )

    # Should work the same as non-cache path
    assert len(components) == 1
    assert components[0].aggregation == "SUM"
    assert components[0].expression == "amount"


@pytest.mark.asyncio
async def test_extract_with_cache_derived_metric(
    clean_session: AsyncSession,
    create_base_metric,
    create_derived_metric,
):
    """
    Test cache-based extraction for derived metrics.
    """
    session = clean_session
    # Create base metrics
    revenue_node, revenue_rev = await create_base_metric(
        "default.revenue",
        "SELECT SUM(amount) FROM default.orders_source",
    )
    orders_node, orders_rev = await create_base_metric(
        "default.orders",
        "SELECT COUNT(*) FROM default.orders_source",
    )

    # Create derived metric
    aov_node, aov_rev = await create_derived_metric(
        "default.aov",
        "SELECT default.revenue / NULLIF(default.orders, 0)",
        [revenue_node, orders_node],
    )

    # Set current revisions
    revenue_node.current = revenue_rev
    orders_node.current = orders_rev
    aov_node.current = aov_rev

    # Build cache
    nodes_cache = {
        "default.revenue": revenue_node,
        "default.orders": orders_node,
        "default.aov": aov_node,
    }
    parent_map: dict[str, list[str]] = {
        "default.revenue": [],  # Base metric
        "default.orders": [],  # Base metric
        "default.aov": ["default.revenue", "default.orders"],  # Derived
    }

    extractor = MetricComponentExtractor(aov_rev.id)
    components, derived_ast = await extractor.extract(
        session,
        nodes_cache=nodes_cache,
        parent_map=parent_map,
        metric_node=aov_node,
    )

    # Should have components from both base metrics
    assert len(components) == 2
    agg_types = {c.aggregation for c in components}
    assert agg_types == {"SUM", "COUNT"}


@pytest.mark.asyncio
async def test_extract_with_cache_nested_derived_metric(
    clean_session: AsyncSession,
    create_base_metric,
    create_derived_metric,
):
    """
    Test cache-based extraction for nested derived metrics (the full path used by build_v3).
    """
    session = clean_session
    # L0: Base metrics
    revenue_node, revenue_rev = await create_base_metric(
        "default.revenue",
        "SELECT SUM(amount) FROM default.orders_source",
    )
    orders_node, orders_rev = await create_base_metric(
        "default.orders",
        "SELECT COUNT(*) FROM default.orders_source",
    )

    # L1: AOV (derived from both base metrics)
    aov_node, aov_rev = await create_derived_metric(
        "default.aov",
        "SELECT default.revenue / NULLIF(default.orders, 0)",
        [revenue_node, orders_node],
    )

    # L2: AOV Growth Index (nested derived from aov)
    _, aov_growth_rev = await create_derived_metric(
        "default.aov_growth_index",
        "SELECT default.aov * 100 / 50",
        [aov_node],
    )

    # Set current revisions
    revenue_node.current = revenue_rev
    orders_node.current = orders_rev
    aov_node.current = aov_rev

    # Build cache (this is how build_v3 does it)
    nodes_cache = {
        "default.revenue": revenue_node,
        "default.orders": orders_node,
        "default.aov": aov_node,
    }
    parent_map: dict[str, list[str]] = {
        "default.revenue": [],
        "default.orders": [],
        "default.aov": ["default.revenue", "default.orders"],
        "default.aov_growth_index": ["default.aov"],
    }

    # Note: aov_growth_index node is not in cache, but its parent (aov) is.
    # This simulates how build_v3 works - it loads needed nodes.
    aov_growth_node = Node(
        name="default.aov_growth_index",
        type=NodeType.METRIC,
        current_version="v1.0",
    )
    aov_growth_node.current = aov_growth_rev
    nodes_cache["default.aov_growth_index"] = aov_growth_node

    extractor = MetricComponentExtractor(aov_growth_rev.id)
    components, derived_ast = await extractor.extract(
        session,
        nodes_cache=nodes_cache,
        parent_map=parent_map,
        metric_node=aov_growth_node,
    )
    derived_sql = str(derived_ast)

    # Should decompose all the way to base components
    assert len(components) == 2
    agg_types = {c.aggregation for c in components}
    assert agg_types == {"SUM", "COUNT"}

    # Should not contain any intermediate metric references
    assert "default.revenue" not in derived_sql
    assert "default.orders" not in derived_sql
    assert "default.aov" not in derived_sql


@pytest.mark.asyncio
async def test_extract_nested_derived_with_avg(
    clean_session: AsyncSession,
    create_base_metric,
    create_derived_metric,
):
    """
    Test nested derived metric where base metric uses AVG (multi-component).

    AVG decomposes to SUM and COUNT components. The nested derived should
    collect both components through the intermediate derived metric.
    """
    session = clean_session
    # Base metric using AVG (decomposes to SUM + COUNT)
    avg_price_node, _ = await create_base_metric(
        "default.avg_price",
        "SELECT AVG(price) FROM default.products",
    )

    # Derived metric referencing AVG metric
    price_index_node, _ = await create_derived_metric(
        "default.price_index",
        "SELECT default.avg_price * 100",
        [avg_price_node],
    )

    # Nested derived
    _, adjusted_index_rev = await create_derived_metric(
        "default.adjusted_price_index",
        "SELECT default.price_index / 1.1",
        [price_index_node],
    )

    extractor = MetricComponentExtractor(adjusted_index_rev.id)
    components, derived_ast = await extractor.extract(session)
    derived_sql = str(derived_ast)

    # Should have both SUM and COUNT components from AVG decomposition
    assert len(components) == 2
    agg_types = {c.aggregation for c in components}
    assert agg_types == {"SUM", "COUNT"}

    # Expressions should be the same (both from price)
    expressions = {c.expression for c in components}
    assert expressions == {"price"}

    # Derived SQL should contain the AVG combiner (SUM/COUNT pattern)
    assert "SUM(" in derived_sql
    assert "/" in derived_sql  # Division from AVG decomposition
