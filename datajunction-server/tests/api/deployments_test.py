import asyncio
import json
from unittest import mock
from datajunction_server.internal.deployment import (
    safe_task,
)
from datajunction_server.models.deployment import (
    ColumnSpec,
    DeploymentResult,
    DeploymentSpec,
    DeploymentStatus,
    DimensionReferenceLinkSpec,
    TransformSpec,
    SourceSpec,
    MetricSpec,
    DimensionSpec,
    CubeSpec,
    DimensionJoinLinkSpec,
)
from datajunction_server.errors import DJException
from datajunction_server.models.dimensionlink import JoinType
from datajunction_server.database.node import Node
from datajunction_server.models.node import (
    MetricDirection,
    MetricUnit,
    NodeMode,
    NodeType,
)
import pytest


@pytest.fixture(autouse=True, scope="module")
def patch_effective_writer_concurrency():
    from datajunction_server.internal.deployment import settings

    with mock.patch.object(
        settings.__class__,
        "effective_writer_concurrency",
        new_callable=mock.PropertyMock,
        return_value=1,
    ):
        yield


@pytest.fixture
def default_repair_orders():
    return SourceSpec(
        name="default.repair_orders",
        description="""All repair orders""",
        table="default.roads.repair_orders",
        columns=[
            ColumnSpec(
                name="repair_order_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="municipality_id",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="hard_hat_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="order_date",
                type="timestamp",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="required_date",
                type="timestamp",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="dispatched_date",
                type="timestamp",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="dispatcher_id",
                type="int",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="${prefix}default.repair_order",
                join_type="inner",
                join_on="${prefix}default.repair_orders.repair_order_id = ${prefix}default.repair_order.repair_order_id",
            ),
            DimensionJoinLinkSpec(
                dimension_node="${prefix}default.dispatcher",
                join_type="inner",
                join_on="${prefix}default.repair_orders.dispatcher_id = ${prefix}default.dispatcher.dispatcher_id",
            ),
        ],
        owners=["dj"],
    )


@pytest.fixture
def default_repair_orders_view():
    return SourceSpec(
        name="default.repair_orders_view",
        description="""All repair orders (view)""",
        query="""CREATE OR REPLACE VIEW roads.repair_orders_view AS SELECT * FROM roads.repair_orders""",
        table="default.roads.repair_orders_view",
        columns=[
            ColumnSpec(
                name="repair_order_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="municipality_id",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="hard_hat_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="order_date",
                type="timestamp",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="required_date",
                type="timestamp",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="dispatched_date",
                type="timestamp",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="dispatcher_id",
                type="int",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_repair_order_details():
    return SourceSpec(
        name="default.repair_order_details",
        description="""Details on repair orders""",
        table="default.roads.repair_order_details",
        columns=[
            ColumnSpec(
                name="repair_order_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="repair_type_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="price",
                type="float",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="quantity",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="discount",
                type="float",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="${prefix}default.repair_order",
                join_type=JoinType.INNER,
                join_on="${prefix}default.repair_order_details.repair_order_id = ${prefix}default.repair_order.repair_order_id",
            ),
        ],
        owners=["dj"],
    )


@pytest.fixture
def default_repair_type():
    return SourceSpec(
        name="default.repair_type",
        description="""Information on types of repairs""",
        table="default.roads.repair_type",
        columns=[
            ColumnSpec(
                name="repair_type_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="repair_type_name",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="contractor_id",
                type="int",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="${prefix}default.contractor",
                join_type=JoinType.INNER,
                join_on="${prefix}default.repair_type.contractor_id = ${prefix}default.contractor.contractor_id",
            ),
        ],
        owners=["dj"],
    )


@pytest.fixture
def default_contractors():
    return SourceSpec(
        name="default.contractors",
        description="""Information on contractors""",
        table="default.roads.contractors",
        columns=[
            ColumnSpec(
                name="contractor_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="company_name",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="contact_name",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="contact_title",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="address",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="city",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="state",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="postal_code",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="country",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="phone",
                type="string",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="${prefix}default.us_state",
                join_type=JoinType.INNER,
                join_on="${prefix}default.contractors.state = ${prefix}default.us_state.state_short",
            ),
        ],
        owners=["dj"],
    )


@pytest.fixture
def default_municipality_municipality_type():
    return SourceSpec(
        name="default.municipality_municipality_type",
        description="""Lookup table for municipality and municipality types""",
        table="default.roads.municipality_municipality_type",
        columns=[
            ColumnSpec(
                name="municipality_id",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="municipality_type_id",
                type="string",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_municipality_type():
    return SourceSpec(
        name="default.municipality_type",
        description="""Information on municipality types""",
        table="default.roads.municipality_type",
        columns=[
            ColumnSpec(
                name="municipality_type_id",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="municipality_type_desc",
                type="string",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_municipality():
    return SourceSpec(
        name="default.municipality",
        description="""Information on municipalities""",
        table="default.roads.municipality",
        columns=[
            ColumnSpec(
                name="municipality_id",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="contact_name",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="contact_title",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="local_region",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="phone",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="state_id",
                type="int",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_dispatchers():
    return SourceSpec(
        name="default.dispatchers",
        description="""Information on dispatchers""",
        table="default.roads.dispatchers",
        columns=[
            ColumnSpec(
                name="dispatcher_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="company_name",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="phone",
                type="string",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_hard_hats():
    return SourceSpec(
        name="default.hard_hats",
        description="""Information on employees""",
        table="default.roads.hard_hats",
        columns=[
            ColumnSpec(
                name="hard_hat_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="last_name",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="first_name",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="title",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="birth_date",
                type="timestamp",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="hire_date",
                type="timestamp",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="address",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="city",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="state",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="postal_code",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="country",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="manager",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="contractor_id",
                type="int",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_hard_hat_state():
    return SourceSpec(
        name="default.hard_hat_state",
        description="""Lookup table for employee's current state""",
        table="default.roads.hard_hat_state",
        columns=[
            ColumnSpec(
                name="hard_hat_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="state_id",
                type="string",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_us_states():
    return SourceSpec(
        name="default.us_states",
        description="""Information on different types of repairs""",
        table="default.roads.us_states",
        columns=[
            ColumnSpec(
                name="state_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="state_name",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="state_abbr",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="state_region",
                type="int",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_us_region():
    return SourceSpec(
        name="default.us_region",
        description="""Information on US regions""",
        table="default.roads.us_region",
        columns=[
            ColumnSpec(
                name="us_region_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="us_region_description",
                type="string",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_repair_order():
    return DimensionSpec(
        name="default.repair_order",
        description="""Repair order dimension""",
        query="""
                        SELECT
                        repair_order_id,
                        municipality_id,
                        hard_hat_id,
                        order_date,
                        required_date,
                        dispatched_date,
                        dispatcher_id
                        FROM ${prefix}default.repair_orders
                    """,
        primary_key=["repair_order_id"],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="${prefix}default.dispatcher",
                join_type="inner",
                join_on="${prefix}default.repair_order.dispatcher_id = ${prefix}default.dispatcher.dispatcher_id",
            ),
            DimensionJoinLinkSpec(
                dimension_node="${prefix}default.municipality_dim",
                join_type="inner",
                join_on="${prefix}default.repair_order.municipality_id = ${prefix}default.municipality_dim.municipality_id",
            ),
            DimensionJoinLinkSpec(
                dimension_node="${prefix}default.hard_hat",
                join_type="inner",
                join_on="${prefix}default.repair_order.hard_hat_id = ${prefix}default.hard_hat.hard_hat_id",
            ),
        ],
        owners=["dj"],
    )


@pytest.fixture
def default_contractor():
    return DimensionSpec(
        name="default.contractor",
        description="""Contractor dimension""",
        query="""
                        SELECT
                        contractor_id,
                        company_name,
                        contact_name,
                        contact_title,
                        address,
                        city,
                        state,
                        postal_code,
                        country,
                        phone
                        FROM ${prefix}default.contractors
                    """,
        primary_key=["contractor_id"],
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_hard_hat():
    return DimensionSpec(
        name="default.hard_hat",
        description="""Hard hat dimension""",
        query="""
                        SELECT
                        hard_hat_id,
                        last_name,
                        first_name,
                        title,
                        birth_date,
                        hire_date,
                        address,
                        city,
                        state,
                        postal_code,
                        country,
                        manager,
                        contractor_id
                        FROM ${prefix}default.hard_hats
                    """,
        primary_key=["hard_hat_id"],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="${prefix}default.us_state",
                join_type="inner",
                join_on="${prefix}default.hard_hat.state = ${prefix}default.us_state.state_short",
            ),
        ],
        owners=["dj"],
    )


@pytest.fixture
def default_us_state():
    return DimensionSpec(
        name="default.us_state",
        description="""US state dimension""",
        query="""
                        SELECT
                        state_id,
                        state_name,
                        state_abbr AS state_short,
                        state_region
                        FROM ${prefix}default.us_states s
                    """,
        primary_key=["state_short"],
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_dispatcher():
    return DimensionSpec(
        name="default.dispatcher",
        description="""Dispatcher dimension""",
        query="""
                        SELECT
                        dispatcher_id,
                        company_name,
                        phone
                        FROM ${prefix}default.dispatchers
                    """,
        primary_key=["dispatcher_id"],
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_municipality_dim():
    return DimensionSpec(
        name="default.municipality_dim",
        description="""Municipality dimension""",
        query="""
                        SELECT
                        m.municipality_id AS municipality_id,
                        contact_name,
                        contact_title,
                        local_region,
                        state_id,
                        mmt.municipality_type_id AS municipality_type_id,
                        mt.municipality_type_desc AS municipality_type_desc
                        FROM ${prefix}default.municipality AS m
                        LEFT JOIN ${prefix}default.municipality_municipality_type AS mmt
                        ON m.municipality_id = mmt.municipality_id
                        LEFT JOIN ${prefix}default.municipality_type AS mt
                        ON mmt.municipality_type_id = mt.municipality_type_desc
                    """,
        primary_key=["municipality_id"],
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_regional_level_agg():
    return TransformSpec(
        name="default.regional_level_agg",
        description="""Regional-level aggregates""",
        query="""
WITH ro as (SELECT
        repair_order_id,
        municipality_id,
        hard_hat_id,
        order_date,
        required_date,
        dispatched_date,
        dispatcher_id
    FROM ${prefix}default.repair_orders)
            SELECT
    usr.us_region_id,
    us.state_name,
    CONCAT(us.state_name, '-', usr.us_region_description) AS location_hierarchy,
    EXTRACT(YEAR FROM ro.order_date) AS order_year,
    EXTRACT(MONTH FROM ro.order_date) AS order_month,
    EXTRACT(DAY FROM ro.order_date) AS order_day,
    COUNT(DISTINCT CASE WHEN ro.dispatched_date IS NOT NULL THEN ro.repair_order_id ELSE NULL END) AS completed_repairs,
    COUNT(DISTINCT ro.repair_order_id) AS total_repairs_dispatched,
    SUM(rd.price * rd.quantity) AS total_amount_in_region,
    AVG(rd.price * rd.quantity) AS avg_repair_amount_in_region,
    -- ELEMENT_AT(ARRAY_SORT(COLLECT_LIST(STRUCT(COUNT(*) AS cnt, rt.repair_type_name AS repair_type_name)), (left, right) -> case when left.cnt < right.cnt then 1 when left.cnt > right.cnt then -1 else 0 end), 0).repair_type_name AS most_common_repair_type,
    AVG(DATEDIFF(ro.dispatched_date, ro.order_date)) AS avg_dispatch_delay,
    COUNT(DISTINCT c.contractor_id) AS unique_contractors
FROM ro
JOIN
    ${prefix}default.municipality m ON ro.municipality_id = m.municipality_id
JOIN
    ${prefix}default.us_states us ON m.state_id = us.state_id
                         AND AVG(rd.price * rd.quantity) >
                            (SELECT AVG(price * quantity) FROM ${prefix}default.repair_order_details WHERE repair_order_id = ro.repair_order_id)
JOIN
    ${prefix}default.us_states us ON m.state_id = us.state_id
JOIN
    ${prefix}default.us_region usr ON us.state_region = usr.us_region_id
JOIN
    ${prefix}default.repair_order_details rd ON ro.repair_order_id = rd.repair_order_id
JOIN
    ${prefix}default.repair_type rt ON rd.repair_type_id = rt.repair_type_id
JOIN
    ${prefix}default.contractors c ON rt.contractor_id = c.contractor_id
GROUP BY
    usr.us_region_id,
    EXTRACT(YEAR FROM ro.order_date),
    EXTRACT(MONTH FROM ro.order_date),
    EXTRACT(DAY FROM ro.order_date)""",
        primary_key=[
            "us_region_id",
            "state_name",
            "order_year",
            "order_month",
            "order_day",
        ],
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_national_level_agg():
    return TransformSpec(
        name="default.national_level_agg",
        description="""National level aggregates""",
        query="""SELECT SUM(rd.price * rd.quantity) AS total_amount_nationwide FROM ${prefix}default.repair_order_details rd""",
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_repair_orders_fact():
    return TransformSpec(
        name="default.repair_orders_fact",
        description="""Fact transform with all details on repair orders""",
        query="""SELECT
  repair_orders.repair_order_id,
  repair_orders.municipality_id,
  repair_orders.hard_hat_id,
  repair_orders.dispatcher_id,
  repair_orders.order_date,
  repair_orders.dispatched_date,
  repair_orders.required_date,
  repair_order_details.discount,
  repair_order_details.price,
  repair_order_details.quantity,
  repair_order_details.repair_type_id,
  repair_order_details.price * repair_order_details.quantity AS total_repair_cost,
  repair_orders.dispatched_date - repair_orders.order_date AS time_to_dispatch,
  repair_orders.dispatched_date - repair_orders.required_date AS dispatch_delay
FROM
  ${prefix}default.repair_orders repair_orders
JOIN
  ${prefix}default.repair_order_details repair_order_details
ON repair_orders.repair_order_id = repair_order_details.repair_order_id""",
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="${prefix}default.municipality_dim",
                join_type="inner",
                join_on="${prefix}default.repair_orders_fact.municipality_id = ${prefix}default.municipality_dim.municipality_id",
            ),
            DimensionJoinLinkSpec(
                dimension_node="${prefix}default.hard_hat",
                join_type="inner",
                join_on="${prefix}default.repair_orders_fact.hard_hat_id = ${prefix}default.hard_hat.hard_hat_id",
            ),
            DimensionJoinLinkSpec(
                dimension_node="${prefix}default.dispatcher",
                join_type="inner",
                join_on="${prefix}default.repair_orders_fact.dispatcher_id = ${prefix}default.dispatcher.dispatcher_id",
            ),
        ],
        owners=["dj"],
    )


@pytest.fixture
def default_regional_repair_efficiency():
    return MetricSpec(
        name="default.regional_repair_efficiency",
        description="""For each US region (as defined in the us_region table), we want to calculate:
            Regional Repair Efficiency = (Number of Completed Repairs / Total Repairs Dispatched) ×
                                         (Total Repair Amount in Region / Total Repair Amount Nationwide) × 100
            Here:
                A "Completed Repair" is one where the dispatched_date is not null.
                "Total Repair Amount in Region" is the total amount spent on repairs in a given region.
                "Total Repair Amount Nationwide" is the total amount spent on all repairs nationwide.""",
        query="""SELECT
    (SUM(rm.completed_repairs) * 1.0 / SUM(rm.total_repairs_dispatched)) *
    (SUM(rm.total_amount_in_region) * 1.0 / SUM(na.total_amount_nationwide)) * 100
FROM
    ${prefix}default.regional_level_agg rm
CROSS JOIN
    ${prefix}default.national_level_agg na""",
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_num_repair_orders():
    return MetricSpec(
        name="default.num_repair_orders",
        description="""Number of repair orders""",
        query="""SELECT count(repair_order_id) FROM ${prefix}default.repair_orders_fact""",
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_avg_repair_price():
    return MetricSpec(
        name="default.avg_repair_price",
        description="""Average repair price""",
        query="""SELECT avg(repair_orders_fact.price) FROM ${prefix}default.repair_orders_fact repair_orders_fact""",
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_total_repair_cost():
    return MetricSpec(
        name="default.total_repair_cost",
        description="""Total repair cost""",
        query="""SELECT sum(total_repair_cost) FROM ${prefix}default.repair_orders_fact""",
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_avg_length_of_employment():
    return MetricSpec(
        name="default.avg_length_of_employment",
        description="""Average length of employment""",
        query="""SELECT avg(CAST(NOW() AS DATE) - hire_date) FROM ${prefix}default.hard_hat""",
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_discounted_orders_rate():
    return MetricSpec(
        name="default.discounted_orders_rate",
        description="""Proportion of Discounted Orders""",
        query="""
                SELECT
                  cast(sum(if(discount > 0.0, 1, 0)) as double) / count(*)
                    AS default_DOT_discounted_orders_rate
                FROM ${prefix}default.repair_orders_fact
                """,
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_total_repair_order_discounts():
    return MetricSpec(
        name="default.total_repair_order_discounts",
        description="""Total repair order discounts""",
        query="""SELECT sum(price * discount) FROM ${prefix}default.repair_orders_fact""",
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_avg_repair_order_discounts():
    return MetricSpec(
        name="default.avg_repair_order_discounts",
        description="""Average repair order discounts""",
        query="""SELECT avg(price * discount) FROM ${prefix}default.repair_orders_fact""",
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_avg_time_to_dispatch():
    return MetricSpec(
        name="default.avg_time_to_dispatch",
        description="""Average time to dispatch a repair order""",
        query="""SELECT avg(cast(repair_orders_fact.time_to_dispatch as int)) FROM ${prefix}default.repair_orders_fact repair_orders_fact""",
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_repairs_cube():
    return CubeSpec(
        name="default.repairs_cube",
        display_name="Repairs Cube",
        description="""Cube for analyzing repair orders""",
        dimensions=[
            "${prefix}default.hard_hat.state",
            "${prefix}default.dispatcher.company_name",
            "${prefix}default.municipality_dim.local_region",
        ],
        metrics=[
            "${prefix}default.num_repair_orders",
            "${prefix}default.avg_repair_price",
            "${prefix}default.total_repair_cost",
        ],
        owners=["dj"],
    )


@pytest.fixture
def roads_nodes(
    default_repair_orders,
    default_repair_orders_view,
    default_repair_order_details,
    default_repair_type,
    default_contractors,
    default_municipality_municipality_type,
    default_municipality_type,
    default_municipality,
    default_dispatchers,
    default_hard_hats,
    default_hard_hat_state,
    default_us_states,
    default_us_region,
    default_repair_order,
    default_contractor,
    default_hard_hat,
    default_us_state,
    default_dispatcher,
    default_municipality_dim,
    default_regional_level_agg,
    default_national_level_agg,
    default_repair_orders_fact,
    default_regional_repair_efficiency,
    default_num_repair_orders,
    default_avg_repair_price,
    default_total_repair_cost,
    default_avg_length_of_employment,
    default_discounted_orders_rate,
    default_total_repair_order_discounts,
    default_avg_repair_order_discounts,
    default_avg_time_to_dispatch,
    default_repairs_cube,
):
    return [
        default_repair_orders,
        default_repair_orders_view,
        default_repair_order_details,
        default_repair_type,
        default_contractors,
        default_municipality_municipality_type,
        default_municipality_type,
        default_municipality,
        default_dispatchers,
        default_hard_hats,
        default_hard_hat_state,
        default_us_states,
        default_us_region,
        default_repair_order,
        default_contractor,
        default_hard_hat,
        default_us_state,
        default_dispatcher,
        default_municipality_dim,
        default_regional_level_agg,
        default_national_level_agg,
        default_repair_orders_fact,
        default_regional_repair_efficiency,
        default_num_repair_orders,
        default_avg_repair_price,
        default_total_repair_cost,
        default_avg_length_of_employment,
        default_discounted_orders_rate,
        default_total_repair_order_discounts,
        default_avg_repair_order_discounts,
        default_avg_time_to_dispatch,
        default_repairs_cube,
    ]


async def deploy_and_wait(client, deployment_spec: DeploymentSpec):
    response = await client.post(
        "/deployments",
        json=deployment_spec.dict(),
    )
    data = response.json()
    deployment_uuid = data["uuid"]
    while data["status"] not in (
        DeploymentStatus.FAILED.value,
        DeploymentStatus.SUCCESS.value,
    ):
        await asyncio.sleep(1)
        response = await client.get(f"/deployments/{deployment_uuid}")
        data = response.json()
    return data


@pytest.mark.xdist_group(name="deployments")
class TestDeployments:
    @pytest.mark.asyncio
    # @pytest.mark.parametrize("client", [False], indirect=True)
    async def test_deploy_failed_on_non_existent_upstream_deps(
        self,
        client,
        default_hard_hat,
        default_hard_hats,
    ):
        """
        Test deployment failures with non-existent upstream dependencies
        """
        namespace = "missing_upstreams"
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[default_hard_hat],
            ),
        )
        assert data == {
            "uuid": mock.ANY,
            "namespace": namespace,
            "status": "failed",
            "results": [
                {
                    "name": "DJInvalidDeploymentConfig",
                    "deploy_type": "general",
                    "status": "failed",
                    "message": f"The following dependencies are not in the deployment and do not pre-exist in the system: {namespace}.default.hard_hats, {namespace}.default.us_state",
                    "operation": "unknown",
                },
            ],
        }

    @pytest.mark.asyncio
    # @pytest.mark.parametrize("client", [False], indirect=True)
    async def test_deploy_failed_on_non_existent_link_deps(
        self,
        client,
        default_hard_hat,
        default_hard_hats,
    ):
        """
        Test deployment failures for a node that has a dimension link to a node that doesn't exist
        """
        namespace = "missing_dimension_node"
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[default_hard_hats, default_hard_hat],
            ),
        )
        assert data == {
            "uuid": mock.ANY,
            "namespace": namespace,
            "status": "failed",
            "results": [
                {
                    "name": "DJInvalidDeploymentConfig",
                    "deploy_type": "general",
                    "status": "failed",
                    "message": f"The following dependencies are not in the deployment and do not pre-exist in the system: {namespace}.default.us_state",
                    "operation": "unknown",
                },
            ],
        }

    @pytest.mark.asyncio
    async def test_deploy_failed_with_bad_node_spec_pk(
        self,
        client,
        default_hard_hats,
        default_us_states,
        default_us_state,
    ):
        """
        Test deployment failures with bad node specifications (primary key that doesn't exist in the query)
        """
        bad_dim_spec = DimensionSpec(
            name="default.hard_hat",
            description="""Hard hat dimension""",
            query="""SELECT last_name, first_name FROM ${prefix}default.hard_hats""",
            primary_key=["hard_hat_id"],
            owners=["dj"],
            dimension_links=[
                DimensionJoinLinkSpec(
                    dimension_node="${prefix}default.us_state",
                    join_type="inner",
                    join_on="${prefix}default.hard_hat.state = ${prefix}default.us_state.state_short",
                ),
            ],
        )
        namespace = "bad_node_spec"
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[
                    bad_dim_spec,
                    default_hard_hats,
                    default_us_states,
                    default_us_state,
                ],
            ),
        )
        assert data == {
            "status": "failed",
            "uuid": mock.ANY,
            "namespace": namespace,
            "results": [
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.hard_hats",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.us_states",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Some columns in the primary key [hard_hat_id] were not found in "
                    "the list of available columns for the node "
                    f"{namespace}.default.hard_hat.",
                    "name": f"{namespace}.default.hard_hat",
                    "status": "failed",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created dimension (v1.0)",
                    "name": f"{namespace}.default.us_state",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "link",
                    "message": f"A node with name `{namespace}.default.hard_hat` does not exist.",
                    "name": f"{namespace}.default.hard_hat -> {namespace}.default.us_state",
                    "status": "failed",
                    "operation": "create",
                },
            ],
        }

    @pytest.mark.asyncio
    async def test_deploy_with_dimension_link_removal(
        self,
        client,
        default_hard_hats,
        default_us_states,
        default_us_state,
    ):
        """
        Test that removing a dimension link from a node works as expected
        """
        namespace = "link_removal"
        dim_spec = DimensionSpec(
            name="default.hard_hat",
            description="""Hard hat dimension""",
            query="""
            SELECT
                hard_hat_id,
                state
            FROM ${prefix}default.hard_hats
            """,
            primary_key=["hard_hat_id"],
            owners=["dj"],
            dimension_links=[
                DimensionJoinLinkSpec(
                    dimension_node="${prefix}default.us_state",
                    join_type="inner",
                    join_on="${prefix}default.hard_hat.state = ${prefix}default.us_state.state_short",
                ),
            ],
        )
        nodes_list = [dim_spec, default_hard_hats, default_us_states, default_us_state]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=nodes_list,
            ),
        )
        assert data["status"] == "success"

        # Remove the dimension link and redeploy
        dim_spec.dimension_links = []
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"
        assert data["results"][-1] == {
            "deploy_type": "link",
            "message": "",
            "name": "link_removal.default.hard_hat -> link_removal.default.us_state",
            "operation": "delete",
            "status": "success",
        }

    @pytest.mark.asyncio
    async def test_deploy_with_reference_dimension_link(
        self,
        client,
        default_hard_hats,
        default_us_states,
        default_us_state,
    ):
        """
        Test that removing a dimension link from a node works as expected
        """
        namespace = "reference_link"
        dim_spec = DimensionSpec(
            name="default.hard_hat",
            description="""Hard hat dimension""",
            query="""
            SELECT
                hard_hat_id,
                state
            FROM ${prefix}default.hard_hats
            """,
            primary_key=["hard_hat_id"],
            owners=["dj"],
            dimension_links=[
                DimensionReferenceLinkSpec(
                    node_column="state",
                    dimension="${prefix}default.us_state.state_short",
                ),
            ],
        )
        nodes_list = [dim_spec, default_hard_hats, default_us_states, default_us_state]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"
        assert data["results"][-1] == {
            "deploy_type": "link",
            "message": "Reference link successfully deployed",
            "name": "reference_link.default.hard_hat -> reference_link.default.us_state",
            "operation": "create",
            "status": "success",
        }

    @pytest.mark.asyncio
    async def test_deploy_dimension_with_update(
        self,
        client,
        default_hard_hats,
        default_us_states,
        default_us_state,
    ):
        """
        Test that updating a dimension node's query works as expected
        """
        namespace = "node_update"
        dim_spec = DimensionSpec(
            name="default.hard_hat",
            description="""Hard hat dimension""",
            query="""
            SELECT
                hard_hat_id,
                state
            FROM ${prefix}default.hard_hats
            """,
            primary_key=["hard_hat_id"],
            owners=["dj"],
            dimension_links=[
                DimensionJoinLinkSpec(
                    dimension_node="${prefix}default.us_state",
                    join_type="inner",
                    join_on="${prefix}default.hard_hat.state = ${prefix}default.us_state.state_short",
                ),
            ],
        )
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[
                    dim_spec,
                    default_hard_hats,
                    default_us_states,
                    default_us_state,
                ],
            ),
        )
        assert data["status"] == "success"
        dim_spec.query = """
        SELECT
            hard_hat_id,
            state,
            first_name,
            last_name
        FROM ${prefix}default.hard_hats
        """
        nodes_list = [dim_spec, default_hard_hats, default_us_states, default_us_state]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"
        assert len(data["results"]) == 5
        assert all(res["status"] == "skipped" for res in data["results"][:3])
        assert data["results"][3:] == [
            {
                "deploy_type": "node",
                "name": f"{namespace}.default.hard_hat",
                "status": "success",
                "operation": "update",
                "message": "Updated dimension (v2.0)\n"
                "└─ Updated display_name, primary_key",
            },
            {
                "deploy_type": "link",
                "name": f"{namespace}.default.hard_hat -> {namespace}.default.us_state",
                "status": "skipped",
                "operation": "create",
                "message": "No change to dimension link",
            },
        ]

    @pytest.mark.asyncio
    async def test_deploy_metric_with_update(
        self,
        client,
        default_hard_hats,
        default_hard_hat,
        default_us_states,
        default_us_state,
        default_avg_length_of_employment,
    ):
        """
        Test that updating a metric node's works as expected
        """
        namespace = "metric_update"
        nodes_list = [
            default_hard_hats,
            default_hard_hat,
            default_us_states,
            default_us_state,
            default_avg_length_of_employment,
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"

        # Bad query - metric should fail to deploy
        default_avg_length_of_employment.query = """
        SELECT hard_hat_id FROM ${prefix}default.hard_hat
        """
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "failed"
        assert data["results"][-1] == {
            "deploy_type": "node",
            "message": "Metric metric_update.default.avg_length_of_employment has an invalid "
            "query, should have an aggregate expression",
            "name": "metric_update.default.avg_length_of_employment",
            "operation": "update",
            "status": "failed",
        }

        # Fix query - metric should deploy successfully
        default_avg_length_of_employment.query = """
        SELECT COUNT(hard_hat_id) FROM ${prefix}default.hard_hat
        """
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"
        assert data["results"][-1] == {
            "deploy_type": "node",
            "message": "Updated metric (v2.0)\n"
            "└─ Updated display_name, direction, unit_enum",
            "name": "metric_update.default.avg_length_of_employment",
            "operation": "update",
            "status": "success",
        }

    @pytest.mark.asyncio
    async def test_deploy_cube_with_update(
        self,
        client,
        default_hard_hats,
        default_hard_hat,
        default_us_states,
        default_us_state,
        default_avg_length_of_employment,
    ):
        """
        Test that updating a cube node's works as expected
        """
        namespace = "cube_update"
        cube = CubeSpec(
            name="default.repairs_cube",
            display_name="Repairs Cube",
            description="""Cube for analyzing repair orders""",
            dimensions=[
                "${prefix}default.hard_hat.state",
            ],
            metrics=[
                "${prefix}default.avg_length_of_employment",
            ],
            owners=["dj"],
        )
        nodes_list = [
            default_hard_hats,
            default_hard_hat,
            default_us_states,
            default_us_state,
            default_avg_length_of_employment,
            cube,
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"

        # Update cube to have a bad dimension - cube should fail to deploy
        cube.dimensions = [
            "${prefix}default.hard_hat.state",
            "${prefix}default.us_state.state_region",
            "${prefix}default.us_state.non_existent_column",
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "failed"
        assert data["results"][-1] == {
            "deploy_type": "node",
            "message": mock.ANY,
            "name": "cube_update.default.repairs_cube",
            "operation": "update",
            "status": "failed",
        }

        # Update cube to add an existing dimension - should deploy successfully
        cube.dimensions = [
            "${prefix}default.hard_hat.state",
            "${prefix}default.us_state.state_region",
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"
        assert data["results"][-1] == {
            "deploy_type": "node",
            "message": "Updated cube (v2.0)\n└─ Updated metrics, dimensions",
            "name": "cube_update.default.repairs_cube",
            "operation": "update",
            "status": "success",
        }

    @pytest.mark.asyncio
    async def test_deploy_failed_with_bad_node_spec_links(
        self,
        client,
        default_hard_hats,
        default_us_states,
        default_us_state,
    ):
        """
        Test deployment failures with bad node specifications (dimension link to a column that doesn't exist)
        """
        namespace = "bad_node_spec_links"
        bad_dim_spec = DimensionSpec(
            name="default.hard_hat",
            description="""Hard hat dimension""",
            query="""
            SELECT
                hard_hat_id,
                last_name,
                first_name
            FROM ${prefix}default.hard_hats
            """,
            primary_key=["hard_hat_id"],
            owners=["dj"],
            dimension_links=[
                DimensionJoinLinkSpec(
                    dimension_node="${prefix}default.us_state",
                    join_type="inner",
                    join_on="${prefix}default.hard_hat.state = ${prefix}default.us_state.state_short",
                ),
            ],
        )
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[
                    bad_dim_spec,
                    default_hard_hats,
                    default_us_states,
                    default_us_state,
                ],
            ),
        )
        assert data == {
            "status": "failed",
            "uuid": mock.ANY,
            "namespace": namespace,
            "results": [
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.hard_hats",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.us_states",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created dimension (v1.0)",
                    "name": f"{namespace}.default.hard_hat",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created dimension (v1.0)",
                    "name": f"{namespace}.default.us_state",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "link",
                    "message": f"Join query {namespace}.default.hard_hat.state = "
                    f"{namespace}.default.us_state.state_short is not valid\n"
                    "The following error happened:\n"
                    f"- Column `{namespace}.default.hard_hat.state` does not exist on "
                    "any valid table. (error code: 206)",
                    "name": f"{namespace}.default.hard_hat -> {namespace}.default.us_state",
                    "status": "failed",
                    "operation": "create",
                },
            ],
        }

    @pytest.mark.asyncio
    async def test_deploy_succeeds_with_existing_deps(
        self,
        client,
        default_hard_hats,
        default_hard_hat,
        default_us_state,
        default_us_states,
    ):
        """
        Test that deploying with all dependencies included succeeds
        """
        namespace = "existing_deps"
        mini_setup = DeploymentSpec(
            namespace=namespace,
            nodes=[
                default_hard_hats,
                default_hard_hat,
                default_us_state,
                default_us_states,
            ],
        )
        data = await deploy_and_wait(client, mini_setup)
        assert data == {
            "status": "success",
            "uuid": mock.ANY,
            "namespace": namespace,
            "results": [
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.hard_hats",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.us_states",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created dimension (v1.0)",
                    "name": f"{namespace}.default.hard_hat",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created dimension (v1.0)",
                    "name": f"{namespace}.default.us_state",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "link",
                    "message": "Join link successfully deployed",
                    "name": f"{namespace}.default.hard_hat -> {namespace}.default.us_state",
                    "status": "success",
                    "operation": "create",
                },
            ],
        }

        # Re-deploying the same setup should be a noop
        data = await deploy_and_wait(client, mini_setup)
        assert all(res["status"] == "skipped" for res in data["results"])
        assert all(res["operation"] == "noop" for res in data["results"])

        # Redeploying half the setup should only deploy the missing nodes

        # deploying a new link should trigger a redeploy of the node it is linked from

    @pytest.mark.asyncio
    async def test_roads_deployment(self, client, roads_nodes):
        namespace = "base"
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=roads_nodes),
        )
        assert data == {
            "status": "success",
            "uuid": mock.ANY,
            "namespace": namespace,
            "results": [
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.contractors",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.hard_hats",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.municipality",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.repair_order_details",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.repair_orders",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.repair_type",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.us_region",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.us_states",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.dispatchers",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created dimension (v1.0)",
                    "name": f"{namespace}.default.hard_hat",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.municipality_municipality_type",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.municipality_type",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created transform (v1.0)",
                    "name": f"{namespace}.default.national_level_agg",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created transform (v1.0)",
                    "name": f"{namespace}.default.regional_level_agg",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created transform (v1.0)",
                    "name": f"{namespace}.default.repair_orders_fact",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created metric (v1.0)",
                    "name": f"{namespace}.default.avg_length_of_employment",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created metric (v1.0)",
                    "name": f"{namespace}.default.avg_repair_order_discounts",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created metric (v1.0)",
                    "name": f"{namespace}.default.avg_repair_price",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created metric (v1.0)",
                    "name": f"{namespace}.default.avg_time_to_dispatch",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created dimension (v1.0)",
                    "name": f"{namespace}.default.contractor",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created metric (v1.0)",
                    "name": f"{namespace}.default.discounted_orders_rate",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created dimension (v1.0)",
                    "name": f"{namespace}.default.dispatcher",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.hard_hat_state",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created dimension (v1.0)",
                    "name": f"{namespace}.default.municipality_dim",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created metric (v1.0)",
                    "name": f"{namespace}.default.num_repair_orders",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created metric (v1.0)",
                    "name": f"{namespace}.default.regional_repair_efficiency",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created dimension (v1.0)",
                    "name": f"{namespace}.default.repair_order",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.repair_orders_view",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created metric (v1.0)",
                    "name": f"{namespace}.default.total_repair_cost",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created metric (v1.0)",
                    "name": f"{namespace}.default.total_repair_order_discounts",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created dimension (v1.0)",
                    "name": f"{namespace}.default.us_state",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "link",
                    "message": "Join link successfully deployed",
                    "name": f"{namespace}.default.repair_orders -> base.default.repair_order",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "link",
                    "message": "Join link successfully deployed",
                    "name": f"{namespace}.default.repair_orders -> base.default.dispatcher",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "link",
                    "message": "Join link successfully deployed",
                    "name": f"{namespace}.default.repair_order_details -> base.default.repair_order",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "link",
                    "message": "Join link successfully deployed",
                    "name": f"{namespace}.default.repair_type -> base.default.contractor",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "link",
                    "message": "Join link successfully deployed",
                    "name": f"{namespace}.default.contractors -> base.default.us_state",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "link",
                    "message": "Join link successfully deployed",
                    "name": f"{namespace}.default.repair_order -> base.default.dispatcher",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "link",
                    "message": "Join link successfully deployed",
                    "name": f"{namespace}.default.repair_order -> base.default.municipality_dim",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "link",
                    "message": "Join link successfully deployed",
                    "name": f"{namespace}.default.repair_order -> base.default.hard_hat",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "link",
                    "message": "Join link successfully deployed",
                    "name": f"{namespace}.default.hard_hat -> base.default.us_state",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "link",
                    "message": "Join link successfully deployed",
                    "name": f"{namespace}.default.repair_orders_fact -> base.default.municipality_dim",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "link",
                    "message": "Join link successfully deployed",
                    "name": f"{namespace}.default.repair_orders_fact -> base.default.hard_hat",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "link",
                    "message": "Join link successfully deployed",
                    "name": f"{namespace}.default.repair_orders_fact -> base.default.dispatcher",
                    "status": "success",
                    "operation": "create",
                },
                {
                    "deploy_type": "node",
                    "message": "Created cube (v1.0)",
                    "name": f"{namespace}.default.repairs_cube",
                    "status": "success",
                    "operation": "create",
                },
            ],
        }

        response = await client.get("/nodes?prefix=base")
        data = response.json()
        assert len(data) == len(roads_nodes)

        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace="base", nodes=roads_nodes),
        )
        assert all(res["status"] == "skipped" for res in data["results"])
        assert all(res["operation"] == "noop" for res in data["results"])


@pytest.mark.asyncio
async def test_node_to_spec_source(module__session, module__client_with_roads):
    """
    Test that a source node can be converted to a spec correctly
    """
    repair_orders = await Node.get_by_name(
        module__session,
        "default.repair_orders",
    )
    repair_orders_spec = await repair_orders.to_spec(module__session)
    assert repair_orders_spec == SourceSpec(
        name="default.repair_orders",
        node_type=NodeType.SOURCE,
        owners=["dj"],
        display_name="default.roads.repair_orders",
        description="All repair orders",
        tags=[],
        mode=NodeMode.PUBLISHED,
        custom_metadata={},
        columns=[
            ColumnSpec(
                name="repair_order_id",
                type="int",
                display_name="Repair Order Id",
                description=None,
            ),
            ColumnSpec(
                name="municipality_id",
                type="string",
                display_name="Municipality Id",
                description=None,
            ),
            ColumnSpec(
                name="hard_hat_id",
                type="int",
                display_name="Hard Hat Id",
                description=None,
            ),
            ColumnSpec(
                name="order_date",
                type="timestamp",
                display_name="Order Date",
                description=None,
            ),
            ColumnSpec(
                name="required_date",
                type="timestamp",
                display_name="Required Date",
                description=None,
            ),
            ColumnSpec(
                name="dispatched_date",
                type="timestamp",
                display_name="Dispatched Date",
                description=None,
            ),
            ColumnSpec(
                name="dispatcher_id",
                type="int",
                display_name="Dispatcher Id",
                description=None,
            ),
        ],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="default.repair_order",
                join_type=JoinType.INNER,
                join_on="default.repair_orders.repair_order_id = default.repair_order.repair_order_id",
            ),
            DimensionJoinLinkSpec(
                dimension_node="default.dispatcher",
                join_type=JoinType.INNER,
                join_on="default.repair_orders.dispatcher_id = default.dispatcher.dispatcher_id",
            ),
        ],
        primary_key=[],
        table="default.roads.repair_orders",
    )


@pytest.mark.asyncio
async def test_node_to_spec_transform(module__session, module__client_with_roads):
    """
    Test that a transform node can be converted to a spec correctly
    """
    repair_orders_fact = await Node.get_by_name(
        module__session,
        "default.repair_orders_fact",
    )
    repair_orders_fact_spec = await repair_orders_fact.to_spec(module__session)
    assert repair_orders_fact_spec == TransformSpec(
        name="default.repair_orders_fact",
        node_type=NodeType.TRANSFORM,
        owners=["dj"],
        display_name="Repair Orders Fact",
        description="Fact transform with all details on repair orders",
        tags=[],
        mode=NodeMode.PUBLISHED,
        custom_metadata={"foo": "bar"},
        columns=[
            ColumnSpec(
                name="repair_order_id",
                type="int",
                display_name="Repair Order Id",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="municipality_id",
                type="string",
                display_name="Municipality Id",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="hard_hat_id",
                type="int",
                display_name="Hard Hat Id",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="dispatcher_id",
                type="int",
                display_name="Dispatcher Id",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="order_date",
                type="timestamp",
                display_name="Order Date",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="dispatched_date",
                type="timestamp",
                display_name="Dispatched Date",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="required_date",
                type="timestamp",
                display_name="Required Date",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="discount",
                type="float",
                display_name="Discount",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="price",
                type="float",
                display_name="Price",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="quantity",
                type="int",
                display_name="Quantity",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="repair_type_id",
                type="int",
                display_name="Repair Type Id",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="total_repair_cost",
                type="float",
                display_name="Total Repair Cost",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="time_to_dispatch",
                type="timestamp",
                display_name="Time To Dispatch",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="dispatch_delay",
                type="timestamp",
                display_name="Dispatch Delay",
                description=None,
                attributes=[],
                partition=None,
            ),
        ],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="default.municipality_dim",
                join_type=JoinType.INNER,
                join_on="default.repair_orders_fact.municipality_id = default.municipality_dim.municipality_id",
            ),
            DimensionJoinLinkSpec(
                dimension_node="default.hard_hat",
                join_type=JoinType.INNER,
                join_on="default.repair_orders_fact.hard_hat_id = default.hard_hat.hard_hat_id",
            ),
            DimensionJoinLinkSpec(
                dimension_node="default.hard_hat_to_delete",
                join_type=JoinType.LEFT,
                join_on="default.repair_orders_fact.hard_hat_id = default.hard_hat_to_delete.hard_hat_id",
            ),
            DimensionJoinLinkSpec(
                dimension_node="default.dispatcher",
                join_type=JoinType.INNER,
                join_on="default.repair_orders_fact.dispatcher_id = default.dispatcher.dispatcher_id",
            ),
        ],
        primary_key=[],
        query=repair_orders_fact.current.query,
    )


@pytest.mark.asyncio
async def test_node_to_spec_dimension(module__session, module__client_with_roads):
    """
    Test that a dimension node can be converted to a spec correctly
    """
    hard_hat = await Node.get_by_name(
        module__session,
        "default.hard_hat",
    )
    hard_hat_spec = await hard_hat.to_spec(module__session)
    assert hard_hat_spec == DimensionSpec(
        name="default.hard_hat",
        node_type=NodeType.DIMENSION,
        owners=["dj"],
        display_name="Hard Hat",
        description="Hard hat dimension",
        tags=[],
        mode=NodeMode.PUBLISHED,
        columns=[
            ColumnSpec(
                name="hard_hat_id",
                type="int",
                display_name="Hard Hat Id",
                description=None,
                attributes=["primary_key"],
                partition=None,
            ),
            ColumnSpec(
                name="last_name",
                type="string",
                display_name="Last Name",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="first_name",
                type="string",
                display_name="First Name",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="title",
                type="string",
                display_name="Title",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="birth_date",
                type="timestamp",
                display_name="Birth Date",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="hire_date",
                type="timestamp",
                display_name="Hire Date",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="address",
                type="string",
                display_name="Address",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="city",
                type="string",
                display_name="City",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="state",
                type="string",
                display_name="State",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="postal_code",
                type="string",
                display_name="Postal Code",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="country",
                type="string",
                display_name="Country",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="manager",
                type="int",
                display_name="Manager",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="contractor_id",
                type="int",
                display_name="Contractor Id",
                description=None,
                attributes=[],
                partition=None,
            ),
        ],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="default.us_state",
                join_type=JoinType.INNER,
                join_on="default.hard_hat.state = default.us_state.state_short",
            ),
        ],
        primary_key=["hard_hat_id"],
        query=hard_hat.current.query,
    )


@pytest.mark.asyncio
async def test_node_to_spec_metric(module__session, module__client_with_roads):
    """
    Test that a metric node can be converted to a spec correctly
    """
    num_repair_orders = await Node.get_by_name(
        module__session,
        "default.num_repair_orders",
    )
    num_repair_orders_spec = await num_repair_orders.to_spec(module__session)
    assert num_repair_orders_spec == MetricSpec(
        name="default.num_repair_orders",
        node_type=NodeType.METRIC,
        owners=["dj"],
        display_name="Num Repair Orders",
        description="Number of repair orders",
        tags=[],
        mode=NodeMode.PUBLISHED,
        custom_metadata={"foo": "bar"},
        query=num_repair_orders.current.query,
        required_dimensions=[],
        direction=MetricDirection.HIGHER_IS_BETTER,
        unit_enum=MetricUnit.DOLLAR,
        significant_digits=None,
        min_decimal_exponent=None,
        max_decimal_exponent=None,
    )


def test_node_spec_equality():
    """
    Test that two node specs are equal.
    """
    namespace = "base"
    orig_spec = DimensionSpec(
        namespace="base",
        name="hard_hat",
        description="Hard hat dimension",
        query="""SELECT
    hard_hat_id,
    last_name,
    first_name
FROM ${prefix}default.hard_hats""",
        primary_key=["hard_hat_id"],
        owners=["dj"],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="default.us_state",
                join_type=JoinType.INNER,
                join_on="${prefix}default.hard_hat.state = ${prefix}default.us_state.state_short",
            ),
        ],
    )
    spec_with_same_query = DimensionSpec(
        namespace="base",
        name="hard_hat",
        description="Hard hat dimension",
        query="""SELECT hard_hat_id, last_name, first_name FROM ${prefix}default.hard_hats""",
        primary_key=["hard_hat_id"],
        owners=["dj"],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="default.us_state",
                join_type=JoinType.INNER,
                join_on="${prefix}default.hard_hat.state = ${prefix}default.us_state.state_short",
            ),
        ],
    )
    assert orig_spec == spec_with_same_query

    spec_with_diff_namespace = DimensionSpec(
        name=f"{namespace}.hard_hat",
        description="Hard hat dimension",
        query="""SELECT hard_hat_id, last_name, first_name FROM base.default.hard_hats""",
        primary_key=["hard_hat_id"],
        owners=["dj"],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="default.us_state",
                join_type=JoinType.INNER,
                join_on=f"{namespace}.default.hard_hat.state = base.default.us_state.state_short",
            ),
        ],
    )
    assert orig_spec == spec_with_diff_namespace


@pytest.mark.asyncio
async def test_safe_task_success():
    async def dummy_coro():
        await asyncio.sleep(0.1)
        return DeploymentResult(
            deploy_type=DeploymentResult.Type.NODE,
            name="node1",
            status=DeploymentResult.Status.SUCCESS,
            operation=DeploymentResult.Operation.CREATE,
            message="ok",
        )

    semaphore = asyncio.Semaphore(1)
    result = await safe_task(
        name="node1",
        deploy_type=DeploymentResult.Type.NODE,
        coroutine=dummy_coro(),
        semaphore=semaphore,
        timeout=1,
    )
    assert result.status == DeploymentResult.Status.SUCCESS
    assert result.name == "node1"


@pytest.mark.asyncio
async def test_safe_task_timeout():
    async def slow_coro():
        await asyncio.sleep(2)
        return DeploymentResult(
            deploy_type=DeploymentResult.Type.NODE,
            name="node1",
            status=DeploymentResult.Status.SUCCESS,
            operation=DeploymentResult.Operation.CREATE,
            message="ok",
        )

    semaphore = asyncio.Semaphore(1)
    result = await safe_task(
        name="node1",
        deploy_type=DeploymentResult.Type.NODE,
        coroutine=slow_coro(),
        semaphore=semaphore,
        timeout=0.1,
    )
    assert result.status == DeploymentResult.Status.FAILED
    assert "timed out" in result.message


@pytest.mark.asyncio
async def test_safe_task_semaphore_limit():
    results = []
    semaphore = asyncio.Semaphore(2)

    async def dummy_coro(i):
        await asyncio.sleep(0.1)
        return DeploymentResult(
            deploy_type=DeploymentResult.Type.NODE,
            name=f"node{i}",
            status=DeploymentResult.Status.SUCCESS,
            operation=DeploymentResult.Operation.CREATE,
            message="ok",
        )

    tasks = [
        safe_task(
            name=f"node{i}",
            deploy_type=DeploymentResult.Type.NODE,
            coroutine=dummy_coro(i),
            semaphore=semaphore,
            timeout=1,
        )
        for i in range(4)
    ]
    results = await asyncio.gather(*tasks)

    assert all(r.status == DeploymentResult.Status.SUCCESS for r in results)
    assert {r.name for r in results} == {"node0", "node1", "node2", "node3"}


@pytest.mark.asyncio
async def test_safe_task_other_failure():
    async def exception_coro():
        await asyncio.sleep(0.1)
        raise DJException("Something went wrong")

    semaphore = asyncio.Semaphore(1)
    result = await safe_task(
        name="node1",
        deploy_type=DeploymentResult.Type.NODE,
        coroutine=exception_coro(),
        semaphore=semaphore,
        timeout=20,
    )
    assert result.status == DeploymentResult.Status.FAILED
    assert "Something went wrong" in result.message


@pytest.mark.asyncio
@pytest.mark.skip(reason="For debugging with full roads spec")
async def test_print_roads_spec(roads_nodes):
    spec = DeploymentSpec(
        namespace="roads",
        nodes=roads_nodes,
    )
    print("Roads Spec:", json.dumps(spec.dict()))
    assert 1 == 2
