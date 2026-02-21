import asyncio
import json
from unittest import mock
import uuid
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

from datajunction_server.models.deployment import (
    ColumnSpec,
    DeploymentSpec,
    DeploymentStatus,
    DimensionReferenceLinkSpec,
    TransformSpec,
    SourceSpec,
    MetricSpec,
    DimensionSpec,
    CubeSpec,
    DimensionJoinLinkSpec,
    GitDeploymentSource,
    LocalDeploymentSource,
)
from datajunction_server.internal.git.github_service import GitHubServiceError
from datajunction_server.api.deployments import InProcessExecutor
from datajunction_server.models.dimensionlink import JoinType
from datajunction_server.database.node import Node
from datajunction_server.database.tag import Tag
from datajunction_server.models.node import (
    MetricDirection,
    MetricUnit,
    NodeMode,
    NodeType,
)
import pytest


@pytest.fixture(autouse=True, scope="module")
def patch_effective_writer_concurrency():
    from datajunction_server.internal.deployment.deployment import settings

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
        catalog="default",
        schema="roads",
        table="repair_orders",
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
        catalog="default",
        schema="roads",
        table="repair_orders_view",
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
        catalog="default",
        schema="roads",
        table="repair_order_details",
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
        catalog="default",
        schema="roads",
        table="repair_type",
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
        catalog="default",
        schema="roads",
        table="contractors",
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
        catalog="default",
        schema="roads",
        table="municipality_municipality_type",
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
        catalog="default",
        schema="roads",
        table="municipality_type",
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
        catalog="default",
        schema="roads",
        table="municipality",
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
        catalog="default",
        schema="roads",
        table="dispatchers",
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
        catalog="default",
        schema="roads",
        table="hard_hats",
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
        catalog="default",
        schema="roads",
        table="hard_hat_state",
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
        catalog="default",
        schema="roads",
        table="us_states",
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
        catalog="default",
        schema="roads",
        table="us_region",
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
        display_name="US State",
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
        required_dimensions=["hard_hat_id"],
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
        direction=MetricDirection.HIGHER_IS_BETTER,
        unit=MetricUnit.PROPORTION,
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
        json=deployment_spec.model_dump(),
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
            "created_at": None,
            "created_by": None,
            "source": None,
        }

    @pytest.mark.asyncio
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
            "created_at": None,
            "created_by": None,
            "source": None,
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
                    "message": "Some columns in the primary key ['hard_hat_id'] were not found in "
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
            "created_at": None,
            "created_by": None,
            "source": None,
        }

    @pytest.mark.asyncio
    async def test_deploy_with_dimension_link_removal(
        self,
        session,
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
        hard_hat = await Node.get_by_name(session, "link_removal.default.hard_hat")
        assert len(hard_hat.current.dimension_links) == 1

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
    async def test_deploy_with_dimension_link_update(
        self,
        session,
        client,
        default_hard_hats,
        default_us_states,
        default_us_state,
    ):
        """
        Test that updating a dimension link from a node works as expected
        """
        namespace = "link_update"
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
        hard_hat = await Node.get_by_name(session, "link_update.default.hard_hat")
        assert len(hard_hat.current.dimension_links) == 1

        # Update the dimension link and redeploy
        dim_spec.dimension_links = [
            DimensionJoinLinkSpec(
                dimension_node="${prefix}default.us_state",
                join_type="left",
                join_on="${prefix}default.hard_hat.state = ${prefix}default.us_state.state_short",
            ),
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"
        assert data["results"][-1] == {
            "deploy_type": "link",
            "message": "Join link successfully deployed",
            "name": "link_update.default.hard_hat -> link_update.default.us_state",
            "operation": "update",
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
        dim_spec.dimension_links = [
            DimensionReferenceLinkSpec(
                node_column="state",
                dimension="${prefix}default.us_state.random",
            ),
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "failed"
        assert (
            "Dimension attribute 'random' not found in dimension"
            in data["results"][-1]["message"]
        )

    @pytest.mark.asyncio
    async def test_deploy_dimension_with_update(
        self,
        client,
        session,
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
            display_name="Hard Hat",
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
        nodes_list = [
            dim_spec,
            default_hard_hats,
            default_us_states,
            default_us_state,
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=nodes_list,
            ),
        )
        assert data["status"] == "success"
        assert len(data["results"]) == 5

        node = await Node.get_by_name(session, f"{namespace}.default.hard_hat")
        assert [col.name for col in node.current.primary_key()] == ["hard_hat_id"]

        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=nodes_list,
            ),
        )
        assert all(res["status"] == "skipped" for res in data["results"])

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
        assert len([res for res in data["results"] if res["status"] == "skipped"]) == 4
        update_hard_hat = next(
            res
            for res in data["results"]
            if res["name"] == "node_update.default.hard_hat"
        )
        assert update_hard_hat == {
            "deploy_type": "node",
            "name": f"{namespace}.default.hard_hat",
            "status": "success",
            "operation": "update",
            "message": "Updated dimension (v2.0)\n└─ Updated dimension_links",
        }
        update_us_state = next(
            res
            for res in data["results"]
            if res["name"] == "node_update.default.us_state"
        )
        assert update_us_state == {
            "deploy_type": "node",
            "message": "Node node_update.default.us_state is unchanged.",
            "name": "node_update.default.us_state",
            "operation": "noop",
            "status": "skipped",
        }

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
        metric_result = next(
            res
            for res in data["results"]
            if res["name"] == "metric_update.default.avg_length_of_employment"
        )
        assert metric_result == {
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
        metric_result = next(
            res
            for res in data["results"]
            if res["name"] == "metric_update.default.avg_length_of_employment"
        )
        assert metric_result == {
            "deploy_type": "node",
            "message": "Updated metric (v2.0)\n└─ Updated display_name",
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
                    "message": "Dimension link from bad_node_spec_links.default.hard_hat to "
                    "bad_node_spec_links.default.us_state is invalid: Join query "
                    "bad_node_spec_links.default.hard_hat.state = "
                    "bad_node_spec_links.default.us_state.state_short is not valid\n"
                    "The following error happened:\n"
                    f"- Column `{namespace}.default.hard_hat.state` does not exist on "
                    "any valid table. (error code: 206)",
                    "name": f"{namespace}.default.hard_hat -> {namespace}.default.us_state",
                    "status": "failed",
                    "operation": "create",
                },
            ],
            "created_at": None,
            "created_by": None,
            "source": None,
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
            "created_at": mock.ANY,
            "created_by": mock.ANY,
            "source": mock.ANY,
        }

        # Re-deploying the same setup should be a noop
        data = await deploy_and_wait(client, mini_setup)
        assert all(res["status"] == "skipped" for res in data["results"])
        assert all(res["operation"] == "noop" for res in data["results"])

    @pytest.mark.asyncio
    async def test_deploy_node_delete(
        self,
        client,
        default_hard_hats,
    ):
        """
        Test that removing a node from the deployment spec will result in deletion
        """
        namespace = "node_update"
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[default_hard_hats],
            ),
        )
        assert data["status"] == "success"
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=[]),
        )
        assert data["results"][-1] == {
            "deploy_type": "node",
            "message": "Node node_update.default.hard_hats has been removed.",
            "name": "node_update.default.hard_hats",
            "operation": "delete",
            "status": "success",
        }

    @pytest.mark.asyncio
    async def test_deploy_tags(
        self,
        session,
        client,
        current_user,
        default_us_states,
        default_us_state,
    ):
        """
        Test that adding tags to a node in the deployment spec will result in an update
        """
        namespace = "node_update"
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[default_us_states, default_us_state],
            ),
        )
        assert data["status"] == "success"
        default_us_state.tags = ["tag1"]

        tag = Tag(name="tag1", created_by_id=current_user.id, tag_type="default")
        session.add(tag)
        await session.commit()

        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[default_us_states, default_us_state],
            ),
        )
        assert data["results"][-1] == {
            "deploy_type": "node",
            "message": "Updated dimension (v2.0)\n└─ Updated tags",
            "name": "node_update.default.us_state",
            "operation": "update",
            "status": "success",
        }
        node = await Node.get_by_name(session, f"{namespace}.default.us_state")
        assert [tag.name for tag in node.tags] == ["tag1"]

    @pytest.mark.asyncio
    async def test_deploy_column_properties(
        self,
        client,
        default_us_states,
        default_us_state,
    ):
        """
        Test that adding tags to a node in the deployment spec will result in an update
        """
        namespace = "node_update"
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[default_us_states, default_us_state],
            ),
        )
        assert data["status"] == "success"

        # Update display name and description of a column
        default_us_state.columns = [
            ColumnSpec(
                name="state_name",
                type="string",
                display_name="State Name 1122",
                description="State name",
            ),
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[default_us_states, default_us_state],
            ),
        )
        assert data["status"] == "success"
        assert data["results"] == [
            {
                "deploy_type": "node",
                "message": "Node node_update.default.us_states is unchanged.",
                "name": "node_update.default.us_states",
                "operation": "noop",
                "status": "skipped",
            },
            {
                "deploy_type": "node",
                "message": "Updated dimension (v2.0)\n└─ Set properties for 1 columns",
                "name": "node_update.default.us_state",
                "operation": "update",
                "status": "success",
            },
        ]

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
            "created_at": mock.ANY,
            "created_by": mock.ANY,
            "source": mock.ANY,
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
    async def test_deploy_nested_namespace_not_treated_as_missing_dependency(
        self,
        client,
    ):
        """
        Test that nested namespaces don't cause parent namespace to be treated as missing dependency.
        This is a regression test for Issue #1775.

        Scenario:
        - Deploy external dimensions to 'external' namespace
        - Deploy metric to 'analytics' namespace that references external dimensions in query
        - The query parsing extracts column references like 'external.dimension.user_type.user_type_id'
        - And parent path 'external.dimension.user_type'
        - The code should filter out namespace prefix 'external.dimension' at line 1558
        """
        # STEP 1: Deploy external dimensions to 'external' namespace
        external_source = SourceSpec(
            name="source.users",
            description="Users source",
            catalog="default",
            schema="public",
            table="users",
            columns=[
                ColumnSpec(name="user_id", type="int"),
                ColumnSpec(name="user_type_id", type="int"),
                ColumnSpec(name="user_type_name", type="string"),
            ],
            dimension_links=[],
            owners=["dj"],
        )
        external_dimension = DimensionSpec(
            name="dimension.user_type",
            description="User type dimension",
            query="""
                SELECT
                    user_type_id,
                    user_type_name
                FROM ${prefix}source.users
            """,
            primary_key=["user_type_id"],
            owners=["dj"],
        )

        data1 = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace="external",
                nodes=[external_source, external_dimension],
            ),
        )
        assert data1["status"] == "success"

        # STEP 2: Deploy metric and cube that reference external dimensions
        # The metric query references 'external.dimension.user_type.user_type_id'
        # which will extract:
        # - 'external.dimension.user_type.user_type_id' (column ref)
        # - 'external.dimension.user_type' (parent, exists in DB)
        # - possibly 'external.dimension' (namespace prefix, should be filtered at line 1558)
        metric_with_external_ref = MetricSpec(
            name="metric.user_count_by_type",
            description="Count users by type",
            query="SELECT count(*) as cnt FROM external.source.users",
            dimension_links=[],
            owners=["dj"],
        )

        # Cube also references the external dimension
        cube_spec = CubeSpec(
            name="cube.user_analysis",
            description="User analysis cube",
            metrics=["${prefix}metric.user_count_by_type"],
            dimensions=[
                "external.dimension.user_type.user_type_id",
                "external.dimension.user_type.user_type_name",
            ],
            owners=["dj"],
        )

        data2 = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace="analytics",
                nodes=[metric_with_external_ref, cube_spec],
            ),
        )

        # Should succeed, not fail with "missing dependency: external.dimension"
        assert data2["status"] == "success"
        assert data2["namespace"] == "analytics"

        # Verify metric and cube were created
        node_results = [r for r in data2["results"] if r["deploy_type"] == "node"]
        assert len(node_results) == 2
        assert any(
            "metric.user_count_by_type" in r["name"] and r["status"] == "success"
            for r in node_results
        )
        assert any(
            "cube.user_analysis" in r["name"] and r["status"] == "success"
            for r in node_results
        )


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
        catalog="default",
        schema="roads",
        table="repair_orders",
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
async def test_node_to_spec_respects_column_order(
    module__session,
    module__client_with_roads,
):
    """
    Test that to_spec() returns columns sorted by their order field,
    regardless of the order they were loaded from the database.

    This test simulates the bug where selectinload might not respect
    the relationship's order_by, by manually reordering columns before
    calling to_spec().
    """
    # Get a node with columns
    repair_orders_fact = await Node.get_by_name(
        module__session,
        "default.repair_orders_fact",
        options=Node.cube_load_options(),
    )

    # Get the original column order values
    original_columns = repair_orders_fact.current.columns
    column_orders = [(col.name, col.order) for col in original_columns]

    # Manually shuffle columns to simulate selectinload not respecting order_by
    # Reverse the list to put them in wrong order
    repair_orders_fact.current.columns = list(reversed(original_columns))

    # Call to_spec - it should sort them back to correct order
    spec = await repair_orders_fact.to_spec(module__session)

    # Verify columns are in the correct order (by order field, not by list position)
    sorted_column_names = [
        name
        for name, _ in sorted(
            column_orders,
            key=lambda x: x[1] if x[1] is not None else float("inf"),
        )
    ]
    actual_column_names = [col.name for col in spec.columns]

    assert actual_column_names == sorted_column_names, (
        f"Columns not in correct order. "
        f"Expected: {sorted_column_names}, "
        f"Got: {actual_column_names}"
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
@pytest.mark.skip(reason="For debugging with full roads spec")
async def test_print_roads_spec(roads_nodes):
    spec = DeploymentSpec(
        namespace="roads",
        nodes=roads_nodes,
    )
    print("Roads Spec:", json.dumps(spec.model_dump()))
    assert 1 == 2


@pytest.mark.xdist_group(name="deployments")
class TestDeploymentHistoryTracking:
    """Tests for history tracking during YAML deployments"""

    @pytest.mark.asyncio
    async def test_deployment_creates_history_for_nodes(
        self,
        client,
        default_hard_hats,
        default_us_states,
        default_us_state,
    ):
        """
        Test that deploying nodes creates history entries for each create/update operation
        """
        namespace = "history_test"
        dim_spec = DimensionSpec(
            name="default.hard_hat",
            description="""Hard hat dimension""",
            query="""
                SELECT
                    hard_hat_id,
                    last_name,
                    first_name,
                    state
                FROM ${prefix}default.hard_hats
            """,
            primary_key=["hard_hat_id"],
            owners=["dj"],
        )

        # Deploy nodes
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[
                    default_hard_hats,
                    default_us_states,
                    default_us_state,
                    dim_spec,
                ],
            ),
        )
        assert data["status"] == DeploymentStatus.SUCCESS.value

        # Check history for source node
        response = await client.get(
            f"/history/node/{namespace}.default.hard_hats/",
        )
        assert response.status_code == 200
        history = response.json()
        assert len(history) >= 1
        # Find the create event
        create_events = [h for h in history if h["activity_type"] == "create"]
        assert len(create_events) == 1
        assert create_events[0]["entity_type"] == "node"
        assert "deployment_id" in create_events[0]["details"]

        # Check history for dimension node
        response = await client.get(
            f"/history/node/{namespace}.default.hard_hat/",
        )
        assert response.status_code == 200
        history = response.json()
        assert len(history) >= 1
        create_events = [h for h in history if h["activity_type"] == "create"]
        assert len(create_events) == 1
        assert create_events[0]["entity_type"] == "node"
        assert "deployment_id" in create_events[0]["details"]

    @pytest.mark.asyncio
    async def test_deployment_creates_history_for_updates(
        self,
        client,
        default_hard_hats,
    ):
        """
        Test that updating nodes via deployment creates history entries
        """
        namespace = "history_update_test"

        # First deployment - create
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[default_hard_hats],
            ),
        )
        assert data["status"] == DeploymentStatus.SUCCESS.value

        # Second deployment - update description
        updated_hard_hats = SourceSpec(
            name=default_hard_hats.name,
            description="Updated description for hard hats table",
            catalog=default_hard_hats.catalog,
            schema=default_hard_hats.schema_,
            table=default_hard_hats.table,
            columns=default_hard_hats.columns,
            owners=default_hard_hats.owners,
        )
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[updated_hard_hats],
            ),
        )
        assert data["status"] == DeploymentStatus.SUCCESS.value

        # Check history shows both create and update
        response = await client.get(
            f"/history/node/{namespace}.default.hard_hats/",
        )
        assert response.status_code == 200
        history = response.json()
        create_events = [h for h in history if h["activity_type"] == "create"]
        update_events = [h for h in history if h["activity_type"] == "update"]
        assert len(create_events) >= 1
        assert len(update_events) >= 1
        # Verify deployment_id is tracked
        assert "deployment_id" in create_events[0]["details"]
        assert "deployment_id" in update_events[0]["details"]

    @pytest.mark.asyncio
    async def test_deployment_creates_history_for_dimension_links(
        self,
        client,
        default_hard_hats,
        default_us_states,
        default_us_state,
    ):
        """
        Test that deploying nodes with dimension links creates history entries for links
        """
        namespace = "history_link_test"
        dim_spec = DimensionSpec(
            name="default.hard_hat",
            description="""Hard hat dimension""",
            query="""
                SELECT
                    hard_hat_id,
                    last_name,
                    first_name,
                    state
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

        # Deploy nodes with dimension links
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[
                    default_hard_hats,
                    default_us_states,
                    default_us_state,
                    dim_spec,
                ],
            ),
        )
        assert data["status"] == DeploymentStatus.SUCCESS.value

        # Check history for dimension links
        response = await client.get(
            f"/history?node={namespace}.default.hard_hat",
        )
        assert response.status_code == 200
        history = response.json()

        # Should have link creation history
        link_events = [h for h in history if h["entity_type"] == "link"]
        assert len(link_events) >= 1
        link_create_events = [h for h in link_events if h["activity_type"] == "create"]
        assert len(link_create_events) >= 1
        # Verify link details are tracked
        assert "dimension_node" in link_create_events[0]["details"]
        assert "deployment_id" in link_create_events[0]["details"]


@pytest.mark.xdist_group(name="deployments")
class TestDeploymentColumnOrdering:
    """Tests for column ordering in deployments"""

    @pytest.mark.asyncio
    async def test_deployment_preserves_column_order(self, client):
        """
        Test that column order is preserved for both source specs and
        inferred columns from transform queries.
        """
        namespace = "column_order_test"

        # Create a source with columns in non-alphabetical order
        source_spec = SourceSpec(
            name="test_source",
            description="Test source",
            catalog="default",
            schema="test_schema",
            table="test_table",
            columns=[
                ColumnSpec(name="z_column", type="string"),
                ColumnSpec(name="a_column", type="int"),
                ColumnSpec(name="m_column", type="timestamp"),
                ColumnSpec(name="b_column", type="float"),
            ],
        )

        # Create a transform that reorders the columns
        transform_spec = TransformSpec(
            name="test_transform",
            description="Test transform for column ordering",
            query="""
                SELECT
                    m_column,
                    b_column,
                    z_column,
                    a_column
                FROM ${prefix}test_source
            """,
        )

        # Deploy both nodes
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[source_spec, transform_spec],
            ),
        )
        assert data["status"] == DeploymentStatus.SUCCESS.value

        # Verify source column order is preserved
        response = await client.get(f"/nodes/{namespace}.test_source/")
        assert response.status_code == 200
        source_columns = [col["name"] for col in response.json()["columns"]]
        assert source_columns == ["z_column", "a_column", "m_column", "b_column"]

        # Verify transform column order matches the SELECT projection
        response = await client.get(f"/nodes/{namespace}.test_transform/")
        assert response.status_code == 200
        transform_columns = [col["name"] for col in response.json()["columns"]]
        assert transform_columns == ["m_column", "b_column", "z_column", "a_column"]


@pytest.mark.xdist_group(name="deployments")
class TestGitOnlyNamespaceDeployments:
    """Tests for git_only namespace deployment verification."""

    @pytest.mark.asyncio
    async def test_git_only_deployment_no_source(self, client):
        """Test that git_only namespace rejects deployment without source."""
        root_namespace = "git_only_no_source_root"
        namespace = "git_only_no_source"

        # Create git root namespace
        await client.post(f"/namespaces/{root_namespace}")
        await client.patch(
            f"/namespaces/{root_namespace}/git",
            json={
                "github_repo_path": "myorg/myrepo",
            },
        )

        # Create branch namespace and set git_only=True
        await client.post(f"/namespaces/{namespace}")
        await client.patch(
            f"/namespaces/{namespace}/git",
            json={
                "parent_namespace": root_namespace,
                "git_branch": "main",
                "git_only": True,
            },
        )

        # Try to deploy without source
        source_spec = SourceSpec(
            name="test_source",
            description="Test source",
            catalog="default",
            schema="test",
            table="test_table",
            columns=[ColumnSpec(name="id", type="int")],
        )

        response = await client.post(
            "/deployments",
            json=DeploymentSpec(
                namespace=namespace,
                nodes=[source_spec],
                # No source field
            ).model_dump(),
        )
        assert response.status_code == 422
        assert "git-only" in response.json()["message"]
        assert "must include a git source" in response.json()["message"]

    @pytest.mark.asyncio
    async def test_git_only_deployment_wrong_source_type(self, client):
        """Test that git_only namespace rejects deployment with local source type."""
        root_namespace = "git_only_wrong_type_root"
        namespace = "git_only_wrong_type"

        # Create git root namespace
        await client.post(f"/namespaces/{root_namespace}")
        await client.patch(
            f"/namespaces/{root_namespace}/git",
            json={
                "github_repo_path": "myorg/myrepo",
            },
        )

        # Create branch namespace and set git_only=True
        await client.post(f"/namespaces/{namespace}")
        await client.patch(
            f"/namespaces/{namespace}/git",
            json={
                "parent_namespace": root_namespace,
                "git_branch": "main",
                "git_only": True,
            },
        )

        source_spec = SourceSpec(
            name="test_source",
            description="Test source",
            catalog="default",
            schema="test",
            table="test_table",
            columns=[ColumnSpec(name="id", type="int")],
        )

        response = await client.post(
            "/deployments",
            json=DeploymentSpec(
                namespace=namespace,
                nodes=[source_spec],
                source=LocalDeploymentSource(
                    hostname="localhost",
                    reason="testing",
                ),
            ).model_dump(),
        )
        assert response.status_code == 422
        assert "git-only" in response.json()["message"]
        assert "source.type='git'" in response.json()["message"]

    @pytest.mark.asyncio
    async def test_git_only_deployment_no_commit_sha(self, client):
        """Test that git_only namespace rejects deployment without commit_sha."""
        root_namespace = "git_only_no_sha_root"
        namespace = "git_only_no_sha"

        # Create git root namespace
        await client.post(f"/namespaces/{root_namespace}")
        await client.patch(
            f"/namespaces/{root_namespace}/git",
            json={
                "github_repo_path": "myorg/myrepo",
            },
        )

        # Create branch namespace and set git_only=True
        await client.post(f"/namespaces/{namespace}")
        await client.patch(
            f"/namespaces/{namespace}/git",
            json={
                "parent_namespace": root_namespace,
                "git_branch": "main",
                "git_only": True,
            },
        )

        source_spec = SourceSpec(
            name="test_source",
            description="Test source",
            catalog="default",
            schema="test",
            table="test_table",
            columns=[ColumnSpec(name="id", type="int")],
        )

        response = await client.post(
            "/deployments",
            json=DeploymentSpec(
                namespace=namespace,
                nodes=[source_spec],
                source=GitDeploymentSource(
                    repository="myorg/myrepo",
                    branch="main",
                    # No commit_sha
                ),
            ).model_dump(),
        )
        assert response.status_code == 422
        assert "git-only" in response.json()["message"]
        assert "commit_sha" in response.json()["message"]

    @pytest.mark.asyncio
    async def test_git_only_deployment_invalid_commit(self, client):
        """Test that git_only namespace rejects deployment with invalid commit."""
        root_namespace = "git_only_invalid_commit_root"
        namespace = "git_only_invalid_commit"

        # Create git root namespace
        await client.post(f"/namespaces/{root_namespace}")
        await client.patch(
            f"/namespaces/{root_namespace}/git",
            json={
                "github_repo_path": "myorg/myrepo",
            },
        )

        # Create branch namespace and set git_only=True
        await client.post(f"/namespaces/{namespace}")
        await client.patch(
            f"/namespaces/{namespace}/git",
            json={
                "parent_namespace": root_namespace,
                "git_branch": "main",
                "git_only": True,
            },
        )

        source_spec = SourceSpec(
            name="test_source",
            description="Test source",
            catalog="default",
            schema="test",
            table="test_table",
            columns=[ColumnSpec(name="id", type="int")],
        )

        # Mock GitHubService to return False for verify_commit
        with patch(
            "datajunction_server.api.deployments.GitHubService",
        ) as mock_github_class:
            mock_github = MagicMock()
            mock_github.verify_commit = AsyncMock(return_value=False)
            mock_github_class.return_value = mock_github

            response = await client.post(
                "/deployments",
                json=DeploymentSpec(
                    namespace=namespace,
                    nodes=[source_spec],
                    source=GitDeploymentSource(
                        repository="myorg/myrepo",
                        branch="main",
                        commit_sha="nonexistent123",
                    ),
                ).model_dump(),
            )

            assert response.status_code == 422
            assert "not found" in response.json()["message"]
            mock_github.verify_commit.assert_called_once_with(
                repo_path="myorg/myrepo",
                commit_sha="nonexistent123",
            )

    @pytest.mark.asyncio
    async def test_git_only_deployment_github_error(self, client):
        """Test that git_only namespace handles GitHub API errors gracefully."""
        root_namespace = "git_only_github_error_root"
        namespace = "git_only_github_error"

        # Create git root namespace
        await client.post(f"/namespaces/{root_namespace}")
        await client.patch(
            f"/namespaces/{root_namespace}/git",
            json={
                "github_repo_path": "myorg/myrepo",
            },
        )

        # Create branch namespace and set git_only=True
        await client.post(f"/namespaces/{namespace}")
        await client.patch(
            f"/namespaces/{namespace}/git",
            json={
                "parent_namespace": root_namespace,
                "git_branch": "main",
                "git_only": True,
            },
        )

        source_spec = SourceSpec(
            name="test_source",
            description="Test source",
            catalog="default",
            schema="test",
            table="test_table",
            columns=[ColumnSpec(name="id", type="int")],
        )

        # Mock GitHubService.verify_commit to raise GitHubServiceError
        with patch(
            "datajunction_server.api.deployments.GitHubService",
        ) as mock_github_class:
            mock_github = MagicMock()
            mock_github.verify_commit = AsyncMock(
                side_effect=GitHubServiceError(
                    "API rate limit exceeded",
                    http_status_code=500,
                    github_status=403,
                ),
            )
            mock_github_class.return_value = mock_github

            response = await client.post(
                "/deployments",
                json=DeploymentSpec(
                    namespace=namespace,
                    nodes=[source_spec],
                    source=GitDeploymentSource(
                        repository="myorg/myrepo",
                        branch="main",
                        commit_sha="abc123def456",
                    ),
                ).model_dump(),
            )

            assert response.status_code == 422
            assert (
                response.json()["message"]
                == "Failed to verify commit: API rate limit exceeded"
            )
            mock_github.verify_commit.assert_called_once_with(
                repo_path="myorg/myrepo",
                commit_sha="abc123def456",
            )

    @pytest.mark.asyncio
    async def test_git_only_deployment_success(self, client):
        """Test successful deployment to git_only namespace with valid commit."""
        root_namespace = "git_only_success_root"
        namespace = "git_only_success"

        # Create git root namespace
        await client.post(f"/namespaces/{root_namespace}")
        await client.patch(
            f"/namespaces/{root_namespace}/git",
            json={
                "github_repo_path": "myorg/myrepo",
            },
        )

        # Create branch namespace and set git_only=True
        await client.post(f"/namespaces/{namespace}")
        await client.patch(
            f"/namespaces/{namespace}/git",
            json={
                "parent_namespace": root_namespace,
                "git_branch": "main",
                "git_only": True,
            },
        )

        source_spec = SourceSpec(
            name="test_source",
            description="Test source",
            catalog="default",
            schema="test",
            table="test_table",
            columns=[ColumnSpec(name="id", type="int")],
        )

        # Mock GitHubService to return True for verify_commit
        with patch(
            "datajunction_server.api.deployments.GitHubService",
        ) as mock_github_class:
            mock_github = MagicMock()
            mock_github.verify_commit = AsyncMock(return_value=True)
            mock_github_class.return_value = mock_github

            data = await deploy_and_wait(
                client,
                DeploymentSpec(
                    namespace=namespace,
                    nodes=[source_spec],
                    source=GitDeploymentSource(
                        repository="myorg/myrepo",
                        branch="main",
                        commit_sha="valid123abc",
                    ),
                ),
            )

            assert data["status"] == DeploymentStatus.SUCCESS.value
            mock_github.verify_commit.assert_called_once_with(
                repo_path="myorg/myrepo",
                commit_sha="valid123abc",
            )

        # Verify node was created
        response = await client.get(f"/nodes/{namespace}.test_source/")
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_non_git_only_namespace_allows_any_source(self, client):
        """Test that non-git_only namespace accepts deployment without source."""
        namespace = "non_git_only"

        # Create namespace without git_only
        await client.post(f"/namespaces/{namespace}")

        source_spec = SourceSpec(
            name="test_source",
            description="Test source",
            catalog="default",
            schema="test",
            table="test_table",
            columns=[ColumnSpec(name="id", type="int")],
        )

        # Deploy without source - should succeed
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[source_spec],
                # No source field
            ),
        )

        assert data["status"] == DeploymentStatus.SUCCESS.value


@pytest.mark.xdist_group(name="deployments")
class TestDeploymentStatusUpdate:
    """Tests for deployment status update edge cases."""

    @pytest.mark.asyncio
    async def test_update_status_nonexistent_deployment(self, session):
        """Test that update_status handles non-existent deployment gracefully."""

        @asynccontextmanager
        async def mock_session_context():
            yield session

        with patch(
            "datajunction_server.api.deployments.session_context",
            mock_session_context,
        ):
            # This should not raise - just return silently
            await InProcessExecutor.update_status(
                deployment_uuid=str(uuid.uuid4()),
                status=DeploymentStatus.FAILED,
                results=None,
            )
        # If we get here without exception, the test passes
