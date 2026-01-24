"""
Post requests for all example entities
"""

from datajunction_server.database.column import Column
from datajunction_server.models.query import QueryWithResults
from datajunction_server.sql.parsing.types import (
    BinaryType,
    BooleanType,
    IntegerType,
    StringType,
    TimestampType,
    FloatType,
)
from datajunction_server.typing import QueryState

SERVICE_SETUP = (  # type: ignore
    (
        "/catalogs/",
        {"name": "draft"},
    ),
    (
        "/engines/",
        {"name": "spark", "version": "3.1.1", "dialect": "spark"},
    ),
    (
        "/catalogs/default/engines/",
        [{"name": "spark", "version": "3.1.1", "dialect": "spark"}],
    ),
    (
        "/engines/",
        {"name": "druid", "version": "", "dialect": "druid"},
    ),
    (
        "/catalogs/default/engines/",
        [{"name": "druid", "version": "", "dialect": "druid"}],
    ),
    (
        "/catalogs/",
        {"name": "public"},
    ),
    (
        "/catalogs/",
        {"name": "basic"},
    ),
    (
        "/catalogs/basic/engines/",
        [{"name": "spark", "version": "3.1.1", "dialect": "spark"}],
    ),
    (
        "/engines/",
        {"name": "postgres", "version": "15.2"},
    ),
    (
        "/catalogs/public/engines/",
        [{"name": "postgres", "version": "15.2"}],
    ),
    (  # DJ must be primed with a "default" namespace
        "/namespaces/default/",
        {},
    ),
    (
        "/namespaces/basic/",
        {},
    ),
)

ROADS = (  # type: ignore
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "repair_order_id", "type": "int"},
                {"name": "municipality_id", "type": "string"},
                {"name": "hard_hat_id", "type": "int"},
                {"name": "order_date", "type": "timestamp"},
                {"name": "required_date", "type": "timestamp"},
                {"name": "dispatched_date", "type": "timestamp"},
                {"name": "dispatcher_id", "type": "int"},
            ],
            "description": "All repair orders",
            "mode": "published",
            "name": "default.repair_orders",
            "catalog": "default",
            "schema_": "roads",
            "table": "repair_orders",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "repair_order_id", "type": "int"},
                {"name": "municipality_id", "type": "string"},
                {"name": "hard_hat_id", "type": "int"},
                {"name": "order_date", "type": "timestamp"},
                {"name": "required_date", "type": "timestamp"},
                {"name": "dispatched_date", "type": "timestamp"},
                {"name": "dispatcher_id", "type": "int"},
            ],
            "description": "All repair orders (view)",
            "mode": "published",
            "name": "default.repair_orders_view",
            "catalog": "default",
            "schema_": "roads",
            "table": "repair_orders_view",
            "query": "CREATE OR REPLACE VIEW roads.repair_orders_view AS SELECT * FROM roads.repair_orders",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "repair_order_id", "type": "int"},
                {"name": "repair_type_id", "type": "int"},
                {"name": "price", "type": "float"},
                {"name": "quantity", "type": "int"},
                {"name": "discount", "type": "float"},
            ],
            "description": "Details on repair orders",
            "mode": "published",
            "name": "default.repair_order_details",
            "catalog": "default",
            "schema_": "roads",
            "table": "repair_order_details",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "repair_type_id", "type": "int"},
                {"name": "repair_type_name", "type": "string"},
                {"name": "contractor_id", "type": "int"},
            ],
            "description": "Information on types of repairs",
            "mode": "published",
            "name": "default.repair_type",
            "catalog": "default",
            "schema_": "roads",
            "table": "repair_type",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "contractor_id", "type": "int"},
                {"name": "company_name", "type": "string"},
                {"name": "contact_name", "type": "string"},
                {"name": "contact_title", "type": "string"},
                {"name": "address", "type": "string"},
                {"name": "city", "type": "string"},
                {"name": "state", "type": "string"},
                {"name": "postal_code", "type": "string"},
                {"name": "country", "type": "string"},
                {"name": "phone", "type": "string"},
            ],
            "description": "Information on contractors",
            "mode": "published",
            "name": "default.contractors",
            "catalog": "default",
            "schema_": "roads",
            "table": "contractors",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "municipality_id", "type": "string"},
                {"name": "municipality_type_id", "type": "string"},
            ],
            "description": "Lookup table for municipality and municipality types",
            "mode": "published",
            "name": "default.municipality_municipality_type",
            "catalog": "default",
            "schema_": "roads",
            "table": "municipality_municipality_type",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "municipality_type_id", "type": "string"},
                {"name": "municipality_type_desc", "type": "string"},
            ],
            "description": "Information on municipality types",
            "mode": "published",
            "name": "default.municipality_type",
            "catalog": "default",
            "schema_": "roads",
            "table": "municipality_type",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "municipality_id", "type": "string"},
                {"name": "contact_name", "type": "string"},
                {"name": "contact_title", "type": "string"},
                {"name": "local_region", "type": "string"},
                {"name": "phone", "type": "string"},
                {"name": "state_id", "type": "int"},
            ],
            "description": "Information on municipalities",
            "mode": "published",
            "name": "default.municipality",
            "catalog": "default",
            "schema_": "roads",
            "table": "municipality",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "dispatcher_id", "type": "int"},
                {"name": "company_name", "type": "string"},
                {"name": "phone", "type": "string"},
            ],
            "description": "Information on dispatchers",
            "mode": "published",
            "name": "default.dispatchers",
            "catalog": "default",
            "schema_": "roads",
            "table": "dispatchers",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "hard_hat_id", "type": "int"},
                {"name": "last_name", "type": "string"},
                {"name": "first_name", "type": "string"},
                {"name": "title", "type": "string"},
                {"name": "birth_date", "type": "timestamp"},
                {"name": "hire_date", "type": "timestamp"},
                {"name": "address", "type": "string"},
                {"name": "city", "type": "string"},
                {"name": "state", "type": "string"},
                {"name": "postal_code", "type": "string"},
                {"name": "country", "type": "string"},
                {"name": "manager", "type": "int"},
                {"name": "contractor_id", "type": "int"},
            ],
            "description": "Information on employees",
            "mode": "published",
            "name": "default.hard_hats",
            "catalog": "default",
            "schema_": "roads",
            "table": "hard_hats",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "hard_hat_id", "type": "int"},
                {"name": "state_id", "type": "string"},
            ],
            "description": "Lookup table for employee's current state",
            "mode": "published",
            "name": "default.hard_hat_state",
            "catalog": "default",
            "schema_": "roads",
            "table": "hard_hat_state",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "state_id", "type": "int"},
                {"name": "state_name", "type": "string"},
                {"name": "state_abbr", "type": "string"},
                {"name": "state_region", "type": "int"},
            ],
            "description": "Information on different types of repairs",
            "mode": "published",
            "name": "default.us_states",
            "catalog": "default",
            "schema_": "roads",
            "table": "us_states",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "us_region_id", "type": "int"},
                {"name": "us_region_description", "type": "string"},
            ],
            "description": "Information on US regions",
            "mode": "published",
            "name": "default.us_region",
            "catalog": "default",
            "schema_": "roads",
            "table": "us_region",
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "Repair order dimension",
            "query": """
                        SELECT
                        repair_order_id,
                        municipality_id,
                        hard_hat_id,
                        order_date,
                        required_date,
                        dispatched_date,
                        dispatcher_id
                        FROM default.repair_orders
                    """,
            "mode": "published",
            "name": "default.repair_order",
            "primary_key": ["repair_order_id"],
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "Contractor dimension",
            "query": """
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
                        FROM default.contractors
                    """,
            "mode": "published",
            "name": "default.contractor",
            "primary_key": ["contractor_id"],
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "Hard hat dimension",
            "query": """
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
                        FROM default.hard_hats
                    """,
            "mode": "published",
            "name": "default.hard_hat",
            "primary_key": ["hard_hat_id"],
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "Hard hat dimension #2",
            "query": """
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
                        FROM default.hard_hats
                    """,
            "mode": "published",
            "name": "default.hard_hat_2",
            "primary_key": ["hard_hat_id"],
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "Hard hat dimension (for deletion)",
            "query": """
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
                        FROM default.hard_hats
                    """,
            "mode": "published",
            "name": "default.hard_hat_to_delete",
            "primary_key": ["hard_hat_id"],
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "Hard hat dimension",
            "query": """
                        SELECT
                        hh.hard_hat_id,
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
                        contractor_id,
                        hhs.state_id AS state_id
                        FROM default.hard_hats hh
                        LEFT JOIN default.hard_hat_state hhs
                        ON hh.hard_hat_id = hhs.hard_hat_id
                        WHERE hh.state_id = 'NY'
                    """,
            "mode": "published",
            "name": "default.local_hard_hats",
            "primary_key": ["hard_hat_id"],
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "Hard hat dimension #1",
            "query": """
                        SELECT hh.hard_hat_id
                        FROM default.hard_hats hh
                    """,
            "mode": "published",
            "name": "default.local_hard_hats_1",
            "primary_key": ["hard_hat_id"],
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "Hard hat dimension #2",
            "query": """
                        SELECT hh.hard_hat_id
                        FROM default.hard_hats hh
                    """,
            "mode": "published",
            "name": "default.local_hard_hats_2",
            "primary_key": ["hard_hat_id"],
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "US state dimension",
            "query": """
                        SELECT
                        state_id,
                        state_name,
                        state_abbr AS state_short,
                        state_region
                        FROM default.us_states s
                    """,
            "mode": "published",
            "name": "default.us_state",
            "primary_key": ["state_short"],
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "Dispatcher dimension",
            "query": """
                        SELECT
                        dispatcher_id,
                        company_name,
                        phone
                        FROM default.dispatchers
                    """,
            "mode": "published",
            "name": "default.dispatcher",
            "primary_key": ["dispatcher_id"],
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "Municipality dimension",
            "query": """
                        SELECT
                        m.municipality_id AS municipality_id,
                        contact_name,
                        contact_title,
                        local_region,
                        state_id,
                        mmt.municipality_type_id AS municipality_type_id,
                        mt.municipality_type_desc AS municipality_type_desc
                        FROM default.municipality AS m
                        LEFT JOIN default.municipality_municipality_type AS mmt
                        ON m.municipality_id = mmt.municipality_id
                        LEFT JOIN default.municipality_type AS mt
                        ON mmt.municipality_type_id = mt.municipality_type_desc
                    """,
            "mode": "published",
            "name": "default.municipality_dim",
            "primary_key": ["municipality_id"],
        },
    ),
    (
        "/nodes/transform/",
        {
            "name": "default.regional_level_agg",
            "description": "Regional-level aggregates",
            "mode": "published",
            "primary_key": [
                "us_region_id",
                "state_name",
                "order_year",
                "order_month",
                "order_day",
            ],
            "query": """
WITH ro as (SELECT
        repair_order_id,
        municipality_id,
        hard_hat_id,
        order_date,
        required_date,
        dispatched_date,
        dispatcher_id
    FROM default.repair_orders)
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
    default.municipality m ON ro.municipality_id = m.municipality_id
JOIN
    default.us_states us ON m.state_id = us.state_id
                         AND AVG(rd.price * rd.quantity) >
                            (SELECT AVG(price * quantity) FROM default.repair_order_details WHERE repair_order_id = ro.repair_order_id)
JOIN
    default.us_states us ON m.state_id = us.state_id
JOIN
    default.us_region usr ON us.state_region = usr.us_region_id
JOIN
    default.repair_order_details rd ON ro.repair_order_id = rd.repair_order_id
JOIN
    default.repair_type rt ON rd.repair_type_id = rt.repair_type_id
JOIN
    default.contractors c ON rt.contractor_id = c.contractor_id
GROUP BY
    usr.us_region_id,
    EXTRACT(YEAR FROM ro.order_date),
    EXTRACT(MONTH FROM ro.order_date),
    EXTRACT(DAY FROM ro.order_date)""",
        },
    ),
    (
        "/nodes/transform/",
        {
            "description": "National level aggregates",
            "name": "default.national_level_agg",
            "mode": "published",
            "query": "SELECT SUM(rd.price * rd.quantity) AS total_amount_nationwide FROM default.repair_order_details rd",
        },
    ),
    (
        "/nodes/transform/",
        {
            "description": "Fact transform with all details on repair orders",
            "name": "default.repair_orders_fact",
            "display_name": "Repair Orders Fact",
            "mode": "published",
            "custom_metadata": {"foo": "bar"},
            "query": """SELECT
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
  default.repair_orders repair_orders
JOIN
  default.repair_order_details repair_order_details
ON repair_orders.repair_order_id = repair_order_details.repair_order_id""",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": """For each US region (as defined in the us_region table), we want to calculate:
            Regional Repair Efficiency = (Number of Completed Repairs / Total Repairs Dispatched) ×
                                         (Total Repair Amount in Region / Total Repair Amount Nationwide) × 100
            Here:
                A "Completed Repair" is one where the dispatched_date is not null.
                "Total Repair Amount in Region" is the total amount spent on repairs in a given region.
                "Total Repair Amount Nationwide" is the total amount spent on all repairs nationwide.""",
            "name": "default.regional_repair_efficiency",
            "query": """SELECT
    (SUM(rm.completed_repairs) * 1.0 / SUM(rm.total_repairs_dispatched)) *
    (SUM(rm.total_amount_in_region) * 1.0 / SUM(na.total_amount_nationwide)) * 100
FROM
    default.regional_level_agg rm
CROSS JOIN
    default.national_level_agg na""",
            "mode": "published",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Number of repair orders",
            "query": ("SELECT count(repair_order_id) FROM default.repair_orders_fact"),
            "mode": "published",
            "name": "default.num_repair_orders",
            "metric_metadata": {
                "direction": "higher_is_better",
                "unit": "dollar",
            },
            "custom_metadata": {
                "foo": "bar",
            },
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Average repair price",
            "query": (
                "SELECT avg(repair_orders_fact.price) FROM default.repair_orders_fact repair_orders_fact"
            ),
            "mode": "published",
            "name": "default.avg_repair_price",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Total repair cost",
            "query": "SELECT sum(total_repair_cost) FROM default.repair_orders_fact",
            "mode": "published",
            "name": "default.total_repair_cost",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Average length of employment",
            "query": (
                "SELECT avg(CAST(NOW() AS DATE) - hire_date) FROM default.hard_hat"
            ),
            "mode": "published",
            "name": "default.avg_length_of_employment",
        },
    ),
    (
        "/nodes/metric/",
        {
            "name": "default.discounted_orders_rate",
            "query": (
                """
                SELECT
                  cast(sum(if(discount > 0.0, 1, 0)) as double) / count(*)
                    AS default_DOT_discounted_orders_rate
                FROM default.repair_orders_fact
                """
            ),
            "mode": "published",
            "description": "Proportion of Discounted Orders",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Total repair order discounts",
            "query": ("SELECT sum(price * discount) FROM default.repair_orders_fact"),
            "mode": "published",
            "name": "default.total_repair_order_discounts",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Average repair order discounts",
            "query": ("SELECT avg(price * discount) FROM default.repair_orders_fact"),
            "mode": "published",
            "name": "default.avg_repair_order_discounts",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Average time to dispatch a repair order",
            "query": (
                "SELECT avg(cast(repair_orders_fact.time_to_dispatch as int)) "
                "FROM default.repair_orders_fact repair_orders_fact"
            ),
            "mode": "published",
            "display_name": "Avg Time To Dispatch",
            "name": "default.avg_time_to_dispatch",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Approximate number of unique hard hats (technicians) who worked on repair orders",
            "query": (
                "SELECT APPROX_COUNT_DISTINCT(hard_hat_id) "
                "FROM default.repair_orders_fact"
            ),
            "mode": "published",
            "display_name": "Unique Hard Hats (Approx)",
            "name": "default.num_unique_hard_hats_approx",
        },
    ),
    (
        ("/nodes/default.repair_orders_fact/link"),
        {
            "dimension_node": "default.municipality_dim",
            "join_type": "inner",
            "join_on": (
                "default.repair_orders_fact.municipality_id = default.municipality_dim.municipality_id"
            ),
        },
    ),
    (
        "/nodes/default.repair_orders_fact/link",
        {
            "dimension_node": "default.hard_hat",
            "join_type": "inner",
            "join_on": (
                "default.repair_orders_fact.hard_hat_id = default.hard_hat.hard_hat_id"
            ),
        },
    ),
    (
        "/nodes/default.repair_orders_fact/link",
        {
            "dimension_node": "default.hard_hat_to_delete",
            "join_type": "left",
            "join_on": (
                "default.repair_orders_fact.hard_hat_id = default.hard_hat_to_delete.hard_hat_id"
            ),
        },
    ),
    (
        "/nodes/default.repair_orders_fact/link",
        {
            "dimension_node": "default.dispatcher",
            "join_type": "inner",
            "join_on": (
                "default.repair_orders_fact.dispatcher_id = default.dispatcher.dispatcher_id"
            ),
        },
    ),
    (
        "/nodes/default.repair_order_details/link",
        {
            "dimension_node": "default.repair_order",
            "join_type": "inner",
            "join_on": (
                "default.repair_order_details.repair_order_id = default.repair_order.repair_order_id"
            ),
        },
    ),
    (
        "/nodes/default.repair_type/link",
        {
            "dimension_node": "default.contractor",
            "join_type": "inner",
            "join_on": (
                "default.repair_type.contractor_id = default.contractor.contractor_id"
            ),
        },
    ),
    (
        "/nodes/default.repair_orders/link",
        {
            "dimension_node": "default.repair_order",
            "join_type": "inner",
            "join_on": (
                "default.repair_orders.repair_order_id = default.repair_order.repair_order_id"
            ),
        },
    ),
    (
        "/nodes/default.repair_orders/link",
        {
            "dimension_node": "default.dispatcher",
            "join_type": "inner",
            "join_on": (
                "default.repair_orders.dispatcher_id = default.dispatcher.dispatcher_id"
            ),
        },
    ),
    (
        "/nodes/default.contractors/link",
        {
            "dimension_node": "default.us_state",
            "join_type": "inner",
            "join_on": ("default.contractors.state = default.us_state.state_short"),
        },
    ),
    (
        "/nodes/default.hard_hat/link",
        {
            "dimension_node": "default.us_state",
            "join_type": "inner",
            "join_on": ("default.hard_hat.state = default.us_state.state_short"),
        },
    ),
    (
        "/nodes/default.repair_order_details/link",
        {
            "dimension_node": "default.repair_order",
            "join_type": "inner",
            "join_on": (
                "default.repair_order_details.repair_order_id = default.repair_order.repair_order_id"
            ),
        },
    ),
    (
        "/nodes/default.repair_order/link",
        {
            "dimension_node": "default.dispatcher",
            "join_type": "inner",
            "join_on": (
                "default.repair_order.dispatcher_id = default.dispatcher.dispatcher_id"
            ),
        },
    ),
    (
        "/nodes/default.repair_order/link",
        {
            "dimension_node": "default.hard_hat",
            "join_type": "inner",
            "join_on": (
                "default.repair_order.hard_hat_id = default.hard_hat.hard_hat_id"
            ),
        },
    ),
    (
        "/nodes/default.repair_order/link",
        {
            "dimension_node": "default.hard_hat_to_delete",
            "join_type": "left",
            "join_on": (
                "default.repair_order.hard_hat_id = default.hard_hat_to_delete.hard_hat_id"
            ),
        },
    ),
    (
        "/nodes/default.repair_order/link",
        {
            "dimension_node": "default.municipality_dim",
            "join_type": "inner",
            "join_on": (
                "default.repair_order.municipality_id = default.municipality_dim.municipality_id"
            ),
        },
    ),
)

NAMESPACED_ROADS = (  # type: ignore
    (  # foo.bar Namespaced copy of roads database example
        "/namespaces/foo.bar/",
        {},
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "repair_order_id", "type": "int"},
                {"name": "municipality_id", "type": "string"},
                {"name": "hard_hat_id", "type": "int"},
                {"name": "order_date", "type": "timestamp"},
                {"name": "required_date", "type": "timestamp"},
                {"name": "dispatched_date", "type": "timestamp"},
                {"name": "dispatcher_id", "type": "int"},
            ],
            "description": "All repair orders",
            "mode": "published",
            "name": "foo.bar.repair_orders",
            "catalog": "default",
            "schema_": "roads",
            "table": "repair_orders",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "repair_order_id", "type": "int"},
                {"name": "repair_type_id", "type": "int"},
                {"name": "price", "type": "float"},
                {"name": "quantity", "type": "int"},
                {"name": "discount", "type": "float"},
            ],
            "description": "Details on repair orders",
            "mode": "published",
            "name": "foo.bar.repair_order_details",
            "catalog": "default",
            "schema_": "roads",
            "table": "repair_order_details",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "repair_type_id", "type": "int"},
                {"name": "repair_type_name", "type": "string"},
                {"name": "contractor_id", "type": "int"},
            ],
            "description": "Information on types of repairs",
            "mode": "published",
            "name": "foo.bar.repair_type",
            "catalog": "default",
            "schema_": "roads",
            "table": "repair_type",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "contractor_id", "type": "int"},
                {"name": "company_name", "type": "string"},
                {"name": "contact_name", "type": "string"},
                {"name": "contact_title", "type": "string"},
                {"name": "address", "type": "string"},
                {"name": "city", "type": "string"},
                {"name": "state", "type": "string"},
                {"name": "postal_code", "type": "string"},
                {"name": "country", "type": "string"},
                {"name": "phone", "type": "string"},
            ],
            "description": "Information on contractors",
            "mode": "published",
            "name": "foo.bar.contractors",
            "catalog": "default",
            "schema_": "roads",
            "table": "contractors",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "municipality_id", "type": "string"},
                {"name": "municipality_type_id", "type": "string"},
            ],
            "description": "Lookup table for municipality and municipality types",
            "mode": "published",
            "name": "foo.bar.municipality_municipality_type",
            "catalog": "default",
            "schema_": "roads",
            "table": "municipality_municipality_type",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "municipality_type_id", "type": "string"},
                {"name": "municipality_type_desc", "type": "string"},
            ],
            "description": "Information on municipality types",
            "mode": "published",
            "name": "foo.bar.municipality_type",
            "catalog": "default",
            "schema_": "roads",
            "table": "municipality_type",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "municipality_id", "type": "string"},
                {"name": "contact_name", "type": "string"},
                {"name": "contact_title", "type": "string"},
                {"name": "local_region", "type": "string"},
                {"name": "phone", "type": "string"},
                {"name": "state_id", "type": "int"},
            ],
            "description": "Information on municipalities",
            "mode": "published",
            "name": "foo.bar.municipality",
            "catalog": "default",
            "schema_": "roads",
            "table": "municipality",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "dispatcher_id", "type": "int"},
                {"name": "company_name", "type": "string"},
                {"name": "phone", "type": "string"},
            ],
            "description": "Information on dispatchers",
            "mode": "published",
            "name": "foo.bar.dispatchers",
            "catalog": "default",
            "schema_": "roads",
            "table": "dispatchers",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "hard_hat_id", "type": "int"},
                {"name": "last_name", "type": "string"},
                {"name": "first_name", "type": "string"},
                {"name": "title", "type": "string"},
                {"name": "birth_date", "type": "timestamp"},
                {"name": "hire_date", "type": "timestamp"},
                {"name": "address", "type": "string"},
                {"name": "city", "type": "string"},
                {"name": "state", "type": "string"},
                {"name": "postal_code", "type": "string"},
                {"name": "country", "type": "string"},
                {"name": "manager", "type": "int"},
                {"name": "contractor_id", "type": "int"},
            ],
            "description": "Information on employees",
            "mode": "published",
            "name": "foo.bar.hard_hats",
            "catalog": "default",
            "schema_": "roads",
            "table": "hard_hats",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "hard_hat_id", "type": "int"},
                {"name": "state_id", "type": "string"},
            ],
            "description": "Lookup table for employee's current state",
            "mode": "published",
            "name": "foo.bar.hard_hat_state",
            "catalog": "default",
            "schema_": "roads",
            "table": "hard_hat_state",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "state_id", "type": "int"},
                {"name": "state_name", "type": "string"},
                {"name": "state_abbr", "type": "string"},
                {"name": "state_region", "type": "int"},
            ],
            "description": "Information on different types of repairs",
            "mode": "published",
            "name": "foo.bar.us_states",
            "catalog": "default",
            "schema_": "roads",
            "table": "us_states",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "us_region_id", "type": "int"},
                {"name": "us_region_description", "type": "string"},
            ],
            "description": "Information on US regions",
            "mode": "published",
            "name": "foo.bar.us_region",
            "catalog": "default",
            "schema_": "roads",
            "table": "us_region",
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "Repair order dimension",
            "query": """
                        SELECT
                        repair_order_id,
                        municipality_id,
                        hard_hat_id,
                        order_date,
                        required_date,
                        dispatched_date,
                        dispatcher_id
                        FROM foo.bar.repair_orders
                    """,
            "mode": "published",
            "name": "foo.bar.repair_order",
            "primary_key": ["repair_order_id"],
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "Contractor dimension",
            "query": """
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
                        FROM foo.bar.contractors
                    """,
            "mode": "published",
            "name": "foo.bar.contractor",
            "primary_key": ["contractor_id"],
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "Hard hat dimension",
            "query": """
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
                        FROM foo.bar.hard_hats
                    """,
            "mode": "published",
            "name": "foo.bar.hard_hat",
            "primary_key": ["hard_hat_id"],
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "Hard hat dimension",
            "query": """
                        SELECT
                        hh.hard_hat_id,
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
                        contractor_id,
                        hhs.state_id AS state_id
                        FROM foo.bar.hard_hats hh
                        LEFT JOIN foo.bar.hard_hat_state hhs
                        ON hh.hard_hat_id = hhs.hard_hat_id
                        WHERE hh.state_id = 'NY'
                    """,
            "mode": "published",
            "name": "foo.bar.local_hard_hats",
            "primary_key": ["hard_hat_id"],
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "US state dimension",
            "query": """
                        SELECT
                        state_id,
                        state_name,
                        state_abbr,
                        state_region,
                        r.us_region_description AS state_region_description
                        FROM foo.bar.us_states s
                        LEFT JOIN foo.bar.us_region r
                        ON s.state_region = r.us_region_id
                    """,
            "mode": "published",
            "name": "foo.bar.us_state",
            "primary_key": ["state_id"],
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "Dispatcher dimension",
            "query": """
                        SELECT
                        dispatcher_id,
                        company_name,
                        phone
                        FROM foo.bar.dispatchers
                    """,
            "mode": "published",
            "name": "foo.bar.dispatcher",
            "primary_key": ["dispatcher_id"],
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "Municipality dimension",
            "query": """
                        SELECT
                        m.municipality_id AS municipality_id,
                        contact_name,
                        contact_title,
                        local_region,
                        state_id,
                        mmt.municipality_type_id AS municipality_type_id,
                        mt.municipality_type_desc AS municipality_type_desc
                        FROM foo.bar.municipality AS m
                        LEFT JOIN foo.bar.municipality_municipality_type AS mmt
                        ON m.municipality_id = mmt.municipality_id
                        LEFT JOIN foo.bar.municipality_type AS mt
                        ON mmt.municipality_type_id = mt.municipality_type_desc
                    """,
            "mode": "published",
            "name": "foo.bar.municipality_dim",
            "primary_key": ["municipality_id"],
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Number of repair orders",
            "query": (
                "SELECT count(repair_order_id) as foo_DOT_bar_DOT_num_repair_orders "
                "FROM foo.bar.repair_orders"
            ),
            "mode": "published",
            "name": "foo.bar.num_repair_orders",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Average repair price",
            "query": "SELECT avg(price) FROM foo.bar.repair_order_details",
            "mode": "published",
            "name": "foo.bar.avg_repair_price",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Total repair cost",
            "query": "SELECT sum(price) FROM foo.bar.repair_order_details",
            "mode": "published",
            "name": "foo.bar.total_repair_cost",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Average length of employment",
            "query": ("SELECT avg(NOW() - hire_date) FROM foo.bar.hard_hats"),
            "mode": "published",
            "name": "foo.bar.avg_length_of_employment",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Total repair order discounts",
            "query": ("SELECT sum(price * discount) FROM foo.bar.repair_order_details"),
            "mode": "published",
            "name": "foo.bar.total_repair_order_discounts",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Total repair order discounts",
            "query": ("SELECT avg(price * discount) FROM foo.bar.repair_order_details"),
            "mode": "published",
            "name": "foo.bar.avg_repair_order_discounts",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Average time to dispatch a repair order",
            "query": (
                "SELECT avg(dispatched_date - order_date) FROM foo.bar.repair_orders"
            ),
            "mode": "published",
            "name": "foo.bar.avg_time_to_dispatch",
        },
    ),
    (
        (
            "/nodes/foo.bar.repair_order_details/columns/repair_order_id/"
            "?dimension=foo.bar.repair_order&dimension_column=repair_order_id"
        ),
        {},
    ),
    (
        (
            "/nodes/foo.bar.repair_type/columns/contractor_id/"
            "?dimension=foo.bar.contractor&dimension_column=contractor_id"
        ),
        {},
    ),
    (
        (
            "/nodes/foo.bar.repair_orders/columns/repair_order_id/"
            "?dimension=foo.bar.repair_order&dimension_column=repair_order_id"
        ),
        {},
    ),
    (
        (
            "/nodes/foo.bar.repair_order_details/columns/repair_order_id/"
            "?dimension=foo.bar.repair_order&dimension_column=repair_order_id"
        ),
        {},
    ),
    (
        (
            "/nodes/foo.bar.repair_order/columns/dispatcher_id/"
            "?dimension=foo.bar.dispatcher&dimension_column=dispatcher_id"
        ),
        {},
    ),
    (
        (
            "/nodes/foo.bar.repair_order/columns/hard_hat_id/"
            "?dimension=foo.bar.hard_hat&dimension_column=hard_hat_id"
        ),
        {},
    ),
    (
        (
            "/nodes/foo.bar.repair_order/columns/municipality_id/"
            "?dimension=foo.bar.municipality_dim&dimension_column=municipality_id"
        ),
        {},
    ),
)

ACCOUNT_REVENUE = (  # type: ignore
    (  # Accounts/Revenue examples begin
        "/nodes/source/",
        {
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "account_type_name", "type": "string"},
                {"name": "account_type_classification", "type": "int"},
                {"name": "preferred_payment_method", "type": "int"},
            ],
            "description": "A source table for account type data",
            "mode": "published",
            "name": "default.account_type_table",
            "catalog": "basic",
            "schema_": "accounting",
            "table": "account_type_table",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "payment_type_name", "type": "string"},
                {"name": "payment_type_classification", "type": "string"},
            ],
            "description": "A source table for different types of payments",
            "mode": "published",
            "name": "default.payment_type_table",
            "catalog": "basic",
            "schema_": "accounting",
            "table": "payment_type_table",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "payment_id", "type": "int"},
                {"name": "payment_amount", "type": "float"},
                {"name": "payment_type", "type": "int"},
                {"name": "customer_id", "type": "int"},
                {"name": "account_type", "type": "string"},
            ],
            "description": "All repair orders",
            "mode": "published",
            "name": "default.revenue",
            "catalog": "basic",
            "schema_": "accounting",
            "table": "revenue",
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "Payment type dimensions",
            "query": (
                "SELECT id, payment_type_name, payment_type_classification "
                "FROM default.payment_type_table"
            ),
            "mode": "published",
            "name": "default.payment_type",
            "primary_key": ["id"],
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "Account type dimension",
            "query": (
                "SELECT id, account_type_name, "
                "account_type_classification FROM "
                "default.account_type_table"
            ),
            "mode": "published",
            "name": "default.account_type",
            "primary_key": ["id"],
        },
    ),
    (
        "/nodes/transform/",
        {
            "query": (
                "SELECT payment_id, payment_amount, customer_id, account_type "
                "FROM default.revenue WHERE payment_amount > 1000000"
            ),
            "description": "Only large revenue payments",
            "mode": "published",
            "name": "default.large_revenue_payments_only",
        },
    ),
    (
        "/nodes/transform/",
        {
            "query": (
                "SELECT payment_id, payment_amount, customer_id, account_type "
                "FROM default.revenue WHERE payment_amount > 1000000"
            ),
            "description": "Only large revenue payments #1",
            "mode": "published",
            "name": "default.large_revenue_payments_only_1",
        },
    ),
    (
        "/nodes/transform/",
        {
            "query": (
                "SELECT payment_id, payment_amount, customer_id, account_type "
                "FROM default.revenue WHERE payment_amount > 1000000"
            ),
            "description": "Only large revenue payments #2",
            "mode": "published",
            "name": "default.large_revenue_payments_only_2",
        },
    ),
    (
        "/nodes/transform/",
        {
            "query": (
                "SELECT payment_id, payment_amount, customer_id, account_type "
                "FROM default.revenue WHERE payment_amount > 1000000"
            ),
            "description": "Only large revenue payments",
            "mode": "published",
            "name": "default.large_revenue_payments_only_custom",
            "custom_metadata": {"foo": "bar"},
        },
    ),
    (
        "/nodes/transform/",
        {
            "query": (
                "SELECT payment_id, payment_amount, customer_id, account_type "
                "FROM default.revenue WHERE "
                "large_revenue_payments_and_business_only > 1000000 "
                "AND account_type='BUSINESS'"
            ),
            "description": "Only large revenue payments from business accounts",
            "mode": "published",
            "name": "default.large_revenue_payments_and_business_only",
        },
    ),
    (
        "/nodes/transform/",
        {
            "query": (
                "SELECT payment_id, payment_amount, customer_id, account_type "
                "FROM default.revenue WHERE "
                "large_revenue_payments_and_business_only > 1000000 "
                "AND account_type='BUSINESS'"
            ),
            "description": "Only large revenue payments from business accounts 1",
            "mode": "published",
            "name": "default.large_revenue_payments_and_business_only_1",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Total number of account types",
            "query": "SELECT count(id) FROM default.account_type",
            "mode": "published",
            "name": "default.number_of_account_types",
        },
    ),
)

BASIC = (  # type: ignore
    (
        "/namespaces/basic.source/",
        {},
    ),
    (
        "/namespaces/basic.transform/",
        {},
    ),
    (
        "/namespaces/basic.dimension/",
        {},
    ),
    (
        "/nodes/source/",
        {
            "name": "basic.source.users",
            "description": "A user table",
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "full_name", "type": "string"},
                {"name": "age", "type": "int"},
                {
                    "name": "country",
                    "type": "string",
                    "dimension": "basic.dimension.countries",
                },
                {"name": "gender", "type": "string"},
                {"name": "preferred_language", "type": "string"},
                {"name": "secret_number", "type": "float"},
                {"name": "created_at", "type": "timestamp"},
                {"name": "post_processing_timestamp", "type": "timestamp"},
            ],
            "mode": "published",
            "catalog": "public",
            "schema_": "basic",
            "table": "dim_users",
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "User dimension",
            "query": (
                "SELECT id, full_name, age, country, gender, preferred_language, "
                "secret_number, created_at, post_processing_timestamp "
                "FROM basic.source.users"
            ),
            "mode": "published",
            "name": "basic.dimension.users",
            "primary_key": ["id"],
        },
    ),
    (
        "/nodes/source/",
        {
            "name": "basic.source.comments",
            "description": "A fact table with comments",
            "columns": [
                {"name": "id", "type": "int"},
                {
                    "name": "user_id",
                    "type": "int",
                    "dimension": "basic.dimension.users",
                },
                {"name": "timestamp", "type": "timestamp"},
                {"name": "text", "type": "string"},
                {"name": "event_timestamp", "type": "timestamp"},
                {"name": "created_at", "type": "timestamp"},
                {"name": "post_processing_timestamp", "type": "timestamp"},
            ],
            "mode": "published",
            "catalog": "public",
            "schema_": "basic",
            "table": "comments",
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "Country dimension",
            "query": "SELECT country, COUNT(1) AS user_cnt "
            "FROM basic.source.users GROUP BY country",
            "mode": "published",
            "name": "basic.dimension.countries",
            "primary_key": ["country"],
        },
    ),
    (
        "/nodes/transform/",
        {
            "description": "Country level agg table",
            "query": (
                "SELECT country, COUNT(DISTINCT id) AS num_users "
                "FROM basic.source.users GROUP BY 1"
            ),
            "mode": "published",
            "name": "basic.transform.country_agg",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Number of comments",
            "query": ("SELECT COUNT(1) FROM basic.source.comments"),
            "mode": "published",
            "name": "basic.num_comments",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Number of users.",
            "type": "metric",
            "query": ("SELECT SUM(1) FROM basic.dimension.users"),
            "mode": "published",
            "name": "basic.num_users",
        },
    ),
)

BASIC_IN_DIFFERENT_CATALOG = (  # type: ignore
    (
        "/namespaces/different.basic/",
        {},
    ),
    (
        "/namespaces/different.basic.source/",
        {},
    ),
    (
        "/namespaces/different.basic.transform/",
        {},
    ),
    (
        "/namespaces/different.basic.dimension/",
        {},
    ),
    (
        "/nodes/source/",
        {
            "name": "different.basic.source.users",
            "description": "A user table",
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "full_name", "type": "string"},
                {"name": "age", "type": "int"},
                {"name": "country", "type": "string"},
                {"name": "gender", "type": "string"},
                {"name": "preferred_language", "type": "string"},
                {"name": "secret_number", "type": "float"},
                {"name": "created_at", "type": "timestamp"},
                {"name": "post_processing_timestamp", "type": "timestamp"},
            ],
            "mode": "published",
            "catalog": "default",
            "schema_": "basic",
            "table": "dim_users",
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "User dimension",
            "query": (
                "SELECT id, full_name, age, country, gender, preferred_language, "
                "secret_number, created_at, post_processing_timestamp "
                "FROM different.basic.source.users"
            ),
            "mode": "published",
            "name": "different.basic.dimension.users",
            "primary_key": ["id"],
        },
    ),
    (
        "/nodes/source/",
        {
            "name": "different.basic.source.comments",
            "description": "A fact table with comments",
            "columns": [
                {"name": "id", "type": "int"},
                {
                    "name": "user_id",
                    "type": "int",
                    "dimension": "different.basic.dimension.users",
                },
                {"name": "timestamp", "type": "timestamp"},
                {"name": "text", "type": "string"},
                {"name": "event_timestamp", "type": "timestamp"},
                {"name": "created_at", "type": "timestamp"},
                {"name": "post_processing_timestamp", "type": "timestamp"},
            ],
            "mode": "published",
            "catalog": "default",
            "schema_": "basic",
            "table": "comments",
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "Country dimension",
            "query": "SELECT country, COUNT(1) AS user_cnt "
            "FROM different.basic.source.users GROUP BY country",
            "mode": "published",
            "name": "different.basic.dimension.countries",
            "primary_key": ["country"],
        },
    ),
    (
        "/nodes/transform/",
        {
            "description": "Country level agg table",
            "query": (
                "SELECT country, COUNT(DISTINCT id) AS num_users "
                "FROM different.basic.source.users GROUP BY 1"
            ),
            "mode": "published",
            "name": "different.basic.transform.country_agg",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Number of comments",
            "query": ("SELECT COUNT(1) FROM different.basic.source.comments"),
            "mode": "published",
            "name": "different.basic.num_comments",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Number of users.",
            "type": "metric",
            "query": (
                "SELECT SUM(num_users) FROM different.basic.transform.country_agg"
            ),
            "mode": "published",
            "name": "different.basic.num_users",
        },
    ),
)

EVENT = (  # type: ignore
    (  # Event examples
        "/nodes/source/",
        {
            "name": "default.event_source",
            "description": "Events",
            "columns": [
                {"name": "event_id", "type": "int"},
                {"name": "event_latency", "type": "int"},
                {"name": "device_id", "type": "int"},
                {"name": "country", "type": "string"},
            ],
            "mode": "published",
            "catalog": "default",
            "schema_": "logs",
            "table": "log_events",
        },
    ),
    (
        "/nodes/transform/",
        {
            "name": "default.long_events",
            "description": "High-Latency Events",
            "query": "SELECT event_id, event_latency, device_id, country "
            "FROM default.event_source WHERE event_latency > 1000000",
            "mode": "published",
        },
    ),
    (
        "/nodes/dimension/",
        {
            "name": "default.country_dim",
            "description": "Country Dimension",
            "query": "SELECT country, COUNT(DISTINCT event_id) AS events_cnt "
            "FROM default.event_source GROUP BY country",
            "mode": "published",
            "primary_key": ["country"],
        },
    ),
    (
        "/nodes/default.event_source/link",
        {
            "dimension_node": "default.country_dim",
            "join_type": "left",
            "join_on": ("default.event_source.country = default.country_dim.country"),
        },
    ),
    (
        "/nodes/default.long_events/link",
        {
            "dimension_node": "default.country_dim",
            "join_type": "left",
            "join_on": ("default.long_events.country = default.country_dim.country"),
        },
    ),
    (
        "/nodes/metric/",
        {
            "name": "default.device_ids_count",
            "description": "Number of Distinct Devices",
            "query": "SELECT COUNT(DISTINCT device_id) FROM default.event_source",
            "mode": "published",
        },
    ),
    (
        "/nodes/metric/",
        {
            "name": "default.long_events_distinct_countries",
            "description": "Number of Distinct Countries for Long Events",
            "query": "SELECT COUNT(DISTINCT country) FROM default.long_events",
            "mode": "published",
        },
    ),
)

DBT = (  # type: ignore
    (
        "/namespaces/dbt.source/",
        {},
    ),
    (
        "/namespaces/dbt.source.jaffle_shop/",
        {},
    ),
    (
        "/namespaces/dbt.transform/",
        {},
    ),
    (
        "/namespaces/dbt.dimension/",
        {},
    ),
    (
        "/namespaces/dbt.source.stripe/",
        {},
    ),
    (  # DBT examples
        "/nodes/source/",
        {
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "first_name", "type": "string"},
                {"name": "last_name", "type": "string"},
            ],
            "description": "Customer table",
            "mode": "published",
            "name": "dbt.source.jaffle_shop.customers",
            "catalog": "public",
            "schema_": "jaffle_shop",
            "table": "customers",
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "User dimension",
            "query": (
                "SELECT id, first_name, last_name FROM dbt.source.jaffle_shop.customers"
            ),
            "mode": "published",
            "name": "dbt.dimension.customers",
            "primary_key": ["id"],
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "id", "type": "int"},
                {
                    "name": "user_id",
                    "type": "int",
                    "dimension": "dbt.dimension.customers",
                },
                {"name": "order_date", "type": "date"},
                {"name": "status", "type": "string"},
                {"name": "_etl_loaded_at", "type": "timestamp"},
            ],
            "description": "Orders fact table",
            "mode": "published",
            "name": "dbt.source.jaffle_shop.orders",
            "catalog": "public",
            "schema_": "jaffle_shop",
            "table": "orders",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "orderid", "type": "int"},
                {"name": "paymentmethod", "type": "string"},
                {"name": "status", "type": "string"},
                {"name": "amount", "type": "int"},
                {"name": "created", "type": "date"},
                {"name": "_batched_at", "type": "timestamp"},
            ],
            "description": "Payments fact table.",
            "mode": "published",
            "name": "dbt.source.stripe.payments",
            "catalog": "public",
            "schema_": "stripe",
            "table": "payments",
        },
    ),
    (
        "/nodes/transform/",
        {
            "query": (
                "SELECT c.id, "
                "        c.first_name, "
                "        c.last_name, "
                "        COUNT(1) AS order_cnt "
                "FROM dbt.source.jaffle_shop.orders o "
                "JOIN dbt.source.jaffle_shop.customers c ON o.user_id = c.id "
                "GROUP BY c.id, "
                "        c.first_name, "
                "        c.last_name "
            ),
            "description": "Country level agg table",
            "mode": "published",
            "name": "dbt.transform.customer_agg",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "item_name", "type": "string"},
                {"name": "sold_count", "type": "int"},
                {"name": "price_per_unit", "type": "float"},
                {"name": "psp", "type": "string"},
            ],
            "description": "A source table for sales",
            "mode": "published",
            "name": "default.sales",
            "catalog": "default",
            "schema_": "revenue",
            "table": "sales",
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "Item dimension",
            "query": (
                "SELECT item_name account_type_classification FROM default.sales"
            ),
            "mode": "published",
            "name": "default.items",
            "primary_key": ["account_type_classification"],
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Total units sold",
            "query": "SELECT SUM(sold_count) as default_DOT_items_sold_count FROM default.sales",
            "mode": "published",
            "name": "default.items_sold_count",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Total profit",
            "query": "SELECT SUM(sold_count * price_per_unit) FROM default.sales",
            "mode": "published",
            "name": "default.total_profit",
        },
    ),
    (
        "/nodes/dbt.source.jaffle_shop.orders/link",
        {
            "dimension_node": "dbt.dimension.customers",
            "join_type": "inner",
            "join_on": (
                "dbt.source.jaffle_shop.orders.user_id = dbt.dimension.customers.id"
            ),
        },
    ),
)

# lateral view explode/cross join unnest examples
LATERAL_VIEW = (  # type: ignore
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "id", "type": "int"},
                {"name": "painter", "type": "string"},
                {
                    "name": "colors",
                    "type": "map<string, string>",
                },
            ],
            "description": "Murals",
            "mode": "published",
            "name": "basic.murals",
            "catalog": "public",
            "schema_": "basic",
            "table": "murals",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "color_id", "type": "int"},
                {"name": "color_name", "type": "string"},
                {
                    "name": "opacity",
                    "type": "float",
                },
                {
                    "name": "luminosity",
                    "type": "float",
                },
                {
                    "name": "garishness",
                    "type": "float",
                },
            ],
            "description": "Patch",
            "mode": "published",
            "name": "basic.patches",
            "catalog": "public",
            "schema_": "basic",
            "table": "patches",
        },
    ),
    (
        "/nodes/transform/",
        {
            "query": """
            SELECT
              cast(color_id as string) color_id,
              color_name,
              opacity,
              luminosity,
              garishness
            FROM basic.patches
            """,
            "description": "Corrected patches",
            "mode": "published",
            "name": "basic.corrected_patches",
        },
    ),
    (
        "/nodes/dimension/",
        {
            "query": """
            SELECT
              id AS mural_id,
              t.color_id,
              t.color_name color_name
            FROM
            (
              select
                id,
                colors
              from basic.murals
             ) murals
             CROSS JOIN UNNEST(colors) AS t(color_id, color_name)
            """,
            "description": "Mural paint colors",
            "mode": "published",
            "name": "basic.paint_colors_trino",
            "primary_key": ["color_id"],
        },
    ),
    (
        "/nodes/dimension/",
        {
            "query": """
            SELECT
              id AS mural_id,
              color_id,
              color_name
            FROM
            (
              select
                id,
                EXPLODE(colors) AS (color_id, color_name)
              from basic.murals
            )
            """,
            "description": "Mural paint colors",
            "mode": "published",
            "name": "basic.paint_colors_spark",
            "primary_key": ["color_id"],
        },
    ),
    (
        "/nodes/metric/",
        {
            "query": """
        SELECT AVG(luminosity) FROM basic.corrected_patches
        """,
            "description": "Average luminosity of color patch",
            "mode": "published",
            "name": "basic.avg_luminosity_patches",
        },
    ),
)

COMPLEX_DIMENSION_LINK = (
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "user_id", "type": "int"},
                {"name": "event_start_date", "type": "int"},
                {"name": "event_end_date", "type": "int"},
                {"name": "elapsed_secs", "type": "int"},
                {"name": "user_registration_country", "type": "string"},
            ],
            "description": "Events table",
            "mode": "published",
            "name": "default.events_table",
            "catalog": "default",
            "schema_": "examples",
            "table": "events",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "user_id", "type": "int"},
                {"name": "snapshot_date", "type": "int"},
                {"name": "registration_country", "type": "string"},
                {"name": "residence_country", "type": "string"},
                {"name": "account_type", "type": "string"},
            ],
            "description": "Users table",
            "mode": "published",
            "name": "default.users_table",
            "catalog": "default",
            "schema_": "examples",
            "table": "users",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "country_code", "type": "string"},
                {"name": "name", "type": "string"},
                {"name": "population", "type": "int"},
            ],
            "description": "Countries table",
            "mode": "published",
            "name": "default.countries_table",
            "catalog": "default",
            "schema_": "examples",
            "table": "countries",
        },
    ),
    (
        "/nodes/transform/",
        {
            "description": "Events fact",
            "query": """
        SELECT
            user_id,
            event_start_date,
            event_end_date,
            elapsed_secs,
            user_registration_country
        FROM default.events_table
    """,
            "mode": "published",
            "name": "default.events",
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "Users",
            "query": """
        SELECT
            user_id,
            snapshot_date,
            registration_country,
            residence_country,
            account_type
        FROM default.users_table
    """,
            "mode": "published",
            "name": "default.users",
            "primary_key": ["user_id", "snapshot_date"],
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "Countries",
            "query": """
        SELECT
            country_code,
            name,
            population
        FROM default.countries_table
    """,
            "mode": "published",
            "name": "default.countries",
            "primary_key": ["country_code"],
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Elapsed Time in Seconds",
            "query": "SELECT SUM(elapsed_secs) FROM default.events",
            "mode": "published",
            "name": "default.elapsed_secs",
        },
    ),
)

DIMENSION_LINK = (  # type: ignore
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "dateint", "type": "int"},
                {"name": "month", "type": "int"},
                {"name": "year", "type": "int"},
                {"name": "day", "type": "int"},
            ],
            "description": "Date table",
            "mode": "published",
            "name": "default.date",
            "catalog": "default",
            "schema_": "examples",
            "table": "date",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "country_code", "type": "string"},
                {"name": "name", "type": "string"},
                {"name": "formation_date", "type": "int"},
                {"name": "last_election_date", "type": "int"},
            ],
            "description": "Countries table",
            "mode": "published",
            "name": "default.countries",
            "catalog": "default",
            "schema_": "examples",
            "table": "countries",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": [
                {"name": "user_id", "type": "int"},
                {"name": "birth_country", "type": "string"},
                {"name": "birth_date", "type": "int"},
                {"name": "residence_country", "type": "string"},
                {"name": "age", "type": "int"},
            ],
            "description": "Users table",
            "mode": "published",
            "name": "default.users",
            "catalog": "default",
            "schema_": "examples",
            "table": "users",
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "Date dimension",
            "query": """
            SELECT
                dateint,
                month,
                year,
                day
            FROM default.date
        """,
            "mode": "published",
            "name": "default.date_dim",
            "primary_key": ["dateint"],
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "Country dimension",
            "query": """
            SELECT
                country_code,
                name,
                formation_date,
                last_election_date
            FROM default.countries
        """,
            "mode": "published",
            "name": "default.special_country_dim",
            "primary_key": ["country_code"],
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "User dimension",
            "query": """
            SELECT
                user_id,
                birth_country,
                residence_country,
                age,
                birth_date
            FROM default.users
            """,
            "mode": "published",
            "name": "default.user_dim",
            "primary_key": ["user_id"],
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Average User Age",
            "query": """
            SELECT
              AVG(age)
            FROM default.user_dim
            """,
            "mode": "published",
            "name": "default.avg_user_age",
        },
    ),
    (
        "/nodes/default.user_dim/link",
        {
            "dimension_node": "default.special_country_dim",
            "join_type": "left",
            "join_on": (
                "default.user_dim.birth_country = default.special_country_dim.country_code"
            ),
            "role": "birth_country",
        },
    ),
    (
        "/nodes/default.user_dim/link",
        {
            "dimension_node": "default.special_country_dim",
            "join_type": "left",
            "join_on": (
                "default.user_dim.residence_country = default.special_country_dim.country_code"
            ),
            "role": "residence_country",
        },
    ),
    (
        "/nodes/default.special_country_dim/link",
        {
            "dimension_node": "default.date_dim",
            "join_type": "left",
            "join_on": (
                "default.special_country_dim.formation_date = default.date_dim.dateint"
            ),
            "role": "formation_date",
        },
    ),
    (
        "/nodes/default.special_country_dim/link",
        {
            "dimension_node": "default.date_dim",
            "join_type": "left",
            "join_on": (
                "default.special_country_dim.last_election_date = default.date_dim.dateint"
            ),
            "role": "last_election_date",
        },
    ),
)

# =============================================================================
# SIMPLE_HLL - Minimal example for testing HLL/APPROX_COUNT_DISTINCT
# =============================================================================
# A simple events table with user_id and category for testing approximate
# distinct count metrics.
# =============================================================================
SIMPLE_HLL = (  # type: ignore
    (
        "/namespaces/hll/",
        {},
    ),
    (
        "/nodes/source/",
        {
            "name": "hll.events",
            "description": "Simple events table for HLL testing",
            "columns": [
                {"name": "event_id", "type": "int"},
                {"name": "user_id", "type": "int"},
                {"name": "category", "type": "string"},
                {"name": "event_time", "type": "timestamp"},
            ],
            "mode": "published",
            "catalog": "default",
            "schema_": "hll",
            "table": "events",
        },
    ),
    (
        "/nodes/dimension/",
        {
            "name": "hll.category_dim",
            "description": "Category dimension",
            "query": "SELECT DISTINCT category AS category FROM hll.events",
            "mode": "published",
            "primary_key": ["category"],
        },
    ),
    (
        "/nodes/metric/",
        {
            "name": "hll.unique_users",
            "description": "Approximate unique user count using HLL",
            "query": "SELECT APPROX_COUNT_DISTINCT(user_id) FROM hll.events",
            "mode": "published",
        },
    ),
    (
        "/nodes/metric/",
        {
            "name": "hll.total_events",
            "description": "Total event count (for comparison)",
            "query": "SELECT COUNT(event_id) FROM hll.events",
            "mode": "published",
        },
    ),
    # Link the dimension to the source node
    (
        "/nodes/hll.events/link",
        {
            "dimension_node": "hll.category_dim",
            "join_type": "left",
            "join_on": "hll.events.category = hll.category_dim.category",
        },
    ),
)

# =============================================================================
# DERIVED_METRICS - Example derived metrics that reference other metrics
# =============================================================================
# These metrics demonstrate derived metric capabilities where a metric
# references another metric node rather than a transform/source directly.
#
# This example set tests:
#   1. Same-parent derived metrics (ratio of metrics from same fact)
#   2. Cross-fact derived metrics (ratio of metrics from different facts with shared dims)
#   3. Period-over-period metrics (WoW, MoM using LAG on base metric)
#   4. Failure case: cross-fact with NO shared dimensions
#
# Schema:
#   - orders_source: order_id, amount, customer_id, order_date
#   - events_source: event_id, page_views, customer_id, event_date
#   - inventory_source: inventory_id, quantity, warehouse_id, inventory_date
#   - dates_source: date_id, date_value, week, month, year
#   - customers_source: customer_id, name, email
#   - warehouses_source: warehouse_id, name, location
#
# Dimensions:
#   - default.derived_date (shared: orders, events)
#   - default.customer (shared: orders, events)
#   - default.warehouse (only: inventory - NO overlap with orders/events)
#
# Base Metrics:
#   - default.dm_revenue (orders_source) -> dims: derived_date, customer
#   - default.dm_orders (orders_source) -> dims: derived_date, customer
#   - default.dm_page_views (events_source) -> dims: derived_date, customer
#   - default.dm_total_inventory (inventory_source) -> dims: warehouse only
#
# Derived Metrics:
#   - default.dm_revenue_per_order (same parent: orders)
#   - default.dm_revenue_per_page_view (cross-fact: orders + events, shared dims)
#   - default.dm_wow_revenue_change (period-over-period)
#   - default.dm_mom_revenue_change (period-over-period)
# =============================================================================
DERIVED_METRICS = (  # type: ignore
    # =========================================================================
    # Source Nodes
    # =========================================================================
    (
        "/nodes/source/",
        {
            "name": "default.orders_source",
            "description": "Orders fact table",
            "columns": [
                {"name": "order_id", "type": "int"},
                {"name": "amount", "type": "float"},
                {"name": "customer_id", "type": "int"},
                {"name": "order_date", "type": "int"},  # FK to dates_source.date_id
            ],
            "mode": "published",
            "catalog": "default",
            "schema_": "derived",
            "table": "orders",
        },
    ),
    (
        "/nodes/source/",
        {
            "name": "default.events_source",
            "description": "Events fact table",
            "columns": [
                {"name": "event_id", "type": "int"},
                {"name": "page_views", "type": "int"},
                {"name": "customer_id", "type": "int"},
                {"name": "event_date", "type": "int"},  # FK to dates_source.date_id
            ],
            "mode": "published",
            "catalog": "default",
            "schema_": "derived",
            "table": "events",
        },
    ),
    (
        "/nodes/source/",
        {
            "name": "default.inventory_source",
            "description": "Inventory fact table (no shared dimensions with orders/events)",
            "columns": [
                {"name": "inventory_id", "type": "int"},
                {"name": "quantity", "type": "int"},
                {"name": "warehouse_id", "type": "int"},
                {"name": "inventory_date", "type": "int"},
            ],
            "mode": "published",
            "catalog": "default",
            "schema_": "derived",
            "table": "inventory",
        },
    ),
    (
        "/nodes/source/",
        {
            "name": "default.dates_source",
            "description": "Date dimension source",
            "columns": [
                {"name": "date_id", "type": "int"},
                {"name": "date_value", "type": "timestamp"},
                {"name": "week", "type": "int"},
                {"name": "month", "type": "int"},
                {"name": "year", "type": "int"},
            ],
            "mode": "published",
            "catalog": "default",
            "schema_": "derived",
            "table": "dates",
        },
    ),
    (
        "/nodes/source/",
        {
            "name": "default.customers_source",
            "description": "Customer dimension source",
            "columns": [
                {"name": "customer_id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": "string"},
            ],
            "mode": "published",
            "catalog": "default",
            "schema_": "derived",
            "table": "customers",
        },
    ),
    (
        "/nodes/source/",
        {
            "name": "default.warehouses_source",
            "description": "Warehouse dimension source",
            "columns": [
                {"name": "warehouse_id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "location", "type": "string"},
            ],
            "mode": "published",
            "catalog": "default",
            "schema_": "derived",
            "table": "warehouses",
        },
    ),
    # =========================================================================
    # Dimension Nodes
    # =========================================================================
    (
        "/nodes/dimension/",
        {
            "name": "default.derived_date",
            "description": "Date dimension",
            "query": """
                SELECT
                    date_id,
                    date_value,
                    week,
                    month,
                    year
                FROM default.dates_source
            """,
            "mode": "published",
            "primary_key": ["date_id"],
        },
    ),
    (
        "/nodes/dimension/",
        {
            "name": "default.customer",
            "description": "Customer dimension",
            "query": """
                SELECT
                    customer_id,
                    name,
                    email
                FROM default.customers_source
            """,
            "mode": "published",
            "primary_key": ["customer_id"],
        },
    ),
    (
        "/nodes/dimension/",
        {
            "name": "default.warehouse",
            "description": "Warehouse dimension (NOT shared with orders/events)",
            "query": """
                SELECT
                    warehouse_id,
                    name,
                    location
                FROM default.warehouses_source
            """,
            "mode": "published",
            "primary_key": ["warehouse_id"],
        },
    ),
    # =========================================================================
    # Dimension Links - Connect facts to shared dimensions
    # =========================================================================
    # orders_source -> date (via order_date)
    (
        "/nodes/default.orders_source/link",
        {
            "dimension_node": "default.derived_date",
            "join_type": "left",
            "join_on": "default.orders_source.order_date = default.derived_date.date_id",
        },
    ),
    # orders_source -> customer (via customer_id)
    (
        "/nodes/default.orders_source/link",
        {
            "dimension_node": "default.customer",
            "join_type": "left",
            "join_on": "default.orders_source.customer_id = default.customer.customer_id",
        },
    ),
    # events_source -> date (via event_date)
    (
        "/nodes/default.events_source/link",
        {
            "dimension_node": "default.derived_date",
            "join_type": "left",
            "join_on": "default.events_source.event_date = default.derived_date.date_id",
        },
    ),
    # events_source -> customer (via customer_id)
    (
        "/nodes/default.events_source/link",
        {
            "dimension_node": "default.customer",
            "join_type": "left",
            "join_on": "default.events_source.customer_id = default.customer.customer_id",
        },
    ),
    # inventory_source -> warehouse (via warehouse_id) - NO overlap with orders/events dims
    (
        "/nodes/default.inventory_source/link",
        {
            "dimension_node": "default.warehouse",
            "join_type": "left",
            "join_on": "default.inventory_source.warehouse_id = default.warehouse.warehouse_id",
        },
    ),
    # =========================================================================
    # Base Metrics
    # =========================================================================
    (
        "/nodes/metric/",
        {
            "name": "default.dm_revenue",
            "description": "Total revenue from orders",
            "query": "SELECT SUM(amount) FROM default.orders_source",
            "mode": "published",
        },
    ),
    (
        "/nodes/metric/",
        {
            "name": "default.dm_orders",
            "description": "Count of orders",
            "query": "SELECT COUNT(*) FROM default.orders_source",
            "mode": "published",
        },
    ),
    (
        "/nodes/metric/",
        {
            "name": "default.dm_page_views",
            "description": "Total page views from events",
            "query": "SELECT SUM(page_views) FROM default.events_source",
            "mode": "published",
        },
    ),
    (
        "/nodes/metric/",
        {
            "name": "default.dm_total_inventory",
            "description": "Total inventory quantity (warehouse-only dimension)",
            "query": "SELECT SUM(quantity) FROM default.inventory_source",
            "mode": "published",
        },
    ),
    # =========================================================================
    # Derived Metrics - Same Parent (orders_source)
    # =========================================================================
    (
        "/nodes/metric/",
        {
            "name": "default.dm_revenue_per_order",
            "description": "Revenue per order (same parent ratio)",
            "query": "SELECT default.dm_revenue / NULLIF(default.dm_orders, 0)",
            "mode": "published",
        },
    ),
    # =========================================================================
    # Derived Metrics - Cross-Fact with Shared Dimensions (orders + events)
    # Available dimensions = intersection = {date, customer}
    # =========================================================================
    (
        "/nodes/metric/",
        {
            "name": "default.dm_revenue_per_page_view",
            "description": "Revenue per page view (cross-fact ratio with shared dimensions)",
            "query": "SELECT default.dm_revenue / NULLIF(default.dm_page_views, 0)",
            "mode": "published",
        },
    ),
    # =========================================================================
    # Derived Metrics - Period-over-Period
    # Same base metric (default.dm_revenue), different ORDER BY dimensions
    # =========================================================================
    (
        "/nodes/metric/",
        {
            "name": "default.dm_wow_revenue_change",
            "description": "Week-over-week revenue change (%)",
            "query": """
                SELECT
                    (default.dm_revenue - LAG(default.dm_revenue, 1) OVER (ORDER BY default.derived_date.week))
                    / NULLIF(LAG(default.dm_revenue, 1) OVER (ORDER BY default.derived_date.week), 0) * 100
            """,
            "mode": "published",
        },
    ),
    (
        "/nodes/metric/",
        {
            "name": "default.dm_mom_revenue_change",
            "description": "Month-over-month revenue change (%)",
            "query": """
                SELECT
                    (default.dm_revenue - LAG(default.dm_revenue, 1) OVER (ORDER BY default.derived_date.month))
                    / NULLIF(LAG(default.dm_revenue, 1) OVER (ORDER BY default.derived_date.month), 0) * 100
            """,
            "mode": "published",
        },
    ),
)

# =============================================================================
# BUILD_V3 - Comprehensive test model for V3 SQL generation
# =============================================================================
# This example tests:
# - Multi-hop dimension traversal with roles
# - Same dimension reachable via different paths (from/to/customer locations)
# - Dimension hierarchies (date: day->week->month->quarter->year,
#                         location: postal_code->city->region->country)
# - Cross-fact derived metrics (orders + page_views)
# - Period-over-period metrics (window functions)
# - Ratio metrics (same fact)
# - Multiple aggregability levels (FULL, LIMITED, NONE)
# =============================================================================
BUILD_V3 = (  # type: ignore
    # =========================================================================
    # Namespace Setup
    # =========================================================================
    (
        "/namespaces/v3/",
        {},
    ),
    # =========================================================================
    # Source Nodes - Raw Data Tables
    # =========================================================================
    # Orders header table
    (
        "/nodes/source/",
        {
            "name": "v3.src_orders",
            "description": "Order headers with customer and shipping info",
            "columns": [
                {"name": "order_id", "type": "int"},
                {"name": "customer_id", "type": "int"},
                {"name": "order_date", "type": "int"},  # FK to dates
                {"name": "from_location_id", "type": "int"},  # warehouse/origin
                {"name": "to_location_id", "type": "int"},  # delivery destination
                {"name": "status", "type": "string"},
            ],
            "mode": "published",
            "catalog": "default",
            "schema_": "v3",
            "table": "orders",
        },
    ),
    # Order line items
    (
        "/nodes/source/",
        {
            "name": "v3.src_order_items",
            "description": "Order line items with product and pricing",
            "columns": [
                {"name": "order_id", "type": "int"},
                {"name": "line_number", "type": "int"},
                {"name": "product_id", "type": "int"},
                {"name": "quantity", "type": "int"},
                {"name": "unit_price", "type": "float"},
            ],
            "mode": "published",
            "catalog": "default",
            "schema_": "v3",
            "table": "order_items",
        },
    ),
    # Page views (second fact for cross-fact testing)
    (
        "/nodes/source/",
        {
            "name": "v3.src_page_views",
            "description": "Web page view events",
            "columns": [
                {"name": "view_id", "type": "int"},
                {"name": "session_id", "type": "string"},
                {"name": "customer_id", "type": "int"},
                {"name": "page_date", "type": "int"},  # FK to dates
                {"name": "page_type", "type": "string"},
                {"name": "product_id", "type": "int"},  # nullable, for product pages
            ],
            "mode": "published",
            "catalog": "default",
            "schema_": "v3",
            "table": "page_views",
        },
    ),
    # Customers dimension source
    (
        "/nodes/source/",
        {
            "name": "v3.src_customers",
            "description": "Customer master data",
            "columns": [
                {"name": "customer_id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": "string"},
                {"name": "registration_date", "type": "int"},  # FK to dates
                {"name": "location_id", "type": "int"},  # customer's home location
            ],
            "mode": "published",
            "catalog": "default",
            "schema_": "v3",
            "table": "customers",
        },
    ),
    # Products dimension source
    (
        "/nodes/source/",
        {
            "name": "v3.src_products",
            "description": "Product catalog",
            "columns": [
                {"name": "product_id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "category", "type": "string"},
                {"name": "subcategory", "type": "string"},
                {"name": "price", "type": "float"},
            ],
            "mode": "published",
            "catalog": "default",
            "schema_": "v3",
            "table": "products",
        },
    ),
    # Date dimension source (with hierarchy columns)
    (
        "/nodes/source/",
        {
            "name": "v3.src_dates",
            "description": "Date dimension with hierarchy levels",
            "columns": [
                {"name": "date_id", "type": "int"},
                {"name": "date_value", "type": "timestamp"},
                {"name": "day_of_week", "type": "int"},
                {"name": "week", "type": "int"},
                {"name": "month", "type": "int"},
                {"name": "quarter", "type": "int"},
                {"name": "year", "type": "int"},
            ],
            "mode": "published",
            "catalog": "default",
            "schema_": "v3",
            "table": "dates",
        },
    ),
    # Location dimension source (with hierarchy columns)
    (
        "/nodes/source/",
        {
            "name": "v3.src_locations",
            "description": "Location dimension with geographic hierarchy",
            "columns": [
                {"name": "location_id", "type": "int"},
                {"name": "postal_code", "type": "string"},
                {"name": "city", "type": "string"},
                {"name": "region", "type": "string"},
                {"name": "country", "type": "string"},
            ],
            "mode": "published",
            "catalog": "default",
            "schema_": "v3",
            "table": "locations",
        },
    ),
    # =========================================================================
    # Transform Nodes - Semantic Unification
    # =========================================================================
    # order_details: Joins orders + order_items at line item grain
    # This is a semantic unification - treating orders and their items as one concept
    (
        "/nodes/transform/",
        {
            "name": "v3.order_details",
            "description": "Order line items with order header info (semantic unification)",
            "query": """
                SELECT
                    o.order_id,
                    oi.line_number,
                    o.customer_id,
                    o.order_date,
                    o.from_location_id,
                    o.to_location_id,
                    o.status,
                    oi.product_id,
                    oi.quantity,
                    oi.unit_price,
                    oi.quantity * oi.unit_price AS line_total
                FROM v3.src_orders o
                JOIN v3.src_order_items oi ON o.order_id = oi.order_id
            """,
            "mode": "published",
            "primary_key": ["order_id", "line_number"],
        },
    ),
    # page_views_enriched: Simple passthrough with computed column
    (
        "/nodes/transform/",
        {
            "name": "v3.page_views_enriched",
            "description": "Page views with computed flags",
            "query": """
                SELECT
                    view_id,
                    session_id,
                    customer_id,
                    page_date,
                    page_type,
                    product_id,
                    CASE WHEN page_type = 'product' THEN 1 ELSE 0 END AS is_product_view,
                    CASE WHEN page_type = 'checkout' THEN 1 ELSE 0 END AS is_checkout_view
                FROM v3.src_page_views
            """,
            "mode": "published",
            "primary_key": ["view_id"],
        },
    ),
    # =========================================================================
    # Dimension Nodes
    # =========================================================================
    (
        "/nodes/dimension/",
        {
            "name": "v3.customer",
            "description": "Customer dimension",
            "query": """
                SELECT
                    customer_id,
                    name,
                    email,
                    registration_date,
                    location_id
                FROM v3.src_customers
            """,
            "mode": "published",
            "primary_key": ["customer_id"],
        },
    ),
    (
        "/nodes/dimension/",
        {
            "name": "v3.product",
            "description": "Product dimension with category hierarchy",
            "query": """
                SELECT
                    product_id,
                    name,
                    category,
                    subcategory,
                    price
                FROM v3.src_products
            """,
            "mode": "published",
            "primary_key": ["product_id"],
        },
    ),
    (
        "/nodes/dimension/",
        {
            "name": "v3.date",
            "description": "Date dimension with time hierarchy (day->week->month->quarter->year)",
            "query": """
                SELECT
                    date_id,
                    date_value,
                    day_of_week,
                    week,
                    month,
                    quarter,
                    year
                FROM v3.src_dates
            """,
            "mode": "published",
            "primary_key": ["date_id"],
        },
    ),
    (
        "/nodes/dimension/",
        {
            "name": "v3.location",
            "description": "Location dimension with geographic hierarchy (postal_code->city->region->country)",
            "query": """
                SELECT
                    location_id,
                    postal_code,
                    city,
                    region,
                    country
                FROM v3.src_locations
            """,
            "mode": "published",
            "primary_key": ["location_id"],
        },
    ),
    # =========================================================================
    # Dimension Links - Building the Dimension Graph
    # =========================================================================
    # --- order_details links ---
    # order_details -> customer (direct)
    (
        "/nodes/v3.order_details/link",
        {
            "dimension_node": "v3.customer",
            "join_type": "left",
            "join_on": "v3.order_details.customer_id = v3.customer.customer_id",
            "role": "customer",
        },
    ),
    # order_details -> date (via order_date) - role: order
    (
        "/nodes/v3.order_details/link",
        {
            "dimension_node": "v3.date",
            "join_type": "left",
            "join_on": "v3.order_details.order_date = v3.date.date_id",
            "role": "order",
        },
    ),
    # order_details -> location (via from_location_id) - role: from
    (
        "/nodes/v3.order_details/link",
        {
            "dimension_node": "v3.location",
            "join_type": "left",
            "join_on": "v3.order_details.from_location_id = v3.location.location_id",
            "role": "from",
        },
    ),
    # order_details -> location (via to_location_id) - role: to
    (
        "/nodes/v3.order_details/link",
        {
            "dimension_node": "v3.location",
            "join_type": "left",
            "join_on": "v3.order_details.to_location_id = v3.location.location_id",
            "role": "to",
        },
    ),
    # order_details -> product (direct)
    (
        "/nodes/v3.order_details/link",
        {
            "dimension_node": "v3.product",
            "join_type": "left",
            "join_on": "v3.order_details.product_id = v3.product.product_id",
        },
    ),
    # --- customer dimension links (for multi-hop traversal) ---
    # customer -> date (via registration_date) - role: registration
    (
        "/nodes/v3.customer/link",
        {
            "dimension_node": "v3.date",
            "join_type": "left",
            "join_on": "v3.customer.registration_date = v3.date.date_id",
            "role": "registration",
            "join_cardinality": "many_to_one",
        },
    ),
    # customer -> location (via location_id) - role: home
    (
        "/nodes/v3.customer/link",
        {
            "dimension_node": "v3.location",
            "join_type": "left",
            "join_on": "v3.customer.location_id = v3.location.location_id",
            "role": "home",
            "join_cardinality": "many_to_one",
        },
    ),
    # --- page_views_enriched links ---
    # page_views -> customer
    (
        "/nodes/v3.page_views_enriched/link",
        {
            "dimension_node": "v3.customer",
            "join_type": "left",
            "join_on": "v3.page_views_enriched.customer_id = v3.customer.customer_id",
            "role": "customer",
        },
    ),
    # page_views -> date (via page_date) - role: page
    (
        "/nodes/v3.page_views_enriched/link",
        {
            "dimension_node": "v3.date",
            "join_type": "left",
            "join_on": "v3.page_views_enriched.page_date = v3.date.date_id",
            "role": "page",
        },
    ),
    # page_views -> product (for product page views)
    (
        "/nodes/v3.page_views_enriched/link",
        {
            "dimension_node": "v3.product",
            "join_type": "left",
            "join_on": "v3.page_views_enriched.product_id = v3.product.product_id",
        },
    ),
    # =========================================================================
    # Base Metrics - On order_details
    # =========================================================================
    # Fully aggregatable metrics (SUM)
    (
        "/nodes/metric/",
        {
            "name": "v3.total_revenue",
            "description": "Total revenue from order line items (fully aggregatable)",
            "query": "SELECT SUM(line_total) FROM v3.order_details",
            "mode": "published",
        },
    ),
    (
        "/nodes/metric/",
        {
            "name": "v3.total_quantity",
            "description": "Total quantity sold (fully aggregatable)",
            "query": "SELECT SUM(quantity) FROM v3.order_details",
            "mode": "published",
        },
    ),
    # Limited aggregability metrics (COUNT DISTINCT)
    (
        "/nodes/metric/",
        {
            "name": "v3.order_count",
            "description": "Count of distinct orders (limited aggregability)",
            "query": "SELECT COUNT(DISTINCT order_id) FROM v3.order_details",
            "mode": "published",
        },
    ),
    (
        "/nodes/metric/",
        {
            "name": "v3.avg_unit_price",
            "description": "Average unit price - decomposes into SUM and COUNT",
            "query": "SELECT AVG(unit_price) FROM v3.order_details",
            "mode": "published",
        },
    ),
    (
        "/nodes/metric/",
        {
            "name": "v3.total_unit_price",
            "description": "Sum of unit prices - shares SUM(unit_price) component with avg_unit_price",
            "query": "SELECT SUM(unit_price) FROM v3.order_details",
            "mode": "published",
        },
    ),
    (
        "/nodes/metric/",
        {
            "name": "v3.customer_count",
            "description": "Count of distinct customers (limited aggregability)",
            "query": "SELECT APPROX_COUNT_DISTINCT(customer_id) FROM v3.order_details",
            "mode": "published",
        },
    ),
    # Note: NONE aggregability metrics (e.g., MEDIAN) cannot be added currently because
    # the MEDIAN function class in DJ doesn't have is_aggregation = True, which causes
    # metric validation to fail. This should be fixed in functions.py.
    # TODO: Add NONE aggregability metric once MEDIAN is properly registered as aggregate.
    # =========================================================================
    # Base Metrics - On page_views_enriched
    # =========================================================================
    (
        "/nodes/metric/",
        {
            "name": "v3.page_view_count",
            "description": "Total page views (fully aggregatable)",
            "query": "SELECT COUNT(view_id) FROM v3.page_views_enriched",
            "mode": "published",
        },
    ),
    (
        "/nodes/metric/",
        {
            "name": "v3.product_view_count",
            "description": "Product page views (fully aggregatable)",
            "query": "SELECT SUM(is_product_view) FROM v3.page_views_enriched",
            "mode": "published",
        },
    ),
    (
        "/nodes/metric/",
        {
            "name": "v3.session_count",
            "description": "Distinct sessions (limited aggregability)",
            "query": "SELECT COUNT(DISTINCT session_id) FROM v3.page_views_enriched",
            "mode": "published",
        },
    ),
    (
        "/nodes/metric/",
        {
            "name": "v3.visitor_count",
            "description": "Distinct visitors (limited aggregability)",
            "query": "SELECT COUNT(DISTINCT customer_id) FROM v3.page_views_enriched",
            "mode": "published",
        },
    ),
    # =========================================================================
    # Derived Metrics - Same Fact Ratios
    # =========================================================================
    (
        "/nodes/metric/",
        {
            "name": "v3.avg_order_value",
            "description": "Average order value (revenue / orders)",
            "query": "SELECT v3.total_revenue / NULLIF(v3.order_count, 0)",
            "mode": "published",
        },
    ),
    (
        "/nodes/metric/",
        {
            "name": "v3.avg_items_per_order",
            "description": "Average items per order",
            "query": "SELECT v3.total_quantity / NULLIF(v3.order_count, 0)",
            "mode": "published",
        },
    ),
    (
        "/nodes/metric/",
        {
            "name": "v3.revenue_per_customer",
            "description": "Revenue per unique customer",
            "query": "SELECT v3.total_revenue / NULLIF(v3.customer_count, 0)",
            "mode": "published",
        },
    ),
    (
        "/nodes/metric/",
        {
            "name": "v3.pages_per_session",
            "description": "Average pages per session",
            "query": "SELECT v3.page_view_count / NULLIF(v3.session_count, 0)",
            "mode": "published",
        },
    ),
    # =========================================================================
    # Derived Metrics - Cross-Fact Ratios
    # These combine metrics from order_details and page_views
    # Available dimensions = intersection of both facts' dimensions
    # =========================================================================
    (
        "/nodes/metric/",
        {
            "name": "v3.conversion_rate",
            "description": "Orders / Visitors (cross-fact ratio)",
            "query": "SELECT CAST(v3.order_count AS DOUBLE) / NULLIF(v3.visitor_count, 0)",
            "mode": "published",
        },
    ),
    (
        "/nodes/metric/",
        {
            "name": "v3.revenue_per_visitor",
            "description": "Revenue / Visitors (cross-fact ratio)",
            "query": "SELECT v3.total_revenue / NULLIF(v3.visitor_count, 0)",
            "mode": "published",
        },
    ),
    (
        "/nodes/metric/",
        {
            "name": "v3.revenue_per_page_view",
            "description": "Revenue / Page Views (cross-fact ratio)",
            "query": "SELECT v3.total_revenue / NULLIF(v3.page_view_count, 0)",
            "mode": "published",
        },
    ),
    # Additional Base Metrics - MIN/MAX aggregations
    (
        "/nodes/metric/",
        {
            "name": "v3.max_unit_price",
            "description": "Maximum unit price (FULL aggregability with MAX merge rule)",
            "query": "SELECT MAX(unit_price) FROM v3.order_details",
            "mode": "published",
        },
    ),
    (
        "/nodes/metric/",
        {
            "name": "v3.min_unit_price",
            "description": "Minimum unit price (FULL aggregability with MIN merge rule)",
            "query": "SELECT MIN(unit_price) FROM v3.order_details",
            "mode": "published",
        },
    ),
    # =========================================================================
    # Non-Decomposable Metrics (Aggregability: NONE)
    # These cannot be pre-aggregated and require full dataset access
    # =========================================================================
    (
        "/nodes/metric/",
        {
            "name": "v3.top_product_by_revenue",
            "description": "Product ID with highest line total (non-decomposable MAX_BY)",
            "query": "SELECT MAX_BY(product_id, line_total) FROM v3.order_details",
            "mode": "published",
        },
    ),
    # Conditional Aggregation (SUM with CASE WHEN)
    (
        "/nodes/metric/",
        {
            "name": "v3.completed_order_revenue",
            "description": "Revenue from completed orders only (conditional aggregation)",
            "query": "SELECT SUM(CASE WHEN status = 'completed' THEN line_total ELSE 0 END) FROM v3.order_details",
            "mode": "published",
        },
    ),
    # Multiple Aggregations in One Metric (MAX - MIN)
    (
        "/nodes/metric/",
        {
            "name": "v3.price_spread",
            "description": "Difference between max and min unit price",
            "query": "SELECT MAX(unit_price) - MIN(unit_price) FROM v3.order_details",
            "mode": "published",
        },
    ),
    # =========================================================================
    # Complex Derived Metric: Combines multiple base metrics
    # Uses price_spread (multi-component) and avg_unit_price (also multi-component)
    # This tests derived metric that references a multi-component metric
    (
        "/nodes/metric/",
        {
            "name": "v3.price_spread_pct",
            "description": "Price spread as percentage of average price",
            "query": "SELECT (v3.max_unit_price - v3.min_unit_price) / NULLIF(v3.avg_unit_price, 0) * 100",
            "mode": "published",
        },
    ),
    # =========================================================================
    # Derived Metrics - Period-over-Period (Window Functions)
    # These have aggregability: NONE due to window functions
    # required_dimensions specifies which dimensions MUST be in the grain
    # =========================================================================
    (
        "/nodes/metric/",
        {
            "name": "v3.wow_revenue_change",
            "description": "Week-over-week revenue change (%) - requires week dimension",
            "query": """
                SELECT
                    (v3.total_revenue - LAG(v3.total_revenue, 1) OVER (ORDER BY v3.date.week[order]))
                    / NULLIF(LAG(v3.total_revenue, 1) OVER (ORDER BY v3.date.week[order]), 0) * 100
            """,
            "mode": "published",
            "required_dimensions": ["v3.date.week[order]"],
        },
    ),
    (
        "/nodes/metric/",
        {
            "name": "v3.wow_order_growth",
            "description": "Week-over-week order count change (%) - requires week dimension",
            "query": """
                SELECT
                    (CAST(v3.order_count AS DOUBLE) - LAG(CAST(v3.order_count AS DOUBLE), 1) OVER (ORDER BY v3.date.week[order]))
                    / NULLIF(LAG(CAST(v3.order_count AS DOUBLE), 1) OVER (ORDER BY v3.date.week[order]), 0) * 100
            """,
            "mode": "published",
            "required_dimensions": ["v3.date.week[order]"],
        },
    ),
    (
        "/nodes/metric/",
        {
            "name": "v3.mom_revenue_change",
            "description": "Month-over-month revenue change (%) - requires month dimension",
            "query": """
                SELECT
                    (v3.total_revenue - LAG(v3.total_revenue, 1) OVER (ORDER BY v3.date.month))
                    / NULLIF(LAG(v3.total_revenue, 1) OVER (ORDER BY v3.date.month), 0) * 100
            """,
            "mode": "published",
            "required_dimensions": ["v3.date.month"],
        },
    ),
    # =========================================================================
    # Rolling/Trailing Period Metrics
    # These compute rolling sums and compare periods using frame clauses
    # Output: one row per day (not per week/month)
    # =========================================================================
    (
        "/nodes/metric/",
        {
            "name": "v3.trailing_7d_revenue",
            "description": "Trailing 7-day revenue (rolling sum of last 7 days)",
            "query": """
                SELECT
                    SUM(v3.total_revenue) OVER (
                        ORDER BY v3.date.date_id[order]
                        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                    )
            """,
            "mode": "published",
            "required_dimensions": ["v3.date.date_id[order]"],
        },
    ),
    (
        "/nodes/metric/",
        {
            "name": "v3.trailing_wow_revenue_change",
            "description": (
                "Trailing week-over-week revenue change (%). "
                "Compares last 7 days to previous 7 days. Output: one row per day."
            ),
            "query": """
                SELECT
                    (
                        SUM(v3.total_revenue) OVER (
                            ORDER BY v3.date.date_id[order]
                            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                        )
                        - SUM(v3.total_revenue) OVER (
                            ORDER BY v3.date.date_id[order]
                            ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING
                        )
                    ) / NULLIF(
                        SUM(v3.total_revenue) OVER (
                            ORDER BY v3.date.date_id[order]
                            ROWS BETWEEN 13 PRECEDING AND 7 PRECEDING
                        ), 0
                    ) * 100
            """,
            "mode": "published",
            "required_dimensions": ["v3.date.date_id[order]"],
        },
    ),
    # =========================================================================
    # Nested Derived Metrics - Metrics referencing other derived metrics
    # These test the inline expansion of intermediate derived metrics
    # =========================================================================
    (
        "/nodes/metric/",
        {
            "name": "v3.wow_aov_change",
            "description": (
                "Week-over-week average order value change (%). "
                "References v3.avg_order_value which is itself derived from "
                "v3.total_revenue / v3.order_count. Tests nested derived metric expansion."
            ),
            "query": """
                SELECT
                    (v3.avg_order_value - LAG(v3.avg_order_value, 1) OVER (ORDER BY v3.date.week[order]))
                    / NULLIF(LAG(v3.avg_order_value, 1) OVER (ORDER BY v3.date.week[order]), 0) * 100
            """,
            "mode": "published",
            "required_dimensions": ["v3.date.week[order]"],
        },
    ),
    (
        "/nodes/metric/",
        {
            "name": "v3.aov_growth_index",
            "description": (
                "Average order value growth index vs baseline. "
                "Non-window derived metric that references v3.avg_order_value (itself a derived metric)."
            ),
            "query": "SELECT v3.avg_order_value / 50.0 * 100",
            "mode": "published",
        },
    ),
    (
        "/nodes/metric/",
        {
            "name": "v3.efficiency_ratio",
            "description": (
                "Revenue efficiency ratio: avg_order_value / pages_per_session. "
                "Tests nested derived metric referencing TWO derived metrics from DIFFERENT facts."
            ),
            "query": "SELECT v3.avg_order_value / NULLIF(v3.pages_per_session, 0)",
            "mode": "published",
        },
    ),
)

EXAMPLES = {  # type: ignore
    "ROADS": ROADS,
    "NAMESPACED_ROADS": NAMESPACED_ROADS,
    "ACCOUNT_REVENUE": ACCOUNT_REVENUE,
    "BASIC": BASIC,
    "BASIC_IN_DIFFERENT_CATALOG": BASIC_IN_DIFFERENT_CATALOG,
    "EVENT": EVENT,
    "DBT": DBT,
    "LATERAL_VIEW": LATERAL_VIEW,
    "DIMENSION_LINK": DIMENSION_LINK,
    "SIMPLE_HLL": SIMPLE_HLL,
    "DERIVED_METRICS": DERIVED_METRICS,
    "BUILD_V3": BUILD_V3,
}


COLUMN_MAPPINGS = {
    "public.basic.comments": [
        Column(name="id", type=IntegerType(), order=0),
        Column(name="user_id", type=IntegerType(), order=1),
        Column(name="timestamp", type=TimestampType(), order=2),
        Column(name="text", type=StringType(), order=3),
    ],
    "default.roads.repair_orders": [
        Column(name="repair_order_id", type=IntegerType(), order=0),
        Column(name="municipality_id", type=StringType(), order=1),
        Column(name="hard_hat_id", type=IntegerType(), order=2),
        Column(name="order_date", type=TimestampType(), order=3),
        Column(name="required_date", type=TimestampType(), order=4),
        Column(name="dispatched_date", type=TimestampType(), order=5),
        Column(name="dispatcher_id", type=IntegerType(), order=6),
        Column(name="rating", type=IntegerType(), order=7),
    ],
    "default.roads.repair_orders_view": [
        Column(name="repair_order_id", type=IntegerType(), order=0),
        Column(name="municipality_id", type=StringType(), order=1),
        Column(name="hard_hat_id", type=IntegerType(), order=2),
        Column(name="order_date", type=TimestampType(), order=3),
        Column(name="required_date", type=TimestampType(), order=4),
        Column(name="dispatched_date", type=TimestampType(), order=5),
        Column(name="dispatcher_id", type=IntegerType(), order=6),
        Column(name="rating", type=IntegerType(), order=7),
    ],
    "default.roads.municipality": [
        Column(name="municipality_id", type=StringType(), order=0),
        Column(name="contact_name", type=StringType(), order=1),
        Column(name="contact_title", type=StringType(), order=2),
        Column(name="local_region", type=StringType(), order=3),
        Column(name="phone", type=StringType(), order=4),
        Column(name="state_id", type="int", order=5),
    ],
    "default.roads.repair_order_details": [
        Column(name="repair_order_id", type=IntegerType(), order=0),
        Column(name="repair_type_id", type=IntegerType(), order=1),
        Column(name="price", type=FloatType(), order=2),
        Column(name="quantity", type=IntegerType(), order=3),
        Column(name="discount", type=FloatType(), order=4),
    ],
    "default.roads.repair_type": [
        Column(name="repair_type_id", type=IntegerType(), order=0),
        Column(name="repair_type_name", type=StringType(), order=1),
        Column(name="contractor_id", type=IntegerType(), order=2),
    ],
    "default.roads.contractors": [
        Column(name="contractor_id", type=IntegerType(), order=0),
        Column(name="company_name", type=StringType(), order=1),
        Column(name="contact_name", type=StringType(), order=2),
        Column(name="contact_title", type=StringType(), order=3),
        Column(name="address", type=StringType(), order=4),
        Column(name="city", type=StringType(), order=5),
        Column(name="state", type=StringType(), order=6),
        Column(name="postal_code", type=StringType(), order=7),
        Column(name="country", type=StringType(), order=8),
        Column(name="phone", type=StringType(), order=9),
    ],
    "default.roads.municipality_municipality_type": [
        Column(name="municipality_id", type=StringType(), order=0),
        Column(name="municipality_type_id", type=StringType(), order=1),
    ],
    "default.roads.municipality_type": [
        Column(name="municipality_type_id", type=StringType(), order=0),
        Column(name="municipality_type_desc", type=StringType(), order=1),
    ],
    "default.roads.dispatchers": [
        Column(name="dispatcher_id", type=IntegerType(), order=0),
        Column(name="company_name", type=StringType(), order=1),
        Column(name="phone", type=StringType(), order=2),
    ],
    "default.roads.hard_hats": [
        Column(name="hard_hat_id", type=IntegerType(), order=0),
        Column(name="last_name", type=StringType(), order=1),
        Column(name="first_name", type=StringType(), order=2),
        Column(name="title", type=StringType(), order=3),
        Column(name="birth_date", type=TimestampType(), order=4),
        Column(name="hire_date", type=TimestampType(), order=5),
        Column(name="address", type=StringType(), order=6),
        Column(name="city", type=StringType(), order=7),
        Column(name="state", type=StringType(), order=8),
        Column(name="postal_code", type=StringType(), order=9),
        Column(name="country", type=StringType(), order=10),
        Column(name="manager", type=IntegerType(), order=11),
        Column(name="contractor_id", type=IntegerType(), order=12),
    ],
    "default.roads.hard_hat_state": [
        Column(name="hard_hat_id", type=IntegerType(), order=0),
        Column(name="state_id", type=StringType(), order=1),
    ],
    "default.roads.us_states": [
        Column(name="state_id", type=IntegerType(), order=0),
        Column(name="state_name", type=StringType(), order=1),
        Column(name="state_abbr", type=StringType(), order=2),
        Column(name="state_region", type=IntegerType(), order=3),
    ],
    "default.roads.us_region": [
        Column(name="us_region_id", type=IntegerType(), order=0),
        Column(name="us_region_description", type=StringType(), order=1),
    ],
    "public.main.view_foo": [
        Column(name="one", type=IntegerType(), order=0),
        Column(name="two", type=StringType(), order=1),
    ],
    "dj_metadata.public.node": [
        Column(name="name", type=StringType(), order=0),
        Column(name="type", type=StringType(), order=1),
        Column(name="display_name", type=StringType(), order=2),
        Column(name="created_at", type=TimestampType(), order=3),
        Column(name="deactivated_at", type=TimestampType(), order=4),
        Column(name="id", type=IntegerType(), order=5),
        Column(name="namespace", type=StringType(), order=6),
        Column(name="current_version", type=StringType(), order=7),
        Column(name="missing_table", type=BooleanType(), order=8),
        Column(name="created_by_id", type=TimestampType(), order=9),
    ],
    "dj_metadata.public.noderevision": [
        Column(name="name", type=StringType(), order=0),
        Column(name="display_name", type=StringType(), order=1),
        Column(name="type", type=StringType(), order=2),
        Column(name="updated_at", type=TimestampType(), order=3),
        Column(name="lineage", type=StringType(), order=4),
        Column(name="description", type=StringType(), order=5),
        Column(name="query", type=StringType(), order=6),
        Column(name="mode", type=StringType(), order=7),
        Column(name="id", type=IntegerType(), order=8),
        Column(name="version", type=StringType(), order=9),
        Column(name="node_id", type=IntegerType(), order=10),
        Column(name="catalog_id", type=IntegerType(), order=11),
        Column(name="schema_", type=StringType(), order=12),
        Column(name="table", type=StringType(), order=13),
        Column(name="metric_metadata_id", type=IntegerType(), order=14),
        Column(name="status", type=StringType(), order=15),
        Column(name="created_by_id", type=IntegerType(), order=16),
        Column(name="query_ast", type=BinaryType(), order=17),
        Column(name="custom_metadata", type=StringType(), order=18),
    ],
    # =========================================================================
    # DERIVED_METRICS canonical example set columns
    # =========================================================================
    "default.derived.orders": [
        Column(name="order_id", type=IntegerType(), order=0),
        Column(name="amount", type=FloatType(), order=1),
        Column(name="customer_id", type=IntegerType(), order=2),
        Column(name="order_date", type=IntegerType(), order=3),
    ],
    "default.derived.events": [
        Column(name="event_id", type=IntegerType(), order=0),
        Column(name="page_views", type=IntegerType(), order=1),
        Column(name="customer_id", type=IntegerType(), order=2),
        Column(name="event_date", type=IntegerType(), order=3),
    ],
    "default.derived.inventory": [
        Column(name="inventory_id", type=IntegerType(), order=0),
        Column(name="quantity", type=IntegerType(), order=1),
        Column(name="warehouse_id", type=IntegerType(), order=2),
        Column(name="inventory_date", type=IntegerType(), order=3),
    ],
    "default.derived.dates": [
        Column(name="date_id", type=IntegerType(), order=0),
        Column(name="date_value", type=TimestampType(), order=1),
        Column(name="week", type=IntegerType(), order=2),
        Column(name="month", type=IntegerType(), order=3),
        Column(name="year", type=IntegerType(), order=4),
    ],
    "default.derived.customers": [
        Column(name="customer_id", type=IntegerType(), order=0),
        Column(name="name", type=StringType(), order=1),
        Column(name="email", type=StringType(), order=2),
    ],
    "default.derived.warehouses": [
        Column(name="warehouse_id", type=IntegerType(), order=0),
        Column(name="name", type=StringType(), order=1),
        Column(name="location", type=StringType(), order=2),
    ],
}

QUERY_DATA_MAPPINGS = {
    (
        "WITHdefault_DOT_repair_ordersAS(SELECTdefault_DOT_dispatcher.company_namedefault_DOT_dispatcher_DOT_company_name,count(default_DOT_repair_orders.repair_order_id)default_DOT_num_repair_ordersFROMroads.repair_ordersASdefault_DOT_repair_ordersLEFTOUTERJOIN(SELECTdefault_DOT_repair_orders.dispatcher_id,default_DOT_repair_orders.hard_hat_id,default_DOT_repair_orders.municipality_id,default_DOT_repair_orders.repair_order_idFROMroads.repair_ordersASdefault_DOT_repair_orders)ASdefault_DOT_repair_orderONdefault_DOT_repair_orders.repair_order_id=default_DOT_repair_order.repair_order_idLEFTOUTERJOIN(SELECTdefault_DOT_dispatchers.company_name,default_DOT_dispatchers.dispatcher_idFROMroads.dispatchersASdefault_DOT_dispatchers)ASdefault_DOT_dispatcherONdefault_DOT_repair_order.dispatcher_id=default_DOT_dispatcher.dispatcher_idGROUPBYdefault_DOT_dispatcher.company_name),default_DOT_repair_order_detailsAS(SELECTdefault_DOT_dispatcher.company_namedefault_DOT_dispatcher_DOT_company_name,avg(default_DOT_repair_order_details.price)ASdefault_DOT_avg_repair_priceFROMroads.repair_order_detailsASdefault_DOT_repair_order_detailsLEFTOUTERJOIN(SELECTdefault_DOT_repair_orders.dispatcher_id,default_DOT_repair_orders.hard_hat_id,default_DOT_repair_orders.municipality_id,default_DOT_repair_orders.repair_order_idFROMroads.repair_ordersASdefault_DOT_repair_orders)ASdefault_DOT_repair_orderONdefault_DOT_repair_order_details.repair_order_id=default_DOT_repair_order.repair_order_idLEFTOUTERJOIN(SELECTdefault_DOT_dispatchers.company_name,default_DOT_dispatchers.dispatcher_idFROMroads.dispatchersASdefault_DOT_dispatchers)ASdefault_DOT_dispatcherONdefault_DOT_repair_order.dispatcher_id=default_DOT_dispatcher.dispatcher_idGROUPBYdefault_DOT_dispatcher.company_name)SELECTdefault_DOT_repair_orders.default_DOT_num_repair_orders,default_DOT_repair_order_details.default_DOT_avg_repair_price,COALESCE(default_DOT_repair_orders.default_DOT_dispatcher_DOT_company_name,default_DOT_repair_order_details.default_DOT_dispatcher_DOT_company_name)default_DOT_dispatcher_DOT_company_nameFROMdefault_DOT_repair_ordersFULLOUTERJOINdefault_DOT_repair_order_detailsONdefault_DOT_repair_orders.default_DOT_dispatcher_DOT_company_name=default_DOT_repair_order_details.default_DOT_dispatcher_DOT_company_nameLIMIT10"
    )
    .strip()
    .replace('"', "")
    .replace("\n", "")
    .replace("\t", "")
    .replace(" ", ""): QueryWithResults(
        **{
            "id": "bd98d6be-e2d2-413e-94c7-96d9411ddee2",
            "submitted_query": (
                "SELECT  avg(repair_order_details.price) AS "
                "default_DOT_avg_repair_price,\\n\\tdispatcher.company_name,"
                "\\n\\tcount(repair_orders.repair_order_id) AS default_DOT_num_repair_orders"
                "default_DOT_num_repair_orders \\n FROM roads.repair_order_details AS "
                "repair_order_details LEFT OUTER JOIN (SELECT  "
                "repair_orders.dispatcher_id,\\n\\trepair_orders.hard_hat_id,\\n\\t"
                "repair_orders.municipality_id,\\n\\trepair_orders.repair_order_id "
                "\\n FROM roads.repair_orders AS repair_orders) AS repair_order ON "
                "repair_order_details.repair_order_id = repair_order.repair_order_id\\nLEFT "
                "OUTER JOIN (SELECT  dispatchers.company_name,\\n\\tdispatchers.dispatcher_id "
                "\\n FROM roads.dispatchers AS dispatchers) AS dispatcher ON "
                "repair_order.dispatcher_id = dispatcher.dispatcher_id \\n GROUP BY  "
                "dispatcher.company_name\\nLIMIT 10"
            ),
            "state": QueryState.FINISHED,
            "results": [
                {
                    "columns": [
                        {
                            "name": "default_DOT_num_repair_orders",
                            "type": "int",
                            "semantic_entity": "default.num_repair_orders",
                            "semantic_type": "metric",
                        },
                        {
                            "name": "default_DOT_avg_repair_price",
                            "type": "float",
                            "semantic_entity": "default.avg_repair_price",
                            "semantic_type": "metric",
                        },
                        {
                            "name": "company_name",
                            "type": "str",
                            "semantic_entity": "default.dispatcher.company_name",
                            "semantic_type": "dimension",
                        },
                    ],
                    "rows": [
                        (1.0, "Foo", 100),
                        (2.0, "Bar", 200),
                    ],
                    "sql": "",
                },
            ],
            "errors": [],
        },
    ),
}
