# pylint: disable=too-many-lines,C0301

"""
Post requests for all example entities
"""
from datajunction_server.database.column import Column
from datajunction_server.models.query import QueryWithResults
from datajunction_server.sql.parsing.types import IntegerType, StringType, TimestampType
from datajunction_server.typing import QueryState

SERVICE_SETUP = (  # type: ignore
    (
        "/catalogs/",
        {"name": "draft"},
    ),
    (
        "/catalogs/",
        {"name": "default"},
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
                "SELECT avg(CAST(NOW() AS DATE) - hire_date) " "FROM default.hard_hat"
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
            "query": ("SELECT avg(NOW() - hire_date) " "FROM foo.bar.hard_hats"),
            "mode": "published",
            "name": "foo.bar.avg_length_of_employment",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Total repair order discounts",
            "query": (
                "SELECT sum(price * discount) " "FROM foo.bar.repair_order_details"
            ),
            "mode": "published",
            "name": "foo.bar.total_repair_order_discounts",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Total repair order discounts",
            "query": (
                "SELECT avg(price * discount) " "FROM foo.bar.repair_order_details"
            ),
            "mode": "published",
            "name": "foo.bar.avg_repair_order_discounts",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Average time to dispatch a repair order",
            "query": (
                "SELECT avg(dispatched_date - order_date) " "FROM foo.bar.repair_orders"
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
            "catalog": "default",
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
            "catalog": "default",
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
            "catalog": "default",
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
            "query": "SELECT COUNT(DISTINCT device_id) " "FROM default.event_source",
            "mode": "published",
        },
    ),
    (
        "/nodes/metric/",
        {
            "name": "default.long_events_distinct_countries",
            "description": "Number of Distinct Countries for Long Events",
            "query": "SELECT COUNT(DISTINCT country) " "FROM default.long_events",
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
                "SELECT id, first_name, last_name "
                "FROM dbt.source.jaffle_shop.customers"
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
                "SELECT item_name " "account_type_classification FROM default.sales"
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
                age
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
    "public.main.view_foo": [
        Column(name="one", type=IntegerType(), order=0),
        Column(name="two", type=StringType(), order=1),
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
                        {"name": "default_DOT_num_repair_orders", "type": "int"},
                        {"name": "default_DOT_avg_repair_price", "type": "float"},
                        {"name": "company_name", "type": "str"},
                    ],
                    "rows": [
                        (1.0, "Foo", 100),
                        (2.0, "Bar", 200),
                    ],
                    "sql": "",
                },
            ],
            "errors": [],
        }
    ),
}
