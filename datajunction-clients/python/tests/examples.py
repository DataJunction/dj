"""
Roads database examples loaded into DJ test session
"""

from typing import Dict, Union

from datajunction_server.database.column import Column
from datajunction_server.errors import DJException, DJQueryServiceClientException
from datajunction_server.models.query import QueryWithResults
from datajunction_server.sql.parsing.types import IntegerType, StringType, TimestampType
from datajunction_server.typing import QueryState

# pylint: disable=too-many-lines

EXAMPLES = (  # type: ignore
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
            "description": "All repair orders",
            "mode": "published",
            "name": "default.repair_orders_foo",
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
            "description": "US state dimension",
            "query": """
                        SELECT
                        state_id,
                        state_name,
                        state_abbr AS state_short,
                        state_region,
                        r.us_region_description AS state_region_description
                        FROM default.us_states s
                        LEFT JOIN default.us_region r
                        ON s.state_region = r.us_region_id
                    """,
            "mode": "published",
            "name": "default.us_state",
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
        "/nodes/metric/",
        {
            "description": "Number of repair orders",
            "query": ("SELECT count(repair_order_id) FROM default.repair_orders"),
            "mode": "published",
            "name": "default.num_repair_orders",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Average repair price",
            "query": (
                "SELECT avg(price) as default_DOT_avg_repair_price "
                "FROM default.repair_order_details"
            ),
            "mode": "published",
            "name": "default.avg_repair_price",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Total repair cost",
            "query": (
                "SELECT sum(price) as default_DOT_total_repair_cost "
                "FROM default.repair_order_details"
            ),
            "mode": "published",
            "name": "default.total_repair_cost",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Average length of employment",
            "query": (
                "SELECT avg(NOW() - hire_date) as default_DOT_avg_length_of_employment "
                "FROM default.hard_hats"
            ),
            "mode": "published",
            "name": "default.avg_length_of_employment",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Total repair order discounts",
            "query": ("SELECT sum(price * discount) FROM default.repair_order_details"),
            "mode": "published",
            "name": "default.total_repair_order_discounts",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Total repair order discounts",
            "query": ("SELECT avg(price * discount) FROM default.repair_order_details"),
            "mode": "published",
            "name": "default.avg_repair_order_discounts",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Average time to dispatch a repair order",
            "query": (
                "SELECT avg(dispatched_date - order_date) FROM default.repair_orders"
            ),
            "mode": "published",
            "name": "default.avg_time_to_dispatch",
        },
    ),
    (
        (
            "/nodes/default.repair_order_details/columns/repair_order_id/"
            "?dimension=default.repair_order&dimension_column=repair_order_id"
        ),
        {},
    ),
    (
        (
            "/nodes/default.repair_orders/columns/municipality_id/"
            "?dimension=default.municipality_dim&dimension_column=municipality_id"
        ),
        {},
    ),
    (
        (
            "/nodes/default.repair_type/columns/contractor_id/"
            "?dimension=default.contractor&dimension_column=contractor_id"
        ),
        {},
    ),
    (
        (
            "/nodes/default.repair_orders/columns/hard_hat_id/"
            "?dimension=default.hard_hat&dimension_column=hard_hat_id"
        ),
        {},
    ),
    (
        (
            "/nodes/default.repair_orders/columns/dispatcher_id/"
            "?dimension=default.dispatcher&dimension_column=dispatcher_id"
        ),
        {},
    ),
    (
        (
            "/nodes/default.hard_hat/columns/state/"
            "?dimension=default.us_state&dimension_column=state_short"
        ),
        {},
    ),
    (
        (
            "/nodes/default.repair_order_details/columns/repair_order_id/"
            "?dimension=default.repair_order&dimension_column=repair_order_id"
        ),
        {},
    ),
    (
        (
            "/nodes/default.repair_order/columns/dispatcher_id/"
            "?dimension=default.dispatcher&dimension_column=dispatcher_id"
        ),
        {},
    ),
    (
        (
            "/nodes/default.repair_order/columns/hard_hat_id/"
            "?dimension=default.hard_hat&dimension_column=hard_hat_id"
        ),
        {},
    ),
    (
        (
            "/nodes/default.repair_order/columns/municipality_id/"
            "?dimension=default.municipality_dim&dimension_column=municipality_id"
        ),
        {},
    ),
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
            "query": ("SELECT count(repair_order_id) FROM foo.bar.repair_orders"),
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
            "/nodes/foo.bar.repair_orders/columns/municipality_id/"
            "?dimension=foo.bar.municipality_dim&dimension_column=municipality_id"
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
            "/nodes/foo.bar.repair_orders/columns/hard_hat_id/"
            "?dimension=foo.bar.hard_hat&dimension_column=hard_hat_id"
        ),
        {},
    ),
    (
        (
            "/nodes/foo.bar.repair_orders/columns/dispatcher_id/"
            "?dimension=foo.bar.dispatcher&dimension_column=dispatcher_id"
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
    (
        "/nodes/cube/",
        {
            "description": "Cube #1 for metrics and dimensions.",
            "mode": "published",
            "name": "foo.bar.cube_one",
            "metrics": ["foo.bar.num_repair_orders"],
            "dimensions": ["foo.bar.municipality_dim.local_region"],
        },
    ),
    (
        "/nodes/cube/",
        {
            "description": "Cube #2 for metrics and dimensions.",
            "mode": "published",
            "name": "default.cube_two",
            "metrics": ["default.num_repair_orders"],
            "dimensions": ["default.municipality_dim.local_region"],
        },
    ),
    (
        "/nodes/transform/",
        {
            "description": "3 columns from default.repair_orders",
            "query": (
                "SELECT repair_order_id, municipality_id, hard_hat_id "
                "FROM default.repair_orders"
            ),
            "mode": "published",
            "name": "default.repair_orders_thin",
        },
    ),
    (
        "/nodes/transform/",
        {
            "description": "3 columns from foo.bar.repair_orders",
            "query": (
                "SELECT repair_order_id, municipality_id, hard_hat_id "
                "FROM foo.bar.repair_orders"
            ),
            "mode": "published",
            "name": "foo.bar.repair_orders_thin",
        },
    ),
    (
        "/nodes/transform/",
        {
            "description": "node with custom metadata",
            "query": (
                "SELECT repair_order_id, municipality_id, hard_hat_id "
                "FROM foo.bar.repair_orders"
            ),
            "mode": "published",
            "name": "foo.bar.with_custom_metadata",
            "custom_metadata": {"foo": "bar"},
        },
    ),
)

COLUMN_MAPPINGS = {
    "default.roads.repair_orders": [
        Column(name="id", type=IntegerType()),
        Column(name="user_id", type=IntegerType()),
        Column(name="timestamp", type=TimestampType()),
        Column(name="text", type=StringType()),
    ],
    "default.store.comments": [
        Column(name="id", type=IntegerType()),
        Column(name="user_id", type=IntegerType()),
        Column(name="timestamp", type=TimestampType()),
        Column(name="text", type=StringType()),
    ],
    "default.store.comments_view": [
        Column(name="id", type=IntegerType()),
        Column(name="user_id", type=IntegerType()),
        Column(name="timestamp", type=TimestampType()),
        Column(name="text", type=StringType()),
    ],
}

QUERY_DATA_MAPPINGS: Dict[str, Union[DJException, QueryWithResults]] = {
    "WITHdefault_DOT_repair_order_detailsAS(SELECTdefault_DOT_repair_order_details."
    "repair_order_id,\tdefault_DOT_repair_order_details.repair_type_id,\tdefault_DOT"
    "_repair_order_details.price,\tdefault_DOT_repair_order_details.quantity,\tdefault"
    "_DOT_repair_order_details.discountFROMroads.repair_order_detailsASdefault_DOT_"
    "repair_order_details),default_DOT_repair_orderAS(SELECTdefault_DOT_repair_orders"
    ".repair_order_id,\tdefault_DOT_repair_orders.municipality_id,\tdefault_DOT_repair"
    "_orders.hard_hat_id,\tdefault_DOT_repair_orders.order_date,\tdefault_DOT_repair_"
    "orders.required_date,\tdefault_DOT_repair_orders.dispatched_date,\tdefault_DOT_"
    "repair_orders.dispatcher_idFROMroads.repair_ordersASdefault_DOT_repair_orders),"
    "default_DOT_hard_hatAS(SELECTdefault_DOT_hard_hats.hard_hat_id,\tdefault_DOT_hard"
    "_hats.last_name,\tdefault_DOT_hard_hats.first_name,\tdefault_DOT_hard_hats.title,"
    "\tdefault_DOT_hard_hats.birth_date,\tdefault_DOT_hard_hats.hire_date,\tdefault_"
    "DOT_hard_hats.address,\tdefault_DOT_hard_hats.city,\tdefault_DOT_hard_hats.state,"
    "\tdefault_DOT_hard_hats.postal_code,\tdefault_DOT_hard_hats.country,\tdefault_DOT"
    "_hard_hats.manager,\tdefault_DOT_hard_hats.contractor_idFROMroads.hard_hatsASdefault"
    "_DOT_hard_hats),default_DOT_repair_order_details_metricsAS(SELECTdefault_DOT_hard_"
    "hat.citydefault_DOT_hard_hat_DOT_city,\tavg(default_DOT_repair_order_details.price)"
    "ASdefault_DOT_avg_repair_priceFROMdefault_DOT_repair_order_detailsLEFTJOINdefault_"
    "DOT_repair_orderONdefault_DOT_repair_order_details.repair_order_id=default_DOT_"
    "repair_order.repair_order_idLEFTJOINdefault_DOT_hard_hatONdefault_DOT_repair_order"
    ".hard_hat_id=default_DOT_hard_hat.hard_hat_idGROUPBYdefault_DOT_hard_hat.city)"
    "SELECTdefault_DOT_repair_order_details_metrics.default_DOT_hard_hat_DOT_city,\t"
    "default_DOT_repair_order_details_metrics.default_DOT_avg_repair_priceFROMdefault"
    "_DOT_repair_order_details_metrics"
    "": QueryWithResults(
        **{
            "id": "bd98d6be-e2d2-413e-94c7-96d9411ddee2",
            "submitted_query": "...",
            "state": QueryState.FINISHED,
            "results": [
                {
                    "columns": [
                        {"name": "default_DOT_hard_hat_DOT_city", "type": "str"},
                        {"name": "default_DOT_avg_repair_price", "type": "float"},
                    ],
                    "rows": [
                        ("Foo", 1.0),
                        ("Bar", 2.0),
                    ],
                    "sql": "",
                },
            ],
            "errors": [],
        },
    ),
    "WITHdefault_DOT_repair_order_detailsAS(SELECTdefault_DOT_repair_order_details."
    "repair_order_id,\tdefault_DOT_repair_order_details.repair_type_id,\tdefault_DOT_repair_order"
    "_details.price,\tdefault_DOT_repair_order_details.quantity,\tdefault_DOT_repair_order_details"
    ".discountFROMroads.repair_order_detailsASdefault_DOT_repair_order_details),default_DOT_repair"
    "_orderAS(SELECTdefault_DOT_repair_orders.repair_order_id,\tdefault_DOT_repair_orders.municipa"
    "lity_id,\tdefault_DOT_repair_orders.hard_hat_id,\tdefault_DOT_repair_orders.order_date,\tdef"
    "ault_DOT_repair_orders.required_date,\tdefault_DOT_repair_orders.dispatched_date,\tdefault_"
    "DOT_repair_orders.dispatcher_idFROMroads.repair_ordersASdefault_DOT_repair_orders),default_"
    "DOT_hard_hatAS(SELECTdefault_DOT_hard_hats.hard_hat_id,\tdefault_DOT_hard_hats.last_name,\t"
    "default_DOT_hard_hats.first_name,\tdefault_DOT_hard_hats.title,\tdefault_DOT_hard_hats.birth"
    "_date,\tdefault_DOT_hard_hats.hire_date,\tdefault_DOT_hard_hats.address,\tdefault_DOT_hard_"
    "hats.city,\tdefault_DOT_hard_hats.state,\tdefault_DOT_hard_hats.postal_code,\tdefault_DOT_"
    "hard_hats.country,\tdefault_DOT_hard_hats.manager,\tdefault_DOT_hard_hats.contractor_idFROM"
    "roads.hard_hatsASdefault_DOT_hard_hats),default_DOT_repair_order_details_metricsAS(SELECT"
    "default_DOT_hard_hat.statedefault_DOT_hard_hat_DOT_state,\tavg(default_DOT_repair_order_"
    "details.price)ASdefault_DOT_avg_repair_priceFROMdefault_DOT_repair_order_detailsLEFTJOIN"
    "default_DOT_repair_orderONdefault_DOT_repair_order_details.repair_order_id=default_DOT_"
    "repair_order.repair_order_idLEFTJOINdefault_DOT_hard_hatONdefault_DOT_repair_order.hard"
    "_hat_id=default_DOT_hard_hat.hard_hat_idGROUPBYdefault_DOT_hard_hat.state)SELECTdefault_"
    "DOT_repair_order_details_metrics.default_DOT_hard_hat_DOT_state,\tdefault_DOT_repair_order"
    "_details_metrics.default_DOT_avg_repair_priceFROMdefault_DOT_repair_order_details_metrics": (
        QueryWithResults(
            **{
                "id": "bd98d6be-e2d2-413e-94c7-96d9411ddee2",
                "submitted_query": "...",
                "state": QueryState.FINISHED,
                "results": [],
                "errors": [],
            },
        )
    ),
    "WITHdefault_DOT_repair_order_detailsAS(SELECTdefault_DOT_hard_hat.postal_codedefault_"
    "DOT_hard_hat_DOT_postal_code,\tavg(default_DOT_repair_order_details.price)ASdefault_"
    "DOT_avg_repair_priceFROMroads.repair_order_detailsASdefault_DOT_repair_order_details"
    "LEFTJOIN(SELECTdefault_DOT_repair_orders.repair_order_id,\tdefault_DOT_repair_"
    "orders.municipality_id,\tdefault_DOT_repair_orders.hard_hat_id,\tdefault_DOT_repair_"
    "orders.dispatcher_idFROMroads.repair_ordersASdefault_DOT_repair_orders)ASdefault_DOT_"
    "repair_orderONdefault_DOT_repair_order_details.repair_order_id=default_DOT_repair_order"
    ".repair_order_idLEFTJOIN(SELECTdefault_DOT_hard_hats.hard_hat_id,\tdefault_DOT_hard"
    "_hats.state,\tdefault_DOT_hard_hats.postal_codeFROMroads.hard_hatsASdefault_DOT_hard_hats"
    ")ASdefault_DOT_hard_hatONdefault_DOT_repair_order.hard_hat_id=default_DOT_hard_hat.hard_"
    "hat_idGROUPBYdefault_DOT_hard_hat.postal_code)SELECTdefault_DOT_repair_order_details."
    "default_DOT_avg_repair_price,\tdefault_DOT_repair_order_details.default_DOT_hard_hat_DOT"
    "_postal_codeFROMdefault_DOT_repair_order_details": (
        DJQueryServiceClientException("Error response from query service")
    ),
    "WITHdefault_DOT_repair_order_detailsAS(SELECTdefault_DOT_repair_order_details.repair_"
    "order_id,\tdefault_DOT_repair_order_details.repair_type_id,\tdefault_DOT_repair_order_"
    "details.price,\tdefault_DOT_repair_order_details.quantity,\tdefault_DOT_repair_order_"
    "details.discountFROMroads.repair_order_detailsASdefault_DOT_repair_order_details),"
    "default_DOT_repair_orderAS(SELECTdefault_DOT_repair_orders.repair_order_id,\tdefault"
    "_DOT_repair_orders.municipality_id,\tdefault_DOT_repair_orders.hard_hat_id,\tdefault"
    "_DOT_repair_orders.order_date,\tdefault_DOT_repair_orders.required_date,\tdefault_DOT"
    "_repair_orders.dispatched_date,\tdefault_DOT_repair_orders.dispatcher_idFROMroads.repair"
    "_ordersASdefault_DOT_repair_orders),default_DOT_hard_hatAS(SELECTdefault_DOT_hard_hats."
    "hard_hat_id,\tdefault_DOT_hard_hats.last_name,\tdefault_DOT_hard_hats.first_name,\t"
    "default_DOT_hard_hats.title,\tdefault_DOT_hard_hats.birth_date,\tdefault_DOT_hard_hats"
    ".hire_date,\tdefault_DOT_hard_hats.address,\tdefault_DOT_hard_hats.city,\tdefault_DOT_"
    "hard_hats.state,\tdefault_DOT_hard_hats.postal_code,\tdefault_DOT_hard_hats.country,\t"
    "default_DOT_hard_hats.manager,\tdefault_DOT_hard_hats.contractor_idFROMroads.hard_hats"
    "ASdefault_DOT_hard_hats)SELECTdefault_DOT_hard_hat.citydefault_DOT_hard_hat_DOT_city,"
    "\tavg(default_DOT_repair_order_details.price)ASdefault_DOT_avg_repair_priceFROMdefault"
    "_DOT_repair_order_detailsLEFTJOINdefault_DOT_repair_orderONdefault_DOT_repair_order_"
    "details.repair_order_id=default_DOT_repair_order.repair_order_idLEFTJOINdefault_DOT_"
    "hard_hatONdefault_DOT_repair_order.hard_hat_id=default_DOT_hard_hat.hard_hat_idGROUP"
    "BYdefault_DOT_hard_hat.city'": QueryWithResults(
        **{
            "id": "bd98d6be-e2d2-413e-94c7-96d9411ddee2",
            "submitted_query": "...",
            "state": QueryState.FINISHED,
            "results": [
                {
                    "columns": [
                        {"name": "default_DOT_avg_repair_price", "type": "float"},
                        {"name": "default_DOT_hard_hat_DOT_city", "type": "str"},
                    ],
                    "rows": [
                        (1.0, "Foo"),
                        (2.0, "Bar"),
                    ],
                    "sql": "",
                },
            ],
            "errors": [],
        },
    ),
    "WITHdefault_DOT_repair_order_detailsAS(SELECTdefault_DOT_repair_order_details."
    "repair_order_id,\tdefault_DOT_repair_order_details.repair_type_id,\tdefault_DOT"
    "_repair_order_details.price,\tdefault_DOT_repair_order_details.quantity,\tdefault"
    "_DOT_repair_order_details.discountFROMroads.repair_order_detailsASdefault_DOT_repair"
    "_order_details),default_DOT_repair_orderAS(SELECTdefault_DOT_repair_orders.repair_"
    "order_id,\tdefault_DOT_repair_orders.municipality_id,\tdefault_DOT_repair_orders.hard"
    "_hat_id,\tdefault_DOT_repair_orders.order_date,\tdefault_DOT_repair_orders.required"
    "_date,\tdefault_DOT_repair_orders.dispatched_date,\tdefault_DOT_repair_orders.dispatcher"
    "_idFROMroads.repair_ordersASdefault_DOT_repair_orders),default_DOT_hard_hatAS(SELECT"
    "default_DOT_hard_hats.hard_hat_id,\tdefault_DOT_hard_hats.last_name,\tdefault_DOT_"
    "hard_hats.first_name,\tdefault_DOT_hard_hats.title,\tdefault_DOT_hard_hats.birth_date"
    ",\tdefault_DOT_hard_hats.hire_date,\tdefault_DOT_hard_hats.address,\tdefault_DOT_hard"
    "_hats.city,\tdefault_DOT_hard_hats.state,\tdefault_DOT_hard_hats.postal_code,\tdefault"
    "_DOT_hard_hats.country,\tdefault_DOT_hard_hats.manager,\tdefault_DOT_hard_hats."
    "contractor_idFROMroads.hard_hatsASdefault_DOT_hard_hats)SELECTdefault_DOT_hard_hat"
    ".citydefault_DOT_hard_hat_DOT_city,\tavg(default_DOT_repair_order_details.price)AS"
    "default_DOT_avg_repair_priceFROMdefault_DOT_repair_order_detailsLEFTJOINdefault_DOT"
    "_repair_orderONdefault_DOT_repair_order_details.repair_order_id=default_DOT_repair_"
    "order.repair_order_idLEFTJOINdefault_DOT_hard_hatONdefault_DOT_repair_order.hard_hat"
    "_id=default_DOT_hard_hat.hard_hat_idGROUPBYdefault_DOT_hard_hat.city": QueryWithResults(
        **{
            "id": "bd98d6be-e2d2-413e-94c7-96d9411ddee2",
            "submitted_query": "...",
            "state": QueryState.FINISHED,
            "results": [
                {
                    "columns": [
                        {"name": "default_DOT_hard_hat_DOT_city", "type": "str"},
                        {"name": "default_DOT_avg_repair_price", "type": "float"},
                    ],
                    "rows": [
                        ("Foo", 1.0),
                        ("Bar", 2.0),
                    ],
                    "sql": "",
                },
            ],
            "errors": [],
        },
    ),
    "SELECTavg(default_DOT_repair_order_details.price)ASdefault_DOT_avg_repair_price,\tdefault"
    "_DOT_hard_hat.citydefault_DOT_hard_hat_DOT_cityFROMroads.repair_order_detailsASdefault_"
    "DOT_repair_order_detailsLEFTJOIN(SELECTdefault_DOT_repair_orders.repair_order_id,\tdefault_"
    "DOT_repair_orders.municipality_id,\tdefault_DOT_repair_orders.hard_hat_id,\tdefault_DOT_"
    "repair_orders.dispatcher_idFROMroads.repair_ordersASdefault_DOT_repair_orders)ASdefault_"
    "DOT_repair_orderONdefault_DOT_repair_order_details.repair_order_id=default_DOT_repair_order"
    ".repair_order_idLEFTJOIN(SELECTdefault_DOT_hard_hats.hard_hat_id,\tdefault_DOT_hard_hats."
    "city,\tdefault_DOT_hard_hats.stateFROMroads.hard_hatsASdefault_DOT_hard_hats)ASdefault_DOT_"
    "hard_hatONdefault_DOT_repair_order.hard_hat_id=default_DOT_hard_hat.hard_hat_idGROUPBY"
    "default_DOT_hard_hat.city": QueryWithResults(
        **{
            "id": "bd98d6be-e2d2-413e-94c7-96d9411ddee2",
            "submitted_query": "...",
            "state": QueryState.FINISHED,
            "results": [
                {
                    "columns": [
                        {"name": "default_DOT_avg_repair_price", "type": "float"},
                        {"name": "default_DOT_hard_hat_DOT_city", "type": "str"},
                    ],
                    "rows": [
                        (1.0, "Foo"),
                        (2.0, "Bar"),
                    ],
                    "sql": "",
                },
            ],
            "errors": [],
        },
    ),
}
