"""
Post requests for all example entities
"""

# pylint: disable=too-many-lines
from dj.models import Column
from dj.typing import ColumnType, QueryState

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
        {"name": "spark", "version": "3.1.1"},
    ),
    (
        "/catalogs/default/engines/",
        [{"name": "spark", "version": "3.1.1"}],
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
    (
        "/nodes/source/",
        {
            "columns": {
                "repair_order_id": {"type": "INT"},
                "municipality_id": {"type": "STR"},
                "hard_hat_id": {"type": "INT"},
                "order_date": {"type": "DATETIME"},
                "required_date": {"type": "DATETIME"},
                "dispatched_date": {"type": "DATETIME"},
                "dispatcher_id": {"type": "INT"},
            },
            "description": "All repair orders",
            "mode": "published",
            "name": "repair_orders",
            "catalog": "default",
            "schema_": "roads",
            "table": "repair_orders",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": {
                "repair_order_id": {"type": "INT"},
                "repair_type_id": {"type": "INT"},
                "price": {"type": "FLOAT"},
                "quantity": {"type": "INT"},
                "discount": {"type": "FLOAT"},
            },
            "description": "Details on repair orders",
            "mode": "published",
            "name": "repair_order_details",
            "catalog": "default",
            "schema_": "roads",
            "table": "repair_order_details",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": {
                "repair_type_id": {"type": "INT"},
                "repair_type_name": {"type": "STR"},
                "contractor_id": {"type": "INT"},
            },
            "description": "Information on types of repairs",
            "mode": "published",
            "name": "repair_type",
            "catalog": "default",
            "schema_": "roads",
            "table": "repair_type",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": {
                "contractor_id": {"type": "INT"},
                "company_name": {"type": "STR"},
                "contact_name": {"type": "STR"},
                "contact_title": {"type": "STR"},
                "address": {"type": "STR"},
                "city": {"type": "STR"},
                "state": {"type": "STR"},
                "postal_code": {"type": "STR"},
                "country": {"type": "STR"},
                "phone": {"type": "STR"},
            },
            "description": "Information on contractors",
            "mode": "published",
            "name": "contractors",
            "catalog": "default",
            "schema_": "roads",
            "table": "contractors",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": {
                "municipality_id": {"type": "STR"},
                "municipality_type_id": {"type": "STR"},
            },
            "description": "Lookup table for municipality and municipality types",
            "mode": "published",
            "name": "municipality_municipality_type",
            "catalog": "default",
            "schema_": "roads",
            "table": "municipality_municipality_type",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": {
                "municipality_type_id": {"type": "STR"},
                "municipality_type_desc": {"type": "STR"},
            },
            "description": "Information on municipality types",
            "mode": "published",
            "name": "municipality_type",
            "catalog": "default",
            "schema_": "roads",
            "table": "municipality_type",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": {
                "municipality_id": {"type": "STR"},
                "contact_name": {"type": "STR"},
                "contact_title": {"type": "STR"},
                "local_region": {"type": "STR"},
                "phone": {"type": "STR"},
                "state_id": {"type": "INT"},
            },
            "description": "Information on municipalities",
            "mode": "published",
            "name": "municipality",
            "catalog": "default",
            "schema_": "roads",
            "table": "municipality",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": {
                "dispatcher_id": {"type": "INT"},
                "company_name": {"type": "STR"},
                "phone": {"type": "STR"},
            },
            "description": "Information on dispatchers",
            "mode": "published",
            "name": "dispatchers",
            "catalog": "default",
            "schema_": "roads",
            "table": "dispatchers",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": {
                "hard_hat_id": {"type": "INT"},
                "last_name": {"type": "STR"},
                "first_name": {"type": "STR"},
                "title": {"type": "STR"},
                "birth_date": {"type": "DATETIME"},
                "hire_date": {"type": "DATETIME"},
                "address": {"type": "STR"},
                "city": {"type": "STR"},
                "state": {"type": "STR"},
                "postal_code": {"type": "STR"},
                "country": {"type": "STR"},
                "manager": {"type": "INT"},
                "contractor_id": {"type": "INT"},
            },
            "description": "Information on employees",
            "mode": "published",
            "name": "hard_hats",
            "catalog": "default",
            "schema_": "roads",
            "table": "hard_hats",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": {
                "hard_hat_id": {"type": "INT"},
                "state_id": {"type": "STR"},
            },
            "description": "Lookup table for employee's current state",
            "mode": "published",
            "name": "hard_hat_state",
            "catalog": "default",
            "schema_": "roads",
            "table": "hard_hat_state",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": {
                "state_id": {"type": "INT"},
                "state_name": {"type": "STR"},
                "state_abbr": {"type": "STR"},
                "state_region": {"type": "INT"},
            },
            "description": "Information on different types of repairs",
            "mode": "published",
            "name": "us_states",
            "catalog": "default",
            "schema_": "roads",
            "table": "us_states",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": {
                "us_region_id": {"type": "INT"},
                "us_region_description": {"type": "STR"},
            },
            "description": "Information on US regions",
            "mode": "published",
            "name": "us_region",
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
                        FROM repair_orders
                    """,
            "mode": "published",
            "name": "repair_order",
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
                        FROM contractors
                    """,
            "mode": "published",
            "name": "contractor",
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
                        FROM hard_hats
                    """,
            "mode": "published",
            "name": "hard_hat",
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
                        FROM hard_hats hh
                        LEFT JOIN hard_hat_state hhs
                        ON hh.hard_hat_id = hhs.hard_hat_id
                        WHERE hh.state_id = 'NY'
                    """,
            "mode": "published",
            "name": "local_hard_hats",
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
                        FROM us_states s
                        LEFT JOIN us_region r
                        ON s.state_region = r.us_region_id
                    """,
            "mode": "published",
            "name": "us_state",
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
                        FROM dispatchers
                    """,
            "mode": "published",
            "name": "dispatcher",
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "Municipality dimension",
            "query": """
                        SELECT
                        m.municipality_id,
                        contact_name,
                        contact_title,
                        local_region,
                        phone,
                        state_id,
                        mmt.municipality_type_id,
                        mt.municipality_type_desc
                        FROM municipality AS m
                        LEFT JOIN municipality_municipality_type AS mmt
                        ON m.municipality_id = mmt.municipality_id
                        LEFT JOIN municipality_type AS mt
                        ON mmt.municipality_type_id = mt.municipality_type_desc
                    """,
            "mode": "published",
            "name": "municipality_dim",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Number of repair orders",
            "query": (
                "SELECT count(repair_order_id) as num_repair_orders "
                "FROM repair_orders"
            ),
            "mode": "published",
            "name": "num_repair_orders",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Average repair price",
            "query": "SELECT avg(price) as avg_repair_price FROM repair_order_details",
            "mode": "published",
            "name": "avg_repair_price",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Total repair cost",
            "query": "SELECT sum(price) as total_repair_cost FROM repair_order_details",
            "mode": "published",
            "name": "total_repair_cost",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Average length of employment",
            "query": (
                "SELECT avg(NOW() - hire_date) as avg_length_of_employment "
                "FROM hard_hats"
            ),
            "mode": "published",
            "name": "avg_length_of_employment",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Total repair order discounts",
            "query": (
                "SELECT sum(price * discount) as total_discount "
                "FROM repair_order_details"
            ),
            "mode": "published",
            "name": "total_repair_order_discounts",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Total repair order discounts",
            "query": (
                "SELECT avg(price * discount) as avg_repair_order_discount "
                "FROM repair_order_details"
            ),
            "mode": "published",
            "name": "avg_repair_order_discounts",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Average time to dispatch a repair order",
            "query": (
                "SELECT avg(dispatched_date - order_date) as avg_time_to_dispatch "
                "FROM repair_orders"
            ),
            "mode": "published",
            "name": "avg_time_to_dispatch",
        },
    ),
    (
        (
            "/nodes/repair_order_details/columns/repair_order_id/"
            "?dimension=repair_order&dimension_column=repair_order_id"
        ),
        {},
    ),
    (
        (
            "/nodes/repair_orders/columns/municipality_id/"
            "?dimension=municipality_dim&dimension_column=municipality_id"
        ),
        {},
    ),
    (
        (
            "/nodes/repair_type/columns/contractor_id/"
            "?dimension=contractor&dimension_column=contractor_id"
        ),
        {},
    ),
    (
        (
            "/nodes/repair_orders/columns/hard_hat_id/"
            "?dimension=hard_hat&dimension_column=hard_hat_id"
        ),
        {},
    ),
    (
        (
            "/nodes/repair_orders/columns/dispatcher_id/"
            "?dimension=dispatcher&dimension_column=dispatcher_id"
        ),
        {},
    ),
    (
        (
            "/nodes/local_hard_hats/columns/state_id/"
            "?dimension=us_state&dimension_column=state_id"
        ),
        {},
    ),
    (  # Accounts/Revenue examples begin
        "/nodes/source/",
        {
            "columns": {
                "id": {"type": "INT"},
                "account_type_name": {"type": "STR"},
                "account_type_classification": {"type": "INT"},
                "preferred_payment_method": {"type": "INT"},
            },
            "description": "A source table for account type data",
            "mode": "published",
            "name": "account_type_table",
            "catalog": "default",
            "schema_": "accounting",
            "table": "account_type_table",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": {
                "id": {"type": "INT"},
                "payment_type_name": {"type": "STR"},
                "payment_type_classification": {"type": "INT"},
            },
            "description": "A source table for different types of payments",
            "mode": "published",
            "name": "payment_type_table",
            "catalog": "default",
            "schema_": "accounting",
            "table": "payment_type_table",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": {
                "payment_id": {"type": "INT"},
                "payment_amount": {"type": "FLOAT"},
                "payment_type": {"type": "INT"},
                "customer_id": {"type": "INT"},
                "account_type": {"type": "STR"},
            },
            "description": "All repair orders",
            "mode": "published",
            "name": "revenue",
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
                "FROM payment_type_table"
            ),
            "mode": "published",
            "name": "payment_type",
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "Account type dimension",
            "query": (
                "SELECT id, account_type_name, "
                "account_type_classification FROM "
                "account_type_table"
            ),
            "mode": "published",
            "name": "account_type",
        },
    ),
    (
        "/nodes/transform/",
        {
            "query": (
                "SELECT payment_id, payment_amount, customer_id, account_type "
                "FROM revenue WHERE payment_amount > 1000000"
            ),
            "description": "Only large revenue payments",
            "mode": "published",
            "name": "large_revenue_payments_only",
        },
    ),
    (
        "/nodes/transform/",
        {
            "query": (
                "SELECT payment_id, payment_amount, customer_id, account_type "
                "FROM revenue WHERE "
                "large_revenue_payments_and_business_only > 1000000 "
                "AND account_type='BUSINESS'"
            ),
            "description": "Only large revenue payments from business accounts",
            "mode": "published",
            "name": "large_revenue_payments_and_business_only",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Total number of account types",
            "query": "SELECT count(id) as num_accounts FROM account_type",
            "mode": "published",
            "name": "number_of_account_types",
        },
    ),
    (  # Basic namespace
        "/nodes/source/",
        {
            "name": "basic.source.comments",
            "description": "A fact table with comments",
            "columns": {
                "id": {"type": "INT"},
                "user_id": {
                    "type": "INT",
                    "dimension": "basic.dimension.users",
                },
                "timestamp": {"type": "TIMESTAMP"},
                "text": {"type": "STR"},
                "event_timestamp": {"type": "TIMESTAMP"},
                "created_at": {"type": "TIMESTAMP"},
                "post_processing_timestamp": {"type": "TIMESTAMP"},
            },
            "mode": "published",
            "catalog": "public",
            "schema_": "basic",
            "table": "comments",
        },
    ),
    (
        "/nodes/source/",
        {
            "name": "basic.source.users",
            "description": "A user table",
            "columns": {
                "id": {"type": "INT"},
                "full_name": {"type": "STR"},
                "age": {"type": "INT"},
                "country": {"type": "STR"},
                "gender": {"type": "STR"},
                "preferred_language": {"type": "STR"},
                "secret_number": {"type": "FLOAT"},
                "created_at": {"type": "TIMESTAMP"},
                "post_processing_timestamp": {"type": "TIMESTAMP"},
            },
            "mode": "published",
            "catalog": "public",
            "schema_": "basic",
            "table": "dim_users",
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
            "query": ("SELECT COUNT(1) AS cnt " "FROM basic.source.comments"),
            "mode": "published",
            "name": "basic.num_comments",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Number of users.",
            "type": "metric",
            "query": ("SELECT SUM(num_users) " "FROM basic.transform.country_agg"),
            "mode": "published",
            "name": "basic.num_users",
        },
    ),
    (  # Event examples
        "/nodes/source/",
        {
            "name": "event_source",
            "description": "Events",
            "columns": {
                "event_id": {"type": "INT"},
                "event_latency": {"type": "INT"},
                "device_id": {"type": "INT"},
                "country": {"type": "STR", "dimension": "countries_dim"},
            },
            "mode": "published",
            "catalog": "default",
            "schema_": "logs",
            "table": "log_events",
        },
    ),
    (
        "/nodes/transform/",
        {
            "name": "long_events",
            "description": "High-Latency Events",
            "query": "SELECT event_id, event_latency, device_id, country "
            "FROM event_source WHERE event_latency > 1000000",
            "mode": "published",
        },
    ),
    (
        "/nodes/dimension/",
        {
            "name": "country_dim",
            "description": "Country Dimension",
            "query": "SELECT country, COUNT(DISTINCT event_id) AS events_cnt "
            "FROM event_source GROUP BY country",
            "mode": "published",
        },
    ),
    (
        "/nodes/event_source/columns/country/?dimension=country_dim&dimension_column=country",
        {},
    ),
    (
        "/nodes/metric/",
        {
            "name": "device_ids_count",
            "description": "Number of Distinct Devices",
            "query": "SELECT COUNT(DISTINCT device_id) " "FROM event_source",
            "mode": "published",
        },
    ),
    (
        "/nodes/metric/",
        {
            "name": "long_events_distinct_countries",
            "description": "Number of Distinct Countries for Long Events",
            "query": "SELECT COUNT(DISTINCT country) " "FROM long_events",
            "mode": "published",
        },
    ),
    (  # DBT examples
        "/nodes/source/",
        {
            "columns": {
                "id": {"type": "INT"},
                "first_name": {"type": "STR"},
                "last_name": {"type": "STR"},
            },
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
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": {
                "id": {"type": "INT"},
                "user_id": {
                    "type": "INT",
                    "dimension": "dbt.dimension.customers",
                },
                "order_date": {"type": "DATE"},
                "status": {"type": "STR"},
                "_etl_loaded_at": {"type": "TIMESTAMP"},
            },
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
            "columns": {
                "id": {"type": "INT"},
                "orderid": {"type": "INT"},
                "paymentmethod": {"type": "STR"},
                "status": {"type": "STR"},
                "amount": {"type": "INT"},
                "created": {"type": "DATE"},
                "_batched_at": {"type": "TIMESTAMP"},
            },
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
                'SELECT "dbt.source.jaffle_shop.customers".id, '
                '        "dbt.source.jaffle_shop.customers".first_name, '
                '        "dbt.source.jaffle_shop.customers".last_name, '
                "        COUNT(1) AS order_cnt "
                "FROM dbt.source.jaffle_shop.orders o "
                "JOIN dbt.source.jaffle_shop.customers c ON o.user_id = c.id "
                'GROUP BY "dbt.source.jaffle_shop.customers".id, '
                '        "dbt.source.jaffle_shop.customers".first_name, '
                '        "dbt.source.jaffle_shop.customers".last_name '
            ),
            "description": "Country level agg table",
            "mode": "published",
            "name": "dbt.transform.customer_agg",
        },
    ),
    (
        "/nodes/source/",
        {
            "columns": {
                "id": {"type": "INT"},
                "item_name": {"type": "STR"},
                "sold_count": {"type": "INT"},
                "price_per_unit": {"type": "FLOAT"},
                "psp": {"type": "STR"},
            },
            "description": "A source table for sales",
            "mode": "published",
            "name": "sales",
            "catalog": "default",
            "schema_": "revenue",
            "table": "sales",
        },
    ),
    (
        "/nodes/dimension/",
        {
            "description": "Item dimension",
            "query": ("SELECT item_name " "account_type_classification FROM " "sales"),
            "mode": "published",
            "name": "items",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Total units sold",
            "query": "SELECT SUM(sold_count) as num_sold FROM sales",
            "mode": "published",
            "name": "items_sold_count",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Total profit",
            "query": "SELECT SUM(sold_count * price_per_unit) as num_sold FROM sales",
            "mode": "published",
            "name": "total_profit",
        },
    ),
)


COLUMN_MAPPINGS = {
    "public.basic.comments": [
        Column(name="id", type=ColumnType("INT")),
        Column(name="user_id", type=ColumnType("INT")),
        Column(name="timestamp", type=ColumnType("TIMESTAMP")),
        Column(name="text", type=ColumnType("STR")),
    ],
}

QUERY_DATA_MAPPINGS = {
    (
        "SELECT  payment_type_table.id,\n\tpayment_type_table.payment_type_classification,\n\t"
        'payment_type_table.payment_type_name \n FROM "accounting"."payment_type_table" AS '
        "payment_type_table"
    ): [
        {
            "submitted_query": (
                "SELECT  payment_type_table.id,\n\tpayment_type_table."
                "payment_type_classification,\n\t"
                'payment_type_table.payment_type_name \n FROM "accounting"."payment_type_table" '
                "AS payment_type_table"
            ),
            "state": QueryState.FINISHED,
            "results": {
                "columns": [
                    {"name": "id", "type": "INT"},
                    {"name": "payment_type_classification", "type": "STR"},
                    {"name": "payment_type_name", "type": "STR"},
                ],
                "rows": [
                    (1, "CARD", "VISA"),
                    (2, "CARD", "MASTERCARD"),
                ],
            },
            "errors": [],
        },
    ],
    'SELECT  COUNT(1) AS cnt \n FROM "basic"."comments" AS basic_DOT_source_DOT_comments': {
        "submitted_query": (
            'SELECT  COUNT(1) AS cnt \n FROM "basic"."comments" AS basic_DOT_source_DOT_comments'
        ),
        "state": QueryState.FINISHED,
        "results": {
            "columns": [{"name": "cnt", "type": "INT"}],
            "rows": [
                (1,),
            ],
        },
        "errors": [],
    },
    'SELECT  * \n FROM "accounting"."revenue"': {
        "submitted_query": ('SELECT  * \n FROM "accounting"."revenue"'),
        "state": QueryState.FINISHED,
        "results": {
            "columns": [{"name": "profit", "type": "FLOAT"}],
            "rows": [
                (129.19,),
            ],
        },
        "errors": [],
    },
    (
        "SELECT  revenue.account_type,\n\trevenue.customer_id,\n\trevenue.payment_amount,"
        '\n\trevenue.payment_id \n FROM "accounting"."revenue" AS revenue\n \n '
        "WHERE  revenue.payment_amount > 1000000"
    ): {
        "submitted_query": (
            "SELECT  revenue.account_type,\n\trevenue.customer_id,\n\trevenue.payment_amount,"
            '\n\trevenue.payment_id \n FROM "accounting"."revenue" AS revenue\n \n '
            "WHERE  revenue.payment_amount > 1000000"
        ),
        "state": QueryState.FINISHED,
        "results": {
            "columns": [
                {"name": "account_type", "type": "STR"},
                {"name": "customer_id", "type": "INT"},
                {"name": "payment_amount", "type": "STR"},
                {"name": "payment_id", "type": "INT"},
            ],
            "rows": [
                ("CHECKING", 2, "22.50", 1),
                ("SAVINGS", 2, "100.50", 1),
                ("CREDIT", 1, "11.50", 1),
                ("CHECKING", 2, "2.50", 1),
            ],
        },
        "errors": [],
    },
}
