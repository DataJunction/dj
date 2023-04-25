"""
Roads database examples loaded into DJ test session
"""
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
            "name": "repair_orders",
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
            "name": "repair_order_details",
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
            "name": "repair_type",
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
            "name": "contractors",
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
            "name": "municipality_municipality_type",
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
            "name": "municipality_type",
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
            "name": "municipality",
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
            "name": "dispatchers",
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
            "name": "hard_hats",
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
            "name": "hard_hat_state",
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
            "name": "us_states",
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
                        FROM contractors
                    """,
            "mode": "published",
            "name": "contractor",
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
                        FROM hard_hats
                    """,
            "mode": "published",
            "name": "hard_hat",
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
                        FROM hard_hats hh
                        LEFT JOIN hard_hat_state hhs
                        ON hh.hard_hat_id = hhs.hard_hat_id
                        WHERE hh.state_id = 'NY'
                    """,
            "mode": "published",
            "name": "local_hard_hats",
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
                        FROM us_states s
                        LEFT JOIN us_region r
                        ON s.state_region = r.us_region_id
                    """,
            "mode": "published",
            "name": "us_state",
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
                        FROM dispatchers
                    """,
            "mode": "published",
            "name": "dispatcher",
            "primary_key": ["dispatcher_id"],
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
            "primary_key": ["municipality_id"],
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
            "/nodes/hard_hat/columns/state/"
            "?dimension=us_state&dimension_column=state_short"
        ),
        {},
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
            "/nodes/repair_order/columns/dispatcher_id/"
            "?dimension=dispatcher&dimension_column=dispatcher_id"
        ),
        {},
    ),
    (
        (
            "/nodes/repair_order/columns/hard_hat_id/"
            "?dimension=hard_hat&dimension_column=hard_hat_id"
        ),
        {},
    ),
    (
        (
            "/nodes/repair_order/columns/municipality_id/"
            "?dimension=municipality_dim&dimension_column=municipality_id"
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
                        m.municipality_id,
                        contact_name,
                        contact_title,
                        local_region,
                        state_id,
                        mmt.municipality_type_id,
                        mt.municipality_type_desc
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
                "SELECT count(repair_order_id) as num_repair_orders "
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
            "query": "SELECT avg(price) as avg_repair_price FROM foo.bar.repair_order_details",
            "mode": "published",
            "name": "foo.bar.avg_repair_price",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Total repair cost",
            "query": "SELECT sum(price) as total_repair_cost FROM foo.bar.repair_order_details",
            "mode": "published",
            "name": "foo.bar.total_repair_cost",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Average length of employment",
            "query": (
                "SELECT avg(NOW() - hire_date) as avg_length_of_employment "
                "FROM foo.bar.hard_hats"
            ),
            "mode": "published",
            "name": "foo.bar.avg_length_of_employment",
        },
    ),
    (
        "/nodes/metric/",
        {
            "description": "Total repair order discounts",
            "query": (
                "SELECT sum(price * discount) as total_discount "
                "FROM foo.bar.repair_order_details"
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
                "SELECT avg(price * discount) as avg_repair_order_discount "
                "FROM foo.bar.repair_order_details"
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
                "SELECT avg(dispatched_date - order_date) as avg_time_to_dispatch "
                "FROM foo.bar.repair_orders"
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
)
