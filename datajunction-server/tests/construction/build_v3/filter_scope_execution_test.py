"""Execute generated SQL to check that request filters retain their scope."""

import sqlite3

import pytest

from datajunction_server.construction.build_v3.cte import (
    _rewrite_filter_for_cte,
)
from datajunction_server.sql.parsing.backends.antlr4 import parse
from tests.construction.build_v3 import get_first_grain_group


def execute(sql: str, fixture_sql: str) -> list[tuple]:
    """Run SQL against small physical tables, omitting the fixture catalog."""
    with sqlite3.connect(":memory:") as db:
        db.executescript(fixture_sql)
        return db.execute(sql.replace("default.v3.", "")).fetchall()


def response_sql(response, endpoint: str) -> str:
    assert response.status_code == 200, response.json()
    data = response.json()
    return get_first_grain_group(data)["sql"] if endpoint == "measures" else data["sql"]


@pytest.mark.parametrize("endpoint", ["measures", "metrics"])
@pytest.mark.parametrize("upstream_transform", [False, True])
async def test_null_dimension_filter_preserves_real_matches(
    client_with_build_v3,
    endpoint,
    upstream_transform,
):
    """A non-null product must not become an unmatched row passing IS NULL."""
    if upstream_transform:
        response = await client_with_build_v3.post(
            "/nodes/transform/",
            json={
                "name": "v3.product_stage",
                "query": "SELECT product_id, category, subcategory FROM v3.src_products",
                "mode": "published",
            },
        )
        assert response.status_code == 201, response.json()
        response = await client_with_build_v3.patch(
            "/nodes/v3.product/",
            json={
                "query": "SELECT product_id, category, subcategory FROM v3.product_stage",
            },
        )
        assert response.status_code == 200, response.json()
    response = await client_with_build_v3.get(
        f"/sql/{endpoint}/v3/",
        params={
            "metrics": ["v3.total_revenue"],
            "dimensions": ["v3.product.category"],
            "filters": ["v3.product.subcategory IS NULL"],
        },
    )
    rows = execute(
        response_sql(response, endpoint),
        """
        CREATE TABLE orders (order_id INTEGER);
        CREATE TABLE order_items (
            order_id INTEGER, product_id INTEGER, quantity INTEGER, unit_price REAL
        );
        CREATE TABLE products (product_id INTEGER, category TEXT, subcategory TEXT);
        INSERT INTO orders VALUES (1), (2), (3);
        INSERT INTO order_items VALUES (1, 1, 1, 10), (2, 2, 1, 7), (3, 3, 1, 5);
        INSERT INTO products VALUES (1, 'A', 'known'), (2, 'B', NULL);
        """,
    )
    assert sorted(rows, key=lambda row: row[0] or "") == [(None, 5.0), ("B", 7.0)]


@pytest.mark.parametrize("endpoint", ["measures", "metrics"])
async def test_role_filter_preserves_other_role_of_shared_dimension(
    client_with_build_v3,
    endpoint,
):
    """Filtering an A-to-B order by origin A must retain destination B."""
    response = await client_with_build_v3.get(
        f"/sql/{endpoint}/v3/",
        params={
            "metrics": ["v3.total_revenue"],
            "dimensions": ["v3.location.city[from]", "v3.location.city[to]"],
            "filters": ["v3.location.city[from] = 'A'"],
        },
    )
    assert execute(
        response_sql(response, endpoint),
        """
        CREATE TABLE locations (location_id INTEGER, city TEXT);
        CREATE TABLE orders (
            order_id INTEGER, from_location_id INTEGER, to_location_id INTEGER
        );
        CREATE TABLE order_items (order_id INTEGER, quantity INTEGER, unit_price REAL);
        INSERT INTO locations VALUES (1, 'A'), (2, 'B'), (3, 'C');
        INSERT INTO orders VALUES (1, 1, 2), (2, 3, 1);
        INSERT INTO order_items VALUES (1, 2, 5), (2, 1, 7);
        """,
    ) == [("A", "B", 10.0)]


@pytest.mark.parametrize("endpoint", ["measures", "metrics"])
@pytest.mark.parametrize(
    "subquery",
    [
        "scalar",
        "exists",
        "shared_cte",
        "relational_total",
        "grouped_total",
        "linked_transform",
    ],
)
async def test_filter_only_source_link_preserves_authored_subquery(
    client_with_build_v3,
    endpoint,
    subquery,
):
    """An unprojected date key must filter the row source, not the scalar count."""
    client = client_with_build_v3
    response = await client.post(
        "/nodes/source/",
        json={
            "name": "v3.order_history_src",
            "columns": [
                {"name": "customer_id", "type": "int"},
                {"name": "report_date", "type": "int"},
            ],
            "mode": "published",
            "catalog": "default",
            "schema_": "v3",
            "table": "order_history_src",
        },
    )
    assert response.status_code in (200, 201), response.json()
    response = await client.post(
        "/nodes/v3.order_history_src/link",
        json={
            "dimension_node": "v3.date",
            "join_on": "v3.order_history_src.report_date = v3.date.date_id",
        },
    )
    assert response.status_code in (200, 201), response.json()
    query = (
        "SELECT o.customer_id, "
        "(SELECT COUNT(*) FROM v3.order_history_src b "
        "WHERE b.customer_id = o.customer_id) AS all_time_orders "
        "FROM v3.order_history_src o"
        if subquery == "scalar"
        else "SELECT o.customer_id, 1 AS all_time_orders "
        "FROM v3.order_history_src o WHERE EXISTS ("
        "SELECT 1 FROM v3.order_history_src b "
        "WHERE b.customer_id = o.customer_id AND b.report_date < 20240101)"
    )
    if subquery == "shared_cte":
        query = (
            "WITH history AS (SELECT customer_id, report_date FROM v3.order_history_src) "
            "SELECT o.customer_id, "
            "(SELECT COUNT(*) FROM history b "
            "WHERE b.customer_id = o.customer_id) AS all_time_orders "
            "FROM history o"
        )
    elif subquery == "relational_total":
        query = (
            "SELECT o.customer_id, totals.orders AS all_time_orders "
            "FROM v3.order_history_src o CROSS JOIN "
            "(SELECT COUNT(*) AS orders FROM v3.order_history_src) totals"
        )
    elif subquery == "grouped_total":
        query = (
            "SELECT o.customer_id, totals.orders AS all_time_orders "
            "FROM v3.order_history_src o JOIN "
            "(SELECT customer_id, COUNT(*) AS orders FROM v3.order_history_src "
            "GROUP BY customer_id) totals ON o.customer_id = totals.customer_id"
        )
    elif subquery == "linked_transform":
        response = await client.post(
            "/nodes/transform/",
            json={
                "name": "v3.history_stage",
                "query": "SELECT customer_id, report_date FROM v3.order_history_src",
                "mode": "published",
            },
        )
        assert response.status_code == 201, response.json()
        response = await client.post(
            "/nodes/v3.history_stage/link",
            json={
                "dimension_node": "v3.date",
                "join_on": "v3.history_stage.report_date = v3.date.date_id",
            },
        )
        assert response.status_code in (200, 201), response.json()
        query = (
            "SELECT o.customer_id, "
            "(SELECT COUNT(*) FROM v3.history_stage b "
            "WHERE b.customer_id = o.customer_id) AS all_time_orders "
            "FROM v3.history_stage o"
        )
    response = await client.post(
        "/nodes/transform/",
        json={
            "name": "v3.order_history",
            "query": query,
            "mode": "published",
        },
    )
    assert response.status_code in (200, 201), response.json()
    response = await client.post(
        "/nodes/metric/",
        json={
            "name": "v3.sum_all_time_orders",
            "query": "SELECT SUM(all_time_orders) FROM v3.order_history",
            "mode": "published",
        },
    )
    assert response.status_code in (200, 201), response.json()
    response = await client.get(
        f"/sql/{endpoint}/v3/",
        params={
            "metrics": ["v3.sum_all_time_orders"],
            "filters": ["v3.date.date_id = 20240101"],
        },
    )
    assert response.status_code == 200, response.json()
    assert execute(
        response_sql(response, endpoint),
        """
        CREATE TABLE order_history_src (customer_id INTEGER, report_date INTEGER);
        INSERT INTO order_history_src VALUES (1, 20240101), (1, 20230101);
        """,
    ) == [(2 if subquery == "scalar" else 1,)]


def test_multi_hop_role_still_pushes_into_owning_fact_scope():
    """Preserve a valid multi-hop binding while removing its SQL role marker."""
    predicate = _rewrite_filter_for_cte(
        "v3.date.date_id[customer->registration] >= 20240101",
        {"v3.date.date_id[customer->registration]": "event_date"},
        {"event_date"},
        parse("SELECT e.event_date FROM events e"),
    )
    assert str(predicate) == "e.event_date >= 20240101"


async def filtered_node_sql(client, endpoint, query, filters, aggregation):
    """Build the same filtered transform through the node and metric endpoints."""
    response = await filtered_node_response(
        client,
        endpoint,
        query,
        filters,
        aggregation,
    )
    return response_sql(response, endpoint)


async def filtered_node_response(client, endpoint, query, filters, aggregation):
    """Return the response so rejected placements can be tested as well."""
    response = await client.post(
        "/nodes/transform/",
        json={"name": "v3.filtered_fact", "query": query, "mode": "published"},
    )
    assert response.status_code == 201, response.json()
    if endpoint == "node":
        response = await client.get(
            "/sql/v3.filtered_fact/",
            params={"filters": filters},
            headers={"Cache-Control": "no-cache"},
        )
    else:
        response = await client.post(
            "/nodes/metric/",
            json={
                "name": "v3.filtered_total",
                "query": f"SELECT {aggregation} FROM v3.filtered_fact",
                "mode": "published",
            },
        )
        assert response.status_code == 201, response.json()
        response = await client.get(
            f"/sql/{endpoint}/v3/",
            params={"metrics": ["v3.filtered_total"], "filters": filters},
            headers={"Cache-Control": "no-cache"},
        )
    return response


@pytest.mark.parametrize("endpoint", ["node", "measures", "metrics"])
@pytest.mark.parametrize(
    "predicate, expected",
    [
        ("category = 'active'", [(1, "active")]),
        ("category IS NULL", [(3, None)]),
        (
            "category = 'active' OR v3.filtered_fact.order_id = 2",
            [(1, "active"), (2, "inactive")],
        ),
    ],
)
async def test_local_filter_on_outer_join_constrains_output(
    client_with_build_v3,
    endpoint,
    predicate,
    expected,
):
    sql = await filtered_node_sql(
        client_with_build_v3,
        endpoint,
        "SELECT o.order_id, p.category FROM v3.src_orders o "
        "LEFT JOIN v3.src_products p ON o.order_id = p.product_id",
        [f"v3.filtered_fact.{predicate}"],
        "COUNT(order_id)",
    )
    result = execute(
        sql,
        """
        CREATE TABLE orders (order_id INTEGER);
        CREATE TABLE products (product_id INTEGER, category TEXT);
        INSERT INTO orders VALUES (1), (2), (3);
        INSERT INTO products VALUES (1, 'active'), (2, 'inactive');
        """,
    )
    assert result == (expected if endpoint == "node" else [(len(expected),)])


@pytest.mark.parametrize("endpoint", ["node", "measures", "metrics"])
async def test_local_date_filter_preserves_shared_upstream_scalar_count(
    client_with_build_v3,
    endpoint,
):
    client = client_with_build_v3
    response = await client.post(
        "/nodes/transform/",
        json={
            "name": "v3.history",
            "query": "SELECT order_date FROM v3.src_orders",
            "mode": "published",
        },
    )
    assert response.status_code == 201, response.json()
    sql = await filtered_node_sql(
        client,
        endpoint,
        "SELECT h.order_date, (SELECT COUNT(*) FROM v3.history) AS all_time_orders "
        "FROM v3.history h",
        ["v3.filtered_fact.order_date = 20240101"],
        "SUM(all_time_orders)",
    )
    assert execute(
        sql,
        "CREATE TABLE orders (order_date INTEGER); "
        "INSERT INTO orders VALUES (20240101), (20230101);",
    ) == ([(20240101, 2)] if endpoint == "node" else [(2,)])


@pytest.mark.parametrize("endpoint", ["node", "measures", "metrics"])
async def test_filter_only_union_status_constrains_both_arms(
    client_with_build_v3,
    endpoint,
):
    sql = await filtered_node_sql(
        client_with_build_v3,
        endpoint,
        "SELECT a.order_id, a.status FROM v3.src_orders a WHERE a.order_id = 1 "
        "UNION ALL SELECT b.order_id, b.status FROM v3.src_orders b WHERE b.order_id = 2",
        ["v3.filtered_fact.status = 'active'"],
        "COUNT(order_id)",
    )
    assert execute(
        sql,
        "CREATE TABLE orders (order_id INTEGER, status TEXT); "
        "INSERT INTO orders VALUES (1, 'active'), (2, 'inactive'), (3, 'active');",
    ) == ([(1, "active")] if endpoint == "node" else [(1,)])


@pytest.fixture
async def customer_date_link(client_with_build_v3):
    client = client_with_build_v3
    response = await client.post(
        "/nodes/v3.src_customers/link",
        json={
            "dimension_node": "v3.date",
            "join_on": "v3.src_customers.registration_date = v3.date.date_id",
        },
    )
    assert response.status_code in (200, 201), response.json()
    return client


CUSTOMER_ORDERS = """
    CREATE TABLE orders (order_id INTEGER, customer_id INTEGER, status TEXT);
    CREATE TABLE customers (customer_id INTEGER, registration_date INTEGER);
    INSERT INTO orders VALUES
        (1, 1, 'active'), (2, 2, 'inactive'), (3, 3, 'active'), (4, 4, 'active');
    INSERT INTO customers VALUES (1, 20240101), (2, 20230101), (4, NULL);
"""


@pytest.mark.parametrize("endpoint", ["node", "measures", "metrics"])
@pytest.mark.parametrize(
    "filters, expected",
    [
        (["v3.date.date_id = 20240101"], [(1, "active")]),
        (["v3.date.date_id IS NULL"], [(3, "active"), (4, "active")]),
        (
            ["v3.date.date_id >= 20230101 AND v3.filtered_fact.status = 'active'"],
            [(1, "active")],
        ),
        (
            ["v3.date.date_id >= 20230101", "v3.filtered_fact.status = 'active'"],
            [(1, "active")],
        ),
        (
            ["v3.date.date_id = 20240101 OR v3.filtered_fact.status = 'inactive'"],
            [(1, "active"), (2, "inactive")],
        ),
    ],
)
async def test_hidden_filter_constrains_authored_outer_join(
    customer_date_link,
    endpoint,
    filters,
    expected,
):
    sql = await filtered_node_sql(
        customer_date_link,
        endpoint,
        "SELECT o.order_id, o.status FROM v3.src_orders o "
        "LEFT JOIN v3.src_customers c ON o.customer_id = c.customer_id",
        filters,
        "COUNT(order_id)",
    )
    assert execute(sql, CUSTOMER_ORDERS) == (
        expected if endpoint == "node" else [(len(expected),)]
    )


@pytest.mark.parametrize("endpoint", ["node", "measures", "metrics"])
async def test_hidden_filter_applies_inside_nullable_query_boundary(
    customer_date_link,
    endpoint,
):
    """The filter narrows the nullable input, so unmatched orders stay."""
    sql = await filtered_node_sql(
        customer_date_link,
        endpoint,
        "SELECT o.order_id FROM v3.src_orders o LEFT JOIN "
        "(SELECT customer_id FROM v3.src_customers) c "
        "ON o.customer_id = c.customer_id",
        ["v3.date.date_id = 20240101"],
        "COUNT(order_id)",
    )
    assert execute(sql, CUSTOMER_ORDERS) == (
        [(1,), (2,), (3,), (4,)] if endpoint == "node" else [(4,)]
    )


@pytest.mark.parametrize("endpoint", ["node", "measures", "metrics"])
@pytest.mark.parametrize("combined", [False, True])
async def test_hidden_and_aggregate_filters_keep_separate_scopes(
    customer_date_link,
    endpoint,
    combined,
):
    filters = ["v3.date.date_id = 20240101", "v3.filtered_fact.total > 1"]
    sql = await filtered_node_sql(
        customer_date_link,
        endpoint,
        "SELECT c.customer_id, COUNT(*) AS total FROM v3.src_customers c "
        "JOIN v3.src_orders o ON c.customer_id = o.customer_id GROUP BY c.customer_id",
        [" AND ".join(filters)] if combined else filters,
        "SUM(total)",
    )
    # Customer 1 has two matching orders; customer 2's count must be filtered
    # before aggregation, while the aggregate predicate applies to the result.
    fixture = CUSTOMER_ORDERS + "INSERT INTO orders VALUES (5, 1, 'active');"
    assert execute(sql, fixture) == ([(1, 2)] if endpoint == "node" else [(2,)])


@pytest.mark.parametrize("endpoint", ["node", "measures", "metrics"])
async def test_hidden_or_aggregate_filter_cannot_split_scopes(
    customer_date_link,
    endpoint,
):
    response = await filtered_node_response(
        customer_date_link,
        endpoint,
        "SELECT c.customer_id, COUNT(*) AS total FROM v3.src_customers c "
        "JOIN v3.src_orders o ON c.customer_id = o.customer_id GROUP BY c.customer_id",
        ["v3.date.date_id = 20240101 OR v3.filtered_fact.total > 1"],
        "SUM(total)",
    )
    assert response.status_code == 422, response.json()
    assert "local references cannot bind" in str(response.json())
