"""Execute generated SQL to check that request filters retain their scope."""

import sqlite3
from types import SimpleNamespace

import pytest

from datajunction_server.construction.build_v3.cte import (
    _build_local_dim_aliases,
    _is_relational_scope,
    _resolve_pushdown_filters_for_cte,
    _rewrite_filter_for_cte,
    inject_filter_into_select,
)
from datajunction_server.models.node_type import NodeType
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


def test_date_filter_preserves_all_time_scalar_count():
    """The same physical table in a scalar expression owns a different scope."""
    fact = SimpleNamespace(
        name="v3.fact",
        type=NodeType.TRANSFORM,
        current=SimpleNamespace(
            columns=[
                SimpleNamespace(name="order_date"),
                SimpleNamespace(name="orders"),
            ],
            dimension_links=[],
        ),
    )
    query = parse(
        "SELECT o.order_date, "
        "(SELECT COUNT(*) FROM orders b WHERE b.customer_id = o.customer_id) AS orders "
        "FROM orders o",
    )
    injections, _ = _resolve_pushdown_filters_for_cte(
        fact,
        query,
        ["v3.date.date_id[order] >= 20240101"],
        {"v3.date.date_id[order]": "order_date"},
    )
    for select, predicate in injections:
        inject_filter_into_select(select, predicate)
    assert execute(
        str(query),
        """
        CREATE TABLE orders (customer_id INTEGER, order_date INTEGER);
        INSERT INTO orders VALUES (1, 20240101), (1, 20230101);
        """,
    ) == [(20240101, 2)]


@pytest.mark.parametrize("endpoint", ["measures", "metrics"])
@pytest.mark.parametrize("subquery", ["scalar", "exists", "shared_cte"])
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
    if subquery == "shared_cte":
        # Narrowing this shared WITH definition would also change its scalar
        # reader. Reject a key that cannot bind to the owning output scope.
        assert response.status_code == 422, response.json()
        return
    assert execute(
        response_sql(response, endpoint),
        """
        CREATE TABLE order_history_src (customer_id INTEGER, report_date INTEGER);
        INSERT INTO order_history_src VALUES (1, 20240101), (1, 20230101);
        """,
    ) == [(1 if subquery == "exists" else 2,)]


def test_dimension_local_key_cannot_narrow_shared_cte():
    """A locally resolvable key still belongs to the consuming dimension role."""
    dimension = SimpleNamespace(
        name="v3.date",
        type=NodeType.DIMENSION,
        current=SimpleNamespace(
            columns=[SimpleNamespace(name="date_id")],
            dimension_links=[],
        ),
    )
    query = parse("SELECT date_id FROM dates")
    injections, consumed = _resolve_pushdown_filters_for_cte(
        dimension,
        query,
        ["v3.date.date_id = 1"],
        _build_local_dim_aliases(dimension),
    )
    assert injections == []
    assert consumed == set()


@pytest.mark.parametrize(
    "query_sql",
    [
        "WITH history AS (SELECT date_id FROM dates) SELECT 1",
        "WITH history AS (SELECT date_id FROM dates UNION ALL "
        "SELECT date_id FROM history) SELECT date_id FROM history",
    ],
)
def test_unused_or_recursive_cte_cannot_own_request_filter(query_sql):
    """An unused or recursive WITH definition cannot prove filter ownership."""
    query = parse(query_sql)
    assert not _is_relational_scope(query.ctes[0].select, query)


def test_hidden_union_key_cannot_consume_request_filter():
    """Do not narrow an asymmetric UNION through a key hidden by its wrapper."""
    query = parse(
        "SELECT amount FROM (SELECT amount, date_id FROM first_source "
        "UNION ALL SELECT amount, date_id FROM second_source) union_rows",
    )
    fact = SimpleNamespace(
        name="v3.fact",
        type=NodeType.TRANSFORM,
        current=SimpleNamespace(
            columns=[SimpleNamespace(name="amount")],
            dimension_links=[],
        ),
    )
    sources = {
        name: SimpleNamespace(
            name=name,
            type=NodeType.SOURCE,
            current=SimpleNamespace(
                columns=[
                    SimpleNamespace(name="amount"),
                    SimpleNamespace(name="date_id"),
                ],
                dimension_links=[
                    SimpleNamespace(
                        role=None,
                        foreign_keys_reversed={"v3.date.date_id": f"{name}.date_id"},
                    ),
                ],
            ),
        )
        for name in ("first_source", "second_source")
    }
    injections, consumed = _resolve_pushdown_filters_for_cte(
        fact,
        query,
        ["v3.date.date_id = 1"],
        {"v3.date.date_id": "date_id"},
        SimpleNamespace(nodes=sources),
    )
    assert injections == []
    assert consumed == set()


def test_multi_hop_role_still_pushes_into_owning_fact_scope():
    """Preserve a valid multi-hop binding while removing its SQL role marker."""
    predicate = _rewrite_filter_for_cte(
        "v3.date.date_id[customer->registration] >= 20240101",
        {"v3.date.date_id[customer->registration]": "event_date"},
        {"event_date"},
        parse("SELECT e.event_date FROM events e"),
    )
    assert str(predicate) == "e.event_date >= 20240101"
