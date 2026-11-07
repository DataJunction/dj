"""A filter that can be true for NULL must stay out of a dimension's CTE."""

import sqlite3
from types import SimpleNamespace

import pytest

from datajunction_server.construction.build_v3 import filters
from datajunction_server.construction.build_v3.dimensions import (
    _register_pushdown_into_upstream,
)
from tests.construction.build_v3 import get_first_grain_group


def execute(sql: str) -> list[tuple]:
    """Run SQL against small physical tables, omitting the fixture catalog."""
    with sqlite3.connect(":memory:") as db:
        db.executescript(
            """
            CREATE TABLE orders (order_id INTEGER);
            CREATE TABLE order_items (
                order_id INTEGER, product_id INTEGER, quantity INTEGER,
                unit_price REAL
            );
            CREATE TABLE products (
                product_id INTEGER, category TEXT, subcategory TEXT
            );
            INSERT INTO orders VALUES (1), (2), (3);
            INSERT INTO order_items VALUES (1, 1, 1, 10), (2, 2, 1, 7), (3, 3, 1, 5);
            INSERT INTO products VALUES (1, 'A', 'known'), (2, 'B', NULL);
            """,
        )
        return db.execute(sql.replace("default.v3.", "")).fetchall()


async def request_sql(client, endpoint: str, filter_str: str) -> str:
    response = await client.get(
        f"/sql/{endpoint}/v3/",
        params={
            "metrics": ["v3.total_revenue"],
            "dimensions": ["v3.product.category"],
            "filters": [filter_str],
        },
    )
    assert response.status_code == 200, response.json()
    data = response.json()
    return get_first_grain_group(data)["sql"] if endpoint == "measures" else data["sql"]


@pytest.mark.parametrize("endpoint", ["measures", "metrics"])
@pytest.mark.parametrize(
    "filter_str, expected",
    [
        ("v3.product.subcategory IS NULL", [(None, 5.0), ("B", 7.0)]),
        (
            "v3.product.subcategory IS NULL OR v3.product.subcategory = 'x'",
            [(None, 5.0), ("B", 7.0)],
        ),
        (
            "COALESCE(v3.product.subcategory, 'none') = 'none'",
            [(None, 5.0), ("B", 7.0)],
        ),
    ],
)
async def test_null_accepting_filter_preserves_real_matches(
    client_with_build_v3,
    endpoint,
    filter_str,
    expected,
):
    """A non-null product must not become an unmatched row passing the filter."""
    sql = await request_sql(client_with_build_v3, endpoint, filter_str)
    rows = sorted(execute(sql), key=lambda row: row[0] or "")
    assert rows == expected


@pytest.mark.parametrize(
    "filter_str, expected",
    [
        ("x = 1", True),
        ("x <> 1", True),
        ("x != 1", True),
        ("x < 1", True),
        ("x > 1", True),
        ("x <= 1", True),
        ("x >= 1", True),
        ("x IN (1, 2)", True),
        ("x NOT IN (1, 2)", True),
        ("x BETWEEN 1 AND 2", True),
        ("x LIKE 'a%'", True),
        ("x NOT LIKE 'a%'", True),
        ("x IS NOT NULL", True),
        ("x IS NULL", False),
        ("x <=> 1", False),
        ("x + 1", False),
        ("LOWER(x) = 'a'", True),
        ("COALESCE(x, 0) = 0", False),
        ("IFNULL(x, 0) IN (0)", False),
        ("COALESCE(x, 0) IS NOT NULL", False),
        ("CASE WHEN x IS NULL THEN 0 ELSE x END > 0", False),
        ("(x IS NULL) = FALSE", False),
        ("NOT x = 1", True),
        ("NOT x IS NULL", False),
        ("x = 1 AND y IS NULL", True),
        ("x IS NULL AND y = 1", True),
        ("x IS NULL AND y IS NULL", False),
        ("x = 1 OR y = 2", True),
        ("x = 1 OR y IS NULL", False),
        ("x IS NULL OR y = 1", False),
        ("x IS NULL OR y IS NULL", False),
        ("f(x)", False),
    ],
)
def test_rejects_null(filter_str, expected):
    assert filters.rejects_null(filters.parse_filter(filter_str)) is expected


def test_null_accepting_filter_not_registered_for_upstream_pushdown():
    ctx = SimpleNamespace(
        dimension_filters=["v3.product.subcategory IS NULL"],
        pushdown_consumed_by_fact={},
        upstream_pushdown_filters={},
    )
    assert not _register_pushdown_into_upstream(
        ctx,
        SimpleNamespace(name="v3.order_details"),
        "v3.product.subcategory",
        [],
    )
    assert ctx.pushdown_consumed_by_fact == {"v3.order_details": set()}
    assert ctx.upstream_pushdown_filters == {}
