"""
Tests for the authorization verifier.
"""

from sqlalchemy.engine import create_engine

from datajunction.auth.verifier import verify_query


def test_verify_query() -> None:
    """
    Test ``verify_query``.
    """
    engine = create_engine("sqlite://")
    connection = engine.connect()
    connection.execute("CREATE TABLE sales (event_time DATETIME, price DECIMAL)")

    permissions = ["SELECT * FROM postgres"]
    query = "SELECT s.event_time, SUM(price) AS sum_price FROM sales AS s;"
    assert verify_query(
        query,
        engine=engine,
        database="postgres",
        catalog=None,
        schema="main",
        permissions=permissions,
    )

    permissions = ["SELECT * FROM postgres.dev"]
    query = "SELECT s.event_time, SUM(price) AS sum_price FROM sales AS s;"
    assert not verify_query(
        query,
        engine=engine,
        database="postgres",
        catalog=None,
        schema="main",
        permissions=permissions,
    )

    permissions = ["SELECT * FROM postgres.null.main.sales"]
    query = "SELECT s.event_time, SUM(price) AS sum_price FROM sales AS s;"
    assert verify_query(
        query,
        engine=engine,
        database="postgres",
        catalog=None,
        schema="main",
        permissions=permissions,
    )

    permissions = ["SELECT * FROM postgres.null.dev.sales"]
    query = "SELECT s.event_time, SUM(price) AS sum_price FROM sales AS s;"
    assert not verify_query(
        query,
        engine=engine,
        database="postgres",
        catalog=None,
        schema="main",
        permissions=permissions,
    )


"""
SELECT a, b FROM T ==> SELECT AVG(a) FROM T GROUP BY b
SELECT COUNT(*), FROM t GROUP BY b !=> SELECT b FROM t

SELECT COUNT(*) FROM t GROUP BY b ==> SELECT 

SELECT SUM(a) FROM (SELECT COUNT(*), b FROM t GROUP BY b) !=> SELECT SUM(a) FROM 
"""
