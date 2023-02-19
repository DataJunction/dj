"""
Tests for ``dj.sql.parse``.
"""
from dj.sql.parse import is_metric


def test_is_metric() -> None:
    """
    Test ``is_metric``.
    """
    assert is_metric("SELECT COUNT(*) FROM my_table")
    assert is_metric("SELECT COUNT(*) AS cnt FROM my_table")
    assert not is_metric("SELECT COUNT(*), SUM(cnt) FROM my_table")
    assert not is_metric("SELECT 42 FROM my_table")
    assert not is_metric("SELECT name FROM my_table")
    assert not is_metric("SELECT 1, name, 12, names, 12 FROM my_table")
    assert not is_metric(None)

    assert is_metric(
        "SELECT CASE WHEN COUNT(*) > 0 THEN COUNT(*) ELSE SUM(cnt) END FROM my_table",
    )
    assert not is_metric(
        "SELECT CASE WHEN COUNT(*) > 0 THEN 221 ELSE 1 END FROM my_table",
    )
