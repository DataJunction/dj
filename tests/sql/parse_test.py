"""
Tests for ``datajunction.sql.parse``.
"""

from datajunction.sql.parse import is_metric


def test_is_metric() -> None:
    """
    Test ``is_metric``.
    """
    assert is_metric("SELECT COUNT(*) FROM my_table")
    assert is_metric("SELECT COUNT(*) AS cnt FROM my_table")
    assert not is_metric("SELECT COUNT(*), SUM(cnt) FROM my_table")
    assert not is_metric("SELECT 42 FROM my_table")
    assert not is_metric(None)
