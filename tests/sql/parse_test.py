"""
Tests for ``dj.sql.parse``.
"""

from dj.sql import parse


def test_is_metric():
    """
    Test parse.is_metric function
    """
    assert parse.is_metric(query="SELECT count(amount) FROM revenue")
    assert not parse.is_metric(query="SELECT a, b FROM c LEFT JOIN d on c.id = d.id")
    assert not parse.is_metric(query=None)
