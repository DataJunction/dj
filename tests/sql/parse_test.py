"""
Tests for ``dj.sql.parse``.
"""
import pytest

from dj.errors import DJInvalidInputException
from dj.models import NodeRevision
from dj.sql import parse


def test_is_metric():
    """
    Test parse.is_metric function
    """
    assert (
        parse.check_is_metric(
            NodeRevision(
                query="SELECT count(amount) as b_name FROM revenue",
                name="b_name",
            ),
        )
        is None
    )
    with pytest.raises(DJInvalidInputException):
        parse.check_is_metric(
            NodeRevision(query="SELECT count(amount) FROM revenue", name="b_name"),
        )
        parse.check_is_metric(
            NodeRevision(
                query="SELECT a, b FROM c LEFT JOIN d on c.id = d.id",
                name="b_name",
            ),
        )
        parse.check_is_metric(NodeRevision(query=None, name="b_name"))
