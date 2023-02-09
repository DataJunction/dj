"""
Tests for API helpers.
"""

import pytest
from sqlmodel import Session

from dj.api import helpers
from dj.errors import DJException


def test_raise_get_node_when_node_does_not_exist(session: Session):
    """
    Test raising when a node doesn't exist
    """
    with pytest.raises(DJException) as exc_info:
        helpers.get_node_by_name(session=session, name="foo")

    assert "A  node with name `foo` does not exist." in str(exc_info.value)


def test_raise_get_database_when_database_does_not_exist(session: Session):
    """
    Test raising when a database doesn't exist
    """
    with pytest.raises(DJException) as exc_info:
        helpers.get_database_by_name(session=session, name="foo")

    assert "Database with name `foo` does not exist." in str(exc_info.value)
