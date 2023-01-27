"""tests for building nodes"""


import pytest
from sqlalchemy import select

from dj.construction.build_planning import generate_build_plan_from_node
from dj.models.node import Node


def test_build_planning_source_fails(construction_session):
    """
    Test build planning source fails
    """
    ref_node = next(
        construction_session.exec(
            select(Node).filter(Node.name == "basic.source.users"),
        ),
    )[0]
    with pytest.raises(Exception) as exc:
        generate_build_plan_from_node(construction_session, ref_node.current)

    assert "Node has no query. Cannot generate a build plan without a query." in str(
        exc,
    )
