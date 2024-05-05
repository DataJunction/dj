"""
Tests for API helpers.
"""

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.api import helpers
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.errors import DJException
from datajunction_server.internal.nodes import propagate_valid_status
from datajunction_server.models.node import NodeStatus


@pytest.mark.asyncio
async def test_raise_get_node_when_node_does_not_exist(session: AsyncSession):
    """
    Test raising when a node doesn't exist
    """
    with pytest.raises(DJException) as exc_info:
        await helpers.get_node_by_name(
            session=session,
            name="foo",
            raise_if_not_exists=True,
        )

    assert "A node with name `foo` does not exist." in str(exc_info.value)
    with pytest.raises(DJException) as exc_info:
        await Node.get_by_name(session=session, name="foo", raise_if_not_exists=True)

    assert "A node with name `foo` does not exist." in str(exc_info.value)


@pytest.mark.asyncio
async def test_propagate_valid_status(session: AsyncSession):
    """
    Test raising when trying to propagate a valid status using an invalid node
    """
    invalid_node = NodeRevision(
        name="foo",
        status=NodeStatus.INVALID,
    )
    with pytest.raises(DJException) as exc_info:
        await propagate_valid_status(
            session=session,
            valid_nodes=[invalid_node],
            catalog_id=1,
        )

    assert "Cannot propagate valid status: Node `foo` is not valid" in str(
        exc_info.value,
    )
