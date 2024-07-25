"""
Tests for API helpers.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.api import helpers
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.user import OAuthProvider, User
from datajunction_server.errors import DJDoesNotExistException, DJException
from datajunction_server.internal.nodes import propagate_valid_status
from datajunction_server.models.node import NodeStatus


@pytest.mark.asyncio
async def test_raise_get_node_when_node_does_not_exist(module__session: AsyncSession):
    """
    Test raising when a node doesn't exist
    """
    with pytest.raises(DJException) as exc_info:
        await helpers.get_node_by_name(
            session=module__session,
            name="foo",
            raise_if_not_exists=True,
        )

    assert "A node with name `foo` does not exist." in str(exc_info.value)
    with pytest.raises(DJException) as exc_info:
        await Node.get_by_name(
            session=module__session,
            name="foo",
            raise_if_not_exists=True,
        )

    assert "A node with name `foo` does not exist." in str(exc_info.value)


@pytest.mark.asyncio
async def test_propagate_valid_status(module__session: AsyncSession):
    """
    Test raising when trying to propagate a valid status using an invalid node
    """
    invalid_node = NodeRevision(
        name="foo",
        status=NodeStatus.INVALID,
    )
    example_user = User(
        id=1,
        username="userfoo",
        password="passwordfoo",
        name="djuser",
        email="userfoo@datajunction.io",
        oauth_provider=OAuthProvider.BASIC,
    )
    with pytest.raises(DJException) as exc_info:
        await propagate_valid_status(
            session=module__session,
            valid_nodes=[invalid_node],
            catalog_id=1,
            current_user=example_user,
        )

    assert "Cannot propagate valid status: Node `foo` is not valid" in str(
        exc_info.value,
    )


@pytest.mark.asyncio
async def test_get_node_namespace():
    """
    Test getting a node's namespace
    """
    # success
    mock_execute = AsyncMock(scalar_one_or_none=MagicMock(return_value="bar"))
    mock_session = AsyncMock(execute=AsyncMock(return_value=mock_execute))
    namespace = await helpers.get_node_namespace(
        session=mock_session,
        namespace="foo",
        raise_if_not_exists=True,
    )
    assert namespace == "bar"
    # error
    mock_execute = AsyncMock(scalar_one_or_none=MagicMock(return_value=None))
    mock_session = AsyncMock(execute=AsyncMock(return_value=mock_execute))
    with pytest.raises(DJDoesNotExistException) as exc_info:
        namespace = await helpers.get_node_namespace(
            session=mock_session,
            namespace="foo",
            raise_if_not_exists=True,
        )
    assert "node namespace `foo` does not exist" in str(exc_info.value)


@pytest.mark.asyncio
async def test_find_existing_cube():
    """
    Test finding an existing cube
    """
    mock_cube = MagicMock(current="foo-cube")
    mock_execute = AsyncMock(
        unique=MagicMock(
            return_value=MagicMock(
                scalars=MagicMock(
                    return_value=MagicMock(all=MagicMock(return_value=[mock_cube])),
                ),
            ),
        ),
    )
    mock_session = AsyncMock(execute=AsyncMock(return_value=mock_execute))
    node = await helpers.find_existing_cube(
        session=mock_session,
        metric_columns=[],
        dimension_columns=[],
        materialized=False,
    )
    assert node == "foo-cube"


@pytest.mark.asyncio
@patch("datajunction_server.api.helpers.ColumnMetadata", MagicMock)
@patch("datajunction_server.api.helpers.validate_cube")
@patch("datajunction_server.api.helpers.Node.get_by_name")
@patch("datajunction_server.api.helpers.find_existing_cube")
@patch("datajunction_server.api.helpers.get_catalog_by_name")
@patch("datajunction_server.api.helpers.build_materialized_cube_node")
@patch("datajunction_server.api.helpers.TranslatedSQL", MagicMock)
async def test_build_sql_for_multiple_metrics(
    mock_build_materialized_cube_node,
    mock_get_catalog_by_name,
    mock_find_existing_cube,
    mock_get_by_name,
    mock_validate_cube,
):
    """
    Test building SQL for multiple metrics
    """
    mock_build_materialized_cube_node.return_value = MagicMock()
    mock_get_catalog_by_name.return_value = MagicMock(
        engines=[MagicMock(), MagicMock()],
    )
    mock_find_existing_cube.return_value = MagicMock(
        availability=MagicMock(catalog="cata-foo"),
    )
    mock_get_by_name.return_value = MagicMock(
        current=MagicMock(catalog=MagicMock(engines=["eng1", "eng2"])),
    )
    mock_metric_columns = [MagicMock(name="col1"), MagicMock(name="col2")]
    mock_metric_nodes = ["mnode1", "mnode2"]
    dimension_columns = [MagicMock(name="dim1"), MagicMock(name="dim2")]
    _ = MagicMock()
    mock_validate_cube.return_value = (
        mock_metric_columns,
        mock_metric_nodes,
        _,
        dimension_columns,
        _,
    )
    mock_session = AsyncMock()

    sql = await helpers.build_sql_for_multiple_metrics(
        session=mock_session,
        metrics=["m1", "m2"],
        dimensions=[],
    )
    assert sql is not None
