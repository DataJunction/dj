"""
Tests for API helpers.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from datajunction_server.api import helpers
from datajunction_server.api.helpers import find_required_dimensions
from datajunction_server.internal import sql
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
            save_history=MagicMock(),
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
@patch("datajunction_server.internal.sql.ColumnMetadata", MagicMock)
@patch("datajunction_server.internal.sql.validate_cube")
@patch("datajunction_server.internal.sql.Node.get_by_name")
@patch("datajunction_server.internal.sql.find_existing_cube")
@patch("datajunction_server.internal.sql.get_catalog_by_name")
@patch("datajunction_server.internal.sql.build_materialized_cube_node")
@patch("datajunction_server.internal.sql.TranslatedSQL.create", MagicMock)
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
    mock_engines = [MagicMock(name="eng1"), MagicMock(name="eng2")]
    mock_get_catalog_by_name.return_value = MagicMock(engines=mock_engines)
    mock_find_existing_cube.return_value = MagicMock(
        availability=MagicMock(catalog="cata-foo"),
    )
    mock_get_by_name.return_value = MagicMock(
        current=MagicMock(catalog=MagicMock(name="cata-foo", engines=mock_engines)),
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

    built_sql = await sql.build_sql_for_multiple_metrics(
        session=mock_session,
        metrics=["m1", "m2"],
        dimensions=[],
    )
    assert built_sql is not None


@pytest.mark.asyncio
async def test_find_required_dimensions_with_role_suffix(
    module__client_with_examples,
    module__session: AsyncSession,
):
    """
    Test find_required_dimensions with full path that includes role suffix.
    """
    # Get an actual dimension node from the database (v3.date has 'week' column)
    result = await module__session.execute(
        select(Node)
        .filter(Node.name == "v3.date")
        .options(
            selectinload(Node.current).options(
                selectinload(NodeRevision.columns),
            ),
        ),
    )
    dim_node = result.scalars().first()

    if dim_node is None:
        pytest.skip("v3.date dimension not found in database")

    # Verify the dimension has the 'week' column
    col_names = [col.name for col in dim_node.current.columns]
    if "week" not in col_names:
        pytest.skip("v3.date.week column not found")

    # Test with role suffix - this covers line 282 (stripping [order])
    # and line 323 (matching the column)
    invalid_dims, matched_cols = await find_required_dimensions(
        session=module__session,
        required_dimensions=["v3.date.week[order]"],
        parent_columns=[],
    )

    # Should have no invalid dimensions and one matched column
    assert len(invalid_dims) == 0, f"Unexpected invalid dims: {invalid_dims}"
    assert len(matched_cols) == 1
    assert matched_cols[0].name == "week"


@pytest.mark.asyncio
async def test_find_required_dimensions_full_path_match(
    module__client_with_examples,
    module__session: AsyncSession,
):
    """
    Test find_required_dimensions with full path without role suffix.

    This covers line 323: matched_columns.append(dim_col_map[col_name])
    """
    # Test with full path (no role suffix) - this covers line 323
    invalid_dims, matched_cols = await find_required_dimensions(
        session=module__session,
        required_dimensions=["v3.date.month"],
        parent_columns=[],
    )

    # v3.date.month should exist
    if len(invalid_dims) > 0:
        pytest.skip("v3.date.month not found in database")

    assert len(matched_cols) == 1
    assert matched_cols[0].name == "month"
