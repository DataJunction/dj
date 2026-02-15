"""Tests for the Node and NodeRevision resolvers."""

import pytest
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, patch


def test_columns_resolver_filters_by_attribute():
    """
    Test that the columns resolver filters columns by attribute
    """
    mock_column = mock.MagicMock()
    from datajunction_server.api.graphql.scalars.node import NodeRevision
    from datajunction_server.database import (
        NodeRevision as DBNodeRevision,
        ColumnAttribute,
        AttributeType,
    )
    from datajunction_server.database.column import Column

    import datajunction_server.sql.parsing.types as ct
    from datajunction_server.models.node_type import NodeType

    primary_key_attribute = AttributeType(namespace="system", name="primary_key")
    dimension_attribute = AttributeType(namespace="system", name="dimension")
    db_node_revision = DBNodeRevision(
        name="source_rev",
        type=NodeType.SOURCE,
        version="1",
        columns=[
            Column(
                name="random_primary_key",
                type=ct.StringType(),
                attributes=[ColumnAttribute(attribute_type=primary_key_attribute)],
                order=0,
            ),
            Column(
                name="user_id",
                type=ct.IntegerType(),
                attributes=[ColumnAttribute(attribute_type=dimension_attribute)],
                order=2,
            ),
            Column(
                name="foo",
                type=ct.FloatType(),
                order=3,
            ),
        ],
    )

    mock_node = mock.MagicMock()
    mock_node.columns = [mock_column]

    result = NodeRevision.columns(
        NodeRevision,
        root=db_node_revision,
        attributes=["primary_key"],
    )
    assert len(result) == 1
    assert result[0].name == "random_primary_key"

    result = NodeRevision.columns(
        NodeRevision,
        root=db_node_revision,
        attributes=["dimension"],
    )
    assert len(result) == 1
    assert result[0].name == "user_id"


@pytest.mark.asyncio
@patch("datajunction_server.api.graphql.resolvers.nodes.DBNode.get_by_name")
@patch("datajunction_server.api.graphql.resolvers.nodes.load_node_options")
async def test_get_node_by_name_with_no_fields(mock_load_options, mock_get_by_name):
    """Test get_node_by_name returns None when no fields provided"""
    from datajunction_server.api.graphql.resolvers.nodes import get_node_by_name

    session = AsyncMock()
    result = await get_node_by_name(session, None, "test.node")

    assert result is None
    mock_get_by_name.assert_not_called()
    mock_load_options.assert_not_called()


@pytest.mark.asyncio
@patch("datajunction_server.api.graphql.resolvers.nodes.DBNode.get_by_name")
@patch("datajunction_server.api.graphql.resolvers.nodes.load_node_options")
async def test_get_node_by_name_with_name_only(mock_load_options, mock_get_by_name):
    """Test get_node_by_name returns NodeName when only 'name' field requested"""
    from datajunction_server.api.graphql.resolvers.nodes import get_node_by_name
    from datajunction_server.api.graphql.scalars.node import NodeName

    session = AsyncMock()
    fields = {"name": {}}
    result = await get_node_by_name(session, fields, "test.node")

    assert isinstance(result, NodeName)
    assert result.name == "test.node"
    mock_get_by_name.assert_not_called()
    mock_load_options.assert_not_called()


@pytest.mark.asyncio
@patch("datajunction_server.api.graphql.resolvers.nodes.DBNode.get_by_name")
@patch("datajunction_server.api.graphql.resolvers.nodes.load_node_options")
async def test_get_node_by_name_with_nodes_key(mock_load_options, mock_get_by_name):
    """Test get_node_by_name when fields contains 'nodes' key"""
    from datajunction_server.api.graphql.resolvers.nodes import get_node_by_name

    session = AsyncMock()
    mock_node = MagicMock()
    mock_get_by_name.return_value = mock_node
    mock_load_options.return_value = ["option1", "option2"]

    fields = {"nodes": {"name": {}, "current": {"displayName": {}}}}
    result = await get_node_by_name(session, fields, "test.node")

    assert result == mock_node
    mock_load_options.assert_called_once_with(
        {"name": {}, "current": {"displayName": {}}},
    )
    mock_get_by_name.assert_called_once_with(
        session,
        name="test.node",
        options=["option1", "option2"],
    )


@pytest.mark.asyncio
@patch("datajunction_server.api.graphql.resolvers.nodes.DBNode.get_by_name")
@patch("datajunction_server.api.graphql.resolvers.nodes.load_node_options")
async def test_get_node_by_name_with_edges_key(mock_load_options, mock_get_by_name):
    """Test get_node_by_name when fields contains 'edges' > 'node' key"""
    from datajunction_server.api.graphql.resolvers.nodes import get_node_by_name

    session = AsyncMock()
    mock_node = MagicMock()
    mock_get_by_name.return_value = mock_node
    mock_load_options.return_value = ["option1"]

    fields = {"edges": {"node": {"name": {}, "type": {}}}}
    result = await get_node_by_name(session, fields, "test.node")

    assert result == mock_node
    mock_load_options.assert_called_once_with({"name": {}, "type": {}})
    mock_get_by_name.assert_called_once_with(
        session,
        name="test.node",
        options=["option1"],
    )


@pytest.mark.asyncio
@patch("datajunction_server.api.graphql.resolvers.nodes.DBNode.get_by_name")
@patch("datajunction_server.api.graphql.resolvers.nodes.load_node_options")
async def test_get_node_by_name_with_direct_fields(mock_load_options, mock_get_by_name):
    """Test get_node_by_name with direct fields (no 'nodes' or 'edges')"""
    from datajunction_server.api.graphql.resolvers.nodes import get_node_by_name

    session = AsyncMock()
    mock_node = MagicMock()
    mock_get_by_name.return_value = mock_node
    mock_load_options.return_value = []

    fields = {"name": {}, "current": {"displayName": {}}, "type": {}}
    result = await get_node_by_name(session, fields, "test.node")

    assert result == mock_node
    mock_load_options.assert_called_once_with(
        {"name": {}, "current": {"displayName": {}}, "type": {}},
    )
    mock_get_by_name.assert_called_once_with(
        session,
        name="test.node",
        options=[],
    )
