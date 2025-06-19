"""Tests for the Node and NodeRevision resolvers."""

from unittest import mock


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
