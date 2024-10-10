"""tests for building nodes"""

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import datajunction_server.sql.parsing.types as ct
from datajunction_server.construction.build import (
    build_materialized_cube_node,
    build_metric_nodes,
    build_temp_select,
    get_default_criteria,
)
from datajunction_server.construction.build_v2 import QueryBuilder
from datajunction_server.database.attributetype import AttributeType, ColumnAttribute
from datajunction_server.database.column import Column
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.user import User
from datajunction_server.errors import DJException
from datajunction_server.models.engine import Dialect
from datajunction_server.models.node_type import NodeType
from datajunction_server.naming import amenable_name
from datajunction_server.sql.parsing.backends.antlr4 import ast, parse


@pytest.mark.asyncio
async def test_build_metric_with_dimensions_aggs(construction_session: AsyncSession):
    """
    Test building metric with dimensions
    """
    num_comments_mtc: Node = await Node.get_by_name(  # type: ignore
        construction_session,
        "basic.num_comments",
    )
    query = await build_metric_nodes(
        construction_session,
        [num_comments_mtc],
        filters=[],
        dimensions=["basic.dimension.users.country", "basic.dimension.users.gender"],
        orderby=[],
    )
    expected = """
    WITH basic_DOT_source_DOT_comments AS (
      SELECT
        basic_DOT_source_DOT_comments.id,
        basic_DOT_source_DOT_comments.user_id,
        basic_DOT_source_DOT_comments.timestamp,
        basic_DOT_source_DOT_comments.text
      FROM basic.source.comments AS basic_DOT_source_DOT_comments
    ),
    basic_DOT_dimension_DOT_users AS (
      SELECT
        basic_DOT_source_DOT_users.id,
        basic_DOT_source_DOT_users.full_name,
        basic_DOT_source_DOT_users.age,
        basic_DOT_source_DOT_users.country,
        basic_DOT_source_DOT_users.gender,
        basic_DOT_source_DOT_users.preferred_language,
        basic_DOT_source_DOT_users.secret_number
      FROM basic.source.users AS basic_DOT_source_DOT_users
    ),
    basic_DOT_source_DOT_comments_metrics AS (
      SELECT
        basic_DOT_dimension_DOT_users.country basic_DOT_dimension_DOT_users_DOT_country,
        basic_DOT_dimension_DOT_users.gender basic_DOT_dimension_DOT_users_DOT_gender,
        COUNT(1) AS basic_DOT_num_comments
      FROM basic_DOT_source_DOT_comments
      INNER JOIN basic_DOT_dimension_DOT_users
        ON basic_DOT_source_DOT_comments.user_id = basic_DOT_dimension_DOT_users.id
      GROUP BY
        basic_DOT_dimension_DOT_users.country, basic_DOT_dimension_DOT_users.gender
    )
    SELECT
      basic_DOT_source_DOT_comments_metrics.basic_DOT_dimension_DOT_users_DOT_country,
      basic_DOT_source_DOT_comments_metrics.basic_DOT_dimension_DOT_users_DOT_gender,
      basic_DOT_source_DOT_comments_metrics.basic_DOT_num_comments
    FROM basic_DOT_source_DOT_comments_metrics
    """
    assert str(parse(str(query))) == str(parse(str(expected)))


@pytest.mark.asyncio
async def test_build_metric_with_required_dimensions(
    construction_session: AsyncSession,
):
    """
    Test building metric with bound dimensions
    """
    num_comments_mtc: Node = await Node.get_by_name(  # type: ignore
        construction_session,
        "basic.num_comments_bnd",
    )

    query = await build_metric_nodes(
        construction_session,
        [num_comments_mtc],
        filters=[],
        dimensions=["basic.dimension.users.country", "basic.dimension.users.gender"],
        orderby=[],
    )
    expected = """
    WITH basic_DOT_source_DOT_comments AS (
      SELECT
        basic_DOT_source_DOT_comments.id,
        basic_DOT_source_DOT_comments.user_id,
        basic_DOT_source_DOT_comments.timestamp,
        basic_DOT_source_DOT_comments.text
      FROM basic.source.comments AS basic_DOT_source_DOT_comments
    ),
    basic_DOT_dimension_DOT_users AS (
      SELECT
        basic_DOT_source_DOT_users.id,
        basic_DOT_source_DOT_users.full_name,
        basic_DOT_source_DOT_users.age,
        basic_DOT_source_DOT_users.country,
        basic_DOT_source_DOT_users.gender,
        basic_DOT_source_DOT_users.preferred_language,
        basic_DOT_source_DOT_users.secret_number
      FROM basic.source.users AS basic_DOT_source_DOT_users
    ),
    basic_DOT_source_DOT_comments_metrics AS (
      SELECT
        basic_DOT_dimension_DOT_users.country basic_DOT_dimension_DOT_users_DOT_country,
        basic_DOT_dimension_DOT_users.gender basic_DOT_dimension_DOT_users_DOT_gender,
        COUNT(1) AS basic_DOT_num_comments_bnd
      FROM basic_DOT_source_DOT_comments
      INNER JOIN basic_DOT_dimension_DOT_users
        ON basic_DOT_source_DOT_comments.user_id = basic_DOT_dimension_DOT_users.id
      GROUP BY  basic_DOT_dimension_DOT_users.country, basic_DOT_dimension_DOT_users.gender
    )
    SELECT
      basic_DOT_source_DOT_comments_metrics.basic_DOT_dimension_DOT_users_DOT_country,
      basic_DOT_source_DOT_comments_metrics.basic_DOT_dimension_DOT_users_DOT_gender,
      basic_DOT_source_DOT_comments_metrics.basic_DOT_num_comments_bnd
    FROM basic_DOT_source_DOT_comments_metrics
    """
    print("blahhhh", str(query))
    assert str(parse(str(query))) == str(parse(str(expected)))


@pytest.mark.asyncio
@pytest.mark.skip(reason="Shouldn't be needed with complex dim links")
async def test_raise_on_build_without_required_dimension_column(
    construction_session: AsyncSession,
    current_user: User,
):
    """
    Test building a node that has a dimension reference without a column and a compound PK
    """
    primary_key: AttributeType = next(
        await construction_session.execute(
            select(AttributeType).filter(AttributeType.name == "primary_key"),
        ),
    )[0]
    countries_dim_ref = Node(
        name="basic.dimension.compound_countries",
        type=NodeType.DIMENSION,
        current_version="1",
        created_by_id=current_user.id,
    )
    NodeRevision(
        name=countries_dim_ref.name,
        type=countries_dim_ref.type,
        node=countries_dim_ref,
        version="1",
        query="""
              SELECT country,
                    'abcd' AS country_id2,
                     COUNT(1) AS user_cnt
              FROM basic.dimension.users
              GROUP BY country
            """,
        columns=[
            Column(
                name="country",
                type=ct.StringType(),
                attributes=[ColumnAttribute(attribute_type=primary_key)],
                order=0,
            ),
            Column(
                name="country_id2",
                type=ct.StringType(),
                attributes=[ColumnAttribute(attribute_type=primary_key)],
                order=1,
            ),
            Column(name="user_cnt", type=ct.IntegerType(), order=2),
        ],
        created_by_id=current_user.id,
    )
    node_foo_ref = Node(
        name="basic.foo",
        type=NodeType.TRANSFORM,
        current_version="1",
        created_by_id=current_user.id,
    )
    node_foo = NodeRevision(
        name=node_foo_ref.name,
        type=node_foo_ref.type,
        node=node_foo_ref,
        version="1",
        query="""SELECT num_users, 'abcd' AS country_id FROM basic.transform.country_agg""",
        columns=[
            Column(
                name="num_users",
                type=ct.IntegerType(),
                order=0,
            ),
            Column(
                name="country_id",
                type=ct.StringType(),
                dimension=countries_dim_ref,
                order=1,
            ),
        ],
        created_by_id=current_user.id,
    )
    construction_session.add(node_foo)

    node_bar_ref = Node(
        name="basic.bar",
        type=NodeType.TRANSFORM,
        current_version="1",
        created_by_id=current_user.id,
    )
    node_bar = NodeRevision(
        name=node_bar_ref.name,
        type=node_bar_ref.type,
        node=node_bar_ref,
        version="1",
        query="SELECT SUM(num_users) AS num_users "
        "FROM basic.foo GROUP BY basic.dimension.compound_countries.country",
        columns=[
            Column(name="num_users", type=ct.IntegerType(), order=0),
        ],
        created_by_id=current_user.id,
    )
    construction_session.add(node_bar)
    await construction_session.commit()
    with pytest.raises(DJException):
        query_builder = await QueryBuilder.create(construction_session, node_bar)
        (
            query_builder.add_dimension(
                "basic.dimension.compound_countries.country_id2",
            ).build()
        )


@pytest.mark.asyncio
async def test_build_metric_with_dimensions_filters(construction_session: AsyncSession):
    """
    Test building metric with dimension filters
    """
    num_comments_mtc: Node = await Node.get_by_name(  # type: ignore
        construction_session,
        "basic.num_comments",
    )
    query = await build_metric_nodes(
        construction_session,
        [num_comments_mtc],
        filters=[
            "basic.dimension.users.age>=25",
            "basic.dimension.users.age<50",
        ],
        dimensions=[],
        orderby=[],
    )
    expected = """
    WITH basic_DOT_source_DOT_comments AS (
      SELECT
        basic_DOT_source_DOT_comments.id,
        basic_DOT_source_DOT_comments.user_id,
        basic_DOT_source_DOT_comments.timestamp,
        basic_DOT_source_DOT_comments.text
      FROM basic.source.comments AS basic_DOT_source_DOT_comments
    ),
    basic_DOT_dimension_DOT_users AS (
      SELECT  basic_DOT_source_DOT_users.id,
        basic_DOT_source_DOT_users.full_name,
        basic_DOT_source_DOT_users.age,
        basic_DOT_source_DOT_users.country,
        basic_DOT_source_DOT_users.gender,
        basic_DOT_source_DOT_users.preferred_language,
        basic_DOT_source_DOT_users.secret_number
      FROM basic.source.users AS basic_DOT_source_DOT_users
      WHERE  basic_DOT_source_DOT_users.age >= 25 AND basic_DOT_source_DOT_users.age < 50
    ),
    basic_DOT_source_DOT_comments_metrics AS (
      SELECT
        COUNT(1) AS basic_DOT_num_comments
      FROM basic_DOT_source_DOT_comments
      INNER JOIN basic_DOT_dimension_DOT_users
        ON basic_DOT_source_DOT_comments.user_id = basic_DOT_dimension_DOT_users.id
    )
    SELECT  basic_DOT_source_DOT_comments_metrics.basic_DOT_num_comments
    FROM basic_DOT_source_DOT_comments_metrics
    """
    assert str(parse(str(query))) == str(parse(expected))


def test_amenable_name():
    """testing for making an amenable name"""
    assert amenable_name("hello.名") == "hello_DOT__UNK"


def test_get_default_criteria():
    """Test getting default criteria for a node revision"""
    result = get_default_criteria(node=NodeRevision(type=NodeType.TRANSFORM))
    assert result.dialect == Dialect.SPARK
    assert result.target_node_name is None


@patch("datajunction_server.construction.build.parse")
def test_build_temp_select(mock_parse):
    """Test building a temporary select statement"""
    mock_columns = [MagicMock(name="foo"), MagicMock(name="bar")]
    mock_select = MagicMock(find_all=MagicMock(return_value=mock_columns))
    mock_parse().select = mock_select
    test_select = build_temp_select(temp_query="SELECT * FROM foo")
    assert test_select == mock_select


def test_build_materialized_cube_node():
    """Test building a materialized cube node"""
    result = build_materialized_cube_node(
        selected_metrics=[],
        selected_dimensions=[MagicMock(name="dim1"), MagicMock(name="dim2")],
        cube=NodeRevision(
            name="foo",
            type=NodeType.CUBE,
            query="SELECT * FROM foo",
            columns=[],
            version="1",
            materializations=[MagicMock()],
            availability=MagicMock(table=MagicMock(name="foo")),
        ),
        filters=["filter1", "filter2"],
        orderby=["order1", "order2"],
        limit=10,
    )
    assert result == ast.Query(
        name=ast.DefaultName(name="", quote_style="", namespace=None),
        alias=None,
        as_=None,
        semantic_entity=None,
        semantic_type=None,
        column_list=[],
        _columns=[],
        select=ast.Select(
            alias=None,
            as_=None,
            semantic_entity=None,
            semantic_type=None,
            quantifier="",
            projection=[],
            from_=ast.From(
                relations=[
                    ast.Relation(
                        primary=ast.Table(
                            name=ast.Name(name="foo", quote_style="", namespace=None),
                            alias=None,
                            as_=None,
                            semantic_entity=None,
                            semantic_type=None,
                            column_list=[],
                            _columns=[],
                        ),
                        extensions=[],
                    ),
                ],
            ),
            group_by=[],
            having=None,
            where=None,
            lateral_views=[],
            set_op=None,
            limit=None,
            organization=None,
            hints=None,
        ),
        ctes=[],
    )
