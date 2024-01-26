"""tests for building nodes"""

from typing import Dict, Optional, Tuple

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

import datajunction_server.sql.parsing.types as ct
from datajunction_server.construction.build import build_node
from datajunction_server.database.attributetype import AttributeType, ColumnAttribute
from datajunction_server.database.column import Column
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.models.node_type import NodeType
from datajunction_server.naming import amenable_name

from ..sql.utils import compare_query_strings
from .fixtures import BUILD_EXPECTATION_PARAMETERS


@pytest.mark.parametrize("node_name,db_id", BUILD_EXPECTATION_PARAMETERS)
def test_build_node(node_name: str, db_id: int, request):
    """
    Test building a node
    """
    construction_session: Session = request.getfixturevalue("construction_session")
    build_expectation: Dict[
        str,
        Dict[Optional[int], Tuple[bool, str]],
    ] = request.getfixturevalue("build_expectation")
    succeeds, expected = build_expectation[node_name][db_id]
    node = next(
        construction_session.execute(
            select(Node).filter(Node.name == node_name),
        ),
    )[0]

    if succeeds:
        ast = build_node(
            construction_session,
            node.current,
        )
        assert compare_query_strings(str(ast), expected)
    else:
        with pytest.raises(Exception) as exc:
            build_node(
                construction_session,
                node.current,
            )
            assert expected in str(exc)


def test_build_metric_with_dimensions_aggs(request):
    """
    Test building metric with dimensions
    """
    construction_session: Session = request.getfixturevalue("construction_session")
    num_comments_mtc: Node = next(
        construction_session.execute(
            select(Node).filter(Node.name == "basic.num_comments"),
        ),
    )[0]
    query = build_node(
        construction_session,
        num_comments_mtc.current,
        dimensions=["basic.dimension.users.country", "basic.dimension.users.gender"],
    )
    expected = """
        SELECT
          COUNT(1) AS basic_DOT_num_comments,
          basic_DOT_dimension_DOT_users.country,
          basic_DOT_dimension_DOT_users.gender
        FROM basic.source.comments AS basic_DOT_source_DOT_comments
        LEFT OUTER JOIN (
          SELECT
            basic_DOT_source_DOT_users.id,
            basic_DOT_source_DOT_users.country,
            basic_DOT_source_DOT_users.gender
          FROM basic.source.users AS basic_DOT_source_DOT_users
        ) AS basic_DOT_dimension_DOT_users ON basic_DOT_source_DOT_comments.user_id = basic_DOT_dimension_DOT_users.id
         GROUP BY
           basic_DOT_dimension_DOT_users.country, basic_DOT_dimension_DOT_users.gender
    """
    assert compare_query_strings(str(query), expected)


def test_build_metric_with_required_dimensions(request):
    """
    Test building metric with bound dimensions
    """
    construction_session: Session = request.getfixturevalue("construction_session")
    num_comments_mtc: Node = next(
        construction_session.execute(
            select(Node).filter(Node.name == "basic.num_comments_bnd"),
        ),
    )[0]
    query = build_node(
        construction_session,
        num_comments_mtc.current,
        dimensions=["basic.dimension.users.country", "basic.dimension.users.gender"],
    )
    expected = """
        SELECT
          COUNT(1) AS basic_DOT_num_comments_bnd,
          basic_DOT_source_DOT_comments.id,
          basic_DOT_source_DOT_comments.text,
          basic_DOT_dimension_DOT_users.country,
          basic_DOT_dimension_DOT_users.gender
        FROM basic.source.comments AS basic_DOT_source_DOT_comments
        LEFT OUTER JOIN (
          SELECT
            basic_DOT_source_DOT_users.id,
            basic_DOT_source_DOT_users.country,
            basic_DOT_source_DOT_users.gender
          FROM basic.source.users AS basic_DOT_source_DOT_users
        ) AS basic_DOT_dimension_DOT_users ON basic_DOT_source_DOT_comments.user_id = basic_DOT_dimension_DOT_users.id
         GROUP BY
           basic_DOT_source_DOT_comments.id, basic_DOT_source_DOT_comments.text, basic_DOT_dimension_DOT_users.country, basic_DOT_dimension_DOT_users.gender
    """
    assert compare_query_strings(str(query), expected)


def test_raise_on_build_without_required_dimension_column(request):
    """
    Test building a node that has a dimension reference without a column and a compound PK
    """
    construction_session: Session = request.getfixturevalue("construction_session")
    primary_key: AttributeType = next(
        construction_session.execute(
            select(AttributeType).filter(AttributeType.name == "primary_key"),
        ),
    )[0]
    countries_dim_ref = Node(
        name="basic.dimension.compound_countries",
        type=NodeType.DIMENSION,
        current_version="1",
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
    )
    node_foo_ref = Node(name="basic.foo", type=NodeType.TRANSFORM, current_version="1")
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
    )
    construction_session.add(node_foo)
    construction_session.flush()

    node_bar_ref = Node(name="basic.bar", type=NodeType.TRANSFORM, current_version="1")
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
    )
    build_node(
        construction_session,
        node_bar,
        dimensions=["basic.dimension.compound_countries.country_id2"],
    )


def test_build_metric_with_dimensions_filters(request):
    """
    Test building metric with dimension filters
    """
    construction_session: Session = request.getfixturevalue("construction_session")
    num_comments_mtc: Node = next(
        construction_session.execute(
            select(Node).filter(Node.name == "basic.num_comments"),
        ),
    )[0]
    query = build_node(
        construction_session,
        num_comments_mtc.current,
        filters=["basic.dimension.users.age>=25", "basic.dimension.users.age<50"],
    )
    expected = """
    SELECT
        COUNT(1) AS basic_DOT_num_comments,
        basic_DOT_dimension_DOT_users.age
    FROM basic.source.comments AS basic_DOT_source_DOT_comments
    LEFT OUTER JOIN (
      SELECT
        basic_DOT_source_DOT_users.id,
        basic_DOT_source_DOT_users.age
      FROM basic.source.users AS basic_DOT_source_DOT_users
    ) AS basic_DOT_dimension_DOT_users
      ON basic_DOT_source_DOT_comments.user_id = basic_DOT_dimension_DOT_users.id
    WHERE
      basic_DOT_dimension_DOT_users.age >= 25
      AND basic_DOT_dimension_DOT_users.age < 50
    """
    assert compare_query_strings(str(query), expected)


def test_build_node_with_unnamed_column(request):
    """
    Test building a node that has an unnamed column (so defaults to col<n>)
    """
    construction_session: Session = request.getfixturevalue("construction_session")
    node_foo_ref = Node(name="foo", type=NodeType.TRANSFORM, current_version="1")
    node_foo = NodeRevision(
        node=node_foo_ref,
        version="1",
        query="""SELECT 1 FROM basic.dimension.countries""",
        columns=[
            Column(name="col1", type=ct.IntegerType(), order=0),
        ],
    )
    build_node(
        construction_session,
        node_foo,
    )


def test_amenable_name():
    """testing for making an amenable name"""
    assert amenable_name("hello.名") == "hello_DOT__UNK"
