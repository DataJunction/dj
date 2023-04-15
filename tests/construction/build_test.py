"""tests for building nodes"""

from typing import Dict, Optional, Tuple

import pytest
from sqlalchemy import select
from sqlmodel import Session

import dj.sql.parsing.types as ct
from dj.construction.build import amenable_name, build_node
from dj.errors import DJException
from dj.models import Column, NodeRevision
from dj.models.node import Node, NodeType

from ..sql.utils import compare_query_strings
from .fixtures import BUILD_EXPECTATION_PARAMETERS


@pytest.mark.parametrize("node_name,db_id", BUILD_EXPECTATION_PARAMETERS)
@pytest.mark.asyncio
async def test_build_node(node_name: str, db_id: int, request):
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
        construction_session.exec(
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


@pytest.mark.asyncio
async def test_build_metric_with_dimensions_aggs(request):
    """
    Test building metric with dimensions
    """
    construction_session: Session = request.getfixturevalue("construction_session")
    num_comments_mtc: Node = next(
        construction_session.exec(
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
          basic_DOT_dimension_DOT_users.country,
          basic_DOT_dimension_DOT_users.gender,
          COUNT(1) AS cnt
        FROM basic.source.comments AS basic_DOT_source_DOT_comments
        LEFT OUTER JOIN (
          SELECT
            basic_DOT_source_DOT_users.country,
            basic_DOT_source_DOT_users.gender,
            basic_DOT_source_DOT_users.id
          FROM basic.source.users AS basic_DOT_source_DOT_users
        ) AS basic_DOT_dimension_DOT_users ON basic_DOT_source_DOT_comments.user_id = basic_DOT_dimension_DOT_users.id
         GROUP BY
           basic_DOT_dimension_DOT_users.country, basic_DOT_dimension_DOT_users.gender
    """
    assert compare_query_strings(str(query), expected)


@pytest.mark.asyncio
async def test_raise_on_build_without_required_dimension_column(request):
    """
    Test building a node that has a dimension reference without a column and no default `id`
    """
    construction_session: Session = request.getfixturevalue("construction_session")
    country_dim: Node = next(
        construction_session.exec(
            select(Node).filter(Node.name == "basic.dimension.countries"),
        ),
    )[0]
    node_foo_ref = Node(name="foo", type=NodeType.TRANSFORM, current_version="1")
    node_foo = NodeRevision(
        name=node_foo_ref.name,
        type=node_foo_ref.type,
        node=node_foo_ref,
        version="1",
        query="""SELECT num_users FROM basic.transform.country_agg""",
        columns=[
            Column(
                name="num_users",
                type=ct.IntegerType(),
                dimension=country_dim,
            ),
        ],
    )
    construction_session.add(node_foo)
    construction_session.flush()

    node_bar_ref = Node(name="bar", type=NodeType.TRANSFORM, current_version="1")
    node_bar = NodeRevision(
        name=node_bar_ref.name,
        type=node_bar_ref.type,
        node=node_bar_ref,
        version="1",
        query="SELECT SUM(num_users) AS num_users "
        "FROM foo GROUP BY basic.dimension.countries.country",
        columns=[
            Column(name="num_users", type=ct.IntegerType()),
        ],
    )
    with pytest.raises(DJException) as exc_info:
        build_node(
            construction_session,
            node_bar,
        )

    assert (
        "Node foo specifying dimension basic.dimension.countries on column "
        "num_users does not specify a dimension column, but basic.dimension"
        ".countries does not have the default key `id`."
    ) in str(exc_info.value)


@pytest.mark.asyncio
async def test_build_metric_with_dimensions_filters(request):
    """
    Test building metric with dimension filters
    """
    construction_session: Session = request.getfixturevalue("construction_session")
    num_comments_mtc: Node = next(
        construction_session.exec(
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
      COUNT(1) AS cnt
    FROM basic.source.comments AS basic_DOT_source_DOT_comments
    LEFT OUTER JOIN (
      SELECT
        basic_DOT_source_DOT_users.age,
        basic_DOT_source_DOT_users.id
      FROM basic.source.users AS basic_DOT_source_DOT_users
    ) AS basic_DOT_dimension_DOT_users
      ON basic_DOT_source_DOT_comments.user_id = basic_DOT_dimension_DOT_users.id
    WHERE
      basic_DOT_dimension_DOT_users.age >= 25
      AND basic_DOT_dimension_DOT_users.age < 50
    """
    assert compare_query_strings(str(query), expected)


@pytest.mark.asyncio
async def test_build_node_with_unnamed_column(request):
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
            Column(name="col1", type=ct.IntegerType()),
        ],
    )
    build_node(
        construction_session,
        node_foo,
    )


def test_amenable_name():
    """testing for making an amenable name"""
    assert amenable_name("hello.名") == "hello_DOT__UNK"
