"""tests for building nodes"""

from typing import Dict, Optional, Tuple

import pytest
from sqlalchemy import select
from sqlmodel import Session

from dj.construction.build import amenable_name, build_node
from dj.errors import DJException
from dj.models import Column, Database, Node, Table
from dj.models.node import NodeType
from dj.typing import ColumnType

from ..sql.utils import compare_query_strings
from .fixtures import BUILD_EXPECTATION_PARAMETERS


@pytest.mark.parametrize("node_name,db_id", BUILD_EXPECTATION_PARAMETERS)
@pytest.mark.asyncio
async def test_build_node(node_name: str, db_id: int, mocker, request):
    """
    Test building a node
    """
    mocker.patch("dj.models.database.Database.do_ping", return_value=True)
    construction_session: Session = request.getfixturevalue("construction_session")
    build_expectation: Dict[
        str,
        Dict[Optional[int], Tuple[bool, str]],
    ] = request.getfixturevalue("build_expectation")
    succeeds, expected = build_expectation[node_name][db_id]
    node = next(construction_session.exec(select(Node).filter(Node.name == node_name)))[
        0
    ]

    if succeeds:
        ast, _ = await build_node(construction_session, node, database_id=db_id)
        assert compare_query_strings(str(ast), expected)
    else:
        with pytest.raises(Exception) as exc:
            await build_node(construction_session, node, database_id=db_id)
            assert expected in str(exc)


@pytest.mark.asyncio
async def test_build_metric_with_dimensions_aggs(mocker, request):
    """
    Test building metric with dimensions
    """
    mocker.patch("dj.models.database.Database.do_ping", return_value=True)

    construction_session: Session = request.getfixturevalue("construction_session")
    num_comments_mtc: Node = next(
        construction_session.exec(
            select(Node).filter(Node.name == "basic.num_comments"),
        ),
    )[0]
    query, _ = await build_node(
        construction_session,
        num_comments_mtc,
        aggs=["basic.dimension.users.country", "basic.dimension.users.gender"],
    )

    expected = """
    SELECT  COUNT(1) AS cnt
    FROM basic.comments
    LEFT JOIN (SELECT  basic.comments.id,
        basic.comments.full_name,
        basic.comments.age,
        basic.comments.country,
        basic.comments.gender,
        basic.comments.preferred_language,
        basic.comments.secret_number
    FROM basic.comments

    ) AS basic_DOT_dimension_DOT_users
            ON basic.comments.user_id = basic_DOT_dimension_DOT_users.id
    GROUP BY  basic_DOT_dimension_DOT_users.country, basic_DOT_dimension_DOT_users.gender
    """

    assert compare_query_strings(str(query), expected)


@pytest.mark.asyncio
async def test_raise_on_build_without_required_dimension_column(mocker, request):
    """
    Test building a node that has a dimension reference without a column and no default `id`
    """
    mocker.patch("dj.models.database.Database.do_ping", return_value=True)

    construction_session: Session = request.getfixturevalue("construction_session")
    country_dim: Node = next(
        construction_session.exec(
            select(Node).filter(Node.name == "basic.dimension.countries"),
        ),
    )[0]
    node_foo = Node(
        name="foo",
        type=NodeType.TRANSFORM,
        query="""SELECT num_users FROM basic.transform.country_agg""",
        columns=[
            Column(name="num_users", type=ColumnType.INT, dimension=country_dim),
        ],
    )
    construction_session.add(node_foo)
    construction_session.flush()
    node_bar = Node(
        name="bar",
        type=NodeType.TRANSFORM,
        query="""SELECT num_users FROM foo GROUP BY basic.dimension.countries.country""",
        columns=[
            Column(name="num_users", type=ColumnType.STR),
        ],
    )
    with pytest.raises(DJException) as exc_info:
        await build_node(
            construction_session,
            node_bar,
        )

    assert (
        "Node foo specifiying dimension basic.dimension.countries on column "
        "num_users does not specify a dimension column, but basic.dimension.countries "
        "does not have the default key `id`."
    ) in str(exc_info.value)


@pytest.mark.asyncio
async def test_build_metric_with_dimensions_filters(mocker, request):
    """
    Test building metric with dimension filters
    """
    mocker.patch("dj.models.database.Database.do_ping", return_value=True)

    construction_session: Session = request.getfixturevalue("construction_session")
    num_comments_mtc: Node = next(
        construction_session.exec(
            select(Node).filter(Node.name == "basic.num_comments"),
        ),
    )[0]
    query, _ = await build_node(
        construction_session,
        num_comments_mtc,
        filters=["basic.dimension.users.age>=25", "basic.dimension.users.age<50"],
    )

    expected = """
    SELECT  COUNT(1) AS cnt
    FROM basic.comments
    LEFT JOIN (SELECT  basic.comments.id,
        basic.comments.full_name,
        basic.comments.age,
        basic.comments.country,
        basic.comments.gender,
        basic.comments.preferred_language,
        basic.comments.secret_number
    FROM basic.comments

    ) AS basic_DOT_dimension_DOT_users
            ON basic.comments.user_id = basic_DOT_dimension_DOT_users.id
    WHERE  basic_DOT_dimension_DOT_users.age >= 25 AND basic_DOT_dimension_DOT_users.age < 50
    """

    assert compare_query_strings(str(query), expected)


@pytest.mark.asyncio
async def test_build_metric_with_database_id_specified(mocker, request):
    """
    Test building metric with a specific database selected equivalent to the most optimal database
    """
    mocker.patch("dj.models.database.Database.do_ping", return_value=True)

    construction_session: Session = request.getfixturevalue("construction_session")
    node_foo = Node(
        name="foo",
        type=NodeType.TRANSFORM,
        query="""SELECT num_users FROM basic.transform.country_agg""",
        columns=[
            Column(name="num_users", type=ColumnType.STR),
        ],
        tables=[
            Table(
                node_id=4254,
                schema="test",
                table="foo",
                columns=[
                    Column(name="num_users", type=ColumnType.STR),
                ],
                cost=10.0,
                database=Database(name="postgres", URI="", cost=10, id=1),
                database_id=1,
            ),
            Table(
                node_id=4254,
                schema="slowtest",
                table="foo",
                columns=[
                    Column(name="num_users", type=ColumnType.STR),
                ],
                cost=10.0,
                database=Database(name="postgres", URI="", cost=100, id=1),
                database_id=2,
            ),
        ],
    )
    await build_node(construction_session, node_foo, database_id=1)
    await build_node(  # Also test when no database_id is set
        construction_session,
        node_foo,
    )


@pytest.mark.asyncio
async def test_build_node_with_unnamed_column(mocker, request):
    """
    Test building a node that has an unnamed column (so defaults to _col<n>)
    """
    mocker.patch("dj.models.database.Database.do_ping", return_value=True)

    construction_session: Session = request.getfixturevalue("construction_session")
    node_foo = Node(
        name="foo",
        type=NodeType.TRANSFORM,
        query="""SELECT 1 FROM basic.dimension.countries""",
        columns=[
            Column(name="_col1", type=ColumnType.INT),
        ],
    )
    await build_node(
        construction_session,
        node_foo,
    )


def test_amenable_name():
    """testing for making an amenable name"""
    assert amenable_name("hello.名") == "hello_DOT__UNK_"
