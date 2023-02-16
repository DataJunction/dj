"""fixtures for testing construction"""
# noqa: W191,E101

from typing import Dict, List, Optional, Tuple

import pytest
from sqlmodel import Session

from dj.models import Column, Database, NodeRevision, Table
from dj.models.column import ColumnType
from dj.models.node import Node, NodeType

BUILD_NODE_NAMES: List[str] = [
    "basic.source.users",
    "basic.source.comments",
    "basic.dimension.users",
    "dbt.source.jaffle_shop.orders",
    "dbt.dimension.customers",
    "dbt.source.jaffle_shop.customers",
    "basic.dimension.countries",
    "basic.transform.country_agg",
    "basic.num_comments",
    "basic.num_users",
    "dbt.transform.customer_agg",
]

BUILD_EXPECTATION_PARAMETERS: List[Tuple[str, Optional[int]]] = list(
    zip(
        BUILD_NODE_NAMES * 3,
        [None] * len(BUILD_NODE_NAMES)
        + [1] * len(BUILD_NODE_NAMES)  # type: ignore
        + [2] * len(BUILD_NODE_NAMES),  # type: ignore
    ),
)


@pytest.fixture
def build_expectation() -> Dict[str, Dict[Optional[int], Tuple[bool, str]]]:
    """map node names with database ids to what their build results should be"""
    return {
        """basic.source.users""": {
            None: (
                False,
                """Node has no query. Cannot generate a build plan without a query.""",
            ),
            1: (
                False,
                """Node has no query. Cannot generate a build plan without a query.""",
            ),
            2: (
                False,
                """Node has no query. Cannot generate a build plan without a query.""",
            ),
        },
        """basic.source.comments""": {
            None: (
                False,
                """Node has no query. Cannot generate a build plan without a query.""",
            ),
            1: (
                False,
                """Node has no query. Cannot generate a build plan without a query.""",
            ),
            2: (
                False,
                """Node has no query. Cannot generate a build plan without a query.""",
            ),
        },
        """basic.dimension.users""": {
            None: (
                True,
                """SELECT  basic.comments.id,
    basic.comments.full_name,
    basic.comments.age,
    basic.comments.country,
    basic.comments.gender,
    basic.comments.preferred_language,
    basic.comments.secret_number
 FROM basic.comments""",
            ),
            1: (
                True,
                """SELECT  basic.comments.id,
    basic.comments.full_name,
    basic.comments.age,
    basic.comments.country,
    basic.comments.gender,
    basic.comments.preferred_language,
    basic.comments.secret_number
 FROM basic.comments""",
            ),
            2: (
                True,
                """SELECT  comments.id,
    comments.full_name,
    comments.age,
    comments.country,
    comments.gender,
    comments.preferred_language,
    comments.secret_number
 FROM comments""",
            ),
        },
        """dbt.source.jaffle_shop.orders""": {
            None: (
                False,
                """Node has no query. Cannot generate a build plan without a query.""",
            ),
            1: (
                False,
                """Node has no query. Cannot generate a build plan without a query.""",
            ),
            2: (
                False,
                """Node has no query. Cannot generate a build plan without a query.""",
            ),
        },
        """dbt.dimension.customers""": {
            None: (
                True,
                """SELECT  jaffle_shop.customers.id,
    jaffle_shop.customers.first_name,
    jaffle_shop.customers.last_name
 FROM jaffle_shop.customers""",
            ),
            1: (
                True,
                """SELECT  jaffle_shop.customers.id,
    jaffle_shop.customers.first_name,
    jaffle_shop.customers.last_name
 FROM jaffle_shop.customers""",
            ),
            2: (False, """The requested database with id 2 cannot run this query."""),
        },
        """dbt.source.jaffle_shop.customers""": {
            None: (
                False,
                """Node has no query. Cannot generate a build plan without a query.""",
            ),
            1: (
                False,
                """Node has no query. Cannot generate a build plan without a query.""",
            ),
            2: (
                False,
                """Node has no query. Cannot generate a build plan without a query.""",
            ),
        },
        """basic.dimension.countries""": {
            None: (
                True,
                """SELECT  basic_DOT_dimension_DOT_users.country,
    COUNT(1) AS user_cnt
 FROM (SELECT  basic.comments.id,
    basic.comments.full_name,
    basic.comments.age,
    basic.comments.country,
    basic.comments.gender,
    basic.comments.preferred_language,
    basic.comments.secret_number
 FROM basic.comments

) AS basic_DOT_dimension_DOT_users

 GROUP BY  basic_DOT_dimension_DOT_users.country""",
            ),
            1: (
                True,
                """SELECT  basic_DOT_dimension_DOT_users.country,
    COUNT(1) AS user_cnt
 FROM (SELECT  basic.comments.id,
    basic.comments.full_name,
    basic.comments.age,
    basic.comments.country,
    basic.comments.gender,
    basic.comments.preferred_language,
    basic.comments.secret_number
 FROM basic.comments

) AS basic_DOT_dimension_DOT_users

 GROUP BY  basic_DOT_dimension_DOT_users.country""",
            ),
            2: (
                True,
                """SELECT  basic_DOT_dimension_DOT_users.country,
    COUNT(1) AS user_cnt
 FROM (SELECT  comments.id,
    comments.full_name,
    comments.age,
    comments.country,
    comments.gender,
    comments.preferred_language,
    comments.secret_number
 FROM comments

) AS basic_DOT_dimension_DOT_users

 GROUP BY  basic_DOT_dimension_DOT_users.country""",
            ),
        },
        """basic.transform.country_agg""": {
            None: (
                True,
                """SELECT  basic.comments.country,
    COUNT(DISTINCT basic.comments.id) AS num_users
 FROM basic.comments

 GROUP BY  basic.comments.country""",
            ),
            1: (
                True,
                """SELECT  basic.comments.country,
    COUNT(DISTINCT basic.comments.id) AS num_users
 FROM basic.comments

 GROUP BY  basic.comments.country""",
            ),
            2: (
                True,
                """SELECT  comments.country,
    COUNT(DISTINCT comments.id) AS num_users
 FROM comments

 GROUP BY  comments.country""",
            ),
        },
        """basic.num_comments""": {
            None: (
                True,
                """SELECT  COUNT(1) AS cnt
 FROM basic.comments""",
            ),
            1: (
                True,
                """SELECT  COUNT(1) AS cnt
 FROM basic.comments""",
            ),
            2: (
                True,
                """SELECT  COUNT(1) AS cnt
 FROM comments""",
            ),
        },
        """basic.num_users""": {
            None: (
                True,
                """SELECT  SUM(basic_DOT_transform_DOT_country_agg.num_users)
 FROM (SELECT  basic.comments.country,
    COUNT(DISTINCT basic.comments.id) AS num_users
 FROM basic.comments

 GROUP BY  basic.comments.country) AS basic_DOT_transform_DOT_country_agg""",
            ),
            1: (
                True,
                """SELECT  SUM(basic_DOT_transform_DOT_country_agg.num_users)
 FROM (SELECT  basic.comments.country,
    COUNT(DISTINCT basic.comments.id) AS num_users
 FROM basic.comments

 GROUP BY  basic.comments.country) AS basic_DOT_transform_DOT_country_agg""",
            ),
            2: (
                True,
                """SELECT  SUM(basic_DOT_transform_DOT_country_agg.num_users)
 FROM (SELECT  comments.country,
    COUNT(DISTINCT comments.id) AS num_users
 FROM comments

 GROUP BY  comments.country) AS basic_DOT_transform_DOT_country_agg""",
            ),
        },
        """dbt.transform.customer_agg""": {
            None: (
                True,
                """SELECT  c.id,
    c.first_name,
    c.last_name,
    COUNT(1) AS order_cnt
 FROM jaffle_shop.orders AS o
INNER JOIN jaffle_shop.customers AS c
        ON o.user_id = c.id
 GROUP BY  c.id, c.first_name, c.last_name""",
            ),
            1: (
                True,
                """SELECT  c.id,
    c.first_name,
    c.last_name,
    COUNT(1) AS order_cnt
 FROM jaffle_shop.orders AS o
INNER JOIN jaffle_shop.customers AS c
        ON o.user_id = c.id
 GROUP BY  c.id, c.first_name, c.last_name""",
            ),
            2: (False, """The requested database with id 2 cannot run this query."""),
        },
    }


@pytest.fixture
def construction_session(  # pylint: disable=too-many-locals
    session: Session,
) -> Session:
    """
    Add some source nodes and transform nodes to facilitate testing of extracting dependencies
    """

    postgres = Database(name="postgres", URI="", cost=10, id=1)

    gsheets = Database(name="gsheets", URI="", cost=100, id=2)

    countries_dim_ref = Node(
        name="basic.dimension.countries",
        type=NodeType.DIMENSION,
        current_version="1",
    )
    countries_dim = NodeRevision(
        name=countries_dim_ref.name,
        type=countries_dim_ref.type,
        node=countries_dim_ref,
        version="1",
        query="""
          SELECT country,
                 COUNT(1) AS user_cnt
          FROM basic.dimension.users
          GROUP BY country
        """,
        columns=[
            Column(name="country", type=ColumnType.STR),
            Column(name="user_cnt", type=ColumnType.INT),
        ],
    )

    user_dim_ref = Node(
        name="basic.dimension.users",
        type=NodeType.DIMENSION,
        current_version="1",
    )
    user_dim = NodeRevision(
        name=user_dim_ref.name,
        type=user_dim_ref.type,
        node=user_dim_ref,
        version="1",
        query="""
          SELECT id,
                 full_name,
                 age,
                 country,
                 gender,
                 preferred_language,
                 secret_number
          FROM basic.source.users
        """,
        columns=[
            Column(name="id", type=ColumnType.INT),
            Column(name="full_name", type=ColumnType.STR),
            Column(name="age", type=ColumnType.INT),
            Column(name="country", type=ColumnType.STR),
            Column(name="gender", type=ColumnType.STR),
            Column(name="preferred_language", type=ColumnType.STR),
            Column(name="secret_number", type=ColumnType.FLOAT),
        ],
    )

    country_agg_tfm_ref = Node(
        name="basic.transform.country_agg",
        type=NodeType.TRANSFORM,
        current_version="1",
    )
    country_agg_tfm = NodeRevision(
        name=country_agg_tfm_ref.name,
        type=country_agg_tfm_ref.type,
        node=country_agg_tfm_ref,
        version="1",
        query="""
        SELECT country,
                COUNT(DISTINCT id) AS num_users
        FROM basic.source.users
        GROUP BY country
        """,
        columns=[
            Column(name="country", type=ColumnType.STR),
            Column(name="num_users", type=ColumnType.INT),
        ],
    )

    users_src_ref = Node(
        name="basic.source.users",
        type=NodeType.SOURCE,
        current_version="1",
    )
    users_src = NodeRevision(
        name=users_src_ref.name,
        type=users_src_ref.type,
        node=users_src_ref,
        version="1",
        columns=[
            Column(name="id", type=ColumnType.INT),
            Column(name="full_name", type=ColumnType.STR),
            Column(name="names_map", type=ColumnType.MAP["str", "str"]),
            Column(
                name="user_metadata",
                type=ColumnType.MAP[
                    "str",
                    ColumnType.MAP["str", ColumnType.MAP["str", "float"]],
                ],
            ),
            Column(name="age", type=ColumnType.INT),
            Column(name="country", type=ColumnType.STR),
            Column(name="gender", type=ColumnType.STR),
            Column(name="preferred_language", type=ColumnType.STR),
            Column(name="secret_number", type=ColumnType.FLOAT),
        ],
        tables=[
            Table(
                node_id=4249,
                schema="basic",
                table="comments",
                columns=[
                    Column(name="id", type=ColumnType.INT),
                    Column(name="full_name", type=ColumnType.STR),
                    Column(name="names_map", type=ColumnType.MAP["str", "str"]),
                    Column(
                        name="user_metadata",
                        type=ColumnType.MAP[
                            "str",
                            ColumnType.MAP["str", ColumnType.MAP["str", "float"]],
                        ],
                    ),
                    Column(name="age", type=ColumnType.INT),
                    Column(name="country", type=ColumnType.STR),
                    Column(name="gender", type=ColumnType.STR),
                    Column(name="preferred_language", type=ColumnType.STR),
                    Column(name="secret_number", type=ColumnType.FLOAT),
                ],
                cost=10.0,
                database=postgres,
                database_id=1,
            ),
            Table(
                node_id=4250,
                table="comments",
                columns=[
                    Column(name="id", type=ColumnType.INT),
                    Column(name="full_name", type=ColumnType.STR),
                    Column(name="names_map", type=ColumnType.MAP["str", "str"]),
                    Column(
                        name="user_metadata",
                        type=ColumnType.MAP[
                            "str",
                            ColumnType.MAP["str", ColumnType.MAP["str", "float"]],
                        ],
                    ),
                    Column(name="age", type=ColumnType.INT),
                    Column(name="country", type=ColumnType.STR),
                    Column(name="gender", type=ColumnType.STR),
                    Column(name="preferred_language", type=ColumnType.STR),
                    Column(name="secret_number", type=ColumnType.FLOAT),
                ],
                cost=100.0,
                database=gsheets,
                database_id=2,
            ),
        ],
    )

    comments_src_ref = Node(
        name="basic.source.comments",
        type=NodeType.SOURCE,
        current_version="1",
    )
    comments_src = NodeRevision(
        name=comments_src_ref.name,
        type=comments_src_ref.type,
        node=comments_src_ref,
        version="1",
        columns=[
            Column(name="id", type=ColumnType.INT),
            Column(
                name="user_id",
                type=ColumnType.INT,
                dimension=user_dim_ref,
            ),
            Column(name="timestamp", type=ColumnType.TIMESTAMP),
            Column(name="text", type=ColumnType.STR),
        ],
        tables=[
            Table(
                node_id=4251,
                schema="basic",
                table="comments",
                columns=[
                    Column(name="id", type=ColumnType.INT),
                    Column(name="user_id", type=ColumnType.INT),
                    Column(name="timestamp", type=ColumnType.TIMESTAMP),
                    Column(name="text", type=ColumnType.STR),
                ],
                cost=10.0,
                database=postgres,
                database_id=1,
            ),
            Table(
                node_id=4252,
                table="comments",
                columns=[
                    Column(name="id", type=ColumnType.INT),
                    Column(name="user_id", type=ColumnType.INT),
                    Column(name="timestamp", type=ColumnType.TIMESTAMP),
                    Column(name="text", type=ColumnType.STR),
                ],
                cost=100.0,
                database=gsheets,
                database_id=2,
            ),
        ],
    )

    num_comments_mtc_ref = Node(
        name="basic.num_comments",
        type=NodeType.METRIC,
        current_version="1",
    )
    num_comments_mtc = NodeRevision(
        name=num_comments_mtc_ref.name,
        type=num_comments_mtc_ref.type,
        node=num_comments_mtc_ref,
        version="1",
        query="""
        SELECT COUNT(1) AS cnt
        FROM basic.source.comments
        """,
        columns=[
            Column(name="cnt", type=ColumnType.INT),
        ],
    )

    num_users_mtc_ref = Node(
        name="basic.num_users",
        type=NodeType.METRIC,
        current_version="1",
    )
    num_users_mtc = NodeRevision(
        name=num_users_mtc_ref.name,
        type=num_users_mtc_ref.type,
        node=num_users_mtc_ref,
        version="1",
        query="""
        SELECT SUM(num_users)
        FROM basic.transform.country_agg
        """,
        columns=[
            Column(name="_col0", type=ColumnType.INT),
        ],
    )

    customers_dim_ref = Node(
        name="dbt.dimension.customers",
        type=NodeType.DIMENSION,
        current_version="1",
    )
    customers_dim = NodeRevision(
        name=customers_dim_ref.name,
        type=customers_dim_ref.type,
        node=customers_dim_ref,
        version="1",
        query="""
          SELECT id,
             first_name,
             last_name
          FROM dbt.source.jaffle_shop.customers
        """,
        columns=[
            Column(name="id", type=ColumnType.INT),
            Column(name="first_name", type=ColumnType.STR),
            Column(name="last_name", type=ColumnType.STR),
        ],
    )

    customers_agg_tfm_ref = Node(
        name="dbt.transform.customer_agg",
        type=NodeType.TRANSFORM,
        current_version="1",
    )
    customers_agg_tfm = NodeRevision(
        name=customers_agg_tfm_ref.name,
        type=customers_agg_tfm_ref.type,
        node=customers_agg_tfm_ref,
        version="1",
        query="""
          SELECT c.id,
                 c.first_name,
                 c.last_name,
                 COUNT(1) AS order_cnt
          FROM dbt.source.jaffle_shop.orders o
          JOIN dbt.source.jaffle_shop.customers c ON o.user_id = c.id
          GROUP BY c.id,
                   c.first_name,
                   c.last_name
        """,
        columns=[
            Column(name="id", type=ColumnType.INT),
            Column(name="first_name", type=ColumnType.STR),
            Column(name="last_name", type=ColumnType.STR),
            Column(name="order_cnt", type=ColumnType.INT),
        ],
    )

    orders_src_ref = Node(
        name="dbt.source.jaffle_shop.orders",
        type=NodeType.SOURCE,
        current_version="1",
    )
    orders_src = NodeRevision(
        name=orders_src_ref.name,
        type=orders_src_ref.type,
        node=orders_src_ref,
        version="1",
        columns=[
            Column(name="id", type=ColumnType.INT),
            Column(
                name="user_id",
                type=ColumnType.INT,
                dimension=customers_dim_ref,
                dimension_column="event_id",
            ),
            Column(name="order_date", type=ColumnType.DATE),
            Column(name="status", type=ColumnType.STR),
            Column(name="_etl_loaded_at", type=ColumnType.TIMESTAMP),
        ],
        tables=[
            Table(
                node_id=4253,
                schema="jaffle_shop",
                table="orders",
                columns=[
                    Column(name="id", type=ColumnType.INT),
                    Column(name="user_id", type=ColumnType.INT),
                    Column(name="order_date", type=ColumnType.DATE),
                    Column(name="status", type=ColumnType.STR),
                    Column(name="_etl_loaded_at", type=ColumnType.TIMESTAMP),
                ],
                cost=10.0,
                database=postgres,
                database_id=1,
            ),
        ],
    )

    customers_src_ref = Node(
        name="dbt.source.jaffle_shop.customers",
        type=NodeType.SOURCE,
        current_version="1",
    )
    customers_src = NodeRevision(
        name=customers_src_ref.name,
        type=customers_src_ref.type,
        node=customers_src_ref,
        version="1",
        columns=[
            Column(name="id", type=ColumnType.INT),
            Column(name="first_name", type=ColumnType.STR),
            Column(name="last_name", type=ColumnType.STR),
        ],
        tables=[
            Table(
                node_id=4254,
                schema="jaffle_shop",
                table="customers",
                columns=[
                    Column(name="id", type=ColumnType.INT),
                    Column(name="first_name", type=ColumnType.STR),
                    Column(name="last_name", type=ColumnType.STR),
                ],
                cost=10.0,
                database=postgres,
                database_id=1,
            ),
        ],
    )

    session.add(postgres)
    session.add(gsheets)
    session.add(countries_dim)
    session.add(user_dim)
    session.add(country_agg_tfm)
    session.add(users_src)
    session.add(comments_src)
    session.add(num_comments_mtc)
    session.add(num_users_mtc)
    session.add(customers_dim)
    session.add(customers_agg_tfm)
    session.add(orders_src)
    session.add(customers_src)

    session.commit()
    return session
