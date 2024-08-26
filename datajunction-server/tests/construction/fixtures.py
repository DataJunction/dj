"""fixtures for testing construction"""
# noqa: W191,E101
# pylint: disable=line-too-long,too-many-statements

from typing import Dict, List, Optional, Tuple

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.attributetype import AttributeType, ColumnAttribute
from datajunction_server.database.column import Column
from datajunction_server.database.database import Database
from datajunction_server.database.dimensionlink import DimensionLink, JoinType
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.user import User
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.parsing.types import (
    DateType,
    FloatType,
    IntegerType,
    MapType,
    StringType,
    TimestampType,
)

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
    zip(BUILD_NODE_NAMES * 3, [None] * len(BUILD_NODE_NAMES)),
)


@pytest.fixture
def build_expectation() -> Dict[str, Dict[Optional[int], Tuple[bool, str]]]:
    """map node names with database ids to what their build results should be"""
    return {
        """basic.source.users""": {
            None: (
                True,
                """
                SELECT
                    id,
                    full_name,
                    names_map,
                    user_metadata,
                    age,
                    country,
                    gender,
                    preferred_language,
                    secret_number
                 FROM basic.source.users
                """,
            ),
        },
        """basic.source.comments""": {
            None: (
                True,
                """
                SELECT
                    id,
                    user_id,
                    timestamp,
                    text
                 FROM basic.source.comments
                """,
            ),
        },
        """basic.dimension.users""": {
            None: (
                True,
                """SELECT  basic_DOT_dimension_DOT_users.id,
    basic_DOT_dimension_DOT_users.full_name,
    basic_DOT_dimension_DOT_users.age,
    basic_DOT_dimension_DOT_users.country,
    basic_DOT_dimension_DOT_users.gender,
    basic_DOT_dimension_DOT_users.preferred_language,
    basic_DOT_dimension_DOT_users.secret_number
 FROM (SELECT  basic_DOT_source_DOT_users.id,
    basic_DOT_source_DOT_users.full_name,
    basic_DOT_source_DOT_users.age,
    basic_DOT_source_DOT_users.country,
    basic_DOT_source_DOT_users.gender,
    basic_DOT_source_DOT_users.preferred_language,
    basic_DOT_source_DOT_users.secret_number
 FROM basic.source.users AS basic_DOT_source_DOT_users)
 AS basic_DOT_dimension_DOT_users""",
            ),
        },
        """dbt.source.jaffle_shop.orders""": {
            None: (
                False,
                """Node has no query. Cannot generate a build plan without a query.""",
            ),
        },
        """dbt.dimension.customers""": {
            None: (
                True,
                """SELECT  dbt_DOT_dimension_DOT_customers.id,
    dbt_DOT_dimension_DOT_customers.first_name,
    dbt_DOT_dimension_DOT_customers.last_name
 FROM (SELECT  dbt_DOT_source_DOT_jaffle_shop_DOT_customers.id,
    dbt_DOT_source_DOT_jaffle_shop_DOT_customers.first_name,
    dbt_DOT_source_DOT_jaffle_shop_DOT_customers.last_name
 FROM dbt.source.jaffle_shop.customers AS dbt_DOT_source_DOT_jaffle_shop_DOT_customers)
 AS dbt_DOT_dimension_DOT_customers""",
            ),
        },
        """dbt.source.jaffle_shop.customers""": {
            None: (
                False,
                """Node has no query. Cannot generate a build plan without a query.""",
            ),
        },
        """basic.dimension.countries""": {
            None: (
                True,
                """SELECT  basic_DOT_dimension_DOT_countries.country,
    basic_DOT_dimension_DOT_countries.user_cnt
 FROM (SELECT  basic_DOT_dimension_DOT_users.country,
    COUNT(1) AS user_cnt
 FROM (SELECT  basic_DOT_source_DOT_users.id,
    basic_DOT_source_DOT_users.full_name,
    basic_DOT_source_DOT_users.age,
    basic_DOT_source_DOT_users.country,
    basic_DOT_source_DOT_users.gender,
    basic_DOT_source_DOT_users.preferred_language,
    basic_DOT_source_DOT_users.secret_number
 FROM basic.source.users AS basic_DOT_source_DOT_users)
 AS basic_DOT_dimension_DOT_users
 GROUP BY  basic_DOT_dimension_DOT_users.country)
 AS basic_DOT_dimension_DOT_countries""",
            ),
        },
        """basic.transform.country_agg""": {
            None: (
                True,
                """SELECT  basic_DOT_transform_DOT_country_agg.country,
    basic_DOT_transform_DOT_country_agg.num_users
 FROM (SELECT  basic_DOT_source_DOT_users.country,
    COUNT( DISTINCT basic_DOT_source_DOT_users.id) AS num_users
 FROM basic.source.users AS basic_DOT_source_DOT_users
 GROUP BY  basic_DOT_source_DOT_users.country)
 AS basic_DOT_transform_DOT_country_agg""",
            ),
        },
        """basic.num_comments""": {
            None: (
                True,
                """SELECT  COUNT(1) AS basic_DOT_num_comments
 FROM basic.source.comments AS basic_DOT_source_DOT_comments""",
            ),
        },
        """basic.num_users""": {
            None: (
                True,
                """SELECT  SUM(basic_DOT_transform_DOT_country_agg.num_users) AS basic_DOT_num_users
 FROM (SELECT  basic_DOT_source_DOT_users.country,
    COUNT(DISTINCT basic_DOT_source_DOT_users.id) AS num_users
 FROM basic.source.users AS basic_DOT_source_DOT_users

 GROUP BY  basic_DOT_source_DOT_users.country) AS basic_DOT_transform_DOT_country_agg""",
            ),
        },
        """dbt.transform.customer_agg""": {
            None: (
                True,
                """SELECT  dbt_DOT_transform_DOT_customer_agg.id,
    dbt_DOT_transform_DOT_customer_agg.first_name,
    dbt_DOT_transform_DOT_customer_agg.last_name,
    dbt_DOT_transform_DOT_customer_agg.order_cnt
 FROM (SELECT  c.id,
    c.first_name,
    c.last_name,
    COUNT(1) AS order_cnt
 FROM dbt.source.jaffle_shop.orders AS o JOIN dbt.source.jaffle_shop.customers AS c ON o.user_id = c.id
 GROUP BY  c.id, c.first_name, c.last_name)
 AS dbt_DOT_transform_DOT_customer_agg""",
            ),
        },
    }


@pytest_asyncio.fixture
async def construction_session(  # pylint: disable=too-many-locals
    session: AsyncSession,
    current_user: User,
) -> AsyncSession:
    """
    Add some source nodes and transform nodes to facilitate testing of extracting dependencies
    """

    postgres = Database(name="postgres", URI="", cost=10, id=1)

    gsheets = Database(name="gsheets", URI="", cost=100, id=2)
    primary_key = AttributeType(namespace="system", name="primary_key", description="")
    countries_dim_ref = Node(
        name="basic.dimension.countries",
        type=NodeType.DIMENSION,
        current_version="1",
        created_by_id=current_user.id,
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
            Column(
                name="country",
                order=0,
                type=StringType(),
                attributes=[ColumnAttribute(attribute_type=primary_key)],
            ),
            Column(name="user_cnt", type=IntegerType(), order=1),
        ],
        created_by_id=current_user.id,
    )

    user_dim_ref = Node(
        name="basic.dimension.users",
        type=NodeType.DIMENSION,
        current_version="1",
        created_by_id=current_user.id,
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
            Column(
                name="id",
                order=0,
                type=IntegerType(),
                attributes=[ColumnAttribute(attribute_type=primary_key)],
            ),
            Column(name="full_name", type=StringType(), order=1),
            Column(name="age", type=IntegerType(), order=2),
            Column(name="country", type=StringType(), order=3),
            Column(name="gender", type=StringType(), order=4),
            Column(name="preferred_language", type=StringType(), order=5),
            Column(name="secret_number", type=FloatType(), order=6),
        ],
        created_by_id=current_user.id,
    )

    country_agg_tfm_ref = Node(
        name="basic.transform.country_agg",
        type=NodeType.TRANSFORM,
        current_version="1",
        created_by_id=current_user.id,
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
            Column(
                name="country",
                type=StringType(),
                dimension=user_dim_ref,
                dimension_column="country",
                order=0,
            ),
            Column(name="num_users", type=IntegerType(), order=1),
        ],
        created_by_id=current_user.id,
    )

    users_src_ref = Node(
        name="basic.source.users",
        type=NodeType.SOURCE,
        current_version="1",
        created_by_id=current_user.id,
    )
    users_src = NodeRevision(
        name=users_src_ref.name,
        type=users_src_ref.type,
        node=users_src_ref,
        version="1",
        columns=[
            Column(name="id", type=IntegerType(), order=0),
            Column(name="full_name", type=StringType(), order=1),
            Column(
                name="names_map",
                type=MapType(key_type=StringType(), value_type=StringType()),
                order=2,
            ),
            Column(
                name="user_metadata",
                type=MapType(
                    key_type=StringType(),
                    value_type=MapType(
                        key_type=StringType(),
                        value_type=MapType(
                            key_type=StringType(),
                            value_type=FloatType(),
                        ),
                    ),
                ),
                order=3,
            ),
            Column(name="age", type=IntegerType(), order=4),
            Column(name="country", type=StringType(), order=5),
            Column(name="gender", type=StringType(), order=6),
            Column(name="preferred_language", type=StringType(), order=7),
            Column(name="secret_number", type=FloatType(), order=8),
        ],
        created_by_id=current_user.id,
    )

    comments_src_ref = Node(
        name="basic.source.comments",
        type=NodeType.SOURCE,
        current_version="1",
        created_by_id=current_user.id,
    )
    comments_src = NodeRevision(
        name=comments_src_ref.name,
        type=comments_src_ref.type,
        node=comments_src_ref,
        version="1",
        columns=[
            Column(name="id", type=IntegerType(), order=0),
            Column(
                name="user_id",
                type=IntegerType(),
                dimension=user_dim_ref,
                order=1,
            ),
            Column(name="timestamp", type=TimestampType(), order=2),
            Column(name="text", type=StringType(), order=3),
        ],
        created_by_id=current_user.id,
    )

    num_comments_mtc_ref = Node(
        name="basic.num_comments",
        type=NodeType.METRIC,
        current_version="1",
        created_by_id=current_user.id,
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
            Column(name="cnt", type=IntegerType(), order=0),
        ],
        parents=[comments_src_ref],
        created_by_id=current_user.id,
    )

    num_comments_mtc_bnd_dims_ref = Node(
        name="basic.num_comments_bnd",
        type=NodeType.METRIC,
        current_version="1",
        created_by_id=current_user.id,
    )
    num_comments_mtc_bnd_dims = NodeRevision(
        name=num_comments_mtc_bnd_dims_ref.name,
        type=num_comments_mtc_bnd_dims_ref.type,
        node=num_comments_mtc_bnd_dims_ref,
        version="1",
        query="""
        SELECT COUNT(1) AS cnt
        FROM basic.source.comments
        """,
        parents=[comments_src_ref],
        columns=[
            Column(name="cnt", type=IntegerType(), order=0),
        ],
        required_dimensions=[
            comments_src.columns[0],  # pylint: disable=E1136
            comments_src.columns[-1],  # pylint: disable=E1136
        ],
        created_by_id=current_user.id,
    )

    num_users_mtc_ref = Node(
        name="basic.num_users",
        type=NodeType.METRIC,
        current_version="1",
        created_by_id=current_user.id,
    )
    num_users_mtc = NodeRevision(
        name=num_users_mtc_ref.name,
        type=num_users_mtc_ref.type,
        node=num_users_mtc_ref,
        version="1",
        query="""
        SELECT SUM(num_users) AS col0
        FROM basic.transform.country_agg
        """,
        columns=[
            Column(name="col0", type=IntegerType(), order=0),
        ],
        parents=[country_agg_tfm_ref],
        created_by_id=current_user.id,
    )
    num_users_us_join_mtc_ref = Node(
        name="basic.num_users_us",
        type=NodeType.METRIC,
        current_version="1",
        created_by_id=current_user.id,
    )
    num_users_us_join_mtc = NodeRevision(
        name=num_users_us_join_mtc_ref.name,
        type=num_users_us_join_mtc_ref.type,
        node=num_users_us_join_mtc_ref,
        version="1",
        query="""
        SELECT SUM(a.num_users) as sum_users
        FROM basic.transform.country_agg a
        INNER JOIN basic.source.users b
        ON a.country=b.country
        WHERE a.country='US'
        """,
        columns=[
            Column(
                name="sum_users",
                type=IntegerType(),
                order=0,
            ),
        ],
        parents=[country_agg_tfm_ref, users_src_ref],
        created_by_id=current_user.id,
    )
    customers_dim_ref = Node(
        name="dbt.dimension.customers",
        type=NodeType.DIMENSION,
        current_version="1",
        created_by_id=current_user.id,
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
            Column(
                name="id",
                type=IntegerType(),
                attributes=[ColumnAttribute(attribute_type=primary_key)],
                order=0,
            ),
            Column(name="first_name", type=StringType(), order=1),
            Column(name="last_name", type=StringType(), order=2),
        ],
        created_by_id=current_user.id,
    )

    customers_agg_tfm_ref = Node(
        name="dbt.transform.customer_agg",
        type=NodeType.TRANSFORM,
        current_version="1",
        created_by_id=current_user.id,
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
            Column(name="id", type=IntegerType(), order=0),
            Column(name="first_name", type=StringType(), order=1),
            Column(name="last_name", type=StringType(), order=2),
            Column(name="order_cnt", type=IntegerType(), order=3),
        ],
        created_by_id=current_user.id,
    )

    orders_src_ref = Node(
        name="dbt.source.jaffle_shop.orders",
        type=NodeType.SOURCE,
        current_version="1",
        created_by_id=current_user.id,
    )
    orders_src = NodeRevision(
        name=orders_src_ref.name,
        type=orders_src_ref.type,
        node=orders_src_ref,
        version="1",
        columns=[
            Column(name="id", type=IntegerType(), order=0),
            Column(
                name="user_id",
                type=IntegerType(),
                dimension=customers_dim_ref,
                dimension_column="event_id",
                order=1,
            ),
            Column(name="order_date", type=DateType(), order=2),
            Column(name="status", type=StringType(), order=3),
            Column(name="_etl_loaded_at", type=TimestampType(), order=4),
        ],
        created_by_id=current_user.id,
    )

    customers_src_ref = Node(
        name="dbt.source.jaffle_shop.customers",
        type=NodeType.SOURCE,
        current_version="1",
        created_by_id=current_user.id,
    )
    customers_src = NodeRevision(
        name=customers_src_ref.name,
        type=customers_src_ref.type,
        node=customers_src_ref,
        version="1",
        columns=[
            Column(name="id", type=IntegerType(), order=0),
            Column(name="first_name", type=StringType(), order=1),
            Column(name="last_name", type=StringType(), order=2),
        ],
        created_by_id=current_user.id,
    )

    comments_users_link = DimensionLink(
        node_revision=comments_src,
        dimension=user_dim_ref,
        join_sql="basic.source.comments.user_id = basic.dimension.users.id",
        join_type=JoinType.INNER,
    )

    orders_customers_link = DimensionLink(
        node_revision=orders_src,
        dimension=customers_dim_ref,
        join_sql="dbt.source.jaffle_shop.orders.user_id = dbt.dimension.customers.id",
        join_type=JoinType.INNER,
    )

    session.add(postgres)
    session.add(gsheets)
    session.add(countries_dim)
    session.add(user_dim)
    session.add(country_agg_tfm)
    session.add(users_src)
    session.add(comments_src)
    session.add(num_users_us_join_mtc)
    session.add(num_comments_mtc)
    session.add(num_comments_mtc_bnd_dims)
    session.add(num_users_mtc)
    session.add(customers_dim)
    session.add(customers_agg_tfm)
    session.add(orders_src)
    session.add(customers_src)

    session.add(comments_users_link)
    session.add(orders_customers_link)
    await session.commit()
    await session.refresh(comments_users_link)
    await session.refresh(orders_customers_link)
    return session
