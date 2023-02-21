"""
Tests for building dj metric queries
"""


import pytest
from sqlmodel import Session

from dj.construction.dj_query import build_dj_metric_query

from ..sql.utils import compare_query_strings


@pytest.mark.asyncio
async def test_build_dj_metric_query(mocker, request):
    """
    Test building a metric query
    """
    mocker.patch("dj.models.database.Database.do_ping", return_value=True)
    construction_session: Session = request.getfixturevalue("construction_session")
    query = """
    SELECT basic.num_users_us
    FROM metrics
    GROUP BY basic.dimension.users.country
    """
    expected = """
SELECT  basic_DOT_num_users_us.sum_users
 FROM (SELECT  SUM(basic_DOT_transform_DOT_country_agg.num_users) AS sum_users,
        basic_DOT_transform_DOT_country_agg.country AS a_DOT_country,
        basic_DOT_transform_DOT_country_agg.num_users AS a_DOT_num_users,
        b.id AS b_DOT_id,
        b.full_name AS b_DOT_full_name,
        b.names_map AS b_DOT_names_map,
        b.user_metadata AS b_DOT_user_metadata,
        b.age AS b_DOT_age,
        b.country AS b_DOT_country,
        b.gender AS b_DOT_gender,
        b.preferred_language AS b_DOT_preferred_language,
        b.secret_number AS b_DOT_secret_number
 FROM (SELECT  basic.comments.country,
        COUNT(DISTINCT basic.comments.id) AS num_users
 FROM basic.comments

 GROUP BY  basic.comments.country) AS basic_DOT_transform_DOT_country_agg
INNER JOIN basic.comments AS b
        ON basic_DOT_transform_DOT_country_agg.country = b.country
 WHERE  basic_DOT_transform_DOT_country_agg.country = 'US'
) AS basic_DOT_num_users_us
LEFT JOIN (SELECT  basic.comments.country,
        COUNT(DISTINCT basic.comments.id) AS num_users
 FROM basic.comments

 GROUP BY  basic.comments.country) AS basic_DOT_transform_DOT_country_agg
        ON basic_DOT_num_users_us.a_DOT_country = basic_DOT_transform_DOT_country_agg.country AND basic_DOT_num_users_us.a_DOT_num_users = basic_DOT_transform_DOT_country_agg.num_users
LEFT JOIN basic.comments AS b
        ON basic_DOT_num_users_us.b_DOT_id = b.id AND basic_DOT_num_users_us.b_DOT_full_name = b.full_name AND basic_DOT_num_users_us.b_DOT_names_map = b.names_map AND basic_DOT_num_users_us.b_DOT_user_metadata = b.user_metadata AND basic_DOT_num_users_us.b_DOT_age = b.age AND basic_DOT_num_users_us.b_DOT_country = b.country AND basic_DOT_num_users_us.b_DOT_gender = b.gender AND basic_DOT_num_users_us.b_DOT_preferred_language = b.preferred_language AND basic_DOT_num_users_us.b_DOT_secret_number = b.secret_number
LEFT JOIN (SELECT  basic.comments.id,
        basic.comments.full_name,
        basic.comments.age,
        basic.comments.country,
        basic.comments.gender,
        basic.comments.preferred_language,
        basic.comments.secret_number
 FROM basic.comments

) AS basic_DOT_dimension_DOT_users
        ON basic_DOT_transform_DOT_country_agg.country = basic_DOT_dimension_DOT_users.country AND basic_DOT_transform_DOT_country_agg.country = basic_DOT_dimension_DOT_users.country
 GROUP BY  basic_DOT_dimension_DOT_users.country
    """
    query_ast, database = await build_dj_metric_query(construction_session, query)
    assert database.id == 1
    assert compare_query_strings(expected, str(query_ast))
