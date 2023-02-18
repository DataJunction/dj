"""
Tests for building dj metric queries
"""

from typing import Dict, Optional, Tuple

import pytest
from sqlalchemy import select
from sqlmodel import Session

from dj.construction.build import amenable_name, build_node_for_database
from dj.construction.dj_query import build_dj_metric_query
from dj.errors import DJException
from dj.models import Column, Database, NodeRevision, Table
from dj.models.node import Node, NodeType
from dj.typing import ColumnType

from ..sql.utils import compare_query_strings
from .fixtures import BUILD_EXPECTATION_PARAMETERS


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
SELECT basic_DOT_num_users_us.sum_users
FROM   (SELECT basic.comments.country,
               Count(DISTINCT basic.comments.id) AS num_users
        FROM   basic.comments
        GROUP  BY basic.comments.country) AS basic_DOT_transform_DOT_country_agg
       ,
       basic.comments,
       (SELECT Sum(basic_DOT_transform_DOT_country_agg.num_users) AS sum_users,
               basic_DOT_transform_DOT_country_agg.country,
               basic_DOT_transform_DOT_country_agg.num_users,
               b.id,
               b.full_name,
               b.age,
               b.country,
               b.gender,
               b.preferred_language,
               b.secret_number
        FROM   (SELECT basic.comments.country,
                       Count(DISTINCT basic.comments.id) AS num_users
                FROM   basic.comments
                GROUP  BY basic.comments.country) AS
               basic_DOT_transform_DOT_country_agg
               INNER JOIN basic.comments AS b
                       ON basic_DOT_transform_DOT_country_agg.country =
                          b.country
        WHERE  basic_DOT_transform_DOT_country_agg.country = 'US') AS
       basic_DOT_num_users_us
       LEFT JOIN (SELECT basic.comments.id,
                         basic.comments.full_name,
                         basic.comments.age,
                         basic.comments.country,
                         basic.comments.gender,
                         basic.comments.preferred_language,
                         basic.comments.secret_number
                  FROM   basic.comments) AS basic_DOT_dimension_DOT_users
              ON basic_DOT_transform_DOT_country_agg.country =
                           basic_DOT_dimension_DOT_users.country
                 AND basic_DOT_transform_DOT_country_agg.country =
                     basic_DOT_dimension_DOT_users.country
GROUP  BY basic_DOT_dimension_DOT_users.country 
    """
    query_ast, db = await build_dj_metric_query(construction_session, query)
    assert db.id==1
    assert compare_query_strings(expected, str(query_ast))