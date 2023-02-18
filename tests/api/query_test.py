"""
Tests for the metrics API.
"""

from uuid import UUID

from fastapi.testclient import TestClient
from freezegun import freeze_time
from pytest_mock import MockerFixture
from sqlmodel import Session

from dj.models.column import Column
from dj.models.node import Node, NodeRevision, NodeType
from dj.models.query import Database, QueryWithResults
from dj.models.table import Table
from dj.sql.parsing.backends.sqloxide import parse
from dj.typing import ColumnType
from tests.sql.utils import TPCDS_QUERY_SET, compare_query_strings, read_query

def test_query_validate_errors(mocker, request, client) -> None:
    """
    Test errors on ``GET /query/validate``.
    """
    mocker.patch("dj.models.database.Database.do_ping", return_value=True)
    session: Session = request.getfixturevalue("construction_session")


    query = """
    SELECT Avg(n_comments),
        age
    FROM   (SELECT basic.num_comments            AS n_comments,
                basic.dimension.users.country country,
                basic.dimension.users.age     AS age
            FROM   not_metrics{}
            GROUP  BY basic.dimension.users.country, basic.dimension.users.country)
    GROUP  BY age 
    """

    response = client.get("/query/validate",
                json = {
                    "sql": query.format("")
                }
    )
    assert response.status_code == 500

    assert  "The name of the table in a Metric query must be `metrics`." in response.text

    response = client.get("/query/validate",
                json = {
                    "sql": query.format(", oops_table")
                }
    )
    assert response.status_code == 500

    assert ("Any SELECT referencing a Metric must source "
           "from a single unaliased Table named `metrics`.") in response.text

def test_query_validate(mocker, request, client) -> None:
    """
    Test ``GET /query/validate``.
    """
    mocker.patch("dj.models.database.Database.do_ping", return_value=True)
    session: Session = request.getfixturevalue("construction_session")


    query = """
    SELECT Avg(n_comments),
        age
    FROM   (SELECT basic.num_comments            AS n_comments,
                basic.dimension.users.country country,
                basic.dimension.users.age     AS age
            FROM   metrics
            GROUP  BY basic.dimension.users.country, basic.dimension.users.country)
    GROUP  BY age 
    """
    expected = """
SELECT Avg(n_comments) AS col0,
       age
FROM   (SELECT basic_DOT_num_comments.cnt            AS n_comments,
               basic_DOT_dimension_DOT_users.country AS country,
               basic_DOT_dimension_DOT_users.age     AS age
        FROM   basic.comments,
               (SELECT COUNT(1) AS cnt,
                       basic.comments.id,
                       basic.comments.user_id,
                       basic.comments.timestamp,
                       basic.comments.text
                FROM   basic.comments) AS basic_DOT_num_comments
               LEFT JOIN (SELECT basic.comments.id,
                                 basic.comments.full_name,
                                 basic.comments.age,
                                 basic.comments.country,
                                 basic.comments.gender,
                                 basic.comments.preferred_language,
                                 basic.comments.secret_number
                          FROM   basic.comments) AS
                         basic_DOT_dimension_DOT_users
                      ON basic.comments.user_id =
basic_DOT_dimension_DOT_users.id
                         AND basic.comments.user_id =
                             basic_DOT_dimension_DOT_users.id
        GROUP  BY basic_DOT_dimension_DOT_users.country,
                  basic_DOT_dimension_DOT_users.country)
GROUP  BY age 
    """


    response = client.get("/query/validate",
                json = {
                    "sql": query
                }
    )
    assert compare_query_strings(response.json()['sql'], expected)