"""
Tests for the metrics API.
"""


from tests.sql.utils import compare_query_strings


def test_query_validate_errors(mocker, client) -> None:
    """
    Test errors on ``GET /query/validate``.
    """
    mocker.patch("dj.models.database.Database.do_ping", return_value=True)

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

    response = client.get("/query/validate", json={"sql": query.format("")})
    assert response.status_code == 500

    assert "The name of the table in a Metric query must be `metrics`." in response.text

    response = client.get("/query/validate", json={"sql": query.format(", oops_table")})
    assert response.status_code == 500

    assert (
        "Any SELECT referencing a Metric must source "
        "from a single unaliased Table named `metrics`."
    ) in response.text


def test_query_validate(mocker, client) -> None:
    """
    Test ``GET /query/validate``.
    """
    mocker.patch("dj.models.database.Database.do_ping", return_value=True)

    query = """
    SELECT Avg(n_comments),
        age
    FROM   (SELECT basic.num_comments            AS n_comments,
                basic.dimension.users.country country,
                basic.dimension.users.age     AS age
            FROM   metrics
            GROUP  BY basic.dimension.users.country, basic.dimension.users.age)
    GROUP  BY age
    """
    expected = """
SELECT  Avg(n_comments) AS col0,
        age
 FROM (SELECT  basic_DOT_num_comments.cnt AS n_comments,
        basic_DOT_dimension_DOT_users.country AS country,
        basic_DOT_dimension_DOT_users.age AS age
 FROM (SELECT  COUNT(1) AS cnt,
        basic.comments.id AS basic_DOT_source_DOT_comments_DOT_id,
        basic.comments.user_id AS basic_DOT_source_DOT_comments_DOT_user_id,
        basic.comments.timestamp AS basic_DOT_source_DOT_comments_DOT_timestamp,
        basic.comments.text AS basic_DOT_source_DOT_comments_DOT_text
 FROM basic.comments

) AS basic_DOT_num_comments
LEFT JOIN basic.comments
        ON basic_DOT_num_comments.basic_DOT_source_DOT_comments_DOT_id = basic.comments.id AND basic_DOT_num_comments.basic_DOT_source_DOT_comments_DOT_user_id = basic.comments.user_id AND basic_DOT_num_comments.basic_DOT_source_DOT_comments_DOT_timestamp = basic.comments.timestamp AND basic_DOT_num_comments.basic_DOT_source_DOT_comments_DOT_text = basic.comments.text
LEFT JOIN (SELECT  basic.comments.id,
        basic.comments.full_name,
        basic.comments.age,
        basic.comments.country,
        basic.comments.gender,
        basic.comments.preferred_language,
        basic.comments.secret_number
 FROM basic.comments

) AS basic_DOT_dimension_DOT_users
        ON basic.comments.user_id = basic_DOT_dimension_DOT_users.id AND basic.comments.user_id = basic_DOT_dimension_DOT_users.id
 GROUP BY  basic_DOT_dimension_DOT_users.country, basic_DOT_dimension_DOT_users.age)

 GROUP BY  age
    """

    response = client.get("/query/validate", json={"sql": query})
    assert compare_query_strings(response.json()["sql"], expected)
