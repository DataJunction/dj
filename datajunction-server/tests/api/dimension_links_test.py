"""Dimension linking related tests."""

import pytest
from requests import Response
from starlette.testclient import TestClient

from tests.conftest import post_and_raise_if_error
from tests.examples import COMPLEX_DIMENSION_LINK, SERVICE_SETUP
from tests.sql.utils import compare_query_strings


@pytest.fixture
def dimensions_link_client(client: TestClient) -> TestClient:
    """
    Add dimension link examples to the roads test client.
    """
    for endpoint, json in SERVICE_SETUP + COMPLEX_DIMENSION_LINK:
        post_and_raise_if_error(  # type: ignore
            client=client,
            endpoint=endpoint,
            json=json,  # type: ignore
        )
    return client


def test_link_dimension_with_errors(
    dimensions_link_client: TestClient,  # pylint: disable=redefined-outer-name
):
    """
    Test linking dimensions with errors
    """
    response = dimensions_link_client.post(
        "/nodes/default.elapsed_secs/link",
        json={
            "dimension_node": "default.users",
            "join_on": ("default.elapsed_secs.x = default.users.y"),
            "join_cardinality": "many_to_one",
        },
    )
    assert response.json()["message"] == (
        "Cannot link dimension to a node of type metric. Must be a source, "
        "dimension, or transform node."
    )
    response = dimensions_link_client.post(
        "/nodes/default.events/link",
        json={
            "dimension_node": "default.users",
            "join_on": ("default.users.user_id = default.users.user_id"),
            "join_cardinality": "many_to_one",
        },
    )
    assert response.json()["message"] == (
        "The join SQL provided does not reference both the origin node default.events "
        "and the dimension node default.users that it's being joined to."
    )

    response = dimensions_link_client.post(
        "/nodes/default.events/link",
        json={
            "dimension_node": "default.users",
            "join_on": ("default.events.order_year = default.users.year"),
            "join_cardinality": "many_to_one",
        },
    )
    assert (
        response.json()["message"]
        == "Join query default.events.order_year = default.users.year is not valid"
    )


@pytest.fixture
def link_events_to_users_without_role(
    dimensions_link_client: TestClient,  # pylint: disable=redefined-outer-name
):
    """
    Link events with the users dimension without a role
    """

    def _link_events_to_users_without_role() -> Response:
        response = dimensions_link_client.post(
            "/nodes/default.events/link",
            json={
                "dimension_node": "default.users",
                "join_type": "left",
                "join_on": (
                    "default.events.user_id = default.users.user_id "
                    "AND default.events.event_start_date = default.users.snapshot_date"
                ),
                "join_cardinality": "one_to_one",
            },
        )
        return response

    return _link_events_to_users_without_role


@pytest.fixture
def link_events_to_users_with_role_direct(
    dimensions_link_client: TestClient,  # pylint: disable=redefined-outer-name
):
    """
    Link events with the users dimension with the role "user_direct",
    indicating a direct mapping between the user's snapshot date with the
    event's start date
    """

    def _link_events_to_users_with_role_direct() -> Response:
        response = dimensions_link_client.post(
            "/nodes/default.events/link",
            json={
                "dimension_node": "default.users",
                "join_type": "left",
                "join_on": (
                    "default.events.user_id = default.users.user_id "
                    "AND default.events.event_start_date = default.users.snapshot_date"
                ),
                "join_cardinality": "one_to_one",
                "role": "user_direct",
            },
        )
        return response

    return _link_events_to_users_with_role_direct


@pytest.fixture
def link_events_to_users_with_role_windowed(
    dimensions_link_client: TestClient,  # pylint: disable=redefined-outer-name
):
    """
    Link events with the users dimension with the role "user_windowed",
    indicating windowed join between events and the user dimension
    """

    def _link_events_to_users_with_role_windowed() -> Response:
        response = dimensions_link_client.post(
            "/nodes/default.events/link",
            json={
                "dimension_node": "default.users",
                "join_type": "left",
                "join_on": "default.events.user_id = default.users.user_id "
                "AND default.events.event_start_date BETWEEN default.users.snapshot_date "
                "AND CAST(DATE_ADD(CAST(default.users.snapshot_date AS DATE), 10) AS INT)",
                "join_cardinality": "one_to_many",
                "role": "user_windowed",
            },
        )
        return response

    return _link_events_to_users_with_role_windowed


@pytest.fixture
def link_users_to_countries_with_role_registration(
    dimensions_link_client: TestClient,  # pylint: disable=redefined-outer-name
):
    """
    Link users to the countries dimension with role "registration_country".
    """

    def _link_users_to_countries_with_role_registration() -> Response:
        response = dimensions_link_client.post(
            "/nodes/default.users/link",
            json={
                "dimension_node": "default.countries",
                "join_type": "inner",
                "join_on": "default.users.registration_country = default.countries.country_code ",
                "join_cardinality": "one_to_one",
                "role": "registration_country",
            },
        )
        return response

    return _link_users_to_countries_with_role_registration


def test_link_complex_dimension_without_role(
    dimensions_link_client: TestClient,  # pylint: disable=redefined-outer-name,
    link_events_to_users_without_role,  # pylint: disable=redefined-outer-name
):
    """
    Test linking complex dimension without role
    """
    response = link_events_to_users_without_role()
    assert response.json() == {
        "message": "Dimension node default.users has been successfully "
        "linked to node default.events.",
    }

    response = dimensions_link_client.get("/nodes/default.events")
    assert response.json()["dimension_links"] == [
        {
            "dimension": {"name": "default.users"},
            "join_cardinality": "one_to_one",
            "join_sql": "default.events.user_id = default.users.user_id "
            "AND default.events.event_start_date = default.users.snapshot_date",
            "join_type": "left",
            "role": None,
        },
    ]

    # Update dimension link
    response = dimensions_link_client.post(
        "/nodes/default.events/link",
        json={
            "dimension_node": "default.users",
            "join_type": "left",
            "join_on": (
                "default.events.user_id = default.users.user_id "
                "AND default.events.event_end_date = default.users.snapshot_date"
            ),
            "join_cardinality": "one_to_many",
        },
    )
    assert response.json() == {
        "message": "The dimension link between default.events and "
        "default.users has been successfully updated.",
    }

    response = dimensions_link_client.get("/history?node=default.events")
    assert [
        (entry["activity_type"], entry["details"])
        for entry in response.json()
        if entry["entity_type"] == "link"
    ] == [
        (
            "create",
            {
                "dimension": "default.users",
                "join_cardinality": "one_to_one",
                "join_sql": "default.events.user_id = default.users.user_id AND "
                "default.events.event_start_date = default.users.snapshot_date",
                "role": None,
            },
        ),
        (
            "update",
            {
                "dimension": "default.users",
                "join_cardinality": "one_to_many",
                "join_sql": "default.events.user_id = default.users.user_id AND "
                "default.events.event_end_date = default.users.snapshot_date",
                "role": None,
            },
        ),
    ]

    # Switch back to original join definition
    link_events_to_users_without_role()

    response = dimensions_link_client.get(
        "/sql/default.events?dimensions=default.users.user_id"
        "&dimensions=default.users.snapshot_date"
        "&dimensions=default.users.registration_country",
    )
    query = response.json()["sql"]
    assert compare_query_strings(
        query,
        # pylint: disable=line-too-long
        """SELECT default_DOT_users.user_id default_DOT_events_DOT_user_id,
  default_DOT_events.event_start_date default_DOT_events_DOT_event_start_date,
  default_DOT_events.event_end_date default_DOT_events_DOT_event_end_date,
  default_DOT_events.elapsed_secs default_DOT_events_DOT_elapsed_secs,
  default_DOT_users.snapshot_date default_DOT_users_DOT_snapshot_date,
  default_DOT_users.registration_country default_DOT_users_DOT_registration_country
FROM (
    SELECT default_DOT_events_table.user_id,
      default_DOT_events_table.event_start_date,
      default_DOT_events_table.event_end_date,
      default_DOT_events_table.elapsed_secs
    FROM examples.events AS default_DOT_events_table
  ) AS default_DOT_events
  LEFT JOIN (
    SELECT default_DOT_users.user_id,
      default_DOT_users.snapshot_date,
      default_DOT_users.registration_country,
      default_DOT_users.residence_country,
      default_DOT_users.account_type
    FROM examples.users AS default_DOT_users
  ) default_DOT_users ON default_DOT_events.user_id = default_DOT_users.user_id
  AND default_DOT_events.event_start_date = default_DOT_users.snapshot_date""",
    )

    response = dimensions_link_client.get("/nodes/default.events/dimensions")
    assert [(attr["name"], attr["path"]) for attr in response.json()] == [
        ("default.users.account_type", ["default.events."]),
        ("default.users.registration_country", ["default.events."]),
        ("default.users.residence_country", ["default.events."]),
        ("default.users.snapshot_date", ["default.events."]),
        ("default.users.user_id", ["default.events."]),
    ]


def test_link_complex_dimension_with_role(
    dimensions_link_client: TestClient,  # pylint: disable=redefined-outer-name
    link_events_to_users_with_role_direct,  # pylint: disable=redefined-outer-name
    link_events_to_users_with_role_windowed,  # pylint: disable=redefined-outer-name
    link_users_to_countries_with_role_registration,  # pylint: disable=redefined-outer-name
):
    """
    Testing linking complex dimension with roles.
    """
    response = link_events_to_users_with_role_direct()
    assert response.json() == {
        "message": "Dimension node default.users has been successfully "
        "linked to node default.events.",
    }

    response = dimensions_link_client.get("/nodes/default.events")
    assert response.json()["dimension_links"] == [
        {
            "dimension": {"name": "default.users"},
            "join_cardinality": "one_to_one",
            "join_sql": "default.events.user_id = default.users.user_id "
            "AND default.events.event_start_date = default.users.snapshot_date",
            "join_type": "left",
            "role": "user_direct",
        },
    ]

    # Add a dimension link with different role
    response = link_events_to_users_with_role_windowed()
    assert response.json() == {
        "message": "Dimension node default.users has been successfully linked to node "
        "default.events.",
    }

    # Add a dimension link on users for registration country
    response = link_users_to_countries_with_role_registration()
    assert response.json() == {
        "message": "Dimension node default.countries has been successfully linked to node "
        "default.users.",
    }

    response = dimensions_link_client.get("/nodes/default.events")
    assert response.json()["dimension_links"] == [
        {
            "dimension": {"name": "default.users"},
            "join_cardinality": "one_to_one",
            "join_sql": "default.events.user_id = default.users.user_id AND "
            "default.events.event_start_date = default.users.snapshot_date",
            "join_type": "left",
            "role": "user_direct",
        },
        {
            "dimension": {"name": "default.users"},
            "join_cardinality": "one_to_many",
            "join_sql": "default.events.user_id = default.users.user_id AND "
            "default.events.event_start_date BETWEEN "
            "default.users.snapshot_date AND "
            "CAST(DATE_ADD(CAST(default.users.snapshot_date AS DATE), 10) AS "
            "INT)",
            "join_type": "left",
            "role": "user_windowed",
        },
    ]

    # Verify that the dimensions on the downstream metric have roles specified
    response = dimensions_link_client.get("/nodes/default.elapsed_secs/dimensions")
    assert [(attr["name"], attr["path"]) for attr in response.json()] == [
        (
            "default.countries.country_code[user_direct->registration_country]",
            ["default.events.user_direct", "default.users.registration_country"],
        ),
        (
            "default.countries.country_code[user_windowed->registration_country]",
            ["default.events.user_windowed", "default.users.registration_country"],
        ),
        (
            "default.countries.name[user_direct->registration_country]",
            ["default.events.user_direct", "default.users.registration_country"],
        ),
        (
            "default.countries.name[user_windowed->registration_country]",
            ["default.events.user_windowed", "default.users.registration_country"],
        ),
        (
            "default.countries.population[user_direct->registration_country]",
            ["default.events.user_direct", "default.users.registration_country"],
        ),
        (
            "default.countries.population[user_windowed->registration_country]",
            ["default.events.user_windowed", "default.users.registration_country"],
        ),
        ("default.users.account_type[user_direct]", ["default.events.user_direct"]),
        ("default.users.account_type[user_windowed]", ["default.events.user_windowed"]),
        (
            "default.users.registration_country[user_direct]",
            ["default.events.user_direct"],
        ),
        (
            "default.users.registration_country[user_windowed]",
            ["default.events.user_windowed"],
        ),
        (
            "default.users.residence_country[user_direct]",
            ["default.events.user_direct"],
        ),
        (
            "default.users.residence_country[user_windowed]",
            ["default.events.user_windowed"],
        ),
        ("default.users.snapshot_date[user_direct]", ["default.events.user_direct"]),
        (
            "default.users.snapshot_date[user_windowed]",
            ["default.events.user_windowed"],
        ),
        ("default.users.user_id[user_direct]", ["default.events.user_direct"]),
        ("default.users.user_id[user_windowed]", ["default.events.user_windowed"]),
    ]

    # Get SQL for the downstream metric grouped by the user dimension of role "user_windowed"
    response = dimensions_link_client.get(
        "/sql/default.elapsed_secs",
        params={
            "dimensions": [
                "default.users.user_id[user_windowed]",
                "default.users.snapshot_date[user_windowed]",
                "default.users.registration_country[user_windowed]",
            ],
            "filters": ["default.users.registration_country[user_windowed] = 'NZ'"],
        },
    )
    query = response.json()["sql"]
    assert compare_query_strings(
        query,
        # pylint: disable=line-too-long
        """SELECT SUM(default_DOT_events.elapsed_secs) default_DOT_elapsed_secs,
  default_DOT_users.user_id default_DOT_users_DOT_user_id,
  default_DOT_users.snapshot_date default_DOT_users_DOT_snapshot_date,
  default_DOT_users.registration_country default_DOT_users_DOT_registration_country
FROM (
    SELECT default_DOT_events_table.user_id,
      default_DOT_events_table.event_start_date,
      default_DOT_events_table.event_end_date,
      default_DOT_events_table.elapsed_secs
    FROM examples.events AS default_DOT_events_table
  ) AS default_DOT_events
  LEFT JOIN (
    SELECT default_DOT_users.user_id,
      default_DOT_users.snapshot_date,
      default_DOT_users.registration_country,
      default_DOT_users.residence_country,
      default_DOT_users.account_type
    FROM examples.users AS default_DOT_users
  ) default_DOT_users ON default_DOT_events.user_id = default_DOT_users.user_id
  AND default_DOT_events.event_start_date BETWEEN default_DOT_users.snapshot_date AND CAST(
    DATE_ADD(
      CAST(default_DOT_users.snapshot_date AS DATE),
      10
    ) AS INT
  )
WHERE default_DOT_users.registration_country = 'NZ'
GROUP BY default_DOT_users.user_id,
  default_DOT_users.snapshot_date,
  default_DOT_users.registration_country""",
    )

    # Get SQL for the downstream metric grouped by the user dimension of role "user"
    response = dimensions_link_client.get(
        "/sql/default.elapsed_secs?dimensions=default.users.user_id[user_direct]"
        "&dimensions=default.users.snapshot_date[user_direct]"
        "&dimensions=default.users.registration_country[user_direct]",
    )
    query = response.json()["sql"]
    assert compare_query_strings(
        query,
        # pylint: disable=line-too-long
        """SELECT SUM(default_DOT_events.elapsed_secs) default_DOT_elapsed_secs,
  default_DOT_users.user_id default_DOT_users_DOT_user_id,
  default_DOT_users.snapshot_date default_DOT_users_DOT_snapshot_date,
  default_DOT_users.registration_country default_DOT_users_DOT_registration_country
FROM (
    SELECT default_DOT_events_table.user_id,
      default_DOT_events_table.event_start_date,
      default_DOT_events_table.event_end_date,
      default_DOT_events_table.elapsed_secs
    FROM examples.events AS default_DOT_events_table
  ) AS default_DOT_events
  LEFT JOIN (
    SELECT default_DOT_users.user_id,
      default_DOT_users.snapshot_date,
      default_DOT_users.registration_country,
      default_DOT_users.residence_country,
      default_DOT_users.account_type
    FROM examples.users AS default_DOT_users
  ) default_DOT_users ON default_DOT_events.user_id = default_DOT_users.user_id
  AND default_DOT_events.event_start_date = default_DOT_users.snapshot_date
GROUP BY default_DOT_users.user_id,
  default_DOT_users.snapshot_date,
  default_DOT_users.registration_country""",
    )

    # Get SQL for the downstream metric grouped by the user's registration country and
    # filtered by the user's residence country
    response = dimensions_link_client.get(
        "/sql/default.elapsed_secs?",
        params={
            "dimensions": [
                "default.countries.name[user_direct->registration_country]",
                "default.users.snapshot_date[user_direct]",
                "default.users.registration_country[user_direct]",
            ],
            "filters": [
                "default.countries.name[user_direct->registration_country] = 'NZ'",
            ],
        },
    )
    query = response.json()["sql"]
    assert compare_query_strings(
        query,
        # pylint: disable=line-too-long
        """SELECT  SUM(default_DOT_events.elapsed_secs) default_DOT_elapsed_secs,
    default_DOT_countries.name default_DOT_countries_DOT_name,
    default_DOT_users.snapshot_date default_DOT_users_DOT_snapshot_date,
    default_DOT_users.registration_country default_DOT_users_DOT_registration_country
 FROM (SELECT  default_DOT_events_table.user_id,
    default_DOT_events_table.event_start_date,
    default_DOT_events_table.event_end_date,
    default_DOT_events_table.elapsed_secs
 FROM examples.events AS default_DOT_events_table)
 AS default_DOT_events INNER  JOIN (SELECT  default_DOT_countries.country_code,
    default_DOT_countries.name,
    default_DOT_countries.population
 FROM examples.countries AS default_DOT_countries
) default_DOT_countries ON default.users.registration_country = default_DOT_countries.country_code
LEFT  JOIN (SELECT  default_DOT_users.user_id,
    default_DOT_users.snapshot_date,
    default_DOT_users.registration_country,
    default_DOT_users.residence_country,
    default_DOT_users.account_type
 FROM examples.users AS default_DOT_users
) default_DOT_users ON default_DOT_events.user_id = default_DOT_users.user_id AND default_DOT_events.event_start_date = default_DOT_users.snapshot_date
 WHERE  default_DOT_countries.name = 'NZ'
 GROUP BY  default_DOT_countries.name, default_DOT_users.snapshot_date, default_DOT_users.registration_country""",
    )


def test_remove_dimension_link(
    dimensions_link_client: TestClient,  # pylint: disable=redefined-outer-name
    link_events_to_users_with_role_direct,  # pylint: disable=redefined-outer-name
    link_events_to_users_without_role,  # pylint: disable=redefined-outer-name
):
    """
    Test removing complex dimension links
    """
    link_events_to_users_with_role_direct()
    response = dimensions_link_client.delete(
        "/nodes/default.events/link",
        json={
            "dimension_node": "default.users",
            "role": "user_direct",
        },
    )
    assert response.json() == {
        "message": "Dimension link default.users (role user_direct) to node "
        "default.events has been removed.",
    }

    # Deleting again should not work
    response = dimensions_link_client.delete(
        "/nodes/default.events/link",
        json={
            "dimension_node": "default.users",
            "role": "user_direct",
        },
    )
    assert response.json() == {
        "message": "Dimension link to node default.users with role user_direct not found",
    }

    link_events_to_users_without_role()
    response = dimensions_link_client.delete(
        "/nodes/default.events/link",
        json={
            "dimension_node": "default.users",
        },
    )
    assert response.json() == {
        "message": "Dimension link default.users to node "
        "default.events has been removed.",
    }

    # Deleting again should not work
    response = dimensions_link_client.delete(
        "/nodes/default.events/link",
        json={
            "dimension_node": "default.users",
        },
    )
    assert response.json() == {
        "message": "Dimension link to node default.users not found",
    }


def test_measures_sql_with_dimension_roles(
    dimensions_link_client: TestClient,  # pylint: disable=redefined-outer-name
    link_events_to_users_with_role_direct,  # pylint: disable=redefined-outer-name
    link_events_to_users_with_role_windowed,  # pylint: disable=redefined-outer-name
    link_users_to_countries_with_role_registration,  # pylint: disable=redefined-outer-name
):
    """
    Test measures SQL with dimension roles
    """
    link_events_to_users_with_role_direct()
    link_events_to_users_with_role_windowed()
    link_users_to_countries_with_role_registration()
    sql_params = {
        "metrics": ["default.elapsed_secs"],
        "dimensions": [
            "default.countries.name[user_direct->registration_country]",
            "default.users.snapshot_date[user_direct]",
            "default.users.registration_country[user_direct]",
        ],
        "filters": ["default.countries.name[user_direct->registration_country] = 'UG'"],
    }
    response = dimensions_link_client.get("/sql/measures", params=sql_params)
    query = response.json()["sql"]
    assert compare_query_strings(
        query,
        """WITH
default_DOT_events AS (SELECT  default_DOT_events.elapsed_secs default_DOT_events_DOT_elapsed_secs,
    default_DOT_countries.name default_DOT_countries_DOT_name,
    default_DOT_users.snapshot_date default_DOT_users_DOT_snapshot_date,
    default_DOT_users.registration_country default_DOT_users_DOT_registration_country
 FROM (SELECT  default_DOT_events_table.user_id,
    default_DOT_events_table.event_start_date,
    default_DOT_events_table.event_end_date,
    default_DOT_events_table.elapsed_secs
 FROM examples.events AS default_DOT_events_table)
 AS default_DOT_events INNER  JOIN (SELECT  default_DOT_countries.country_code,
    default_DOT_countries.name,
    default_DOT_countries.population
 FROM examples.countries AS default_DOT_countries
) default_DOT_countries ON default.users.registration_country = default_DOT_countries.country_code
LEFT  JOIN (SELECT  default_DOT_users.user_id,
    default_DOT_users.snapshot_date,
    default_DOT_users.registration_country,
    default_DOT_users.residence_country,
    default_DOT_users.account_type
 FROM examples.users AS default_DOT_users
) default_DOT_users ON default_DOT_events.user_id = default_DOT_users.user_id
AND default_DOT_events.event_start_date = default_DOT_users.snapshot_date
 WHERE  default_DOT_countries.name = 'UG'
)
SELECT  default_DOT_events.default_DOT_events_DOT_elapsed_secs,
    default_DOT_events.default_DOT_countries_DOT_name,
    default_DOT_events.default_DOT_users_DOT_snapshot_date,
    default_DOT_events.default_DOT_users_DOT_registration_country
 FROM default_DOT_events""",
    )
