# pylint: disable=too-many-lines
"""
Dimension linking related tests.

TODO: convert to module scope later, for now these tests are pretty fast, only ~20 sec.
"""

import pytest
import pytest_asyncio
from httpx import AsyncClient
from requests import Response

from datajunction_server.sql.parsing.backends.antlr4 import parse
from tests.conftest import post_and_raise_if_error
from tests.examples import COMPLEX_DIMENSION_LINK, SERVICE_SETUP


@pytest_asyncio.fixture
async def dimensions_link_client(client: AsyncClient) -> AsyncClient:
    """
    Add dimension link examples to the roads test client.
    """
    for endpoint, json in SERVICE_SETUP + COMPLEX_DIMENSION_LINK:
        await post_and_raise_if_error(  # type: ignore
            client=client,
            endpoint=endpoint,
            json=json,  # type: ignore
        )
    return client


@pytest.mark.asyncio
async def test_link_dimension_with_errors(
    dimensions_link_client: AsyncClient,  # pylint: disable=redefined-outer-name
):
    """
    Test linking dimensions with errors
    """
    response = await dimensions_link_client.post(
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
    response = await dimensions_link_client.post(
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

    response = await dimensions_link_client.post(
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
    dimensions_link_client: AsyncClient,  # pylint: disable=redefined-outer-name
):
    """
    Link events with the users dimension without a role
    """

    async def _link_events_to_users_without_role() -> Response:
        response = await dimensions_link_client.post(
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
    dimensions_link_client: AsyncClient,  # pylint: disable=redefined-outer-name
):
    """
    Link events with the users dimension with the role "user_direct",
    indicating a direct mapping between the user's snapshot date with the
    event's start date
    """

    async def _link_events_to_users_with_role_direct() -> Response:
        response = await dimensions_link_client.post(
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
    dimensions_link_client: AsyncClient,  # pylint: disable=redefined-outer-name
):
    """
    Link events with the users dimension with the role "user_windowed",
    indicating windowed join between events and the user dimension
    """

    async def _link_events_to_users_with_role_windowed() -> Response:
        response = await dimensions_link_client.post(
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
    dimensions_link_client: AsyncClient,  # pylint: disable=redefined-outer-name
):
    """
    Link users to the countries dimension with role "registration_country".
    """

    async def _link_users_to_countries_with_role_registration() -> Response:
        response = await dimensions_link_client.post(
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


@pytest.fixture
def reference_link_events_user_registration_country(
    dimensions_link_client: AsyncClient,  # pylint: disable=redefined-outer-name
):
    """
    Link users to the countries dimension with role "registration_country".
    """

    async def _reference_link_events_user_registration_country() -> Response:
        response = await dimensions_link_client.post(
            "/nodes/default.events/columns/user_registration_country/link",
            params={
                "dimension_node": "default.users",
                "dimension_column": "registration_country",
            },
        )
        assert response.status_code == 201
        return response

    return _reference_link_events_user_registration_country


@pytest.mark.asyncio
async def test_link_complex_dimension_without_role(
    dimensions_link_client: AsyncClient,  # pylint: disable=redefined-outer-name,
    link_events_to_users_without_role,  # pylint: disable=redefined-outer-name
):
    """
    Test linking complex dimension without role
    """
    response = await link_events_to_users_without_role()
    assert response.json() == {
        "message": "Dimension node default.users has been successfully "
        "linked to node default.events.",
    }

    response = await dimensions_link_client.get("/nodes/default.events")
    assert response.json()["dimension_links"] == [
        {
            "dimension": {"name": "default.users"},
            "join_cardinality": "one_to_one",
            "join_sql": "default.events.user_id = default.users.user_id "
            "AND default.events.event_start_date = default.users.snapshot_date",
            "join_type": "left",
            "foreign_keys": {
                "default.events.event_start_date": "default.users.snapshot_date",
                "default.events.user_id": "default.users.user_id",
            },
            "role": None,
        },
    ]

    # Update dimension link
    response = await dimensions_link_client.post(
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

    response = await dimensions_link_client.get("/nodes/default.events")
    assert response.json()["dimension_links"][0]["foreign_keys"] == {
        "default.events.event_end_date": "default.users.snapshot_date",
        "default.events.user_id": "default.users.user_id",
    }

    response = await dimensions_link_client.get("/history?node=default.events")
    assert [
        (entry["activity_type"], entry["details"])
        for entry in response.json()
        if entry["entity_type"] == "link"
    ] == [
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
    ]

    # Switch back to original join definition
    await link_events_to_users_without_role()

    response = await dimensions_link_client.get(
        "/sql/default.events?dimensions=default.users.user_id"
        "&dimensions=default.users.snapshot_date"
        "&dimensions=default.users.registration_country",
    )
    query = response.json()["sql"]
    expected = """WITH default_DOT_events AS (
      SELECT
        default_DOT_events_table.user_id,
        default_DOT_events_table.event_start_date,
        default_DOT_events_table.event_end_date,
        default_DOT_events_table.elapsed_secs,
        default_DOT_events_table.user_registration_country
      FROM examples.events AS default_DOT_events_table
    ),
    default_DOT_users AS (
      SELECT
        default_DOT_users_table.user_id,
        default_DOT_users_table.snapshot_date,
        default_DOT_users_table.registration_country,
        default_DOT_users_table.residence_country,
        default_DOT_users_table.account_type
      FROM examples.users AS default_DOT_users_table
    )
    SELECT
        default_DOT_events.user_id default_DOT_users_DOT_user_id,
        default_DOT_events.event_start_date default_DOT_users_DOT_snapshot_date,
        default_DOT_events.event_end_date default_DOT_events_DOT_event_end_date,
        default_DOT_events.elapsed_secs default_DOT_events_DOT_elapsed_secs,
        default_DOT_events.user_registration_country default_DOT_events_DOT_user_registration_country,
        default_DOT_users.registration_country default_DOT_users_DOT_registration_country
    FROM default_DOT_events
    LEFT JOIN default_DOT_users
      ON default_DOT_events.user_id = default_DOT_users.user_id
        AND default_DOT_events.event_start_date = default_DOT_users.snapshot_date
    """
    assert str(parse(query)) == str(parse(expected))

    response = await dimensions_link_client.get("/nodes/default.events/dimensions")
    assert [(attr["name"], attr["path"]) for attr in response.json()] == [
        ("default.users.account_type", ["default.events"]),
        ("default.users.registration_country", ["default.events"]),
        ("default.users.residence_country", ["default.events"]),
        ("default.users.snapshot_date", ["default.events"]),
        ("default.users.user_id", ["default.events"]),
    ]


@pytest.mark.asyncio
async def test_link_complex_dimension_with_role(
    dimensions_link_client: AsyncClient,  # pylint: disable=redefined-outer-name
    link_events_to_users_with_role_direct,  # pylint: disable=redefined-outer-name
    link_events_to_users_with_role_windowed,  # pylint: disable=redefined-outer-name
    link_users_to_countries_with_role_registration,  # pylint: disable=redefined-outer-name
):
    """
    Testing linking complex dimension with roles.
    """
    response = await link_events_to_users_with_role_direct()
    assert response.json() == {
        "message": "Dimension node default.users has been successfully "
        "linked to node default.events.",
    }

    response = await dimensions_link_client.get("/nodes/default.events")
    assert response.json()["dimension_links"] == [
        {
            "dimension": {"name": "default.users"},
            "join_cardinality": "one_to_one",
            "join_sql": "default.events.user_id = default.users.user_id "
            "AND default.events.event_start_date = default.users.snapshot_date",
            "join_type": "left",
            "role": "user_direct",
            "foreign_keys": {
                "default.events.event_start_date": "default.users.snapshot_date",
                "default.events.user_id": "default.users.user_id",
            },
        },
    ]

    # Add a dimension link with different role
    response = await link_events_to_users_with_role_windowed()
    assert response.json() == {
        "message": "Dimension node default.users has been successfully linked to node "
        "default.events.",
    }

    # Add a dimension link on users for registration country
    response = await link_users_to_countries_with_role_registration()
    assert response.json() == {
        "message": "Dimension node default.countries has been successfully linked to node "
        "default.users.",
    }

    response = await dimensions_link_client.get("/nodes/default.events")
    assert sorted(response.json()["dimension_links"], key=lambda x: x["role"]) == [
        {
            "dimension": {"name": "default.users"},
            "join_cardinality": "one_to_one",
            "join_sql": "default.events.user_id = default.users.user_id AND "
            "default.events.event_start_date = default.users.snapshot_date",
            "join_type": "left",
            "role": "user_direct",
            "foreign_keys": {
                "default.events.event_start_date": "default.users.snapshot_date",
                "default.events.user_id": "default.users.user_id",
            },
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
            "foreign_keys": {"default.events.user_id": "default.users.user_id"},
        },
    ]

    # Verify that the dimensions on the downstream metric have roles specified
    response = await dimensions_link_client.get(
        "/nodes/default.elapsed_secs/dimensions",
    )
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
    response = await dimensions_link_client.get(
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
    expected = """WITH default_DOT_events AS (
  SELECT
    default_DOT_events_table.user_id,
    default_DOT_events_table.event_start_date,
    default_DOT_events_table.event_end_date,
    default_DOT_events_table.elapsed_secs,
    default_DOT_events_table.user_registration_country
  FROM examples.events AS default_DOT_events_table
), default_DOT_users AS (
  SELECT
    default_DOT_users_table.user_id,
    default_DOT_users_table.snapshot_date,
    default_DOT_users_table.registration_country,
    default_DOT_users_table.residence_country,
    default_DOT_users_table.account_type
  FROM examples.users AS default_DOT_users_table
),
default_DOT_events_metrics AS (
  SELECT
    default_DOT_users.user_id default_DOT_users_DOT_user_id_LBRACK_user_windowed_RBRACK,
    default_DOT_users.snapshot_date default_DOT_users_DOT_snapshot_date_LBRACK_user_windowed_RBRACK,
    default_DOT_users.registration_country default_DOT_users_DOT_registration_country_LBRACK_user_windowed_RBRACK,
    SUM(default_DOT_events.elapsed_secs) default_DOT_elapsed_secs
  FROM default_DOT_events
  LEFT JOIN default_DOT_users
    ON default_DOT_events.user_id = default_DOT_users.user_id
    AND default_DOT_events.event_start_date = default_DOT_users.snapshot_date
  GROUP BY
    default_DOT_users.user_id,
    default_DOT_users.snapshot_date,
    default_DOT_users.registration_country
)
SELECT
  default_DOT_events_metrics.default_DOT_users_DOT_user_id_LBRACK_user_windowed_RBRACK,
  default_DOT_events_metrics.default_DOT_users_DOT_snapshot_date_LBRACK_user_windowed_RBRACK,
  default_DOT_events_metrics.default_DOT_users_DOT_registration_country_LBRACK_user_windowed_RBRACK,
  default_DOT_events_metrics.default_DOT_elapsed_secs
FROM default_DOT_events_metrics
"""
    assert str(parse(query)) == str(parse(expected))

    # Get SQL for the downstream metric grouped by the user dimension of role "user"
    response = await dimensions_link_client.get(
        "/sql/default.elapsed_secs?dimensions=default.users.user_id[user_direct]"
        "&dimensions=default.users.snapshot_date[user_direct]"
        "&dimensions=default.users.registration_country[user_direct]",
    )
    query = response.json()["sql"]
    expected = """WITH default_DOT_events AS (
      SELECT
        default_DOT_events_table.user_id,
        default_DOT_events_table.event_start_date,
        default_DOT_events_table.event_end_date,
        default_DOT_events_table.elapsed_secs,
        default_DOT_events_table.user_registration_country
      FROM examples.events AS default_DOT_events_table
    ), default_DOT_users AS (
      SELECT
        default_DOT_users_table.user_id,
        default_DOT_users_table.snapshot_date,
        default_DOT_users_table.registration_country,
        default_DOT_users_table.residence_country,
        default_DOT_users_table.account_type
      FROM examples.users AS default_DOT_users_table
    ),
    default_DOT_events_metrics AS (
      SELECT
        default_DOT_users.user_id default_DOT_users_DOT_user_id_LBRACK_user_direct_RBRACK,
        default_DOT_users.snapshot_date default_DOT_users_DOT_snapshot_date_LBRACK_user_direct_RBRACK,
        default_DOT_users.registration_country default_DOT_users_DOT_registration_country_LBRACK_user_direct_RBRACK,
        SUM(default_DOT_events.elapsed_secs) default_DOT_elapsed_secs
      FROM default_DOT_events
      LEFT JOIN default_DOT_users
        ON default_DOT_events.user_id = default_DOT_users.user_id
        AND default_DOT_events.event_start_date = default_DOT_users.snapshot_date
      GROUP BY
        default_DOT_users.user_id,
        default_DOT_users.snapshot_date,
        default_DOT_users.registration_country
    )
    SELECT
      default_DOT_events_metrics.default_DOT_users_DOT_user_id_LBRACK_user_direct_RBRACK,
      default_DOT_events_metrics.default_DOT_users_DOT_snapshot_date_LBRACK_user_direct_RBRACK,
      default_DOT_events_metrics.default_DOT_users_DOT_registration_country_LBRACK_user_direct_RBRACK,
      default_DOT_events_metrics.default_DOT_elapsed_secs
    FROM default_DOT_events_metrics"""
    assert str(parse(query)) == str(parse(expected))

    # Get SQL for the downstream metric grouped by the user's registration country and
    # filtered by the user's residence country
    # TODO  # pylint: disable=fixme
    response = await dimensions_link_client.get(
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
    expected = """WITH default_DOT_events AS (
  SELECT
    default_DOT_events_table.user_id,
    default_DOT_events_table.event_start_date,
    default_DOT_events_table.event_end_date,
    default_DOT_events_table.elapsed_secs,
    default_DOT_events_table.user_registration_country
  FROM examples.events AS default_DOT_events_table
), default_DOT_users AS (
  SELECT
    default_DOT_users_table.user_id,
    default_DOT_users_table.snapshot_date,
    default_DOT_users_table.registration_country,
    default_DOT_users_table.residence_country,
    default_DOT_users_table.account_type
  FROM examples.users AS default_DOT_users_table
), default_DOT_countries AS (
  SELECT
    default_DOT_countries_table.country_code,
    default_DOT_countries_table.name,
    default_DOT_countries_table.population
  FROM examples.countries AS default_DOT_countries_table
),
default_DOT_events_metrics AS (
  SELECT
    default_DOT_users.snapshot_date default_DOT_users_DOT_snapshot_date_LBRACK_user_direct_RBRACK,
    default_DOT_users.registration_country default_DOT_users_DOT_registration_country_LBRACK_user_direct_RBRACK,
    SUM(default_DOT_events.elapsed_secs) default_DOT_elapsed_secs
  FROM default_DOT_events
  LEFT JOIN default_DOT_users
    ON default_DOT_events.user_id = default_DOT_users.user_id
    AND default_DOT_events.event_start_date = default_DOT_users.snapshot_date
  INNER JOIN default_DOT_countries
    ON default_DOT_users.registration_country = default_DOT_countries.country_code
  GROUP BY
    default_DOT_users.snapshot_date,
    default_DOT_users.registration_country
)
SELECT
  default_DOT_events_metrics.default_DOT_users_DOT_snapshot_date_LBRACK_user_direct_RBRACK,
  default_DOT_events_metrics.default_DOT_users_DOT_registration_country_LBRACK_user_direct_RBRACK,
  default_DOT_events_metrics.default_DOT_elapsed_secs
FROM default_DOT_events_metrics
"""
    assert str(parse(query)) == str(parse(expected))


@pytest.mark.asyncio
async def test_remove_dimension_link(
    dimensions_link_client: AsyncClient,  # pylint: disable=redefined-outer-name
    link_events_to_users_with_role_direct,  # pylint: disable=redefined-outer-name
    link_events_to_users_without_role,  # pylint: disable=redefined-outer-name
):
    """
    Test removing complex dimension links
    """
    response = await link_events_to_users_with_role_direct()
    assert response.json() == {
        "message": "Dimension node default.users has been successfully linked to node "
        "default.events.",
    }
    response = await dimensions_link_client.get("/nodes/default.events")
    # assert response.json()["dimension_links"] == []
    response = await dimensions_link_client.request(
        "DELETE",
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

    response = await dimensions_link_client.get("/nodes/default.events")
    assert response.json()["dimension_links"] == []
    # Deleting again should not work
    response = await dimensions_link_client.request(
        "DELETE",
        "/nodes/default.events/link",
        json={
            "dimension_node": "default.users",
            "role": "user_direct",
        },
    )
    assert response.json() == {
        "message": "Dimension link to node default.users with role user_direct not found",
    }

    await link_events_to_users_without_role()
    response = await dimensions_link_client.request(
        "DELETE",
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
    response = await dimensions_link_client.request(
        "DELETE",
        "/nodes/default.events/link",
        json={
            "dimension_node": "default.users",
        },
    )
    assert response.json() == {
        "message": "Dimension link to node default.users not found",
    }


@pytest.mark.asyncio
async def test_measures_sql_with_dimension_roles(
    dimensions_link_client: AsyncClient,  # pylint: disable=redefined-outer-name
    link_events_to_users_with_role_direct,  # pylint: disable=redefined-outer-name
    link_events_to_users_with_role_windowed,  # pylint: disable=redefined-outer-name
    link_users_to_countries_with_role_registration,  # pylint: disable=redefined-outer-name
):
    """
    Test measures SQL with dimension roles
    """
    await link_events_to_users_with_role_direct()
    await link_events_to_users_with_role_windowed()
    await link_users_to_countries_with_role_registration()
    sql_params = {
        "metrics": ["default.elapsed_secs"],
        "dimensions": [
            "default.countries.name[user_direct->registration_country]",
            "default.users.snapshot_date[user_direct]",
            "default.users.registration_country[user_direct]",
        ],
        "filters": ["default.countries.name[user_direct->registration_country] = 'UG'"],
    }
    response = await dimensions_link_client.get("/sql/measures/v2", params=sql_params)
    query = response.json()[0]["sql"]
    expected = """WITH default_DOT_events AS (
  SELECT
    default_DOT_events_table.user_id,
    default_DOT_events_table.event_start_date,
    default_DOT_events_table.event_end_date,
    default_DOT_events_table.elapsed_secs,
    default_DOT_events_table.user_registration_country
  FROM examples.events AS default_DOT_events_table
), default_DOT_users AS (
  SELECT
    default_DOT_users_table.user_id,
    default_DOT_users_table.snapshot_date,
    default_DOT_users_table.registration_country,
    default_DOT_users_table.residence_country,
    default_DOT_users_table.account_type
  FROM examples.users AS default_DOT_users_table
), default_DOT_countries AS (
  SELECT
    default_DOT_countries_table.country_code,
    default_DOT_countries_table.name,
    default_DOT_countries_table.population
  FROM examples.countries AS default_DOT_countries_table
)
SELECT
  default_DOT_events.elapsed_secs default_DOT_events_DOT_elapsed_secs,
  default_DOT_countries.name default_DOT_countries_DOT_name
FROM default_DOT_events
LEFT JOIN default_DOT_users
  ON default_DOT_events.user_id = default_DOT_users.user_id
  AND default_DOT_events.event_start_date = default_DOT_users.snapshot_date
INNER JOIN default_DOT_countries
  ON default_DOT_users.registration_country = default_DOT_countries.country_code"""
    assert str(parse(query)) == str(parse(expected))
    # TODO Implement caching for v2 measures SQL endpoint  # pylint: disable=fixme
    # query_request = (
    #     (
    #         await session.execute(
    #             select(QueryRequest).where(
    #                 QueryRequest.query_type == QueryBuildType.MEASURES,
    #             ),
    #         )
    #     )
    #     .scalars()
    #     .all()
    # )
    # assert len(query_request) == 1
    # assert query_request[0].nodes == ["default.elapsed_secs@v1.0"]
    # assert query_request[0].parents == [
    #     "default.events@v1.0",
    #     "default.events_table@v1.0",
    # ]
    # assert query_request[0].dimensions == [
    #     "default.countries.name[user_direct->registration_country]@v1.0",
    #     "default.users.snapshot_date[user_direct]@v1.0",
    #     "default.users.registration_country[user_direct]@v1.0",
    # ]
    # assert query_request[0].filters == [
    #     "default.countries.name[user_direct -> registration_country]@v1.0 = 'UG'",
    # ]


@pytest.mark.asyncio
async def test_reference_dimension_links_errors(
    dimensions_link_client: AsyncClient,  # pylint: disable=redefined-outer-name
    reference_link_events_user_registration_country,  # pylint: disable=redefined-outer-name
):
    """
    Test various reference dimension link errors
    """
    # Not a dimension node being linked
    dimensions_link_client.post("/nodes/{}")
    response = await dimensions_link_client.post(
        "/nodes/default.events/columns/user_registration_country/link",
        params={
            "dimension_node": "default.users_table",
            "dimension_column": "user_id",
        },
    )
    assert response.status_code == 422
    assert response.json()["message"] == "Node default.events is not of type dimension!"

    # Wrong type to create a reference dimension link
    response = await dimensions_link_client.post(
        "/nodes/default.events/columns/user_registration_country/link",
        params={
            "dimension_node": "default.users",
            "dimension_column": "snapshot_date",
        },
    )
    assert response.status_code == 422
    assert response.json()["message"] == (
        "The column user_registration_country has type string and is being linked to"
        " the dimension default.users via the dimension column snapshot_date, which "
        "has type int. These column types are incompatible and the dimension cannot "
        "be linked"
    )

    # Delete reference link twice
    await reference_link_events_user_registration_country()
    response = await dimensions_link_client.delete(
        "/nodes/default.events/columns/user_registration_country/link",
    )
    assert response.status_code == 200
    assert response.json()["message"] == (
        "The reference dimension link on default.events.user_registration_country"
        " has been removed."
    )
    response = await dimensions_link_client.delete(
        "/nodes/default.events/columns/user_registration_country/link",
    )
    assert response.status_code == 200
    assert response.json()["message"] == (
        "There is no reference dimension link on default.events.user_registration_country."
    )


@pytest.mark.asyncio
async def test_measures_sql_with_reference_dimension_links(
    dimensions_link_client: AsyncClient,  # pylint: disable=redefined-outer-name
    reference_link_events_user_registration_country,  # pylint: disable=redefined-outer-name
    link_events_to_users_without_role,  # pylint: disable=redefined-outer-name
):
    """
    Test measures SQL generation with reference dimension links
    """
    await reference_link_events_user_registration_country()

    response = await dimensions_link_client.get(
        "/nodes/default.elapsed_secs/dimensions",
    )
    dimensions_data = response.json()
    assert dimensions_data == [
        {
            "filter_only": False,
            "is_primary_key": False,
            "name": "default.users.registration_country",
            "node_display_name": "Default: Users",
            "node_name": "default.users",
            "path": [
                "default.events.user_registration_country",
            ],
            "type": "string",
        },
    ]

    await link_events_to_users_without_role()
    response = await dimensions_link_client.get(
        "/nodes/default.elapsed_secs/dimensions",
    )
    dimensions_data = response.json()
    assert [dim["name"] for dim in dimensions_data] == [
        "default.users.account_type",
        "default.users.registration_country",
        "default.users.registration_country",
        "default.users.residence_country",
        "default.users.snapshot_date",
        "default.users.user_id",
    ]

    sql_params = {
        "metrics": ["default.elapsed_secs"],
        "dimensions": [
            "default.users.registration_country",
        ],
    }

    response = await dimensions_link_client.get("/sql/measures/v2", params=sql_params)
    response_data = response.json()
    expected_sql = """WITH
default_DOT_events AS (
  SELECT
    default_DOT_events_table.user_id,
    default_DOT_events_table.event_start_date,
    default_DOT_events_table.event_end_date,
    default_DOT_events_table.elapsed_secs,
    default_DOT_events_table.user_registration_country
  FROM examples.events AS default_DOT_events_table
)
SELECT
  default_DOT_events.elapsed_secs default_DOT_events_DOT_elapsed_secs,
  default_DOT_events.user_registration_country default_DOT_users_DOT_registration_country
FROM default_DOT_events"""
    assert str(parse(response_data[0]["sql"])) == str(parse(expected_sql))
    assert response_data[0]["errors"] == []
    assert response_data[0]["columns"] == [
        {
            "name": "default_DOT_events_DOT_elapsed_secs",
            "type": "int",
            "column": "elapsed_secs",
            "node": "default.events",
            "semantic_entity": "default.events.elapsed_secs",
            "semantic_type": "measure",
        },
        {
            "name": "default_DOT_users_DOT_registration_country",
            "type": "string",
            "column": "registration_country",
            "node": "default.users",
            "semantic_entity": "default.users.registration_country",
            "semantic_type": "dimension",
        },
    ]
    response = await dimensions_link_client.delete(
        "/nodes/default.events/columns/user_registration_country/link",
    )
    assert response.status_code == 200
    response = await dimensions_link_client.get("/sql/measures/v2", params=sql_params)
    response_data = response.json()
    expected_sql = """
    WITH default_DOT_events AS (
      SELECT
        default_DOT_events_table.user_id,
        default_DOT_events_table.event_start_date,
        default_DOT_events_table.event_end_date,
        default_DOT_events_table.elapsed_secs,
        default_DOT_events_table.user_registration_country
      FROM examples.events AS default_DOT_events_table
    ),
    default_DOT_users AS (
      SELECT
        default_DOT_users_table.user_id,
        default_DOT_users_table.snapshot_date,
        default_DOT_users_table.registration_country,
        default_DOT_users_table.residence_country,
        default_DOT_users_table.account_type
      FROM examples.users AS default_DOT_users_table
    )
    SELECT
      default_DOT_events.elapsed_secs default_DOT_events_DOT_elapsed_secs,
      default_DOT_users.registration_country default_DOT_users_DOT_registration_country
    FROM default_DOT_events
    LEFT JOIN default_DOT_users
      ON default_DOT_events.user_id = default_DOT_users.user_id
      AND default_DOT_events.event_start_date = default_DOT_users.snapshot_date
    """
    assert str(parse(response_data[0]["sql"])) == str(parse(expected_sql))
    assert response_data[0]["errors"] == []


@pytest.mark.asyncio
async def test_dimension_link_cross_join(
    dimensions_link_client: AsyncClient,  # pylint: disable=redefined-outer-name
):
    """
    Testing linking complex dimension with CROSS JOIN as the join type.
    """
    response = await dimensions_link_client.post(
        "/nodes/dimension",
        json={
            "description": "Areas",
            "query": """
            SELECT tab.area, 1 AS area_rep FROM VALUES ('A'), ('B'), ('C') AS tab(area)
            """,
            "mode": "published",
            "name": "default.areas",
            "primary_key": ["area"],
        },
    )
    assert response.status_code == 201
    response = await dimensions_link_client.post(
        "/nodes/default.events/link",
        json={
            "dimension_node": "default.areas",
            "join_type": "cross",
            "join_on": "",
            "join_cardinality": "many_to_one",
        },
    )
    assert response.status_code == 201
    response = await dimensions_link_client.get("/nodes/default.events/dimensions")
    assert [dim["name"] for dim in response.json()] == [
        "default.areas.area",
        "default.areas.area_rep",
    ]

    response = await dimensions_link_client.get(
        "/sql/default.events?dimensions=default.areas.area&dimensions=default.areas.area_rep",
    )
    expected = """WITH
    default_DOT_events AS (
      SELECT
        default_DOT_events_table.user_id,
        default_DOT_events_table.event_start_date,
        default_DOT_events_table.event_end_date,
        default_DOT_events_table.elapsed_secs,
        default_DOT_events_table.user_registration_country
      FROM examples.events AS default_DOT_events_table
    ),
    default_DOT_areas AS (
      SELECT
        tab.area,
      1 AS area_rep
      FROM VALUES ('A'),
      ('B'),
      ('C') AS tab(area)
    )
    SELECT
      default_DOT_events.user_id default_DOT_events_DOT_user_id,
      default_DOT_events.event_start_date default_DOT_events_DOT_event_start_date,
      default_DOT_events.event_end_date default_DOT_events_DOT_event_end_date,
      default_DOT_events.elapsed_secs default_DOT_events_DOT_elapsed_secs,
      default_DOT_events.user_registration_country default_DOT_events_DOT_user_registration_country,
      default_DOT_areas.area default_DOT_areas_DOT_area,
      default_DOT_areas.area_rep default_DOT_areas_DOT_area_rep
    FROM default_DOT_events CROSS JOIN default_DOT_areas
    """
    assert str(parse(response.json()["sql"])) == str(parse(expected))
