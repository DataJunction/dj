"""
Dimension linking related tests.

Each test gets its own isolated database with COMPLEX_DIMENSION_LINK data loaded fresh.
"""

import pytest
import pytest_asyncio
from httpx import AsyncClient
from requests import Response

from datajunction_server.sql.parsing.backends.antlr4 import parse
from tests.conftest import post_and_raise_if_error
from tests.construction.build_v3 import assert_sql_equal
from tests.examples import COMPLEX_DIMENSION_LINK, SERVICE_SETUP


@pytest_asyncio.fixture
async def dimensions_link_client(isolated_client: AsyncClient) -> AsyncClient:
    """
    Function-scoped fixture that provides a client with COMPLEX_DIMENSION_LINK data.

    Uses isolated_client for complete isolation - each test gets its own fresh
    database with the dimension link examples loaded.
    """
    for endpoint, json in SERVICE_SETUP + COMPLEX_DIMENSION_LINK:
        await post_and_raise_if_error(
            client=isolated_client,
            endpoint=endpoint,
            json=json,  # type: ignore
        )
    return isolated_client


@pytest.mark.asyncio
async def test_link_dimension_with_errors(
    dimensions_link_client: AsyncClient,
):
    """
    Test linking dimensions with errors
    """
    response = await dimensions_link_client.post(
        "/nodes/default.does_not_exist/link",
        json={
            "dimension_node": "default.users",
            "join_on": ("default.does_not_exist.x = default.users.y"),
            "join_cardinality": "many_to_one",
        },
    )
    assert (
        response.json()["message"]
        == "A node with name `default.does_not_exist` does not exist."
    )

    response = await dimensions_link_client.post(
        "/nodes/default.events/link",
        json={
            "dimension_node": "default.random_dimension",
            "join_on": ("default.events.x = default.random_dimension.y"),
            "join_cardinality": "many_to_one",
        },
    )
    assert (
        response.json()["message"]
        == "A node with name `default.random_dimension` does not exist."
    )

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

    # Test linking a non-dimension node as the dimension_node (source node instead of dimension)
    response = await dimensions_link_client.post(
        "/nodes/default.events/link",
        json={
            "dimension_node": "default.events_table",
            "join_on": ("default.events.user_id = default.events_table.user_id"),
            "join_cardinality": "many_to_one",
        },
    )
    assert response.json()["message"] == (
        "Cannot link dimension to a node of type source. Must be a dimension node."
    )


@pytest.fixture
def link_events_to_users_without_role(
    dimensions_link_client: AsyncClient,
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
    dimensions_link_client: AsyncClient,
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
        assert response.status_code == 201
        return response

    return _link_events_to_users_with_role_direct


@pytest.fixture
def link_events_to_users_with_role_windowed(
    dimensions_link_client: AsyncClient,
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
    dimensions_link_client: AsyncClient,
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
    dimensions_link_client: AsyncClient,
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
    dimensions_link_client: AsyncClient,
    link_events_to_users_without_role,
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
            "default_value": None,
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
                "version": "v1.2",
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
                "version": "v1.1",
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
    assert sorted(
        [(attr["name"], attr["path"]) for attr in response.json()],
        key=lambda x: x[0],
    ) == sorted(
        [
            ("default.users.account_type", ["default.events", "default.users"]),
            ("default.users.registration_country", ["default.events", "default.users"]),
            ("default.users.residence_country", ["default.events", "default.users"]),
            ("default.users.snapshot_date", ["default.events", "default.users"]),
            ("default.users.user_id", ["default.events", "default.users"]),
        ],
        key=lambda x: x[0],
    )


@pytest.mark.asyncio
async def test_link_complex_dimension_with_role(
    dimensions_link_client: AsyncClient,
    link_events_to_users_with_role_direct,
    link_events_to_users_with_role_windowed,
    link_users_to_countries_with_role_registration,
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
            "default_value": None,
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
    assert sorted(
        response.json()["dimension_links"],
        key=lambda x: x["role"] or "",
    ) == sorted(
        [
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
                "default_value": None,
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
                "foreign_keys": {
                    "default.events.event_start_date": None,
                    "default.events.user_id": "default.users.user_id",
                },
                "default_value": None,
            },
        ],
        key=lambda x: x["role"],  # type: ignore
    )

    # Verify that the dimensions on the downstream metric have roles specified
    response = await dimensions_link_client.get(
        "/nodes/default.elapsed_secs/dimensions",
    )
    assert sorted(
        [(attr["name"], attr["path"]) for attr in response.json()],
        key=lambda x: x[0],
    ) == [
        (
            "default.countries.country_code[user_direct->registration_country]",
            ["default.elapsed_secs", "default.users", "default.countries"],
        ),
        (
            "default.countries.country_code[user_windowed->registration_country]",
            ["default.elapsed_secs", "default.users", "default.countries"],
        ),
        (
            "default.countries.name[user_direct->registration_country]",
            ["default.elapsed_secs", "default.users", "default.countries"],
        ),
        (
            "default.countries.name[user_windowed->registration_country]",
            ["default.elapsed_secs", "default.users", "default.countries"],
        ),
        (
            "default.countries.population[user_direct->registration_country]",
            ["default.elapsed_secs", "default.users", "default.countries"],
        ),
        (
            "default.countries.population[user_windowed->registration_country]",
            ["default.elapsed_secs", "default.users", "default.countries"],
        ),
        (
            "default.users.account_type[user_direct]",
            ["default.elapsed_secs", "default.users"],
        ),
        (
            "default.users.account_type[user_windowed]",
            ["default.elapsed_secs", "default.users"],
        ),
        (
            "default.users.registration_country[user_direct]",
            ["default.elapsed_secs", "default.users"],
        ),
        (
            "default.users.registration_country[user_windowed]",
            ["default.elapsed_secs", "default.users"],
        ),
        (
            "default.users.residence_country[user_direct]",
            ["default.elapsed_secs", "default.users"],
        ),
        (
            "default.users.residence_country[user_windowed]",
            ["default.elapsed_secs", "default.users"],
        ),
        (
            "default.users.snapshot_date[user_direct]",
            ["default.elapsed_secs", "default.users"],
        ),
        (
            "default.users.snapshot_date[user_windowed]",
            ["default.elapsed_secs", "default.users"],
        ),
        (
            "default.users.user_id[user_direct]",
            ["default.elapsed_secs", "default.users"],
        ),
        (
            "default.users.user_id[user_windowed]",
            ["default.elapsed_secs", "default.users"],
        ),
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
    user_windowed.user_id default_DOT_users_DOT_user_id_LBRACK_user_windowed_RBRACK,
    user_windowed.snapshot_date default_DOT_users_DOT_snapshot_date_LBRACK_user_windowed_RBRACK,
    user_windowed.registration_country default_DOT_users_DOT_registration_country_LBRACK_user_windowed_RBRACK,
    SUM(default_DOT_events.elapsed_secs) default_DOT_elapsed_secs
  FROM default_DOT_events
  LEFT JOIN default_DOT_users AS user_windowed ON default_DOT_events.user_id = user_windowed.user_id
    AND default_DOT_events.event_start_date BETWEEN user_windowed.snapshot_date
    AND CAST(DATE_ADD(CAST(user_windowed.snapshot_date AS DATE), 10) AS INT)
  WHERE  user_windowed.registration_country = 'NZ'
  GROUP BY
    user_windowed.user_id,
    user_windowed.snapshot_date,
    user_windowed.registration_country
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
        user_direct.user_id default_DOT_users_DOT_user_id_LBRACK_user_direct_RBRACK,
        user_direct.snapshot_date default_DOT_users_DOT_snapshot_date_LBRACK_user_direct_RBRACK,
        user_direct.registration_country default_DOT_users_DOT_registration_country_LBRACK_user_direct_RBRACK,
        SUM(default_DOT_events.elapsed_secs) default_DOT_elapsed_secs
      FROM default_DOT_events
      LEFT JOIN default_DOT_users AS user_direct
        ON default_DOT_events.user_id = user_direct.user_id
        AND default_DOT_events.event_start_date = user_direct.snapshot_date
      GROUP BY
        user_direct.user_id,
        user_direct.snapshot_date,
        user_direct.registration_country
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
    response = await dimensions_link_client.get(
        "/sql/default.elapsed_secs?",
        params={
            "dimensions": [
                # "default.countries.country_code[user_windowed->registration_country]",
                "default.countries.name[user_direct->registration_country]",
                "default.users.snapshot_date[user_direct]",
                "default.users.registration_country[user_direct]",
            ],
            "filters": [
                "default.countries.name[user_direct->registration_country] = 'NZ'",
            ],
        },
    )
    data = response.json()
    query = data["sql"]
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
    user_direct__registration_country.name default_DOT_countries_DOT_name_LBRACK_user_direct_MINUS__GT_registration_country_RBRACK,
    user_direct.snapshot_date default_DOT_users_DOT_snapshot_date_LBRACK_user_direct_RBRACK,
    user_direct.registration_country default_DOT_users_DOT_registration_country_LBRACK_user_direct_RBRACK,
    SUM(default_DOT_events.elapsed_secs) default_DOT_elapsed_secs
  FROM default_DOT_events
  LEFT JOIN default_DOT_users AS user_direct
    ON default_DOT_events.user_id = user_direct.user_id
    AND default_DOT_events.event_start_date = user_direct.snapshot_date
  INNER JOIN default_DOT_countries AS user_direct__registration_country
    ON user_direct.registration_country = user_direct__registration_country.country_code
  WHERE  user_direct__registration_country.name = 'NZ'
  GROUP BY
    user_direct__registration_country.name,
    user_direct.snapshot_date,
    user_direct.registration_country
)
SELECT
  default_DOT_events_metrics.default_DOT_countries_DOT_name_LBRACK_user_direct_MINUS__GT_registration_country_RBRACK,
  default_DOT_events_metrics.default_DOT_users_DOT_snapshot_date_LBRACK_user_direct_RBRACK,
  default_DOT_events_metrics.default_DOT_users_DOT_registration_country_LBRACK_user_direct_RBRACK,
  default_DOT_events_metrics.default_DOT_elapsed_secs
FROM default_DOT_events_metrics
"""
    assert str(parse(query)) == str(parse(expected))
    assert data["columns"] == [
        {
            "column": "name[user_direct->registration_country]",
            "name": "default_DOT_countries_DOT_name_LBRACK_user_direct_MINUS__GT_registration_country_RBRACK",
            "node": "default.countries",
            "semantic_entity": "default.countries.name[user_direct->registration_country]",
            "semantic_type": "dimension",
            "type": "string",
        },
        {
            "column": "snapshot_date[user_direct]",
            "name": "default_DOT_users_DOT_snapshot_date_LBRACK_user_direct_RBRACK",
            "node": "default.users",
            "semantic_entity": "default.users.snapshot_date[user_direct]",
            "semantic_type": "dimension",
            "type": "int",
        },
        {
            "column": "registration_country[user_direct]",
            "name": "default_DOT_users_DOT_registration_country_LBRACK_user_direct_RBRACK",
            "node": "default.users",
            "semantic_entity": "default.users.registration_country[user_direct]",
            "semantic_type": "dimension",
            "type": "string",
        },
        {
            "column": "default_DOT_elapsed_secs",
            "name": "default_DOT_elapsed_secs",
            "node": "default.elapsed_secs",
            "semantic_entity": "default.elapsed_secs.default_DOT_elapsed_secs",
            "semantic_type": "metric",
            "type": "bigint",
        },
    ]


@pytest.mark.asyncio
async def test_remove_dimension_link(
    dimensions_link_client: AsyncClient,
    link_events_to_users_with_role_direct,
    link_events_to_users_without_role,
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
    dimensions_link_client: AsyncClient,
    link_events_to_users_with_role_direct,
    link_events_to_users_with_role_windowed,
    link_users_to_countries_with_role_registration,
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
  user_direct__registration_country.name default_DOT_countries_DOT_name_LBRACK_user_direct_MINUS__GT_registration_country_RBRACK,
  user_direct.snapshot_date default_DOT_users_DOT_snapshot_date_LBRACK_user_direct_RBRACK,
  user_direct.registration_country default_DOT_users_DOT_registration_country_LBRACK_user_direct_RBRACK
FROM default_DOT_events
LEFT JOIN default_DOT_users AS user_direct
  ON default_DOT_events.user_id = user_direct.user_id
  AND default_DOT_events.event_start_date = user_direct.snapshot_date
INNER JOIN default_DOT_countries AS user_direct__registration_country
  ON user_direct.registration_country = user_direct__registration_country.country_code
WHERE  user_direct__registration_country.name = 'UG'"""
    assert str(parse(query)) == str(parse(expected))


@pytest.mark.asyncio
async def test_reference_dimension_links_errors(
    dimensions_link_client: AsyncClient,
    reference_link_events_user_registration_country,
):
    """
    Test various reference dimension link errors
    """
    # Not a dimension node being linked
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
async def test_reference_dimension_links(
    dimensions_link_client: AsyncClient,
    link_events_to_users_without_role,
):
    """
    Test reference dimension links on dimension nodes
    """
    await link_events_to_users_without_role()
    response = await dimensions_link_client.get(
        "/nodes/default.elapsed_secs/dimensions",
    )
    dimensions_data = response.json()
    assert [dim["name"] for dim in dimensions_data] == [
        "default.users.user_id",
        "default.users.snapshot_date",
        "default.users.registration_country",
        "default.users.residence_country",
        "default.users.account_type",
    ]
    response = await dimensions_link_client.post(
        "/nodes/default.users/columns/residence_country/link",
        params={
            "dimension_node": "default.countries",
            "dimension_column": "name",
        },
    )
    assert response.status_code == 201
    response = await dimensions_link_client.get(
        "/nodes/default.elapsed_secs/dimensions",
    )
    dimensions_data = response.json()
    assert [dim["name"] for dim in dimensions_data] == [
        "default.countries.name",
        "default.users.user_id",
        "default.users.snapshot_date",
        "default.users.registration_country",
        "default.users.residence_country",
        "default.users.account_type",
    ]


@pytest.mark.asyncio
async def test_measures_sql_with_reference_dimension_links(
    dimensions_link_client: AsyncClient,
    reference_link_events_user_registration_country,
    link_events_to_users_without_role,
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
            "name": "default.users.registration_country",
            "node_display_name": "Users",
            "node_name": "default.users",
            "path": [
                "default.events.user_registration_country",
            ],
            "type": "string",
            "properties": [],
        },
    ]

    await link_events_to_users_without_role()
    response = await dimensions_link_client.get(
        "/nodes/default.elapsed_secs/dimensions",
    )
    dimensions_data = response.json()
    assert set([dim["name"] for dim in dimensions_data]) == set(
        [
            "default.users.account_type",
            "default.users.registration_country",
            "default.users.registration_country",
            "default.users.residence_country",
            "default.users.snapshot_date",
            "default.users.user_id",
        ],
    )

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
            "column": "user_registration_country",
            "node": "default.events",
            "semantic_entity": "default.users.registration_country",
            "semantic_type": "dimension",
        },
    ]
    response = await dimensions_link_client.delete(
        "/nodes/default.events/columns/user_registration_country/link",
    )
    assert response.status_code == 200
    response = await dimensions_link_client.get("/sql/measures/v2", params=sql_params)
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
    dimensions_link_client: AsyncClient,
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


@pytest.mark.asyncio
async def test_dimension_link_deleted_dimension_node(
    dimensions_link_client: AsyncClient,
    link_events_to_users_without_role,
):
    """
    Test dimension links with deleted dimension node
    """
    response = await link_events_to_users_without_role()
    assert response.json() == {
        "message": "Dimension node default.users has been successfully linked to node "
        "default.events.",
    }
    response = await dimensions_link_client.get("/nodes/default.events")
    assert [
        link["dimension"]["name"] for link in response.json()["dimension_links"]
    ] == ["default.users"]

    gql_find_nodes_query = """
      query Node {
        findNodes(names: ["default.events"]) {
          current {
            dimensionLinks {
              dimension {
                name
              }
            }
          }
        }
      }
    """
    response = await dimensions_link_client.post(
        "/graphql",
        json={"query": gql_find_nodes_query},
    )
    assert response.json()["data"]["findNodes"] == [
        {
            "current": {"dimensionLinks": [{"dimension": {"name": "default.users"}}]},
        },
    ]

    # Deactivate the dimension node
    response = await dimensions_link_client.delete("/nodes/default.users")

    # The dimension link should be hidden
    response = await dimensions_link_client.get("/nodes/default.events")
    assert response.json()["dimension_links"] == []
    response = await dimensions_link_client.post(
        "/graphql",
        json={"query": gql_find_nodes_query},
    )
    assert response.json()["data"]["findNodes"] == [{"current": {"dimensionLinks": []}}]

    # Restore the dimension node
    response = await dimensions_link_client.post("/nodes/default.users/restore")
    assert response.status_code == 200

    # The dimension link should be recovered
    response = await dimensions_link_client.get("/nodes/default.events")
    assert [
        link["dimension"]["name"] for link in response.json()["dimension_links"]
    ] == ["default.users"]
    response = await dimensions_link_client.post(
        "/graphql",
        json={"query": gql_find_nodes_query},
    )
    assert response.json()["data"]["findNodes"] == [
        {
            "current": {"dimensionLinks": [{"dimension": {"name": "default.users"}}]},
        },
    ]

    # Hard delete the dimension node
    response = await dimensions_link_client.delete("/nodes/default.users/hard")

    # The dimension link to default.users should be gone
    response = await dimensions_link_client.get("/nodes/default.events")
    final_dim_names = [
        link["dimension"]["name"] for link in response.json()["dimension_links"]
    ]
    assert "default.users" not in final_dim_names  # users link should be removed
    response = await dimensions_link_client.post(
        "/graphql",
        json={"query": gql_find_nodes_query},
    )
    gql_result = response.json()["data"]["findNodes"]
    gql_dim_names = [
        dl["dimension"]["name"] for dl in gql_result[0]["current"]["dimensionLinks"]
    ]
    assert "default.users" not in gql_dim_names  # users link should be removed


@pytest.mark.asyncio
async def test_dimension_link_with_default_value(
    dimensions_link_client: AsyncClient,
):
    """
    Test dimension link with default_value to handle NULL results from LEFT JOINs.
    When default_value is set, the dimension column should be wrapped with COALESCE.
    """
    # Create dimension link with default_value
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
            "default_value": "Unknown",
        },
    )
    assert response.status_code == 201
    assert response.json() == {
        "message": "Dimension node default.users has been successfully "
        "linked to node default.events.",
    }

    # Verify the link has default_value set
    response = await dimensions_link_client.get("/nodes/default.events")
    link = response.json()["dimension_links"][0]
    assert link["default_value"] == "Unknown"
    assert link["dimension"]["name"] == "default.users"

    # Verify that updating the link preserves the default_value
    response = await dimensions_link_client.post(
        "/nodes/default.events/link",
        json={
            "dimension_node": "default.users",
            "join_type": "left",
            "join_on": (
                "default.events.user_id = default.users.user_id "
                "AND default.events.event_start_date = default.users.snapshot_date"
            ),
            "join_cardinality": "many_to_one",
            "default_value": "N/A",
        },
    )
    assert response.status_code == 201  # Update

    response = await dimensions_link_client.get("/nodes/default.events")
    link = response.json()["dimension_links"][0]
    assert link["default_value"] == "N/A"

    # Verify SQL generation wraps dimension with COALESCE
    response = await dimensions_link_client.get(
        "/sql/default.events?dimensions=default.users.registration_country",
    )
    query = response.json()["sql"]
    # The dimension column should be wrapped in COALESCE with the default_value
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
      default_DOT_events.user_id default_DOT_events_DOT_user_id,
      default_DOT_events.event_start_date default_DOT_events_DOT_event_start_date,
      default_DOT_events.event_end_date default_DOT_events_DOT_event_end_date,
      default_DOT_events.elapsed_secs default_DOT_events_DOT_elapsed_secs,
      default_DOT_events.user_registration_country default_DOT_events_DOT_user_registration_country,
      COALESCE(default_DOT_users.registration_country, 'N/A') AS default_DOT_users_DOT_registration_country
    FROM default_DOT_events
    LEFT JOIN default_DOT_users
      ON default_DOT_events.user_id = default_DOT_users.user_id
        AND default_DOT_events.event_start_date = default_DOT_users.snapshot_date
    """
    assert_sql_equal(query, expected_sql)

    # Test v3 measures SQL generation with default_value
    response = await dimensions_link_client.get(
        "/sql/measures/v3/",
        params={
            "metrics": ["default.elapsed_secs"],
            "dimensions": ["default.users.registration_country"],
        },
    )
    assert response.status_code == 200
    result = response.json()
    assert len(result["grain_groups"]) == 1
    v3_measures_sql = result["grain_groups"][0]["sql"]
    expected_v3_measures_sql = """
    WITH default_events AS (
      SELECT user_id, event_start_date, elapsed_secs
      FROM default.examples.events
    ),
    default_users AS (
      SELECT user_id, snapshot_date, registration_country
      FROM default.examples.users
    )
    SELECT
      COALESCE(t2.registration_country, 'N/A') AS registration_country,
      SUM(t1.elapsed_secs) elapsed_secs_sum_88a2603f
    FROM default_events t1
    LEFT OUTER JOIN default_users t2
      ON t1.user_id = t2.user_id AND t1.event_start_date = t2.snapshot_date
    GROUP BY t2.registration_country
    """
    assert_sql_equal(v3_measures_sql, expected_v3_measures_sql)

    # Test v3 metrics SQL generation with default_value
    response = await dimensions_link_client.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["default.elapsed_secs"],
            "dimensions": ["default.users.registration_country"],
        },
    )
    assert response.status_code == 200
    v3_metrics_sql = response.json()["sql"]
    expected_v3_metrics_sql = """
    WITH default_events AS (
      SELECT user_id, event_start_date, elapsed_secs
      FROM default.examples.events
    ),
    default_users AS (
      SELECT user_id, snapshot_date, registration_country
      FROM default.examples.users
    ),
    events_0 AS (
      SELECT
        COALESCE(t2.registration_country, 'N/A') AS registration_country,
        SUM(t1.elapsed_secs) elapsed_secs_sum_88a2603f
      FROM default_events t1
      LEFT OUTER JOIN default_users t2
        ON t1.user_id = t2.user_id AND t1.event_start_date = t2.snapshot_date
      GROUP BY t2.registration_country
    )
    SELECT
      events_0.registration_country AS registration_country,
      SUM(events_0.elapsed_secs_sum_88a2603f) AS elapsed_secs
    FROM events_0
    GROUP BY events_0.registration_country
    """
    assert_sql_equal(v3_metrics_sql, expected_v3_metrics_sql)
