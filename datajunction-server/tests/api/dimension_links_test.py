"""Dimension linking related tests."""
from unittest import mock

import pytest
from starlette.testclient import TestClient

from tests.conftest import post_and_raise_if_error
from tests.examples import EXAMPLES
from tests.sql.utils import compare_query_strings


@pytest.fixture
def dimensions_link_client(client_with_roads: TestClient) -> TestClient:
    """
    Add dimension link examples to the roads test client.
    """
    for endpoint, json in EXAMPLES["DIMENSION_LINK"]:
        post_and_raise_if_error(  # type: ignore
            client=client_with_roads,
            endpoint=endpoint,
            json=json,  # type: ignore
        )
    return client_with_roads


def test_link_dimension_with_errors(
    dimensions_link_client: TestClient,  # pylint: disable=redefined-outer-name
):
    """
    Test linking dimensions with errors
    """
    response = dimensions_link_client.post(
        "/nodes/default.num_repair_orders/link",
        json={
            "dimension_node": "default.date_dim",
            "join_sql": (
                "default.num_repair_orders A JOIN default.date_dim D ON A.x = D.y"
            ),
            "join_cardinality": "many_to_one",
        },
    )
    assert response.json()["message"] == (
        "Cannot link dimension to a node of type metric. Must be a source, "
        "dimension, or transform node."
    )
    response = dimensions_link_client.post(
        "/nodes/default.regional_level_agg/link",
        json={
            "dimension_node": "default.date_dim",
            "join_sql": (
                "default.regional_level_agg A JOIN default.regional_level_agg D "
                "ON A.order_year = D.order_year"
            ),
            "join_cardinality": "many_to_one",
        },
    )
    assert response.json()["message"] == (
        "The join SQL provided does not reference both the origin node "
        "default.regional_level_agg and the dimension node default.date_dim that "
        "it's being joined to."
    )

    response = dimensions_link_client.post(
        "/nodes/default.regional_level_agg/link",
        json={
            "dimension_node": "default.date_dim",
            "join_sql": (
                "default.regional_level_agg A JOIN default.date_dim D "
                "ON A.order_year = D.year "
                "AND A.order_month = D.month "
                "AND A.order_day = D.daym"
            ),
            "join_cardinality": "many_to_one",
        },
    )
    assert response.json()["message"] == "Column D.daym does not exist on node"

    response = dimensions_link_client.post(
        "/nodes/default.regional_level_agg/link",
        json={
            "dimension_node": "default.date_dim",
            "join_sql": ("default.regional_level_agg A"),
            "join_cardinality": "many_to_one",
        },
    )
    assert response.json()["message"] == (
        "Provided SQL `default.regional_level_agg A` does not contain JOIN clause"
    )


def test_link_dimension_success(
    dimensions_link_client: TestClient,  # pylint: disable=redefined-outer-name
):
    """
    Test linking dimensions (with join SQL) successfully
    """
    response = dimensions_link_client.post(
        "/nodes/default.regional_level_agg/link",
        json={
            "dimension_node": "default.date_dim",
            "join_sql": (
                "default.regional_level_agg A LEFT JOIN default.date_dim D "
                "ON A.order_year = D.year "
                "AND A.order_month = D.month "
                "AND A.order_day = D.day"
            ),
            "join_cardinality": "many_to_one",
        },
    )
    assert response.json() == {
        "message": "Dimension node default.date_dim has been successfully linked to "
        "node default.regional_level_agg.",
    }

    response = dimensions_link_client.get("/nodes/default.regional_level_agg")
    assert response.json()["dimension_links"] == [
        {
            "dimension": {"name": "default.date_dim"},
            "join_cardinality": "many_to_one",
            "join_sql": "default.regional_level_agg A LEFT JOIN default.date_dim D ON "
            "A.order_year = D.year AND A.order_month = D.month AND "
            "A.order_day = D.day",
            "role": None,
        },
    ]

    # Update dimension link
    response = dimensions_link_client.post(
        "/nodes/default.regional_level_agg/link",
        json={
            "dimension_node": "default.date_dim",
            "join_sql": (
                "default.regional_level_agg A JOIN default.date_dim D "
                "ON A.order_year = D.year "
                "AND A.order_month = D.month"
            ),
            "join_cardinality": "many_to_one",
        },
    )
    assert response.json() == {
        "message": "The dimension link between default.regional_level_agg and "
        "default.date_dim has been successfully updated.",
    }

    response = dimensions_link_client.get("/history?node=default.regional_level_agg")
    assert [entry for entry in response.json() if entry["entity_type"] == "link"] == [
        {
            "activity_type": "create",
            "created_at": mock.ANY,
            "details": {
                "dimension": "default.date_dim",
                "join_cardinality": "many_to_one",
                "join_sql": "default.regional_level_agg A LEFT JOIN default.date_dim "
                "D ON A.order_year = D.year AND A.order_month = "
                "D.month AND A.order_day = D.day",
                "role": None,
            },
            "entity_name": "default.regional_level_agg",
            "entity_type": "link",
            "id": mock.ANY,
            "node": "default.regional_level_agg",
            "post": {},
            "pre": {},
            "user": mock.ANY,
        },
        {
            "activity_type": "update",
            "created_at": mock.ANY,
            "details": {
                "dimension": "default.date_dim",
                "join_cardinality": "many_to_one",
                "join_sql": "default.regional_level_agg A JOIN default.date_dim "
                "D ON A.order_year = D.year AND A.order_month = D.month",
                "role": None,
            },
            "entity_name": "default.regional_level_agg",
            "entity_type": "link",
            "id": mock.ANY,
            "node": "default.regional_level_agg",
            "post": {},
            "pre": {},
            "user": mock.ANY,
        },
    ]

    response = dimensions_link_client.get(
        "/sql/default.regional_level_agg?dimensions=default.date_dim.year",
    )
    query = response.json()["sql"]
    compare_query_strings(
        query,
        # pylint: disable=line-too-long
        """SELECT
  default_DOT_regional_level_agg.us_region_id default_DOT_regional_level_agg_DOT_us_region_id,
  default_DOT_regional_level_agg.state_name default_DOT_regional_level_agg_DOT_state_name,
  default_DOT_regional_level_agg.location_hierarchy default_DOT_regional_level_agg_DOT_location_hierarchy,
  default_DOT_regional_level_agg.order_year default_DOT_regional_level_agg_DOT_order_year,
  default_DOT_regional_level_agg.order_month default_DOT_regional_level_agg_DOT_order_month,
  default_DOT_regional_level_agg.order_day default_DOT_regional_level_agg_DOT_order_day,
  default_DOT_regional_level_agg.completed_repairs default_DOT_regional_level_agg_DOT_completed_repairs,
  default_DOT_regional_level_agg.total_repairs_dispatched default_DOT_regional_level_agg_DOT_total_repairs_dispatched,
  default_DOT_regional_level_agg.total_amount_in_region default_DOT_regional_level_agg_DOT_total_amount_in_region,
  default_DOT_regional_level_agg.avg_repair_amount_in_region default_DOT_regional_level_agg_DOT_avg_repair_amount_in_region,
  default_DOT_regional_level_agg.avg_dispatch_delay default_DOT_regional_level_agg_DOT_avg_dispatch_delay,
  default_DOT_regional_level_agg.unique_contractors default_DOT_regional_level_agg_DOT_unique_contractors,
  default.date_dim.year default_DOT_date_dim_DOT_year
FROM (
    SELECT default_DOT_us_region.us_region_id,
      default_DOT_us_states.state_name,
      CONCAT(
        default_DOT_us_states.state_name,
        '-',
        default_DOT_us_region.us_region_description
      ) AS location_hierarchy,
      EXTRACT(YEAR, ro.order_date) AS order_year,
      EXTRACT(MONTH, ro.order_date) AS order_month,
      EXTRACT(DAY, ro.order_date) AS order_day,
      COUNT(
        DISTINCT CASE
          WHEN ro.dispatched_date IS NOT NULL THEN ro.repair_order_id
          ELSE NULL
        END
      ) AS completed_repairs,
      COUNT(DISTINCT ro.repair_order_id) AS total_repairs_dispatched,
      SUM(
        default_DOT_repair_order_details.price * default_DOT_repair_order_details.quantity
      ) AS total_amount_in_region,
      AVG(
        default_DOT_repair_order_details.price * default_DOT_repair_order_details.quantity
      ) AS avg_repair_amount_in_region,
      AVG(DATEDIFF(ro.dispatched_date, ro.order_date)) AS avg_dispatch_delay,
      COUNT(DISTINCT default_DOT_contractors.contractor_id) AS unique_contractors
    FROM (
        SELECT default_DOT_repair_orders.repair_order_id,
          default_DOT_repair_orders.municipality_id,
          default_DOT_repair_orders.hard_hat_id,
          default_DOT_repair_orders.order_date,
          default_DOT_repair_orders.required_date,
          default_DOT_repair_orders.dispatched_date,
          default_DOT_repair_orders.dispatcher_id
        FROM roads.repair_orders AS default_DOT_repair_orders
      ) AS ro
      JOIN roads.municipality AS default_DOT_municipality ON ro.municipality_id = default_DOT_municipality.municipality_id
      JOIN roads.us_states AS default_DOT_us_states ON default_DOT_municipality.state_id = default_DOT_us_states.state_id
      AND AVG(
        default_DOT_repair_order_details.price * default_DOT_repair_order_details.quantity
      ) > (
        SELECT AVG(
            default_DOT_repair_order_details.price * default_DOT_repair_order_details.quantity
          )
        FROM roads.repair_order_details AS default_DOT_repair_order_details
        WHERE default_DOT_repair_order_details.repair_order_id = ro.repair_order_id
      )
      JOIN roads.us_states AS default_DOT_us_states ON default_DOT_municipality.state_id = default_DOT_us_states.state_id
      JOIN roads.us_region AS default_DOT_us_region ON default_DOT_us_states.state_region = default_DOT_us_region.us_region_id
      JOIN roads.repair_order_details AS default_DOT_repair_order_details ON ro.repair_order_id = default_DOT_repair_order_details.repair_order_id
      JOIN roads.repair_type AS default_DOT_repair_type ON default_DOT_repair_order_details.repair_type_id = default_DOT_repair_type.repair_type_id
      JOIN roads.contractors AS default_DOT_contractors ON default_DOT_repair_type.contractor_id = default_DOT_contractors.contractor_id
    GROUP BY default_DOT_us_region.us_region_id,
      EXTRACT(YEAR, ro.order_date),
      EXTRACT(MONTH, ro.order_date),
      EXTRACT(DAY, ro.order_date)
  ) AS default_DOT_regional_level_agg
  JOIN (
    SELECT default_DOT_date.dateint,
      default_DOT_date.month,
      default_DOT_date.year,
      default_DOT_date.day
    FROM examples.date AS default_DOT_date
  ) AS default_DOT_date_dim ON default.regional_level_agg.order_year = default_DOT_date_dim.year
  AND default.regional_level_agg.order_month = default_DOT_date_dim.month
        """,
    )

    response = dimensions_link_client.get(
        "/nodes/default.regional_level_agg/dimensions",
    )
    assert [(attr["name"], attr["path"]) for attr in response.json()] == [
        ("default.date_dim.dateint", ["default.regional_level_agg."]),
        ("default.date_dim.day", ["default.regional_level_agg."]),
        ("default.date_dim.month", ["default.regional_level_agg."]),
        ("default.date_dim.year", ["default.regional_level_agg."]),
        ("default.regional_level_agg.order_day", []),
        ("default.regional_level_agg.order_month", []),
        ("default.regional_level_agg.order_year", []),
        ("default.regional_level_agg.state_name", []),
        ("default.regional_level_agg.us_region_id", []),
    ]


def test_link_dimension_with_role(
    dimensions_link_client: TestClient,  # pylint: disable=redefined-outer-name
):
    """
    Test linking dimension with role
    """
    # Link the date dimension with a role of "order_date"
    response = dimensions_link_client.post(
        "/nodes/default.regional_level_agg/link",
        json={
            "dimension_node": "default.date_dim",
            "join_sql": (
                "default.regional_level_agg A LEFT JOIN default.date_dim D "
                "ON A.order_year = D.year "
                "AND A.order_month = D.month "
                "AND A.order_day = D.day"
            ),
            "join_cardinality": "many_to_one",
            "role": "order_date",
        },
    )
    assert response.status_code == 201

    response = dimensions_link_client.get(
        "/nodes/default.regional_level_agg/dimensions",
    )
    assert [(attr["name"], attr["path"]) for attr in response.json()] == [
        ("default.date_dim.dateint", ["default.regional_level_agg.order_date"]),
        ("default.date_dim.day", ["default.regional_level_agg.order_date"]),
        ("default.date_dim.month", ["default.regional_level_agg.order_date"]),
        ("default.date_dim.year", ["default.regional_level_agg.order_date"]),
        ("default.regional_level_agg.order_day", []),
        ("default.regional_level_agg.order_month", []),
        ("default.regional_level_agg.order_year", []),
        ("default.regional_level_agg.state_name", []),
        ("default.regional_level_agg.us_region_id", []),
    ]

    # Link the date dimension with a role of "backorder_date"
    response = dimensions_link_client.post(
        "/nodes/default.regional_level_agg/link",
        json={
            "dimension_node": "default.date_dim",
            "join_sql": (
                "default.regional_level_agg A LEFT JOIN default.date_dim D "
                "ON A.order_year = D.year "
                "AND A.order_month = D.month "
                "AND A.order_day = D.day"
            ),
            "join_cardinality": "many_to_one",
            "role": "backorder_date",
        },
    )
    assert response.status_code == 201

    response = dimensions_link_client.get("/nodes/default.regional_level_agg")
    assert response.json()["dimension_links"] == [
        {
            "dimension": {"name": "default.date_dim"},
            "join_cardinality": "many_to_one",
            "join_sql": "default.regional_level_agg A LEFT JOIN default.date_dim D ON "
            "A.order_year = D.year AND A.order_month = D.month AND "
            "A.order_day = D.day",
            "role": "order_date",
        },
        {
            "dimension": {"name": "default.date_dim"},
            "join_cardinality": "many_to_one",
            "join_sql": "default.regional_level_agg A LEFT JOIN default.date_dim D ON "
            "A.order_year = D.year AND A.order_month = D.month AND "
            "A.order_day = D.day",
            "role": "backorder_date",
        },
    ]

    response = dimensions_link_client.get(
        "/nodes/default.regional_level_agg/dimensions",
    )
    assert [(attr["name"], attr["path"]) for attr in response.json()] == [
        ("default.date_dim.dateint", ["default.regional_level_agg.backorder_date"]),
        ("default.date_dim.dateint", ["default.regional_level_agg.order_date"]),
        ("default.date_dim.day", ["default.regional_level_agg.backorder_date"]),
        ("default.date_dim.day", ["default.regional_level_agg.order_date"]),
        ("default.date_dim.month", ["default.regional_level_agg.backorder_date"]),
        ("default.date_dim.month", ["default.regional_level_agg.order_date"]),
        ("default.date_dim.year", ["default.regional_level_agg.backorder_date"]),
        ("default.date_dim.year", ["default.regional_level_agg.order_date"]),
        ("default.regional_level_agg.order_day", []),
        ("default.regional_level_agg.order_month", []),
        ("default.regional_level_agg.order_year", []),
        ("default.regional_level_agg.state_name", []),
        ("default.regional_level_agg.us_region_id", []),
    ]

    # Update the "backorder_date" dimension role
    response = dimensions_link_client.post(
        "/nodes/default.regional_level_agg/link",
        json={
            "dimension_node": "default.date_dim",
            "join_sql": (
                "default.regional_level_agg R LEFT JOIN default.date_dim D "
                "ON R.order_year = D.year "
                "AND R.order_month = D.month"
            ),
            "join_cardinality": "many_to_one",
            "role": "backorder_date",
        },
    )
    assert response.status_code == 201

    # Check that only the backorder_date dimension role was updated
    response = dimensions_link_client.get("/nodes/default.regional_level_agg")
    assert response.json()["dimension_links"] == [
        {
            "dimension": {"name": "default.date_dim"},
            "join_cardinality": "many_to_one",
            "join_sql": "default.regional_level_agg A LEFT JOIN default.date_dim D ON "
            "A.order_year = D.year AND A.order_month = D.month AND "
            "A.order_day = D.day",
            "role": "order_date",
        },
        {
            "dimension": {"name": "default.date_dim"},
            "join_cardinality": "many_to_one",
            "join_sql": "default.regional_level_agg R LEFT JOIN default.date_dim D ON "
            "R.order_year = D.year AND R.order_month = D.month",
            "role": "backorder_date",
        },
    ]
