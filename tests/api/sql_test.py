"""Tests for the /sql/ endpoint"""
import pytest
from sqlmodel import Session
from starlette.testclient import TestClient

from dj.models import Column, Database, Node
from dj.models.node import NodeRevision, NodeType
from dj.typing import ColumnType
from tests.sql.utils import compare_query_strings


def test_sql(
    session: Session,
    client: TestClient,
) -> None:
    """
    Test ``GET /sql/{name}/``.
    """
    database = Database(name="test", URI="blah://", tables=[])

    source_node = Node(name="my_table", type=NodeType.SOURCE, current_version="1")
    source_node_rev = NodeRevision(
        name=source_node.name,
        node=source_node,
        version="1",
        schema_="rev",
        table="my_table",
        columns=[Column(name="one", type=ColumnType["STR"])],
        type=NodeType.SOURCE,
    )

    node = Node(name="a-metric", type=NodeType.METRIC, current_version="1")
    node_revision = NodeRevision(
        name=node.name,
        node=node,
        version="1",
        query="SELECT COUNT(*) FROM my_table",
        type=NodeType.METRIC,
    )
    session.add(database)
    session.add(node_revision)
    session.add(source_node_rev)
    session.commit()

    response = client.get("/sql/a-metric/")
    assert response.json() == {
        "sql": 'SELECT  COUNT(*) AS col0 \n FROM "rev"."my_table" AS my_table',
    }


@pytest.mark.parametrize(
    "node_name, dimensions, filters, sql",
    [
        # querying on source node with filter on joinable dimension
        (
            "repair_orders",
            [],
            ["hard_hat.state='CA'"],
            """
            SELECT  repair_orders.repair_order_id,
                repair_orders.municipality_id,
                repair_orders.hard_hat_id,
                repair_orders.order_date,
                repair_orders.required_date,
                repair_orders.dispatched_date,
                repair_orders.dispatcher_id
             FROM "roads"."repair_orders" AS repair_orders
            LEFT JOIN (SELECT  hard_hats.hard_hat_id,
                hard_hats.last_name,
                hard_hats.first_name,
                hard_hats.title,
                hard_hats.birth_date,
                hard_hats.hire_date,
                hard_hats.address,
                hard_hats.city,
                hard_hats.state,
                hard_hats.postal_code,
                hard_hats.country,
                hard_hats.manager,
                hard_hats.contractor_id
             FROM "roads"."hard_hats" AS hard_hats

            ) AS hard_hat
                    ON repair_orders.hard_hat_id = hard_hat.hard_hat_id
             WHERE  hard_hat.state = 'CA'
            """,
        ),
        # querying source node with filters directly on the node
        (
            "repair_orders",
            [],
            ["repair_orders.order_date='2009-08-14'"],
            """
                SELECT  repair_orders.repair_order_id,
                    repair_orders.municipality_id,
                    repair_orders.hard_hat_id,
                    repair_orders.order_date,
                    repair_orders.required_date,
                    repair_orders.dispatched_date,
                    repair_orders.dispatcher_id
                 FROM "roads"."repair_orders" AS repair_orders
                 WHERE  repair_orders.order_date = '2009-08-14'
                """,
        ),
        # querying transform node with filters on joinable dimension
        (
            "long_events",
            [],
            ["country_dim.events_cnt >= 20"],
            """
            SELECT  event_source.event_id,
                event_source.event_latency,
                event_source.device_id,
                event_source.country
            FROM "logs"."log_events" AS event_source
            LEFT JOIN (SELECT  event_source.country,
                COUNT(DISTINCT event_source.event_id) AS events_cnt
             FROM "logs"."log_events" AS event_source

             GROUP BY  event_source.country) AS country_dim
                    ON event_source.country = country_dim.country
             WHERE  event_source.event_latency > 1000000 AND country_dim.events_cnt >= 20
            """,
        ),
        # querying transform node with filters directly on the node
        (
            "long_events",
            [],
            ["event_source.device_id = 'Android'"],
            """
            SELECT
              event_source.event_id,
              event_source.event_latency,
              event_source.device_id,
              event_source.country
            FROM "logs"."log_events" AS event_source
            WHERE event_source.event_latency > 1000000 AND event_source.device_id = 'Android'
            """,
        ),
        (
            "municipality_dim",
            [],
            ["state_id = 'CA'"],
            """
            SELECT  municipality.municipality_id,
                municipality.contact_name,
                municipality.contact_title,
                municipality.local_region,
                municipality.phone,
                municipality.state_id,
                municipality_municipality_type.municipality_type_id,
                municipality_type.municipality_type_desc
             FROM "roads"."municipality" AS municipality
            LEFT JOIN "roads"."municipality_municipality_type" AS municipality_municipality_type
                    ON municipality.municipality_id = municipality_municipality_type.municipality_id
            LEFT JOIN "roads"."municipality_type" AS municipality_type
                    ON municipality_municipality_type.municipality_type_id
                        = municipality_type.municipality_type_desc
             WHERE  municipality.state_id = 'CA'
            """,
        ),
        (
            "num_repair_orders",
            [],
            [],
            """SELECT  count(repair_orders.repair_order_id) AS num_repair_orders
               FROM "roads"."repair_orders" AS repair_orders
            """,
        ),
        (
            "num_repair_orders",
            ["hard_hat.state"],
            ["repair_orders.dispatcher_id=1", "hard_hat.state='AZ'"],
            """SELECT  count(repair_orders.repair_order_id) AS num_repair_orders,
                 hard_hat.state
               FROM "roads"."repair_orders" AS repair_orders
               LEFT JOIN (SELECT  hard_hats.address,
                 hard_hats.birth_date,
                 hard_hats.city,
                 hard_hats.contractor_id,
                 hard_hats.country,
                 hard_hats.first_name,
                 hard_hats.hard_hat_id,
                 hard_hats.hire_date,
                 hard_hats.last_name,
                 hard_hats.manager,
                 hard_hats.postal_code,
                 hard_hats.state,
                 hard_hats.title
               FROM "roads"."hard_hats" AS hard_hats
            ) AS hard_hat
                    ON repair_orders.hard_hat_id = hard_hat.hard_hat_id
             WHERE  repair_orders.dispatcher_id = 1 AND hard_hat.state = 'AZ'
             GROUP BY  hard_hat.state""",
        ),
        (
            "num_repair_orders",
            [
                "hard_hat.city",
                "hard_hat.last_name",
                "dispatcher.company_name",
                "municipality_dim.local_region",
            ],
            [
                "repair_orders.dispatcher_id=1",
                "hard_hat.state != 'AZ'",
                "dispatcher.phone = '4082021022'",
                "repair_orders.order_date >= '2020-01-01'",
            ],
            """SELECT  count(repair_orders.repair_order_id) AS num_repair_orders,
                hard_hat.city,
                municipality_dim.local_region,
                dispatcher.company_name,
                hard_hat.last_name
             FROM "roads"."repair_orders" AS repair_orders
            LEFT JOIN (SELECT  hard_hats.hard_hat_id,
                hard_hats.last_name,
                hard_hats.first_name,
                hard_hats.title,
                hard_hats.birth_date,
                hard_hats.hire_date,
                hard_hats.address,
                hard_hats.city,
                hard_hats.state,
                hard_hats.postal_code,
                hard_hats.country,
                hard_hats.manager,
                hard_hats.contractor_id
             FROM "roads"."hard_hats" AS hard_hats

            ) AS hard_hat
                    ON repair_orders.hard_hat_id = hard_hat.hard_hat_id
            LEFT JOIN (SELECT  dispatchers.dispatcher_id,
                dispatchers.company_name,
                dispatchers.phone
             FROM "roads"."dispatchers" AS dispatchers

            ) AS dispatcher
                    ON repair_orders.dispatcher_id = dispatcher.dispatcher_id
            LEFT JOIN (SELECT  municipality.municipality_id,
                municipality.contact_name,
                municipality.contact_title,
                municipality.local_region,
                municipality.phone,
                municipality.state_id,
                municipality_municipality_type.municipality_type_id,
                municipality_type.municipality_type_desc
             FROM "roads"."municipality" AS municipality
            LEFT JOIN "roads"."municipality_municipality_type" AS municipality_municipality_type
                    ON municipality.municipality_id = municipality_municipality_type.municipality_id
            LEFT JOIN "roads"."municipality_type" AS municipality_type
                    ON municipality_municipality_type.municipality_type_id
                    = municipality_type.municipality_type_desc
            ) AS municipality_dim
                    ON repair_orders.municipality_id = municipality_dim.municipality_id
             WHERE  repair_orders.dispatcher_id = 1 AND hard_hat.state <> 'AZ'
                AND dispatcher.phone = '4082021022' AND repair_orders.order_date >= '2020-01-01'
             GROUP BY  hard_hat.city, hard_hat.last_name, dispatcher.company_name,
             municipality_dim.local_region""",
        ),
    ],
)
def test_sql_with_filters(
    node_name,
    dimensions,
    filters,
    sql,
    client_with_examples: TestClient,
):
    """
    Test ``GET /sqk/{node_name}/`` with various filters and dimensions.
    The cases to cover include:
    * filters from
    """
    response = client_with_examples.get(
        f"/sql/{node_name}/",
        params={"dimensions": dimensions, "filters": filters},
    )
    data = response.json()
    assert compare_query_strings(data["sql"], sql)
