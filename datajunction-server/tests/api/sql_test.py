"""Tests for the /sql/ endpoint"""
import datetime
from typing import Callable, List, Optional

# pylint: disable=line-too-long,too-many-lines
# pylint: disable=C0302
import duckdb
import pytest
from sqlalchemy.orm import Session
from starlette.testclient import TestClient

from datajunction_server.database.column import Column
from datajunction_server.database.database import Database
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.internal.access.authorization import validate_access
from datajunction_server.models import access
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.sql.parsing.types import StringType
from tests.sql.utils import compare_query_strings


def test_sql(
    session: Session,
    client: TestClient,
) -> None:
    """
    Test ``GET /sql/{name}/``.
    """
    database = Database(name="test", URI="blah://", tables=[])

    source_node = Node(
        name="default.my_table",
        type=NodeType.SOURCE,
        current_version="1",
    )
    source_node_rev = NodeRevision(
        name=source_node.name,
        node=source_node,
        version="1",
        schema_="rev",
        table="my_table",
        columns=[Column(name="one", type=StringType(), order=0)],
        type=NodeType.SOURCE,
    )

    node = Node(name="default.a_metric", type=NodeType.METRIC, current_version="1")
    node_revision = NodeRevision(
        name=node.name,
        node=node,
        version="1",
        query="SELECT COUNT(*) FROM default.my_table",
        type=NodeType.METRIC,
    )
    session.add(database)
    session.add(node_revision)
    session.add(source_node_rev)
    session.commit()

    response = client.get("/sql/default.a_metric/").json()
    assert compare_query_strings(
        response["sql"],
        "SELECT  COUNT(*) default_DOT_a_metric \n FROM rev.my_table AS default_DOT_my_table\n",
    )
    assert response["columns"] == [
        {
            "column": "a_metric",
            "name": "default_DOT_a_metric",
            "node": "default",
            "type": "bigint",
            "semantic_type": None,
            "semantic_entity": "default.a_metric",
        },
    ]
    assert response["dialect"] is None


@pytest.mark.parametrize(
    "groups, node_name, dimensions, filters, sql, columns, rows",
    [
        # querying on source node with filter on joinable dimension
        (
            ["ROADS"],
            "default.repair_orders",
            ["default.hard_hat.state"],
            ["default.hard_hat.state='NY'"],
            """
            SELECT default_DOT_repair_orders.repair_order_id default_DOT_repair_orders_DOT_repair_order_id,
              default_DOT_repair_orders.municipality_id default_DOT_repair_orders_DOT_municipality_id,
              default_DOT_repair_orders.hard_hat_id default_DOT_repair_orders_DOT_hard_hat_id,
              default_DOT_repair_orders.order_date default_DOT_repair_orders_DOT_order_date,
              default_DOT_repair_orders.required_date default_DOT_repair_orders_DOT_required_date,
              default_DOT_repair_orders.dispatched_date default_DOT_repair_orders_DOT_dispatched_date,
              default_DOT_repair_orders.dispatcher_id default_DOT_repair_orders_DOT_dispatcher_id,
              default_DOT_hard_hat.state default_DOT_hard_hat_DOT_state
            FROM roads.repair_orders AS default_DOT_repair_orders
              LEFT JOIN (
                SELECT default_DOT_repair_orders.repair_order_id,
                  default_DOT_repair_orders.municipality_id,
                  default_DOT_repair_orders.hard_hat_id,
                  default_DOT_repair_orders.dispatcher_id
                FROM roads.repair_orders AS default_DOT_repair_orders
              ) AS default_DOT_repair_order ON default_DOT_repair_orders.repair_order_id = default_DOT_repair_order.repair_order_id
              LEFT JOIN (
                SELECT default_DOT_hard_hats.hard_hat_id,
                  default_DOT_hard_hats.state
                FROM roads.hard_hats AS default_DOT_hard_hats
              ) AS default_DOT_hard_hat ON default_DOT_repair_order.hard_hat_id = default_DOT_hard_hat.hard_hat_id
            WHERE default_DOT_hard_hat.state = 'NY'
            """,
            [
                {
                    "column": "repair_order_id",
                    "name": "default_DOT_repair_orders_DOT_repair_order_id",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.repair_order_id",
                    "semantic_type": None,
                    "type": "int",
                },
                {
                    "column": "municipality_id",
                    "name": "default_DOT_repair_orders_DOT_municipality_id",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.municipality_id",
                    "semantic_type": None,
                    "type": "string",
                },
                {
                    "column": "hard_hat_id",
                    "name": "default_DOT_repair_orders_DOT_hard_hat_id",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.hard_hat_id",
                    "semantic_type": None,
                    "type": "int",
                },
                {
                    "column": "order_date",
                    "name": "default_DOT_repair_orders_DOT_order_date",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.order_date",
                    "semantic_type": None,
                    "type": "timestamp",
                },
                {
                    "column": "required_date",
                    "name": "default_DOT_repair_orders_DOT_required_date",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.required_date",
                    "semantic_type": None,
                    "type": "timestamp",
                },
                {
                    "column": "dispatched_date",
                    "name": "default_DOT_repair_orders_DOT_dispatched_date",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.dispatched_date",
                    "semantic_type": None,
                    "type": "timestamp",
                },
                {
                    "column": "dispatcher_id",
                    "name": "default_DOT_repair_orders_DOT_dispatcher_id",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.dispatcher_id",
                    "semantic_type": None,
                    "type": "int",
                },
                {
                    "column": "state",
                    "name": "default_DOT_hard_hat_DOT_state",
                    "node": "default.hard_hat",
                    "semantic_entity": "default.hard_hat.state",
                    "semantic_type": None,
                    "type": "string",
                },
            ],
            [
                [
                    10021,
                    "Philadelphia",
                    7,
                    "2007-05-10",
                    "2009-08-27",
                    "2007-12-01",
                    3,
                    "NY",
                ],
            ],
        ),
        # querying source node with filters directly on the node
        (
            ["ROADS"],
            "default.repair_orders",
            [],
            ["default.repair_orders.order_date='2009-08-14'"],
            """
              SELECT  default_DOT_repair_orders.repair_order_id default_DOT_repair_orders_DOT_repair_order_id,
                default_DOT_repair_orders.municipality_id default_DOT_repair_orders_DOT_municipality_id,
                default_DOT_repair_orders.hard_hat_id default_DOT_repair_orders_DOT_hard_hat_id,
                default_DOT_repair_orders.order_date default_DOT_repair_orders_DOT_order_date,
                default_DOT_repair_orders.required_date default_DOT_repair_orders_DOT_required_date,
                default_DOT_repair_orders.dispatched_date default_DOT_repair_orders_DOT_dispatched_date,
                default_DOT_repair_orders.dispatcher_id default_DOT_repair_orders_DOT_dispatcher_id
              FROM roads.repair_orders AS default_DOT_repair_orders
              WHERE  default_DOT_repair_orders.order_date = '2009-08-14'
            """,
            [
                {
                    "column": "repair_order_id",
                    "name": "default_DOT_repair_orders_DOT_repair_order_id",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.repair_order_id",
                    "semantic_type": None,
                    "type": "int",
                },
                {
                    "column": "municipality_id",
                    "name": "default_DOT_repair_orders_DOT_municipality_id",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.municipality_id",
                    "semantic_type": None,
                    "type": "string",
                },
                {
                    "column": "hard_hat_id",
                    "name": "default_DOT_repair_orders_DOT_hard_hat_id",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.hard_hat_id",
                    "semantic_type": None,
                    "type": "int",
                },
                {
                    "column": "order_date",
                    "name": "default_DOT_repair_orders_DOT_order_date",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.order_date",
                    "semantic_type": None,
                    "type": "timestamp",
                },
                {
                    "column": "required_date",
                    "name": "default_DOT_repair_orders_DOT_required_date",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.required_date",
                    "semantic_type": None,
                    "type": "timestamp",
                },
                {
                    "column": "dispatched_date",
                    "name": "default_DOT_repair_orders_DOT_dispatched_date",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.dispatched_date",
                    "semantic_type": None,
                    "type": "timestamp",
                },
                {
                    "column": "dispatcher_id",
                    "name": "default_DOT_repair_orders_DOT_dispatcher_id",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.dispatcher_id",
                    "semantic_type": None,
                    "type": "int",
                },
            ],
            [],
        ),
        # querying transform node with filters on joinable dimension
        (
            ["EVENT"],
            "default.long_events",
            [],
            ["default.country_dim.events_cnt >= 20"],
            """
            SELECT  default_DOT_long_events.event_id default_DOT_long_events_DOT_event_id,
    default_DOT_long_events.event_latency default_DOT_long_events_DOT_event_latency,
    default_DOT_long_events.device_id default_DOT_long_events_DOT_device_id,
    default_DOT_long_events.country default_DOT_long_events_DOT_country,
    default_DOT_country_dim.events_cnt default_DOT_country_dim_DOT_events_cnt
 FROM (SELECT  default_DOT_event_source.event_id,
    default_DOT_event_source.event_latency,
    default_DOT_event_source.device_id,
    default_DOT_event_source.country
 FROM logs.log_events AS default_DOT_event_source
 WHERE  default_DOT_event_source.event_latency > 1000000)
 AS default_DOT_long_events LEFT JOIN (SELECT  default_DOT_event_source.country,
    COUNT( DISTINCT default_DOT_event_source.event_id) AS events_cnt
 FROM logs.log_events AS default_DOT_event_source
 GROUP BY  default_DOT_event_source.country)
 AS default_DOT_country_dim ON default_DOT_long_events.country = default_DOT_country_dim.country
 WHERE  default_DOT_country_dim.events_cnt >= 20
            """,
            [
                {
                    "column": "event_id",
                    "name": "default_DOT_long_events_DOT_event_id",
                    "node": "default.long_events",
                    "semantic_entity": "default.long_events.event_id",
                    "semantic_type": None,
                    "type": "int",
                },
                {
                    "column": "event_latency",
                    "name": "default_DOT_long_events_DOT_event_latency",
                    "node": "default.long_events",
                    "semantic_entity": "default.long_events.event_latency",
                    "semantic_type": None,
                    "type": "int",
                },
                {
                    "column": "device_id",
                    "name": "default_DOT_long_events_DOT_device_id",
                    "node": "default.long_events",
                    "semantic_entity": "default.long_events.device_id",
                    "semantic_type": None,
                    "type": "int",
                },
                {
                    "column": "country",
                    "name": "default_DOT_long_events_DOT_country",
                    "node": "default.long_events",
                    "semantic_entity": "default.long_events.country",
                    "semantic_type": None,
                    "type": "string",
                },
                {
                    "column": "events_cnt",
                    "name": "default_DOT_country_dim_DOT_events_cnt",
                    "node": "default.country_dim",
                    "semantic_entity": "default.country_dim.events_cnt",
                    "semantic_type": None,
                    "type": "bigint",
                },
            ],
            [],
        ),
        # querying transform node with filters on dimension PK column
        # * it should not join in the dimension but use the FK column on transform instead
        # * it should push down the filter to the parent transform
        (
            ["EVENT"],
            "default.long_events",
            [],
            [
                "default.country_dim.country = 'ABCD'",
            ],  # country is PK of default.country_dim
            """
                SELECT  default_DOT_long_events.event_id default_DOT_long_events_DOT_event_id,
                    default_DOT_long_events.event_latency default_DOT_long_events_DOT_event_latency,
                    default_DOT_long_events.device_id default_DOT_long_events_DOT_device_id,
                    default_DOT_long_events.country default_DOT_country_dim_DOT_country
                FROM (SELECT  default_DOT_event_source.event_id,
                    default_DOT_event_source.event_latency,
                    default_DOT_event_source.device_id,
                    default_DOT_event_source.country
                FROM logs.log_events AS default_DOT_event_source
                WHERE  default_DOT_event_source.event_latency > 1000000 AND default_DOT_event_source.country = 'ABCD')
                AS default_DOT_long_events
                WHERE  default_DOT_long_events.country = 'ABCD'
                """,
            [
                {
                    "column": "event_id",
                    "name": "default_DOT_long_events_DOT_event_id",
                    "node": "default.long_events",
                    "type": "int",
                    "semantic_type": None,
                    "semantic_entity": "default.long_events.event_id",
                },
                {
                    "column": "event_latency",
                    "name": "default_DOT_long_events_DOT_event_latency",
                    "node": "default.long_events",
                    "type": "int",
                    "semantic_type": None,
                    "semantic_entity": "default.long_events.event_latency",
                },
                {
                    "column": "device_id",
                    "name": "default_DOT_long_events_DOT_device_id",
                    "node": "default.long_events",
                    "type": "int",
                    "semantic_type": None,
                    "semantic_entity": "default.long_events.device_id",
                },
                {
                    "column": "country",
                    "name": "default_DOT_country_dim_DOT_country",
                    "node": "default.country_dim",
                    "type": "string",
                    "semantic_type": None,
                    "semantic_entity": "default.country_dim.country",
                },
            ],
            [],
        ),
        # querying transform node with filters directly on the node
        (
            ["EVENT"],
            "default.long_events",
            [],
            ["default.long_events.device_id = 'Android'"],
            """
            SELECT  default_DOT_long_events.event_id default_DOT_long_events_DOT_event_id,
                default_DOT_long_events.event_latency default_DOT_long_events_DOT_event_latency,
                default_DOT_long_events.device_id default_DOT_long_events_DOT_device_id,
                default_DOT_long_events.country default_DOT_long_events_DOT_country
             FROM (SELECT  default_DOT_event_source.event_id,
                default_DOT_event_source.event_latency,
                default_DOT_event_source.device_id,
                default_DOT_event_source.country
             FROM logs.log_events AS default_DOT_event_source
             WHERE  default_DOT_event_source.event_latency > 1000000)
             AS default_DOT_long_events
             WHERE  default_DOT_long_events.device_id = 'Android'
            """,
            [
                {
                    "column": "event_id",
                    "name": "default_DOT_long_events_DOT_event_id",
                    "node": "default.long_events",
                    "type": "int",
                    "semantic_type": None,
                    "semantic_entity": "default.long_events.event_id",
                },
                {
                    "column": "event_latency",
                    "name": "default_DOT_long_events_DOT_event_latency",
                    "node": "default.long_events",
                    "type": "int",
                    "semantic_type": None,
                    "semantic_entity": "default.long_events.event_latency",
                },
                {
                    "column": "device_id",
                    "name": "default_DOT_long_events_DOT_device_id",
                    "node": "default.long_events",
                    "type": "int",
                    "semantic_type": None,
                    "semantic_entity": "default.long_events.device_id",
                },
                {
                    "column": "country",
                    "name": "default_DOT_long_events_DOT_country",
                    "node": "default.long_events",
                    "type": "string",
                    "semantic_type": None,
                    "semantic_entity": "default.long_events.country",
                },
            ],
            [],
        ),
        (
            ["ROADS"],
            "default.municipality",
            [],
            ["default.municipality.state_id = 'CA'"],
            """
            SELECT
              default_DOT_municipality.municipality_id default_DOT_municipality_DOT_municipality_id,
              default_DOT_municipality.contact_name default_DOT_municipality_DOT_contact_name,
              default_DOT_municipality.contact_title default_DOT_municipality_DOT_contact_title,
              default_DOT_municipality.local_region default_DOT_municipality_DOT_local_region,
              default_DOT_municipality.phone default_DOT_municipality_DOT_phone,
              default_DOT_municipality.state_id default_DOT_municipality_DOT_state_id
            FROM roads.municipality AS default_DOT_municipality
            WHERE  default_DOT_municipality.state_id = 'CA'
            """,
            [
                {
                    "column": "municipality_id",
                    "name": "default_DOT_municipality_DOT_municipality_id",
                    "node": "default.municipality",
                    "semantic_entity": "default.municipality.municipality_id",
                    "semantic_type": None,
                    "type": "string",
                },
                {
                    "column": "contact_name",
                    "name": "default_DOT_municipality_DOT_contact_name",
                    "node": "default.municipality",
                    "semantic_entity": "default.municipality.contact_name",
                    "semantic_type": None,
                    "type": "string",
                },
                {
                    "column": "contact_title",
                    "name": "default_DOT_municipality_DOT_contact_title",
                    "node": "default.municipality",
                    "semantic_entity": "default.municipality.contact_title",
                    "semantic_type": None,
                    "type": "string",
                },
                {
                    "column": "local_region",
                    "name": "default_DOT_municipality_DOT_local_region",
                    "node": "default.municipality",
                    "semantic_entity": "default.municipality.local_region",
                    "semantic_type": None,
                    "type": "string",
                },
                {
                    "column": "phone",
                    "name": "default_DOT_municipality_DOT_phone",
                    "node": "default.municipality",
                    "semantic_entity": "default.municipality.phone",
                    "semantic_type": None,
                    "type": "string",
                },
                {
                    "column": "state_id",
                    "name": "default_DOT_municipality_DOT_state_id",
                    "node": "default.municipality",
                    "semantic_entity": "default.municipality.state_id",
                    "semantic_type": None,
                    "type": "int",
                },
            ],
            [],
        ),
        (
            ["ROADS"],
            "default.num_repair_orders",
            [],
            [],
            """SELECT  count(default_DOT_repair_orders_fact.repair_order_id) default_DOT_num_repair_orders
 FROM (SELECT  default_DOT_repair_orders.repair_order_id,
    default_DOT_repair_orders.municipality_id,
    default_DOT_repair_orders.hard_hat_id,
    default_DOT_repair_orders.dispatcher_id,
    default_DOT_repair_orders.order_date,
    default_DOT_repair_orders.dispatched_date,
    default_DOT_repair_orders.required_date,
    default_DOT_repair_order_details.discount,
    default_DOT_repair_order_details.price,
    default_DOT_repair_order_details.quantity,
    default_DOT_repair_order_details.repair_type_id,
    default_DOT_repair_order_details.price * default_DOT_repair_order_details.quantity AS total_repair_cost,
    default_DOT_repair_orders.dispatched_date - default_DOT_repair_orders.order_date AS time_to_dispatch,
    default_DOT_repair_orders.dispatched_date - default_DOT_repair_orders.required_date AS dispatch_delay
 FROM roads.repair_orders AS default_DOT_repair_orders JOIN roads.repair_order_details AS default_DOT_repair_order_details ON default_DOT_repair_orders.repair_order_id = default_DOT_repair_order_details.repair_order_id)
 AS default_DOT_repair_orders_fact""",
            [
                {
                    "column": "default_DOT_num_repair_orders",
                    "name": "default_DOT_num_repair_orders",
                    "node": "default.num_repair_orders",
                    "type": "bigint",
                    "semantic_type": None,
                    "semantic_entity": "default.num_repair_orders.default_DOT_num_repair_orders",
                },
            ],
            [[25]],
        ),
        (
            ["ROADS"],
            "default.num_repair_orders",
            ["default.hard_hat.state"],
            [
                "default.repair_orders_fact.dispatcher_id=1",
                "default.hard_hat.state='AZ'",
            ],
            """
            SELECT  count(default_DOT_repair_orders_fact.repair_order_id) default_DOT_num_repair_orders,
                default_DOT_hard_hat.state default_DOT_hard_hat_DOT_state
             FROM (SELECT  default_DOT_repair_orders.repair_order_id,
                default_DOT_repair_orders.municipality_id,
                default_DOT_repair_orders.hard_hat_id,
                default_DOT_repair_orders.dispatcher_id,
                default_DOT_repair_orders.order_date,
                default_DOT_repair_orders.dispatched_date,
                default_DOT_repair_orders.required_date,
                default_DOT_repair_order_details.discount,
                default_DOT_repair_order_details.price,
                default_DOT_repair_order_details.quantity,
                default_DOT_repair_order_details.repair_type_id,
                default_DOT_repair_order_details.price * default_DOT_repair_order_details.quantity AS total_repair_cost,
                default_DOT_repair_orders.dispatched_date - default_DOT_repair_orders.order_date AS time_to_dispatch,
                default_DOT_repair_orders.dispatched_date - default_DOT_repair_orders.required_date AS dispatch_delay
             FROM roads.repair_orders AS default_DOT_repair_orders JOIN roads.repair_order_details AS default_DOT_repair_order_details ON default_DOT_repair_orders.repair_order_id = default_DOT_repair_order_details.repair_order_id
             WHERE  default_DOT_repair_orders.dispatcher_id = 1)
             AS default_DOT_repair_orders_fact LEFT JOIN (SELECT  default_DOT_hard_hats.hard_hat_id,
                default_DOT_hard_hats.state
             FROM roads.hard_hats AS default_DOT_hard_hats)
             AS default_DOT_hard_hat ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
             WHERE  default_DOT_repair_orders_fact.dispatcher_id = 1 AND default_DOT_hard_hat.state = 'AZ'
             GROUP BY  default_DOT_hard_hat.state
            """,
            [
                {
                    "column": "default_DOT_num_repair_orders",
                    "name": "default_DOT_num_repair_orders",
                    "node": "default.num_repair_orders",
                    "type": "bigint",
                    "semantic_type": None,
                    "semantic_entity": "default.num_repair_orders.default_DOT_num_repair_orders",
                },
                {
                    "column": "state",
                    "name": "default_DOT_hard_hat_DOT_state",
                    "node": "default.hard_hat",
                    "type": "string",
                    "semantic_type": None,
                    "semantic_entity": "default.hard_hat.state",
                },
            ],
            [],
        ),
        (
            ["ROADS"],
            "default.num_repair_orders",
            [
                "default.hard_hat.city",
                "default.hard_hat.last_name",
                "default.dispatcher.company_name",
                "default.municipality_dim.local_region",
            ],
            [
                "default.dispatcher.dispatcher_id=1",
                "default.hard_hat.state != 'AZ'",
                "default.dispatcher.phone = '4082021022'",
                "default.repair_orders_fact.order_date >= '2020-01-01'",
            ],
            """
            SELECT  count(default_DOT_repair_orders_fact.repair_order_id) default_DOT_num_repair_orders,
                default_DOT_hard_hat.city default_DOT_hard_hat_DOT_city,
                default_DOT_hard_hat.last_name default_DOT_hard_hat_DOT_last_name,
                default_DOT_dispatcher.company_name default_DOT_dispatcher_DOT_company_name,
                default_DOT_municipality_dim.local_region default_DOT_municipality_dim_DOT_local_region
             FROM (SELECT  default_DOT_repair_orders.repair_order_id,
                default_DOT_repair_orders.municipality_id,
                default_DOT_repair_orders.hard_hat_id,
                default_DOT_repair_orders.dispatcher_id,
                default_DOT_repair_orders.order_date,
                default_DOT_repair_orders.dispatched_date,
                default_DOT_repair_orders.required_date,
                default_DOT_repair_order_details.discount,
                default_DOT_repair_order_details.price,
                default_DOT_repair_order_details.quantity,
                default_DOT_repair_order_details.repair_type_id,
                default_DOT_repair_order_details.price * default_DOT_repair_order_details.quantity AS total_repair_cost,
                default_DOT_repair_orders.dispatched_date - default_DOT_repair_orders.order_date AS time_to_dispatch,
                default_DOT_repair_orders.dispatched_date - default_DOT_repair_orders.required_date AS dispatch_delay
             FROM roads.repair_orders AS default_DOT_repair_orders JOIN roads.repair_order_details AS default_DOT_repair_order_details ON default_DOT_repair_orders.repair_order_id = default_DOT_repair_order_details.repair_order_id
             WHERE  default_DOT_repair_orders.dispatcher_id = 1)
             AS default_DOT_repair_orders_fact LEFT JOIN (SELECT  default_DOT_dispatchers.dispatcher_id,
                default_DOT_dispatchers.company_name,
                default_DOT_dispatchers.phone
             FROM roads.dispatchers AS default_DOT_dispatchers)
             AS default_DOT_dispatcher ON default_DOT_repair_orders_fact.dispatcher_id = default_DOT_dispatcher.dispatcher_id
            LEFT JOIN (SELECT  default_DOT_hard_hats.hard_hat_id,
                default_DOT_hard_hats.last_name,
                default_DOT_hard_hats.city,
                default_DOT_hard_hats.state
             FROM roads.hard_hats AS default_DOT_hard_hats)
             AS default_DOT_hard_hat ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
            LEFT JOIN (SELECT  default_DOT_municipality.municipality_id AS municipality_id,
                default_DOT_municipality.local_region
             FROM roads.municipality AS default_DOT_municipality LEFT  JOIN roads.municipality_municipality_type AS default_DOT_municipality_municipality_type ON default_DOT_municipality.municipality_id = default_DOT_municipality_municipality_type.municipality_id
            LEFT  JOIN roads.municipality_type AS default_DOT_municipality_type ON default_DOT_municipality_municipality_type.municipality_type_id = default_DOT_municipality_type.municipality_type_desc)
             AS default_DOT_municipality_dim ON default_DOT_repair_orders_fact.municipality_id = default_DOT_municipality_dim.municipality_id
             WHERE  default_DOT_repair_orders_fact.dispatcher_id = 1 AND default_DOT_hard_hat.state != 'AZ' AND default_DOT_dispatcher.phone = '4082021022' AND default_DOT_repair_orders_fact.order_date >= '2020-01-01'
             GROUP BY  default_DOT_hard_hat.city, default_DOT_hard_hat.last_name, default_DOT_dispatcher.company_name, default_DOT_municipality_dim.local_region
            """,
            [
                {
                    "name": "default_DOT_num_repair_orders",
                    "column": "default_DOT_num_repair_orders",
                    "node": "default.num_repair_orders",
                    "type": "bigint",
                    "semantic_type": None,
                    "semantic_entity": "default.num_repair_orders.default_DOT_num_repair_orders",
                },
                {
                    "name": "default_DOT_hard_hat_DOT_city",
                    "column": "city",
                    "node": "default.hard_hat",
                    "type": "string",
                    "semantic_type": None,
                    "semantic_entity": "default.hard_hat.city",
                },
                {
                    "name": "default_DOT_hard_hat_DOT_last_name",
                    "column": "last_name",
                    "node": "default.hard_hat",
                    "type": "string",
                    "semantic_type": None,
                    "semantic_entity": "default.hard_hat.last_name",
                },
                {
                    "name": "default_DOT_dispatcher_DOT_company_name",
                    "column": "company_name",
                    "node": "default.dispatcher",
                    "type": "string",
                    "semantic_type": None,
                    "semantic_entity": "default.dispatcher.company_name",
                },
                {
                    "name": "default_DOT_municipality_dim_DOT_local_region",
                    "column": "local_region",
                    "node": "default.municipality_dim",
                    "type": "string",
                    "semantic_type": None,
                    "semantic_entity": "default.municipality_dim.local_region",
                },
            ],
            [],
        ),
        # metric with second-order dimension
        (
            ["ROADS"],
            "default.avg_repair_price",
            ["default.hard_hat.city"],
            [],
            """
            SELECT  avg(default_DOT_repair_orders_fact.price) default_DOT_avg_repair_price,
                default_DOT_hard_hat.city default_DOT_hard_hat_DOT_city
             FROM (SELECT  default_DOT_repair_orders.repair_order_id,
                default_DOT_repair_orders.municipality_id,
                default_DOT_repair_orders.hard_hat_id,
                default_DOT_repair_orders.dispatcher_id,
                default_DOT_repair_orders.order_date,
                default_DOT_repair_orders.dispatched_date,
                default_DOT_repair_orders.required_date,
                default_DOT_repair_order_details.discount,
                default_DOT_repair_order_details.price,
                default_DOT_repair_order_details.quantity,
                default_DOT_repair_order_details.repair_type_id,
                default_DOT_repair_order_details.price * default_DOT_repair_order_details.quantity AS total_repair_cost,
                default_DOT_repair_orders.dispatched_date - default_DOT_repair_orders.order_date AS time_to_dispatch,
                default_DOT_repair_orders.dispatched_date - default_DOT_repair_orders.required_date AS dispatch_delay
             FROM roads.repair_orders AS default_DOT_repair_orders JOIN roads.repair_order_details AS default_DOT_repair_order_details ON default_DOT_repair_orders.repair_order_id = default_DOT_repair_order_details.repair_order_id)
             AS default_DOT_repair_orders_fact LEFT JOIN (SELECT  default_DOT_hard_hats.hard_hat_id,
                default_DOT_hard_hats.city,
                default_DOT_hard_hats.state
             FROM roads.hard_hats AS default_DOT_hard_hats)
             AS default_DOT_hard_hat ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
             GROUP BY  default_DOT_hard_hat.city
            """,
            [
                {
                    "column": "default_DOT_avg_repair_price",
                    "name": "default_DOT_avg_repair_price",
                    "node": "default.avg_repair_price",
                    "type": "double",
                    "semantic_type": None,
                    "semantic_entity": "default.avg_repair_price.default_DOT_avg_repair_price",
                },
                {
                    "column": "city",
                    "name": "default_DOT_hard_hat_DOT_city",
                    "node": "default.hard_hat",
                    "type": "string",
                    "semantic_type": None,
                    "semantic_entity": "default.hard_hat.city",
                },
            ],
            [
                [54672.75, "Jersey City"],
                [76555.33333333333, "Billerica"],
                [64190.6, "Southgate"],
                [65682.0, "Phoenix"],
                [54083.5, "Southampton"],
                [65595.66666666667, "Powder Springs"],
                [39301.5, "Middletown"],
                [70418.0, "Muskogee"],
                [53374.0, "Niagara Falls"],
            ],
        ),
        # metric with multiple nth order dimensions that can share some of the joins
        (
            ["ROADS"],
            "default.avg_repair_price",
            ["default.hard_hat.city", "default.dispatcher.company_name"],
            [],
            """
              SELECT  avg(default_DOT_repair_orders_fact.price) default_DOT_avg_repair_price,
                default_DOT_hard_hat.city default_DOT_hard_hat_DOT_city,
                default_DOT_dispatcher.company_name default_DOT_dispatcher_DOT_company_name
             FROM (SELECT  default_DOT_repair_orders.repair_order_id,
                default_DOT_repair_orders.municipality_id,
                default_DOT_repair_orders.hard_hat_id,
                default_DOT_repair_orders.dispatcher_id,
                default_DOT_repair_orders.order_date,
                default_DOT_repair_orders.dispatched_date,
                default_DOT_repair_orders.required_date,
                default_DOT_repair_order_details.discount,
                default_DOT_repair_order_details.price,
                default_DOT_repair_order_details.quantity,
                default_DOT_repair_order_details.repair_type_id,
                default_DOT_repair_order_details.price * default_DOT_repair_order_details.quantity AS total_repair_cost,
                default_DOT_repair_orders.dispatched_date - default_DOT_repair_orders.order_date AS time_to_dispatch,
                default_DOT_repair_orders.dispatched_date - default_DOT_repair_orders.required_date AS dispatch_delay
             FROM roads.repair_orders AS default_DOT_repair_orders JOIN roads.repair_order_details AS default_DOT_repair_order_details ON default_DOT_repair_orders.repair_order_id = default_DOT_repair_order_details.repair_order_id)
             AS default_DOT_repair_orders_fact LEFT JOIN (SELECT  default_DOT_dispatchers.dispatcher_id,
                default_DOT_dispatchers.company_name
             FROM roads.dispatchers AS default_DOT_dispatchers)
             AS default_DOT_dispatcher ON default_DOT_repair_orders_fact.dispatcher_id = default_DOT_dispatcher.dispatcher_id
            LEFT JOIN (SELECT  default_DOT_hard_hats.hard_hat_id,
                default_DOT_hard_hats.city,
                default_DOT_hard_hats.state
             FROM roads.hard_hats AS default_DOT_hard_hats)
             AS default_DOT_hard_hat ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
             GROUP BY  default_DOT_hard_hat.city, default_DOT_dispatcher.company_name
            """,
            [
                {
                    "column": "default_DOT_avg_repair_price",
                    "name": "default_DOT_avg_repair_price",
                    "node": "default.avg_repair_price",
                    "type": "double",
                    "semantic_type": None,
                    "semantic_entity": "default.avg_repair_price.default_DOT_avg_repair_price",
                },
                {
                    "column": "city",
                    "name": "default_DOT_hard_hat_DOT_city",
                    "node": "default.hard_hat",
                    "type": "string",
                    "semantic_type": None,
                    "semantic_entity": "default.hard_hat.city",
                },
                {
                    "column": "company_name",
                    "name": "default_DOT_dispatcher_DOT_company_name",
                    "node": "default.dispatcher",
                    "type": "string",
                    "semantic_type": None,
                    "semantic_entity": "default.dispatcher.company_name",
                },
            ],
            [
                [63708.0, "Jersey City", "Federal Roads Group"],
                [67253.0, "Billerica", "Pothole Pete"],
                [57332.5, "Southgate", "Asphalts R Us"],
                [51661.0, "Jersey City", "Pothole Pete"],
                [76463.0, "Phoenix", "Asphalts R Us"],
                [81206.5, "Billerica", "Asphalts R Us"],
                [63918.0, "Southampton", "Asphalts R Us"],
                [59499.5, "Southgate", "Federal Roads Group"],
                [27222.0, "Southampton", "Federal Roads Group"],
                [62597.0, "Southampton", "Pothole Pete"],
                [54901.0, "Phoenix", "Federal Roads Group"],
                [66929.5, "Powder Springs", "Asphalts R Us"],
                [39301.5, "Middletown", "Federal Roads Group"],
                [70418.0, "Muskogee", "Federal Roads Group"],
                [62928.0, "Powder Springs", "Pothole Pete"],
                [53374.0, "Niagara Falls", "Federal Roads Group"],
                [87289.0, "Southgate", "Pothole Pete"],
            ],
        ),
        # dimension with aliased join key should just use the alias directly
        (
            ["ROADS"],
            "default.num_repair_orders",
            ["default.us_state.state_short"],
            [],
            """
            SELECT  count(default_DOT_repair_orders_fact.repair_order_id) default_DOT_num_repair_orders,
                default_DOT_us_state.state_short default_DOT_us_state_DOT_state_short
             FROM (SELECT  default_DOT_repair_orders.repair_order_id,
                default_DOT_repair_orders.municipality_id,
                default_DOT_repair_orders.hard_hat_id,
                default_DOT_repair_orders.dispatcher_id,
                default_DOT_repair_orders.order_date,
                default_DOT_repair_orders.dispatched_date,
                default_DOT_repair_orders.required_date,
                default_DOT_repair_order_details.discount,
                default_DOT_repair_order_details.price,
                default_DOT_repair_order_details.quantity,
                default_DOT_repair_order_details.repair_type_id,
                default_DOT_repair_order_details.price * default_DOT_repair_order_details.quantity AS total_repair_cost,
                default_DOT_repair_orders.dispatched_date - default_DOT_repair_orders.order_date AS time_to_dispatch,
                default_DOT_repair_orders.dispatched_date - default_DOT_repair_orders.required_date AS dispatch_delay
             FROM roads.repair_orders AS default_DOT_repair_orders JOIN roads.repair_order_details AS default_DOT_repair_order_details ON default_DOT_repair_orders.repair_order_id = default_DOT_repair_order_details.repair_order_id)
             AS default_DOT_repair_orders_fact LEFT JOIN (SELECT  default_DOT_hard_hats.hard_hat_id,
                default_DOT_hard_hats.state
             FROM roads.hard_hats AS default_DOT_hard_hats)
             AS default_DOT_hard_hat ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
            LEFT JOIN (SELECT  default_DOT_us_states.state_abbr AS state_short
             FROM roads.us_states AS default_DOT_us_states)
             AS default_DOT_us_state ON default_DOT_hard_hat.state = default_DOT_us_state.state_short
             GROUP BY  default_DOT_us_state.state_short
            """,
            [
                {
                    "column": "default_DOT_num_repair_orders",
                    "name": "default_DOT_num_repair_orders",
                    "node": "default.num_repair_orders",
                    "type": "bigint",
                    "semantic_type": None,
                    "semantic_entity": "default.num_repair_orders.default_DOT_num_repair_orders",
                },
                {
                    "column": "state_short",
                    "name": "default_DOT_us_state_DOT_state_short",
                    "node": "default.us_state",
                    "type": "string",
                    "semantic_type": None,
                    "semantic_entity": "default.us_state.state_short",
                },
            ],
            [
                [2, "AZ"],
                [2, "CT"],
                [3, "GA"],
                [3, "MA"],
                [5, "MI"],
                [4, "NJ"],
                [1, "NY"],
                [1, "OK"],
                [4, "PA"],
            ],
        ),
        # querying on source node while pulling in joinable dimension
        # (should not group by the dimension attribute)
        (
            ["ROADS"],
            "default.repair_orders",
            ["default.hard_hat.state"],
            ["default.hard_hat.state='NY'"],
            """
            SELECT  default_DOT_repair_orders.repair_order_id default_DOT_repair_orders_DOT_repair_order_id,
                default_DOT_repair_orders.municipality_id default_DOT_repair_orders_DOT_municipality_id,
                default_DOT_repair_orders.hard_hat_id default_DOT_repair_orders_DOT_hard_hat_id,
                default_DOT_repair_orders.order_date default_DOT_repair_orders_DOT_order_date,
                default_DOT_repair_orders.required_date default_DOT_repair_orders_DOT_required_date,
                default_DOT_repair_orders.dispatched_date default_DOT_repair_orders_DOT_dispatched_date,
                default_DOT_repair_orders.dispatcher_id default_DOT_repair_orders_DOT_dispatcher_id,
                default_DOT_hard_hat.state default_DOT_hard_hat_DOT_state
             FROM roads.repair_orders AS default_DOT_repair_orders LEFT JOIN (SELECT  default_DOT_repair_orders.repair_order_id,
                default_DOT_repair_orders.municipality_id,
                default_DOT_repair_orders.hard_hat_id,
                default_DOT_repair_orders.dispatcher_id
             FROM roads.repair_orders AS default_DOT_repair_orders)
             AS default_DOT_repair_order ON default_DOT_repair_orders.repair_order_id = default_DOT_repair_order.repair_order_id
            LEFT JOIN (SELECT  default_DOT_hard_hats.hard_hat_id,
                default_DOT_hard_hats.state
             FROM roads.hard_hats AS default_DOT_hard_hats)
             AS default_DOT_hard_hat ON default_DOT_repair_order.hard_hat_id = default_DOT_hard_hat.hard_hat_id
             WHERE  default_DOT_hard_hat.state = 'NY'
            """,
            [
                {
                    "column": "repair_order_id",
                    "name": "default_DOT_repair_orders_DOT_repair_order_id",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.repair_order_id",
                    "semantic_type": None,
                    "type": "int",
                },
                {
                    "column": "municipality_id",
                    "name": "default_DOT_repair_orders_DOT_municipality_id",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.municipality_id",
                    "semantic_type": None,
                    "type": "string",
                },
                {
                    "column": "hard_hat_id",
                    "name": "default_DOT_repair_orders_DOT_hard_hat_id",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.hard_hat_id",
                    "semantic_type": None,
                    "type": "int",
                },
                {
                    "column": "order_date",
                    "name": "default_DOT_repair_orders_DOT_order_date",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.order_date",
                    "semantic_type": None,
                    "type": "timestamp",
                },
                {
                    "column": "required_date",
                    "name": "default_DOT_repair_orders_DOT_required_date",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.required_date",
                    "semantic_type": None,
                    "type": "timestamp",
                },
                {
                    "column": "dispatched_date",
                    "name": "default_DOT_repair_orders_DOT_dispatched_date",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.dispatched_date",
                    "semantic_type": None,
                    "type": "timestamp",
                },
                {
                    "column": "dispatcher_id",
                    "name": "default_DOT_repair_orders_DOT_dispatcher_id",
                    "node": "default.repair_orders",
                    "semantic_entity": "default.repair_orders.dispatcher_id",
                    "semantic_type": None,
                    "type": "int",
                },
                {
                    "column": "state",
                    "name": "default_DOT_hard_hat_DOT_state",
                    "node": "default.hard_hat",
                    "semantic_entity": "default.hard_hat.state",
                    "semantic_type": None,
                    "type": "string",
                },
            ],
            [
                [
                    10021,
                    "Philadelphia",
                    7,
                    "2007-05-10",
                    "2009-08-27",
                    "2007-12-01",
                    3,
                    "NY",
                ],
            ],
        ),
    ],
)
def test_sql_with_filters(  # pylint: disable=too-many-arguments
    groups: List[str],
    node_name,
    dimensions,
    filters,
    sql,
    columns,
    rows,
    client_with_query_service_example_loader: Callable[
        [Optional[List[str]]],
        TestClient,
    ],
):
    """
    Test ``GET /sql/{node_name}/`` with various filters and dimensions.
    """
    custom_client = client_with_query_service_example_loader(groups)
    response = custom_client.get(
        f"/sql/{node_name}/",
        params={"dimensions": dimensions, "filters": filters},
    )
    data = response.json()
    assert str(parse(str(data["sql"]))) == str(parse(str(sql)))
    assert data["columns"] == columns

    # Run the query against local duckdb file if it's part of the roads model
    if "ROADS" in groups:
        response = custom_client.get(
            f"/data/{node_name}/",
            params={"dimensions": dimensions, "filters": filters},
        )
        data = response.json()
        assert data["results"][0]["rows"] == rows


@pytest.mark.parametrize(
    "node_name, dimensions, filters, orderby, sql",
    [
        # querying on source node with filter on joinable dimension
        (
            "foo.bar.repair_orders",
            [],
            ["foo.bar.hard_hat.state='CA'"],
            [],
            """
            SELECT  foo_DOT_bar_DOT_repair_orders.repair_order_id foo_DOT_bar_DOT_repair_orders_DOT_repair_order_id,
                foo_DOT_bar_DOT_repair_orders.municipality_id foo_DOT_bar_DOT_repair_orders_DOT_municipality_id,
                foo_DOT_bar_DOT_repair_orders.hard_hat_id foo_DOT_bar_DOT_repair_orders_DOT_hard_hat_id,
                foo_DOT_bar_DOT_repair_orders.order_date foo_DOT_bar_DOT_repair_orders_DOT_order_date,
                foo_DOT_bar_DOT_repair_orders.required_date foo_DOT_bar_DOT_repair_orders_DOT_required_date,
                foo_DOT_bar_DOT_repair_orders.dispatched_date foo_DOT_bar_DOT_repair_orders_DOT_dispatched_date,
                foo_DOT_bar_DOT_repair_orders.dispatcher_id foo_DOT_bar_DOT_repair_orders_DOT_dispatcher_id,
                foo_DOT_bar_DOT_hard_hat.state foo_DOT_bar_DOT_hard_hat_DOT_state
             FROM roads.repair_orders AS foo_DOT_bar_DOT_repair_orders LEFT JOIN (SELECT  foo_DOT_bar_DOT_repair_orders.repair_order_id,
                foo_DOT_bar_DOT_repair_orders.municipality_id,
                foo_DOT_bar_DOT_repair_orders.hard_hat_id,
                foo_DOT_bar_DOT_repair_orders.dispatcher_id
             FROM roads.repair_orders AS foo_DOT_bar_DOT_repair_orders)
             AS foo_DOT_bar_DOT_repair_order ON foo_DOT_bar_DOT_repair_orders.repair_order_id = foo_DOT_bar_DOT_repair_order.repair_order_id
            LEFT JOIN (SELECT  foo_DOT_bar_DOT_hard_hats.hard_hat_id,
                foo_DOT_bar_DOT_hard_hats.state
             FROM roads.hard_hats AS foo_DOT_bar_DOT_hard_hats)
             AS foo_DOT_bar_DOT_hard_hat ON foo_DOT_bar_DOT_repair_order.hard_hat_id = foo_DOT_bar_DOT_hard_hat.hard_hat_id
             WHERE  foo_DOT_bar_DOT_hard_hat.state = 'CA'
            """,
        ),
        # querying source node with filters directly on the node
        (
            "foo.bar.repair_orders",
            [],
            ["foo.bar.repair_orders.order_date='2009-08-14'"],
            [],
            """
            SELECT
              foo_DOT_bar_DOT_repair_orders.repair_order_id foo_DOT_bar_DOT_repair_orders_DOT_repair_order_id,
              foo_DOT_bar_DOT_repair_orders.municipality_id foo_DOT_bar_DOT_repair_orders_DOT_municipality_id,
              foo_DOT_bar_DOT_repair_orders.hard_hat_id foo_DOT_bar_DOT_repair_orders_DOT_hard_hat_id,
              foo_DOT_bar_DOT_repair_orders.order_date foo_DOT_bar_DOT_repair_orders_DOT_order_date,
              foo_DOT_bar_DOT_repair_orders.required_date foo_DOT_bar_DOT_repair_orders_DOT_required_date,
              foo_DOT_bar_DOT_repair_orders.dispatched_date foo_DOT_bar_DOT_repair_orders_DOT_dispatched_date,
              foo_DOT_bar_DOT_repair_orders.dispatcher_id foo_DOT_bar_DOT_repair_orders_DOT_dispatcher_id
            FROM roads.repair_orders AS foo_DOT_bar_DOT_repair_orders
            WHERE
              foo_DOT_bar_DOT_repair_orders.order_date = '2009-08-14'
            """,
        ),
        (
            "foo.bar.num_repair_orders",
            [],
            [],
            [],
            """
            SELECT
              count(foo_DOT_bar_DOT_repair_orders.repair_order_id) AS foo_DOT_bar_DOT_num_repair_orders
            FROM roads.repair_orders AS foo_DOT_bar_DOT_repair_orders
            """,
        ),
        (
            "foo.bar.num_repair_orders",
            ["foo.bar.hard_hat.state"],
            ["foo.bar.repair_orders.dispatcher_id=1", "foo.bar.hard_hat.state='AZ'"],
            [],
            """
            SELECT  count(foo_DOT_bar_DOT_repair_orders.repair_order_id) AS foo_DOT_bar_DOT_num_repair_orders,
                foo_DOT_bar_DOT_hard_hat.state foo_DOT_bar_DOT_hard_hat_DOT_state
             FROM roads.repair_orders AS foo_DOT_bar_DOT_repair_orders LEFT JOIN (SELECT  foo_DOT_bar_DOT_repair_orders.repair_order_id,
                foo_DOT_bar_DOT_repair_orders.municipality_id,
                foo_DOT_bar_DOT_repair_orders.hard_hat_id,
                foo_DOT_bar_DOT_repair_orders.dispatcher_id
             FROM roads.repair_orders AS foo_DOT_bar_DOT_repair_orders
             WHERE  foo_DOT_bar_DOT_repair_orders.dispatcher_id = 1)
             AS foo_DOT_bar_DOT_repair_order ON foo_DOT_bar_DOT_repair_orders.repair_order_id = foo_DOT_bar_DOT_repair_order.repair_order_id
            LEFT JOIN (SELECT  foo_DOT_bar_DOT_hard_hats.hard_hat_id,
                foo_DOT_bar_DOT_hard_hats.state
             FROM roads.hard_hats AS foo_DOT_bar_DOT_hard_hats)
             AS foo_DOT_bar_DOT_hard_hat ON foo_DOT_bar_DOT_repair_order.hard_hat_id = foo_DOT_bar_DOT_hard_hat.hard_hat_id
             WHERE  foo_DOT_bar_DOT_repair_orders.dispatcher_id = 1 AND foo_DOT_bar_DOT_hard_hat.state = 'AZ'
             GROUP BY  foo_DOT_bar_DOT_hard_hat.state
            """,
        ),
        (
            "foo.bar.num_repair_orders",
            [
                "foo.bar.hard_hat.city",
                "foo.bar.hard_hat.last_name",
                "foo.bar.dispatcher.company_name",
                "foo.bar.municipality_dim.local_region",
            ],
            [
                "foo.bar.repair_orders.dispatcher_id=1",
                "foo.bar.hard_hat.state != 'AZ'",
                "foo.bar.dispatcher.phone = '4082021022'",
                "foo.bar.repair_orders.order_date >= '2020-01-01'",
            ],
            ["foo.bar.hard_hat.last_name"],
            """
            SELECT  count(foo_DOT_bar_DOT_repair_orders.repair_order_id) AS foo_DOT_bar_DOT_num_repair_orders,
                foo_DOT_bar_DOT_hard_hat.city foo_DOT_bar_DOT_hard_hat_DOT_city,
                foo_DOT_bar_DOT_hard_hat.last_name foo_DOT_bar_DOT_hard_hat_DOT_last_name,
                foo_DOT_bar_DOT_dispatcher.company_name foo_DOT_bar_DOT_dispatcher_DOT_company_name,
                foo_DOT_bar_DOT_municipality_dim.local_region foo_DOT_bar_DOT_municipality_dim_DOT_local_region
             FROM roads.repair_orders AS foo_DOT_bar_DOT_repair_orders LEFT JOIN (SELECT  foo_DOT_bar_DOT_repair_orders.repair_order_id,
                foo_DOT_bar_DOT_repair_orders.municipality_id,
                foo_DOT_bar_DOT_repair_orders.hard_hat_id,
                foo_DOT_bar_DOT_repair_orders.dispatcher_id
             FROM roads.repair_orders AS foo_DOT_bar_DOT_repair_orders
             WHERE  foo_DOT_bar_DOT_repair_orders.dispatcher_id = 1)
             AS foo_DOT_bar_DOT_repair_order ON foo_DOT_bar_DOT_repair_orders.repair_order_id = foo_DOT_bar_DOT_repair_order.repair_order_id
            LEFT JOIN (SELECT  foo_DOT_bar_DOT_dispatchers.dispatcher_id,
                foo_DOT_bar_DOT_dispatchers.company_name,
                foo_DOT_bar_DOT_dispatchers.phone
             FROM roads.dispatchers AS foo_DOT_bar_DOT_dispatchers)
             AS foo_DOT_bar_DOT_dispatcher ON foo_DOT_bar_DOT_repair_order.dispatcher_id = foo_DOT_bar_DOT_dispatcher.dispatcher_id
            LEFT JOIN (SELECT  foo_DOT_bar_DOT_hard_hats.hard_hat_id,
                foo_DOT_bar_DOT_hard_hats.last_name,
                foo_DOT_bar_DOT_hard_hats.city,
                foo_DOT_bar_DOT_hard_hats.state
             FROM roads.hard_hats AS foo_DOT_bar_DOT_hard_hats)
             AS foo_DOT_bar_DOT_hard_hat ON foo_DOT_bar_DOT_repair_order.hard_hat_id = foo_DOT_bar_DOT_hard_hat.hard_hat_id
            LEFT JOIN (SELECT  foo_DOT_bar_DOT_municipality.municipality_id AS municipality_id,
                foo_DOT_bar_DOT_municipality.local_region
             FROM roads.municipality AS foo_DOT_bar_DOT_municipality LEFT JOIN roads.municipality_municipality_type AS foo_DOT_bar_DOT_municipality_municipality_type ON foo_DOT_bar_DOT_municipality.municipality_id = foo_DOT_bar_DOT_municipality_municipality_type.municipality_id
            LEFT JOIN roads.municipality_type AS foo_DOT_bar_DOT_municipality_type ON foo_DOT_bar_DOT_municipality_municipality_type.municipality_type_id = foo_DOT_bar_DOT_municipality_type.municipality_type_desc)
             AS foo_DOT_bar_DOT_municipality_dim ON foo_DOT_bar_DOT_repair_order.municipality_id = foo_DOT_bar_DOT_municipality_dim.municipality_id
             WHERE  foo_DOT_bar_DOT_repair_orders.dispatcher_id = 1 AND foo_DOT_bar_DOT_hard_hat.state != 'AZ' AND foo_DOT_bar_DOT_dispatcher.phone = '4082021022' AND foo_DOT_bar_DOT_repair_orders.order_date >= '2020-01-01'
             GROUP BY  foo_DOT_bar_DOT_hard_hat.city, foo_DOT_bar_DOT_hard_hat.last_name, foo_DOT_bar_DOT_dispatcher.company_name, foo_DOT_bar_DOT_municipality_dim.local_region
            ORDER BY foo_DOT_bar_DOT_hard_hat.last_name
            """,
        ),
        (
            "foo.bar.avg_repair_price",
            ["foo.bar.hard_hat.city"],
            [],
            [],
            """
            SELECT
              avg(foo_DOT_bar_DOT_repair_order_details.price) foo_DOT_bar_DOT_avg_repair_price,
              foo_DOT_bar_DOT_hard_hat.city foo_DOT_bar_DOT_hard_hat_DOT_city
            FROM roads.repair_order_details AS foo_DOT_bar_DOT_repair_order_details
            LEFT JOIN (
              SELECT
                foo_DOT_bar_DOT_repair_orders.repair_order_id,
                foo_DOT_bar_DOT_repair_orders.municipality_id,
                foo_DOT_bar_DOT_repair_orders.hard_hat_id,
                foo_DOT_bar_DOT_repair_orders.dispatcher_id
              FROM roads.repair_orders AS foo_DOT_bar_DOT_repair_orders
            ) AS foo_DOT_bar_DOT_repair_order
            ON foo_DOT_bar_DOT_repair_order_details.repair_order_id
               = foo_DOT_bar_DOT_repair_order.repair_order_id
            LEFT JOIN (
              SELECT
                foo_DOT_bar_DOT_hard_hats.hard_hat_id,
                foo_DOT_bar_DOT_hard_hats.city
              FROM roads.hard_hats AS foo_DOT_bar_DOT_hard_hats
            ) AS foo_DOT_bar_DOT_hard_hat
            ON foo_DOT_bar_DOT_repair_order.hard_hat_id = foo_DOT_bar_DOT_hard_hat.hard_hat_id
            GROUP BY foo_DOT_bar_DOT_hard_hat.city
            """,
        ),
    ],
)
def test_sql_with_filters_on_namespaced_nodes(  # pylint: disable=R0913
    node_name,
    dimensions,
    filters,
    orderby,
    sql,
    client_with_namespaced_roads: TestClient,
):
    """
    Test ``GET /sql/{node_name}/`` with various filters and dimensions using a
    version of the DJ roads database with namespaces.
    """
    response = client_with_namespaced_roads.get(
        f"/sql/{node_name}/",
        params={"dimensions": dimensions, "filters": filters, "orderby": orderby},
    )
    data = response.json()
    assert str(parse(str(data["sql"]))) == str(parse(str(sql)))


def test_sql_with_filters_orderby_no_access(  # pylint: disable=R0913
    client_with_namespaced_roads: TestClient,
):
    """
    Test ``GET /sql/{node_name}/`` with various filters and dimensions using a
    version of the DJ roads database with namespaces.
    """

    def validate_access_override():
        def _validate_access(access_control: access.AccessControl):
            access_control.deny_all()

        return _validate_access

    app = client_with_namespaced_roads.app
    app.dependency_overrides[validate_access] = validate_access_override

    node_name = "foo.bar.num_repair_orders"
    dimensions = [
        "foo.bar.hard_hat.city",
        "foo.bar.hard_hat.last_name",
        "foo.bar.dispatcher.company_name",
        "foo.bar.municipality_dim.local_region",
    ]
    filters = [
        "foo.bar.repair_orders.dispatcher_id=1",
        "foo.bar.hard_hat.state != 'AZ'",
        "foo.bar.dispatcher.phone = '4082021022'",
        "foo.bar.repair_orders.order_date >= '2020-01-01'",
    ]
    orderby = ["foo.bar.hard_hat.last_name"]
    response = client_with_namespaced_roads.get(
        f"/sql/{node_name}/",
        params={"dimensions": dimensions, "filters": filters, "orderby": orderby},
    )
    data = response.json()
    assert sorted(list(data["message"])) == sorted(
        list(
            "Authorization of User `dj` for this request failed."
            "\nThe following requests were denied:\nread:node/foo.bar.dispatcher, "
            "read:node/foo.bar.repair_orders, read:node/foo.bar.municipality_dim, "
            "read:node/foo.bar.num_repair_orders, read:node/foo.bar.hard_hat.",
        ),
    )
    assert data["errors"][0]["code"] == 500


def test_cross_join_unnest(
    client_example_loader: Callable[[Optional[List[str]]], TestClient],
):
    """
    Verify cross join unnest on a joined in dimension works
    """
    custom_client = client_example_loader(["LATERAL_VIEW"])
    custom_client.post(
        "/nodes/basic.corrected_patches/columns/color_id/"
        "?dimension=basic.paint_colors_trino&dimension_column=color_id",
    )
    response = custom_client.get(
        "/sql/basic.avg_luminosity_patches/",
        params={
            "filters": [],
            "dimensions": [
                "basic.paint_colors_trino.color_id",
                "basic.paint_colors_trino.color_name",
            ],
        },
    )
    expected = """
    SELECT
      paint_colors_trino.color_id basic_DOT_avg_luminosity_patches_DOT_color_id,
      basic_DOT_paint_colors_trino.color_name basic_DOT_avg_luminosity_patches_DOT_color_name,
      AVG(basic_DOT_corrected_patches.luminosity) AS basic_DOT_avg_luminosity_patches_DOT_basic_DOT_avg_luminosity_patches
    FROM (
      SELECT
        CAST(basic_DOT_patches.color_id AS VARCHAR) color_id,
        basic_DOT_patches.color_name,
        basic_DOT_patches.garishness,
        basic_DOT_patches.luminosity,
        basic_DOT_patches.opacity
      FROM basic.patches AS basic_DOT_patches
    ) AS basic_DOT_corrected_patches
    LEFT JOIN (
      SELECT
        t.color_name color_name,
        t.color_id
      FROM (
        SELECT
          basic_DOT_murals.id,
          basic_DOT_murals.colors
        FROM basic.murals AS basic_DOT_murals
      ) murals
      CROSS JOIN UNNEST(murals.colors) t( color_id, color_name)
    ) AS basic_DOT_paint_colors_trino
    ON basic_DOT_corrected_patches.color_id = basic_DOT_paint_colors_trino.color_id
    GROUP BY
      paint_colors_trino.color_id,
      basic_DOT_paint_colors_trino.color_name
    """
    query = response.json()["sql"]
    compare_query_strings(query, expected)


def test_lateral_view_explode(
    client_example_loader: Callable[[Optional[List[str]]], TestClient],
):
    """
    Verify lateral view explode on a joined in dimension works
    """
    custom_client = client_example_loader(["LATERAL_VIEW"])
    custom_client.post(
        "/nodes/basic.corrected_patches/columns/color_id/"
        "?dimension=basic.paint_colors_spark&dimension_column=color_id",
    )
    response = custom_client.get(
        "/sql/basic.avg_luminosity_patches/",
        params={
            "filters": [],
            "dimensions": [
                "basic.paint_colors_spark.color_id",
                "basic.paint_colors_spark.color_name",
            ],
            "limit": 5,
        },
    )
    expected = """
    SELECT  AVG(basic_DOT_corrected_patches.luminosity) basic_DOT_avg_luminosity_patches,
        basic_DOT_paint_colors_spark.color_id basic_DOT_paint_colors_spark_DOT_color_id,
        basic_DOT_paint_colors_spark.color_name basic_DOT_paint_colors_spark_DOT_color_name
     FROM (SELECT  CAST(basic_DOT_patches.color_id AS STRING) color_id,
        basic_DOT_patches.color_name,
        basic_DOT_patches.opacity,
        basic_DOT_patches.luminosity,
        basic_DOT_patches.garishness
     FROM basic.patches AS basic_DOT_patches)
     AS basic_DOT_corrected_patches LEFT JOIN (SELECT  color_id,
        color_name
     FROM (SELECT  basic_DOT_murals.id,
        EXPLODE(basic_DOT_murals.colors) AS ( color_id, color_name)
     FROM basic.murals AS basic_DOT_murals

    ))
     AS basic_DOT_paint_colors_spark ON basic_DOT_corrected_patches.color_id = basic_DOT_paint_colors_spark.color_id
     GROUP BY  basic_DOT_paint_colors_spark.color_id, basic_DOT_paint_colors_spark.color_name

    LIMIT 5
    """
    query = response.json()["sql"]
    compare_query_strings(query, expected)


def test_get_sql_for_metrics_failures(client_with_account_revenue: TestClient):
    """
    Test failure modes when getting sql for multiple metrics.
    """
    # Getting sql for no metrics fails appropriately
    response = client_with_account_revenue.get(
        "/sql/",
        params={
            "metrics": [],
            "dimensions": ["default.account_type.account_type_name"],
            "filters": [],
        },
    )
    assert response.status_code == 422
    data = response.json()
    assert data == {
        "message": "At least one metric is required",
        "errors": [],
        "warnings": [],
    }

    # Getting sql for metric with no dimensions works
    response = client_with_account_revenue.get(
        "/sql/",
        params={
            "metrics": ["default.number_of_account_types"],
            "dimensions": [],
            "filters": [],
        },
    )
    assert response.status_code == 200


def test_get_sql_for_metrics_no_access(client_with_roads: TestClient):
    """
    Test getting sql for multiple metrics.
    """

    def validate_access_override():
        def _validate_access(access_control: access.AccessControl):
            if access_control.state == "direct":
                access_control.approve_all()
            else:
                access_control.deny_all()

        return _validate_access

    app = client_with_roads.app
    app.dependency_overrides[validate_access] = validate_access_override

    response = client_with_roads.get(
        "/sql/",
        params={
            "metrics": ["default.discounted_orders_rate", "default.num_repair_orders"],
            "dimensions": [
                "default.hard_hat.country",
                "default.hard_hat.postal_code",
                "default.hard_hat.city",
                "default.hard_hat.state",
                "default.dispatcher.company_name",
                "default.municipality_dim.local_region",
            ],
            "filters": ["default.hard_hat.city = 'Las Vegas'"],
            "orderby": [],
            "limit": 100,
        },
    )
    data = response.json()
    # assert "Authorization of User `dj` for this request failed.\n" in data["message"]
    assert "The following requests were denied:\n" in data["message"]
    assert "read:node/default.municipality_dim" in data["message"]
    assert "read:node/default.dispatcher" in data["message"]
    assert "read:node/default.repair_orders_fact" in data["message"]
    assert "read:node/default.hard_hat" in data["message"]
    assert data["errors"][0]["code"] == 500


def test_get_sql_for_metrics(client_with_roads: TestClient):
    """
    Test getting sql for multiple metrics.
    """

    response = client_with_roads.get(
        "/sql/",
        params={
            "metrics": ["default.discounted_orders_rate", "default.num_repair_orders"],
            "dimensions": [
                "default.hard_hat.country",
                "default.hard_hat.postal_code",
                "default.hard_hat.city",
                "default.hard_hat.state",
                "default.dispatcher.company_name",
                "default.municipality_dim.local_region",
            ],
            "filters": [],
            "orderby": [
                "default.hard_hat.country",
                "default.num_repair_orders",
                "default.dispatcher.company_name",
                "default.discounted_orders_rate",
            ],
            "limit": 100,
        },
    )
    data = response.json()
    expected_sql = """
    WITH
    default_DOT_repair_orders_fact AS (SELECT
        default_DOT_hard_hat.country default_DOT_hard_hat_DOT_country,
        default_DOT_hard_hat.postal_code default_DOT_hard_hat_DOT_postal_code,
        default_DOT_hard_hat.city default_DOT_hard_hat_DOT_city,
        default_DOT_hard_hat.state default_DOT_hard_hat_DOT_state,
        default_DOT_dispatcher.company_name default_DOT_dispatcher_DOT_company_name,
        default_DOT_municipality_dim.local_region default_DOT_municipality_dim_DOT_local_region,
        CAST(sum(if(default_DOT_repair_orders_fact.discount > 0.0, 1, 0)) AS DOUBLE) / count(*) AS default_DOT_discounted_orders_rate,
        count(default_DOT_repair_orders_fact.repair_order_id) default_DOT_num_repair_orders
     FROM (SELECT  default_DOT_repair_orders.repair_order_id,
        default_DOT_repair_orders.municipality_id,
        default_DOT_repair_orders.hard_hat_id,
        default_DOT_repair_orders.dispatcher_id,
        default_DOT_repair_orders.order_date,
        default_DOT_repair_orders.dispatched_date,
        default_DOT_repair_orders.required_date,
        default_DOT_repair_order_details.discount,
        default_DOT_repair_order_details.price,
        default_DOT_repair_order_details.quantity,
        default_DOT_repair_order_details.repair_type_id,
        default_DOT_repair_order_details.price * default_DOT_repair_order_details.quantity AS total_repair_cost,
        default_DOT_repair_orders.dispatched_date - default_DOT_repair_orders.order_date AS time_to_dispatch,
        default_DOT_repair_orders.dispatched_date - default_DOT_repair_orders.required_date AS dispatch_delay
     FROM roads.repair_orders AS default_DOT_repair_orders JOIN roads.repair_order_details AS default_DOT_repair_order_details ON default_DOT_repair_orders.repair_order_id = default_DOT_repair_order_details.repair_order_id)
     AS default_DOT_repair_orders_fact LEFT JOIN (SELECT  default_DOT_dispatchers.dispatcher_id,
        default_DOT_dispatchers.company_name
     FROM roads.dispatchers AS default_DOT_dispatchers)
     AS default_DOT_dispatcher ON default_DOT_repair_orders_fact.dispatcher_id = default_DOT_dispatcher.dispatcher_id
    LEFT JOIN (SELECT
        default_DOT_hard_hats.hard_hat_id,
        default_DOT_hard_hats.city,
        default_DOT_hard_hats.state,
        default_DOT_hard_hats.postal_code,
        default_DOT_hard_hats.country
     FROM roads.hard_hats AS default_DOT_hard_hats)
     AS default_DOT_hard_hat ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
    LEFT JOIN (SELECT  default_DOT_municipality.municipality_id AS municipality_id,
        default_DOT_municipality.local_region
     FROM roads.municipality AS default_DOT_municipality LEFT JOIN roads.municipality_municipality_type AS default_DOT_municipality_municipality_type ON default_DOT_municipality.municipality_id = default_DOT_municipality_municipality_type.municipality_id
    LEFT JOIN roads.municipality_type AS default_DOT_municipality_type ON default_DOT_municipality_municipality_type.municipality_type_id = default_DOT_municipality_type.municipality_type_desc)
     AS default_DOT_municipality_dim ON default_DOT_repair_orders_fact.municipality_id = default_DOT_municipality_dim.municipality_id
     GROUP BY  default_DOT_hard_hat.country, default_DOT_hard_hat.postal_code, default_DOT_hard_hat.city, default_DOT_hard_hat.state, default_DOT_dispatcher.company_name, default_DOT_municipality_dim.local_region
    )

    SELECT  default_DOT_repair_orders_fact.default_DOT_discounted_orders_rate,
        default_DOT_repair_orders_fact.default_DOT_num_repair_orders,
        default_DOT_repair_orders_fact.default_DOT_hard_hat_DOT_country,
        default_DOT_repair_orders_fact.default_DOT_hard_hat_DOT_postal_code,
        default_DOT_repair_orders_fact.default_DOT_hard_hat_DOT_city,
        default_DOT_repair_orders_fact.default_DOT_hard_hat_DOT_state,
        default_DOT_repair_orders_fact.default_DOT_dispatcher_DOT_company_name,
        default_DOT_repair_orders_fact.default_DOT_municipality_dim_DOT_local_region
     FROM default_DOT_repair_orders_fact
    ORDER BY default_DOT_hard_hat_DOT_country, default_DOT_num_repair_orders, default_DOT_dispatcher_DOT_company_name, default_DOT_discounted_orders_rate
    LIMIT 100
    """
    assert str(parse(str(data["sql"]))) == str(parse(str(expected_sql)))
    assert data["columns"] == [
        {
            "column": "default_DOT_discounted_orders_rate",
            "name": "default_DOT_discounted_orders_rate",
            "node": "default.discounted_orders_rate",
            "semantic_entity": "default.discounted_orders_rate.default_DOT_discounted_orders_rate",
            "semantic_type": "metric",
            "type": "double",
        },
        {
            "column": "default_DOT_num_repair_orders",
            "name": "default_DOT_num_repair_orders",
            "node": "default.num_repair_orders",
            "semantic_entity": "default.num_repair_orders.default_DOT_num_repair_orders",
            "semantic_type": "metric",
            "type": "bigint",
        },
        {
            "column": "country",
            "name": "default_DOT_hard_hat_DOT_country",
            "node": "default.hard_hat",
            "semantic_entity": "default.hard_hat.country",
            "semantic_type": "dimension",
            "type": "string",
        },
        {
            "column": "postal_code",
            "name": "default_DOT_hard_hat_DOT_postal_code",
            "node": "default.hard_hat",
            "semantic_entity": "default.hard_hat.postal_code",
            "semantic_type": "dimension",
            "type": "string",
        },
        {
            "column": "city",
            "name": "default_DOT_hard_hat_DOT_city",
            "node": "default.hard_hat",
            "semantic_entity": "default.hard_hat.city",
            "semantic_type": "dimension",
            "type": "string",
        },
        {
            "column": "state",
            "name": "default_DOT_hard_hat_DOT_state",
            "node": "default.hard_hat",
            "semantic_entity": "default.hard_hat.state",
            "semantic_type": "dimension",
            "type": "string",
        },
        {
            "column": "company_name",
            "name": "default_DOT_dispatcher_DOT_company_name",
            "node": "default.dispatcher",
            "semantic_entity": "default.dispatcher.company_name",
            "semantic_type": "dimension",
            "type": "string",
        },
        {
            "column": "local_region",
            "name": "default_DOT_municipality_dim_DOT_local_region",
            "node": "default.municipality_dim",
            "semantic_entity": "default.municipality_dim.local_region",
            "semantic_type": "dimension",
            "type": "string",
        },
    ]


def test_get_sql_including_dimension_ids(client_with_roads: TestClient):
    """
    Test getting SQL when there are dimensions ids included
    """
    response = client_with_roads.get(
        "/sql/",
        params={
            "metrics": ["default.avg_repair_price", "default.total_repair_cost"],
            "dimensions": [
                "default.dispatcher.company_name",
                "default.dispatcher.dispatcher_id",
            ],
            "filters": [],
        },
    )
    assert response.status_code == 200
    data = response.json()
    expected = """
        WITH
        default_DOT_repair_orders_fact AS (SELECT  default_DOT_repair_orders_fact.dispatcher_id default_DOT_dispatcher_DOT_dispatcher_id,
            default_DOT_dispatcher.company_name default_DOT_dispatcher_DOT_company_name,
            avg(default_DOT_repair_orders_fact.price) default_DOT_avg_repair_price,
            sum(default_DOT_repair_orders_fact.total_repair_cost) default_DOT_total_repair_cost
         FROM (SELECT  default_DOT_repair_orders.repair_order_id,
            default_DOT_repair_orders.municipality_id,
            default_DOT_repair_orders.hard_hat_id,
            default_DOT_repair_orders.dispatcher_id,
            default_DOT_repair_orders.order_date,
            default_DOT_repair_orders.dispatched_date,
            default_DOT_repair_orders.required_date,
            default_DOT_repair_order_details.discount,
            default_DOT_repair_order_details.price,
            default_DOT_repair_order_details.quantity,
            default_DOT_repair_order_details.repair_type_id,
            default_DOT_repair_order_details.price * default_DOT_repair_order_details.quantity AS total_repair_cost,
            default_DOT_repair_orders.dispatched_date - default_DOT_repair_orders.order_date AS time_to_dispatch,
            default_DOT_repair_orders.dispatched_date - default_DOT_repair_orders.required_date AS dispatch_delay
         FROM roads.repair_orders AS default_DOT_repair_orders JOIN roads.repair_order_details AS default_DOT_repair_order_details ON default_DOT_repair_orders.repair_order_id = default_DOT_repair_order_details.repair_order_id)
         AS default_DOT_repair_orders_fact LEFT JOIN (SELECT  default_DOT_dispatchers.dispatcher_id,
            default_DOT_dispatchers.company_name
         FROM roads.dispatchers AS default_DOT_dispatchers)
         AS default_DOT_dispatcher ON default_DOT_repair_orders_fact.dispatcher_id = default_DOT_dispatcher.dispatcher_id
         GROUP BY  default_DOT_dispatcher.company_name, default_DOT_repair_orders_fact.dispatcher_id
        )

        SELECT  default_DOT_repair_orders_fact.default_DOT_avg_repair_price,
            default_DOT_repair_orders_fact.default_DOT_total_repair_cost,
            default_DOT_repair_orders_fact.default_DOT_dispatcher_DOT_dispatcher_id,
            default_DOT_repair_orders_fact.default_DOT_dispatcher_DOT_company_name
         FROM default_DOT_repair_orders_fact"""
    assert str(parse(str(expected))) == str(parse(str(data["sql"])))

    response = client_with_roads.get(
        "/sql/",
        params={
            "metrics": ["default.avg_repair_price", "default.total_repair_cost"],
            "dimensions": [
                "default.hard_hat.hard_hat_id",
                "default.hard_hat.first_name",
            ],
            "filters": [],
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert compare_query_strings(
        data["sql"],
        """
        WITH
        default_DOT_repair_orders_fact AS (SELECT  default_DOT_repair_orders_fact.hard_hat_id default_DOT_hard_hat_DOT_hard_hat_id,
            default_DOT_hard_hat.first_name default_DOT_hard_hat_DOT_first_name,
            avg(default_DOT_repair_orders_fact.price) default_DOT_avg_repair_price,
            sum(default_DOT_repair_orders_fact.total_repair_cost) default_DOT_total_repair_cost
         FROM (SELECT  default_DOT_repair_orders.repair_order_id,
            default_DOT_repair_orders.municipality_id,
            default_DOT_repair_orders.hard_hat_id,
            default_DOT_repair_orders.dispatcher_id,
            default_DOT_repair_orders.order_date,
            default_DOT_repair_orders.dispatched_date,
            default_DOT_repair_orders.required_date,
            default_DOT_repair_order_details.discount,
            default_DOT_repair_order_details.price,
            default_DOT_repair_order_details.quantity,
            default_DOT_repair_order_details.repair_type_id,
            default_DOT_repair_order_details.price * default_DOT_repair_order_details.quantity AS total_repair_cost,
            default_DOT_repair_orders.dispatched_date - default_DOT_repair_orders.order_date AS time_to_dispatch,
            default_DOT_repair_orders.dispatched_date - default_DOT_repair_orders.required_date AS dispatch_delay
         FROM roads.repair_orders AS default_DOT_repair_orders JOIN roads.repair_order_details AS default_DOT_repair_order_details ON default_DOT_repair_orders.repair_order_id = default_DOT_repair_order_details.repair_order_id)
         AS default_DOT_repair_orders_fact LEFT JOIN (SELECT  default_DOT_hard_hats.hard_hat_id,
            default_DOT_hard_hats.first_name,
            default_DOT_hard_hats.state
         FROM roads.hard_hats AS default_DOT_hard_hats)
         AS default_DOT_hard_hat ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
         GROUP BY  default_DOT_repair_orders_fact.hard_hat_id, default_DOT_hard_hat.first_name
        )
        SELECT  default_DOT_repair_orders_fact.default_DOT_avg_repair_price,
            default_DOT_repair_orders_fact.default_DOT_total_repair_cost,
            default_DOT_repair_orders_fact.default_DOT_hard_hat_DOT_hard_hat_id,
            default_DOT_repair_orders_fact.default_DOT_hard_hat_DOT_first_name
         FROM default_DOT_repair_orders_fact
        """,
    )


def test_get_sql_including_dimensions_with_disambiguated_columns(
    client_with_roads: TestClient,
):
    """
    Test getting SQL that includes dimensions with SQL that has to disambiguate projection columns with prefixes
    """
    response = client_with_roads.get(
        "/sql/",
        params={
            "metrics": ["default.total_repair_cost"],
            "dimensions": [
                "default.municipality_dim.state_id",
                "default.municipality_dim.municipality_type_id",
                "default.municipality_dim.municipality_type_desc",
                "default.municipality_dim.municipality_id",
            ],
            "filters": [],
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data["columns"] == [
        {
            "column": "default_DOT_total_repair_cost",
            "name": "default_DOT_total_repair_cost",
            "node": "default.total_repair_cost",
            "semantic_entity": "default.total_repair_cost.default_DOT_total_repair_cost",
            "semantic_type": "metric",
            "type": "double",
        },
        {
            "column": "municipality_id",
            "name": "default_DOT_municipality_dim_DOT_municipality_id",
            "node": "default.municipality_dim",
            "semantic_entity": "default.municipality_dim.municipality_id",
            "semantic_type": "dimension",
            "type": "string",
        },
        {
            "column": "state_id",
            "name": "default_DOT_municipality_dim_DOT_state_id",
            "node": "default.municipality_dim",
            "semantic_entity": "default.municipality_dim.state_id",
            "semantic_type": "dimension",
            "type": "int",
        },
        {
            "column": "municipality_type_id",
            "name": "default_DOT_municipality_dim_DOT_municipality_type_id",
            "node": "default.municipality_dim",
            "semantic_entity": "default.municipality_dim.municipality_type_id",
            "semantic_type": "dimension",
            "type": "string",
        },
        {
            "column": "municipality_type_desc",
            "name": "default_DOT_municipality_dim_DOT_municipality_type_desc",
            "node": "default.municipality_dim",
            "semantic_entity": "default.municipality_dim.municipality_type_desc",
            "semantic_type": "dimension",
            "type": "string",
        },
    ]
    assert compare_query_strings(
        data["sql"],
        """
        WITH
        default_DOT_repair_orders_fact AS (SELECT  default_DOT_repair_orders_fact.municipality_id default_DOT_municipality_dim_DOT_municipality_id,
            default_DOT_municipality_dim.state_id default_DOT_municipality_dim_DOT_state_id,
            default_DOT_municipality_dim.municipality_type_id default_DOT_municipality_dim_DOT_municipality_type_id,
            default_DOT_municipality_dim.municipality_type_desc default_DOT_municipality_dim_DOT_municipality_type_desc,
            sum(default_DOT_repair_orders_fact.total_repair_cost) default_DOT_total_repair_cost
         FROM (SELECT  default_DOT_repair_orders.repair_order_id,
            default_DOT_repair_orders.municipality_id,
            default_DOT_repair_orders.hard_hat_id,
            default_DOT_repair_orders.dispatcher_id,
            default_DOT_repair_orders.order_date,
            default_DOT_repair_orders.dispatched_date,
            default_DOT_repair_orders.required_date,
            default_DOT_repair_order_details.discount,
            default_DOT_repair_order_details.price,
            default_DOT_repair_order_details.quantity,
            default_DOT_repair_order_details.repair_type_id,
            default_DOT_repair_order_details.price * default_DOT_repair_order_details.quantity AS total_repair_cost,
            default_DOT_repair_orders.dispatched_date - default_DOT_repair_orders.order_date AS time_to_dispatch,
            default_DOT_repair_orders.dispatched_date - default_DOT_repair_orders.required_date AS dispatch_delay
         FROM roads.repair_orders AS default_DOT_repair_orders JOIN roads.repair_order_details AS default_DOT_repair_order_details ON default_DOT_repair_orders.repair_order_id = default_DOT_repair_order_details.repair_order_id)
         AS default_DOT_repair_orders_fact LEFT JOIN (SELECT  default_DOT_municipality.municipality_id AS municipality_id,
            default_DOT_municipality.state_id,
            default_DOT_municipality_municipality_type.municipality_type_id AS municipality_type_id,
            default_DOT_municipality_type.municipality_type_desc AS municipality_type_desc
         FROM roads.municipality AS default_DOT_municipality LEFT  JOIN roads.municipality_municipality_type AS default_DOT_municipality_municipality_type ON default_DOT_municipality.municipality_id = default_DOT_municipality_municipality_type.municipality_id
        LEFT  JOIN roads.municipality_type AS default_DOT_municipality_type ON default_DOT_municipality_municipality_type.municipality_type_id = default_DOT_municipality_type.municipality_type_desc)
         AS default_DOT_municipality_dim ON default_DOT_repair_orders_fact.municipality_id = default_DOT_municipality_dim.municipality_id
         GROUP BY  default_DOT_municipality_dim.state_id, default_DOT_municipality_dim.municipality_type_id, default_DOT_municipality_dim.municipality_type_desc, default_DOT_repair_orders_fact.municipality_id
        )

        SELECT  default_DOT_repair_orders_fact.default_DOT_total_repair_cost,
            default_DOT_repair_orders_fact.default_DOT_municipality_dim_DOT_municipality_id,
            default_DOT_repair_orders_fact.default_DOT_municipality_dim_DOT_state_id,
            default_DOT_repair_orders_fact.default_DOT_municipality_dim_DOT_municipality_type_id,
            default_DOT_repair_orders_fact.default_DOT_municipality_dim_DOT_municipality_type_desc
         FROM default_DOT_repair_orders_fact
        """,
    )

    response = client_with_roads.get(
        "/sql/",
        params={
            "metrics": ["default.avg_repair_price", "default.total_repair_cost"],
            "dimensions": [
                "default.hard_hat.hard_hat_id",
            ],
            "filters": [],
        },
    )
    assert response.status_code == 200
    data = response.json()
    expected = """WITH
        default_DOT_repair_orders_fact AS (SELECT  default_DOT_repair_orders_fact.hard_hat_id default_DOT_hard_hat_DOT_hard_hat_id,
            avg(default_DOT_repair_orders_fact.price) default_DOT_avg_repair_price,
            sum(default_DOT_repair_orders_fact.total_repair_cost) default_DOT_total_repair_cost
         FROM (SELECT  default_DOT_repair_orders.repair_order_id,
            default_DOT_repair_orders.municipality_id,
            default_DOT_repair_orders.hard_hat_id,
            default_DOT_repair_orders.dispatcher_id,
            default_DOT_repair_orders.order_date,
            default_DOT_repair_orders.dispatched_date,
            default_DOT_repair_orders.required_date,
            default_DOT_repair_order_details.discount,
            default_DOT_repair_order_details.price,
            default_DOT_repair_order_details.quantity,
            default_DOT_repair_order_details.repair_type_id,
            default_DOT_repair_order_details.price * default_DOT_repair_order_details.quantity AS total_repair_cost,
            default_DOT_repair_orders.dispatched_date - default_DOT_repair_orders.order_date AS time_to_dispatch,
            default_DOT_repair_orders.dispatched_date - default_DOT_repair_orders.required_date AS dispatch_delay
         FROM roads.repair_orders AS default_DOT_repair_orders JOIN roads.repair_order_details AS default_DOT_repair_order_details ON default_DOT_repair_orders.repair_order_id = default_DOT_repair_order_details.repair_order_id)
         AS default_DOT_repair_orders_fact
         GROUP BY  default_DOT_repair_orders_fact.hard_hat_id
        )

        SELECT  default_DOT_repair_orders_fact.default_DOT_avg_repair_price,
            default_DOT_repair_orders_fact.default_DOT_total_repair_cost,
            default_DOT_repair_orders_fact.default_DOT_hard_hat_DOT_hard_hat_id
         FROM default_DOT_repair_orders_fact"""
    assert str(parse(data["sql"])) == str(parse(expected))


def test_get_sql_for_metrics_filters_validate_dimensions(
    client_with_namespaced_roads: TestClient,
):
    """
    Test that we extract the columns from filters to validate that they are from shared dimensions
    """
    response = client_with_namespaced_roads.get(
        "/sql/",
        params={
            "metrics": ["foo.bar.num_repair_orders", "foo.bar.avg_repair_price"],
            "dimensions": [
                "foo.bar.hard_hat.country",
            ],
            "filters": ["default.hard_hat.city = 'Las Vegas'"],
            "limit": 10,
        },
    )
    data = response.json()
    assert data["message"] == (
        "The filter `default.hard_hat.city = 'Las Vegas'` references the dimension "
        "attribute `default.hard_hat.city`, which is not available on every metric and "
        "thus cannot be included."
    )


def test_get_sql_for_metrics_orderby_not_in_dimensions(
    client_example_loader: Callable[[Optional[List[str]]], TestClient],
):
    """
    Test that we extract the columns from filters to validate that they are from shared dimensions
    """
    custom_client = client_example_loader(["ROADS", "NAMESPACED_ROADS"])
    response = custom_client.get(
        "/sql/",
        params={
            "metrics": ["foo.bar.num_repair_orders", "foo.bar.avg_repair_price"],
            "dimensions": [
                "foo.bar.hard_hat.country",
            ],
            "orderby": ["default.hard_hat.city"],
            "limit": 10,
        },
    )
    data = response.json()
    assert data["message"] == (
        "Columns ['default.hard_hat.city'] in order by "
        "clause must also be specified in the metrics or dimensions"
    )


def test_get_sql_for_metrics_orderby_not_in_dimensions_no_access(
    client_example_loader: Callable[[Optional[List[str]]], TestClient],
):
    """
    Test that we extract the columns from filters to validate that they are from shared dimensions
    """
    if isinstance(client_example_loader, TestClient):

        def validate_access_override():
            def _validate_access(access_control: access.AccessControl):
                for request in access_control.requests:
                    if (
                        request.access_object.resource_type == access.ResourceType.NODE
                        and request.access_object.name
                        in (
                            "foo.bar.avg_repair_price",
                            "default.hard_hat.city",
                        )
                    ):
                        request.deny()
                    else:
                        request.approve()

            return _validate_access

        app = client_example_loader.app
        app.dependency_overrides[validate_access] = validate_access_override

    custom_client = client_example_loader(["ROADS", "NAMESPACED_ROADS"])
    response = custom_client.get(
        "/sql/",
        params={
            "metrics": ["foo.bar.num_repair_orders", "foo.bar.avg_repair_price"],
            "dimensions": [
                "foo.bar.hard_hat.country",
            ],
            "orderby": ["default.hard_hat.city"],
            "limit": 10,
        },
    )
    data = response.json()
    assert data["message"] == (
        "Columns ['default.hard_hat.city'] in order by "
        "clause must also be specified in the metrics or dimensions"
    )


def test_sql_structs(client_with_roads: TestClient):
    """
    Create a transform with structs and verify that metric expressions that reference these
    structs, along with grouping by dimensions that reference these structs will work when
    building metrics SQL.
    """
    client_with_roads.post(
        "/nodes/transform",
        json={
            "name": "default.simple_agg",
            "description": "simple agg",
            "mode": "published",
            "query": """SELECT
  EXTRACT(YEAR FROM ro.relevant_dates.order_dt) AS order_year,
  EXTRACT(MONTH FROM ro.relevant_dates.order_dt) AS order_month,
  EXTRACT(DAY FROM ro.relevant_dates.order_dt) AS order_day,
  SUM(DATEDIFF(ro.relevant_dates.dispatched_dt, ro.relevant_dates.order_dt)) AS dispatch_delay_sum,
  COUNT(ro.repair_order_id) AS repair_orders_cnt
FROM (
  SELECT
    repair_order_id,
    STRUCT(required_date as required_dt, order_date as order_dt, dispatched_date as dispatched_dt) relevant_dates
  FROM default.repair_orders
) AS ro
GROUP BY
  EXTRACT(YEAR FROM ro.relevant_dates.order_dt),
  EXTRACT(MONTH FROM ro.relevant_dates.order_dt),
  EXTRACT(DAY FROM ro.relevant_dates.order_dt)""",
        },
    )

    client_with_roads.post(
        "/nodes/metric",
        json={
            "name": "default.average_dispatch_delay",
            "description": "average dispatch delay",
            "mode": "published",
            "query": """select SUM(D.dispatch_delay_sum)/SUM(D.repair_orders_cnt) from default.simple_agg D""",
        },
    )
    dimension_attr = [
        {
            "namespace": "system",
            "name": "dimension",
        },
    ]
    for column in ["order_year", "order_month", "order_day"]:
        client_with_roads.post(
            f"/nodes/default.simple_agg/columns/{column}/attributes/",
            json=dimension_attr,
        )
    sql_params = {
        "metrics": ["default.average_dispatch_delay"],
        "dimensions": [
            "default.simple_agg.order_year",
            "default.simple_agg.order_month",
            "default.simple_agg.order_day",
        ],
        "filters": ["default.simple_agg.order_year = 2020"],
    }

    expected = """WITH
    default_DOT_simple_agg AS (SELECT  default_DOT_simple_agg.order_year default_DOT_simple_agg_DOT_order_year,
    default_DOT_simple_agg.order_month default_DOT_simple_agg_DOT_order_month,
    default_DOT_simple_agg.order_day default_DOT_simple_agg_DOT_order_day,
    SUM(default_DOT_simple_agg.dispatch_delay_sum) / SUM(default_DOT_simple_agg.repair_orders_cnt) default_DOT_average_dispatch_delay
FROM (SELECT  EXTRACT(YEAR, ro.relevant_dates.order_dt) AS order_year,
    EXTRACT(MONTH, ro.relevant_dates.order_dt) AS order_month,
    EXTRACT(DAY, ro.relevant_dates.order_dt) AS order_day,
    SUM(DATEDIFF(ro.relevant_dates.dispatched_dt, ro.relevant_dates.order_dt)) AS dispatch_delay_sum,
    COUNT(ro.repair_order_id) AS repair_orders_cnt
 FROM (SELECT  default_DOT_repair_orders.repair_order_id,
    struct(default_DOT_repair_orders.required_date AS required_dt, default_DOT_repair_orders.order_date AS order_dt, default_DOT_repair_orders.dispatched_date AS dispatched_dt) relevant_dates
 FROM roads.repair_orders AS default_DOT_repair_orders LEFT JOIN (SELECT  default_DOT_repair_orders.repair_order_id,
    default_DOT_repair_orders.municipality_id,
    default_DOT_repair_orders.hard_hat_id,
    default_DOT_repair_orders.order_date,
    default_DOT_repair_orders.required_date,
    default_DOT_repair_orders.dispatched_date,
    default_DOT_repair_orders.dispatcher_id
 FROM roads.repair_orders AS default_DOT_repair_orders)
 AS default_DOT_repair_order ON default_DOT_repair_orders.repair_order_id = default_DOT_repair_order.repair_order_id

) AS ro
 GROUP BY  EXTRACT(YEAR, ro.relevant_dates.order_dt), EXTRACT(MONTH, ro.relevant_dates.order_dt), EXTRACT(DAY, ro.relevant_dates.order_dt))
 AS default_DOT_simple_agg
 WHERE  default_DOT_simple_agg.order_year = 2020
 GROUP BY  default_DOT_simple_agg.order_year, default_DOT_simple_agg.order_month, default_DOT_simple_agg.order_day
)

SELECT  default_DOT_simple_agg.default_DOT_average_dispatch_delay,
    default_DOT_simple_agg.default_DOT_simple_agg_DOT_order_year,
    default_DOT_simple_agg.default_DOT_simple_agg_DOT_order_month,
    default_DOT_simple_agg.default_DOT_simple_agg_DOT_order_day
 FROM default_DOT_simple_agg"""
    response = client_with_roads.get("/sql", params=sql_params)
    data = response.json()
    assert str(parse(str(expected))) == str(parse(str(data["sql"])))

    # Test the same query string but with `ro` as a CTE
    client_with_roads.patch(
        "/nodes/transform",
        json={
            "name": "default.simple_agg",
            "query": """WITH ro as (
  SELECT
    repair_order_id,
    STRUCT(required_date as required_dt, order_date as order_dt, dispatched_date as dispatched_dt) relevant_dates
  FROM default.repair_orders
)
SELECT
  EXTRACT(YEAR FROM ro.relevant_dates.order_dt) AS order_year,
  EXTRACT(MONTH FROM ro.relevant_dates.order_dt) AS order_month,
  EXTRACT(DAY FROM ro.relevant_dates.order_dt) AS order_day,
  SUM(DATEDIFF(ro.relevant_dates.dispatched_dt, ro.relevant_dates.order_dt)) AS dispatch_delay_sum,
  COUNT(ro.repair_order_id) AS repair_orders_cnt
FROM ro
GROUP BY
  EXTRACT(YEAR FROM ro.relevant_dates.order_dt),
  EXTRACT(MONTH FROM ro.relevant_dates.order_dt),
  EXTRACT(DAY FROM ro.relevant_dates.order_dt)""",
        },
    )

    response = client_with_roads.get("/sql", params=sql_params)
    data = response.json()
    assert compare_query_strings(data["sql"], expected)


@pytest.mark.parametrize(
    "metrics, dimensions, filters, sql, columns, rows",
    [
        # One metric with two measures + one local dimension. Both referenced measures should
        # show up in the generated measures SQL
        (
            ["default.total_repair_order_discounts"],
            ["default.dispatcher.dispatcher_id"],
            [],
            """WITH
            default_DOT_repair_orders_fact AS (SELECT  default_DOT_repair_orders_fact.dispatcher_id default_DOT_dispatcher_DOT_dispatcher_id,
                default_DOT_repair_orders_fact.discount default_DOT_repair_orders_fact_DOT_discount,
                default_DOT_repair_orders_fact.price default_DOT_repair_orders_fact_DOT_price
             FROM (SELECT  default_DOT_repair_orders.repair_order_id,
                default_DOT_repair_orders.municipality_id,
                default_DOT_repair_orders.hard_hat_id,
                default_DOT_repair_orders.dispatcher_id,
                default_DOT_repair_orders.order_date,
                default_DOT_repair_orders.dispatched_date,
                default_DOT_repair_orders.required_date,
                default_DOT_repair_order_details.discount,
                default_DOT_repair_order_details.price,
                default_DOT_repair_order_details.quantity,
                default_DOT_repair_order_details.repair_type_id,
                default_DOT_repair_order_details.price * default_DOT_repair_order_details.quantity AS total_repair_cost,
                default_DOT_repair_orders.dispatched_date - default_DOT_repair_orders.order_date AS time_to_dispatch,
                default_DOT_repair_orders.dispatched_date - default_DOT_repair_orders.required_date AS dispatch_delay
             FROM roads.repair_orders AS default_DOT_repair_orders JOIN roads.repair_order_details AS default_DOT_repair_order_details ON default_DOT_repair_orders.repair_order_id = default_DOT_repair_order_details.repair_order_id)
             AS default_DOT_repair_orders_fact
            )
            SELECT  default_DOT_repair_orders_fact.default_DOT_repair_orders_fact_DOT_discount,
                default_DOT_repair_orders_fact.default_DOT_repair_orders_fact_DOT_price,
                default_DOT_repair_orders_fact.default_DOT_dispatcher_DOT_dispatcher_id
             FROM default_DOT_repair_orders_fact""",
            [
                {
                    "column": "discount",
                    "name": "default_DOT_repair_orders_fact_DOT_discount",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.discount",
                    "semantic_type": "measure",
                    "type": "float",
                },
                {
                    "column": "price",
                    "name": "default_DOT_repair_orders_fact_DOT_price",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.price",
                    "semantic_type": "measure",
                    "type": "float",
                },
                {
                    "column": "dispatcher_id",
                    "name": "default_DOT_dispatcher_DOT_dispatcher_id",
                    "node": "default.dispatcher",
                    "semantic_entity": "default.dispatcher.dispatcher_id",
                    "semantic_type": "dimension",
                    "type": "int",
                },
            ],
            [
                (0.05000000074505806, 63708.0, 3),
                (0.05000000074505806, 67253.0, 1),
                (0.05000000074505806, 66808.0, 2),
                (0.05000000074505806, 18497.0, 1),
                (0.05000000074505806, 76463.0, 2),
                (0.05000000074505806, 87858.0, 2),
                (0.05000000074505806, 63918.0, 2),
                (0.05000000074505806, 21083.0, 3),
                (0.05000000074505806, 74555.0, 2),
                (0.05000000074505806, 27222.0, 3),
                (0.05000000074505806, 73600.0, 1),
                (0.009999999776482582, 54901.0, 3),
                (0.009999999776482582, 51594.0, 1),
                (0.009999999776482582, 65114.0, 2),
                (0.009999999776482582, 48919.0, 3),
                (0.009999999776482582, 70418.0, 3),
                (0.009999999776482582, 29684.0, 3),
                (0.009999999776482582, 62928.0, 1),
                (0.009999999776482582, 97916.0, 3),
                (0.009999999776482582, 44120.0, 1),
                (0.009999999776482582, 53374.0, 3),
                (0.009999999776482582, 87289.0, 1),
                (0.009999999776482582, 92366.0, 1),
                (0.009999999776482582, 47857.0, 2),
                (0.009999999776482582, 68745.0, 2),
            ],
        ),
        # Two metrics with overlapping measures + one joinable dimension
        (
            [
                "default.total_repair_order_discounts",
                "default.avg_repair_order_discounts",
            ],
            ["default.dispatcher.dispatcher_id"],
            [],
            """WITH
            default_DOT_repair_orders_fact AS (SELECT  default_DOT_repair_orders_fact.dispatcher_id default_DOT_dispatcher_DOT_dispatcher_id,
                default_DOT_repair_orders_fact.discount default_DOT_repair_orders_fact_DOT_discount,
                default_DOT_repair_orders_fact.price default_DOT_repair_orders_fact_DOT_price
             FROM (SELECT  default_DOT_repair_orders.repair_order_id,
                default_DOT_repair_orders.municipality_id,
                default_DOT_repair_orders.hard_hat_id,
                default_DOT_repair_orders.dispatcher_id,
                default_DOT_repair_orders.order_date,
                default_DOT_repair_orders.dispatched_date,
                default_DOT_repair_orders.required_date,
                default_DOT_repair_order_details.discount,
                default_DOT_repair_order_details.price,
                default_DOT_repair_order_details.quantity,
                default_DOT_repair_order_details.repair_type_id,
                default_DOT_repair_order_details.price * default_DOT_repair_order_details.quantity AS total_repair_cost,
                default_DOT_repair_orders.dispatched_date - default_DOT_repair_orders.order_date AS time_to_dispatch,
                default_DOT_repair_orders.dispatched_date - default_DOT_repair_orders.required_date AS dispatch_delay
             FROM roads.repair_orders AS default_DOT_repair_orders JOIN roads.repair_order_details AS default_DOT_repair_order_details ON default_DOT_repair_orders.repair_order_id = default_DOT_repair_order_details.repair_order_id)
             AS default_DOT_repair_orders_fact
            )
            SELECT  default_DOT_repair_orders_fact.default_DOT_repair_orders_fact_DOT_discount,
                default_DOT_repair_orders_fact.default_DOT_repair_orders_fact_DOT_price,
                default_DOT_repair_orders_fact.default_DOT_dispatcher_DOT_dispatcher_id
             FROM default_DOT_repair_orders_fact""",
            [
                {
                    "column": "discount",
                    "name": "default_DOT_repair_orders_fact_DOT_discount",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.discount",
                    "semantic_type": "measure",
                    "type": "float",
                },
                {
                    "column": "price",
                    "name": "default_DOT_repair_orders_fact_DOT_price",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.price",
                    "semantic_type": "measure",
                    "type": "float",
                },
                {
                    "column": "dispatcher_id",
                    "name": "default_DOT_dispatcher_DOT_dispatcher_id",
                    "node": "default.dispatcher",
                    "semantic_entity": "default.dispatcher.dispatcher_id",
                    "semantic_type": "dimension",
                    "type": "int",
                },
            ],
            [
                (0.05000000074505806, 63708.0, 3),
                (0.05000000074505806, 67253.0, 1),
                (0.05000000074505806, 66808.0, 2),
                (0.05000000074505806, 18497.0, 1),
                (0.05000000074505806, 76463.0, 2),
                (0.05000000074505806, 87858.0, 2),
                (0.05000000074505806, 63918.0, 2),
                (0.05000000074505806, 21083.0, 3),
                (0.05000000074505806, 74555.0, 2),
                (0.05000000074505806, 27222.0, 3),
                (0.05000000074505806, 73600.0, 1),
                (0.009999999776482582, 54901.0, 3),
                (0.009999999776482582, 51594.0, 1),
                (0.009999999776482582, 65114.0, 2),
                (0.009999999776482582, 48919.0, 3),
                (0.009999999776482582, 70418.0, 3),
                (0.009999999776482582, 29684.0, 3),
                (0.009999999776482582, 62928.0, 1),
                (0.009999999776482582, 97916.0, 3),
                (0.009999999776482582, 44120.0, 1),
                (0.009999999776482582, 53374.0, 3),
                (0.009999999776482582, 87289.0, 1),
                (0.009999999776482582, 92366.0, 1),
                (0.009999999776482582, 47857.0, 2),
                (0.009999999776482582, 68745.0, 2),
            ],
        ),
        # Two metrics with different measures + two dimensions from different sources
        (
            ["default.avg_time_to_dispatch", "default.total_repair_cost"],
            ["default.us_state.state_name", "default.dispatcher.company_name"],
            ["default.us_state.state_name = 'New Jersey'"],
            """WITH
default_DOT_repair_orders_fact AS (SELECT  default_DOT_repair_orders_fact.total_repair_cost default_DOT_repair_orders_fact_DOT_total_repair_cost,
    default_DOT_repair_orders_fact.time_to_dispatch default_DOT_repair_orders_fact_DOT_time_to_dispatch,
    default_DOT_us_state.state_name default_DOT_us_state_DOT_state_name,
    default_DOT_dispatcher.company_name default_DOT_dispatcher_DOT_company_name
 FROM (SELECT  default_DOT_repair_orders.repair_order_id,
    default_DOT_repair_orders.municipality_id,
    default_DOT_repair_orders.hard_hat_id,
    default_DOT_repair_orders.dispatcher_id,
    default_DOT_repair_orders.order_date,
    default_DOT_repair_orders.dispatched_date,
    default_DOT_repair_orders.required_date,
    default_DOT_repair_order_details.discount,
    default_DOT_repair_order_details.price,
    default_DOT_repair_order_details.quantity,
    default_DOT_repair_order_details.repair_type_id,
    default_DOT_repair_order_details.price * default_DOT_repair_order_details.quantity AS total_repair_cost,
    default_DOT_repair_orders.dispatched_date - default_DOT_repair_orders.order_date AS time_to_dispatch,
    default_DOT_repair_orders.dispatched_date - default_DOT_repair_orders.required_date AS dispatch_delay
 FROM roads.repair_orders AS default_DOT_repair_orders JOIN roads.repair_order_details AS default_DOT_repair_order_details ON default_DOT_repair_orders.repair_order_id = default_DOT_repair_order_details.repair_order_id)
 AS default_DOT_repair_orders_fact LEFT JOIN (SELECT  default_DOT_dispatchers.dispatcher_id,
    default_DOT_dispatchers.company_name
 FROM roads.dispatchers AS default_DOT_dispatchers)
 AS default_DOT_dispatcher ON default_DOT_repair_orders_fact.dispatcher_id = default_DOT_dispatcher.dispatcher_id
LEFT JOIN (SELECT  default_DOT_hard_hats.hard_hat_id,
    default_DOT_hard_hats.state
 FROM roads.hard_hats AS default_DOT_hard_hats)
 AS default_DOT_hard_hat ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
LEFT JOIN (SELECT  default_DOT_us_states.state_name,
    default_DOT_us_states.state_abbr AS state_short
 FROM roads.us_states AS default_DOT_us_states)
 AS default_DOT_us_state ON default_DOT_hard_hat.state = default_DOT_us_state.state_short
 WHERE  default_DOT_us_state.state_name = 'New Jersey'

)

SELECT  default_DOT_repair_orders_fact.default_DOT_repair_orders_fact_DOT_total_repair_cost,
    default_DOT_repair_orders_fact.default_DOT_repair_orders_fact_DOT_time_to_dispatch,
    default_DOT_repair_orders_fact.default_DOT_us_state_DOT_state_name,
    default_DOT_repair_orders_fact.default_DOT_dispatcher_DOT_company_name
 FROM default_DOT_repair_orders_fact""",
            [
                {
                    "column": "total_repair_cost",
                    "name": "default_DOT_repair_orders_fact_DOT_total_repair_cost",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.total_repair_cost",
                    "semantic_type": "measure",
                    "type": "float",
                },
                {
                    "column": "time_to_dispatch",
                    "name": "default_DOT_repair_orders_fact_DOT_time_to_dispatch",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.time_to_dispatch",
                    "semantic_type": "measure",
                    "type": "timestamp",
                },
                {
                    "column": "state_name",
                    "name": "default_DOT_us_state_DOT_state_name",
                    "node": "default.us_state",
                    "semantic_entity": "default.us_state.state_name",
                    "semantic_type": "dimension",
                    "type": "string",
                },
                {
                    "column": "company_name",
                    "name": "default_DOT_dispatcher_DOT_company_name",
                    "node": "default.dispatcher",
                    "semantic_entity": "default.dispatcher.company_name",
                    "semantic_type": "dimension",
                    "type": "string",
                },
            ],
            [
                (92366.0, 204, "New Jersey", "Pothole Pete"),
                (44120.0, 196, "New Jersey", "Pothole Pete"),
                (18497.0, 146, "New Jersey", "Pothole Pete"),
                (63708.0, 150, "New Jersey", "Federal Roads Group"),
            ],
        ),
        (
            # Two parent transforms (requires COALESCE of dimensions across them), no filters
            [
                "default.avg_time_to_dispatch",
                "default.total_repair_cost",
                "default.num_repair_orders",
                "default.total_repair_order_discounts",
                "default.avg_length_of_employment",
                "default.avg_repair_price",
            ],
            ["default.us_state.state_name"],
            [],
            """WITH
default_DOT_repair_orders_fact AS (SELECT  default_DOT_repair_orders_fact.repair_order_id default_DOT_repair_orders_fact_DOT_repair_order_id,
    default_DOT_repair_orders_fact.discount default_DOT_repair_orders_fact_DOT_discount,
    default_DOT_repair_orders_fact.price default_DOT_repair_orders_fact_DOT_price,
    default_DOT_repair_orders_fact.total_repair_cost default_DOT_repair_orders_fact_DOT_total_repair_cost,
    default_DOT_repair_orders_fact.time_to_dispatch default_DOT_repair_orders_fact_DOT_time_to_dispatch,
    default_DOT_us_state.state_name default_DOT_us_state_DOT_state_name
 FROM (SELECT  default_DOT_repair_orders.repair_order_id,
    default_DOT_repair_orders.municipality_id,
    default_DOT_repair_orders.hard_hat_id,
    default_DOT_repair_orders.dispatcher_id,
    default_DOT_repair_orders.order_date,
    default_DOT_repair_orders.dispatched_date,
    default_DOT_repair_orders.required_date,
    default_DOT_repair_order_details.discount,
    default_DOT_repair_order_details.price,
    default_DOT_repair_order_details.quantity,
    default_DOT_repair_order_details.repair_type_id,
    default_DOT_repair_order_details.price * default_DOT_repair_order_details.quantity AS total_repair_cost,
    default_DOT_repair_orders.dispatched_date - default_DOT_repair_orders.order_date AS time_to_dispatch,
    default_DOT_repair_orders.dispatched_date - default_DOT_repair_orders.required_date AS dispatch_delay
 FROM roads.repair_orders AS default_DOT_repair_orders JOIN roads.repair_order_details AS default_DOT_repair_order_details ON default_DOT_repair_orders.repair_order_id = default_DOT_repair_order_details.repair_order_id)
 AS default_DOT_repair_orders_fact LEFT JOIN (SELECT  default_DOT_hard_hats.hard_hat_id,
    default_DOT_hard_hats.state
 FROM roads.hard_hats AS default_DOT_hard_hats)
 AS default_DOT_hard_hat ON default_DOT_repair_orders_fact.hard_hat_id = default_DOT_hard_hat.hard_hat_id
LEFT JOIN (SELECT  default_DOT_us_states.state_name,
    default_DOT_us_states.state_abbr AS state_short
 FROM roads.us_states AS default_DOT_us_states)
 AS default_DOT_us_state ON default_DOT_hard_hat.state = default_DOT_us_state.state_short

),
default_DOT_hard_hat AS (SELECT  default_DOT_hard_hat.hire_date default_DOT_hard_hat_DOT_hire_date,
    default_DOT_us_state.state_name default_DOT_us_state_DOT_state_name
 FROM (SELECT  default_DOT_hard_hats.hard_hat_id,
    default_DOT_hard_hats.last_name,
    default_DOT_hard_hats.first_name,
    default_DOT_hard_hats.title,
    default_DOT_hard_hats.birth_date,
    default_DOT_hard_hats.hire_date,
    default_DOT_hard_hats.address,
    default_DOT_hard_hats.city,
    default_DOT_hard_hats.state,
    default_DOT_hard_hats.postal_code,
    default_DOT_hard_hats.country,
    default_DOT_hard_hats.manager,
    default_DOT_hard_hats.contractor_id
 FROM roads.hard_hats AS default_DOT_hard_hats)
 AS default_DOT_hard_hat LEFT JOIN (SELECT  default_DOT_us_states.state_name,
    default_DOT_us_states.state_abbr AS state_short
 FROM roads.us_states AS default_DOT_us_states)
 AS default_DOT_us_state ON default_DOT_hard_hat.state = default_DOT_us_state.state_short
)
SELECT  default_DOT_repair_orders_fact.default_DOT_repair_orders_fact_DOT_repair_order_id,
    default_DOT_repair_orders_fact.default_DOT_repair_orders_fact_DOT_discount,
    default_DOT_repair_orders_fact.default_DOT_repair_orders_fact_DOT_price,
    default_DOT_repair_orders_fact.default_DOT_repair_orders_fact_DOT_total_repair_cost,
    default_DOT_repair_orders_fact.default_DOT_repair_orders_fact_DOT_time_to_dispatch,
    default_DOT_hard_hat.default_DOT_hard_hat_DOT_hire_date,
    COALESCE(default_DOT_repair_orders_fact.default_DOT_us_state_DOT_state_name, default_DOT_hard_hat.default_DOT_us_state_DOT_state_name) default_DOT_us_state_DOT_state_name
 FROM default_DOT_repair_orders_fact FULL OUTER JOIN default_DOT_hard_hat ON default_DOT_repair_orders_fact.default_DOT_us_state_DOT_state_name = default_DOT_hard_hat.default_DOT_us_state_DOT_state_name""",
            [
                {
                    "column": "repair_order_id",
                    "name": "default_DOT_repair_orders_fact_DOT_repair_order_id",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.repair_order_id",
                    "semantic_type": "measure",
                    "type": "int",
                },
                {
                    "column": "discount",
                    "name": "default_DOT_repair_orders_fact_DOT_discount",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.discount",
                    "semantic_type": "measure",
                    "type": "float",
                },
                {
                    "column": "price",
                    "name": "default_DOT_repair_orders_fact_DOT_price",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.price",
                    "semantic_type": "measure",
                    "type": "float",
                },
                {
                    "column": "total_repair_cost",
                    "name": "default_DOT_repair_orders_fact_DOT_total_repair_cost",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.total_repair_cost",
                    "semantic_type": "measure",
                    "type": "float",
                },
                {
                    "column": "time_to_dispatch",
                    "name": "default_DOT_repair_orders_fact_DOT_time_to_dispatch",
                    "node": "default.repair_orders_fact",
                    "semantic_entity": "default.repair_orders_fact.time_to_dispatch",
                    "semantic_type": "measure",
                    "type": "timestamp",
                },
                {
                    "column": "hire_date",
                    "name": "default_DOT_hard_hat_DOT_hire_date",
                    "node": "default.hard_hat",
                    "semantic_entity": "default.hard_hat.hire_date",
                    "semantic_type": "measure",
                    "type": "timestamp",
                },
                {
                    "column": "state_name",
                    "name": "default_DOT_us_state_DOT_state_name",
                    "node": "default.us_state",
                    "semantic_entity": "default.us_state.state_name",
                    "semantic_type": "dimension",
                    "type": "string",
                },
            ],
            [
                (
                    10012,
                    0.009999999776482582,
                    54901.0,
                    54901.0,
                    217,
                    datetime.date(2013, 3, 5),
                    "Arizona",
                ),
                (
                    10017,
                    0.009999999776482582,
                    29684.0,
                    29684.0,
                    231,
                    datetime.date(2003, 4, 14),
                    "Connecticut",
                ),
                (
                    10025,
                    0.009999999776482582,
                    68745.0,
                    68745.0,
                    203,
                    datetime.date(2013, 10, 17),
                    "Georgia",
                ),
                (
                    10009,
                    0.05000000074505806,
                    74555.0,
                    74555.0,
                    220,
                    datetime.date(1990, 7, 2),
                    "Massachusetts",
                ),
                (
                    10024,
                    0.009999999776482582,
                    47857.0,
                    47857.0,
                    204,
                    datetime.date(2012, 1, 13),
                    "Michigan",
                ),
                (
                    10023,
                    0.009999999776482582,
                    92366.0,
                    92366.0,
                    204,
                    datetime.date(2009, 2, 6),
                    "New Jersey",
                ),
                (
                    10021,
                    0.009999999776482582,
                    53374.0,
                    53374.0,
                    205,
                    datetime.date(2013, 1, 2),
                    "New York",
                ),
                (
                    10016,
                    0.009999999776482582,
                    70418.0,
                    70418.0,
                    232,
                    datetime.date(2020, 11, 15),
                    "Oklahoma",
                ),
                (
                    10013,
                    0.009999999776482582,
                    51594.0,
                    51594.0,
                    216,
                    datetime.date(2003, 2, 2),
                    "Pennsylvania",
                ),
                (
                    10005,
                    0.05000000074505806,
                    76463.0,
                    76463.0,
                    145,
                    datetime.date(2013, 3, 5),
                    "Arizona",
                ),
                (
                    10015,
                    0.009999999776482582,
                    48919.0,
                    48919.0,
                    233,
                    datetime.date(2003, 4, 14),
                    "Connecticut",
                ),
                (
                    10018,
                    0.009999999776482582,
                    62928.0,
                    62928.0,
                    230,
                    datetime.date(2013, 10, 17),
                    "Georgia",
                ),
                (
                    10006,
                    0.05000000074505806,
                    87858.0,
                    87858.0,
                    144,
                    datetime.date(1990, 7, 2),
                    "Massachusetts",
                ),
                (
                    10022,
                    0.009999999776482582,
                    87289.0,
                    87289.0,
                    204,
                    datetime.date(2012, 1, 13),
                    "Michigan",
                ),
                (
                    10020,
                    0.009999999776482582,
                    44120.0,
                    44120.0,
                    196,
                    datetime.date(2009, 2, 6),
                    "New Jersey",
                ),
                (
                    10011,
                    0.05000000074505806,
                    73600.0,
                    73600.0,
                    218,
                    datetime.date(2003, 2, 2),
                    "Pennsylvania",
                ),
                (
                    10014,
                    0.009999999776482582,
                    65114.0,
                    65114.0,
                    216,
                    datetime.date(2013, 10, 17),
                    "Georgia",
                ),
                (
                    10002,
                    0.05000000074505806,
                    67253.0,
                    67253.0,
                    149,
                    datetime.date(1990, 7, 2),
                    "Massachusetts",
                ),
                (
                    10019,
                    0.009999999776482582,
                    97916.0,
                    97916.0,
                    199,
                    datetime.date(2012, 1, 13),
                    "Michigan",
                ),
                (
                    10004,
                    0.05000000074505806,
                    18497.0,
                    18497.0,
                    146,
                    datetime.date(2009, 2, 6),
                    "New Jersey",
                ),
                (
                    10010,
                    0.05000000074505806,
                    27222.0,
                    27222.0,
                    219,
                    datetime.date(2003, 2, 2),
                    "Pennsylvania",
                ),
                (
                    10008,
                    0.05000000074505806,
                    21083.0,
                    21083.0,
                    223,
                    datetime.date(2012, 1, 13),
                    "Michigan",
                ),
                (
                    10001,
                    0.05000000074505806,
                    63708.0,
                    63708.0,
                    150,
                    datetime.date(2009, 2, 6),
                    "New Jersey",
                ),
                (
                    10007,
                    0.05000000074505806,
                    63918.0,
                    63918.0,
                    224,
                    datetime.date(2003, 2, 2),
                    "Pennsylvania",
                ),
                (
                    10003,
                    0.05000000074505806,
                    66808.0,
                    66808.0,
                    146,
                    datetime.date(2012, 1, 13),
                    "Michigan",
                ),
            ],
        ),
    ],
)
def test_measures_sql_with_filters(  # pylint: disable=too-many-arguments
    metrics,
    dimensions,
    filters,
    sql,
    columns,
    rows,
    client_with_roads: TestClient,
    duckdb_conn: duckdb.DuckDBPyConnection,  # pylint: disable=c-extension-no-member
):
    """
    Test ``GET /sql/measures`` with various metrics, filters, and dimensions.
    """
    sql_params = {
        "metrics": metrics,
        "dimensions": dimensions,
        "filters": filters,
    }
    response = client_with_roads.get("/sql/measures", params=sql_params)
    data = response.json()
    assert str(parse(str(data["sql"]))) == str(parse(str(sql)))
    result = duckdb_conn.sql(data["sql"])
    assert result.fetchall() == rows
    assert data["columns"] == columns
