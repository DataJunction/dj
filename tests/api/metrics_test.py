"""
Tests for the metrics API.
"""


from fastapi.testclient import TestClient
from sqlmodel import Session

from dj.models.column import Column
from dj.models.database import Database
from dj.models.node import Node, NodeRevision, NodeType
from dj.models.table import Table
from dj.typing import ColumnType
from tests.sql.utils import compare_query_strings


#
#
# def test_read_metrics(client_with_examples: TestClient) -> None:
#     """
#     Test ``GET /metrics/``.
#     """
#     response = client_with_examples.get("/metrics/")
#     data = response.json()
#
#     assert response.status_code == 200
#     assert len(data) > 10
#
#
# def test_read_metric(session: Session, client: TestClient) -> None:
#     """
#     Test ``GET /metric/{node_id}/``.
#     """
#     parent_rev = NodeRevision(
#         name="parent",
#         version="1",
#         tables=[
#             Table(
#                 database=Database(name="test", URI="sqlite://"),
#                 table="A",
#                 columns=[
#                     Column(name="ds", type=ColumnType.STR),
#                     Column(name="user_id", type=ColumnType.INT),
#                     Column(name="foo", type=ColumnType.FLOAT),
#                 ],
#             ),
#         ],
#         columns=[
#             Column(name="ds", type=ColumnType.STR),
#             Column(name="user_id", type=ColumnType.INT),
#             Column(name="foo", type=ColumnType.FLOAT),
#         ],
#     )
#     parent_node = Node(
#         name=parent_rev.name,
#         type=NodeType.SOURCE,
#         current_version="1",
#     )
#     parent_rev.node = parent_node
#
#     child_node = Node(
#         name="child",
#         type=NodeType.METRIC,
#         current_version="1",
#     )
#     child_rev = NodeRevision(
#         name=child_node.name,
#         node=child_node,
#         version="1",
#         query="SELECT COUNT(*) FROM parent",
#         parents=[parent_node],
#     )
#
#     session.add(child_rev)
#     session.commit()
#
#     response = client.get("/metrics/child/")
#     data = response.json()
#
#     assert response.status_code == 200
#     assert data["name"] == "child"
#     assert data["query"] == "SELECT COUNT(*) FROM parent"
#     assert data["dimensions"] == ["parent.ds", "parent.foo", "parent.user_id"]
#
#
# def test_read_metrics_errors(session: Session, client: TestClient) -> None:
#     """
#     Test errors on ``GET /metrics/{node_id}/``.
#     """
#     database = Database(name="test", URI="sqlite://")
#     node = Node(
#         name="a-metric",
#         type=NodeType.TRANSFORM,
#         current_version="1",
#     )
#     node_revision = NodeRevision(
#         name=node.name,
#         node=node,
#         version="1",
#         query="SELECT 1 AS col",
#     )
#     session.add(database)
#     session.add(node_revision)
#     session.execute("CREATE TABLE my_table (one TEXT)")
#     session.commit()
#
#     response = client.get("/metrics/foo")
#     assert response.status_code == 404
#     data = response.json()
#     assert data["message"] == "A node with name `foo` does not exist."
#
#     response = client.get("/metrics/a-metric")
#     assert response.status_code == 400
#     assert response.json() == {"detail": "Not a metric node: `a-metric`"}
#
#
# def test_common_dimensions(
#     client_with_examples: TestClient,
# ) -> None:
#     """
#     Test ``GET /metrics/common/dimensions``.
#     """
#     response = client_with_examples.get(
#         "/metrics/common/dimensions?metric=total_repair_order_discounts&metric=total_repair_cost",
#     )
#     assert response.status_code == 200
#     assert set(response.json()) == set(
#         [
#             "repair_order_details.discount",
#             "repair_order_details.repair_type_id",
#             "repair_order_details.repair_order_id",
#             "repair_order.dispatched_date",
#             "repair_order.order_date",
#             "repair_order.required_date",
#             "repair_order.dispatcher_id",
#             "repair_order.municipality_id",
#             "repair_order_details.quantity",
#             "repair_order.repair_order_id",
#             "repair_order.hard_hat_id",
#             "repair_order_details.price",
#         ],
#     )
#


def test_comm2dimensions(
    client_with_examples: TestClient,
) -> None:
    """
    Test ``GET /metrics/avg_repair_order_discounts/``.
    """
    response = client_with_examples.get(
        "/sql/avg_repair_order_discounts/",
        params={
            "dimensions": ["hard_hat.state"],
        }
    )
    assert response.status_code == 200
    data = response.json()
    expected_sql = """
    SELECT
      avg(repair_order_details.price * repair_order_details.discount) AS avg_repair_order_discount,
	  hard_hat.state 
	FROM "roads"."repair_order_details" AS repair_order_details
    LEFT JOIN (
      SELECT
        repair_orders.dispatched_date,
	    repair_orders.dispatcher_id,
	    repair_orders.hard_hat_id,
	    repair_orders.municipality_id,
    	repair_orders.order_date,
	    repair_orders.repair_order_id,
	    repair_orders.required_date 
      FROM "roads"."repair_orders" AS repair_orders
    ) AS repair_order
        ON repair_order_details.repair_order_id = repair_order.repair_order_id
        LEFT JOIN (
          SELECT
            hard_hats.address,
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
        GROUP BY  hard_hat.state
    """
    assert compare_query_strings(expected_sql, data["sql"])


#
# def test_raise_common_dimensions_not_a_metric_node(
#     client_with_examples: TestClient,
# ) -> None:
#     """
#     Test raising ``GET /metrics/common/dimensions`` when not a metric node
#     """
#     response = client_with_examples.get(
#         "/metrics/common/dimensions?metric=total_repair_order_discounts&metric=payment_type",
#     )
#     assert response.status_code == 500
#     assert response.json()["message"] == "Not a metric node: payment_type"
#
#
# def test_raise_common_dimensions_metric_not_found(
#     client_with_examples: TestClient,
# ) -> None:
#     """
#     Test raising ``GET /metrics/common/dimensions`` when metric not found
#     """
#     response = client_with_examples.get(
#         "/metrics/common/dimensions?metric=foo&metric=bar",
#     )
#     assert response.status_code == 500
#     assert response.json() == {
#         "message": "Metric node not found: foo\nMetric node not found: bar",
#         "errors": [
#             {
#                 "code": 203,
#                 "message": "Metric node not found: foo",
#                 "debug": None,
#                 "context": "",
#             },
#             {
#                 "code": 203,
#                 "message": "Metric node not found: bar",
#                 "debug": None,
#                 "context": "",
#             },
#         ],
#         "warnings": [],
#     }
