"""Tests for the /sql/ endpoint"""
import pytest
from sqlmodel import Session
from starlette.testclient import TestClient

from dj.models import Column, Database, Node
from dj.models.node import NodeRevision, NodeType
from dj.sql.parsing.types import StringType
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
        columns=[Column(name="one", type=StringType())],
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
        "sql": "SELECT  COUNT(*) col0 \n FROM rev.my_table AS my_table\n",
        "columns": [{"name": "col0", "type": "bigint"}],
        "dialect": None,
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
            SELECT
              repair_orders.dispatched_date,
              repair_orders.dispatcher_id,
              repair_orders.hard_hat_id,
              repair_orders.municipality_id,
              repair_orders.order_date,
              repair_orders.repair_order_id,
              repair_orders.required_date
            FROM roads.repair_orders AS repair_orders
            LEFT OUTER JOIN (
              SELECT
                hard_hats.hard_hat_id,
                hard_hats.state
              FROM roads.hard_hats AS hard_hats
            ) AS hard_hat ON repair_orders.hard_hat_id = hard_hat.hard_hat_id
            WHERE
              hard_hat.state = 'CA'
            """,
        ),
        # querying source node with filters directly on the node
        (
            "repair_orders",
            [],
            ["repair_orders.order_date='2009-08-14'"],
            """
            SELECT
              repair_orders.repair_order_id,
              repair_orders.municipality_id,
              repair_orders.hard_hat_id,
              repair_orders.order_date,
              repair_orders.required_date,
              repair_orders.dispatched_date,
              repair_orders.dispatcher_id
            FROM roads.repair_orders AS repair_orders
            WHERE
              repair_orders.order_date = '2009-08-14'
            """,
        ),
        # querying transform node with filters on joinable dimension
        (
            "long_events",
            [],
            ["country_dim.events_cnt >= 20"],
            """
            SELECT
              event_source.event_id,
              event_source.event_latency,
              event_source.device_id,
              event_source.country
            FROM logs.log_events AS event_source
            LEFT OUTER JOIN (
              SELECT
                event_source.country,
                COUNT(DISTINCT event_source.event_id) AS events_cnt
              FROM logs.log_events AS event_source
              GROUP BY
                event_source.country
            ) AS country_dim
                    ON event_source.country = country_dim.country
             WHERE
               event_source.event_latency > 1000000 AND
               country_dim.events_cnt >= 20
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
            FROM logs.log_events AS event_source
            WHERE event_source.event_latency > 1000000 AND event_source.device_id = 'Android'
            """,
        ),
        (
            "municipality_dim",
            [],
            ["state_id = 'CA'"],
            """
            SELECT
              municipality.contact_name,
              municipality.contact_title,
              municipality.local_region,
              municipality.municipality_id,
              municipality_municipality_type.municipality_type_id,
              municipality_type.municipality_type_desc,
              municipality.state_id
            FROM roads.municipality AS municipality
            LEFT JOIN roads.municipality_municipality_type AS municipality_municipality_type
            ON municipality.municipality_id = municipality_municipality_type.municipality_id
            LEFT JOIN roads.municipality_type AS municipality_type
            ON municipality_municipality_type.municipality_type_id
               = municipality_type.municipality_type_desc
            WHERE
              municipality.state_id = 'CA'""",
        ),
        (
            "num_repair_orders",
            [],
            [],
            """
            SELECT
              count(repair_orders.repair_order_id) AS num_repair_orders
            FROM roads.repair_orders AS repair_orders
            """,
        ),
        (
            "num_repair_orders",
            ["hard_hat.state"],
            ["repair_orders.dispatcher_id=1", "hard_hat.state='AZ'"],
            """
            SELECT
              hard_hat.state,
              count(repair_orders.repair_order_id) AS num_repair_orders
            FROM roads.repair_orders AS repair_orders
            LEFT OUTER JOIN (
              SELECT
                hard_hats.hard_hat_id,
                hard_hats.state
              FROM roads.hard_hats AS hard_hats
            ) AS hard_hat
            ON repair_orders.hard_hat_id = hard_hat.hard_hat_id
            WHERE
              repair_orders.dispatcher_id = 1 AND
              hard_hat.state = 'AZ'
            GROUP BY
              hard_hat.state
            """,
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
            """
            SELECT
              dispatcher.company_name,
              hard_hat.city,
              hard_hat.last_name,
              municipality_dim.local_region,
              count(repair_orders.repair_order_id) AS num_repair_orders
            FROM roads.repair_orders AS repair_orders
            LEFT OUTER JOIN (
              SELECT
                dispatchers.company_name,
                dispatchers.dispatcher_id,
                dispatchers.phone
              FROM roads.dispatchers AS dispatchers
            ) AS dispatcher
              ON repair_orders.dispatcher_id = dispatcher.dispatcher_id
            LEFT OUTER JOIN (
              SELECT
                hard_hats.city,
                hard_hats.hard_hat_id,
                hard_hats.last_name,
                hard_hats.state
              FROM roads.hard_hats AS hard_hats
            ) AS hard_hat
              ON repair_orders.hard_hat_id = hard_hat.hard_hat_id
            LEFT OUTER JOIN (
              SELECT
                municipality.local_region,
                municipality.municipality_id
              FROM roads.municipality AS municipality
              LEFT JOIN roads.municipality_municipality_type AS municipality_municipality_type
                ON municipality.municipality_id =
                   municipality_municipality_type.municipality_id
              LEFT JOIN roads.municipality_type AS municipality_type
                ON municipality_municipality_type.municipality_type_id =
                   municipality_type.municipality_type_desc
            ) AS municipality_dim
            ON repair_orders.municipality_id = municipality_dim.municipality_id
            WHERE
              repair_orders.dispatcher_id = 1
              AND hard_hat.state != 'AZ'
              AND dispatcher.phone = '4082021022'
              AND repair_orders.order_date >= '2020-01-01'
            GROUP BY
              hard_hat.city,
              hard_hat.last_name,
              dispatcher.company_name,
              municipality_dim.local_region
            """,
        ),
        # metric with second-order dimension
        (
            "avg_repair_price",
            ["hard_hat.city"],
            [],
            """
            SELECT
              avg(repair_order_details.price) AS avg_repair_price,
              hard_hat.city
            FROM roads.repair_order_details AS repair_order_details
            LEFT OUTER JOIN (
              SELECT
                repair_orders.dispatcher_id,
                repair_orders.hard_hat_id,
                repair_orders.municipality_id,
                repair_orders.repair_order_id
              FROM roads.repair_orders AS repair_orders
            ) AS repair_order
              ON repair_order_details.repair_order_id = repair_order.repair_order_id
            LEFT OUTER JOIN (
              SELECT
                hard_hats.city,
                hard_hats.hard_hat_id,
                hard_hats.state
              FROM roads.hard_hats AS hard_hats
            ) AS hard_hat
              ON repair_order.hard_hat_id = hard_hat.hard_hat_id
            GROUP BY
              hard_hat.city
            """,
        ),
        # metric with multiple nth order dimensions that can share some of the joins
        (
            "avg_repair_price",
            ["hard_hat.city", "dispatcher.company_name"],
            [],
            """
            SELECT
              avg(repair_order_details.price) AS avg_repair_price,
              dispatcher.company_name,
              hard_hat.city
            FROM roads.repair_order_details AS repair_order_details
            LEFT OUTER JOIN (
              SELECT
                repair_orders.dispatcher_id,
                repair_orders.hard_hat_id,
                repair_orders.municipality_id,
                repair_orders.repair_order_id
              FROM roads.repair_orders AS repair_orders
            ) AS repair_order ON repair_order_details.repair_order_id = repair_order.repair_order_id
            LEFT OUTER JOIN (
              SELECT
                dispatchers.company_name,
                dispatchers.dispatcher_id
              FROM roads.dispatchers AS dispatchers
            ) AS dispatcher ON repair_order.dispatcher_id = dispatcher.dispatcher_id
            LEFT OUTER JOIN (
              SELECT
                hard_hats.city,
                hard_hats.hard_hat_id,
                hard_hats.state
              FROM roads.hard_hats AS hard_hats
            ) AS hard_hat ON repair_order.hard_hat_id = hard_hat.hard_hat_id
            GROUP BY
              hard_hat.city,
              dispatcher.company_name
            """,
        ),
        # dimension with aliased join key should just use the alias directly
        (
            "num_repair_orders",
            ["us_state.state_region_description"],
            [],
            """
            SELECT
              count(repair_orders.repair_order_id) AS num_repair_orders,
              us_state.state_region_description
            FROM roads.repair_orders AS repair_orders
            LEFT OUTER JOIN (
              SELECT
                hard_hats.hard_hat_id,
                hard_hats.state
              FROM roads.hard_hats AS hard_hats
            ) AS hard_hat
            ON repair_orders.hard_hat_id = hard_hat.hard_hat_id
            LEFT OUTER JOIN (
              SELECT
                us_states.state_id,
                us_region.us_region_description AS state_region_description,
                us_states.state_abbr AS state_short
              FROM roads.us_states AS us_states
              LEFT JOIN roads.us_region AS us_region
              ON us_states.state_region = us_region.us_region_id
            ) AS us_state
            ON hard_hat.state = us_state.state_short
            GROUP BY
              us_state.state_region_description
            """,
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
    Test ``GET /sql/{node_name}/`` with various filters and dimensions.
    """
    response = client_with_examples.get(
        f"/sql/{node_name}/",
        params={"dimensions": dimensions, "filters": filters},
    )
    data = response.json()
    assert compare_query_strings(data["sql"], sql)


@pytest.mark.parametrize(
    "node_name, dimensions, filters, sql",
    [
        # querying on source node with filter on joinable dimension
        (
            "foo.bar.repair_orders",
            [],
            ["foo.bar.hard_hat.state='CA'"],
            """
            SELECT
              foo_DOT_bar_DOT_repair_orders.dispatched_date,
              foo_DOT_bar_DOT_repair_orders.dispatcher_id,
              foo_DOT_bar_DOT_repair_orders.hard_hat_id,
              foo_DOT_bar_DOT_repair_orders.municipality_id,
              foo_DOT_bar_DOT_repair_orders.order_date,
              foo_DOT_bar_DOT_repair_orders.repair_order_id,
              foo_DOT_bar_DOT_repair_orders.required_date
            FROM roads.repair_orders AS foo_DOT_bar_DOT_repair_orders
            LEFT OUTER JOIN (
              SELECT
                foo_DOT_bar_DOT_hard_hats.hard_hat_id,
                foo_DOT_bar_DOT_hard_hats.state
              FROM roads.hard_hats AS foo_DOT_bar_DOT_hard_hats
            ) AS foo_DOT_bar_DOT_hard_hat
            ON foo_DOT_bar_DOT_repair_orders.hard_hat_id =
               foo_DOT_bar_DOT_hard_hat.hard_hat_id
            WHERE  foo_DOT_bar_DOT_hard_hat.state = 'CA'
            """,
        ),
        # querying source node with filters directly on the node
        (
            "foo.bar.repair_orders",
            [],
            ["foo.bar.repair_orders.order_date='2009-08-14'"],
            """
            SELECT
              foo_DOT_bar_DOT_repair_orders.dispatched_date,
              foo_DOT_bar_DOT_repair_orders.dispatcher_id,
              foo_DOT_bar_DOT_repair_orders.hard_hat_id,
              foo_DOT_bar_DOT_repair_orders.municipality_id,
              foo_DOT_bar_DOT_repair_orders.order_date,
              foo_DOT_bar_DOT_repair_orders.repair_order_id,
              foo_DOT_bar_DOT_repair_orders.required_date
            FROM roads.repair_orders AS foo_DOT_bar_DOT_repair_orders
            WHERE
              foo_DOT_bar_DOT_repair_orders.order_date = '2009-08-14'
            """,
        ),
        (
            "foo.bar.municipality_dim",
            [],
            ["state_id = 'CA'"],
            """
            SELECT
              foo_DOT_bar_DOT_municipality.contact_name,
              foo_DOT_bar_DOT_municipality.contact_title,
              foo_DOT_bar_DOT_municipality.local_region,
              foo_DOT_bar_DOT_municipality.municipality_id,
              foo_DOT_bar_DOT_municipality_municipality_type.municipality_type_id,
              foo_DOT_bar_DOT_municipality_type.municipality_type_desc,
              foo_DOT_bar_DOT_municipality.state_id
            FROM roads.municipality AS foo_DOT_bar_DOT_municipality
            LEFT JOIN roads.municipality_municipality_type
              AS foo_DOT_bar_DOT_municipality_municipality_type
              ON foo_DOT_bar_DOT_municipality.municipality_id =
                 foo_DOT_bar_DOT_municipality_municipality_type.municipality_id
            LEFT JOIN roads.municipality_type
              AS foo_DOT_bar_DOT_municipality_type
              ON foo_DOT_bar_DOT_municipality_municipality_type.municipality_type_id =
                 foo_DOT_bar_DOT_municipality_type.municipality_type_desc
            WHERE
              foo_DOT_bar_DOT_municipality.state_id = 'CA'
            """,
        ),
        (
            "foo.bar.num_repair_orders",
            [],
            [],
            """
            SELECT
              count(foo_DOT_bar_DOT_repair_orders.repair_order_id) AS num_repair_orders
            FROM roads.repair_orders AS foo_DOT_bar_DOT_repair_orders
            """,
        ),
        (
            "foo.bar.num_repair_orders",
            ["foo.bar.hard_hat.state"],
            ["foo.bar.repair_orders.dispatcher_id=1", "foo.bar.hard_hat.state='AZ'"],
            """
            SELECT
              foo_DOT_bar_DOT_hard_hat.state,
              count(foo_DOT_bar_DOT_repair_orders.repair_order_id) AS num_repair_orders
            FROM roads.repair_orders AS foo_DOT_bar_DOT_repair_orders
            LEFT OUTER JOIN (
              SELECT
                foo_DOT_bar_DOT_hard_hats.hard_hat_id,
                foo_DOT_bar_DOT_hard_hats.state
              FROM roads.hard_hats AS foo_DOT_bar_DOT_hard_hats
            ) AS foo_DOT_bar_DOT_hard_hat
              ON foo_DOT_bar_DOT_repair_orders.hard_hat_id =
                 foo_DOT_bar_DOT_hard_hat.hard_hat_id
            WHERE
              foo_DOT_bar_DOT_repair_orders.dispatcher_id = 1
              AND foo_DOT_bar_DOT_hard_hat.state = 'AZ'
            GROUP BY
              foo_DOT_bar_DOT_hard_hat.state
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
            """
            SELECT
              foo_DOT_bar_DOT_dispatcher.company_name,
              foo_DOT_bar_DOT_hard_hat.city,
              foo_DOT_bar_DOT_hard_hat.last_name,
              foo_DOT_bar_DOT_municipality_dim.local_region,
              count(foo_DOT_bar_DOT_repair_orders.repair_order_id) AS num_repair_orders
            FROM roads.repair_orders AS foo_DOT_bar_DOT_repair_orders
              LEFT OUTER JOIN (
                SELECT foo_DOT_bar_DOT_dispatchers.company_name,
                  foo_DOT_bar_DOT_dispatchers.dispatcher_id,
                  foo_DOT_bar_DOT_dispatchers.phone
                FROM roads.dispatchers AS foo_DOT_bar_DOT_dispatchers
              ) AS foo_DOT_bar_DOT_dispatcher
              ON foo_DOT_bar_DOT_repair_orders.dispatcher_id =
                 foo_DOT_bar_DOT_dispatcher.dispatcher_id
              LEFT OUTER JOIN (
                SELECT foo_DOT_bar_DOT_hard_hats.city,
                  foo_DOT_bar_DOT_hard_hats.hard_hat_id,
                  foo_DOT_bar_DOT_hard_hats.last_name,
                  foo_DOT_bar_DOT_hard_hats.state
                FROM roads.hard_hats AS foo_DOT_bar_DOT_hard_hats
              ) AS foo_DOT_bar_DOT_hard_hat
              ON foo_DOT_bar_DOT_repair_orders.hard_hat_id = foo_DOT_bar_DOT_hard_hat.hard_hat_id
              LEFT OUTER JOIN (
                SELECT foo_DOT_bar_DOT_municipality.local_region,
                  foo_DOT_bar_DOT_municipality.municipality_id
                FROM roads.municipality AS foo_DOT_bar_DOT_municipality
                  LEFT JOIN roads.municipality_municipality_type
                    AS foo_DOT_bar_DOT_municipality_municipality_type
                    ON foo_DOT_bar_DOT_municipality.municipality_id =
                       foo_DOT_bar_DOT_municipality_municipality_type.municipality_id
                  LEFT JOIN roads.municipality_type AS foo_DOT_bar_DOT_municipality_type
                  ON foo_DOT_bar_DOT_municipality_municipality_type.municipality_type_id =
                     foo_DOT_bar_DOT_municipality_type.municipality_type_desc
              ) AS foo_DOT_bar_DOT_municipality_dim
              ON foo_DOT_bar_DOT_repair_orders.municipality_id =
                 foo_DOT_bar_DOT_municipality_dim.municipality_id
            WHERE foo_DOT_bar_DOT_repair_orders.dispatcher_id = 1
              AND foo_DOT_bar_DOT_hard_hat.state != 'AZ'
              AND foo_DOT_bar_DOT_dispatcher.phone = '4082021022'
              AND foo_DOT_bar_DOT_repair_orders.order_date >= '2020-01-01'
            GROUP BY foo_DOT_bar_DOT_hard_hat.city,
              foo_DOT_bar_DOT_hard_hat.last_name,
              foo_DOT_bar_DOT_dispatcher.company_name,
              foo_DOT_bar_DOT_municipality_dim.local_region
            """,
        ),
        (
            "foo.bar.avg_repair_price",
            ["foo.bar.hard_hat.city"],
            [],
            """
            SELECT
              avg(foo_DOT_bar_DOT_repair_order_details.price) AS avg_repair_price,
              foo_DOT_bar_DOT_hard_hat.city
            FROM roads.repair_order_details AS foo_DOT_bar_DOT_repair_order_details
            LEFT OUTER JOIN (
              SELECT
                foo_DOT_bar_DOT_repair_orders.dispatcher_id,
                foo_DOT_bar_DOT_repair_orders.hard_hat_id,
                foo_DOT_bar_DOT_repair_orders.municipality_id,
                foo_DOT_bar_DOT_repair_orders.repair_order_id
              FROM roads.repair_orders AS foo_DOT_bar_DOT_repair_orders
            ) AS foo_DOT_bar_DOT_repair_order
            ON foo_DOT_bar_DOT_repair_order_details.repair_order_id
               = foo_DOT_bar_DOT_repair_order.repair_order_id
            LEFT OUTER JOIN (
              SELECT
                foo_DOT_bar_DOT_hard_hats.city,
                foo_DOT_bar_DOT_hard_hats.hard_hat_id
              FROM roads.hard_hats AS foo_DOT_bar_DOT_hard_hats
            ) AS foo_DOT_bar_DOT_hard_hat
            ON foo_DOT_bar_DOT_repair_order.hard_hat_id = foo_DOT_bar_DOT_hard_hat.hard_hat_id
            GROUP BY foo_DOT_bar_DOT_hard_hat.city
            """,
        ),
    ],
)
def test_sql_with_filters_on_namespaced_nodes(
    node_name,
    dimensions,
    filters,
    sql,
    client_with_examples: TestClient,
):
    """
    Test ``GET /sql/{node_name}/`` with various filters and dimensions using a
    version of the DJ roads database with namespaces.
    """
    response = client_with_examples.get(
        f"/sql/{node_name}/",
        params={
            "dimensions": dimensions,
            "filters": filters,
        },
    )
    data = response.json()
    assert compare_query_strings(data["sql"], sql)


def test_cross_join_unnest(client_with_examples: TestClient):
    """
    Verify cross join unnest on a joined in dimension works
    """
    client_with_examples.post(
        "/nodes/basic.corrected_patches/columns/color_id/"
        "?dimension=basic.paint_colors_trino&dimension_column=color_id",
    )
    response = client_with_examples.get(
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
      paint_colors_trino.color_id,
      basic_DOT_paint_colors_trino.color_name,
      AVG(basic_DOT_corrected_patches.luminosity) AS cnt
    FROM (
      SELECT
        CAST(basic_DOT_patches.color_id AS VARCHAR) color_id,
        basic_DOT_patches.color_name,
        basic_DOT_patches.garishness,
        basic_DOT_patches.luminosity,
        basic_DOT_patches.opacity
      FROM basic.patches AS basic_DOT_patches
    ) AS basic_DOT_corrected_patches
    LEFT OUTER JOIN (
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


def test_lateral_view_explode(client_with_examples: TestClient):
    """
    Verify lateral view explode on a joined in dimension works
    """
    client_with_examples.post(
        "/nodes/basic.corrected_patches/columns/color_id/"
        "?dimension=basic.paint_colors_spark&dimension_column=color_id",
    )
    response = client_with_examples.get(
        "/sql/basic.avg_luminosity_patches/",
        params={
            "filters": [],
            "dimensions": [
                "basic.paint_colors_spark.color_id",
                "basic.paint_colors_spark.color_name",
            ],
        },
    )
    expected = """
    SELECT
      paint_colors_spark.color_id,
      basic_DOT_paint_colors_spark.color_name,
      AVG(basic_DOT_corrected_patches.luminosity) AS cnt
    FROM (
      SELECT
        CAST(basic_DOT_patches.color_id AS VARCHAR) color_id,
        basic_DOT_patches.color_name,
        basic_DOT_patches.garishness,
        basic_DOT_patches.luminosity,
        basic_DOT_patches.opacity
      FROM basic.patches AS basic_DOT_patches
    ) AS basic_DOT_corrected_patches
    LEFT OUTER JOIN (
      SELECT
        color_name color_name,
        color_id
      FROM (
        SELECT
          basic_DOT_murals.id,
          basic_DOT_murals.colors
        FROM basic.murals AS basic_DOT_murals
      ) murals
      LATERAL VIEW EXPLODE(murals.colors) AS color_id, color_name
    ) AS basic_DOT_paint_colors_spark
    ON basic_DOT_corrected_patches.color_id = basic_DOT_paint_colors_trino.color_id
    GROUP BY
      paint_colors_spark.color_id,
      basic_DOT_paint_colors_trino.color_name
    """
    query = response.json()["sql"]
    compare_query_strings(query, expected)
