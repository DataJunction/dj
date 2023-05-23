"""Tests for the /sql/ endpoint"""
# pylint: disable=line-too-long
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
            "default.repair_orders",
            [],
            ["default.hard_hat.state='CA'"],
            """
              SELECT  default_DOT_hard_hat.state,
                      default_DOT_repair_orders.dispatched_date,
                      default_DOT_repair_orders.dispatcher_id,
                      default_DOT_repair_orders.hard_hat_id,
                      default_DOT_repair_orders.municipality_id,
                      default_DOT_repair_orders.order_date,
                      default_DOT_repair_orders.repair_order_id,
                      default_DOT_repair_orders.required_date
              FROM roads.repair_orders AS default_DOT_repair_orders
              LEFT OUTER JOIN (SELECT  default_DOT_hard_hats.hard_hat_id,
                      default_DOT_hard_hats.state
              FROM roads.hard_hats AS default_DOT_hard_hats) AS default_DOT_hard_hat
              ON default_DOT_repair_orders.hard_hat_id = default_DOT_hard_hat.hard_hat_id
              WHERE  default_DOT_hard_hat.state = 'CA'
            """,
        ),
        # querying source node with filters directly on the node
        (
            "default.repair_orders",
            [],
            ["default.repair_orders.order_date='2009-08-14'"],
            """
              SELECT  default_DOT_repair_orders.dispatched_date,
                      default_DOT_repair_orders.dispatcher_id,
                      default_DOT_repair_orders.hard_hat_id,
                      default_DOT_repair_orders.municipality_id,
                      default_DOT_repair_orders.order_date,
                      default_DOT_repair_orders.repair_order_id,
                      default_DOT_repair_orders.required_date
              FROM roads.repair_orders AS default_DOT_repair_orders
              WHERE  default_DOT_repair_orders.order_date = '2009-08-14'
            """,
        ),
        # querying transform node with filters on joinable dimension
        (
            "default.long_events",
            [],
            ["default.country_dim.events_cnt >= 20"],
            """
              SELECT  default_DOT_event_source.country,
                      default_DOT_country_dim.events_cnt,
                      default_DOT_event_source.device_id,
                      default_DOT_event_source.event_id,
                      default_DOT_event_source.event_latency
              FROM logs.log_events AS default_DOT_event_source
              LEFT OUTER JOIN (SELECT  default_DOT_event_source.country,
                      COUNT( DISTINCT default_DOT_event_source.event_id) AS events_cnt
              FROM logs.log_events AS default_DOT_event_source
              GROUP BY  default_DOT_event_source.country) AS default_DOT_country_dim
              ON default_DOT_event_source.country = default_DOT_country_dim.country
              WHERE  default_DOT_event_source.event_latency > 1000000
              AND default_DOT_country_dim.events_cnt >= 20
            """,
        ),
        # querying transform node with filters directly on the node
        (
            "default.long_events",
            [],
            ["event_source.device_id = 'Android'"],
            """
              SELECT  default_DOT_event_source.country,
                      default_DOT_event_source.device_id,
                      default_DOT_event_source.event_id,
                      default_DOT_event_source.event_latency
              FROM logs.log_events AS default_DOT_event_source
              WHERE  default_DOT_event_source.event_latency > 1000000
              AND default_DOT_event_source.device_id = 'Android'
            """,
        ),
        (
            "default.municipality_dim",
            [],
            ["default.municipality.state_id = 'CA'"],
            """
            SELECT
              default_DOT_municipality.contact_name,
              default_DOT_municipality.contact_title,
              default_DOT_municipality.local_region,
              default_DOT_municipality.municipality_id,
              default_DOT_municipality_municipality_type.municipality_type_id,
              default_DOT_municipality_type.municipality_type_desc,
              default_DOT_municipality.state_id
            FROM
              roads.municipality AS default_DOT_municipality
              LEFT JOIN roads.municipality_municipality_type AS default_DOT_municipality_municipality_type ON default_DOT_municipality.municipality_id = default_DOT_municipality_municipality_type.municipality_id
              LEFT JOIN roads.municipality_type AS default_DOT_municipality_type ON default_DOT_municipality_municipality_type.municipality_type_id = default_DOT_municipality_type.municipality_type_desc
            WHERE
              default_DOT_municipality.state_id = 'CA'
            """,
        ),
        (
            "default.num_repair_orders",
            [],
            [],
            """
              SELECT  count(default_DOT_repair_orders.repair_order_id) default_DOT_num_repair_orders
              FROM roads.repair_orders AS default_DOT_repair_orders
            """,
        ),
        (
            "default.num_repair_orders",
            ["default.hard_hat.state"],
            ["default.repair_orders.dispatcher_id=1", "default.hard_hat.state='AZ'"],
            """
              SELECT  count(default_DOT_repair_orders.repair_order_id) default_DOT_num_repair_orders,
                      default_DOT_repair_orders.dispatcher_id,
                      default_DOT_hard_hat.state
              FROM roads.repair_orders AS default_DOT_repair_orders
              LEFT OUTER JOIN (SELECT  default_DOT_hard_hats.hard_hat_id,
                      default_DOT_hard_hats.state
              FROM roads.hard_hats AS default_DOT_hard_hats) AS default_DOT_hard_hat ON default_DOT_repair_orders.hard_hat_id = default_DOT_hard_hat.hard_hat_id
              WHERE  default_DOT_repair_orders.dispatcher_id = 1 AND default_DOT_hard_hat.state = 'AZ'
              GROUP BY  default_DOT_hard_hat.state
            """,
        ),
        (
            "default.num_repair_orders",
            [
                "default.hard_hat.city",
                "default.hard_hat.last_name",
                "default.dispatcher.company_name",
                "default.municipality_dim.local_region",
            ],
            [
                "default.repair_orders.dispatcher_id=1",
                "default.hard_hat.state != 'AZ'",
                "default.dispatcher.phone = '4082021022'",
                "default.repair_orders.order_date >= '2020-01-01'",
            ],
            """
            SELECT  default_DOT_hard_hat.city,
                    default_DOT_dispatcher.company_name,
                    count(default_DOT_repair_orders.repair_order_id) default_DOT_num_repair_orders,
                    default_DOT_repair_orders.dispatcher_id,
                    default_DOT_hard_hat.last_name,
                    default_DOT_municipality_dim.local_region,
                    default_DOT_repair_orders.order_date,
                    default_DOT_dispatcher.phone,
                    default_DOT_hard_hat.state
            FROM roads.repair_orders AS default_DOT_repair_orders LEFT OUTER JOIN (SELECT  default_DOT_dispatchers.company_name,
                    default_DOT_dispatchers.dispatcher_id,
                    default_DOT_dispatchers.phone
            FROM roads.dispatchers AS default_DOT_dispatchers) AS default_DOT_dispatcher ON default_DOT_repair_orders.dispatcher_id = default_DOT_dispatcher.dispatcher_id
            LEFT OUTER JOIN (SELECT  default_DOT_hard_hats.city,
                    default_DOT_hard_hats.hard_hat_id,
                    default_DOT_hard_hats.last_name,
                    default_DOT_hard_hats.state
            FROM roads.hard_hats AS default_DOT_hard_hats) AS default_DOT_hard_hat ON default_DOT_repair_orders.hard_hat_id = default_DOT_hard_hat.hard_hat_id
            LEFT OUTER JOIN (SELECT  default_DOT_municipality.local_region,
                    default_DOT_municipality.municipality_id
            FROM roads.municipality AS default_DOT_municipality LEFT  JOIN roads.municipality_municipality_type AS default_DOT_municipality_municipality_type ON default_DOT_municipality.municipality_id = default_DOT_municipality_municipality_type.municipality_id
            LEFT  JOIN roads.municipality_type AS default_DOT_municipality_type ON default_DOT_municipality_municipality_type.municipality_type_id = default_DOT_municipality_type.municipality_type_desc) AS default_DOT_municipality_dim ON default_DOT_repair_orders.municipality_id = default_DOT_municipality_dim.municipality_id
            WHERE  default_DOT_repair_orders.dispatcher_id = 1 AND default_DOT_hard_hat.state != 'AZ' AND default_DOT_dispatcher.phone = '4082021022' AND default_DOT_repair_orders.order_date >= '2020-01-01'
            GROUP BY  default_DOT_hard_hat.city, default_DOT_hard_hat.last_name, default_DOT_dispatcher.company_name, default_DOT_municipality_dim.local_region
            """,
        ),
        # metric with second-order dimension
        (
            "default.avg_repair_price",
            ["default.hard_hat.city"],
            [],
            """
              SELECT  avg(default_DOT_repair_order_details.price) AS default_DOT_avg_repair_price,
                      default_DOT_hard_hat.city
              FROM roads.repair_order_details AS default_DOT_repair_order_details LEFT OUTER JOIN (SELECT  default_DOT_repair_orders.dispatcher_id,
                      default_DOT_repair_orders.hard_hat_id,
                      default_DOT_repair_orders.municipality_id,
                      default_DOT_repair_orders.repair_order_id
              FROM roads.repair_orders AS default_DOT_repair_orders) AS default_DOT_repair_order ON default_DOT_repair_order_details.repair_order_id = default_DOT_repair_order.repair_order_id
              LEFT OUTER JOIN (SELECT  default_DOT_hard_hats.city,
                      default_DOT_hard_hats.hard_hat_id,
                      default_DOT_hard_hats.state
              FROM roads.hard_hats AS default_DOT_hard_hats) AS default_DOT_hard_hat ON default_DOT_repair_order.hard_hat_id = default_DOT_hard_hat.hard_hat_id
              GROUP BY  default_DOT_hard_hat.city
            """,
        ),
        # metric with multiple nth order dimensions that can share some of the joins
        (
            "default.avg_repair_price",
            ["default.hard_hat.city", "default.dispatcher.company_name"],
            [],
            """
              SELECT  avg(default_DOT_repair_order_details.price) AS default_DOT_avg_repair_price,
                      default_DOT_dispatcher.company_name,
                      default_DOT_hard_hat.city
              FROM roads.repair_order_details AS default_DOT_repair_order_details LEFT OUTER JOIN (SELECT  default_DOT_repair_orders.dispatcher_id,
                      default_DOT_repair_orders.hard_hat_id,
                      default_DOT_repair_orders.municipality_id,
                      default_DOT_repair_orders.repair_order_id
              FROM roads.repair_orders AS default_DOT_repair_orders) AS default_DOT_repair_order ON default_DOT_repair_order_details.repair_order_id = default_DOT_repair_order.repair_order_id
              LEFT OUTER JOIN (SELECT  default_DOT_dispatchers.company_name,
                      default_DOT_dispatchers.dispatcher_id
              FROM roads.dispatchers AS default_DOT_dispatchers) AS default_DOT_dispatcher ON default_DOT_repair_order.dispatcher_id = default_DOT_dispatcher.dispatcher_id
              LEFT OUTER JOIN (SELECT  default_DOT_hard_hats.city,
                      default_DOT_hard_hats.hard_hat_id,
                      default_DOT_hard_hats.state
              FROM roads.hard_hats AS default_DOT_hard_hats) AS default_DOT_hard_hat ON default_DOT_repair_order.hard_hat_id = default_DOT_hard_hat.hard_hat_id
              GROUP BY  default_DOT_hard_hat.city, default_DOT_dispatcher.company_name
            """,
        ),
        # dimension with aliased join key should just use the alias directly
        (
            "default.num_repair_orders",
            ["default.us_state.state_region_description"],
            [],
            """
              SELECT  default_DOT_us_state.state_region_description,
                      count(default_DOT_repair_orders.repair_order_id) default_DOT_num_repair_orders
              FROM roads.repair_orders AS default_DOT_repair_orders LEFT OUTER JOIN (SELECT  default_DOT_hard_hats.hard_hat_id,
                      default_DOT_hard_hats.state
              FROM roads.hard_hats AS default_DOT_hard_hats) AS default_DOT_hard_hat ON default_DOT_repair_orders.hard_hat_id = default_DOT_hard_hat.hard_hat_id
              LEFT OUTER JOIN (SELECT  default_DOT_us_states.state_id,
                      default_DOT_us_region.us_region_description AS state_region_description,
                      default_DOT_us_states.state_abbr AS state_short
              FROM roads.us_states AS default_DOT_us_states LEFT  JOIN roads.us_region AS default_DOT_us_region ON default_DOT_us_states.state_region = default_DOT_us_region.us_region_id) AS default_DOT_us_state ON default_DOT_hard_hat.state = default_DOT_us_state.state_short
              GROUP BY  default_DOT_us_state.state_region_description
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
    "node_name, dimensions, filters, orderby, sql",
    [
        # querying on source node with filter on joinable dimension
        (
            "foo.bar.repair_orders",
            [],
            ["foo.bar.hard_hat.state='CA'"],
            [],
            """
            SELECT  foo_DOT_bar_DOT_repair_orders.dispatched_date,
                    foo_DOT_bar_DOT_repair_orders.dispatcher_id,
                    foo_DOT_bar_DOT_hard_hat.state,
                    foo_DOT_bar_DOT_repair_orders.hard_hat_id,
                    foo_DOT_bar_DOT_repair_orders.municipality_id,
                    foo_DOT_bar_DOT_repair_orders.order_date,
                    foo_DOT_bar_DOT_repair_orders.repair_order_id,
                    foo_DOT_bar_DOT_repair_orders.required_date
            FROM roads.repair_orders AS foo_DOT_bar_DOT_repair_orders LEFT OUTER JOIN (SELECT  foo_DOT_bar_DOT_hard_hats.hard_hat_id,
                    foo_DOT_bar_DOT_hard_hats.state
            FROM roads.hard_hats AS foo_DOT_bar_DOT_hard_hats) AS foo_DOT_bar_DOT_hard_hat ON foo_DOT_bar_DOT_repair_orders.hard_hat_id = foo_DOT_bar_DOT_hard_hat.hard_hat_id
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
            SELECT  foo_DOT_bar_DOT_repair_orders.dispatcher_id,
                    count(foo_DOT_bar_DOT_repair_orders.repair_order_id) AS foo_DOT_bar_DOT_num_repair_orders,
                    foo_DOT_bar_DOT_hard_hat.state
            FROM roads.repair_orders AS foo_DOT_bar_DOT_repair_orders LEFT OUTER JOIN (SELECT  foo_DOT_bar_DOT_hard_hats.hard_hat_id,
                    foo_DOT_bar_DOT_hard_hats.state
            FROM roads.hard_hats AS foo_DOT_bar_DOT_hard_hats) AS foo_DOT_bar_DOT_hard_hat ON foo_DOT_bar_DOT_repair_orders.hard_hat_id = foo_DOT_bar_DOT_hard_hat.hard_hat_id
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
            SELECT
              foo_DOT_bar_DOT_hard_hat.city,
              foo_DOT_bar_DOT_dispatcher.company_name,
              foo_DOT_bar_DOT_repair_orders.dispatcher_id,
              count(
                foo_DOT_bar_DOT_repair_orders.repair_order_id
              ) AS foo_DOT_bar_DOT_num_repair_orders,
              foo_DOT_bar_DOT_hard_hat.last_name,
              foo_DOT_bar_DOT_municipality_dim.local_region,
              foo_DOT_bar_DOT_repair_orders.order_date,
              foo_DOT_bar_DOT_dispatcher.phone,
              foo_DOT_bar_DOT_hard_hat.state
            FROM
              roads.repair_orders AS foo_DOT_bar_DOT_repair_orders
              LEFT OUTER JOIN (
                SELECT
                  foo_DOT_bar_DOT_dispatchers.company_name,
                  foo_DOT_bar_DOT_dispatchers.dispatcher_id,
                  foo_DOT_bar_DOT_dispatchers.phone
                FROM
                  roads.dispatchers AS foo_DOT_bar_DOT_dispatchers
              ) AS foo_DOT_bar_DOT_dispatcher ON foo_DOT_bar_DOT_repair_orders.dispatcher_id = foo_DOT_bar_DOT_dispatcher.dispatcher_id
              LEFT OUTER JOIN (
                SELECT
                  foo_DOT_bar_DOT_hard_hats.city,
                  foo_DOT_bar_DOT_hard_hats.hard_hat_id,
                  foo_DOT_bar_DOT_hard_hats.last_name,
                  foo_DOT_bar_DOT_hard_hats.state
                FROM
                  roads.hard_hats AS foo_DOT_bar_DOT_hard_hats
              ) AS foo_DOT_bar_DOT_hard_hat ON foo_DOT_bar_DOT_repair_orders.hard_hat_id = foo_DOT_bar_DOT_hard_hat.hard_hat_id
              LEFT OUTER JOIN (
                SELECT
                  foo_DOT_bar_DOT_municipality.local_region,
                  foo_DOT_bar_DOT_municipality.municipality_id
                FROM
                  roads.municipality AS foo_DOT_bar_DOT_municipality
                  LEFT JOIN roads.municipality_municipality_type AS foo_DOT_bar_DOT_municipality_municipality_type ON foo_DOT_bar_DOT_municipality.municipality_id = foo_DOT_bar_DOT_municipality_municipality_type.municipality_id
                  LEFT JOIN roads.municipality_type AS foo_DOT_bar_DOT_municipality_type ON foo_DOT_bar_DOT_municipality_municipality_type.municipality_type_id = foo_DOT_bar_DOT_municipality_type.municipality_type_desc
              ) AS foo_DOT_bar_DOT_municipality_dim ON foo_DOT_bar_DOT_repair_orders.municipality_id = foo_DOT_bar_DOT_municipality_dim.municipality_id
            WHERE
              foo_DOT_bar_DOT_repair_orders.dispatcher_id = 1
              AND foo_DOT_bar_DOT_hard_hat.state != 'AZ'
              AND foo_DOT_bar_DOT_dispatcher.phone = '4082021022'
              AND foo_DOT_bar_DOT_repair_orders.order_date >= '2020-01-01'
            GROUP BY
              foo_DOT_bar_DOT_hard_hat.city,
              foo_DOT_bar_DOT_hard_hat.last_name,
              foo_DOT_bar_DOT_dispatcher.company_name,
              foo_DOT_bar_DOT_municipality_dim.local_region
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
def test_sql_with_filters_on_namespaced_nodes(  # pylint: disable=R0913
    node_name,
    dimensions,
    filters,
    orderby,
    sql,
    client_with_examples: TestClient,
):
    """
    Test ``GET /sql/{node_name}/`` with various filters and dimensions using a
    version of the DJ roads database with namespaces.
    """
    response = client_with_examples.get(
        f"/sql/{node_name}/",
        params={"dimensions": dimensions, "filters": filters, "orderby": orderby},
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
            "limit": 5,
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

    LIMIT 5
    """
    query = response.json()["sql"]
    compare_query_strings(query, expected)


def test_get_sql_for_metrics_failures(client_with_examples: TestClient):
    """
    Test failure modes when getting sql for multiple metrics.
    """
    # Getting sql for no metrics fails appropriately
    response = client_with_examples.get(
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

    # Getting sql with no dimensions fails appropriately
    response = client_with_examples.get(
        "/sql/",
        params={
            "metrics": ["default.number_of_account_types"],
            "dimensions": [],
            "filters": [],
        },
    )
    assert response.status_code == 422
    data = response.json()
    assert data == {
        "message": "At least one dimension is required",
        "errors": [],
        "warnings": [],
    }


def test_get_sql_for_metrics(client_with_examples: TestClient):
    """
    Test getting sql for multiple metrics.
    """
    response = client_with_examples.get(
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
            "orderby": ["default.hard_hat.country", "default.dispatcher.phone"],
            "limit": 100,
        },
    )
    data = response.json()
    expected_sql = """
                  SELECT
                    default_DOT_hard_hat.city,
                    default_DOT_dispatcher.company_name,
                    default_DOT_hard_hat.country,
                    count(
                      default_DOT_repair_orders.repair_order_id
                    ) default_DOT_num_repair_orders,
                    default_DOT_municipality_dim.local_region,
                    default_DOT_dispatcher.phone,
                    default_DOT_hard_hat.postal_code,
                    default_DOT_hard_hat.state,
                    CAST(
                      sum(
                        if(
                          default_DOT_repair_order_details.discount > 0.0,
                          1, 0
                        )
                      ) AS DOUBLE
                    ) / count(*) AS default_DOT_discounted_orders_rate
                  FROM
                    roads.repair_orders AS default_DOT_repair_orders
                    LEFT OUTER JOIN (
                      SELECT
                        default_DOT_dispatchers.company_name,
                        default_DOT_dispatchers.dispatcher_id,
                        default_DOT_dispatchers.phone
                      FROM
                        roads.dispatchers AS default_DOT_dispatchers
                    ) AS default_DOT_dispatcher ON default_DOT_repair_orders.dispatcher_id = default_DOT_dispatcher.dispatcher_id
                    LEFT OUTER JOIN (
                      SELECT
                        default_DOT_hard_hats.city,
                        default_DOT_hard_hats.country,
                        default_DOT_hard_hats.hard_hat_id,
                        default_DOT_hard_hats.postal_code,
                        default_DOT_hard_hats.state
                      FROM
                        roads.hard_hats AS default_DOT_hard_hats
                    ) AS default_DOT_hard_hat ON default_DOT_repair_orders.hard_hat_id = default_DOT_hard_hat.hard_hat_id
                    LEFT OUTER JOIN (
                      SELECT
                        default_DOT_municipality.local_region,
                        default_DOT_municipality.municipality_id
                      FROM
                        roads.municipality AS default_DOT_municipality
                        LEFT JOIN roads.municipality_municipality_type AS default_DOT_municipality_municipality_type ON default_DOT_municipality.municipality_id = default_DOT_municipality_municipality_type.municipality_id
                        LEFT JOIN roads.municipality_type AS default_DOT_municipality_type ON default_DOT_municipality_municipality_type.municipality_type_id = default_DOT_municipality_type.municipality_type_desc
                    ) AS default_DOT_municipality_dim ON default_DOT_repair_orders.municipality_id = default_DOT_municipality_dim.municipality_id
                  GROUP BY
                    default_DOT_hard_hat.country,
                    default_DOT_hard_hat.postal_code,
                    default_DOT_hard_hat.city,
                    default_DOT_hard_hat.state,
                    default_DOT_dispatcher.company_name,
                    default_DOT_municipality_dim.local_region
                  ORDER BY
                    default_DOT_hard_hat.country,
                    default_DOT_dispatcher.phone
                  LIMIT
                    100

    """
    assert compare_query_strings(data["sql"], expected_sql)
    assert data["columns"] == [
        {"name": "city", "type": "string"},
        {"name": "company_name", "type": "string"},
        {"name": "country", "type": "string"},
        {"name": "default_DOT_num_repair_orders", "type": "bigint"},
        {"name": "local_region", "type": "string"},
        {"name": "phone", "type": "string"},
        {"name": "postal_code", "type": "string"},
        {"name": "state", "type": "string"},
        {"name": "default_DOT_discounted_orders_rate", "type": "double"},
    ]
