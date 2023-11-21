"""
Integration tests to be run against the latest full demo datajunction environment
"""
# pylint: disable=too-many-lines,line-too-long,protected-access
import namesgenerator
import pandas
import pytest

from datajunction import DJBuilder
from datajunction.exceptions import DJClientException
from datajunction.models import AvailabilityState, ColumnAttribute, NodeMode, NodeStatus


@pytest.mark.skipif("not config.getoption('integration')")
def test_integration():  # pylint: disable=too-many-statements,too-many-locals,line-too-long
    """
    Integration test
    """
    dj = DJBuilder()  # pylint: disable=invalid-name,line-too-long

    # Create a namespace
    namespace = f"integration.python.{namesgenerator.get_random_name()}"
    dj.create_namespace(namespace)

    # List namespaces
    matching_namespace = None
    for existing_namespace in dj.list_namespaces():
        if existing_namespace == namespace:
            matching_namespace = existing_namespace
    assert matching_namespace

    # Create a sources
    dj.create_source(
        name=f"{namespace}.repair_orders",
        description="Repair orders",
        catalog="warehouse",
        schema="roads",
        table="repair_orders",
        columns=[
            {"name": "repair_order_id", "type": "int"},
            {"name": "municipality_id", "type": "string"},
            {"name": "hard_hat_id", "type": "int"},
            {"name": "order_date", "type": "timestamp"},
            {"name": "required_date", "type": "timestamp"},
            {"name": "dispatched_date", "type": "timestamp"},
            {"name": "dispatcher_id", "type": "int"},
        ],
    )

    repair_order_details = dj.create_source(
        name=f"{namespace}.repair_order_details",
        description="Details on repair orders",
        display_name="Default: Repair Order Details",
        schema="roads",
        catalog="warehouse",
        table="repair_order_details",
        columns=[
            {"name": "repair_order_id", "type": "int"},
            {"name": "repair_type_id", "type": "int"},
            {"name": "price", "type": "float"},
            {"name": "quantity", "type": "int"},
            {"name": "discount", "type": "float"},
        ],
    )

    # Create dimension
    dj.create_dimension(
        description="Repair order dimension",
        display_name="Default: Repair Order",
        name=f"{namespace}.repair_order",
        primary_key=["repair_order_id"],
        query=f"""
                SELECT
                repair_order_id,
                municipality_id,
                hard_hat_id,
                dispatcher_id
                FROM {namespace}.repair_orders
            """,
    )
    repair_order_details.link_dimension(
        column="repair_order_id",
        dimension=f"{namespace}.repair_order",
        dimension_column="repair_order_id",
    )

    # Get source and link with dimension
    repair_orders = dj.source(f"{namespace}.repair_orders")
    repair_orders.link_dimension(
        column="repair_order_id",
        dimension=f"{namespace}.repair_order",
        dimension_column="repair_order_id",
    )

    # Create a transform
    dj.create_transform(
        name=f"{namespace}.repair_orders_w_dispatchers",
        description="Repair orders that have a dispatcher",
        query=f"""
            SELECT
            repair_order_id,
            municipality_id,
            hard_hat_id,
            dispatcher_id
            FROM {namespace}.repair_orders
            WHERE dispatcher_id IS NOT NULL
        """,
    )

    # Get transform
    dj.transform(f"{namespace}.repair_orders_w_dispatchers")

    # Create a source and dimension nodes
    dj.create_source(
        name=f"{namespace}.dispatchers",
        description="Different third party dispatcher companies that coordinate repairs",
        catalog="warehouse",
        schema="roads",
        table="dispatchers",
        columns=[
            {"name": "dispatcher_id", "type": "int"},
            {"name": "company_name", "type": "string"},
            {"name": "phone", "type": "string"},
        ],
    )
    dj.create_dimension(
        name=f"{namespace}.all_dispatchers",
        description="All dispatchers",
        primary_key=["dispatcher_id"],
        query=f"""
            SELECT
            dispatcher_id,
            company_name,
            phone
            FROM {namespace}.dispatchers
        """,
    )

    # Get dimension
    dj.dimension(f"{namespace}.all_dispatchers")

    # Create metrics
    dj.create_metric(
        name=f"{namespace}.num_repair_orders",
        description="Number of repair orders",
        query=f"SELECT COUNT(repair_order_id) FROM {namespace}.repair_orders",
    )
    dj.create_metric(
        name=f"{namespace}.avg_repair_price",
        description="Average repair price",
        query=f"SELECT AVG(price) FROM {namespace}.repair_order_details",
    )
    dj.create_metric(
        name=f"{namespace}.total_repair_cost",
        description="Total repair price",
        query=f"SELECT SUM(price) FROM {namespace}.repair_order_details",
    )

    # List metrics
    assert len(dj.list_metrics(namespace=namespace)) == 3

    # Create a dimension link
    source = dj.source(f"{namespace}.repair_orders")
    source.link_dimension(
        column="dispatcher_id",
        dimension=f"{namespace}.all_dispatchers",
        dimension_column="dispatcher_id",
    )
    # purposfull error, take it back
    source.unlink_dimension(
        column="dispatcher_id",
        dimension=f"{namespace}.all_dispatchers",
        dimension_column="dispatcher_id",
    )

    dimension = dj.dimension(f"{namespace}.repair_order")
    dimension.link_dimension(
        column="dispatcher_id",
        dimension=f"{namespace}.all_dispatchers",
        dimension_column="dispatcher_id",
    )

    # List dimensions for a metric
    assert len(dj.metric(f"{namespace}.num_repair_orders").dimensions()) > 0
    assert len(dj.metric(f"{namespace}.avg_repair_price").dimensions()) > 0
    assert len(dj.metric(f"{namespace}.total_repair_cost").dimensions()) > 0

    # List common dimensions for multiple metrics
    # names only
    common_dimensions = dj.common_dimensions(
        metrics=[
            f"{namespace}.num_repair_orders",
            f"{namespace}.avg_repair_price",
            f"{namespace}.total_repair_cost",
        ],
        name_only=True,
    )
    assert common_dimensions == [
        f"{namespace}.all_dispatchers.company_name",
        f"{namespace}.all_dispatchers.dispatcher_id",
        f"{namespace}.all_dispatchers.phone",
        f"{namespace}.repair_orders.repair_order_id",
    ]
    # with details
    common_dimensions = dj.common_dimensions(
        metrics=[
            f"{namespace}.num_repair_orders",
            f"{namespace}.avg_repair_price",
            f"{namespace}.total_repair_cost",
        ],
    )
    assert common_dimensions == [
        {
            "name": f"{namespace}.all_dispatchers.company_name",
            "type": "string",
            "path": [
                f"{namespace}.repair_orders.repair_order_id",
                f"{namespace}.repair_order.dispatcher_id",
            ],
        },
        {
            "name": f"{namespace}.all_dispatchers.dispatcher_id",
            "type": "int",
            "path": [
                f"{namespace}.repair_orders.repair_order_id",
                f"{namespace}.repair_order.dispatcher_id",
            ],
        },
        {
            "name": f"{namespace}.all_dispatchers.phone",
            "type": "string",
            "path": [
                f"{namespace}.repair_orders.repair_order_id",
                f"{namespace}.repair_order.dispatcher_id",
            ],
        },
        {
            "name": f"{namespace}.repair_orders.repair_order_id",
            "type": "int",
            "path": [],
        },
    ]

    # List common metrics for multiple dimensions
    # names only
    common_metrics = dj.common_metrics(
        dimensions=[f"{namespace}.all_dispatchers", f"{namespace}.repair_order"],
        name_only=True,
    )
    assert set(common_metrics) == set(
        [
            f"{namespace}.num_repair_orders",
            f"{namespace}.avg_repair_price",
            f"{namespace}.total_repair_cost",
        ],
    )
    # with details
    common_metrics = dj.common_metrics(
        dimensions=[f"{namespace}.all_dispatchers", f"{namespace}.repair_order"],
    )
    capitalized_namespace = namespace.replace("_", " ").replace(".", ": ").title()
    dotted_namespace = namespace.replace(".", "_DOT_")
    expected = [
        {
            "name": f"{namespace}.avg_repair_price",
            "display_name": f"{capitalized_namespace}: Avg Repair Price",
            "description": "Average repair price",
            "query": f"SELECT  AVG(price) {dotted_namespace}_DOT_avg_repair_price \n FROM {namespace}.repair_order_details\n\n",
        },
        {
            "name": f"{namespace}.num_repair_orders",
            "display_name": f"{capitalized_namespace}: Num Repair Orders",
            "description": "Number of repair orders",
            "query": f"SELECT  COUNT(repair_order_id) {dotted_namespace}_DOT_num_repair_orders \n FROM {namespace}.repair_orders\n\n",
        },
        {
            "name": f"{namespace}.total_repair_cost",
            "display_name": f"{capitalized_namespace}: Total Repair Cost",
            "description": "Total repair price",
            "query": f"SELECT  SUM(price) {dotted_namespace}_DOT_total_repair_cost \n FROM {namespace}.repair_order_details\n\n",
        },
    ]
    common_metrics.sort(key=lambda x: x["name"])
    assert common_metrics == expected

    # Get SQL for a set of metrics and dimensions
    query = dj.sql(
        metrics=[
            f"{namespace}.num_repair_orders",
            f"{namespace}.avg_repair_price",
            f"{namespace}.total_repair_cost",
        ],
        dimensions=[
            f"{namespace}.all_dispatchers.dispatcher_id",
            f"{namespace}.all_dispatchers.company_name",
            f"{namespace}.all_dispatchers.phone",
        ],
        filters=[],
    )
    expected_query = f"""
    WITH
    m0_{dotted_namespace}_DOT_num_repair_orders AS (SELECT  {dotted_namespace}_DOT_all_dispatchers.company_name,
        {dotted_namespace}_DOT_all_dispatchers.dispatcher_id,
        {dotted_namespace}_DOT_all_dispatchers.phone,
        COUNT({dotted_namespace}_DOT_repair_orders.repair_order_id) {dotted_namespace}_DOT_num_repair_orders
    FROM roads.repair_orders AS {dotted_namespace}_DOT_repair_orders LEFT OUTER JOIN (SELECT  {dotted_namespace}_DOT_repair_orders.dispatcher_id,
        {dotted_namespace}_DOT_repair_orders.repair_order_id
    FROM roads.repair_orders AS {dotted_namespace}_DOT_repair_orders)
    AS {dotted_namespace}_DOT_repair_order ON {dotted_namespace}_DOT_repair_orders.repair_order_id = {dotted_namespace}_DOT_repair_order.repair_order_id
    LEFT OUTER JOIN (SELECT  {dotted_namespace}_DOT_dispatchers.company_name,
        {dotted_namespace}_DOT_dispatchers.dispatcher_id,
        {dotted_namespace}_DOT_dispatchers.phone
    FROM roads.dispatchers AS {dotted_namespace}_DOT_dispatchers)
    AS {dotted_namespace}_DOT_all_dispatchers ON {dotted_namespace}_DOT_repair_order.dispatcher_id = {dotted_namespace}_DOT_all_dispatchers.dispatcher_id
    GROUP BY  {dotted_namespace}_DOT_all_dispatchers.dispatcher_id, {dotted_namespace}_DOT_all_dispatchers.company_name, {dotted_namespace}_DOT_all_dispatchers.phone
    ),
    m1_{dotted_namespace}_DOT_avg_repair_price AS (SELECT  {dotted_namespace}_DOT_all_dispatchers.company_name,
        {dotted_namespace}_DOT_all_dispatchers.dispatcher_id,
        {dotted_namespace}_DOT_all_dispatchers.phone,
        AVG({dotted_namespace}_DOT_repair_order_details.price) {dotted_namespace}_DOT_avg_repair_price
    FROM roads.repair_order_details AS {dotted_namespace}_DOT_repair_order_details LEFT OUTER JOIN (SELECT  {dotted_namespace}_DOT_repair_orders.dispatcher_id,
        {dotted_namespace}_DOT_repair_orders.repair_order_id
    FROM roads.repair_orders AS {dotted_namespace}_DOT_repair_orders)
    AS {dotted_namespace}_DOT_repair_order ON {dotted_namespace}_DOT_repair_order_details.repair_order_id = {dotted_namespace}_DOT_repair_order.repair_order_id
    LEFT OUTER JOIN (SELECT  {dotted_namespace}_DOT_dispatchers.company_name,
        {dotted_namespace}_DOT_dispatchers.dispatcher_id,
        {dotted_namespace}_DOT_dispatchers.phone
    FROM roads.dispatchers AS {dotted_namespace}_DOT_dispatchers)
    AS {dotted_namespace}_DOT_all_dispatchers ON {dotted_namespace}_DOT_repair_order.dispatcher_id = {dotted_namespace}_DOT_all_dispatchers.dispatcher_id
    GROUP BY  {dotted_namespace}_DOT_all_dispatchers.dispatcher_id, {dotted_namespace}_DOT_all_dispatchers.company_name, {dotted_namespace}_DOT_all_dispatchers.phone
    ),
    m2_{dotted_namespace}_DOT_total_repair_cost AS (SELECT  {dotted_namespace}_DOT_all_dispatchers.company_name,
        {dotted_namespace}_DOT_all_dispatchers.dispatcher_id,
        {dotted_namespace}_DOT_all_dispatchers.phone,
        SUM({dotted_namespace}_DOT_repair_order_details.price) {dotted_namespace}_DOT_total_repair_cost
    FROM roads.repair_order_details AS {dotted_namespace}_DOT_repair_order_details LEFT OUTER JOIN (SELECT  {dotted_namespace}_DOT_repair_orders.dispatcher_id,
        {dotted_namespace}_DOT_repair_orders.repair_order_id
    FROM roads.repair_orders AS {dotted_namespace}_DOT_repair_orders)
    AS {dotted_namespace}_DOT_repair_order ON {dotted_namespace}_DOT_repair_order_details.repair_order_id = {dotted_namespace}_DOT_repair_order.repair_order_id
    LEFT OUTER JOIN (SELECT  {dotted_namespace}_DOT_dispatchers.company_name,
        {dotted_namespace}_DOT_dispatchers.dispatcher_id,
        {dotted_namespace}_DOT_dispatchers.phone
    FROM roads.dispatchers AS {dotted_namespace}_DOT_dispatchers)
    AS {dotted_namespace}_DOT_all_dispatchers ON {dotted_namespace}_DOT_repair_order.dispatcher_id = {dotted_namespace}_DOT_all_dispatchers.dispatcher_id
    GROUP BY  {dotted_namespace}_DOT_all_dispatchers.dispatcher_id, {dotted_namespace}_DOT_all_dispatchers.company_name, {dotted_namespace}_DOT_all_dispatchers.phone
    )

    SELECT  m0_{dotted_namespace}_DOT_num_repair_orders.{dotted_namespace}_DOT_num_repair_orders,
        m1_{dotted_namespace}_DOT_avg_repair_price.{dotted_namespace}_DOT_avg_repair_price,
        m2_{dotted_namespace}_DOT_total_repair_cost.{dotted_namespace}_DOT_total_repair_cost,
        COALESCE(m0_{dotted_namespace}_DOT_num_repair_orders.company_name, m1_{dotted_namespace}_DOT_avg_repair_price.company_name, m2_{dotted_namespace}_DOT_total_repair_cost.company_name) company_name,
        COALESCE(m0_{dotted_namespace}_DOT_num_repair_orders.dispatcher_id, m1_{dotted_namespace}_DOT_avg_repair_price.dispatcher_id, m2_{dotted_namespace}_DOT_total_repair_cost.dispatcher_id) dispatcher_id,
        COALESCE(m0_{dotted_namespace}_DOT_num_repair_orders.phone, m1_{dotted_namespace}_DOT_avg_repair_price.phone, m2_{dotted_namespace}_DOT_total_repair_cost.phone) phone
    FROM m0_{dotted_namespace}_DOT_num_repair_orders FULL OUTER JOIN m1_{dotted_namespace}_DOT_avg_repair_price ON m0_{dotted_namespace}_DOT_num_repair_orders.company_name = m1_{dotted_namespace}_DOT_avg_repair_price.company_name AND m0_{dotted_namespace}_DOT_num_repair_orders.dispatcher_id = m1_{dotted_namespace}_DOT_avg_repair_price.dispatcher_id AND m0_{dotted_namespace}_DOT_num_repair_orders.phone = m1_{dotted_namespace}_DOT_avg_repair_price.phone
    FULL OUTER JOIN m2_{dotted_namespace}_DOT_total_repair_cost ON m0_{dotted_namespace}_DOT_num_repair_orders.company_name = m2_{dotted_namespace}_DOT_total_repair_cost.company_name AND m0_{dotted_namespace}_DOT_num_repair_orders.dispatcher_id = m2_{dotted_namespace}_DOT_total_repair_cost.dispatcher_id AND m0_{dotted_namespace}_DOT_num_repair_orders.phone = m2_{dotted_namespace}_DOT_total_repair_cost.phone
    """

    assert "".join(query.split()) == "".join(expected_query.split())

    # Get data for a set of metrics and dimensions
    result = dj.data(
        metrics=[
            f"{namespace}.num_repair_orders",
            f"{namespace}.avg_repair_price",
            f"{namespace}.total_repair_cost",
        ],
        dimensions=[
            f"{namespace}.all_dispatchers.dispatcher_id",
            f"{namespace}.all_dispatchers.company_name",
            f"{namespace}.all_dispatchers.phone",
        ],
        async_=False,
    )
    expected_df = pandas.DataFrame.from_dict(
        {
            f"{dotted_namespace}_DOT_num_repair_orders": {
                0: 9,
                1: 8,
                2: 8,
            },
            f"{dotted_namespace}_DOT_avg_repair_price": {
                0: 51913.888889,
                1: 62205.875000,
                2: 68914.750000,
            },
            f"{dotted_namespace}_DOT_total_repair_cost": {
                0: 467225.0,
                1: 497647.0,
                2: 551318.0,
            },
            "company_name": {
                0: "Federal Roads Group",
                1: "Pothole Pete",
                2: "Asphalts R Us",
            },
            "dispatcher_id": {
                0: 3,
                1: 1,
                2: 2,
            },
            "phone": {
                0: "(333) 333-3333",
                1: "(111) 111-1111",
                2: "(222) 222-2222",
            },
        },
    )
    pandas.testing.assert_frame_equal(result, expected_df)

    # Create a transform 2 downstream from a transform 1
    dj.create_transform(
        name=f"{namespace}.repair_orders_w_hard_hats",
        description="Repair orders that have a hard hat assigned",
        query=f"""
            SELECT
            repair_order_id,
            municipality_id,
            hard_hat_id,
            dispatcher_id
            FROM {namespace}.repair_orders_w_dispatchers
            WHERE hard_hat_id IS NOT NULL
        """,
    )

    # Get transform 2 that's downstream from transform 1 and make sure it's valid
    transform_2 = dj.transform(f"{namespace}.repair_orders_w_hard_hats")
    assert transform_2.status == NodeStatus.VALID

    # Add an availability state to the transform
    response = transform_2.add_availability(
        AvailabilityState(
            catalog="default",
            schema_="materialized",
            table="contractor",
            valid_through_ts=1688660209,
        ),
    )
    assert response == {"message": "Availability state successfully posted"}

    # Add an availability state to the transform
    response = transform_2.set_column_attributes(
        "hard_hat_id",
        [
            ColumnAttribute(
                name="dimension",
            ),
        ],
    )
    assert response == [
        {
            "attributes": [
                {"attribute_type": {"name": "dimension", "namespace": "system"}},
            ],
            "dimension": None,
            "name": "hard_hat_id",
            "type": "int",
        },
    ]

    # Create a draft transform 4 that's downstream from a not yet created transform 3
    dj.create_transform(
        name=f"{namespace}.repair_orders_w_repair_order_id",
        description="Repair orders without a null ID",
        query=f"""
            SELECT
            repair_order_id,
            municipality_id,
            hard_hat_id,
            dispatcher_id
            FROM {namespace}.repair_orders_w_municipalities
            WHERE repair_order_id IS NOT NULL
        """,
        mode=NodeMode.DRAFT,
    )
    transform_4 = dj.transform(f"{namespace}.repair_orders_w_repair_order_id")
    transform_4.refresh()
    assert transform_4.mode == NodeMode.DRAFT
    assert transform_4.status == NodeStatus.INVALID
    # Check that transform 4 is invalid because transform 3 does not exist
    with pytest.raises(DJClientException):
        transform_4.validate()

    # Create a draft transform 3 that's downstream from transform 2
    dj.create_transform(
        name=f"{namespace}.repair_orders_w_municipalities",
        description="Repair orders that have a municipality listed",
        query=f"""
            SELECT
            repair_order_id,
            municipality_id,
            hard_hat_id,
            dispatcher_id
            FROM {namespace}.repair_orders_w_hard_hats
            WHERE municipality_id IS NOT NULL
        """,
        mode=NodeMode.DRAFT,
    )
    transform_3 = dj.transform(f"{namespace}.repair_orders_w_municipalities")
    # Check that transform 3 is valid
    assert transform_3.validate() == NodeStatus.VALID

    # Check that transform 4 is now valid after transform 3 was created
    transform_4.refresh()
    assert transform_4.validate() == NodeStatus.VALID

    # Check that publishing transform 3 works
    transform_3.publish()

    # Delete the test namespace
    dj.delete_namespace(namespace=namespace, cascade=True)
