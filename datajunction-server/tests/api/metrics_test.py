# pylint: disable=too-many-lines
"""
Tests for the metrics API.
"""
import pytest
from httpx import AsyncClient
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.attributetype import AttributeType, ColumnAttribute
from datajunction_server.database.column import Column
from datajunction_server.database.database import Database
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.user import User
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.parsing.types import FloatType, IntegerType, StringType

expected_dimensions = [
    {
        "filter_only": False,
        "name": "default.dispatcher.company_name",
        "node_name": "default.dispatcher",
        "node_display_name": "Default: Dispatcher",
        "is_primary_key": False,
        "type": "string",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.dispatcher.dispatcher_id",
        "node_name": "default.dispatcher",
        "node_display_name": "Default: Dispatcher",
        "is_primary_key": True,
        "type": "int",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.dispatcher.phone",
        "node_name": "default.dispatcher",
        "node_display_name": "Default: Dispatcher",
        "is_primary_key": False,
        "type": "string",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.hard_hat.address",
        "node_name": "default.hard_hat",
        "node_display_name": "Default: Hard Hat",
        "is_primary_key": False,
        "type": "string",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.hard_hat.birth_date",
        "node_name": "default.hard_hat",
        "node_display_name": "Default: Hard Hat",
        "is_primary_key": False,
        "type": "timestamp",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.hard_hat.city",
        "node_name": "default.hard_hat",
        "node_display_name": "Default: Hard Hat",
        "is_primary_key": False,
        "type": "string",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.hard_hat.contractor_id",
        "node_name": "default.hard_hat",
        "node_display_name": "Default: Hard Hat",
        "is_primary_key": False,
        "type": "int",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.hard_hat.country",
        "node_name": "default.hard_hat",
        "node_display_name": "Default: Hard Hat",
        "is_primary_key": False,
        "type": "string",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.hard_hat.first_name",
        "node_name": "default.hard_hat",
        "node_display_name": "Default: Hard Hat",
        "is_primary_key": False,
        "type": "string",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.hard_hat.hard_hat_id",
        "node_name": "default.hard_hat",
        "node_display_name": "Default: Hard Hat",
        "is_primary_key": True,
        "type": "int",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.hard_hat.hire_date",
        "node_name": "default.hard_hat",
        "node_display_name": "Default: Hard Hat",
        "is_primary_key": False,
        "type": "timestamp",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.hard_hat.last_name",
        "node_name": "default.hard_hat",
        "node_display_name": "Default: Hard Hat",
        "is_primary_key": False,
        "type": "string",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.hard_hat.manager",
        "node_name": "default.hard_hat",
        "node_display_name": "Default: Hard Hat",
        "is_primary_key": False,
        "type": "int",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.hard_hat.postal_code",
        "node_name": "default.hard_hat",
        "node_display_name": "Default: Hard Hat",
        "is_primary_key": False,
        "type": "string",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.hard_hat.state",
        "node_name": "default.hard_hat",
        "node_display_name": "Default: Hard Hat",
        "is_primary_key": False,
        "type": "string",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.hard_hat.title",
        "node_name": "default.hard_hat",
        "node_display_name": "Default: Hard Hat",
        "is_primary_key": False,
        "type": "string",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.hard_hat_to_delete.address",
        "node_name": "default.hard_hat_to_delete",
        "node_display_name": "Default: Hard Hat To Delete",
        "is_primary_key": False,
        "type": "string",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.hard_hat_to_delete.birth_date",
        "node_name": "default.hard_hat_to_delete",
        "node_display_name": "Default: Hard Hat To Delete",
        "is_primary_key": False,
        "type": "timestamp",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.hard_hat_to_delete.city",
        "node_name": "default.hard_hat_to_delete",
        "node_display_name": "Default: Hard Hat To Delete",
        "is_primary_key": False,
        "type": "string",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.hard_hat_to_delete.contractor_id",
        "node_name": "default.hard_hat_to_delete",
        "node_display_name": "Default: Hard Hat To Delete",
        "is_primary_key": False,
        "type": "int",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.hard_hat_to_delete.country",
        "node_name": "default.hard_hat_to_delete",
        "node_display_name": "Default: Hard Hat To Delete",
        "is_primary_key": False,
        "type": "string",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.hard_hat_to_delete.first_name",
        "node_name": "default.hard_hat_to_delete",
        "node_display_name": "Default: Hard Hat To Delete",
        "is_primary_key": False,
        "type": "string",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.hard_hat_to_delete.hard_hat_id",
        "node_name": "default.hard_hat_to_delete",
        "node_display_name": "Default: Hard Hat To Delete",
        "is_primary_key": True,
        "type": "int",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.hard_hat_to_delete.hire_date",
        "node_name": "default.hard_hat_to_delete",
        "node_display_name": "Default: Hard Hat To Delete",
        "is_primary_key": False,
        "type": "timestamp",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.hard_hat_to_delete.last_name",
        "node_name": "default.hard_hat_to_delete",
        "node_display_name": "Default: Hard Hat To Delete",
        "is_primary_key": False,
        "type": "string",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.hard_hat_to_delete.manager",
        "node_name": "default.hard_hat_to_delete",
        "node_display_name": "Default: Hard Hat To Delete",
        "is_primary_key": False,
        "type": "int",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.hard_hat_to_delete.postal_code",
        "node_name": "default.hard_hat_to_delete",
        "node_display_name": "Default: Hard Hat To Delete",
        "is_primary_key": False,
        "type": "string",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.hard_hat_to_delete.state",
        "node_name": "default.hard_hat_to_delete",
        "node_display_name": "Default: Hard Hat To Delete",
        "is_primary_key": False,
        "type": "string",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.hard_hat_to_delete.title",
        "node_name": "default.hard_hat_to_delete",
        "node_display_name": "Default: Hard Hat To Delete",
        "is_primary_key": False,
        "type": "string",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.municipality_dim.contact_name",
        "node_name": "default.municipality_dim",
        "node_display_name": "Default: Municipality Dim",
        "is_primary_key": False,
        "type": "string",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.municipality_dim.contact_title",
        "node_name": "default.municipality_dim",
        "node_display_name": "Default: Municipality Dim",
        "is_primary_key": False,
        "type": "string",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.municipality_dim.local_region",
        "node_name": "default.municipality_dim",
        "node_display_name": "Default: Municipality Dim",
        "is_primary_key": False,
        "type": "string",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.municipality_dim.municipality_id",
        "node_name": "default.municipality_dim",
        "node_display_name": "Default: Municipality Dim",
        "is_primary_key": True,
        "type": "string",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.municipality_dim.municipality_type_desc",
        "node_name": "default.municipality_dim",
        "node_display_name": "Default: Municipality Dim",
        "is_primary_key": False,
        "type": "string",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.municipality_dim.municipality_type_id",
        "node_name": "default.municipality_dim",
        "node_display_name": "Default: Municipality Dim",
        "is_primary_key": False,
        "type": "string",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.municipality_dim.state_id",
        "node_name": "default.municipality_dim",
        "node_display_name": "Default: Municipality Dim",
        "is_primary_key": False,
        "type": "int",
        "path": ["default.repair_orders_fact"],
    },
    {
        "filter_only": False,
        "name": "default.us_state.state_id",
        "node_name": "default.us_state",
        "node_display_name": "Default: Us State",
        "is_primary_key": False,
        "type": "int",
        "path": ["default.repair_orders_fact", "default.hard_hat"],
    },
    {
        "filter_only": False,
        "name": "default.us_state.state_name",
        "node_name": "default.us_state",
        "node_display_name": "Default: Us State",
        "is_primary_key": False,
        "type": "string",
        "path": ["default.repair_orders_fact", "default.hard_hat"],
    },
    {
        "filter_only": False,
        "name": "default.us_state.state_region",
        "node_name": "default.us_state",
        "node_display_name": "Default: Us State",
        "is_primary_key": False,
        "type": "int",
        "path": ["default.repair_orders_fact", "default.hard_hat"],
    },
    {
        "filter_only": False,
        "name": "default.us_state.state_short",
        "node_name": "default.us_state",
        "node_display_name": "Default: Us State",
        "is_primary_key": True,
        "type": "string",
        "path": ["default.repair_orders_fact", "default.hard_hat"],
    },
]


@pytest.mark.asyncio
async def test_read_metrics(module__client_with_roads: AsyncClient) -> None:
    """
    Test ``GET /metrics/``.
    """
    response = await module__client_with_roads.get("/metrics/")
    data = response.json()

    assert response.status_code == 200
    assert len(data) > 5

    response = await module__client_with_roads.get("/metrics/default.num_repair_orders")
    data = response.json()
    assert data["metric_metadata"] == {
        "direction": "higher_is_better",
        "unit": {
            "abbreviation": None,
            "category": None,
            "description": None,
            "label": "Dollar",
            "name": "DOLLAR",
        },
    }
    assert data["upstream_node"] == "default.repair_orders_fact"
    assert data["expression"] == "count(repair_order_id)"

    response = await module__client_with_roads.get(
        "/metrics/default.discounted_orders_rate",
    )
    data = response.json()
    assert data["incompatible_druid_functions"] == ["IF"]


@pytest.mark.asyncio
async def test_read_metric(
    module__session: AsyncSession,
    module__client: AsyncClient,
    module__current_user: User,
) -> None:
    """
    Test ``GET /metric/{node_id}/``.
    """
    await module__client.get("/attributes/")
    dimension_attribute = (
        await module__session.execute(
            select(AttributeType).where(AttributeType.name == "dimension"),
        )
    ).scalar_one()
    parent_rev = NodeRevision(
        name="parent",
        type=NodeType.SOURCE,
        version="1",
        columns=[
            Column(
                name="ds",
                type=StringType(),
                attributes=[ColumnAttribute(attribute_type=dimension_attribute)],
                order=0,
            ),
            Column(
                name="user_id",
                type=IntegerType(),
                attributes=[ColumnAttribute(attribute_type=dimension_attribute)],
                order=2,
            ),
            Column(
                name="foo",
                type=FloatType(),
                attributes=[ColumnAttribute(attribute_type=dimension_attribute)],
                order=3,
            ),
        ],
        created_by_id=module__current_user.id,
    )
    parent_node = Node(
        name=parent_rev.name,
        namespace="default",
        type=NodeType.SOURCE,
        current_version="1",
        created_by_id=module__current_user.id,
    )
    parent_rev.node = parent_node

    child_node = Node(
        name="child",
        namespace="default",
        type=NodeType.METRIC,
        current_version="1",
        created_by_id=module__current_user.id,
    )
    child_rev = NodeRevision(
        name=child_node.name,
        node=child_node,
        type=child_node.type,
        version="1",
        query="SELECT COUNT(*) FROM parent",
        parents=[parent_node],
        created_by_id=module__current_user.id,
    )

    module__session.add(child_rev)
    await module__session.commit()

    response = await module__client.get("/metrics/child/")
    data = response.json()

    assert response.status_code == 200
    assert data["name"] == "child"
    assert data["query"] == "SELECT COUNT(*) FROM parent"
    assert data["dimensions"] == [
        {
            "is_primary_key": False,
            "name": "parent.ds",
            "node_display_name": "Parent",
            "node_name": "parent",
            "path": [],
            "type": "string",
            "filter_only": False,
        },
        {
            "is_primary_key": False,
            "name": "parent.foo",
            "node_display_name": "Parent",
            "node_name": "parent",
            "path": [],
            "type": "float",
            "filter_only": False,
        },
        {
            "is_primary_key": False,
            "name": "parent.user_id",
            "node_display_name": "Parent",
            "node_name": "parent",
            "path": [],
            "type": "int",
            "filter_only": False,
        },
    ]


@pytest.mark.asyncio
async def test_read_metrics_errors(
    module__session: AsyncSession,
    module__client: AsyncClient,
    module__current_user: User,
) -> None:
    """
    Test errors on ``GET /metrics/{node_id}/``.
    """
    database = Database(name="test", URI="sqlite://")
    node = Node(
        name="a-metric",
        namespace="default",
        type=NodeType.TRANSFORM,
        current_version="1",
        created_by_id=module__current_user.id,
    )
    node_revision = NodeRevision(
        name=node.name,
        type=node.type,
        node=node,
        version="1",
        query="SELECT 1 AS col",
        created_by_id=module__current_user.id,
    )
    module__session.add(database)
    module__session.add(node_revision)
    await module__session.execute(text("CREATE TABLE my_table (one TEXT)"))
    await module__session.commit()

    response = await module__client.get("/metrics/foo")
    assert response.status_code == 404
    data = response.json()
    assert data["message"] == "A node with name `foo` does not exist."

    response = await module__client.get("/metrics/a-metric")
    assert response.status_code == 400
    assert response.json() == {"detail": "Not a metric node: `a-metric`"}


@pytest.mark.asyncio
async def test_common_dimensions(
    module__client_with_roads: AsyncClient,
) -> None:
    """
    Test ``GET /metrics/common/dimensions``.
    """
    response = await module__client_with_roads.get(
        "/metrics/common/dimensions?"
        "metric=default.total_repair_order_discounts"
        "&metric=default.total_repair_cost",
    )
    assert response.status_code == 200
    assert response.json() == expected_dimensions


@pytest.mark.asyncio
async def test_no_common_dimensions(
    module__client_with_roads: AsyncClient,
) -> None:
    """
    Test getting common dimensions for metrics that have none in common
    """
    await module__client_with_roads.post(
        "/nodes/source/",
        json={
            "columns": [
                {
                    "name": "counts",
                    "type": "struct<a string, b bigint>",
                },
            ],
            "description": "Collection of dreams",
            "mode": "published",
            "name": "basic.dreams_1",
            "catalog": "public",
            "schema_": "basic",
            "table": "dreams",
        },
    )

    response = await module__client_with_roads.post(
        "/nodes/metric/",
        json={
            "query": "SELECT SUM(counts.b) FROM basic.dreams_1",
            "description": "Dream Counts",
            "mode": "published",
            "name": "basic.dream_count_1",
        },
    )
    response = await module__client_with_roads.get(
        "/metrics/common/dimensions?"
        "metric=basic.dream_count_1&metric=default.total_repair_order_discounts",
    )
    assert response.status_code == 200
    assert response.json() == []


@pytest.mark.asyncio
async def test_raise_common_dimensions_not_a_metric_node(
    module__client_with_account_revenue,
) -> None:
    """
    Test raising ``GET /metrics/common/dimensions`` when not a metric node
    """
    response = await module__client_with_account_revenue.get(
        "/metrics/common/dimensions?"
        "metric=default.total_repair_order_discounts"
        "&metric=default.payment_type",
    )
    assert response.status_code == 500
    assert response.json()["message"] == "Not a metric node: default.payment_type"


@pytest.mark.asyncio
async def test_raise_common_dimensions_metric_not_found(
    module__client_with_roads: AsyncClient,
) -> None:
    """
    Test raising ``GET /metrics/common/dimensions`` when metric not found
    """
    response = await module__client_with_roads.get(
        "/metrics/common/dimensions?metric=default.foo&metric=default.bar",
    )
    assert response.status_code == 500
    assert response.json() == {
        "errors": [
            {
                "code": 203,
                "context": "",
                "debug": None,
                "message": "Metric nodes not found: default.foo,default.bar",
            },
        ],
        "message": "Metric nodes not found: default.foo,default.bar",
        "warnings": [],
    }


@pytest.mark.asyncio
async def test_get_dimensions(module__client_with_roads: AsyncClient):
    """
    Testing get dimensions for a metric
    """
    response = await module__client_with_roads.get("/metrics/default.avg_repair_price/")

    data = response.json()
    assert data["dimensions"] == expected_dimensions


@pytest.mark.asyncio
async def test_get_multi_link_dimensions(
    module__client_with_dimension_link,
):
    """
    In some cases, the same dimension may be linked to different columns on a node.
    The returned dimension attributes should the join path between the given dimension
    attribute and the original node, in order to help disambiguate the source of the dimension.
    """
    response = await module__client_with_dimension_link.get(
        "/metrics/default.avg_user_age/",
    )
    assert response.json()["dimensions"] == [
        {
            "is_primary_key": True,
            "name": "default.date_dim.dateint[birth_country->formation_date]",
            "node_display_name": "Default: Date Dim",
            "node_name": "default.date_dim",
            "path": [
                "default.user_dim.birth_country",
                "default.special_country_dim.formation_date",
            ],
            "type": "int",
            "filter_only": False,
        },
        {
            "is_primary_key": True,
            "name": "default.date_dim.dateint[birth_country->last_election_date]",
            "node_display_name": "Default: Date Dim",
            "node_name": "default.date_dim",
            "path": [
                "default.user_dim.birth_country",
                "default.special_country_dim.last_election_date",
            ],
            "type": "int",
            "filter_only": False,
        },
        {
            "is_primary_key": True,
            "name": "default.date_dim.dateint[residence_country->formation_date]",
            "node_display_name": "Default: Date Dim",
            "node_name": "default.date_dim",
            "path": [
                "default.user_dim.residence_country",
                "default.special_country_dim.formation_date",
            ],
            "type": "int",
            "filter_only": False,
        },
        {
            "is_primary_key": True,
            "name": "default.date_dim.dateint[residence_country->last_election_date]",
            "node_display_name": "Default: Date Dim",
            "node_name": "default.date_dim",
            "path": [
                "default.user_dim.residence_country",
                "default.special_country_dim.last_election_date",
            ],
            "type": "int",
            "filter_only": False,
        },
        {
            "is_primary_key": False,
            "name": "default.date_dim.day[birth_country->formation_date]",
            "node_display_name": "Default: Date Dim",
            "node_name": "default.date_dim",
            "path": [
                "default.user_dim.birth_country",
                "default.special_country_dim.formation_date",
            ],
            "type": "int",
            "filter_only": False,
        },
        {
            "is_primary_key": False,
            "name": "default.date_dim.day[birth_country->last_election_date]",
            "node_display_name": "Default: Date Dim",
            "node_name": "default.date_dim",
            "path": [
                "default.user_dim.birth_country",
                "default.special_country_dim.last_election_date",
            ],
            "type": "int",
            "filter_only": False,
        },
        {
            "is_primary_key": False,
            "name": "default.date_dim.day[residence_country->formation_date]",
            "node_display_name": "Default: Date Dim",
            "node_name": "default.date_dim",
            "path": [
                "default.user_dim.residence_country",
                "default.special_country_dim.formation_date",
            ],
            "type": "int",
            "filter_only": False,
        },
        {
            "is_primary_key": False,
            "name": "default.date_dim.day[residence_country->last_election_date]",
            "node_display_name": "Default: Date Dim",
            "node_name": "default.date_dim",
            "path": [
                "default.user_dim.residence_country",
                "default.special_country_dim.last_election_date",
            ],
            "type": "int",
            "filter_only": False,
        },
        {
            "is_primary_key": False,
            "name": "default.date_dim.month[birth_country->formation_date]",
            "node_display_name": "Default: Date Dim",
            "node_name": "default.date_dim",
            "path": [
                "default.user_dim.birth_country",
                "default.special_country_dim.formation_date",
            ],
            "type": "int",
            "filter_only": False,
        },
        {
            "is_primary_key": False,
            "name": "default.date_dim.month[birth_country->last_election_date]",
            "node_display_name": "Default: Date Dim",
            "node_name": "default.date_dim",
            "path": [
                "default.user_dim.birth_country",
                "default.special_country_dim.last_election_date",
            ],
            "type": "int",
            "filter_only": False,
        },
        {
            "is_primary_key": False,
            "name": "default.date_dim.month[residence_country->formation_date]",
            "node_display_name": "Default: Date Dim",
            "node_name": "default.date_dim",
            "path": [
                "default.user_dim.residence_country",
                "default.special_country_dim.formation_date",
            ],
            "type": "int",
            "filter_only": False,
        },
        {
            "is_primary_key": False,
            "name": "default.date_dim.month[residence_country->last_election_date]",
            "node_display_name": "Default: Date Dim",
            "node_name": "default.date_dim",
            "path": [
                "default.user_dim.residence_country",
                "default.special_country_dim.last_election_date",
            ],
            "type": "int",
            "filter_only": False,
        },
        {
            "is_primary_key": False,
            "name": "default.date_dim.year[birth_country->formation_date]",
            "node_display_name": "Default: Date Dim",
            "node_name": "default.date_dim",
            "path": [
                "default.user_dim.birth_country",
                "default.special_country_dim.formation_date",
            ],
            "type": "int",
            "filter_only": False,
        },
        {
            "is_primary_key": False,
            "name": "default.date_dim.year[birth_country->last_election_date]",
            "node_display_name": "Default: Date Dim",
            "node_name": "default.date_dim",
            "path": [
                "default.user_dim.birth_country",
                "default.special_country_dim.last_election_date",
            ],
            "type": "int",
            "filter_only": False,
        },
        {
            "is_primary_key": False,
            "name": "default.date_dim.year[residence_country->formation_date]",
            "node_display_name": "Default: Date Dim",
            "node_name": "default.date_dim",
            "path": [
                "default.user_dim.residence_country",
                "default.special_country_dim.formation_date",
            ],
            "type": "int",
            "filter_only": False,
        },
        {
            "is_primary_key": False,
            "name": "default.date_dim.year[residence_country->last_election_date]",
            "node_display_name": "Default: Date Dim",
            "node_name": "default.date_dim",
            "path": [
                "default.user_dim.residence_country",
                "default.special_country_dim.last_election_date",
            ],
            "type": "int",
            "filter_only": False,
        },
        {
            "is_primary_key": True,
            "name": "default.special_country_dim.country_code[birth_country]",
            "node_display_name": "Default: Special Country Dim",
            "node_name": "default.special_country_dim",
            "path": ["default.user_dim.birth_country"],
            "type": "string",
            "filter_only": False,
        },
        {
            "is_primary_key": True,
            "name": "default.special_country_dim.country_code[residence_country]",
            "node_display_name": "Default: Special Country Dim",
            "node_name": "default.special_country_dim",
            "path": ["default.user_dim.residence_country"],
            "type": "string",
            "filter_only": False,
        },
        {
            "is_primary_key": False,
            "name": "default.special_country_dim.formation_date[birth_country]",
            "node_display_name": "Default: Special Country Dim",
            "node_name": "default.special_country_dim",
            "path": ["default.user_dim.birth_country"],
            "type": "int",
            "filter_only": False,
        },
        {
            "is_primary_key": False,
            "name": "default.special_country_dim.formation_date[residence_country]",
            "node_display_name": "Default: Special Country Dim",
            "node_name": "default.special_country_dim",
            "path": ["default.user_dim.residence_country"],
            "type": "int",
            "filter_only": False,
        },
        {
            "is_primary_key": False,
            "name": "default.special_country_dim.last_election_date[birth_country]",
            "node_display_name": "Default: Special Country Dim",
            "node_name": "default.special_country_dim",
            "path": ["default.user_dim.birth_country"],
            "type": "int",
            "filter_only": False,
        },
        {
            "is_primary_key": False,
            "name": "default.special_country_dim.last_election_date[residence_country]",
            "node_display_name": "Default: Special Country Dim",
            "node_name": "default.special_country_dim",
            "path": ["default.user_dim.residence_country"],
            "type": "int",
            "filter_only": False,
        },
        {
            "is_primary_key": False,
            "name": "default.special_country_dim.name[birth_country]",
            "node_display_name": "Default: Special Country Dim",
            "node_name": "default.special_country_dim",
            "path": ["default.user_dim.birth_country"],
            "type": "string",
            "filter_only": False,
        },
        {
            "is_primary_key": False,
            "name": "default.special_country_dim.name[residence_country]",
            "node_display_name": "Default: Special Country Dim",
            "node_name": "default.special_country_dim",
            "path": ["default.user_dim.residence_country"],
            "type": "string",
            "filter_only": False,
        },
        {
            "is_primary_key": False,
            "name": "default.user_dim.age",
            "node_display_name": "Default: User Dim",
            "node_name": "default.user_dim",
            "path": [],
            "type": "int",
            "filter_only": False,
        },
        {
            "is_primary_key": False,
            "name": "default.user_dim.birth_country",
            "node_display_name": "Default: User Dim",
            "node_name": "default.user_dim",
            "path": [],
            "type": "string",
            "filter_only": False,
        },
        {
            "is_primary_key": False,
            "name": "default.user_dim.residence_country",
            "node_display_name": "Default: User Dim",
            "node_name": "default.user_dim",
            "path": [],
            "type": "string",
            "filter_only": False,
        },
        {
            "is_primary_key": True,
            "name": "default.user_dim.user_id",
            "node_display_name": "Default: User Dim",
            "node_name": "default.user_dim",
            "path": [],
            "type": "int",
            "filter_only": False,
        },
    ]


@pytest.mark.asyncio
async def test_type_inference_structs(module__client_with_roads: AsyncClient):
    """
    Testing type resolution for structs select
    """
    await module__client_with_roads.post(
        "/nodes/source/",
        json={
            "columns": [
                {
                    "name": "counts",
                    "type": "struct<a string, b bigint>",
                },
            ],
            "description": "Collection of dreams",
            "mode": "published",
            "name": "basic.dreams_3",
            "catalog": "public",
            "schema_": "basic",
            "table": "dreams",
        },
    )

    response = await module__client_with_roads.post(
        "/nodes/metric/",
        json={
            "query": "SELECT SUM(counts.b) FROM basic.dreams_3",
            "description": "Dream Counts",
            "mode": "published",
            "name": "basic.dream_count",
        },
    )
    response.json()


@pytest.mark.asyncio
async def test_metric_expression_auto_aliased(module__client_with_roads: AsyncClient):
    """
    Testing that a metric's expression column is automatically aliased
    """
    await module__client_with_roads.post("/namespaces/basic")
    await module__client_with_roads.post(
        "/nodes/source/",
        json={
            "columns": [
                {
                    "name": "counts",
                    "type": "struct<a string, b bigint>",
                },
            ],
            "description": "Collection of dreams",
            "mode": "published",
            "name": "basic.dreams_4",
            "catalog": "public",
            "schema_": "basic",
            "table": "dreams",
        },
    )

    response = await module__client_with_roads.post(
        "/nodes/metric/",
        json={
            "query": "SELECT SUM(counts.b) + SUM(counts.b) FROM basic.dreams_4",
            "description": "Dream Counts",
            "mode": "published",
            "name": "basic.dream_count_4",
        },
    )
    assert response.status_code == 201
    data = response.json()
    assert data["query"] == "SELECT SUM(counts.b) + SUM(counts.b) FROM basic.dreams_4"
    assert data["columns"] == [
        {
            "attributes": [],
            "dimension": None,
            "display_name": "Basic: Dream Count 4",
            "name": "basic_DOT_dream_count_4",
            "partition": None,
            "type": "bigint",
        },
    ]


@pytest.mark.asyncio
async def test_raise_on_malformated_expression_alias(
    module__client_with_roads: AsyncClient,
):
    """
    Test that using an invalid alias for a metric expression is saved, but the alias
    is overridden when creating the column name
    """
    await module__client_with_roads.post("/namespaces/basic")
    response = await module__client_with_roads.post(
        "/nodes/source/",
        json={
            "columns": [
                {
                    "name": "counts",
                    "type": "struct<a string, b bigint>",
                },
            ],
            "description": "Collection of dreams",
            "mode": "published",
            "name": "basic.dreams_5",
            "catalog": "public",
            "schema_": "basic",
            "table": "dreams",
        },
    )
    assert response.status_code == 200

    response = await module__client_with_roads.post(
        "/nodes/metric/",
        json={
            "query": "SELECT SUM(counts.b) as foo FROM basic.dreams_5",
            "description": "Dream Counts",
            "mode": "published",
            "name": "basic.dream_count_5",
        },
    )
    assert response.status_code == 201
    data = response.json()
    assert data["query"] == "SELECT SUM(counts.b) as foo FROM basic.dreams_5"
    assert data["columns"][0]["name"] == "basic_DOT_dream_count_5"


@pytest.mark.asyncio
async def test_raise_on_multiple_expressions(module__client_with_roads: AsyncClient):
    """
    Testing raising when there is more than one expression
    """
    await module__client_with_roads.post("/namespaces/basic")
    response = await module__client_with_roads.post(
        "/nodes/source/",
        json={
            "columns": [
                {
                    "name": "counts",
                    "type": "struct<a string, b bigint>",
                },
            ],
            "description": "Collection of dreams",
            "mode": "published",
            "name": "basic.dreams_2",
            "catalog": "public",
            "schema_": "basic",
            "table": "dreams",
        },
    )
    assert response.status_code == 200

    response = await module__client_with_roads.post(
        "/nodes/metric/",
        json={
            "query": "SELECT SUM(counts.b), COUNT(counts.b) FROM basic.dreams_2",
            "description": "Dream Counts",
            "mode": "published",
            "name": "basic.dream_count_2",
        },
    )
    assert response.status_code == 400
    assert (
        "Metric queries can only have a single expression, found 2"
    ) in response.json()["message"]


@pytest.mark.asyncio
async def test_list_metric_metadata(module__client: AsyncClient):
    """
    Test listing metric metadata values
    """
    metric_metadata_options = (await module__client.get("/metrics/metadata")).json()
    assert metric_metadata_options == {
        "directions": ["higher_is_better", "lower_is_better", "neutral"],
        "units": [
            {
                "abbreviation": None,
                "category": "",
                "description": None,
                "label": "Unknown",
                "name": "unknown",
            },
            {
                "abbreviation": None,
                "category": "",
                "description": None,
                "label": "Unitless",
                "name": "unitless",
            },
            {
                "abbreviation": "%",
                "category": "",
                "description": "A ratio expressed as a number out of 100. Values "
                "range from 0 to 100.",
                "label": "Percentage",
                "name": "percentage",
            },
            {
                "abbreviation": "",
                "category": "",
                "description": "A ratio that compares a part to a whole. Values "
                "range from 0 to 1.",
                "label": "Proportion",
                "name": "proportion",
            },
            {
                "abbreviation": "$",
                "category": "currency",
                "description": None,
                "label": "Dollar",
                "name": "dollar",
            },
            {
                "abbreviation": "s",
                "category": "time",
                "description": None,
                "label": "Second",
                "name": "second",
            },
            {
                "abbreviation": "m",
                "category": "time",
                "description": None,
                "label": "Minute",
                "name": "minute",
            },
            {
                "abbreviation": "h",
                "category": "time",
                "description": None,
                "label": "Hour",
                "name": "hour",
            },
            {
                "abbreviation": "d",
                "category": "time",
                "description": None,
                "label": "Day",
                "name": "day",
            },
            {
                "abbreviation": "w",
                "category": "time",
                "description": None,
                "label": "Week",
                "name": "week",
            },
            {
                "abbreviation": "mo",
                "category": "time",
                "description": None,
                "label": "Month",
                "name": "month",
            },
            {
                "abbreviation": "y",
                "category": "time",
                "description": None,
                "label": "Year",
                "name": "year",
            },
        ],
    }
