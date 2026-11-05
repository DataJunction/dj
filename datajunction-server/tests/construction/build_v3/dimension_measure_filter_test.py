"""
A filter on a roled dimension attribute also applies to each joined dimension
that links the same role itself, however many hops it is from the fact. A
dimension with no such link of its own is left unfiltered.
"""

import pytest

from tests.construction.build_v3 import assert_sql_equal

SHIPPING = "v3.location.location_id[shipping] IN (1, 2)"
BILLING = "v3.location.location_id[billing] IN (1, 2)"


async def post(client, path, body):
    resp = await client.post(path, json=body)
    assert resp.status_code in (200, 201), resp.json()


async def source(client, name, table, columns):
    await post(
        client,
        "/nodes/source/",
        {
            "name": f"v3.{name}",
            "columns": [{"name": c, "type": t} for c, t in columns],
            "mode": "published",
            "catalog": "default",
            "schema_": "v3",
            "table": table,
        },
    )


async def dimension(client, name, columns, key):
    await post(
        client,
        "/nodes/dimension/",
        {
            "name": f"v3.{name}",
            "query": f"SELECT {', '.join(columns)} FROM v3.src_{name}",
            "mode": "published",
            "primary_key": key,
        },
    )


async def link(client, node, dimension_node, join_on, role=None, join_type="left"):
    await post(
        client,
        f"/nodes/v3.{node}/link/",
        {
            "dimension_node": dimension_node,
            "join_type": join_type,
            "join_on": join_on,
            **({"role": role} if role else {}),
        },
    )


async def build_shipments(client):
    """`shipments` links `location` as shipping and billing."""
    await source(
        client,
        "src_shipments",
        "shipments",
        [
            ("shipment_id", "int"),
            ("warehouse_id", "int"),
            ("shipping_location_id", "int"),
            ("billing_location_id", "int"),
            ("amount", "double"),
        ],
    )
    await post(
        client,
        "/nodes/transform/",
        {
            "name": "v3.shipments",
            "query": "SELECT shipment_id, warehouse_id, shipping_location_id, "
            "billing_location_id, amount FROM v3.src_shipments",
            "mode": "published",
            "primary_key": ["shipment_id"],
        },
    )
    await link(
        client,
        "shipments",
        "v3.location",
        "v3.shipments.shipping_location_id = v3.location.location_id",
        "shipping",
    )
    await link(
        client,
        "shipments",
        "v3.location",
        "v3.shipments.billing_location_id = v3.location.location_id",
        "billing",
    )
    await post(
        client,
        "/nodes/metric/",
        {
            "name": "v3.shipment_amount",
            "query": "SELECT SUM(amount) FROM v3.shipments",
            "mode": "published",
        },
    )


async def add_warehouse(client, with_location_link):
    await source(
        client,
        "src_warehouse",
        "warehouses",
        [("warehouse_id", "int"), ("location_id", "int"), ("tier", "string")],
    )
    await dimension(
        client,
        "warehouse",
        ["warehouse_id", "location_id", "tier"],
        ["warehouse_id"],
    )
    await link(
        client,
        "shipments",
        "v3.warehouse",
        "v3.shipments.warehouse_id = v3.warehouse.warehouse_id",
    )
    if with_location_link:
        await link(
            client,
            "warehouse",
            "v3.location",
            "v3.warehouse.location_id = v3.location.location_id",
            "shipping",
        )


async def add_supplier_and_contract(client, supplier_location_link):
    """`warehouse` -> `supplier` -> `contract`."""
    await source(
        client,
        "src_supplier",
        "suppliers",
        [
            ("supplier_id", "int"),
            ("warehouse_id", "int"),
            ("location_id", "int"),
            ("name", "string"),
        ],
    )
    await dimension(
        client,
        "supplier",
        ["supplier_id", "warehouse_id", "location_id", "name"],
        ["supplier_id"],
    )
    await source(
        client,
        "src_contract",
        "contracts",
        [("supplier_id", "int"), ("location_id", "int"), ("status", "string")],
    )
    await dimension(
        client,
        "contract",
        ["supplier_id", "location_id", "status"],
        ["supplier_id"],
    )
    await link(
        client,
        "warehouse",
        "v3.supplier",
        "v3.warehouse.warehouse_id = v3.supplier.warehouse_id",
    )
    await link(
        client,
        "supplier",
        "v3.contract",
        "v3.supplier.supplier_id = v3.contract.supplier_id",
    )
    await link(
        client,
        "contract",
        "v3.location",
        "v3.contract.location_id = v3.location.location_id",
        "shipping",
    )
    if supplier_location_link:
        await link(
            client,
            "supplier",
            "v3.location",
            "v3.supplier.location_id = v3.location.location_id",
            "shipping",
        )


async def metric_sql(client, dimensions, filters):
    response = await client.get(
        "/sql/metrics/v3/",
        params={
            "metrics": ["v3.shipment_amount"],
            "dimensions": dimensions,
            "filters": filters,
            "dialect": "trino",
        },
    )
    assert response.status_code == 200, response.json()
    return response.json()["sql"]


@pytest.mark.asyncio
async def test_one_hop_dimension_with_the_role_is_filtered(client_with_build_v3):
    await build_shipments(client_with_build_v3)
    await add_warehouse(client_with_build_v3, with_location_link=True)
    assert_sql_equal(
        await metric_sql(client_with_build_v3, ["v3.warehouse.tier"], [SHIPPING]),
        """
        WITH
        v3_shipments AS (
          SELECT warehouse_id, amount
          FROM default.v3.shipments
          WHERE shipping_location_id IN (1, 2)
        ),
        v3_warehouse AS (
          SELECT warehouse_id, tier
          FROM default.v3.warehouses
          WHERE location_id IN (1, 2)
        ),
        shipments_0 AS (
          SELECT t2.tier, SUM(t1.amount) amount_sum_HASH
          FROM v3_shipments t1
          LEFT OUTER JOIN v3_warehouse t2 ON t1.warehouse_id = t2.warehouse_id
          GROUP BY t2.tier
        )
        SELECT shipments_0.tier AS tier,
          SUM(shipments_0.amount_sum_HASH) AS shipment_amount
        FROM shipments_0
        GROUP BY shipments_0.tier
        """,
        normalize_aliases=True,
    )


@pytest.mark.asyncio
async def test_dimension_with_another_role_is_not_filtered(client_with_build_v3):
    await build_shipments(client_with_build_v3)
    await add_warehouse(client_with_build_v3, with_location_link=True)
    assert_sql_equal(
        await metric_sql(client_with_build_v3, ["v3.warehouse.tier"], [BILLING]),
        """
        WITH
        v3_shipments AS (
          SELECT warehouse_id, amount
          FROM default.v3.shipments
          WHERE billing_location_id IN (1, 2)
        ),
        v3_warehouse AS (
          SELECT warehouse_id, tier FROM default.v3.warehouses
        ),
        shipments_0 AS (
          SELECT t2.tier, SUM(t1.amount) amount_sum_HASH
          FROM v3_shipments t1
          LEFT OUTER JOIN v3_warehouse t2 ON t1.warehouse_id = t2.warehouse_id
          GROUP BY t2.tier
        )
        SELECT shipments_0.tier AS tier,
          SUM(shipments_0.amount_sum_HASH) AS shipment_amount
        FROM shipments_0
        GROUP BY shipments_0.tier
        """,
        normalize_aliases=True,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("supplier_location_link", [False, True])
async def test_three_hop_dimensions(client_with_build_v3, supplier_location_link):
    """The warehouse has no link to `location` and is never filtered; the
    supplier is filtered only when it has its own `shipping` link."""
    await build_shipments(client_with_build_v3)
    await add_warehouse(client_with_build_v3, with_location_link=False)
    await add_supplier_and_contract(client_with_build_v3, supplier_location_link)
    supplier_where = "WHERE location_id IN (1, 2)" if supplier_location_link else ""
    assert_sql_equal(
        await metric_sql(
            client_with_build_v3,
            ["v3.supplier.name", "v3.contract.status"],
            [SHIPPING],
        ),
        f"""
        WITH
        v3_contract AS (
          SELECT supplier_id, status
          FROM default.v3.contracts
          WHERE location_id IN (1, 2)
        ),
        v3_shipments AS (
          SELECT warehouse_id, amount
          FROM default.v3.shipments
          WHERE shipping_location_id IN (1, 2)
        ),
        v3_supplier AS (
          SELECT supplier_id, warehouse_id, name
          FROM default.v3.suppliers
          {supplier_where}
        ),
        v3_warehouse AS (
          SELECT warehouse_id FROM default.v3.warehouses
        ),
        shipments_0 AS (
          SELECT t3.name, t4.status, SUM(t1.amount) amount_sum_HASH
          FROM v3_shipments t1
          LEFT OUTER JOIN v3_warehouse t2 ON t1.warehouse_id = t2.warehouse_id
          LEFT OUTER JOIN v3_supplier t3 ON t2.warehouse_id = t3.warehouse_id
          LEFT OUTER JOIN v3_contract t4 ON t3.supplier_id = t4.supplier_id
          GROUP BY t3.name, t4.status
        )
        SELECT shipments_0.name AS name,
          shipments_0.status AS status,
          SUM(shipments_0.amount_sum_HASH) AS shipment_amount
        FROM shipments_0
        GROUP BY shipments_0.name, shipments_0.status
        """,
        normalize_aliases=True,
    )
