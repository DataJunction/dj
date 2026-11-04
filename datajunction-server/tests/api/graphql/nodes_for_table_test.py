"""
Tests for the physical-table filter and the nodesForTable GraphQL query
"""

import pytest
from httpx import AsyncClient


async def _find_nodes(client: AsyncClient, tables: str) -> list[dict]:
    """
    Run findNodes with the given tables filter literal.
    """
    query = f"""
    {{
        findNodes(tables: {tables}) {{
            name
            type
        }}
    }}
    """
    response = await client.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert "errors" not in data, data
    return data["data"]["findNodes"]


@pytest.mark.asyncio
async def test_find_nodes_by_table(client_with_roads: AsyncClient) -> None:
    """
    A table may be fully qualified, partially qualified or bare, in any case.
    """
    # Two source nodes in the roads example point at the same physical table.
    expected = {"default.repair_order_details", "foo.bar.repair_order_details"}

    for entry in (
        "default.roads.repair_order_details",
        "roads.repair_order_details",
        "repair_order_details",
        "DEFAULT.Roads.Repair_Order_Details",
        # A source node name is the qualified table with a prefix; parts are
        # read from the right, so it resolves too.
        "source.default.roads.repair_order_details",
    ):
        nodes = await _find_nodes(client_with_roads, f'["{entry}"]')
        assert {node["name"] for node in nodes} == expected, entry
        assert {node["type"] for node in nodes} == {"SOURCE"}, entry


@pytest.mark.asyncio
async def test_find_nodes_by_multiple_tables(client_with_roads: AsyncClient) -> None:
    """
    Multiple entries are OR'd together.
    """
    nodes = await _find_nodes(
        client_with_roads,
        '["roads.repair_order_details", "repair_type"]',
    )
    assert {node["name"] for node in nodes} == {
        "default.repair_order_details",
        "foo.bar.repair_order_details",
        "default.repair_type",
        "foo.bar.repair_type",
    }


@pytest.mark.asyncio
async def test_find_nodes_by_table_no_match(client_with_roads: AsyncClient) -> None:
    """
    A wrong catalog or schema, an unknown table, and a blank entry all match
    nothing rather than falling back to every node. (An empty list is no filter
    at all, as with every other list filter here.)
    """
    for tables in (
        '["nope.roads.repair_order_details"]',
        '["nope.repair_order_details"]',
        '["does_not_exist"]',
        '[" "]',
    ):
        assert await _find_nodes(client_with_roads, tables) == [], tables


@pytest.mark.asyncio
async def test_find_nodes_paginated_by_table(client_with_roads: AsyncClient) -> None:
    """
    The filter is available on the paginated query, and feeds totalCount.
    """
    query = """
    {
        findNodesPaginated(tables: ["roads.repair_order_details"]) {
            totalCount
            edges {
                node {
                    name
                }
            }
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()["data"]["findNodesPaginated"]
    assert data["totalCount"] == 2
    assert {edge["node"]["name"] for edge in data["edges"]} == {
        "default.repair_order_details",
        "foo.bar.repair_order_details",
    }


@pytest.mark.asyncio
async def test_nodes_for_table(client_with_roads: AsyncClient) -> None:
    """
    nodesForTable returns the source node plus everything downstream of it.
    """
    query = """
    {
        nodesForTable(table: "roads.repair_order_details") {
            name
            type
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    nodes = response.json()["data"]["nodesForTable"]
    by_type: dict[str, set[str]] = {}
    for node in nodes:
        by_type.setdefault(node["type"], set()).add(node["name"])
    assert by_type["SOURCE"] == {
        "default.repair_order_details",
        "foo.bar.repair_order_details",
    }
    assert "default.repair_orders_fact" in by_type["TRANSFORM"]
    assert "default.total_repair_cost" in by_type["METRIC"]


@pytest.mark.asyncio
async def test_nodes_for_table_node_types(client_with_roads: AsyncClient) -> None:
    """
    nodeTypes filters the whole result, the source node included.
    """
    query = """
    {
        nodesForTable(table: "roads.repair_order_details", nodeTypes: [METRIC]) {
            name
            type
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    nodes = response.json()["data"]["nodesForTable"]
    assert nodes
    assert {node["type"] for node in nodes} == {"METRIC"}
    assert "default.total_repair_cost" in {node["name"] for node in nodes}


@pytest.mark.asyncio
async def test_nodes_for_table_source_only(client_with_roads: AsyncClient) -> None:
    """
    includeDownstream: false returns only the source nodes on the table.
    """
    query = """
    {
        nodesForTable(
            table: "roads.repair_order_details"
            includeDownstream: false
        ) {
            name
            type
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    nodes = response.json()["data"]["nodesForTable"]
    assert {node["type"] for node in nodes} == {"SOURCE"}
    assert {node["name"] for node in nodes} == {
        "default.repair_order_details",
        "foo.bar.repair_order_details",
    }


@pytest.mark.asyncio
async def test_nodes_for_table_unknown(client_with_roads: AsyncClient) -> None:
    """
    An unknown table yields no nodes rather than an error.
    """
    query = """
    {
        nodesForTable(table: "does_not_exist") {
            name
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert "errors" not in data, data
    assert data["data"]["nodesForTable"] == []


@pytest.mark.asyncio
async def test_nodes_for_table_pages_through_sources(
    client_with_roads: AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Sources beyond one page are still returned, with their downstream nodes.
    """
    from datajunction_server.api.graphql.queries import tables

    # Two sources share the table, so a page size of one spans two pages.
    monkeypatch.setattr(tables, "SOURCE_PAGE_SIZE", 1)
    query = """
    {
        nodesForTable(table: "roads.repair_order_details") {
            name
            type
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    nodes = response.json()["data"]["nodesForTable"]
    names = {node["name"] for node in nodes}
    assert {"default.repair_order_details", "foo.bar.repair_order_details"} <= names
    assert "default.total_repair_cost" in names
    assert len(nodes) == len(names)


@pytest.mark.asyncio
async def test_find_nodes_by_table_with_fragment(
    client_with_roads: AsyncClient,
) -> None:
    """
    The table filter composes with filters that already joined the revision.
    """
    query = """
    {
        findNodes(tables: ["roads.repair_order_details"], fragment: "foo.bar") {
            name
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    nodes = response.json()["data"]["findNodes"]
    assert [node["name"] for node in nodes] == ["foo.bar.repair_order_details"]
