"""
Tests for customMetadataFilters on the GraphQL findNodes query.
"""

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_find_nodes_by_custom_metadata_matching(client_with_roads: AsyncClient):
    """findNodes with a matching customMetadataFilter returns the node."""
    # default.repair_orders_fact seeds custom_metadata {"foo": "bar"}
    query = """
    {
      findNodes(customMetadataFilters: [{key: "foo", op: EQ, value: "bar"}]) {
        name
        current { customMetadata }
      }
    }
    """
    resp = await client_with_roads.post("/graphql", json={"query": query})
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("errors") is None
    names = {n["name"] for n in data["data"]["findNodes"]}
    assert names == {
        "default.repair_orders_fact",
        "default.num_repair_orders",
        "default.large_revenue_payments_only_custom",
    }


@pytest.mark.asyncio
async def test_find_nodes_by_custom_metadata_non_matching(
    client_with_roads: AsyncClient,
):
    """findNodes with a non-matching customMetadataFilter excludes the node."""
    query = """
    {
      findNodes(customMetadataFilters: [{key: "foo", op: EQ, value: "notbar"}]) {
        name
      }
    }
    """
    resp = await client_with_roads.post("/graphql", json={"query": query})
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("errors") is None
    names = {n["name"] for n in data["data"]["findNodes"]}
    assert "default.repair_orders_fact" not in names


@pytest.mark.asyncio
async def test_find_nodes_paginated_by_custom_metadata_matching(
    client_with_roads: AsyncClient,
):
    """findNodesPaginated filters edges and totalCount by custom metadata."""
    query = """
    {
      findNodesPaginated(
        customMetadataFilters: [{key: "foo", op: EQ, value: "bar"}]
        limit: 100
      ) {
        edges { node { name } }
        totalCount
      }
    }
    """
    resp = await client_with_roads.post("/graphql", json={"query": query})
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("errors") is None
    results = data["data"]["findNodesPaginated"]
    names = {edge["node"]["name"] for edge in results["edges"]}
    assert names == {
        "default.repair_orders_fact",
        "default.num_repair_orders",
        "default.large_revenue_payments_only_custom",
    }
    assert results["totalCount"] == 3


@pytest.mark.asyncio
async def test_find_nodes_paginated_by_custom_metadata_non_matching(
    client_with_roads: AsyncClient,
):
    """findNodesPaginated with a non-matching filter returns no edges."""
    query = """
    {
      findNodesPaginated(
        customMetadataFilters: [{key: "foo", op: EQ, value: "notbar"}]
        limit: 100
      ) {
        edges { node { name } }
        totalCount
      }
    }
    """
    resp = await client_with_roads.post("/graphql", json={"query": query})
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("errors") is None
    results = data["data"]["findNodesPaginated"]
    assert results["edges"] == []
    assert results["totalCount"] == 0
