"""
Tests for tags GQL queries.
"""

import pytest
import pytest_asyncio
from httpx import AsyncClient


@pytest_asyncio.fixture(scope="module")
async def client_with_tags(
    module__client_with_roads: AsyncClient,
) -> AsyncClient:
    """
    Provides a DJ client fixture seeded with tags
    """
    await module__client_with_roads.post(
        "/tags/",
        json={
            "name": "sales_report",
            "display_name": "Sales Report",
            "description": "All metrics for sales",
            "tag_type": "report",
            "tag_metadata": {},
        },
    )
    await module__client_with_roads.post(
        "/tags/",
        json={
            "name": "other_report",
            "display_name": "Other Report",
            "description": "Random",
            "tag_type": "report",
            "tag_metadata": {},
        },
    )
    await module__client_with_roads.post(
        "/tags/",
        json={
            "name": "coffee",
            "display_name": "Coffee",
            "description": "A drink",
            "tag_type": "drinks",
            "tag_metadata": {},
        },
    )
    await module__client_with_roads.post(
        "/tags/",
        json={
            "name": "tea",
            "display_name": "Tea",
            "description": "Another drink",
            "tag_type": "drinks",
            "tag_metadata": {},
        },
    )

    await module__client_with_roads.post(
        "/nodes/default.total_repair_cost/tags/?tag_names=sales_report",
    )
    await module__client_with_roads.post(
        "/nodes/default.avg_repair_price/tags/?tag_names=sales_report",
    )
    await module__client_with_roads.post(
        "/nodes/default.num_repair_orders/tags/?tag_names=other_report",
    )
    return module__client_with_roads


@pytest.mark.asyncio
async def test_list_tags(
    client_with_tags: AsyncClient,
) -> None:
    """
    Test listing tags
    """
    query = """
    {
      listTags {
        name
        description
        displayName
        tagType
        tagMetadata
      }
    }
    """
    response = await client_with_tags.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data == {
        "data": {
            "listTags": [
                {
                    "description": "All metrics for sales",
                    "displayName": "Sales Report",
                    "name": "sales_report",
                    "tagMetadata": {},
                    "tagType": "report",
                },
                {
                    "description": "Random",
                    "displayName": "Other Report",
                    "name": "other_report",
                    "tagMetadata": {},
                    "tagType": "report",
                },
                {
                    "description": "A drink",
                    "displayName": "Coffee",
                    "name": "coffee",
                    "tagMetadata": {},
                    "tagType": "drinks",
                },
                {
                    "description": "Another drink",
                    "displayName": "Tea",
                    "name": "tea",
                    "tagMetadata": {},
                    "tagType": "drinks",
                },
            ],
        },
    }


@pytest.mark.asyncio
async def test_list_tag_types(
    client_with_tags: AsyncClient,
) -> None:
    """
    Test listing tag types
    """
    query = """
    {
      listTagTypes
    }
    """
    response = await client_with_tags.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data == {"data": {"listTagTypes": ["report", "drinks"]}}


@pytest.mark.asyncio
async def test_find_tags_by_type(
    client_with_tags: AsyncClient,
) -> None:
    """
    Test finding tags by tag type
    """
    query = """
    {
      listTags(tagTypes: ["drinks"]) {
        name
      }
    }
    """
    response = await client_with_tags.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data == {"data": {"listTags": [{"name": "coffee"}, {"name": "tea"}]}}

    query = """
    {
      listTags(tagTypes: ["report"]) {
        name
      }
    }
    """
    response = await client_with_tags.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data == {
        "data": {"listTags": [{"name": "sales_report"}, {"name": "other_report"}]},
    }

    query = """
    {
      listTags(tagTypes: ["random"]) {
        name
      }
    }
    """
    response = await client_with_tags.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data == {"data": {"listTags": []}}


@pytest.mark.asyncio
async def test_find_tags_by_name(
    client_with_tags: AsyncClient,
) -> None:
    """
    Test finding tags by tag type
    """
    query = """
    {
      listTags(tagNames: ["coffee", "tea"]) {
        name
      }
    }
    """
    response = await client_with_tags.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data == {"data": {"listTags": [{"name": "coffee"}, {"name": "tea"}]}}


@pytest.mark.asyncio
async def test_tag_get_nodes(
    client_with_tags: AsyncClient,
) -> None:
    """
    Test listing tags
    """
    query = """
    {
      listTags (tagNames: ["sales_report"]) {
        name
        nodes {
          name
          current {
            columns {
              name
            }
          }
        }
      }
    }
    """
    response = await client_with_tags.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data == {
        "data": {
            "listTags": [
                {
                    "name": "sales_report",
                    "nodes": [
                        {
                            "current": {
                                "columns": [
                                    {
                                        "name": "default_DOT_avg_repair_price",
                                    },
                                ],
                            },
                            "name": "default.avg_repair_price",
                        },
                        {
                            "current": {
                                "columns": [
                                    {
                                        "name": "default_DOT_total_repair_cost",
                                    },
                                ],
                            },
                            "name": "default.total_repair_cost",
                        },
                    ],
                },
            ],
        },
    }

    query = """
    {
      listTags {
        name
        nodes {
          name
        }
      }
    }
    """
    response = await client_with_tags.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()
    assert data == {
        "data": {
            "listTags": [
                {
                    "name": "sales_report",
                    "nodes": [
                        {
                            "name": "default.avg_repair_price",
                        },
                        {
                            "name": "default.total_repair_cost",
                        },
                    ],
                },
                {
                    "name": "other_report",
                    "nodes": [
                        {
                            "name": "default.num_repair_orders",
                        },
                    ],
                },
                {
                    "name": "coffee",
                    "nodes": [],
                },
                {
                    "name": "tea",
                    "nodes": [],
                },
            ],
        },
    }
