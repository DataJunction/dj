"""
Tests for the collections GraphQL queries.
"""

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_list_collections_basic(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test listing collections with basic fields.
    This tests eager loading of created_by and SQL subquery for node_count.
    """
    # Create a few collections via REST API
    response = await client_with_roads.post(
        "/collections/",
        json={
            "name": "Repair Metrics",
            "description": "Collection of repair-related metrics",
        },
    )
    assert response.status_code == 201

    response = await client_with_roads.post(
        "/collections/",
        json={
            "name": "Dimension Tables",
            "description": "Collection of dimension tables",
        },
    )
    assert response.status_code == 201

    # Add some nodes to the first collection
    response = await client_with_roads.post(
        "/collections/Repair%20Metrics/nodes/",
        json=["default.num_repair_orders", "default.avg_repair_price"],
    )
    assert response.status_code == 204

    # Add nodes to the second collection
    response = await client_with_roads.post(
        "/collections/Dimension%20Tables/nodes/",
        json=["default.hard_hat", "default.dispatcher", "default.municipality_dim"],
    )
    assert response.status_code == 204

    # Query collections via GraphQL
    query = """
    {
        listCollections {
            id
            name
            description
            nodeCount
            createdBy {
                username
                email
            }
            createdAt
        }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    collections = data["data"]["listCollections"]
    assert len(collections) == 2

    # Verify the collections data
    repair_metrics = next(c for c in collections if c["name"] == "Repair Metrics")
    assert repair_metrics["description"] == "Collection of repair-related metrics"
    assert repair_metrics["nodeCount"] == 2
    assert repair_metrics["createdBy"]["username"] == "dj"
    assert repair_metrics["createdBy"]["email"] == "dj@datajunction.io"
    assert repair_metrics["createdAt"] is not None

    dimension_tables = next(c for c in collections if c["name"] == "Dimension Tables")
    assert dimension_tables["description"] == "Collection of dimension tables"
    assert dimension_tables["nodeCount"] == 3
    assert dimension_tables["createdBy"]["username"] == "dj"


@pytest.mark.asyncio
async def test_list_collections_with_nodes(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test listing collections and fetching their nodes.
    This tests the dataloader implementation for the nodes() field.
    """
    # Create a collection with nodes
    response = await client_with_roads.post(
        "/collections/",
        json={
            "name": "Test Collection",
            "description": "A test collection with various nodes",
        },
    )
    assert response.status_code == 201

    # Add nodes to the collection
    response = await client_with_roads.post(
        "/collections/Test%20Collection/nodes/",
        json=[
            "default.repair_orders",
            "default.hard_hat",
            "default.num_repair_orders",
        ],
    )
    assert response.status_code == 204

    # Query collections with nodes via GraphQL
    query = """
    {
        listCollections {
            name
            nodeCount
            nodes {
                name
                type
            }
        }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    collections = data["data"]["listCollections"]
    assert len(collections) == 1

    collection = collections[0]
    assert collection["name"] == "Test Collection"
    assert collection["nodeCount"] == 3

    # Verify nodes were loaded
    nodes = collection["nodes"]
    assert len(nodes) == 3

    node_names = {node["name"] for node in nodes}
    assert node_names == {
        "default.repair_orders",
        "default.hard_hat",
        "default.num_repair_orders",
    }

    # Verify node types
    node_types = {node["name"]: node["type"] for node in nodes}
    assert node_types["default.repair_orders"] == "SOURCE"
    assert node_types["default.hard_hat"] == "DIMENSION"
    assert node_types["default.num_repair_orders"] == "METRIC"


@pytest.mark.asyncio
async def test_list_collections_empty_collection(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test listing collections that have no nodes.
    Verifies that node_count is 0 and nodes list is empty.
    """
    # Create an empty collection
    response = await client_with_roads.post(
        "/collections/",
        json={
            "name": "Empty Collection",
            "description": "A collection with no nodes",
        },
    )
    assert response.status_code == 201

    # Query via GraphQL
    query = """
    {
        listCollections {
            name
            nodeCount
            nodes {
                name
            }
        }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    collections = data["data"]["listCollections"]
    assert len(collections) == 1

    collection = collections[0]
    assert collection["name"] == "Empty Collection"
    assert collection["nodeCount"] == 0
    assert collection["nodes"] == []


@pytest.mark.asyncio
async def test_list_collections_filter_by_creator(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test filtering collections by creator username.
    """
    # Create a collection (will be created by 'dj' user)
    response = await client_with_roads.post(
        "/collections/",
        json={
            "name": "My Collection",
            "description": "Collection created by dj",
        },
    )
    assert response.status_code == 201

    # Query filtering by creator
    query = """
    {
        listCollections(createdBy: "dj") {
            name
            createdBy {
                username
            }
        }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    collections = data["data"]["listCollections"]
    assert len(collections) >= 1

    # All collections should be created by 'dj'
    for collection in collections:
        assert collection["createdBy"]["username"] == "dj"

    # Query filtering by non-existent user
    query = """
    {
        listCollections(createdBy: "nonexistent") {
            name
        }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    collections = data["data"]["listCollections"]
    assert len(collections) == 0


@pytest.mark.asyncio
async def test_list_collections_with_limit(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test limiting the number of collections returned.
    """
    # Create several collections
    for i in range(5):
        response = await client_with_roads.post(
            "/collections/",
            json={
                "name": f"Collection {i}",
                "description": f"Collection number {i}",
            },
        )
        assert response.status_code == 201

    # Query with limit
    query = """
    {
        listCollections(limit: 3) {
            name
        }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    collections = data["data"]["listCollections"]
    assert len(collections) == 3


@pytest.mark.asyncio
async def test_list_collections_dataloader_batching(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test that the dataloader properly batches node loading for multiple collections.
    This test creates multiple collections and verifies that nodes are loaded
    efficiently when querying multiple collections at once.
    """
    # Create multiple collections with nodes
    collections_data = [
        {
            "name": "Collection A",
            "nodes": ["default.repair_orders", "default.hard_hat"],
        },
        {
            "name": "Collection B",
            "nodes": ["default.dispatcher", "default.municipality_dim"],
        },
        {
            "name": "Collection C",
            "nodes": ["default.num_repair_orders"],
        },
    ]

    for coll_data in collections_data:
        # Create collection
        response = await client_with_roads.post(
            "/collections/",
            json={
                "name": coll_data["name"],
                "description": f"Test collection {coll_data['name']}",
            },
        )
        assert response.status_code == 201

        # Add nodes
        collection_name = coll_data["name"].replace(" ", "%20")
        response = await client_with_roads.post(
            f"/collections/{collection_name}/nodes/",
            json=coll_data["nodes"],
        )
        assert response.status_code == 204

    # Query all collections with nodes in a single request
    # The dataloader should batch the node loading
    query = """
    {
        listCollections {
            name
            nodeCount
            nodes {
                name
                type
            }
        }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    collections = data["data"]["listCollections"]
    assert len(collections) == 3

    # Verify each collection has the correct nodes
    for coll in collections:
        if coll["name"] == "Collection A":
            assert coll["nodeCount"] == 2
            assert len(coll["nodes"]) == 2
            node_names = {n["name"] for n in coll["nodes"]}
            assert node_names == {"default.repair_orders", "default.hard_hat"}
        elif coll["name"] == "Collection B":
            assert coll["nodeCount"] == 2
            assert len(coll["nodes"]) == 2
            node_names = {n["name"] for n in coll["nodes"]}
            assert node_names == {"default.dispatcher", "default.municipality_dim"}
        elif coll["name"] == "Collection C":
            assert coll["nodeCount"] == 1
            assert len(coll["nodes"]) == 1
            assert coll["nodes"][0]["name"] == "default.num_repair_orders"


@pytest.mark.asyncio
async def test_list_collections_without_nodes_field(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test listing collections without requesting the nodes field.
    This verifies that the dataloader is only used when nodes are requested.
    """
    # Create a collection with nodes
    response = await client_with_roads.post(
        "/collections/",
        json={
            "name": "Test Collection",
            "description": "A test collection",
        },
    )
    assert response.status_code == 201

    response = await client_with_roads.post(
        "/collections/Test%20Collection/nodes/",
        json=["default.repair_orders", "default.hard_hat"],
    )
    assert response.status_code == 204

    # Query without nodes field - should still work and show nodeCount
    query = """
    {
        listCollections {
            name
            nodeCount
            description
        }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    collections = data["data"]["listCollections"]
    assert len(collections) == 1
    assert collections[0]["name"] == "Test Collection"
    assert collections[0]["nodeCount"] == 2
    assert collections[0]["description"] == "A test collection"
    # nodes field should not be in response when not requested
    assert "nodes" not in collections[0]


@pytest.mark.asyncio
async def test_list_collections_created_by_user_info(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test that created_by user information is properly eager loaded.
    """
    # Create a collection
    response = await client_with_roads.post(
        "/collections/",
        json={
            "name": "User Test Collection",
            "description": "Testing user info",
        },
    )
    assert response.status_code == 201

    # Query with full user details
    query = """
    {
        listCollections {
            name
            createdBy {
                id
                username
                email
                name
                oauthProvider
                isAdmin
            }
        }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    collections = data["data"]["listCollections"]
    assert len(collections) == 1

    collection = collections[0]
    user = collection["createdBy"]
    assert user["username"] == "dj"
    assert user["email"] == "dj@datajunction.io"
    assert user["name"] == "DJ"
    assert user["oauthProvider"] == "BASIC"
    assert user["isAdmin"] is False
    assert user["id"] is not None


@pytest.mark.asyncio
async def test_list_collections_no_collections(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test listing collections when there are none.
    """
    # Query without creating any collections
    query = """
    {
        listCollections {
            name
        }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    collections = data["data"]["listCollections"]
    assert collections == []


@pytest.mark.asyncio
async def test_list_collections_with_various_node_types(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test listing collections that contain various node types.
    """
    # Create a collection with mixed node types
    response = await client_with_roads.post(
        "/collections/",
        json={
            "name": "Mixed Types",
            "description": "Collection with various node types",
        },
    )
    assert response.status_code == 201

    # Add different types of nodes
    response = await client_with_roads.post(
        "/collections/Mixed%20Types/nodes/",
        json=[
            "default.repair_orders",  # SOURCE
            "default.hard_hat",  # DIMENSION
            "default.num_repair_orders",  # METRIC
            "default.repair_orders_fact",  # TRANSFORM
        ],
    )
    assert response.status_code == 204

    # Query with node type information
    query = """
    {
        listCollections(limit: 1) {
            name
            nodeCount
            nodes {
                name
                type
                currentVersion
            }
        }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    collections = data["data"]["listCollections"]
    assert len(collections) == 1

    collection = collections[0]
    assert collection["name"] == "Mixed Types"
    assert collection["nodeCount"] == 4

    # Verify different node types are present
    nodes = collection["nodes"]
    node_types = {node["name"]: node["type"] for node in nodes}
    assert "SOURCE" in node_types.values()
    assert "DIMENSION" in node_types.values()
    assert "METRIC" in node_types.values()
    assert "TRANSFORM" in node_types.values()

    # All nodes should have a current version
    for node in nodes:
        assert node["currentVersion"] is not None
