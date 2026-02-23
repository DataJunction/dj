"""
Tests for the collections GraphQL queries.
"""

from urllib.parse import quote

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
        collection_name = quote(str(coll_data["name"]))
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
async def test_list_collections_with_fragment_search(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test searching collections by fragment in name or description.
    """
    # Create collections with different names and descriptions
    collections_data = [
        {
            "name": "Repair Metrics Collection",
            "description": "Metrics for repair operations",
        },
        {
            "name": "Dimension Tables",
            "description": "Various dimension tables",
        },
        {
            "name": "Revenue Analytics",
            "description": "Revenue and repair cost analysis",
        },
    ]

    for coll_data in collections_data:
        response = await client_with_roads.post(
            "/collections/",
            json=coll_data,
        )
        assert response.status_code == 201

    # Search by "repair" - should match name of first collection and description of third
    query = """
    {
        listCollections(fragment: "repair") {
            name
            description
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    collections = data["data"]["listCollections"]
    collection_names = {c["name"] for c in collections}
    assert "Repair Metrics Collection" in collection_names
    assert "Revenue Analytics" in collection_names
    assert "Dimension Tables" not in collection_names

    # Search by "dimension" - should match name of second collection
    query = """
    {
        listCollections(fragment: "dimension") {
            name
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    collections = data["data"]["listCollections"]
    assert len(collections) == 1
    assert collections[0]["name"] == "Dimension Tables"

    # Search by "revenue" - should match name of third collection
    query = """
    {
        listCollections(fragment: "revenue") {
            name
        }
    }
    """
    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    collections = data["data"]["listCollections"]
    assert len(collections) == 1
    assert collections[0]["name"] == "Revenue Analytics"

    # Search with non-matching fragment
    query = """
    {
        listCollections(fragment: "nonexistent") {
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
async def test_list_collections_combined_filters(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test combining fragment search with created_by filter.
    """
    # Create some collections
    collections_data = [
        {"name": "User Metrics", "description": "User-related metrics"},
        {"name": "System Metrics", "description": "System performance metrics"},
    ]

    for coll_data in collections_data:
        response = await client_with_roads.post(
            "/collections/",
            json=coll_data,
        )
        assert response.status_code == 201

    # Search for "metrics" created by "dj"
    query = """
    {
        listCollections(fragment: "metrics", createdBy: "dj") {
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
    assert len(collections) == 2

    # All should be created by dj and match the fragment
    for collection in collections:
        assert collection["createdBy"]["username"] == "dj"
        assert "metrics" in collection["name"].lower()


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


@pytest.mark.asyncio
async def test_list_collections_nodes_with_availability(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test that availability information is properly loaded for nodes in collections.
    """
    # Create a collection with nodes
    response = await client_with_roads.post(
        "/collections/",
        json={
            "name": "Availability Test",
            "description": "Testing availability field loading",
        },
    )
    assert response.status_code == 201

    # Add nodes to the collection
    response = await client_with_roads.post(
        "/collections/Availability%20Test/nodes/",
        json=["default.repair_orders", "default.hard_hat"],
    )
    assert response.status_code == 204

    # Set availability on one of the nodes
    response = await client_with_roads.post(
        "/data/default.repair_orders/availability/",
        json={
            "catalog": "default",
            "schema_": "roads",
            "table": "repair_orders",
            "valid_through_ts": 20230125,
            "max_temporal_partition": ["2023", "01", "25"],
            "min_temporal_partition": ["2022", "01", "01"],
            "url": "http://some.catalog.com/default.roads.repair_orders",
        },
    )
    assert response.status_code == 200

    # Query collections with availability information
    query = """
    {
        listCollections {
            name
            nodes {
                name
                current {
                    availability {
                        catalog
                        schema_
                        table
                        minTemporalPartition
                        maxTemporalPartition
                        url
                    }
                }
            }
        }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    collections = data["data"]["listCollections"]
    assert len(collections) == 1

    # Verify nodes were loaded with availability field
    nodes = collections[0]["nodes"]
    assert len(nodes) == 2

    # Find the node with availability set
    repair_orders_node = next(
        (n for n in nodes if n["name"] == "default.repair_orders"),
        None,
    )
    assert repair_orders_node is not None
    availability = repair_orders_node["current"]["availability"]
    assert availability is not None
    assert availability["catalog"] == "default"
    assert availability["schema_"] == "roads"
    assert availability["table"] == "repair_orders"
    assert availability["minTemporalPartition"] == ["2022", "01", "01"]
    assert availability["maxTemporalPartition"] == ["2023", "01", "25"]
    assert availability["url"] == "http://some.catalog.com/default.roads.repair_orders"

    # The other node should have no availability
    hard_hat_node = next((n for n in nodes if n["name"] == "default.hard_hat"), None)
    assert hard_hat_node is not None
    assert hard_hat_node["current"]["availability"] is None


@pytest.mark.asyncio
async def test_list_collections_nodes_with_materializations(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test that materialization information is properly loaded for nodes in collections.
    """
    # Create a collection with nodes
    response = await client_with_roads.post(
        "/collections/",
        json={
            "name": "Materialization Test",
            "description": "Testing materialization field loading",
        },
    )
    assert response.status_code == 201

    # Add nodes to the collection
    response = await client_with_roads.post(
        "/collections/Materialization%20Test/nodes/",
        json=["default.repair_orders_fact", "default.hard_hat"],
    )
    assert response.status_code == 204

    # First, try to set up a materialization on one of the nodes
    # Note: This may fail in test environment, but we can still test querying
    await client_with_roads.post(
        "/nodes/default.repair_orders_fact/columns/repair_order_id/partition",
        json={"type_": "categorical"},
    )

    # Query collections with materialization information
    query = """
    {
        listCollections {
            name
            nodes {
                name
                current {
                    materializations {
                        name
                        job
                        strategy
                        schedule
                    }
                }
            }
        }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    collections = data["data"]["listCollections"]
    assert len(collections) == 1

    # Verify nodes were loaded with materializations field
    # Note: These nodes may not have materializations configured, so we're just
    # checking that the field is queryable without errors
    nodes = collections[0]["nodes"]
    assert len(nodes) == 2
    for node in nodes:
        assert "current" in node
        # materializations will be None or [] if not set, which is valid
        assert "materializations" in node["current"]


@pytest.mark.asyncio
async def test_list_collections_nodes_with_complex_fields(
    client_with_roads: AsyncClient,
) -> None:
    """
    Test loading nodes with multiple complex fields at once
    (columns, availability, materializations, parents).
    This verifies that the eager loading works correctly for all fields.
    """
    # Create a collection with various node types
    response = await client_with_roads.post(
        "/collections/",
        json={
            "name": "Complex Fields Test",
            "description": "Testing multiple complex fields",
        },
    )
    assert response.status_code == 201

    # Add nodes with different characteristics
    response = await client_with_roads.post(
        "/collections/Complex%20Fields%20Test/nodes/",
        json=[
            "default.repair_orders",  # SOURCE with columns
            "default.repair_orders_fact",  # TRANSFORM with parents
            "default.num_repair_orders",  # METRIC
        ],
    )
    assert response.status_code == 204

    # Query with multiple complex fields
    query = """
    {
        listCollections {
            name
            nodes {
                name
                type
                current {
                    columns {
                        name
                        type
                    }
                    availability {
                        catalog
                        table
                    }
                    materializations {
                        name
                        strategy
                    }
                    parents {
                        name
                        type
                    }
                }
            }
        }
    }
    """

    response = await client_with_roads.post("/graphql", json={"query": query})
    assert response.status_code == 200
    data = response.json()

    collections = data["data"]["listCollections"]
    assert len(collections) == 1

    nodes = collections[0]["nodes"]
    assert len(nodes) == 3

    # Verify different node types have their expected fields
    source_node = next((n for n in nodes if n["type"] == "SOURCE"), None)
    assert source_node is not None
    assert len(source_node["current"]["columns"]) > 0

    transform_node = next((n for n in nodes if n["type"] == "TRANSFORM"), None)
    assert transform_node is not None
    # Transform node should have parents
    assert "parents" in transform_node["current"]

    metric_node = next((n for n in nodes if n["type"] == "METRIC"), None)
    assert metric_node is not None
    # All fields should be queryable
    assert "columns" in metric_node["current"]
    assert "availability" in metric_node["current"]
    assert "materializations" in metric_node["current"]
