"""
Tests for BFS-based join path finding in BuildV3.

These tests cover:
- Multi-hop dimension graphs (depth >= 5)
- Cycle prevention in dimension link graphs
- Batched query performance with large graphs
"""

import pytest
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from datajunction_server.construction.build_v3.loaders import (
    find_join_paths_batch,
    load_dimension_links_batch,
)
from datajunction_server.database.node import Node


@pytest.fixture(scope="module")
async def client_with_bfs_graph(module__client):
    """
    Create dimension graphs specifically for BFS testing.

    Graphs created:
    - Chain: bfs.chain_fact -> dim_1 -> dim_2 -> dim_3 -> dim_4 -> dim_5 (5-hop chain)
    - Cycle: bfs.cycle_fact -> cycle_a -> cycle_b -> cycle_c -> cycle_a (with cycle)
    - Star: bfs.star_fact -> 20 dimensions (batch performance)
    """
    # Create catalog and engine
    response = await module__client.post(
        "/catalogs/",
        json={"name": "bfs_catalog"},
    )
    assert response.status_code in (200, 201), (
        f"Failed to create catalog: {response.text}"
    )

    # Create engine globally first
    response = await module__client.post(
        "/engines/",
        json={
            "name": "bfs_engine",
            "version": "1.0",
            "dialect": "spark",
        },
    )
    assert response.status_code in (200, 201), (
        f"Failed to create engine: {response.text}"
    )

    # Link engine to catalog
    response = await module__client.post(
        "/catalogs/bfs_catalog/engines/",
        json=[
            {
                "name": "bfs_engine",
                "version": "1.0",
            },
        ],
    )
    assert response.status_code in (200, 201), (
        f"Failed to link engine to catalog: {response.text}"
    )

    # Create namespace
    response = await module__client.post(
        "/namespaces/bfs",
    )
    assert response.status_code in (200, 201), (
        f"Failed to create namespace: {response.text}"
    )

    # ===== CHAIN GRAPH (5-hop depth test) =====
    # Create sources for chain
    for i in range(1, 6):
        response = await module__client.post(
            "/nodes/source/",
            json={
                "name": f"bfs.src_chain_{i}",
                "description": f"Source {i} in chain",
                "catalog": "bfs_catalog",
                "schema_": "bfs",
                "table": f"src_chain_{i}",
                "columns": [{"name": f"id_{i}", "type": "int"}],
            },
        )
        assert response.status_code in (200, 201), (
            f"Failed to create bfs.src_chain_{i}: {response.text}"
        )

    response = await module__client.post(
        "/nodes/source/",
        json={
            "name": "bfs.src_chain_fact",
            "description": "Source for chain fact",
            "catalog": "bfs_catalog",
            "schema_": "bfs",
            "table": "src_chain_fact",
            "columns": [
                {"name": "fact_id", "type": "int"},
                {"name": "chain_1_id", "type": "int"},
            ],
        },
    )
    assert response.status_code in (200, 201), (
        f"Failed to create bfs.src_chain_fact: {response.text}"
    )

    # Create chain dimensions
    for i in range(1, 6):
        response = await module__client.post(
            "/nodes/dimension/",
            json={
                "name": f"bfs.chain_{i}",
                "description": f"Chain dimension {i}",
                "query": f"SELECT id_{i} FROM bfs.src_chain_{i}",
                "mode": "published",
                "primary_key": [f"id_{i}"],
            },
        )
        assert response.status_code in (200, 201), (
            f"Failed to create bfs.chain_{i}: {response.text}"
        )

    # Create chain fact
    response = await module__client.post(
        "/nodes/transform/",
        json={
            "name": "bfs.chain_fact",
            "description": "Fact at start of chain",
            "query": "SELECT fact_id, chain_1_id FROM bfs.src_chain_fact",
            "mode": "published",
        },
    )
    assert response.status_code in (200, 201), (
        f"Failed to create bfs.chain_fact: {response.text}"
    )

    # Create chain links
    response = await module__client.post(
        "/nodes/bfs.chain_fact/link",
        json={
            "dimension_node": "bfs.chain_1",
            "join_on": "bfs.chain_fact.chain_1_id = bfs.chain_1.id_1",
            "join_type": "left",
            "join_cardinality": "many_to_one",
        },
    )
    assert response.status_code in (200, 201), (
        f"Failed to create link bfs.chain_fact->chain_1: {response.text}"
    )

    for i in range(1, 5):
        response = await module__client.post(
            f"/nodes/bfs.chain_{i}/link",
            json={
                "dimension_node": f"bfs.chain_{i + 1}",
                "join_on": f"bfs.chain_{i}.id_{i} = bfs.chain_{i + 1}.id_{i + 1}",
                "join_type": "left",
                "join_cardinality": "one_to_one",
            },
        )
        assert response.status_code in (200, 201), (
            f"Failed to create link bfs.chain_{i}->chain_{i + 1}: {response.text}"
        )

    # ===== CYCLE GRAPH (cycle prevention test) =====
    # Create sources
    for dim_name in ["cycle_a", "cycle_b", "cycle_c", "cycle_target"]:
        response = await module__client.post(
            "/nodes/source/",
            json={
                "name": f"bfs.src_{dim_name}",
                "description": f"Source for {dim_name}",
                "catalog": "bfs_catalog",
                "schema_": "bfs",
                "table": f"src_{dim_name}",
                "columns": [{"name": "id", "type": "int"}],
            },
        )
        assert response.status_code in (200, 201), (
            f"Failed to create bfs.src_{dim_name}: {response.text}"
        )

    response = await module__client.post(
        "/nodes/source/",
        json={
            "name": "bfs.src_cycle_fact",
            "description": "Source for cycle fact",
            "catalog": "bfs_catalog",
            "schema_": "bfs",
            "table": "src_cycle_fact",
            "columns": [
                {"name": "fact_id", "type": "int"},
                {"name": "dim_a_id", "type": "int"},
            ],
        },
    )
    assert response.status_code in (200, 201), (
        f"Failed to create bfs.src_cycle_fact: {response.text}"
    )

    # Create cycle dimensions
    for dim_name in ["cycle_a", "cycle_b", "cycle_c", "cycle_target"]:
        response = await module__client.post(
            "/nodes/dimension/",
            json={
                "name": f"bfs.{dim_name}",
                "description": f"Dimension {dim_name}",
                "query": f"SELECT id FROM bfs.src_{dim_name}",
                "mode": "published",
                "primary_key": ["id"],
            },
        )
        assert response.status_code in (200, 201), (
            f"Failed to create bfs.{dim_name}: {response.text}"
        )

    # Create cycle fact
    response = await module__client.post(
        "/nodes/transform/",
        json={
            "name": "bfs.cycle_fact",
            "description": "Fact with cycle",
            "query": "SELECT fact_id, dim_a_id FROM bfs.src_cycle_fact",
            "mode": "published",
        },
    )
    assert response.status_code in (200, 201), (
        f"Failed to create bfs.cycle_fact: {response.text}"
    )

    # Create cycle links: fact -> a -> b -> c -> a (cycle), a -> target
    response = await module__client.post(
        "/nodes/bfs.cycle_fact/link",
        json={
            "dimension_node": "bfs.cycle_a",
            "join_on": "bfs.cycle_fact.dim_a_id = bfs.cycle_a.id",
            "join_type": "left",
            "join_cardinality": "many_to_one",
        },
    )
    assert response.status_code in (200, 201), (
        f"Failed to create link bfs.cycle_fact->cycle_a: {response.text}"
    )

    response = await module__client.post(
        "/nodes/bfs.cycle_a/link",
        json={
            "dimension_node": "bfs.cycle_b",
            "join_on": "bfs.cycle_a.id = bfs.cycle_b.id",
            "join_type": "left",
            "join_cardinality": "one_to_one",
        },
    )
    assert response.status_code in (200, 201), (
        f"Failed to create link bfs.cycle_a->cycle_b: {response.text}"
    )

    response = await module__client.post(
        "/nodes/bfs.cycle_b/link",
        json={
            "dimension_node": "bfs.cycle_c",
            "join_on": "bfs.cycle_b.id = bfs.cycle_c.id",
            "join_type": "left",
            "join_cardinality": "one_to_one",
        },
    )
    assert response.status_code in (200, 201), (
        f"Failed to create link bfs.cycle_b->cycle_c: {response.text}"
    )

    # Create the cycle: c -> a
    response = await module__client.post(
        "/nodes/bfs.cycle_c/link",
        json={
            "dimension_node": "bfs.cycle_a",
            "join_on": "bfs.cycle_c.id = bfs.cycle_a.id",
            "join_type": "left",
            "join_cardinality": "one_to_one",
        },
    )
    assert response.status_code in (200, 201), (
        f"Failed to create link bfs.cycle_c->cycle_a (cycle): {response.text}"
    )

    # Also add path to target from a
    response = await module__client.post(
        "/nodes/bfs.cycle_a/link",
        json={
            "dimension_node": "bfs.cycle_target",
            "join_on": "bfs.cycle_a.id = bfs.cycle_target.id",
            "join_type": "left",
            "join_cardinality": "one_to_one",
        },
    )
    assert response.status_code in (200, 201), (
        f"Failed to create link bfs.cycle_a->cycle_target: {response.text}"
    )

    # ===== STAR GRAPH (batch performance test) =====
    num_dims = 20

    # Create sources for star dimensions
    for i in range(num_dims):
        response = await module__client.post(
            "/nodes/source/",
            json={
                "name": f"bfs.src_star_{i}",
                "description": f"Star source {i}",
                "catalog": "bfs_catalog",
                "schema_": "bfs",
                "table": f"src_star_{i}",
                "columns": [{"name": "id", "type": "int"}],
            },
        )
        assert response.status_code in (200, 201), (
            f"Failed to create bfs.src_star_{i}: {response.text}"
        )

    # Create star fact source
    star_columns = [{"name": "fact_id", "type": "int"}]
    for i in range(num_dims):
        star_columns.append({"name": f"star_{i}_id", "type": "int"})

    response = await module__client.post(
        "/nodes/source/",
        json={
            "name": "bfs.src_star_fact",
            "description": "Star fact source",
            "catalog": "bfs_catalog",
            "schema_": "bfs",
            "table": "src_star_fact",
            "columns": star_columns,
        },
    )
    assert response.status_code in (200, 201), (
        f"Failed to create bfs.src_star_fact: {response.text}"
    )

    # Create star dimensions
    for i in range(num_dims):
        response = await module__client.post(
            "/nodes/dimension/",
            json={
                "name": f"bfs.star_{i}",
                "description": f"Star dimension {i}",
                "query": f"SELECT id FROM bfs.src_star_{i}",
                "mode": "published",
                "primary_key": ["id"],
            },
        )
        assert response.status_code in (200, 201), (
            f"Failed to create bfs.star_{i}: {response.text}"
        )

    # Create star fact
    star_select = ["fact_id"] + [f"star_{i}_id" for i in range(num_dims)]
    response = await module__client.post(
        "/nodes/transform/",
        json={
            "name": "bfs.star_fact",
            "description": "Star schema fact",
            "query": f"SELECT {', '.join(star_select)} FROM bfs.src_star_fact",
            "mode": "published",
        },
    )
    assert response.status_code in (200, 201), (
        f"Failed to create bfs.star_fact: {response.text}"
    )

    # Create star links
    for i in range(num_dims):
        response = await module__client.post(
            "/nodes/bfs.star_fact/link",
            json={
                "dimension_node": f"bfs.star_{i}",
                "join_on": f"bfs.star_fact.star_{i}_id = bfs.star_{i}.id",
                "join_type": "left",
                "join_cardinality": "many_to_one",
            },
        )
        assert response.status_code in (200, 201), (
            f"Failed to create link bfs.star_fact->star_{i}: {response.text}"
        )

    return module__client


@pytest.mark.asyncio
class TestBFSJoinPathFinding:
    """Tests for BFS join path finding with complex dimension graphs."""

    async def test_multi_hop_path_depth_5(self, module__session, client_with_bfs_graph):
        """
        Test BFS finds a 5-hop path through multiple intermediate dimensions.

        Graph structure:
        bfs.chain_fact -> chain_1 -> chain_2 -> chain_3 -> chain_4 -> chain_5

        This tests that BFS correctly explores deep paths.
        """
        # Get the fact node with current revision eagerly loaded
        result = await module__session.execute(
            select(Node)
            .where(Node.name == "bfs.chain_fact")
            .options(selectinload(Node.current)),
        )
        fact_node = result.scalar_one()
        source_rev_id = fact_node.current.id

        # Find paths using BFS (returns link IDs)
        path_ids = await find_join_paths_batch(
            module__session,
            source_revision_ids={source_rev_id},
            target_dimension_names={"bfs.chain_5"},
        )

        # Should find the 5-hop path
        matching_paths = [
            (key, link_ids)
            for key, link_ids in path_ids.items()
            if key[1] == "bfs.chain_5"
        ]
        assert len(matching_paths) == 1, f"Expected 1 path, found {len(matching_paths)}"

        key, link_ids = matching_paths[0]
        _, _, role_path = key

        # Verify it's a 5-hop path with correct dimensions
        assert len(link_ids) == 5, f"Expected 5-hop path, got {len(link_ids)}"
        assert role_path == "", f"Expected empty role, got '{role_path}'"

        # Load the actual DimensionLink objects
        link_dict = await load_dimension_links_batch(module__session, set(link_ids))
        links = [link_dict[lid] for lid in link_ids]

        # Validate each hop in the chain
        expected_chain = [
            "bfs.chain_1",
            "bfs.chain_2",
            "bfs.chain_3",
            "bfs.chain_4",
            "bfs.chain_5",
        ]
        for i, link in enumerate(links):
            assert link.dimension.name == expected_chain[i], (
                f"Hop {i}: expected {expected_chain[i]}, got {link.dimension.name}"
            )

    async def test_cycle_prevention(self, module__session, client_with_bfs_graph):
        """
        Test BFS correctly handles cycles in the dimension graph.

        Graph structure:
        bfs.cycle_fact -> cycle_a -> cycle_b -> cycle_c -> cycle_a (cycle!)
        Also: cycle_a -> cycle_target

        BFS should still find the path to cycle_target without infinite loops.
        """
        result = await module__session.execute(
            select(Node)
            .where(Node.name == "bfs.cycle_fact")
            .options(selectinload(Node.current)),
        )
        fact_node = result.scalar_one()
        source_rev_id = fact_node.current.id

        path_ids = await find_join_paths_batch(
            module__session,
            source_revision_ids={source_rev_id},
            target_dimension_names={"bfs.cycle_target"},
        )

        # Should find path to target despite the cycle
        matching_paths = [
            (key, link_ids)
            for key, link_ids in path_ids.items()
            if key[1] == "bfs.cycle_target"
        ]
        assert len(matching_paths) == 1, f"Expected 1 path, found {len(matching_paths)}"

        key, link_ids = matching_paths[0]

        # Should be 2-hop: fact -> cycle_a -> cycle_target
        assert len(link_ids) == 2, f"Expected 2-hop path, got {len(link_ids)}"

        # Load the actual DimensionLink objects
        link_dict = await load_dimension_links_batch(module__session, set(link_ids))
        links = [link_dict[lid] for lid in link_ids]

        # Validate the path goes through cycle_a (not through the cycle)
        assert links[0].dimension.name == "bfs.cycle_a", (
            f"First hop: expected bfs.cycle_a, got {links[0].dimension.name}"
        )
        assert links[1].dimension.name == "bfs.cycle_target", (
            f"Second hop: expected bfs.cycle_target, got {links[1].dimension.name}"
        )

    async def test_batch_query_performance(
        self,
        module__session,
        client_with_bfs_graph,
    ):
        """
        Test that BFS uses batched queries efficiently.

        Star schema: bfs.star_fact links to 20 dimensions directly.
        BFS should find all 20 paths with minimal queries.
        """
        result = await module__session.execute(
            select(Node)
            .where(Node.name == "bfs.star_fact")
            .options(selectinload(Node.current)),
        )
        fact_node = result.scalar_one()
        source_rev_id = fact_node.current.id

        # Request all 20 dimensions at once
        target_dims = {f"bfs.star_{i}" for i in range(20)}

        path_ids = await find_join_paths_batch(
            module__session,
            source_revision_ids={source_rev_id},
            target_dimension_names=target_dims,
        )

        # Should find all 20 dimensions
        found_targets = {key[1] for key in path_ids.keys()}
        assert len(found_targets) == 20, (
            f"Expected to find 20 dimensions, found {len(found_targets)}"
        )

        # Collect all link IDs and batch load them
        all_link_ids = set()
        for link_ids in path_ids.values():
            all_link_ids.update(link_ids)
        link_dict = await load_dimension_links_batch(module__session, all_link_ids)

        # All should be 1-hop paths (direct links) with correct dimension names
        for i in range(20):
            expected_dim = f"bfs.star_{i}"
            matching = [
                (key, link_ids)
                for key, link_ids in path_ids.items()
                if key[1] == expected_dim
            ]
            assert len(matching) == 1, f"Expected to find {expected_dim}"

            key, link_ids = matching[0]
            assert len(link_ids) == 1, (
                f"Expected 1-hop path for {expected_dim}, got {len(link_ids)}"
            )

            links = [link_dict[lid] for lid in link_ids]
            assert links[0].dimension.name == expected_dim, (
                f"Expected dimension {expected_dim}, got {links[0].dimension.name}"
            )
