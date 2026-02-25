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
    - Roles: bfs.role_fact -> role_1[role_a] -> role_2[role_b] -> role_3[role_c] -> role_4[role_d] -> role_5[role_e] (5-hop with roles)
    - MultiRole: bfs.multirole_fact -> multirole_dim[path_x] and -> multirole_dim[path_y] (multiple paths with roles)
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

    # ===== ROLE GRAPH (5-hop with roles) =====
    # Create sources for role chain
    for i in range(1, 6):
        response = await module__client.post(
            "/nodes/source/",
            json={
                "name": f"bfs.src_role_{i}",
                "description": f"Source {i} in role chain",
                "catalog": "bfs_catalog",
                "schema_": "bfs",
                "table": f"src_role_{i}",
                "columns": [{"name": f"id_{i}", "type": "int"}],
            },
        )
        assert response.status_code in (200, 201), (
            f"Failed to create bfs.src_role_{i}: {response.text}"
        )

    response = await module__client.post(
        "/nodes/source/",
        json={
            "name": "bfs.src_role_fact",
            "description": "Source for role fact",
            "catalog": "bfs_catalog",
            "schema_": "bfs",
            "table": "src_role_fact",
            "columns": [
                {"name": "fact_id", "type": "int"},
                {"name": "role_1_id", "type": "int"},
            ],
        },
    )
    assert response.status_code in (200, 201), (
        f"Failed to create bfs.src_role_fact: {response.text}"
    )

    # Create role dimensions
    for i in range(1, 6):
        response = await module__client.post(
            "/nodes/dimension/",
            json={
                "name": f"bfs.role_{i}",
                "description": f"Role dimension {i}",
                "query": f"SELECT id_{i} FROM bfs.src_role_{i}",
                "mode": "published",
                "primary_key": [f"id_{i}"],
            },
        )
        assert response.status_code in (200, 201), (
            f"Failed to create bfs.role_{i}: {response.text}"
        )

    # Create role fact
    response = await module__client.post(
        "/nodes/transform/",
        json={
            "name": "bfs.role_fact",
            "description": "Fact with role chain",
            "query": "SELECT fact_id, role_1_id FROM bfs.src_role_fact",
            "mode": "published",
        },
    )
    assert response.status_code in (200, 201), (
        f"Failed to create bfs.role_fact: {response.text}"
    )

    # Create role chain with TWO roles at each level
    # This creates 2^5 = 32 possible paths through the graph
    role_pairs = [
        ["role_a", "role_alt_a"],
        ["role_b", "role_alt_b"],
        ["role_c", "role_alt_c"],
        ["role_d", "role_alt_d"],
        ["role_e", "role_alt_e"],
    ]

    # First hop: fact -> role_1 (two different roles)
    for role_name in role_pairs[0]:
        response = await module__client.post(
            "/nodes/bfs.role_fact/link",
            json={
                "dimension_node": "bfs.role_1",
                "join_on": "bfs.role_fact.role_1_id = bfs.role_1.id_1",
                "join_type": "left",
                "join_cardinality": "many_to_one",
                "role": role_name,
            },
        )
        assert response.status_code in (200, 201), (
            f"Failed to create link bfs.role_fact->role_1[{role_name}]: {response.text}"
        )

    # Subsequent hops: role_i -> role_{i+1} (two roles each)
    for i in range(1, 5):
        for role_name in role_pairs[i]:
            response = await module__client.post(
                f"/nodes/bfs.role_{i}/link",
                json={
                    "dimension_node": f"bfs.role_{i + 1}",
                    "join_on": f"bfs.role_{i}.id_{i} = bfs.role_{i + 1}.id_{i + 1}",
                    "join_type": "left",
                    "join_cardinality": "one_to_one",
                    "role": role_name,
                },
            )
            assert response.status_code in (200, 201), (
                f"Failed to create link bfs.role_{i}->role_{i + 1}[{role_name}]: {response.text}"
            )

    # ===== MULTIROLE GRAPH (multiple paths with different roles) =====
    # Create multirole dimension and intermediates
    response = await module__client.post(
        "/nodes/source/",
        json={
            "name": "bfs.src_multirole_dim",
            "description": "Source for multirole dimension",
            "catalog": "bfs_catalog",
            "schema_": "bfs",
            "table": "src_multirole_dim",
            "columns": [{"name": "id", "type": "int"}],
        },
    )
    assert response.status_code in (200, 201), (
        f"Failed to create bfs.src_multirole_dim: {response.text}"
    )

    for path_name in ["path_x", "path_y"]:
        response = await module__client.post(
            "/nodes/source/",
            json={
                "name": f"bfs.src_{path_name}",
                "description": f"Source for {path_name}",
                "catalog": "bfs_catalog",
                "schema_": "bfs",
                "table": f"src_{path_name}",
                "columns": [{"name": "id", "type": "int"}],
            },
        )
        assert response.status_code in (200, 201), (
            f"Failed to create bfs.src_{path_name}: {response.text}"
        )

    response = await module__client.post(
        "/nodes/source/",
        json={
            "name": "bfs.src_multirole_fact",
            "description": "Source for multirole fact",
            "catalog": "bfs_catalog",
            "schema_": "bfs",
            "table": "src_multirole_fact",
            "columns": [
                {"name": "fact_id", "type": "int"},
                {"name": "path_x_id", "type": "int"},
                {"name": "path_y_id", "type": "int"},
            ],
        },
    )
    assert response.status_code in (200, 201), (
        f"Failed to create bfs.src_multirole_fact: {response.text}"
    )

    # Create dimensions
    response = await module__client.post(
        "/nodes/dimension/",
        json={
            "name": "bfs.multirole_dim",
            "description": "Dimension reachable via multiple role paths",
            "query": "SELECT id FROM bfs.src_multirole_dim",
            "mode": "published",
            "primary_key": ["id"],
        },
    )
    assert response.status_code in (200, 201), (
        f"Failed to create bfs.multirole_dim: {response.text}"
    )

    for path_name in ["path_x", "path_y"]:
        response = await module__client.post(
            "/nodes/dimension/",
            json={
                "name": f"bfs.{path_name}",
                "description": f"Intermediate dimension {path_name}",
                "query": f"SELECT id FROM bfs.src_{path_name}",
                "mode": "published",
                "primary_key": ["id"],
            },
        )
        assert response.status_code in (200, 201), (
            f"Failed to create bfs.{path_name}: {response.text}"
        )

    # Create multirole fact
    response = await module__client.post(
        "/nodes/transform/",
        json={
            "name": "bfs.multirole_fact",
            "description": "Fact with multiple role paths",
            "query": "SELECT fact_id, path_x_id, path_y_id FROM bfs.src_multirole_fact",
            "mode": "published",
        },
    )
    assert response.status_code in (200, 201), (
        f"Failed to create bfs.multirole_fact: {response.text}"
    )

    # Create two paths to multirole_dim with different roles
    # Path 1: fact -> path_x[via_x] -> multirole_dim
    response = await module__client.post(
        "/nodes/bfs.multirole_fact/link",
        json={
            "dimension_node": "bfs.path_x",
            "join_on": "bfs.multirole_fact.path_x_id = bfs.path_x.id",
            "join_type": "left",
            "join_cardinality": "many_to_one",
            "role": "via_x",
        },
    )
    assert response.status_code in (200, 201), (
        f"Failed to create link bfs.multirole_fact->path_x[via_x]: {response.text}"
    )

    response = await module__client.post(
        "/nodes/bfs.path_x/link",
        json={
            "dimension_node": "bfs.multirole_dim",
            "join_on": "bfs.path_x.id = bfs.multirole_dim.id",
            "join_type": "left",
            "join_cardinality": "one_to_one",
        },
    )
    assert response.status_code in (200, 201), (
        f"Failed to create link bfs.path_x->multirole_dim: {response.text}"
    )

    # Path 2: fact -> path_y[via_y] -> multirole_dim
    response = await module__client.post(
        "/nodes/bfs.multirole_fact/link",
        json={
            "dimension_node": "bfs.path_y",
            "join_on": "bfs.multirole_fact.path_y_id = bfs.path_y.id",
            "join_type": "left",
            "join_cardinality": "many_to_one",
            "role": "via_y",
        },
    )
    assert response.status_code in (200, 201), (
        f"Failed to create link bfs.multirole_fact->path_y[via_y]: {response.text}"
    )

    response = await module__client.post(
        "/nodes/bfs.path_y/link",
        json={
            "dimension_node": "bfs.multirole_dim",
            "join_on": "bfs.path_y.id = bfs.multirole_dim.id",
            "join_type": "left",
            "join_cardinality": "one_to_one",
        },
    )
    assert response.status_code in (200, 201), (
        f"Failed to create link bfs.path_y->multirole_dim: {response.text}"
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

    async def test_multi_hop_with_roles_5_depth(
        self,
        module__session,
        client_with_bfs_graph,
    ):
        """
        Test BFS correctly builds role paths through 5-hop chain with multiple roles at each level.

        Graph structure (2 roles at each level):
        bfs.role_fact -> role_1[role_a OR role_alt_a]
                      -> role_2[role_b OR role_alt_b]
                      -> role_3[role_c OR role_alt_c]
                      -> role_4[role_d OR role_alt_d]
                      -> role_5[role_e OR role_alt_e]

        This creates 2^5 = 32 possible paths. BFS should find all of them.
        """
        result = await module__session.execute(
            select(Node)
            .where(Node.name == "bfs.role_fact")
            .options(selectinload(Node.current)),
        )
        fact_node = result.scalar_one()
        source_rev_id = fact_node.current.id

        # Find all paths using BFS
        path_ids = await find_join_paths_batch(
            module__session,
            source_revision_ids={source_rev_id},
            target_dimension_names={"bfs.role_5"},
        )

        # Should find 32 paths (2^5 combinations)
        matching_paths = [
            (key, link_ids)
            for key, link_ids in path_ids.items()
            if key[1] == "bfs.role_5"
        ]
        expected_path_count = 2**5  # 32 paths
        assert len(matching_paths) == expected_path_count, (
            f"Expected {expected_path_count} paths, found {len(matching_paths)}"
        )

        # Collect all link IDs and load them
        all_link_ids = set()
        for _, link_ids in matching_paths:
            all_link_ids.update(link_ids)
        link_dict = await load_dimension_links_batch(module__session, set(all_link_ids))

        # Validate a sample of paths
        found_role_paths = set()
        for key, link_ids in matching_paths:
            _, _, role_path = key
            found_role_paths.add(role_path)

            # All paths should be 5-hop
            assert len(link_ids) == 5, (
                f"Expected 5-hop path for role_path '{role_path}', got {len(link_ids)}"
            )

            links = [link_dict[lid] for lid in link_ids]

            # Validate dimension sequence
            expected_chain = [
                "bfs.role_1",
                "bfs.role_2",
                "bfs.role_3",
                "bfs.role_4",
                "bfs.role_5",
            ]
            for i, link in enumerate(links):
                assert link.dimension.name == expected_chain[i], (
                    f"Path '{role_path}' hop {i}: expected {expected_chain[i]}, got {link.dimension.name}"
                )

            # Validate role_path format (should have 4 "->" separators for 5 roles)
            assert role_path.count("->") == 4, (
                f"Expected 4 '->' separators in role_path, got: '{role_path}'"
            )

        # Verify we have the expected specific paths
        assert found_role_paths == {
            "role_a->role_alt_b->role_alt_c->role_alt_d->role_alt_e",
            "role_a->role_alt_b->role_alt_c->role_alt_d->role_e",
            "role_a->role_alt_b->role_alt_c->role_d->role_alt_e",
            "role_a->role_alt_b->role_alt_c->role_d->role_e",
            "role_a->role_alt_b->role_c->role_alt_d->role_alt_e",
            "role_a->role_alt_b->role_c->role_alt_d->role_e",
            "role_a->role_alt_b->role_c->role_d->role_alt_e",
            "role_a->role_alt_b->role_c->role_d->role_e",
            "role_a->role_b->role_alt_c->role_alt_d->role_alt_e",
            "role_a->role_b->role_alt_c->role_alt_d->role_e",
            "role_a->role_b->role_alt_c->role_d->role_alt_e",
            "role_a->role_b->role_alt_c->role_d->role_e",
            "role_a->role_b->role_c->role_alt_d->role_alt_e",
            "role_a->role_b->role_c->role_alt_d->role_e",
            "role_a->role_b->role_c->role_d->role_alt_e",
            "role_a->role_b->role_c->role_d->role_e",
            "role_alt_a->role_alt_b->role_alt_c->role_alt_d->role_alt_e",
            "role_alt_a->role_alt_b->role_alt_c->role_alt_d->role_e",
            "role_alt_a->role_alt_b->role_alt_c->role_d->role_alt_e",
            "role_alt_a->role_alt_b->role_alt_c->role_d->role_e",
            "role_alt_a->role_alt_b->role_c->role_alt_d->role_alt_e",
            "role_alt_a->role_alt_b->role_c->role_alt_d->role_e",
            "role_alt_a->role_alt_b->role_c->role_d->role_alt_e",
            "role_alt_a->role_alt_b->role_c->role_d->role_e",
            "role_alt_a->role_b->role_alt_c->role_alt_d->role_alt_e",
            "role_alt_a->role_b->role_alt_c->role_alt_d->role_e",
            "role_alt_a->role_b->role_alt_c->role_d->role_alt_e",
            "role_alt_a->role_b->role_alt_c->role_d->role_e",
            "role_alt_a->role_b->role_c->role_alt_d->role_alt_e",
            "role_alt_a->role_b->role_c->role_alt_d->role_e",
            "role_alt_a->role_b->role_c->role_d->role_alt_e",
            "role_alt_a->role_b->role_c->role_d->role_e",
        }

    async def test_multiple_paths_different_roles(
        self,
        module__session,
        client_with_bfs_graph,
    ):
        """
        Test BFS finds multiple paths to same dimension through different roles.

        Graph structure:
        bfs.multirole_fact -> path_x[via_x] -> multirole_dim
        bfs.multirole_fact -> path_y[via_y] -> multirole_dim

        Should find two distinct 2-hop paths with different role_paths.
        """
        result = await module__session.execute(
            select(Node)
            .where(Node.name == "bfs.multirole_fact")
            .options(selectinload(Node.current)),
        )
        fact_node = result.scalar_one()
        source_rev_id = fact_node.current.id

        # Find all paths to multirole_dim
        path_ids = await find_join_paths_batch(
            module__session,
            source_revision_ids={source_rev_id},
            target_dimension_names={"bfs.multirole_dim"},
        )

        # Should find two paths with different roles
        matching_paths = [
            (key, link_ids)
            for key, link_ids in path_ids.items()
            if key[1] == "bfs.multirole_dim"
        ]
        assert len(matching_paths) == 2, (
            f"Expected 2 paths, found {len(matching_paths)}"
        )

        # Collect all link IDs and load them
        all_link_ids = set()
        for _, link_ids in matching_paths:
            all_link_ids.update(link_ids)
        link_dict = await load_dimension_links_batch(module__session, all_link_ids)

        # Verify both paths
        found_role_paths = set()
        for key, link_ids in matching_paths:
            _, _, role_path = key
            found_role_paths.add(role_path)

            # Each should be a 2-hop path
            assert len(link_ids) == 2, (
                f"Expected 2-hop path for role '{role_path}', got {len(link_ids)}"
            )

            links = [link_dict[lid] for lid in link_ids]
            # Last hop should always be multirole_dim
            assert links[1].dimension.name == "bfs.multirole_dim"

        # Should have found both role paths
        assert "via_x" in found_role_paths, "Expected to find role_path 'via_x'"
        assert "via_y" in found_role_paths, "Expected to find role_path 'via_y'"

    async def test_no_path_for_nonexistent_role(
        self,
        module__session,
        client_with_bfs_graph,
    ):
        """
        Test BFS behavior when requested role doesn't exist.

        Graph has:
        - bfs.multirole_fact -> path_x[via_x] -> multirole_dim
        - bfs.multirole_fact -> path_y[via_y] -> multirole_dim

        But we request a dimension that doesn't exist via any path.
        Should return empty results.
        """
        result = await module__session.execute(
            select(Node)
            .where(Node.name == "bfs.multirole_fact")
            .options(selectinload(Node.current)),
        )
        fact_node = result.scalar_one()
        source_rev_id = fact_node.current.id

        # Request a dimension that doesn't exist
        path_ids = await find_join_paths_batch(
            module__session,
            source_revision_ids={source_rev_id},
            target_dimension_names={"bfs.nonexistent_dim"},
        )

        # Should find no paths
        assert len(path_ids) == 0, f"Expected no paths, found {len(path_ids)}"
