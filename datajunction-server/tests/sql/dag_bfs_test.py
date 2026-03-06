"""
Tests for BFS-based get_dimensions_dag in datajunction_server.sql.dag.

These tests verify correctness of the BFS replacement for the recursive CTE
approach, covering chain graphs, star graphs, cycle prevention, and role paths.
"""

import time

import pytest
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from datajunction_server.database.node import Node
from datajunction_server.sql.dag import get_dimensions_dag

# Re-use the shared BFS graph fixture from the join path tests
from tests.construction.build_v3.join_path_bfs_test import client_with_bfs_graph  # noqa: F401


@pytest.mark.asyncio
class TestGetDimensionsDagBFS:
    """Tests for BFS-based get_dimensions_dag on the BFS graph fixtures."""

    async def test_chain_graph_returns_all_dimensions(
        self,
        module__session,
        client_with_bfs_graph,  # noqa: F811
    ):
        """
        get_dimensions_dag on bfs.chain_fact should return dimension attributes
        from all 5 chain dimensions (chain_1 through chain_5).
        """
        result = await module__session.execute(
            select(Node)
            .where(Node.name == "bfs.chain_fact")
            .options(selectinload(Node.current)),
        )
        fact_node = result.scalar_one()

        dims = await get_dimensions_dag(module__session, fact_node.current)

        dim_node_names = {d.node_name for d in dims}
        for i in range(1, 6):
            assert f"bfs.chain_{i}" in dim_node_names, (
                f"Expected bfs.chain_{i} in dimensions, got {dim_node_names}"
            )

    async def test_chain_graph_path_depth(
        self,
        module__session,
        client_with_bfs_graph,  # noqa: F811
    ):
        """
        The path for chain_5 should reflect the 5-hop traversal.
        chain_fact -> chain_1 -> chain_2 -> chain_3 -> chain_4 -> chain_5
        path for chain_5 attributes should have 5 nodes: fact + 4 intermediate dims.
        """
        result = await module__session.execute(
            select(Node)
            .where(Node.name == "bfs.chain_fact")
            .options(selectinload(Node.current)),
        )
        fact_node = result.scalar_one()

        dims = await get_dimensions_dag(module__session, fact_node.current)

        chain5_dims = [d for d in dims if d.node_name == "bfs.chain_5"]
        assert chain5_dims, "Expected at least one attribute from bfs.chain_5"

        # path should be [chain_fact, chain_1, chain_2, chain_3, chain_4]
        # (path up to but not including chain_5)
        path = chain5_dims[0].path
        assert len(path) == 5, f"Expected path length 5 for chain_5, got {path}"
        assert path[0] == "bfs.chain_fact"
        assert path[-1] == "bfs.chain_4"

    async def test_star_graph_returns_all_dimensions(
        self,
        module__session,
        client_with_bfs_graph,  # noqa: F811
    ):
        """
        get_dimensions_dag on bfs.star_fact should return attributes from all
        20 star dimensions.
        """
        result = await module__session.execute(
            select(Node)
            .where(Node.name == "bfs.star_fact")
            .options(selectinload(Node.current)),
        )
        fact_node = result.scalar_one()

        dims = await get_dimensions_dag(module__session, fact_node.current)

        dim_node_names = {d.node_name for d in dims}
        for i in range(20):
            assert f"bfs.star_{i}" in dim_node_names, (
                f"Expected bfs.star_{i} in dimensions, got {dim_node_names}"
            )

    async def test_cycle_graph_no_infinite_loop(
        self,
        module__session,
        client_with_bfs_graph,  # noqa: F811
    ):
        """
        get_dimensions_dag on bfs.cycle_fact should terminate (no infinite loop)
        and return cycle_target among the discovered dimensions.
        """
        result = await module__session.execute(
            select(Node)
            .where(Node.name == "bfs.cycle_fact")
            .options(selectinload(Node.current)),
        )
        fact_node = result.scalar_one()

        start = time.perf_counter()
        dims = await get_dimensions_dag(module__session, fact_node.current)
        elapsed = time.perf_counter() - start

        assert elapsed < 10, f"Cycle graph took too long: {elapsed:.2f}s"

        dim_node_names = {d.node_name for d in dims}
        assert "bfs.cycle_target" in dim_node_names, (
            f"Expected bfs.cycle_target in dimensions, got {dim_node_names}"
        )

    async def test_role_graph_role_suffix(
        self,
        module__session,
        client_with_bfs_graph,  # noqa: F811
    ):
        """
        Role paths should be reflected in DimensionAttributeOutput.name as [role_path] suffix.
        bfs.role_fact -> role_1[role_a or role_alt_a] -> ... -> role_5[role_e or role_alt_e]
        """
        result = await module__session.execute(
            select(Node)
            .where(Node.name == "bfs.role_fact")
            .options(selectinload(Node.current)),
        )
        fact_node = result.scalar_one()

        dims = await get_dimensions_dag(module__session, fact_node.current)

        # role_5 attributes should have a role suffix in their name
        role5_dims = [d for d in dims if d.node_name == "bfs.role_5"]
        assert role5_dims, "Expected dimension attributes from bfs.role_5"

        for d in role5_dims:
            assert "[" in d.name and "]" in d.name, (
                f"Expected role suffix in name '{d.name}'"
            )
            # Role path should contain "->" separators (5 hops = 4 separators)
            role_part = d.name[d.name.index("[") + 1 : d.name.rindex("]")]
            assert role_part.count("->") == 4, (
                f"Expected 4 '->' in role path, got '{role_part}'"
            )

    async def test_with_attributes_false_returns_nodes(
        self,
        module__session,
        client_with_bfs_graph,  # noqa: F811
    ):
        """
        with_attributes=False should return Node objects instead of DimensionAttributeOutput.
        """
        result = await module__session.execute(
            select(Node)
            .where(Node.name == "bfs.chain_fact")
            .options(selectinload(Node.current)),
        )
        fact_node = result.scalar_one()

        nodes = await get_dimensions_dag(
            module__session,
            fact_node.current,
            with_attributes=False,
        )

        assert all(isinstance(n, Node) for n in nodes), (
            "Expected all results to be Node objects when with_attributes=False"
        )
        node_names = {n.name for n in nodes}
        for i in range(1, 6):
            assert f"bfs.chain_{i}" in node_names

    async def test_star_graph_timing(
        self,
        module__session,
        client_with_bfs_graph,  # noqa: F811
    ):
        """
        Star graph (20 dims) should complete in a reasonable time.
        """
        result = await module__session.execute(
            select(Node)
            .where(Node.name == "bfs.star_fact")
            .options(selectinload(Node.current)),
        )
        fact_node = result.scalar_one()

        start = time.perf_counter()
        dims = await get_dimensions_dag(module__session, fact_node.current)
        elapsed = time.perf_counter() - start

        assert elapsed < 10, f"Star graph took too long: {elapsed:.2f}s"
        assert len({d.node_name for d in dims}) == 20

    async def test_chain_graph_timing(
        self,
        module__session,
        client_with_bfs_graph,  # noqa: F811
    ):
        """
        5-hop chain graph should complete in a reasonable time.
        """
        result = await module__session.execute(
            select(Node)
            .where(Node.name == "bfs.chain_fact")
            .options(selectinload(Node.current)),
        )
        fact_node = result.scalar_one()

        start = time.perf_counter()
        dims = await get_dimensions_dag(module__session, fact_node.current)
        elapsed = time.perf_counter() - start

        assert elapsed < 10, f"Chain graph took too long: {elapsed:.2f}s"
        assert len({d.node_name for d in dims}) == 5
