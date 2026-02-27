"""Tests for skills API endpoints."""

import pytest
from httpx import AsyncClient


class TestSkillsAPI:
    """Tests for /skills/* endpoints.

    Note: Skills endpoints are public (no authentication required)
    since they're documentation/usage guides.
    """

    @pytest.mark.asyncio
    async def test_get_core_skill(self, client_with_roads: AsyncClient):
        """Test GET /skills/dj-core endpoint (public, no auth required)."""
        response = await client_with_roads.get("/skills/dj-core")

        assert response.status_code == 200
        skill = response.json()

        # Verify structure
        assert skill["name"] == "datajunction-core"
        assert "version" in skill
        assert "description" in skill
        assert "keywords" in skill
        assert "instructions" in skill
        assert "metadata" in skill

        # Verify content
        assert isinstance(skill["keywords"], list)
        assert len(skill["keywords"]) > 0
        assert "DataJunction" in skill["keywords"]
        assert isinstance(skill["instructions"], str)
        assert len(skill["instructions"]) > 100  # Should have substantial content

        # Verify metadata
        assert skill["metadata"]["provider"] == "default"
        assert "dj_version" in skill["metadata"]
        assert "generated_at" in skill["metadata"]

    @pytest.mark.asyncio
    async def test_get_builder_skill(self, client_with_roads: AsyncClient):
        """Test GET /skills/dj-builder endpoint."""
        response = await client_with_roads.get("/skills/dj-builder")

        assert response.status_code == 200
        skill = response.json()

        assert skill["name"] == "datajunction-builder"
        assert "create metric" in skill["keywords"]
        assert "dimension link" in skill["keywords"]
        assert len(skill["instructions"]) > 100

    @pytest.mark.asyncio
    async def test_get_consumer_skill(self, client_with_roads: AsyncClient):
        """Test GET /skills/dj-consumer endpoint."""
        response = await client_with_roads.get("/skills/dj-consumer")

        assert response.status_code == 200
        skill = response.json()

        assert skill["name"] == "datajunction-consumer"
        assert "query metric" in skill["keywords"]
        assert "SQL generation" in skill["keywords"]
        assert len(skill["instructions"]) > 100

    @pytest.mark.asyncio
    async def test_get_namespace_skill_not_implemented(
        self,
        client_with_roads: AsyncClient,
    ):
        """Test GET /skills/namespaces/{namespace} - Phase 3 feature."""
        response = await client_with_roads.get("/skills/namespaces/default")

        # Phase 3 not yet implemented
        assert response.status_code == 404
        assert (
            "Auto-generation will be available in Phase 3" in response.json()["detail"]
        )

    @pytest.mark.asyncio
    async def test_skills_contain_expected_content(
        self,
        client_with_roads: AsyncClient,
    ):
        """Test that skills contain expected DJ concepts."""
        response = await client_with_roads.get("/skills/dj-core")
        instructions = response.json()["instructions"]

        # Core concepts should be present
        expected_terms = [
            "DataJunction",
            "semantic layer",
            "dimension link",
            "star schema",
            "Node Types",
        ]

        for term in expected_terms:
            assert term in instructions, f"Expected '{term}' in core skill instructions"

    @pytest.mark.asyncio
    async def test_builder_skill_has_metric_patterns(
        self,
        client_with_roads: AsyncClient,
    ):
        """Test that builder skill includes metric creation patterns."""
        response = await client_with_roads.get("/skills/dj-builder")
        instructions = response.json()["instructions"]

        # Should include metric creation guidance
        assert "Creating Metrics" in instructions
        assert "COUNT" in instructions or "SUM" in instructions
        assert "dimension_links" in instructions

    @pytest.mark.asyncio
    async def test_consumer_skill_has_query_patterns(
        self,
        client_with_roads: AsyncClient,
    ):
        """Test that consumer skill includes query patterns."""
        response = await client_with_roads.get("/skills/dj-consumer")
        instructions = response.json()["instructions"]

        # Should include query guidance
        assert "/sql/metrics/v3" in instructions
        assert "filters" in instructions
        assert "dimensions" in instructions
