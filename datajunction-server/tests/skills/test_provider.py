"""Tests for skill provider."""

import pytest

from datajunction_server.skills.provider import (
    DefaultSkillProvider,
    SkillProvider,
    get_skill_provider,
)


class TestSkillProvider:
    """Tests for SkillProvider interface and implementations."""

    def test_default_provider_is_abstract_subclass(self):
        """Test that DefaultSkillProvider implements SkillProvider."""
        assert issubclass(DefaultSkillProvider, SkillProvider)

    @pytest.mark.asyncio
    async def test_default_provider_get_core_skill(self, session):
        """Test DefaultSkillProvider.get_core_skill()."""
        provider = DefaultSkillProvider()
        skill = provider.get_core_skill(session)

        assert isinstance(skill, dict)
        assert skill["name"] == "datajunction-core"
        assert "version" in skill
        assert "instructions" in skill
        assert len(skill["instructions"]) > 0

    @pytest.mark.asyncio
    async def test_default_provider_get_builder_skill(self, session):
        """Test DefaultSkillProvider.get_builder_skill()."""
        provider = DefaultSkillProvider()
        skill = provider.get_builder_skill(session)

        assert skill["name"] == "datajunction-builder"
        assert "create metric" in skill["keywords"]

    @pytest.mark.asyncio
    async def test_default_provider_get_consumer_skill(self, session):
        """Test DefaultSkillProvider.get_consumer_skill()."""
        provider = DefaultSkillProvider()
        skill = provider.get_consumer_skill(session)

        assert skill["name"] == "datajunction-consumer"
        assert "query metric" in skill["keywords"]

    @pytest.mark.asyncio
    async def test_default_provider_get_namespace_skill_returns_none(self, session):
        """Test that default provider returns None for namespace skills."""
        provider = DefaultSkillProvider()
        skill = provider.get_namespace_skill("finance", session)

        assert skill is None

    def test_get_skill_provider_returns_default(self):
        """Test get_skill_provider() returns DefaultSkillProvider by default."""
        provider = get_skill_provider()

        assert isinstance(provider, DefaultSkillProvider)

    def test_custom_provider_can_be_loaded(self, monkeypatch):
        """Test that custom provider can be loaded via settings."""

        # Create a custom provider
        class CustomSkillProvider(SkillProvider):
            def get_core_skill(self, session):
                return {"name": "custom-core", "instructions": "Custom content"}

            def get_builder_skill(self, session):
                return {"name": "custom-builder", "instructions": "Custom content"}

            def get_consumer_skill(self, session):
                return {"name": "custom-consumer", "instructions": "Custom content"}

        # Mock settings to return custom provider class
        from datajunction_server import config

        mock_settings = config.Settings()
        mock_settings.skill_provider_class = (
            "tests.skills.test_provider.CustomSkillProvider"
        )

        # Inject the custom class into the test module's namespace
        import sys

        sys.modules[__name__].CustomSkillProvider = CustomSkillProvider

        def mock_get_settings():
            return mock_settings

        monkeypatch.setattr(
            "datajunction_server.skills.provider.get_settings",
            mock_get_settings,
        )

        # Get provider
        provider = get_skill_provider()

        # Should be our custom provider
        assert isinstance(provider, CustomSkillProvider)
        assert provider.get_core_skill(None)["name"] == "custom-core"

    def test_invalid_provider_falls_back_to_default(self, monkeypatch):
        """Test that invalid provider class falls back to DefaultSkillProvider."""
        from datajunction_server import config

        mock_settings = config.Settings()
        mock_settings.skill_provider_class = "nonexistent.module.Provider"

        def mock_get_settings():
            return mock_settings

        monkeypatch.setattr(
            "datajunction_server.skills.provider.get_settings",
            mock_get_settings,
        )

        # Should fall back to default
        provider = get_skill_provider()
        assert isinstance(provider, DefaultSkillProvider)
