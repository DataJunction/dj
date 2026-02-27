"""Skill provider interface and default implementation."""

import importlib
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from datajunction_server import __version__ as DJ_VERSION

logger = logging.getLogger(__name__)


class SkillProvider(ABC):
    """Base class for providing DJ skills.

    Implementations can override skill content to provide
    deployment-specific documentation (e.g., Netflix-specific conventions).
    """

    @abstractmethod
    def get_core_skill(self, session: Session) -> dict:
        """Return dj-core skill content.

        Args:
            session: Database session for dynamic content

        Returns:
            dict with keys: name, version, description, keywords, instructions, metadata
        """
        pass

    @abstractmethod
    def get_builder_skill(self, session: Session) -> dict:
        """Return dj-builder skill content.

        Args:
            session: Database session for dynamic content

        Returns:
            dict with keys: name, version, description, keywords, instructions, metadata
        """
        pass

    @abstractmethod
    def get_consumer_skill(self, session: Session) -> dict:
        """Return dj-consumer skill content.

        Args:
            session: Database session for dynamic content

        Returns:
            dict with keys: name, version, description, keywords, instructions, metadata
        """
        pass

    def get_namespace_skill(
        self,
        namespace: str,
        session: Session
    ) -> Optional[dict]:
        """Return namespace-specific skill, or None for default generation.

        Override this to provide custom namespace documentation.

        Args:
            namespace: Namespace name (e.g., "finance", "growth")
            session: Database session

        Returns:
            Skill dict, or None to use default auto-generation
        """
        return None


class DefaultSkillProvider(SkillProvider):
    """Default OSS skill provider.

    Serves base skill content from markdown files.
    """

    def get_core_skill(self, session: Session) -> dict:
        """Return vanilla DJ core skill."""
        from datajunction_server.skills.content.loaders import load_skill_markdown

        instructions = load_skill_markdown("datajunction-core")

        return {
            "name": "datajunction-core",
            "version": DJ_VERSION,
            "description": "DataJunction semantic layer fundamentals",
            "keywords": [
                "DataJunction",
                "DJ",
                "semantic layer",
                "dimension link",
                "star schema",
                "metric",
                "SQL generation",
            ],
            "instructions": instructions,
            "metadata": {
                "provider": "default",
                "dj_version": DJ_VERSION,
                "generated_at": datetime.utcnow().isoformat(),
            }
        }

    def get_builder_skill(self, session: Session) -> dict:
        """Return DJ builder skill."""
        from datajunction_server.skills.content.loaders import load_skill_markdown

        instructions = load_skill_markdown("datajunction-builder")

        return {
            "name": "datajunction-builder",
            "version": DJ_VERSION,
            "description": "DataJunction semantic layer - building metrics and dimensions",
            "keywords": [
                "create metric",
                "define dimension",
                "dimension link",
                "build cube",
                "publish node",
                "metric creation",
            ],
            "instructions": instructions,
            "metadata": {
                "provider": "default",
                "dj_version": DJ_VERSION,
                "generated_at": datetime.utcnow().isoformat(),
            }
        }

    def get_consumer_skill(self, session: Session) -> dict:
        """Return DJ consumer skill."""
        from datajunction_server.skills.content.loaders import load_skill_markdown

        instructions = load_skill_markdown("datajunction-consumer")

        return {
            "name": "datajunction-consumer",
            "version": DJ_VERSION,
            "description": "DataJunction semantic layer - querying metrics and generating SQL",
            "keywords": [
                "query metric",
                "generate SQL",
                "available dimensions",
                "common dimensions",
                "run query",
                "SQL generation",
            ],
            "instructions": instructions,
            "metadata": {
                "provider": "default",
                "dj_version": DJ_VERSION,
                "generated_at": datetime.utcnow().isoformat(),
            }
        }


def get_skill_provider() -> SkillProvider:
    """Get configured skill provider.

    Loads provider class from skill_provider_class setting.
    Falls back to DefaultSkillProvider if not configured or loading fails.

    Returns:
        SkillProvider instance
    """
    from datajunction_server.utils import get_settings

    settings = get_settings()
    provider_class = settings.skill_provider_class

    if provider_class:
        try:
            # Load custom provider (e.g., Netflix's)
            module_path, class_name = provider_class.rsplit(".", 1)
            module = importlib.import_module(module_path)
            provider_cls = getattr(module, class_name)
            logger.info(f"Loaded custom skill provider: {provider_class}")
            return provider_cls()
        except Exception as e:
            # Fall back to default if custom provider fails
            logger.warning(
                f"Failed to load custom skill provider '{provider_class}': {e}. "
                f"Falling back to DefaultSkillProvider."
            )
            return DefaultSkillProvider()

    # Use default OSS provider
    return DefaultSkillProvider()
