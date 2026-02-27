"""Loaders for skill content from markdown files."""

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Path to skills directory (relative to this file)
SKILLS_DIR = Path(__file__).parent.parent.parent.parent / "skills"


def load_skill_markdown(skill_name: str) -> str:
    """Load skill content from markdown file.

    Args:
        skill_name: Skill name without extension (e.g., "datajunction-core")

    Returns:
        Skill content as string

    Raises:
        FileNotFoundError: If skill file doesn't exist
    """
    skill_file = SKILLS_DIR / f"{skill_name}.md"

    if not skill_file.exists():
        raise FileNotFoundError(
            f"Skill file not found: {skill_file}. Expected skills in: {SKILLS_DIR}",
        )

    logger.debug(f"Loading skill from {skill_file}")
    return skill_file.read_text()
