"""Skills API endpoints."""

import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.skills import get_skill_provider
from datajunction_server.utils import get_session

logger = logging.getLogger(__name__)

router = APIRouter(tags=["skills"])


class SkillResponse(BaseModel):
    """Skill response model."""

    name: str
    version: str
    description: str
    keywords: list[str]
    instructions: str
    metadata: dict


@router.get("/skills/dj-core", response_model=SkillResponse)
async def get_core_skill(
    session: AsyncSession = Depends(get_session),
) -> SkillResponse:
    """Get DJ core skill.

    Returns skill content suitable for Claude Code or other AI assistants.
    Can be customized per deployment via SKILL_PROVIDER_CLASS setting.

    Note: This endpoint is public (no authentication required) since skills
    are documentation/usage guides.
    """
    try:
        provider = get_skill_provider()
        skill_data = provider.get_core_skill(session)
        return SkillResponse(**skill_data)
    except FileNotFoundError as e:
        logger.error(f"Skill file not found: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Skill content not found. Please ensure skills are properly installed."
        )
    except Exception as e:
        logger.error(f"Error generating core skill: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate skill: {str(e)}"
        )


@router.get("/skills/dj-builder", response_model=SkillResponse)
async def get_builder_skill(
    session: AsyncSession = Depends(get_session),
) -> SkillResponse:
    """Get DJ builder skill.

    Returns skill content for building the semantic layer (creating metrics, dimensions, etc.).

    Note: This endpoint is public (no authentication required).
    """
    try:
        provider = get_skill_provider()
        skill_data = provider.get_builder_skill(session)
        return SkillResponse(**skill_data)
    except FileNotFoundError as e:
        logger.error(f"Skill file not found: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Skill content not found. Please ensure skills are properly installed."
        )
    except Exception as e:
        logger.error(f"Error generating builder skill: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate skill: {str(e)}"
        )


@router.get("/skills/dj-consumer", response_model=SkillResponse)
async def get_consumer_skill(
    session: AsyncSession = Depends(get_session),
) -> SkillResponse:
    """Get DJ consumer skill.

    Returns skill content for querying the semantic layer (generating SQL, running queries, etc.).

    Note: This endpoint is public (no authentication required).
    """
    try:
        provider = get_skill_provider()
        skill_data = provider.get_consumer_skill(session)
        return SkillResponse(**skill_data)
    except FileNotFoundError as e:
        logger.error(f"Skill file not found: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Skill content not found. Please ensure skills are properly installed."
        )
    except Exception as e:
        logger.error(f"Error generating consumer skill: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate skill: {str(e)}"
        )


@router.get("/skills/namespaces/{namespace}", response_model=SkillResponse)
async def get_namespace_skill(
    namespace: str,
    session: AsyncSession = Depends(get_session),
) -> SkillResponse:
    """Get namespace-specific skill.

    Returns skill content for a specific namespace (e.g., "finance", "growth").
    This can be customized per deployment or auto-generated from the catalog.

    Note: This endpoint is public (no authentication required).
    """
    try:
        provider = get_skill_provider()
        skill_data = provider.get_namespace_skill(namespace, session)

        if skill_data is None:
            # TODO: Phase 3 - Auto-generate from catalog
            raise HTTPException(
                status_code=404,
                detail=(
                    f"Namespace skill '{namespace}' not available. "
                    f"Auto-generation will be available in Phase 3."
                )
            )

        return SkillResponse(**skill_data)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating namespace skill for {namespace}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate skill: {str(e)}"
        )
