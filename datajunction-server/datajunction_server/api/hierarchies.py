"""
Hierarchies API endpoints.

Handles creation, retrieval, updating, deletion, and validation of hierarchies.
"""

from http import HTTPStatus
from typing import Callable, List, cast

from fastapi import Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.api.helpers import get_save_history
from datajunction_server.database.hierarchy import (
    Hierarchy,
    HierarchyLevel,
)
from datajunction_server.database.history import History
from datajunction_server.database.node import Node
from datajunction_server.database.user import User
from datajunction_server.errors import (
    DJAlreadyExistsException,
    DJDoesNotExistException,
    DJInvalidInputException,
)
from datajunction_server.internal.history import ActivityType, EntityType
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.models.user import UserNameOnly
from datajunction_server.models.node import NodeNameOutput
from datajunction_server.models.hierarchy import (
    HierarchyCreateRequest,
    HierarchyLevelInput,
    HierarchyOutput,
    HierarchyInfo,
    HierarchyLevelOutput,
    HierarchyUpdateRequest,
    HierarchyValidationResult,
    HierarchyValidationError,
)
from datajunction_server.utils import (
    get_current_user,
    get_session,
)

router = SecureAPIRouter(tags=["hierarchies"])


@router.get("/hierarchies/", response_model=List[HierarchyInfo])
async def list_all_hierarchies(
    limit: int = Query(100, description="Maximum number of hierarchies to return"),
    offset: int = Query(0, description="Number of hierarchies to skip"),
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> List[HierarchyInfo]:
    """
    List all available hierarchies.
    """
    hierarchies = await Hierarchy.list_all(session, limit=limit, offset=offset)

    return [
        HierarchyInfo(
            name=h.name,
            display_name=h.display_name,
            description=h.description,
            created_by=UserNameOnly(username=h.created_by.username),
            created_at=h.created_at,
            level_count=len(h.levels),
        )
        for h in hierarchies
    ]


@router.post(
    "/hierarchies/",
    response_model=HierarchyOutput,
    status_code=HTTPStatus.CREATED,
)
async def create_hierarchy(
    hierarchy_data: HierarchyCreateRequest,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    save_history: Callable = Depends(get_save_history),
) -> HierarchyOutput:
    """
    Create a new hierarchy definition.
    """
    # Check if hierarchy already exists
    existing = await Hierarchy.get_by_name(session, hierarchy_data.name)
    if existing:
        raise DJAlreadyExistsException(
            message=f"Hierarchy '{hierarchy_data.name}' already exists",
        )

    # Validate hierarchy structure (this also resolves dimension node names to IDs)
    validation_errors = await Hierarchy.validate_levels(session, hierarchy_data.levels)
    if validation_errors:
        raise DJInvalidInputException(
            message=f"Hierarchy validation failed: {'; '.join(validation_errors)}",
        )

    # Resolve dimension node names to IDs for creation (validation already confirmed they exist)
    dimension_nodes = {}
    for level in hierarchy_data.levels:
        node = await Node.get_by_name(session, level.dimension_node)
        if node:
            dimension_nodes[level.dimension_node] = node.id

    # Create hierarchy
    hierarchy = Hierarchy(
        name=hierarchy_data.name,
        display_name=hierarchy_data.display_name,
        description=hierarchy_data.description,
        created_by_id=current_user.id,
    )
    session.add(hierarchy)
    await session.flush()  # Get the ID

    # Create levels
    for level_input in hierarchy_data.levels:
        level = HierarchyLevel(
            hierarchy_id=hierarchy.id,
            name=level_input.name,
            dimension_node_id=dimension_nodes[level_input.dimension_node],
            level_order=level_input.level_order,
            grain_columns=level_input.grain_columns,
        )
        session.add(level)

    await session.commit()

    # Log creation in history
    await save_history(
        event=History(
            entity_type=EntityType.HIERARCHY,
            entity_name=hierarchy.name,
            activity_type=ActivityType.CREATE,
            user=current_user.username,
            post={
                "name": hierarchy.name,
                "display_name": hierarchy.display_name,
                "description": hierarchy.description,
                "levels": [
                    {
                        "name": level_input.name,
                        "dimension_node_id": dimension_nodes[
                            level_input.dimension_node
                        ],
                        "level_order": level_input.level_order,
                        "grain_columns": level_input.grain_columns,
                    }
                    for level_input in hierarchy_data.levels
                ],
            },
        ),
        session=session,
    )

    # Reload with relationships
    created_hierarchy = cast(
        Hierarchy,
        await Hierarchy.get_by_id(session, hierarchy.id),
    )
    return _convert_to_output(created_hierarchy)


@router.get("/hierarchies/{name}", response_model=HierarchyOutput)
async def get_hierarchy(
    name: str,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> HierarchyOutput:
    """
    Get a specific hierarchy by name.
    """
    hierarchy = await Hierarchy.get_by_name(session, name)
    if not hierarchy:
        raise DJDoesNotExistException(message=f"Hierarchy '{name}' not found")
    return _convert_to_output(hierarchy)


@router.put("/hierarchies/{name}", response_model=HierarchyOutput)
async def update_hierarchy(
    name: str,
    update_data: HierarchyUpdateRequest,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    save_history=Depends(get_save_history),
) -> HierarchyOutput:
    """
    Update a hierarchy.
    """
    hierarchy = await Hierarchy.get_by_name(session, name)
    if not hierarchy:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f"Hierarchy '{name}' not found",
        )

    # Capture pre-state for history
    pre_state = {
        "name": hierarchy.name,
        "display_name": hierarchy.display_name,
        "description": hierarchy.description,
        "levels": [
            {
                "name": level.name,
                "dimension_node_id": level.dimension_node_id,
                "level_order": level.level_order,
                "grain_columns": level.grain_columns,
            }
            for level in sorted(hierarchy.levels, key=lambda lvl: lvl.level_order)
        ],
    }

    # Update basic fields
    if update_data.display_name is not None:
        hierarchy.display_name = update_data.display_name
    if update_data.description is not None:
        hierarchy.description = update_data.description

    # Update levels if provided
    if update_data.levels is not None:
        # Validate hierarchy structure (this also resolves dimension node names to IDs)
        validation_errors = await Hierarchy.validate_levels(session, update_data.levels)
        if validation_errors:
            raise DJInvalidInputException(
                message=f"Hierarchy validation failed: {'; '.join(validation_errors)}",
            )

        # Resolve dimension node names to IDs for creation (validation already confirmed they exist)
        dimension_node_names = {level.dimension_node for level in update_data.levels}
        dimension_nodes = {
            node.name: node
            for node in await Node.get_by_names(session, list(dimension_node_names))
        }

        # Delete existing levels and create new ones
        for level in hierarchy.levels:
            session.delete(level)

        # Create new levels
        levels = [
            HierarchyLevel(
                hierarchy_id=hierarchy.id,
                name=level.name,
                dimension_node_id=dimension_nodes[level.dimension_node].id,
                level_order=level.level_order,
                grain_columns=level.grain_columns,
            )
            for level in update_data.levels
        ]
        session.add_all(levels)
        hierarchy.levels = levels
        session.add(hierarchy)

    await session.commit()

    updated_hierarchy = cast(
        Hierarchy,
        await Hierarchy.get_by_id(session, hierarchy.id),
    )

    # Log update in history
    post_state = {
        "name": updated_hierarchy.name,
        "display_name": updated_hierarchy.display_name,
        "description": updated_hierarchy.description,
        "levels": [
            {
                "name": level.name,
                "dimension_node_id": level.dimension_node_id,
                "level_order": level.level_order,
                "grain_columns": level.grain_columns,
            }
            for level in sorted(
                updated_hierarchy.levels,
                key=lambda lvl: lvl.level_order,
            )
        ],
    }
    await save_history(
        event=History(
            entity_type=EntityType.HIERARCHY,
            entity_name=updated_hierarchy.name,
            activity_type=ActivityType.UPDATE,
            user=current_user.username,
            pre=pre_state,
            post=post_state,
        ),
        session=session,
    )

    return _convert_to_output(updated_hierarchy)


@router.delete("/hierarchies/{name}", status_code=HTTPStatus.NO_CONTENT)
async def delete_hierarchy(
    name: str,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
    save_history=Depends(get_save_history),
) -> None:
    """
    Delete a hierarchy.
    """
    hierarchy = await Hierarchy.get_by_name(session, name)
    if not hierarchy:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f"Hierarchy '{name}' not found",
        )

    # Capture pre-state for history before deletion
    pre_state = {
        "name": hierarchy.name,
        "display_name": hierarchy.display_name,
        "description": hierarchy.description,
        "levels": [
            {
                "name": level.name,
                "dimension_node_id": level.dimension_node_id,
                "level_order": level.level_order,
                "grain_columns": level.grain_columns,
            }
            for level in sorted(hierarchy.levels, key=lambda lvl: lvl.level_order)
        ],
    }

    await session.delete(hierarchy)
    await session.commit()

    # Log deletion in history
    await save_history(
        event=History(
            entity_type=EntityType.HIERARCHY,
            entity_name=name,
            activity_type=ActivityType.DELETE,
            user=current_user.username,
            pre=pre_state,
        ),
        session=session,
    )


@router.get("/hierarchies/{name}/levels", response_model=List[HierarchyLevelOutput])
async def get_hierarchy_levels(
    name: str,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> List[HierarchyLevelOutput]:
    """
    Get all levels for a specific hierarchy.
    """
    hierarchy = await Hierarchy.get_by_name(session, name)
    if not hierarchy:
        raise DJDoesNotExistException(message=f"Hierarchy '{name}' not found")

    return [
        HierarchyLevelOutput(
            name=level.name,
            dimension_node=NodeNameOutput(name=level.dimension_node.name),
            level_order=level.level_order,
            grain_columns=level.grain_columns,
        )
        for level in sorted(hierarchy.levels, key=lambda lvl: lvl.level_order)
    ]


@router.post("/hierarchies/{name}/validate", response_model=HierarchyValidationResult)
async def validate_hierarchy(
    name: str,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> HierarchyValidationResult:
    """
    Validate a hierarchy's structure and relationships.
    """
    hierarchy = await Hierarchy.get_by_name(session, name)
    if not hierarchy:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail=f"Hierarchy '{name}' not found",
        )

    # Convert database objects to input objects for validation
    level_inputs = [
        HierarchyLevelInput(
            name=level.name,
            dimension_node=level.dimension_node.name,
            level_order=level.level_order,
            grain_columns=level.grain_columns,
        )
        for level in hierarchy.levels
    ]

    validation_errors = await Hierarchy.validate_levels(session, level_inputs)

    errors = [
        HierarchyValidationError(
            error_type="validation_error",
            message=error,
        )
        for error in validation_errors
    ]

    return HierarchyValidationResult(
        is_valid=len(errors) == 0,
        errors=errors,
    )


def _convert_to_output(hierarchy: Hierarchy) -> HierarchyOutput:
    """Convert database hierarchy to output model."""
    return HierarchyOutput(
        name=hierarchy.name,
        display_name=hierarchy.display_name,
        description=hierarchy.description,
        created_by=UserNameOnly(username=hierarchy.created_by.username),
        created_at=hierarchy.created_at,
        levels=[
            HierarchyLevelOutput(
                name=level.name,
                dimension_node=NodeNameOutput(name=level.dimension_node.name),
                level_order=level.level_order,
                grain_columns=level.grain_columns,
            )
            for level in sorted(hierarchy.levels, key=lambda lvl: int(lvl.level_order))
        ],
    )
