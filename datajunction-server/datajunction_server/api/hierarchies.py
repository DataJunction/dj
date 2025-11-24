"""
Hierarchies API endpoints.

Handles creation, retrieval, updating, deletion, and validation of hierarchies.
"""

from http import HTTPStatus
from typing import Callable, List, cast

from fastapi import Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.api.helpers import get_save_history
from datajunction_server.database.hierarchy import (
    Hierarchy,
    HierarchyLevel,
)
from datajunction_server.database.history import History
from datajunction_server.database.node import Node
from datajunction_server.database.user import User
from datajunction_server.models.node_type import NodeType
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
    HierarchyOutput,
    HierarchyInfo,
    HierarchyLevelOutput,
    HierarchyUpdateRequest,
    DimensionHierarchiesResponse,
    DimensionHierarchyNavigation,
    NavigationTarget,
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


@router.get(
    "/nodes/{dimension}/hierarchies/",
    response_model=DimensionHierarchiesResponse,
)
async def get_dimension_hierarchies(
    dimension: str,
    *,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> DimensionHierarchiesResponse:
    """
    Get all hierarchies that use a specific dimension node and show navigation options.

    This endpoint helps users discover:
    - What hierarchies include this dimension
    - What position the dimension occupies in each hierarchy
    - What other dimensions they can drill up or down to
    """
    # Validate that the dimension node exists and is a dimension
    node = cast(
        Node,
        await Node.get_by_name(session, dimension, raise_if_not_exists=True),
    )

    if node.type != NodeType.DIMENSION:
        raise DJInvalidInputException(
            message=f"Node '{dimension}' is not a dimension node (type: {node.type})",
        )

    # Find all hierarchies that use this dimension
    hierarchies_using_dimension = await Hierarchy.get_using_dimension(
        session,
        node.id,
    )

    # Build navigation information for each hierarchy
    navigation_info = []
    for hierarchy in hierarchies_using_dimension:
        # Find the level that references this dimension
        current_level = None
        for level in hierarchy.levels:  # pragma: no cover
            if level.dimension_node_id == node.id:
                current_level = level
                break

        if not current_level:
            continue  # pragma: no cover

        # Get sorted levels for navigation
        sorted_levels = sorted(hierarchy.levels, key=lambda lvl: lvl.level_order)

        # Build drill-up targets (lower level_order = coarser grain)
        drill_up = [
            NavigationTarget(
                level_name=level.name,
                dimension_node=level.dimension_node.name,
                level_order=level.level_order,
                steps=current_level.level_order - level.level_order,
            )
            for level in sorted_levels
            if level.level_order < current_level.level_order
        ]

        # Build drill-down targets (higher level_order = finer grain)
        drill_down = [
            NavigationTarget(
                level_name=level.name,
                dimension_node=level.dimension_node.name,
                level_order=level.level_order,
                steps=level.level_order - current_level.level_order,
            )
            for level in sorted_levels
            if level.level_order > current_level.level_order
        ]

        navigation_info.append(
            DimensionHierarchyNavigation(
                hierarchy_name=hierarchy.name,
                hierarchy_display_name=hierarchy.display_name,
                current_level=current_level.name,
                current_level_order=current_level.level_order,
                drill_up=drill_up,
                drill_down=drill_down,
            ),
        )

    return DimensionHierarchiesResponse(
        dimension_node=dimension,
        hierarchies=navigation_info,
    )


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
    validation_errors, existing_nodes = await Hierarchy.validate_levels(
        session,
        hierarchy_data.levels,
    )
    if validation_errors:
        raise DJInvalidInputException(
            message=f"Hierarchy validation failed: {'; '.join(validation_errors)}",
        )

    # Resolve dimension node names to IDs for creation (validation already confirmed they exist)
    dimension_nodes = {
        level.dimension_node: existing_nodes[level.dimension_node].id
        for level in hierarchy_data.levels
    }

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
    for idx, level_input in enumerate(hierarchy_data.levels):
        level = HierarchyLevel(
            hierarchy_id=hierarchy.id,
            name=level_input.name,
            dimension_node_id=dimension_nodes[level_input.dimension_node],
            level_order=idx,
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
                        "level_order": idx,
                        "grain_columns": level_input.grain_columns,
                    }
                    for idx, level_input in enumerate(hierarchy_data.levels)
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
        raise DJDoesNotExistException(message=f"Hierarchy '{name}' not found")

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
        validation_errors, dimension_nodes = await Hierarchy.validate_levels(
            session,
            update_data.levels,
        )
        if validation_errors:
            raise DJInvalidInputException(
                message=f"Hierarchy validation failed: {'; '.join(validation_errors)}",
            )

        # Delete existing levels by clearing the collection
        hierarchy.levels.clear()
        await session.flush()

        # Create new levels
        for idx, level_input in enumerate(update_data.levels):
            level = HierarchyLevel(
                hierarchy_id=hierarchy.id,
                name=level_input.name,
                dimension_node_id=dimension_nodes[level_input.dimension_node].id,
                level_order=idx,
                grain_columns=level_input.grain_columns,
            )
            hierarchy.levels.append(level)

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
        raise DJDoesNotExistException(message=f"Hierarchy '{name}' not found")

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
