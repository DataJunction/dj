"""
Pydantic models for hierarchies.
"""

from datetime import datetime

from pydantic import BaseModel, Field, field_validator

from datajunction_server.models.node import NodeNameOutput
from datajunction_server.models.user import UserNameOnly


class HierarchyLevelInput(BaseModel):
    """Input model for creating a hierarchy level."""

    name: str
    dimension_node: str  # Dimension node name
    grain_columns: list[str] | None = None

    @classmethod
    def validate_list(
        cls,
        levels: list["HierarchyLevelInput"],
    ) -> list["HierarchyLevelInput"]:
        """Validate a list of hierarchy levels."""
        names = [level.name for level in levels]
        if len(set(names)) != len(names):
            raise ValueError("Level names must be unique")
        return levels


class HierarchyLevelOutput(BaseModel):
    """Output model for hierarchy levels."""

    name: str
    dimension_node: NodeNameOutput
    level_order: int
    grain_columns: list[str] | None = None

    class Config:
        from_attributes = True


class HierarchyCreateRequest(BaseModel):
    """Request model for creating a hierarchy."""

    name: str
    display_name: str | None = None
    description: str | None = None
    levels: list[HierarchyLevelInput] = Field(min_length=2)

    @field_validator("levels")
    @classmethod
    def validate_levels(
        cls,
        levels: list[HierarchyLevelInput],
    ) -> list[HierarchyLevelInput]:
        """Validate hierarchy levels and auto-assign level_order from list position."""
        return HierarchyLevelInput.validate_list(levels)


class HierarchyUpdateRequest(BaseModel):
    """Request model for updating a hierarchy."""

    display_name: str | None = None
    description: str | None = None
    levels: list[HierarchyLevelInput] | None = Field(None, min_length=2)

    @field_validator("levels")
    @classmethod
    def validate_levels(
        cls,
        levels: list[HierarchyLevelInput] | None,
    ) -> list[HierarchyLevelInput] | None:
        """Validate hierarchy levels if provided and auto-assign level_order."""
        if levels:
            return HierarchyLevelInput.validate_list(levels)
        return levels  # pragma: no cover


class HierarchyOutput(BaseModel):
    """Output model for hierarchies."""

    name: str
    display_name: str | None = None
    description: str | None = None
    created_by: UserNameOnly
    created_at: datetime
    levels: list[HierarchyLevelOutput]

    class Config:
        from_attributes = True


class HierarchyInfo(BaseModel):
    """Simplified hierarchy info for listings."""

    name: str
    display_name: str | None = None
    description: str | None = None
    created_by: UserNameOnly
    created_at: datetime
    level_count: int

    class Config:
        from_attributes = True


class HierarchyType(BaseModel):
    """Information about the type of hierarchy (single vs multi-dimension)."""

    type: str  # "single_dimension" | "multi_dimension"
    dimension_nodes: list[str]  # List of dimension node names used
    description: str


class NavigationTarget(BaseModel):
    """A level that can be navigated to in a hierarchy."""

    level_name: str
    dimension_node: str
    level_order: int
    steps: int  # How many levels away (1 = adjacent, 2 = two steps, etc.)


class DimensionHierarchyNavigation(BaseModel):
    """Navigation information for a dimension within a specific hierarchy."""

    hierarchy_name: str
    hierarchy_display_name: str | None = None
    current_level: str
    current_level_order: int
    drill_up: list[NavigationTarget] = []
    drill_down: list[NavigationTarget] = []


class DimensionHierarchiesResponse(BaseModel):
    """Response showing all hierarchies that use a dimension and navigation options."""

    dimension_node: str
    hierarchies: list[DimensionHierarchyNavigation]
