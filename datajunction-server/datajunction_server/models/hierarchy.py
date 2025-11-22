"""
Pydantic models for hierarchies.
"""

from typing import List, Optional
from datetime import datetime

from datajunction_server.models.node import NodeNameOutput
from datajunction_server.models.user import UserNameOnly
from pydantic import BaseModel, Field, field_validator


class HierarchyLevelInput(BaseModel):
    """Input model for creating a hierarchy level."""

    name: str
    dimension_node: str  # Node name, not ID
    level_order: int
    grain_columns: Optional[List[str]] = None


class HierarchyLevelOutput(BaseModel):
    """Output model for hierarchy levels."""

    name: str
    dimension_node: NodeNameOutput
    level_order: int
    grain_columns: Optional[List[str]] = None

    class Config:
        from_attributes = True


class HierarchyCreateRequest(BaseModel):
    """Request model for creating a hierarchy."""

    name: str
    display_name: Optional[str] = None
    description: Optional[str] = None
    levels: List[HierarchyLevelInput] = Field(min_length=2)

    @field_validator("levels")
    @classmethod
    def validate_levels(
        cls,
        levels: List[HierarchyLevelInput],
    ) -> List[HierarchyLevelInput]:
        """Validate hierarchy levels."""
        # Check unique level names
        names = [level.name for level in levels]
        if len(set(names)) != len(names):
            raise ValueError("Level names must be unique")

        # Check unique level orders
        orders = [level.level_order for level in levels]
        if len(set(orders)) != len(orders):
            raise ValueError("Level orders must be unique")

        # Validate level order sequence starts at 0 and is consecutive
        sorted_orders = sorted(orders)
        expected_orders = list(range(len(orders)))
        if sorted_orders != expected_orders:
            raise ValueError("Level orders must be consecutive starting from 0")

        return levels


class HierarchyUpdateRequest(BaseModel):
    """Request model for updating a hierarchy."""

    display_name: Optional[str] = None
    description: Optional[str] = None
    levels: Optional[List[HierarchyLevelInput]] = None

    @field_validator("levels")
    @classmethod
    def validate_levels(
        cls,
        levels: Optional[List[HierarchyLevelInput]],
    ) -> Optional[List[HierarchyLevelInput]]:
        """Validate hierarchy levels if provided."""
        if levels is None:
            return levels  # pragma: no cover

        if len(levels) < 2:
            raise ValueError("Hierarchy must have at least 2 levels")

        # Check unique level names
        names = [level.name for level in levels]
        if len(set(names)) != len(names):
            raise ValueError("Level names must be unique")

        # Check unique level orders
        orders = [level.level_order for level in levels]
        if len(set(orders)) != len(orders):
            raise ValueError("Level orders must be unique")

        return levels


class HierarchyOutput(BaseModel):
    """Output model for hierarchies."""

    name: str
    display_name: Optional[str] = None
    description: Optional[str] = None
    created_by: UserNameOnly
    created_at: datetime
    levels: List[HierarchyLevelOutput]

    class Config:
        from_attributes = True


class HierarchyInfo(BaseModel):
    """Simplified hierarchy info for listings."""

    name: str
    display_name: Optional[str] = None
    description: Optional[str] = None
    created_by: UserNameOnly
    created_at: datetime
    level_count: int

    class Config:
        from_attributes = True


class HierarchyValidationError(BaseModel):
    """Model for hierarchy validation errors."""

    level_name: Optional[str] = None
    error_type: str
    message: str


class HierarchyValidationResult(BaseModel):
    """Result of hierarchy validation."""

    is_valid: bool
    errors: List[HierarchyValidationError] = []
    warnings: List[str] = []


class HierarchyType(BaseModel):
    """Information about the type of hierarchy (single vs multi-dimension)."""

    type: str  # "single_dimension" | "multi_dimension"
    dimension_nodes: List[str]  # List of dimension node names used
    description: str


class HierarchyAnalysis(BaseModel):
    """Analysis of a hierarchy's structure and characteristics."""

    name: str
    hierarchy_type: HierarchyType
    level_count: int
    max_depth: int
    validation_result: HierarchyValidationResult
    suggested_optimizations: List[str] = []


class NavigationTarget(BaseModel):
    """A level that can be navigated to in a hierarchy."""

    level_name: str
    dimension_node: str
    level_order: int
    steps: int  # How many levels away (1 = adjacent, 2 = two steps, etc.)


class DimensionHierarchyNavigation(BaseModel):
    """Navigation information for a dimension within a specific hierarchy."""

    hierarchy_name: str
    hierarchy_display_name: Optional[str] = None
    current_level: str
    current_level_order: int
    drill_up: List[NavigationTarget] = []
    drill_down: List[NavigationTarget] = []


class DimensionHierarchiesResponse(BaseModel):
    """Response showing all hierarchies that use a dimension and navigation options."""

    dimension_node: str
    hierarchies: List[DimensionHierarchyNavigation]
