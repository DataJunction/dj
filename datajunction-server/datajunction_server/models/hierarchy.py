"""
Pydantic models for hierarchies.
"""

from typing import List, Optional

from pydantic import BaseModel, Field, field_validator


class HierarchyLevelInput(BaseModel):
    """Input model for creating a hierarchy level."""

    name: str
    dimension_node: str  # Node name, not ID
    level_order: int
    grain_columns: Optional[List[str]] = None


class HierarchyLevelOutput(BaseModel):
    """Output model for hierarchy levels."""

    id: int
    name: str
    dimension_node_id: int
    dimension_node_name: str
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
            return levels

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

    id: int
    name: str
    display_name: Optional[str] = None
    description: Optional[str] = None
    levels: List[HierarchyLevelOutput]

    class Config:
        from_attributes = True


class HierarchyInfo(BaseModel):
    """Simplified hierarchy info for listings."""

    id: int
    name: str
    display_name: Optional[str] = None
    description: Optional[str] = None
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
