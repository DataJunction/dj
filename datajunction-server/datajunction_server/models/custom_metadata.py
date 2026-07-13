"""API models for custom_metadata schema registry and filtering."""

from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel


class CustomMetadataOp(str, Enum):
    EQ = "eq"
    NE = "ne"
    EXISTS = "exists"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    CONTAINS = "contains"  # array/object containment


class CustomMetadataFilter(BaseModel):
    key: str
    op: CustomMetadataOp = CustomMetadataOp.EQ
    value: Optional[Any] = None
