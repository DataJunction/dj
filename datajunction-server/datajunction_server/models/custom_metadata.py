"""API models for custom_metadata schema registry and filtering."""

from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel

from datajunction_server.models.node_type import NodeType


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


class CustomMetadataSchemaCreate(BaseModel):
    key: str
    node_type: Optional[NodeType] = None
    namespace: Optional[str] = None
    json_schema: dict
    filterable: bool = True
    description: Optional[str] = None


class CustomMetadataSchemaOutput(BaseModel):
    id: int
    key: str
    node_type: Optional[NodeType] = None
    namespace: Optional[str] = None
    json_schema: dict
    value_kind: Optional[str] = None
    filterable: bool
    description: Optional[str] = None

    class Config:
        from_attributes = True


class ViolationSample(BaseModel):
    node_name: str
    errors: list[str]


class ViolationReport(BaseModel):
    schema_id: int
    violation_count: int
    samples: list[ViolationSample]
