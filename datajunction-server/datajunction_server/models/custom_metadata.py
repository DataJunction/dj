"""API models for custom_metadata schema registry and filtering."""

from enum import Enum
from typing import Any

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
    value: Any | None = None


class CustomMetadataSchemaCreate(BaseModel):
    key: str
    node_type: NodeType | None = None
    namespace: str | None = None
    json_schema: dict
    filterable: bool = True
    description: str | None = None
    owner: str | None = None
    reserved: bool = False


class CustomMetadataSchemaOutput(BaseModel):
    id: int
    key: str
    node_type: NodeType | None = None
    namespace: str | None = None
    json_schema: dict
    value_kind: str | None = None
    filterable: bool
    description: str | None = None
    owner: str | None = None
    reserved: bool = False
    created_by_id: int | None = None
    updated_by_id: int | None = None

    class Config:
        from_attributes = True


class ViolationSample(BaseModel):
    node_name: str
    errors: list[str]


class ViolationReport(BaseModel):
    schema_id: int
    violation_count: int
    samples: list[ViolationSample]
