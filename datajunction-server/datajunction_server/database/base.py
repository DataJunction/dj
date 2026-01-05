"""
SQLAlchemy base model and custom type decorators.
"""

from typing import Dict, List, Optional, Type, TypeVar

from pydantic import BaseModel
from sqlalchemy import JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import TypeDecorator

Base = declarative_base()

T = TypeVar("T", bound=BaseModel)


class PydanticListType(TypeDecorator):
    """
    SQLAlchemy TypeDecorator for storing lists of Pydantic models as JSON.

    Automatically serializes Pydantic models to dicts on write and
    deserializes back to Pydantic models on read.

    Usage:
        measures: Mapped[List[PreAggMeasure]] = mapped_column(
            PydanticListType(PreAggMeasure),
            nullable=False,
        )
    """

    impl = JSON
    cache_ok = True

    def __init__(self, pydantic_type: Type[T]):
        super().__init__()
        self.pydantic_type = pydantic_type

    def process_bind_param(
        self,
        value: Optional[List[T]],
        dialect,
    ) -> Optional[List[Dict]]:
        """Serialize Pydantic models to dicts for storage."""
        if value is None:
            return None
        return [
            item.model_dump() if isinstance(item, BaseModel) else item for item in value
        ]

    def process_result_value(
        self,
        value: Optional[List[Dict]],
        dialect,
    ) -> Optional[List[T]]:
        """Deserialize dicts back to Pydantic models."""
        if value is None:
            return None
        return [self.pydantic_type.model_validate(item) for item in value]
