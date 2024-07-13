"""
DJ graphql api
"""
from enum import Enum
import inspect
from typing import Any
import strawberry


super_rpt = strawberry.experimental.pydantic.fields.replace_pydantic_types

def replace_pydantic_types(type_: Any, is_input: bool) -> Any:
    if inspect.isclass(type_) and issubclass(type_, Enum):
        return strawberry.enum(type_)
    return super_rpt(type_, is_input)

strawberry.experimental.pydantic.fields.replace_pydantic_types = replace_pydantic_types
