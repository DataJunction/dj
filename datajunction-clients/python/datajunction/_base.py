"""Base client dataclasses."""

from dataclasses import MISSING, fields, is_dataclass
from types import UnionType
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Optional,
    Type,
    TypeVar,
    Union,
    get_args,
    get_origin,
)

if TYPE_CHECKING:  # pragma: no cover
    from datajunction.client import DJClient


T = TypeVar("T")


class SerializableMixin:  # pylint: disable=too-few-public-methods
    """
    Mixin for serializing dictionaries to dataclasses
    """

    @staticmethod
    def _serialize_nested(
        field_type: Type,
        field_value: Any,
        dj_client: Optional["DJClient"],
    ):
        """
        Handle nested field serialization
        """
        if is_dataclass(field_type) and isinstance(field_value, dict):
            return field_type.from_dict(dj_client, field_value)
        return field_value

    @staticmethod
    def _serialize_list(
        field_type: Type,
        field_value: Any,
        dj_client: Optional["DJClient"],
    ):
        """
        Handle serialization of lists of both primitive and dataclass object types
        """
        if not isinstance(field_value, list):
            return field_value  # Not a list, return as-is

        list_inner_type = get_args(field_type)[0]
        type_candidates = (
            list(get_args(list_inner_type))
            if get_origin(list_inner_type) in (Union, UnionType)
            else [list_inner_type]
        )

        def serialize_item(item):
            for candidate in type_candidates:
                try:
                    return (
                        candidate.from_dict(dj_client, item)
                        if isinstance(item, dict)
                        else item
                    )
                except (TypeError, AttributeError):  # Ignore and try the next candidate
                    pass
            return item  # pragma: no cover

        return [serialize_item(item) for item in field_value]

    @classmethod
    def from_dict(
        cls: Type[T],
        dj_client: Optional["DJClient"],
        data: Dict[str, Any],
    ) -> T:
        """
        Create an instance of the given dataclass `cls` from a dictionary `data`.
        This will handle nested dataclasses and optional types.
        """
        if not is_dataclass(cls):
            return cls(**data)

        field_values = {}
        for field in fields(cls):
            if field.name == "dj_client":
                continue

            # Resolve optional types to their inner type
            field_type = field.type
            origin = get_origin(field_type)
            if origin in (Union, UnionType):
                field_type = next(  # pragma: no cover
                    typ
                    for typ in get_args(field_type)
                    if typ is not type(None)  # noqa
                )

            # Serialize field value
            field_value = data.get(field.name)
            serialization_func = (
                SerializableMixin._serialize_list
                if get_origin(field_type) is list
                else SerializableMixin._serialize_nested
            )
            field_values[field.name] = serialization_func(
                field_type,
                field_value,
                dj_client,
            )

            # Apply default if necessary
            if (
                field.name not in field_values or field_values[field.name] is None
            ) and field.default is not MISSING:
                field_values[field.name] = field.default
        if is_dataclass(cls) and "dj_client" in cls.__dataclass_fields__.keys():  # type: ignore
            return cls(dj_client=dj_client, **field_values)  # type: ignore
        return cls(**field_values)
