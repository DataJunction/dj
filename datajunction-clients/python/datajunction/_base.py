"""Base client dataclasses."""
from dataclasses import fields, is_dataclass
from typing import TYPE_CHECKING, Any, Dict, Type, TypeVar, Union, get_args, get_origin

if TYPE_CHECKING:  # pragma: no cover
    from datajunction.client import DJClient


T = TypeVar("T")


class SerializableMixin:  # pylint: disable=too-few-public-methods
    """
    Mixin for serializing dictionaries to dataclasses
    """

    @classmethod
    def from_dict(cls: Type[T], dj_client: "DJClient", data: Dict[str, Any]) -> T:
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
            field_type = field.type
            field_value = data.get(field.name)

            # For optional field types, look at the inner type
            if get_origin(field_type) is Union and type(None) in get_args(field_type):
                field_type = next(  # pragma: no cover
                    t for t in get_args(field_type) if t is not type(None)  # noqa: E721
                )

            if get_origin(field_type) is list:
                list_inner_type = get_args(field_type)[0]
                if is_dataclass(list_inner_type) and isinstance(field_value, list):
                    field_values[field.name] = [
                        list_inner_type.from_dict(dj_client=dj_client, data=item)
                        if isinstance(item, dict)
                        else item
                        for item in field_value
                    ]
                else:
                    field_values[field.name] = field_value  # type: ignore
            elif is_dataclass(field_type) and isinstance(field_value, dict):
                field_values[field.name] = field_type.from_dict(
                    dj_client=dj_client,
                    data=field_value,
                )
            else:
                field_values[field.name] = field_value  # type: ignore
        if is_dataclass(cls) and "dj_client" in cls.__dataclass_fields__.keys():  # type: ignore
            return cls(dj_client=dj_client, **field_values)  # type: ignore
        return cls(**field_values)
