"""Utils for handling GraphQL queries."""
import dataclasses
import datetime
import json
import re
from base64 import b64decode, b64encode
from dataclasses import dataclass
from typing import Any, Callable, Dict, Generic, Iterable, TypeVar

import strawberry
from strawberry import field

CURSOR_SEPARATOR = "-"


def convert_camel_case(name):
    """
    Convert from camel case to snake case
    """
    pattern = re.compile(r"(?<!^)(?=[A-Z])")
    name = pattern.sub("_", name).lower()
    return name


def extract_subfields(selection):
    """Extract subfields"""
    subfield = {}
    for sub_selection in selection.selections:
        field_name = convert_camel_case(sub_selection.name)
        if sub_selection.selections:
            subfield[field_name] = extract_subfields(sub_selection)
        else:
            subfield[field_name] = None

    return subfield


def extract_fields(query_fields) -> Dict[str, Any]:
    """
    Extract fields from GraphQL query input into a dictionary
    """
    fields = {}

    for query_field in query_fields.selected_fields:
        for selection in query_field.selections:
            field_name = convert_camel_case(selection.name)
            if selection.selections:
                subfield = extract_subfields(selection)
                fields[field_name] = subfield
            else:
                fields[field_name] = None

    return fields


class DateTimeJSONEncoder(json.JSONEncoder):
    """
    JSON encoder that handles datetime objects
    """

    def default(self, obj):  # pylint: disable=arguments-renamed
        """
        Check if there are datetime objects and serialize them as ISO
        format strings.
        """
        if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
            return obj.isoformat()
        return super().default(obj)


class DateTimeJSONDecoder(json.JSONDecoder):
    """
    JSON decoder that handles ISO format datetime strings
    """

    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, source):  # pylint: disable=method-hidden
        """
        Check if the string is in ISO 8601 format and convert to a datetime
        object if it is.
        """
        for k, v in source.items():
            if isinstance(v, str):
                try:
                    source[k] = datetime.datetime.fromisoformat(str(v))
                except ValueError:
                    pass
        return source


@dataclass
class Cursor:
    """
    Dataclass that serializes into a string and back.
    """

    def encode(self) -> str:
        """Serialize this cursor into a string."""
        json_str = json.dumps(dataclasses.asdict(self), cls=DateTimeJSONEncoder)
        return b64encode(json_str.encode()).decode()

    @classmethod
    def decode(cls, serialized: str) -> "Cursor":
        """Parse the string into an instance of this Cursor class."""
        json_str = b64decode(serialized.encode()).decode()
        json_obj = json.loads(json_str, cls=DateTimeJSONDecoder)
        return cls(**json_obj)


GenericItem = TypeVar("GenericItem")
GenericItemNode = TypeVar("GenericItemNode")


@strawberry.type
class PageInfo:  # pylint: disable=too-few-public-methods
    """Metadata about a page in a connection."""

    has_next_page: bool = field(
        description="When paginating forwards, are there more nodes?",
    )
    has_prev_page: bool = field(
        description="When paginating forwards, are there more nodes?",
    )
    start_cursor: str | None = field(
        description="When paginating back, the cursor to continue.",
    )
    end_cursor: str | None = field(
        description="When paginating forwards, the cursor to continue.",
    )


@strawberry.type
class Edge(Generic[GenericItemNode]):  # pylint: disable=too-few-public-methods
    """Metadata about an item in a connection."""

    node: GenericItemNode


@strawberry.type
class Connection(Generic[GenericItemNode]):  # pylint: disable=too-few-public-methods
    """
    Pagination for a list of items.
    """

    page_info: PageInfo
    edges: list[Edge[GenericItemNode]]

    @classmethod
    def from_list(
        cls,
        first_item: GenericItem | None,
        last_item: GenericItem | None,
        items: Iterable[GenericItem],
        encode_cursor: Callable[[GenericItem], Cursor],
    ) -> "Connection":
        """
        Construct a Connection from a list of items.
        """
        start_cursor = encode_cursor(first_item).encode() if first_item else None
        end_cursor = encode_cursor(last_item).encode() if last_item else None
        return Connection(  # type: ignore
            page_info=PageInfo(  # type: ignore
                has_prev_page=first_item is not None,
                start_cursor=start_cursor,
                has_next_page=last_item is not None,
                end_cursor=end_cursor,
            ),
            edges=[Edge(node=item) for item in items],  # type: ignore
        )
