"""
GraphQL scalars
"""
import dataclasses
import datetime
from base64 import b64decode, b64encode
from dataclasses import dataclass
from typing import Callable, Generic, List, Optional, TypeVar, Union

import orjson
import strawberry
from strawberry import field

BigInt = strawberry.scalar(
    Union[int, str],  # type: ignore
    serialize=int,
    parse_value=str,
    description="BigInt field",
)

GenericItem = TypeVar("GenericItem")
GenericItemNode = TypeVar("GenericItemNode")


@dataclass
class Cursor:
    """
    Dataclass that serializes into a string and back.
    """

    def encode(self) -> str:
        """Serialize this cursor into a string."""

        def default(obj):
            if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
                return obj.isoformat()
            raise TypeError(f"Type {type(obj)} not serializable")  # pragma: no cover

        json_bytes = orjson.dumps(dataclasses.asdict(self), default=default)
        return b64encode(json_bytes).decode()

    @classmethod
    def decode(cls, serialized: str) -> "Cursor":
        """Parse the string into an instance of this Cursor class."""
        json_bytes = b64decode(serialized.encode())

        def object_hook(obj):
            for k, v in obj.items():
                if isinstance(v, str):
                    try:
                        obj[k] = datetime.datetime.fromisoformat(v)
                    except ValueError:  # pragma: no cover
                        pass  # pragma: no cover
            return obj

        json_obj = orjson.loads(json_bytes)
        json_obj = object_hook(json_obj)
        return cls(**json_obj)


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
        items: List[GenericItem],
        before: Optional[str],
        after: Optional[str],
        limit: int,
        encode_cursor: Callable[[GenericItem], Cursor],
    ) -> "Connection":
        """
        Construct a Connection from a list of items.
        """
        has_next_page = len(items) > limit or (
            before is not None and len(items) > 0 and items[0] is not None
        )
        has_prev_page = (before is not None and len(items) > limit) or (
            after is not None and len(items) > 0 and items[0] is not None
        )
        start_cursor = encode_cursor(items[0]).encode() if items else None
        end_cursor = encode_cursor(items[-1]).encode() if items else None
        return Connection(  # type: ignore
            page_info=PageInfo(  # type: ignore
                has_prev_page=has_prev_page,
                start_cursor=start_cursor,
                has_next_page=has_next_page,
                end_cursor=end_cursor,
            ),
            edges=[Edge(node=item) for item in items[:limit]],  # type: ignore
        )
