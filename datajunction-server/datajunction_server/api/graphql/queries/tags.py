"""
Tags related queries.
"""

from typing import Annotated

import strawberry
from strawberry.types import Info

from datajunction_server.api.graphql.scalars.tag import Tag
from datajunction_server.database.tag import Tag as DBTag


def _to_graphql_tag(db_tag: DBTag) -> Tag:
    return Tag(  # type: ignore
        name=db_tag.name,
        display_name=db_tag.display_name,
        description=db_tag.description,
        tag_type=db_tag.tag_type,
        tag_metadata=db_tag.tag_metadata,
    )


async def list_tags(
    *,
    info: Info = None,
    tag_names: list[str] | None = None,
    tag_types: list[str] | None = None,
) -> list[Tag]:
    """
    Find available tags by the search parameters
    """
    session = info.context["session"]  # type: ignore
    db_tags = await DBTag.find_tags(session, tag_names, tag_types)
    return [_to_graphql_tag(db_tag) for db_tag in db_tags]


async def search_tags(
    search: Annotated[
        str,
        strawberry.argument(
            description="Query string to match against tag name, display name, and description.",
        ),
    ],
    limit: Annotated[
        int,
        strawberry.argument(description="Maximum number of matching tags to return."),
    ] = 10,
    *,
    info: Info = None,
) -> list[Tag]:
    """
    Trigram-ranked search across tags, used by the global search bar.
    """
    if not search or not search.strip():
        return []
    session = info.context["session"]  # type: ignore
    db_tags = await DBTag.search_tags(session, search.strip(), limit=limit)
    return [_to_graphql_tag(db_tag) for db_tag in db_tags]


async def list_tag_types(
    *,
    info: Info = None,
) -> list[str]:
    """
    List all tag types
    """
    session = info.context["session"]  # type: ignore
    return await DBTag.get_tag_types(session)
