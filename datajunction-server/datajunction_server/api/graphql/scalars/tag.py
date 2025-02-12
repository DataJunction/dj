"""Tag-related scalars."""

import strawberry
from strawberry.types import Info

from datajunction_server.api.graphql.resolvers.tags import get_nodes_by_tag
from datajunction_server.api.graphql.scalars.node import Node, TagBase
from datajunction_server.api.graphql.utils import extract_fields


@strawberry.type
class Tag(TagBase):
    """
    A DJ node tag with nodes
    """

    @strawberry.field(description="The nodes with this tag")
    async def nodes(self, info: Info) -> list[Node]:
        """
        Lazy load the nodes with this tag.
        """
        fields = extract_fields(info)
        return await get_nodes_by_tag(  # type: ignore
            session=info.context["session"],
            fields=fields,
            tag_name=self.name,
        )
