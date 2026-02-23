"""
Collection GraphQL scalar types.
"""

from datetime import datetime
from typing import TYPE_CHECKING

import strawberry
from strawberry.types import Info

from datajunction_server.api.graphql.scalars.node import Node
from datajunction_server.api.graphql.scalars.user import User

if TYPE_CHECKING:
    from datajunction_server.database.collection import (
        Collection as DBCollection,
    )


@strawberry.type
class Collection:
    """
    A collection of nodes.
    """

    id: int
    name: str
    description: str | None
    created_at: datetime
    created_by: User
    node_count: int

    @strawberry.field
    async def nodes(self, info: Info) -> list[Node]:
        """
        Get the nodes in this collection.
        Uses dataloader to batch requests efficiently.
        """
        loader = info.context.get("collection_nodes_loader")
        if not loader:
            # Fallback if dataloader not configured
            session = info.context["session"]
            from datajunction_server.database.collection import (
                Collection as DBCollection,
            )

            collection = await DBCollection.get_by_name(session, self.name)
            if not collection:
                return []
            return collection.nodes  # type: ignore

        # Use dataloader for efficient batching
        nodes = await loader.load(self.id)
        return nodes  # type: ignore

    @classmethod
    def from_db_collection(
        cls,
        collection: "DBCollection",
        node_count: int,
    ) -> "Collection":
        """
        Create a Collection from a database Collection.

        Args:
            collection: Database collection object
            node_count: Pre-computed node count (from SQL subquery)
        """
        return cls(  # type: ignore
            id=collection.id,
            name=collection.name or "",
            description=collection.description,
            created_at=collection.created_at,
            created_by=User(  # type: ignore
                id=collection.created_by.id,
                username=collection.created_by.username,
                email=collection.created_by.email,
                name=collection.created_by.name,
                oauth_provider=collection.created_by.oauth_provider,
                is_admin=collection.created_by.is_admin,
            ),
            node_count=node_count,
        )
