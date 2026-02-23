"""
Collection GraphQL queries.
"""

from typing import Annotated

import strawberry
from sqlalchemy import func, select
from sqlalchemy.orm import joinedload
from sqlalchemy.sql.operators import is_
from strawberry.types import Info

from datajunction_server.api.graphql.scalars.collection import Collection
from datajunction_server.database.collection import Collection as DBCollection
from datajunction_server.database.collection import CollectionNodes
from datajunction_server.database.user import User


async def list_collections(
    fragment: Annotated[
        str | None,
        strawberry.argument(
            description="Search fragment to filter collections by name or description",
        ),
    ] = None,
    created_by: Annotated[
        str | None,
        strawberry.argument(
            description="Filter to collections created by this user",
        ),
    ] = None,
    limit: Annotated[
        int | None,
        strawberry.argument(description="Limit collections"),
    ] = 100,
    *,
    info: Info,
) -> list[Collection]:
    """
    List collections, optionally filtered by fragment, creator, or limit.
    """
    session = info.context["session"]

    # Subquery to count nodes per collection
    node_count_subquery = (
        select(
            CollectionNodes.collection_id,
            func.count(CollectionNodes.node_id).label("node_count"),
        )
        .group_by(CollectionNodes.collection_id)
        .subquery()
    )

    statement = (
        select(DBCollection, node_count_subquery.c.node_count)
        .outerjoin(
            node_count_subquery,
            DBCollection.id == node_count_subquery.c.collection_id,
        )
        .where(is_(DBCollection.deactivated_at, None))
        .options(
            joinedload(DBCollection.created_by),  # Eager load creator
        )
    )

    # Filter by fragment (search in name or description)
    if fragment:
        statement = statement.where(
            (DBCollection.name.ilike(f"%{fragment}%"))
            | (DBCollection.description.ilike(f"%{fragment}%")),
        )

    if created_by:
        statement = statement.join(
            User,
            DBCollection.created_by_id == User.id,
        ).where(User.username == created_by)

    statement = statement.order_by(DBCollection.created_at.desc())

    if limit and limit > 0:  # pragma: no branch
        statement = statement.limit(limit)

    result = await session.execute(statement)

    return [
        Collection.from_db_collection(collection, node_count or 0)
        for collection, node_count in result.unique().all()
    ]
