import hashlib
import json
from typing import Any
from sqlalchemy.ext.asyncio import AsyncSession
from datajunction_server.database.queryrequest import QueryBuildType, QueryRequest


async def build_cache_key(
    session: AsyncSession,
    query_type: QueryBuildType,
    nodes: list[str],
    dimensions: list[str],
    filters: list[str],
    engine_name: str | None = None,
    engine_version: str | None = None,
    limit: int | None = None,
    orderby: list[str] | None = None,
    other_args: dict[str, Any] | None = None,
) -> str:
    """
    Returns a cache key for the query request.
    """
    versioned_request = await QueryRequest.to_versioned_query_request(
        session=session,
        nodes=nodes,
        dimensions=dimensions,
        filters=filters,
        orderby=orderby or [],
        query_type=QueryBuildType(query_type),
    )
    versioned_with_metadata = {
        **versioned_request,  # type: ignore
        "engine_name": engine_name,
        "engine_version": engine_version,
        "limit": limit,
        "other_args": other_args,
    }
    serialized = json.dumps(versioned_with_metadata, sort_keys=True)
    digest = hashlib.sha256(serialized.encode()).hexdigest()
    return f"sql:{query_type}:{digest}"
