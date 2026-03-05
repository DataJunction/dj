"""
DataLoaders for batching and caching GraphQL queries.
"""

import json
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import selectinload
from strawberry.dataloader import DataLoader
from starlette.requests import Request

from datajunction_server.api.graphql.resolvers.nodes import load_node_options
from datajunction_server.database.collection import Collection as DBCollection
from datajunction_server.database.node import Node as DBNode
from datajunction_server.utils import session_context


async def batch_load_nodes(
    keys: list[tuple[str, dict[str, Any] | None]],
    request: Request,
) -> list[DBNode | None]:
    """
    Batch load nodes by name with optional field selection.

    Args:
        keys: List of (node_name, fields_dict) tuples
        request: The Starlette request object for creating sessions

    Returns:
        List of nodes in the same order as keys
    """
    # Extract unique node names
    node_names = [name for name, _ in keys]

    # For simplicity, use the most comprehensive fields dict among all requests
    # This ensures we load everything needed by any caller in the batch
    all_fields = {}
    for _, fields in keys:
        if fields:
            all_fields.update(fields)

    # Create a fresh session for this batch
    async with session_context(request) as session:
        # Build loading options based on requested fields
        options = load_node_options(all_fields) if all_fields else []

        # Load all nodes in a single query
        nodes = await DBNode.find_by(
            session,
            names=node_names,
            options=options,
        )

        # Create a lookup map
        node_map = {node.name: node for node in nodes}

        # Return nodes in the same order as requested keys
        return [node_map.get(name) for name in node_names]


async def batch_load_nodes_by_name_only(
    names: list[str],
    request: Request,
) -> list[DBNode | None]:
    """
    Simplified batch loader that loads nodes by name with default fields.

    This is used when we don't need fine-grained field selection.
    Creates its own independent session to avoid concurrent operation errors.

    Args:
        names: List of node names
        request: The Starlette request object for creating independent session

    Returns:
        List of nodes in the same order as names
    """
    # Create independent session for this dataloader batch
    async with session_context(request) as session:
        nodes = await DBNode.find_by(
            session,
            names=names,
            options=load_node_options({"name": None, "current": {"name": None}}),
        )

        # Create a lookup map
        node_map = {node.name: node for node in nodes}

        # Return nodes in the same order as requested
        return [node_map.get(name) for name in names]


def create_node_by_name_loader(request: Request) -> DataLoader[str, DBNode | None]:
    """
    Create a DataLoader for loading nodes by name.

    This loader batches multiple node lookups within a single request and caches
    the results to avoid duplicate queries. Creates independent sessions.

    Args:
        request: The Starlette request object

    Returns:
        A DataLoader instance for batching node lookups
    """
    return DataLoader(
        load_fn=lambda keys: batch_load_nodes_by_name_only(keys, request),
    )


async def batch_load_collection_nodes(
    keys: list[tuple[int, str]],
    request: Request,
) -> list[list[DBNode]]:
    """
    Batch load nodes for multiple collections with field-aware eager loading.

    Keys are (collection_id, fields_json) tuples where fields_json is a
    JSON-serialized dict of requested GraphQL fields (for load_node_options).

    Args:
        keys: List of (collection_id, fields_json) tuples
        request: The Starlette request object for creating sessions

    Returns:
        List of node lists, one per key, in the same order
    """
    collection_ids = [cid for cid, _ in keys]

    # Merge all requested fields across all loaders in this batch
    all_fields: dict[str, Any] = {}
    for _, fields_json in keys:
        if fields_json:
            all_fields.update(json.loads(fields_json))

    async with session_context(request) as session:
        node_options = load_node_options(all_fields)
        stmt = (
            select(DBCollection)
            .where(DBCollection.id.in_(collection_ids))
            .options(selectinload(DBCollection.nodes).options(*node_options))
        )
        result = await session.execute(stmt)
        collections = result.unique().scalars().all()

        collection_nodes_map = {c.id: c.nodes for c in collections}
        return [collection_nodes_map.get(cid, []) for cid in collection_ids]


def create_collection_nodes_loader(
    request: Request,
) -> DataLoader[tuple[int, str], list[DBNode]]:
    """
    Create a DataLoader for loading nodes by collection ID.

    Keys are (collection_id, fields_json) tuples so the loader can
    eagerly load only the node relationships the query actually requests.

    Args:
        request: The Starlette request object

    Returns:
        A DataLoader instance for batching collection node lookups
    """
    return DataLoader(
        load_fn=lambda keys: batch_load_collection_nodes(keys, request),
    )
