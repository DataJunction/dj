"""
DataLoaders for batching and caching GraphQL queries.
"""

import json
import logging
from typing import Any, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.orm import joinedload, load_only, noload, selectinload
from strawberry.dataloader import DataLoader
from starlette.requests import Request

from datajunction_server.api.graphql.resolvers.nodes import load_node_options
from datajunction_server.construction.build_v3.loaders import find_upstream_node_names
from datajunction_server.database.collection import Collection as DBCollection
from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.node import (
    Node as DBNode,
    NodeRevision as DBNodeRevision,
)
from datajunction_server.internal.namespaces import (
    get_parent_namespaces,
    resolve_git_info_from_map,
)
from datajunction_server.sql.decompose import (
    MetricComponent,
    MetricComponentExtractor,
)
from datajunction_server.sql.parsing import ast
from datajunction_server.utils import session_context

logger = logging.getLogger(__name__)


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
        if fields_json:  # pragma: no branch
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


async def batch_load_git_info(
    namespaces: list[str],
    request: Request,
) -> list[Optional[dict]]:
    """
    Batch load git info for multiple namespaces in a single query.

    Collects all ancestor namespaces across all requested namespaces,
    loads them in one query, then resolves git info for each from the
    shared map.
    """
    # Collect all unique namespace names we need to look up
    all_ancestor_names: set[str] = set()
    for ns in namespaces:
        ancestors = get_parent_namespaces(ns) + [ns]
        all_ancestor_names.update(ancestors)

    async with session_context(request) as session:
        # Single query to load all namespace rows
        stmt = select(NodeNamespace).where(
            NodeNamespace.namespace.in_(all_ancestor_names),
        )
        rows = (await session.execute(stmt)).scalars().all()
        ns_map = {ns.namespace: ns for ns in rows}

        # Handle FK hops: some branch namespaces point to a parent outside
        # the string hierarchy. Collect missing parent_namespace values and
        # load them in one extra query.
        missing_fk_parents: set[str] = set()
        for ns in namespaces:
            ancestors = get_parent_namespaces(ns) + [ns]
            for name in reversed(ancestors):
                row = ns_map.get(name)
                if row and row.git_branch:
                    if row.parent_namespace and row.parent_namespace not in ns_map:
                        missing_fk_parents.add(row.parent_namespace)
                    break

        if missing_fk_parents:
            fk_stmt = select(NodeNamespace).where(
                NodeNamespace.namespace.in_(missing_fk_parents),
            )
            fk_rows = (await session.execute(fk_stmt)).scalars().all()
            for fk_row in fk_rows:
                ns_map[fk_row.namespace] = fk_row

    # Resolve git info for each requested namespace using the shared map
    return [resolve_git_info_from_map(ns, ns_map) for ns in namespaces]


def create_git_info_loader(
    request: Request,
) -> DataLoader[str, Optional[dict]]:
    """
    Create a DataLoader that batches git info lookups by namespace.

    Multiple nodes sharing the same or overlapping namespace hierarchies
    will be resolved with at most 2 DB queries (ancestors + FK hops)
    instead of one query per node.
    """
    return DataLoader(
        load_fn=lambda keys: batch_load_git_info(keys, request),
    )


async def batch_load_extracted_measures(
    node_revision_ids: list[int],
    request: Request,
) -> list[Optional[Tuple[list[MetricComponent], "ast.Query"]]]:
    """
    Batch-extract metric components for multiple metric node revisions.

    Opens ONE reader session and pre-populates the caches that
    MetricComponentExtractor.extract() accepts:

    - ``nodes_cache``: name -> Node (with current.query loaded), covering
      every metric in the batch plus their full upstream metric chain.
      This replaces the per-metric ``_load_metric_data`` that runs 2 DB
      queries per call.
    - ``parent_map``: child_name -> [parent_names], built from one recursive
      CTE. Replaces the per-base-metric "is this parent derived?" check
      inside the extractor's recursion.

    With both caches populated, ``extract()`` takes the cached path at
    ``_build_metric_data_from_cache`` and fires zero DB queries (even for
    derived metrics that recurse into parents). Nets hundreds to thousands
    of queries down to three per request:

      1. map nr_id -> node name
      2. upstream CTE (all ancestor metric names)
      3. bulk-load Node objects for the upstream set
    """
    async with session_context(
        request,
        session_label="graphql extracted_measures batch",
    ) as session:
        # 1) nr_id -> name, so we can kick off the upstream walk and later
        # look up each batch entry's metric_node.
        nr_stmt = (
            select(DBNodeRevision.id, DBNode.name)
            .join(DBNode, DBNodeRevision.node_id == DBNode.id)
            .where(DBNodeRevision.id.in_(node_revision_ids))
        )
        nr_rows = (await session.execute(nr_stmt)).all()
        nr_to_name: dict[int, str] = {row.id: row.name for row in nr_rows}

        # 2) Walk the upstream graph once; pulls every ancestor that's a
        # parent of any batch metric, and produces parent_map as a side effect.
        all_names, parent_map = await find_upstream_node_names(
            session,
            list(nr_to_name.values()),
        )

        # 3) Bulk-load Node objects for the full ancestor set. We only need
        # name, type, and current.query — everything else is noloaded so this
        # is a narrow query. noload(Node.created_by/Node.tags) are safe because
        # MetricComponentExtractor never reads them.
        nodes_cache: dict[str, DBNode] = {}
        if all_names:
            node_stmt = (
                select(DBNode)
                .where(DBNode.name.in_(all_names))
                .options(
                    load_only(
                        DBNode.name,
                        DBNode.type,
                        DBNode.current_version,
                    ),
                    noload(DBNode.created_by),
                    noload(DBNode.tags),
                    joinedload(DBNode.current).options(
                        noload(DBNodeRevision.created_by),
                        load_only(
                            DBNodeRevision.id,
                            DBNodeRevision.name,
                            DBNodeRevision.query,
                        ),
                    ),
                )
            )
            nodes_cache = {
                n.name: n
                for n in (await session.execute(node_stmt)).unique().scalars().all()
            }

        # 4) Per nr_id: invoke extract() with the cache trio so the extractor
        # takes the zero-DB-query path through _build_metric_data_from_cache,
        # including its recursion.
        results: list[Optional[Tuple[list[MetricComponent], "ast.Query"]]] = []
        for nr_id in node_revision_ids:
            name = nr_to_name.get(nr_id)
            metric_node = nodes_cache.get(name) if name else None
            if metric_node is None or metric_node.current is None:
                results.append(None)
                continue
            try:
                extractor = MetricComponentExtractor(nr_id)
                components, derived_ast = await extractor.extract(
                    session,
                    nodes_cache=nodes_cache,
                    parent_map=parent_map,
                    metric_node=metric_node,
                )
                results.append((components, derived_ast))
            except Exception as exc:  # pragma: no cover
                logger.warning(
                    "extracted_measures extraction failed for nr_id=%s: %s",
                    nr_id,
                    exc,
                )
                results.append(None)
        return results


def create_extracted_measures_loader(
    request: Request,
) -> DataLoader[int, Optional[Tuple[list[MetricComponent], "ast.Query"]]]:
    """
    Create a DataLoader that batches MetricComponentExtractor.extract() calls
    across a GraphQL request. Keys are NodeRevision ids.

    For a query like `findNodes(nodeTypes: [METRIC]) { current { extractedMeasures {...} } }`
    returning N metrics, Strawberry batches all per-node loader.load(nr_id)
    calls within a single event-loop tick into one
    batch_load_extracted_measures(ids) — collapsing N sessions + N extractions
    into 1 session + N extractions sharing nodes_cache and parent_map.
    """
    return DataLoader(
        load_fn=lambda keys: batch_load_extracted_measures(keys, request),
    )
