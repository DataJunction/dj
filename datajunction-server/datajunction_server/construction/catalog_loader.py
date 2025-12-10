"""
Lean catalog loader for SQL generation.

This module provides a way to load all node metadata needed for SQL generation
in a minimal number of direct SQL queries, avoiding SQLAlchemy's ORM lazy loading.

The goal: Load everything needed to generate SQL in 2-5 queries total, not 2000+.

Usage:
    catalog = await load_catalog_for_metrics(
        session,
        metric_names=["my.metric"],
        dimension_names=["dim.column1"],
    )

    # Now access data from memory - no DB hits
    node = catalog.get_node("my.metric")
    print(node.columns)
"""

import logging
from dataclasses import dataclass, field
from typing import Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


@dataclass
class ColumnInfo:
    """Minimal column information needed for SQL generation."""

    id: int
    name: str
    type: str
    node_revision_id: int
    order: int
    # Dimension link info (if this column links to a dimension)
    dimension_node_name: Optional[str] = None
    dimension_column_name: Optional[str] = None
    # Additional attributes
    is_primary_key: bool = False


@dataclass
class DimensionLinkInfo:
    """Information about a dimension link."""

    id: int
    node_revision_id: int
    dimension_node_name: str
    join_type: Optional[str] = None
    join_sql: Optional[str] = None
    join_cardinality: Optional[str] = None
    role: Optional[str] = None


@dataclass
class AvailabilityInfo:
    """Availability state for a node."""

    catalog: str
    schema_: Optional[str] = None
    table: Optional[str] = None
    valid_through_ts: Optional[int] = None


@dataclass
class NodeInfo:
    """Minimal node information needed for SQL generation."""

    id: int
    name: str
    type: str
    version: str
    revision_id: int
    query: Optional[str] = None
    mode: Optional[str] = None
    # For source nodes
    catalog_name: Optional[str] = None
    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    # Cached/pickled AST
    query_ast_pickle: Optional[bytes] = None
    # Availability
    availability: Optional[AvailabilityInfo] = None

    columns: list[ColumnInfo] = field(default_factory=list)
    dimension_links: list[DimensionLinkInfo] = field(default_factory=list)
    parent_names: list[str] = field(default_factory=list)
    required_dimension_names: list[str] = field(default_factory=list)

    def get_column(self, name: str) -> Optional[ColumnInfo]:
        """Get column by name."""
        for col in self.columns:
            if col.name == name:
                return col
        return None

    def primary_key_columns(self) -> list[ColumnInfo]:
        """Get primary key columns."""
        return [col for col in self.columns if col.is_primary_key]

    def get_dimension_link(
        self,
        dimension_name: str,
        role: str = None,
    ) -> Optional[DimensionLinkInfo]:
        """Get dimension link by dimension name and optional role."""
        for link in self.dimension_links:
            if link.dimension_node_name == dimension_name:
                if role is None or link.role == role:
                    return link
        return None


@dataclass
class EngineInfo:
    """Engine information."""

    id: int
    name: str
    version: str
    dialect: str


@dataclass
class CatalogInfo:
    """Catalog information."""

    id: int
    name: str
    engines: list[EngineInfo] = field(default_factory=list)


@dataclass
class LoadedCatalog:
    """
    Pre-loaded catalog data for SQL generation.

    All data is loaded upfront in a few queries, then accessed in-memory.
    No lazy loading, no N+1 queries.
    """

    nodes: dict[str, NodeInfo] = field(default_factory=dict)
    catalogs: dict[str, CatalogInfo] = field(default_factory=dict)

    # Index by revision ID for quick lookup
    _nodes_by_revision_id: dict[int, NodeInfo] = field(default_factory=dict)

    def get_node(self, name: str) -> Optional[NodeInfo]:
        return self.nodes.get(name)

    def get_node_by_revision_id(self, revision_id: int) -> Optional[NodeInfo]:
        return self._nodes_by_revision_id.get(revision_id)

    def get_catalog(self, name: str) -> Optional[CatalogInfo]:
        return self.catalogs.get(name)

    def build_indexes(self):
        """Build internal indexes after loading."""
        self._nodes_by_revision_id = {
            node.revision_id: node for node in self.nodes.values()
        }


async def load_catalog_for_metrics(
    session: AsyncSession,
    metric_names: list[str],
    dimension_names: list[str],
) -> LoadedCatalog:
    """
    Load all catalog data needed to generate SQL for the given metrics and dimensions.

    This performs a minimal number of direct SQL queries to fetch:
    1. The metric nodes and their ancestors (transforms, sources)
    2. The dimension nodes referenced by the requested dimensions
    3. All columns and dimension links needed for join resolution

    Returns a LoadedCatalog that can be queried in-memory without any DB access.
    """
    catalog = LoadedCatalog()

    logger.info(
        f"[CatalogLoader] Starting load for metrics={metric_names}, dimensions={dimension_names}",
    )

    # Step 1: Load the metric nodes and find all their ancestors via recursive CTE
    node_names_to_load = await _find_all_ancestor_nodes(session, metric_names)

    # Add dimension nodes (extract node name from "node.column" format)
    for dim in dimension_names:
        if "." in dim:
            dim_node_name = dim.rsplit(".", 1)[0]
            node_names_to_load.add(dim_node_name)

    logger.info(f"[CatalogLoader] Found {len(node_names_to_load)} nodes in DAG")

    # Step 2: Load all nodes in one query
    if node_names_to_load:
        await _load_nodes(session, catalog, list(node_names_to_load))

    # Step 3: Load all columns for loaded nodes in one query
    revision_ids = [n.revision_id for n in catalog.nodes.values()]
    if revision_ids:
        await _load_columns(session, catalog, revision_ids)

    # Step 4: Load all dimension links for loaded nodes in one query
    if revision_ids:
        await _load_dimension_links(session, catalog, revision_ids)

    # Step 5: Load parent relationships
    if revision_ids:
        await _load_parent_relationships(session, catalog, revision_ids)

    # Step 6: Load required dimensions for metric nodes
    if revision_ids:
        await _load_required_dimensions(session, catalog, revision_ids)

    # Step 7: Load availability states
    if revision_ids:
        await _load_availability(session, catalog, revision_ids)

    # Step 8: Load catalogs and engines
    catalog_names = {n.catalog_name for n in catalog.nodes.values() if n.catalog_name}
    if catalog_names:
        await _load_catalogs(session, catalog, list(catalog_names))

    # Build indexes
    catalog.build_indexes()

    logger.info(
        f"[CatalogLoader] Loaded {len(catalog.nodes)} nodes, "
        f"{sum(len(n.columns) for n in catalog.nodes.values())} columns, "
        f"{sum(len(n.dimension_links) for n in catalog.nodes.values())} dimension links",
    )

    return catalog


async def _find_all_ancestor_nodes(
    session: AsyncSession,
    starting_node_names: list[str],
) -> set[str]:
    """
    Find all ancestor nodes (parents, grandparents, etc.) for the given nodes.
    Uses a recursive CTE to walk the node relationship graph.
    """
    if not starting_node_names:
        return set()

    # Recursive CTE to find all ancestors
    # This walks up the DAG from the starting nodes
    query = text("""
        WITH RECURSIVE ancestors AS (
            -- Base case: start with the requested nodes
            SELECT DISTINCT n.id, n.name, nr.id as revision_id
            FROM node n
            JOIN noderevision nr ON n.id = nr.node_id AND n.current_version = nr.version
            WHERE n.name = ANY(:names) AND n.deactivated_at IS NULL

            UNION

            -- Recursive case: find parents of nodes we've found
            SELECT DISTINCT parent.id, parent.name, parent_rev.id as revision_id
            FROM ancestors a
            JOIN noderevision child_rev ON child_rev.id = a.revision_id
            JOIN noderelationship rel ON rel.child_id = child_rev.id
            JOIN node parent ON parent.id = rel.parent_id AND parent.deactivated_at IS NULL
            JOIN noderevision parent_rev ON parent.id = parent_rev.node_id
                AND parent.current_version = parent_rev.version
        )
        SELECT DISTINCT name FROM ancestors
    """)

    result = await session.execute(query, {"names": starting_node_names})
    return {row[0] for row in result.fetchall()}


async def _load_nodes(
    session: AsyncSession,
    catalog: LoadedCatalog,
    node_names: list[str],
) -> None:
    """Load node and revision info for all specified nodes in one query."""

    query = text("""
        SELECT
            n.id,
            n.name,
            n.type,
            n.current_version,
            nr.id as revision_id,
            nr.query,
            nr.mode,
            c.name as catalog_name,
            nr.schema_ as schema_name,
            nr."table" as table_name,
            nr.query_ast as query_ast_pickle
        FROM node n
        JOIN noderevision nr ON n.id = nr.node_id AND n.current_version = nr.version
        LEFT JOIN catalog c ON nr.catalog_id = c.id
        WHERE n.name = ANY(:names) AND n.deactivated_at IS NULL
    """)

    result = await session.execute(query, {"names": node_names})

    for row in result.mappings():
        node = NodeInfo(
            id=row["id"],
            name=row["name"],
            type=row["type"],
            version=row["current_version"],
            revision_id=row["revision_id"],
            query=row["query"],
            mode=row["mode"],
            catalog_name=row["catalog_name"],
            schema_name=row["schema_name"],
            table_name=row["table_name"],
            query_ast_pickle=row["query_ast_pickle"],
        )
        catalog.nodes[node.name] = node


async def _load_columns(
    session: AsyncSession,
    catalog: LoadedCatalog,
    revision_ids: list[int],
) -> None:
    """Load all columns for the given revisions in one query."""

    query = text("""
        SELECT
            col.id,
            col.name,
            col.type,
            col.node_revision_id,
            col."order",
            dim_node.name as dimension_node_name,
            col.dimension_column as dimension_column_name
        FROM "column" col
        LEFT JOIN node dim_node ON col.dimension_id = dim_node.id
        WHERE col.node_revision_id = ANY(:revision_ids)
        ORDER BY col.node_revision_id, col."order"
    """)

    result = await session.execute(query, {"revision_ids": revision_ids})

    # Build a map of revision_id -> node for quick lookup
    rev_to_node = {n.revision_id: n for n in catalog.nodes.values()}

    for row in result.mappings():
        col = ColumnInfo(
            id=row["id"],
            name=row["name"],
            type=row["type"],
            node_revision_id=row["node_revision_id"],
            order=row["order"],
            dimension_node_name=row["dimension_node_name"],
            dimension_column_name=row["dimension_column_name"],
        )
        node = rev_to_node.get(row["node_revision_id"])
        if node:
            node.columns.append(col)

    # Load primary key attributes
    await _load_primary_key_columns(session, catalog, revision_ids)


async def _load_primary_key_columns(
    session: AsyncSession,
    catalog: LoadedCatalog,
    revision_ids: list[int],
) -> None:
    """Mark columns that are primary keys."""

    query = text("""
        SELECT
            ca.column_id
        FROM columnattribute ca
        JOIN attributetype at ON ca.attribute_type_id = at.id
        JOIN "column" col ON ca.column_id = col.id
        WHERE at.name = 'primary_key'
        AND col.node_revision_id = ANY(:revision_ids)
    """)

    result = await session.execute(query, {"revision_ids": revision_ids})
    pk_column_ids = {row[0] for row in result.fetchall()}

    # Mark columns as primary keys
    for node in catalog.nodes.values():
        for col in node.columns:
            if col.id in pk_column_ids:
                col.is_primary_key = True


async def _load_dimension_links(
    session: AsyncSession,
    catalog: LoadedCatalog,
    revision_ids: list[int],
) -> None:
    """Load all dimension links for the given revisions in one query."""

    query = text("""
        SELECT
            dl.id,
            dl.node_revision_id,
            dim_node.name as dimension_node_name,
            dl.join_type,
            dl.join_sql,
            dl.join_cardinality,
            dl.role
        FROM dimensionlink dl
        JOIN node dim_node ON dl.dimension_id = dim_node.id
        WHERE dl.node_revision_id = ANY(:revision_ids)
    """)

    result = await session.execute(query, {"revision_ids": revision_ids})

    rev_to_node = {n.revision_id: n for n in catalog.nodes.values()}

    for row in result.mappings():
        link = DimensionLinkInfo(
            id=row["id"],
            node_revision_id=row["node_revision_id"],
            dimension_node_name=row["dimension_node_name"],
            join_type=row["join_type"],
            join_sql=row["join_sql"],
            join_cardinality=row["join_cardinality"],
            role=row["role"],
        )
        node = rev_to_node.get(row["node_revision_id"])
        if node:
            node.dimension_links.append(link)


async def _load_parent_relationships(
    session: AsyncSession,
    catalog: LoadedCatalog,
    revision_ids: list[int],
) -> None:
    """Load parent relationships for the given revisions in one query."""

    query = text("""
        SELECT
            rel.child_id as revision_id,
            parent_node.name as parent_name
        FROM noderelationship rel
        JOIN node parent_node ON rel.parent_id = parent_node.id
        WHERE rel.child_id = ANY(:revision_ids)
    """)

    result = await session.execute(query, {"revision_ids": revision_ids})

    rev_to_node = {n.revision_id: n for n in catalog.nodes.values()}

    for row in result.mappings():
        node = rev_to_node.get(row["revision_id"])
        if node:
            node.parent_names.append(row["parent_name"])


async def _load_required_dimensions(
    session: AsyncSession,
    catalog: LoadedCatalog,
    revision_ids: list[int],
) -> None:
    """Load required dimensions for metric nodes."""

    query = text("""
        SELECT
            mrd.metric_id as revision_id,
            col.name as column_name,
            nr.name as node_name
        FROM metric_required_dimensions mrd
        JOIN "column" col ON mrd.bound_dimension_id = col.id
        JOIN noderevision nr ON col.node_revision_id = nr.id
        WHERE mrd.metric_id = ANY(:revision_ids)
    """)

    result = await session.execute(query, {"revision_ids": revision_ids})

    rev_to_node = {n.revision_id: n for n in catalog.nodes.values()}

    for row in result.mappings():
        node = rev_to_node.get(row["revision_id"])
        if node:
            # Store as "node_name.column_name" format
            dim_ref = f"{row['node_name']}.{row['column_name']}"
            node.required_dimension_names.append(dim_ref)


async def _load_availability(
    session: AsyncSession,
    catalog: LoadedCatalog,
    revision_ids: list[int],
) -> None:
    """Load availability states for nodes."""

    query = text("""
        SELECT
            nas.node_id as revision_id,
            avail.catalog,
            avail.schema_,
            avail."table",
            avail.valid_through_ts
        FROM nodeavailabilitystate nas
        JOIN availabilitystate avail ON nas.availability_id = avail.id
        WHERE nas.node_id = ANY(:revision_ids)
    """)

    result = await session.execute(query, {"revision_ids": revision_ids})

    rev_to_node = {n.revision_id: n for n in catalog.nodes.values()}

    for row in result.mappings():
        node = rev_to_node.get(row["revision_id"])
        if node:
            node.availability = AvailabilityInfo(
                catalog=row["catalog"],
                schema_=row["schema_"],
                table=row["table"],
                valid_through_ts=row["valid_through_ts"],
            )


async def _load_catalogs(
    session: AsyncSession,
    catalog: LoadedCatalog,
    catalog_names: list[str],
) -> None:
    """Load catalog and engine information."""

    # Load catalogs
    query = text("""
        SELECT id, name FROM catalog WHERE name = ANY(:names)
    """)
    result = await session.execute(query, {"names": catalog_names})

    catalog_ids = []
    for row in result.mappings():
        cat_info = CatalogInfo(id=row["id"], name=row["name"])
        catalog.catalogs[cat_info.name] = cat_info
        catalog_ids.append(row["id"])

    # Load engines for catalogs
    if catalog_ids:
        query = text("""
            SELECT
                ce.catalog_id,
                e.id,
                e.name,
                e.version,
                e.dialect
            FROM catalogengines ce
            JOIN engine e ON ce.engine_id = e.id
            WHERE ce.catalog_id = ANY(:catalog_ids)
        """)
        result = await session.execute(query, {"catalog_ids": catalog_ids})

        # Map catalog_id to catalog
        id_to_catalog = {c.id: c for c in catalog.catalogs.values()}

        for row in result.mappings():
            cat = id_to_catalog.get(row["catalog_id"])
            if cat:
                cat.engines.append(
                    EngineInfo(
                        id=row["id"],
                        name=row["name"],
                        version=row["version"],
                        dialect=row["dialect"],
                    ),
                )
