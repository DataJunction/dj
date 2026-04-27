"""
Cube view management.

Generates and submits CREATE OR REPLACE VIEW DDL for cube views
via the query service client (dj-query).
"""

import logging
import time
from typing import Dict, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.construction.build_v3 import (
    build_combiner_sql,
    build_measures_sql,
)
from datajunction_server.database.node import Node
from datajunction_server.instrumentation.provider import get_metrics_provider
from datajunction_server.models.dialect import Dialect
from datajunction_server.models.query import QueryCreate
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.utils import get_settings, session_context

_logger = logging.getLogger(__name__)


def _cube_view_names(
    cube_name: str,
    version: str,
) -> tuple[str, str]:
    """
    Derive the versioned and unversioned view names for a cube.
    """
    settings = get_settings()
    cube_stem = cube_name.replace(".", "_")
    version_suffix = version.replace(".", "_")
    versioned = (
        f"{settings.view_catalog}.{settings.view_schema}.{cube_stem}_{version_suffix}"
    )
    unversioned = f"{settings.view_catalog}.{settings.view_schema}.{cube_stem}"
    return versioned, unversioned


async def _build_view_body(
    cube_name: str,
    session: AsyncSession,
) -> tuple[str, bool]:
    """
    Build the SQL body for a cube's versioned view.

    Returns (sql_body, is_materialized).
    If materialized, returns SELECT * FROM materialized_table.
    Otherwise, returns the combined measures SQL.
    """
    node = await Node.get_cube_by_name(session, cube_name)
    revision = node.current  # type: ignore

    # Check for materialization
    if revision.availability:
        avail = revision.availability
        if avail.catalog and avail.table:
            schema_part = f"{avail.schema_}." if avail.schema_ else ""
            table_ref = f"{avail.catalog}.{schema_part}{avail.table}"
            return f"SELECT * FROM {table_ref}", True

    # Build measures SQL
    metrics = revision.cube_node_metrics
    dimensions = revision.cube_node_dimensions

    if metrics:
        result = await build_measures_sql(
            session=session,
            metrics=metrics,
            dimensions=dimensions,
            filters=[],
            dialect=Dialect.SPARK,
            use_materialized=False,
        )
        if result.grain_groups:
            combined = build_combiner_sql(result.grain_groups)
            return combined.sql, False

    return revision.query or "SELECT 1", False


async def create_cube_views(
    cube_name: str,
    query_service_client: QueryServiceClient,
    request_headers: Optional[Dict[str, str]] = None,
) -> None:
    """
    Generate and submit view DDL for a cube.

    Creates two views:
    - Versioned view: contains measures SQL or points at materialized table
    - Unversioned view: SELECT * FROM versioned view (stable name for Preset)

    Opens its own DB session (the request session may be closed by the time
    this background task runs).

    Fire-and-forget — logs errors but does not raise.
    """
    _logger.info(
        "[views] Starting view creation for cube %s",
        cube_name,
    )
    _t0 = time.monotonic()
    _tags = {"cube_name": cube_name}
    try:
        async with session_context() as session:
            node = await Node.get_cube_by_name(session, cube_name)
            revision = node.current  # type: ignore
            versioned_view, unversioned_view = _cube_view_names(
                cube_name,
                revision.version,
            )
            _logger.info(
                "[views] Resolved views for cube %s: versioned=%s, unversioned=%s",
                cube_name,
                versioned_view,
                unversioned_view,
            )

            view_body, is_materialized = await _build_view_body(cube_name, session)
            _logger.info(
                "[views] Built view body for cube %s (materialized=%s, body_length=%d)",
                cube_name,
                is_materialized,
                len(view_body),
            )

            # Resolve catalog/engine for query submission
            catalog_name = None
            engine_name = None
            engine_version = None
            if revision.catalog and revision.catalog.engines:
                catalog_name = revision.catalog.name
                engine_name = revision.catalog.engines[0].name
                engine_version = revision.catalog.engines[0].version

        _logger.info(
            "[views] Using catalog=%s engine=%s for cube %s",
            catalog_name,
            engine_name,
            cube_name,
        )

        # Submit versioned view DDL
        versioned_ddl = f"CREATE OR REPLACE VIEW {versioned_view} AS {view_body}"
        _logger.info(
            "[views] Submitting versioned view DDL to query service for cube %s: %s",
            cube_name,
            versioned_ddl[:200],
        )
        query_service_client.create_view(
            view_name=versioned_view,
            query_create=QueryCreate(
                engine_name=engine_name or "trino",
                catalog_name=catalog_name or "default",
                engine_version=engine_version or "",
                submitted_query=versioned_ddl,
                async_=False,
            ),
            request_headers=request_headers,
        )
        _logger.info(
            "[views] Successfully submitted versioned view %s for cube %s",
            versioned_view,
            cube_name,
        )

        # Submit unversioned view DDL
        unversioned_ddl = (
            f"CREATE OR REPLACE VIEW {unversioned_view} "
            f"AS SELECT * FROM {versioned_view}"
        )
        _logger.info(
            "[views] Submitting unversioned view DDL to query service for cube %s: %s",
            cube_name,
            unversioned_ddl,
        )
        query_service_client.create_view(
            view_name=unversioned_view,
            query_create=QueryCreate(
                engine_name=engine_name or "trino",
                catalog_name=catalog_name or "default",
                engine_version=engine_version or "",
                submitted_query=unversioned_ddl,
                async_=False,
            ),
            request_headers=request_headers,
        )
        _logger.info(
            "[views] Successfully submitted unversioned view %s for cube %s",
            unversioned_view,
            cube_name,
        )

        get_metrics_provider().timer(
            "dj.views.create_latency_ms",
            (time.monotonic() - _t0) * 1000,
            _tags,
        )
        get_metrics_provider().counter(
            "dj.views.create",
            tags={**_tags, "status": "success"},
        )

    except Exception:
        _logger.exception(
            "[views] Failed to create views for cube %s (non-blocking)",
            cube_name,
        )
        get_metrics_provider().counter(
            "dj.views.create",
            tags={**_tags, "status": "failed"},
        )
