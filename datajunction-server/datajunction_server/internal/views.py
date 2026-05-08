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
from datajunction_server.transpilation import transpile_sql
from datajunction_server.utils import get_settings, session_context

_logger = logging.getLogger(__name__)


class CubeViewNames:
    """View names for a cube across both Spark and Trino dialects."""

    def __init__(self, cube_name: str, version: str):
        settings = get_settings()
        cube_stem = cube_name.replace(".", "_")
        version_suffix = version.replace(".", "_")
        prefix = f"{settings.view_catalog}.{settings.view_schema}"

        self.spark_versioned = f"{prefix}.{cube_stem}_{version_suffix}"
        self.spark_unversioned = f"{prefix}.{cube_stem}"
        self.trino_versioned = f"{prefix}.{cube_stem}_{version_suffix}__trino"
        self.trino_unversioned = f"{prefix}.{cube_stem}__trino"


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
        if avail.catalog and avail.table:  # pragma: no branch
            schema_part = f"{avail.schema_}." if avail.schema_ else ""
            table_ref = f"{avail.catalog}.{schema_part}{avail.table}"
            return f"SELECT * FROM {table_ref}", True

    # Build measures SQL
    metrics = revision.cube_node_metrics
    dimensions = revision.cube_node_dimensions

    if not metrics:  # pragma: no cover
        raise ValueError(
            f"Cube '{cube_name}' has no metrics — cannot build view SQL",
        )

    result = await build_measures_sql(
        session=session,
        metrics=metrics,
        dimensions=dimensions,
        filters=[],
        dialect=Dialect.SPARK,
        use_materialized=False,
    )
    if not result.grain_groups:  # pragma: no cover
        raise ValueError(
            f"Cube '{cube_name}' produced no grain groups — cannot build view SQL",
        )

    combined = build_combiner_sql(result.grain_groups)
    return combined.sql, False


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
            names = CubeViewNames(cube_name, revision.version)
            _logger.info(
                "[views] Resolved views for cube %s: spark=%s, trino=%s",
                cube_name,
                names.spark_versioned,
                names.trino_versioned,
            )

            spark_body, is_materialized = await _build_view_body(cube_name, session)
            _logger.info(
                "[views] Built view body for cube %s (materialized=%s, body_length=%d)",
                cube_name,
                is_materialized,
                len(spark_body),
            )

            # Resolve catalog/engine for query submission
            catalog_name = None
            engine_name = None
            engine_version = None
            if revision.catalog and revision.catalog.engines:  # pragma: no branch
                catalog_name = revision.catalog.name
                engine_name = revision.catalog.engines[0].name
                engine_version = revision.catalog.engines[0].version

        # Transpile to Trino
        trino_body = transpile_sql(spark_body, dialect=Dialect.TRINO)

        _logger.info(
            "[views] Using catalog=%s engine=%s for cube %s",
            catalog_name,
            engine_name,
            cube_name,
        )

        # Helper to submit a single view DDL
        async def _submit_view(view_name: str, ddl: str, engine: str):
            _logger.info(
                "[views] Submitting %s view DDL for cube %s: %s",
                engine,
                cube_name,
                ddl[:200],
            )
            await query_service_client.create_view(
                view_name=view_name,
                query_create=QueryCreate(
                    engine_name=engine,
                    catalog_name=catalog_name or "default",
                    engine_version=engine_version or "",
                    submitted_query=ddl,
                    async_=False,
                ),
                request_headers=request_headers,
            )
            _logger.info(
                "[views] Successfully submitted %s for cube %s",
                view_name,
                cube_name,
            )

        # Spark views
        await _submit_view(
            names.spark_versioned,
            f"CREATE OR REPLACE VIEW {names.spark_versioned} AS {spark_body}",
            engine_name or "SPARKSQL",
        )
        await _submit_view(
            names.spark_unversioned,
            f"CREATE OR REPLACE VIEW {names.spark_unversioned} AS SELECT * FROM {names.spark_versioned}",
            engine_name or "SPARKSQL",
        )

        # Trino views
        await _submit_view(
            names.trino_versioned,
            f"CREATE OR REPLACE VIEW {names.trino_versioned} AS {trino_body}",
            "TRINO_DIRECT",
        )
        await _submit_view(
            names.trino_unversioned,
            f"CREATE OR REPLACE VIEW {names.trino_unversioned} AS SELECT * FROM {names.trino_versioned}",
            "TRINO_DIRECT",
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
