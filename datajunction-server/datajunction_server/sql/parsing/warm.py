"""Fill the parse cache with stored node queries."""

import asyncio
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.parsing import cache
from datajunction_server.utils import DatabaseSessionManager, get_settings

_logger = logging.getLogger(__name__)
BATCH_SIZE = 500


@dataclass
class WarmSummary:
    """What a warm-up did."""

    parsed: int
    skipped: int
    seconds: float


async def warm_parse_cache(session: AsyncSession) -> WarmSummary:
    """Parse the queries of current node revisions into the cache, newest first."""
    started = time.perf_counter()
    parsed = skipped = 0
    last_id = sys.maxsize
    while (room := cache.PARSE_CACHE_SIZE - len(cache._cache)) > 0:
        rows = (
            await session.execute(
                select(NodeRevision.id, NodeRevision.query)
                .join(
                    Node,
                    (Node.id == NodeRevision.node_id)
                    & (NodeRevision.version == Node.current_version),
                )
                .where(
                    Node.deactivated_at.is_(None),
                    Node.type != NodeType.SOURCE,
                    NodeRevision.query.is_not(None),
                    NodeRevision.id < last_id,
                )
                .order_by(NodeRevision.id.desc())
                .limit(min(BATCH_SIZE, room)),
            )
        ).all()
        if not rows:
            break
        for last_id, query in rows:
            try:
                cache.parse_cached(query)
                parsed += 1
            except Exception:  # noqa: BLE001
                skipped += 1
    return WarmSummary(parsed, skipped, time.perf_counter() - started)


async def _warm_with_new_engine() -> WarmSummary:
    settings = get_settings()
    engine, sessionmaker = DatabaseSessionManager.create_engine_and_session(
        settings.reader_db,
        "reader",
    )
    try:
        async with sessionmaker() as session:
            return await warm_parse_cache(session)
    finally:
        # Closes the connections so a later fork inherits none.
        await engine.dispose()


def warm_parse_cache_sync() -> WarmSummary:
    """
    Warm the cache from plain synchronous code, such as module import.

    Runs in its own thread so it also works when an event loop is running.
    """
    with ThreadPoolExecutor(max_workers=1) as pool:
        summary = pool.submit(asyncio.run, _warm_with_new_engine()).result()
    _logger.info(
        "Parse cache warmed: %d parsed, %d skipped, %.1fs",
        summary.parsed,
        summary.skipped,
        summary.seconds,
    )
    return summary


def warm_parse_cache_if_enabled() -> WarmSummary | None:
    """Warm the cache when PARSE_CACHE_PRELOAD is on."""
    if not get_settings().parse_cache_preload:
        return None
    return warm_parse_cache_sync()
