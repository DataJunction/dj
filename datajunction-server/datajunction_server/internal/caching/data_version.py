"""
Versions of the data that cached GraphQL responses were built from.

A response is stored under the current version, so changing the version makes
every response stored before it unreachable. The version lives in the shared
cache, and a session that commits a write replaces it.
"""

import logging
import uuid
from collections.abc import Callable

from sqlalchemy import event, inspect
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.internal.caching.interface import CacheInterface
from datajunction_server.internal.history import EntityType

logger = logging.getLogger(__name__)

DATA_KEY = "graphql:data_version"
# Availability is updated thousands of times a day, so it has a version of its
# own, read only by queries that mention it.
AVAILABILITY_KEY = "graphql:availability_version"
# An expired version only makes stored responses unreachable.
AVAILABILITY_TABLES = {"availabilitystate", "nodeavailabilitystate"}
VERSION_TTL = 30 * 24 * 3600


def read_data_version(cache: CacheInterface, query: str) -> str | None:
    """
    The version a response to ``query`` would be stored under, or None if one of
    the versions was missing. A missing version is created for the next request,
    and this one is not cached: its data may predate a write whose version
    was lost.
    """
    keys = [DATA_KEY, *([AVAILABILITY_KEY] if "availability" in query else [])]
    versions = [cache.get(key) for key in keys]
    for key, version in zip(keys, versions):
        if version is None:
            cache.set(key, uuid.uuid4().hex, timeout=VERSION_TTL)
    return None if None in versions else ":".join(versions)  # type: ignore[arg-type]


def is_availability_change(obj) -> bool:
    """
    Whether a row only records availability: the availability tables, its history
    events, and a node revision whose only change is the availability it points
    to.
    """
    table = obj.__table__.name
    if table in AVAILABILITY_TABLES:
        return True
    if table == "history":
        return obj.entity_type == EntityType.AVAILABILITY
    if table == "noderevision":
        changed = {
            attr.key for attr in inspect(obj).attrs if attr.history.has_changes()
        }
        return changed <= {"availability", "availability_id"}
    return False


def invalidate_on_commit(
    session: AsyncSession,
    get_cache: Callable[[], CacheInterface],
) -> None:
    """
    Replace the versions a write touched once the session commits it.

    The version changes after the commit, so a response built from data that
    predates it is stored under a version no one reads again. The cache is only
    looked up when something is committed.
    """
    changed: set[str] = session.info.setdefault("changed_versions", set())

    def key_for(obj) -> str:
        return AVAILABILITY_KEY if is_availability_change(obj) else DATA_KEY

    @event.listens_for(session.sync_session, "after_flush")
    def flushed(sync_session, _flush_context):
        for obj in (*sync_session.new, *sync_session.dirty, *sync_session.deleted):
            changed.add(key_for(obj))

    @event.listens_for(session.sync_session, "do_orm_execute")
    def executed(state):
        # Bulk statements change rows without going through a flush.
        if state.statement.is_dml:
            changed.add(
                AVAILABILITY_KEY
                if state.statement.table.name in AVAILABILITY_TABLES
                else DATA_KEY,
            )

    @event.listens_for(session.sync_session, "after_commit")
    def committed(_sync_session):
        keys = list(changed)
        changed.clear()
        if not keys:
            return
        try:
            cache = get_cache()
            for key in keys:
                cache.set(key, uuid.uuid4().hex, timeout=VERSION_TTL)
        except Exception:
            logger.exception("Could not replace versions %s", keys)

    @event.listens_for(session.sync_session, "after_rollback")
    def rolled_back(_sync_session):
        changed.clear()
