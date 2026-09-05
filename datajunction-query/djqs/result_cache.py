"""
Stale-while-revalidate caching for completed query results.
"""

import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any

from cachelib.base import BaseCache

from djqs.constants import SQLALCHEMY_URI
from djqs.models.query import QueryCreate, QueryResults

DEFAULT_FRESHNESS_SECONDS = 12 * 60 * 60
DEFAULT_RETENTION_SECONDS = 24 * 60 * 60
MAX_RETENTION_SECONDS = 7 * 24 * 60 * 60


@dataclass
class CachedQueryResult:
    """A successful query result together with its freshness timestamp."""

    result: QueryResults
    created_at: float


def parse_cache_control(cache_control: str | None) -> tuple[bool, bool, int]:
    """
    Return ``(no_cache, no_store, retention_seconds)`` for Cache-Control.

    ``max-age`` extends or shortens result retention only. It never changes the
    shared freshness window, which prevents one caller from making a shared
    result appear fresh indefinitely.
    """
    directives = [
        directive.strip().lower() for directive in (cache_control or "").split(",")
    ]
    no_cache = "no-cache" in directives
    no_store = "no-store" in directives
    retention = DEFAULT_RETENTION_SECONDS
    for directive in directives:
        name, separator, value = directive.partition("=")
        if name != "max-age" or not separator:
            continue
        try:
            candidate = int(value.strip().strip('"'))
        except ValueError:
            continue
        if candidate > 0:
            retention = min(candidate, MAX_RETENTION_SECONDS)
            break
    return no_cache, no_store, retention


def has_stale_while_revalidate(cache_control: str | None) -> bool:
    """Return whether the caller explicitly opts into result SWR."""
    return any(
        directive.strip().lower().partition("=")[0] == "stale-while-revalidate"
        for directive in (cache_control or "").split(",")
    )


def build_result_cache_key(
    query: QueryCreate,
    headers: dict[str, str] | None = None,
) -> str:
    """Build a stable key for a query's execution-affecting inputs."""
    sqlalchemy_uri = next(
        (
            value
            for name, value in (headers or {}).items()
            if name.lower() == SQLALCHEMY_URI.lower()
        ),
        None,
    )
    payload = json.dumps(
        {
            "catalog_name": query.catalog_name,
            "engine_name": query.engine_name,
            "engine_version": query.engine_version,
            "submitted_query": query.submitted_query,
            "sqlalchemy_uri": sqlalchemy_uri,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return f"query-results:{hashlib.sha256(payload.encode()).hexdigest()}"


def get_cached_result(
    cache: BaseCache,
    key: str,
) -> tuple[QueryResults | None, bool]:
    """Return a cached result and whether it is still within freshness."""
    cached: Any = cache.get(key)
    if not isinstance(cached, CachedQueryResult):
        return None, False
    return cached.result, (time.time() - cached.created_at) < DEFAULT_FRESHNESS_SECONDS
