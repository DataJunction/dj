from abc import ABC, abstractmethod
import hashlib
import json
import logging
from typing import Generic, Protocol, TypeVar
from fastapi import BackgroundTasks, Request

from datajunction_server.internal.caching.interface import Cache


class DataClassLike(Protocol):
    __dataclass_fields__: dict


ResultType = TypeVar("ResultType")
ParamsType = TypeVar("ParamsType", dict, DataClassLike)


class CacheManager(ABC, Generic[ParamsType, ResultType]):
    """
    A generic manager for handling caching operations.
    """

    _cache_key_prefix: str | None = None
    default_timeout: int = 3600  # Default cache timeout in seconds

    def __init__(self, cache: Cache):
        self.cache = cache
        self.logger = logging.getLogger(self.__class__.__name__)

    @property
    def cache_key_prefix(self):
        return self._cache_key_prefix or self.__class__.__name__.lower()

    @abstractmethod
    async def fallback(self, request: Request, params: ParamsType) -> ResultType:
        """
        The fallback function to call if the cache is not hit. This should be overridden
        in subclasses.
        """

    async def build_cache_key(self, request: Request, params: ParamsType) -> str:
        """
        Generic cache key function which sorts and hashes the context keys.
        """
        if hasattr(params, "__dataclass_fields__"):
            data = params.__dict__  # pragma: no cover
        elif isinstance(params, dict):
            data = params
        else:
            raise TypeError(f"Unsupported params type: {type(params)}")
        canonical = json.dumps(data, sort_keys=True)
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        return f"{self.cache_key_prefix}:{digest}"

    @abstractmethod
    async def get_or_load(
        self,
        background_tasks: BackgroundTasks,
        request: Request,
        params: ParamsType,
    ) -> ResultType:
        """
        Load value from cache if possible, otherwise compute via fallback. The behavior
        of this method may vary depending on caching strategy. It should always respect
        the Cache-Control headers in the request.
        """


class RefreshAheadCacheManager(CacheManager):
    """
    Cache manager implementing refresh-ahead caching.

    This strategy always serves the currently cached value immediately, regardless of whether
    it's stale, and then triggers a background refresh to update the cache with the latest data.
    """

    async def get_or_load(
        self,
        background_tasks: BackgroundTasks,
        request: Request,
        params: ParamsType,
    ) -> ResultType:
        """
        Load value from cache if possible, otherwise compute via fallback.
        Respects Cache-Control headers:
          - no-cache: does not use cache when present, always computes fresh value
          - no-store: skips storing fresh values into the cache
        """
        cache_control = request.headers.get("Cache-Control", "").lower()
        no_store = "no-store" in cache_control
        no_cache = "no-cache" in cache_control

        key: str = await self.build_cache_key(request, params)
        if not no_cache:
            if cached := self.cache.get(key):
                if not no_store:
                    background_tasks.add_task(self._refresh_cache, key, request, params)
                return cached
            self.logger.info(
                "Cache miss (key=%s) for request with parameters=%s, computing fresh value.",
                key,
                params,
            )
        else:
            self.logger.info(
                "no-cache header present for request with parameters=%s, computing fresh value.",
                params,
            )

        result = await self.fallback(request, params)

        if not no_store:
            background_tasks.add_task(
                self.cache.set,
                key,
                result,
                timeout=self.default_timeout,
            )

        return result

    async def _refresh_cache(
        self,
        key: str,
        request: Request,
        params: ParamsType,
    ) -> None:
        """
        Async cache refresher that re-runs fallback and updates the cache.
        """
        self.logger.info("Refreshing cache for key=%s", key)
        result = await self.fallback(request, params)
        self.cache.set(key, result, timeout=self.default_timeout)
        self.logger.info("Successfully refreshed cache for key=%s", key)
