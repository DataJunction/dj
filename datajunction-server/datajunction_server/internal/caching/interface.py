"""
Caching interface and logging wrapper
"""

import logging
from abc import ABC, abstractmethod
from typing import Any


class CacheInterface(ABC):
    """Cache interface"""

    @abstractmethod
    def get(self, key: str) -> Any | None:
        """Get a cached value"""

    @abstractmethod
    def set(self, key: str, value: Any, timeout: int = 300) -> None:
        """Cache a value"""

    @abstractmethod
    def delete(self, key: str) -> None:
        """Delete a cache key"""

    def try_lock(self, key: str, lease: int) -> bool:
        """
        Take an exclusive lock on ``key`` for up to ``lease`` seconds without
        waiting, and return whether it was taken. A cache with no locks always
        grants it.
        """
        return True

    def unlock(self, key: str) -> None:
        """Release a lock taken with ``try_lock``"""


class Cache(CacheInterface):
    """A wrapper for the cache interface to ensure standardized logging"""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def get(self, key: str) -> Any | None:  # type: ignore
        """Log the cache check and then use the implemented cache"""
        self.logger.info(
            "%s: Getting cached value for key %s",
            self.__class__.__name__,
            key,
        )

    def set(self, key: str, value: Any, timeout: int = 300) -> None:
        """Log the cache attempt and then use the implemented cache"""
        self.logger.info(
            "%s: Setting value for key %s with timeout: %s",
            self.__class__.__name__,
            key,
            timeout,
        )

    def delete(self, key: str) -> None:
        """Log the cache deletion attempt and then use the implemented cache"""
        self.logger.info("%s: Deleting key %s", self.__class__.__name__, key)
