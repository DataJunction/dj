"""
Caching interface and logging wrapper
"""
import logging
from abc import ABC, abstractmethod
from typing import Any, Optional


class CacheInterface(ABC):
    """Cache interface"""

    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        """Get a cached value"""

    @abstractmethod
    def set(self, key: str, value: Any, timeout: int = 300) -> None:
        """Cache a value"""


class Cache(CacheInterface):
    """A wrapper for the cache interface to ensure standardized logging"""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def get(self, key: str) -> Optional[Any]:
        """Log the cache check and then use the implemented cache"""
        self.logger.info("Getting cached value for key: %s", key)
        return super().get(key)

    def set(self, key: str, value: Any, timeout: int = 300) -> None:
        """Log the cache attempt and then use the implemented cache"""
        self.logger.info("Setting value for key: %s with timeout: %s", key, timeout)
        super().set(key, value, timeout)
