"""
Configuration for the metric repository.
"""

import urllib.parse
from datetime import timedelta
from pathlib import Path
from typing import Optional

from cachelib.base import BaseCache
from cachelib.file import FileSystemCache
from cachelib.redis import RedisCache
from celery import Celery
from pydantic import BaseSettings


class Settings(BaseSettings):  # pylint: disable=too-few-public-methods
    """
    Configuration for the metric repository.
    """

    name: str = "DJ server"
    description: str = "A DataJunction metrics repository"
    url: str = "http://localhost:8000/"

    # SQLAlchemy URI for the metadata database.
    index: str = "sqlite:///dj.db"

    # Directory where the repository lives. This should have 2 subdirectories, "nodes" and
    # "databases".
    repository: Path = Path(".")

    # Where to store the results from queries.
    results_backend: BaseCache = FileSystemCache("/tmp/dj", default_timeout=0)

    # Cache for paginating results and potentially other things.
    redis_cache: Optional[str] = None
    paginating_timeout: timedelta = timedelta(minutes=5)

    # Configure Celery for async requests. If not configured async queries will be
    # executed using FastAPI's ``BackgroundTasks``.
    celery_broker: Optional[str] = None

    # How long to wait when pinging databases to find out the fastest online database.
    do_ping_timeout: timedelta = timedelta(seconds=5)

    @property
    def celery(self) -> Celery:
        """
        Return Celery app.
        """
        return Celery(__name__, broker=self.celery_broker)

    @property
    def cache(self) -> Optional[BaseCache]:
        """
        Configure the Redis cache.
        """
        if self.redis_cache is None:
            return None

        parsed = urllib.parse.urlparse(self.redis_cache)
        return RedisCache(
            host=parsed.hostname,
            port=parsed.port,
            password=parsed.password,
            db=parsed.path.strip("/"),
        )
