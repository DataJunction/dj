"""
Configuration for the datajunction server.
"""

import urllib.parse
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional

from cachelib.base import BaseCache
from cachelib.file import FileSystemCache
from cachelib.redis import RedisCache
from celery import Celery
from pydantic import BaseModel, BaseSettings

if TYPE_CHECKING:
    pass


class DatabaseConfig(BaseModel):
    """
    Metadata database configuration.
    """

    uri: str
    pool_size: int = 20
    max_overflow: int = 100
    pool_timeout: int = 10
    connect_timeout: int = 5
    pool_pre_ping: bool = True
    echo: bool = False
    keepalives: int = 1
    keepalives_idle: int = 30
    keepalives_interval: int = 10
    keepalives_count: int = 5


class Settings(BaseSettings):  # pragma: no cover
    """
    DataJunction configuration.
    """

    class Config:
        env_nested_delimiter = "__"  # Enables nesting like WRITER_DB__URI

    name: str = "DJ server"
    description: str = "A DataJunction metrics layer"
    url: str = "http://localhost:8000/"

    # A list of hostnames that are allowed to make cross-site HTTP requests
    cors_origin_whitelist: List[str] = ["http://localhost:3000"]

    # Config for the metadata database, with support for writer and reader clusters
    # `writer_db` is the primary database used for write operations
    # [optional] `reader_db` is used for read operations and defaults to `writer_db`
    # if no dedicated read replica is configured.
    writer_db: DatabaseConfig = DatabaseConfig(
        uri="postgresql+psycopg://dj:dj@postgres_metadata:5432/dj",
    )
    reader_db: DatabaseConfig = writer_db

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

    # Query service
    query_service: Optional[str] = None

    # The namespace where source nodes for registered tables should exist
    source_node_namespace: Optional[str] = "source"

    # This specifies what the DJ_LOGICAL_TIMESTAMP() macro should be replaced with.
    # This defaults to an Airflow compatible value, but other examples include:
    #   ${dj_logical_timestamp}
    #   {{ dj_logical_timestamp }}
    #   $dj_logical_timestamp
    dj_logical_timestamp_format: Optional[str] = "${dj_logical_timestamp}"

    # DJ UI host, used for OAuth redirection
    frontend_host: Optional[str] = "http://localhost:3000"

    # Enabled transpilation plugin names
    transpilation_plugins: List[str] = ["default", "sqlglot"]

    # 128 bit DJ secret, used to encrypt passwords and JSON web tokens
    secret: str = "a-fake-secretkey"

    # GitHub OAuth application client ID
    github_oauth_client_id: Optional[str] = None

    # GitHub OAuth application client secret
    github_oauth_client_secret: Optional[str] = None

    # Google OAuth application client ID
    google_oauth_client_id: Optional[str] = None

    # Google OAuth application client secret
    google_oauth_client_secret: Optional[str] = None

    # Google OAuth application client secret file
    google_oauth_client_secret_file: Optional[str] = None

    # Interval in seconds with which to expire caching of any indexes
    index_cache_expire = 60

    default_catalog_id: int = 0

    # Maximum amount of nodes to return for requests to list all nodes
    node_list_max = 10000

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
