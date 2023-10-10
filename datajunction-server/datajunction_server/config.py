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
from pydantic import BaseSettings

if TYPE_CHECKING:
    from datajunction_server.models.access import AccessControl


class Settings(
    BaseSettings,
):  # pylint: disable=too-few-public-methods #pragma: no cover
    """
    DataJunction configuration.
    """

    name: str = "DJ server"
    description: str = "A DataJunction metrics layer"
    url: str = "http://localhost:8000/"

    # A list of hostnames that are allowed to make cross-site HTTP requests
    cors_origin_whitelist: List[str] = ["http://localhost:3000"]

    # SQLAlchemy URI for the metadata database.
    index: str = "sqlite:///dj.db?check_same_thread=False"

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

    # Library to use when transpiling SQL to other dialects
    sql_transpilation_library: Optional[str] = None

    # DJ secret, used to encrypt passwords and JSON web tokens
    secret: Optional[str] = None

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
