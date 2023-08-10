"""
Configuration for the query service
"""

from datetime import timedelta

from cachelib.base import BaseCache
from cachelib.file import FileSystemCache
from pydantic import BaseSettings


class Settings(BaseSettings):  # pylint: disable=too-few-public-methods
    """
    Configuration for the query service
    """

    name: str = "DJQS"
    description: str = "A DataJunction Query Service"
    url: str = "http://localhost:8001/"

    # SQLAlchemy URI for the metadata database.
    index: str = "sqlite:///djqs.db?check_same_thread=False"

    # The default engine to use for reflection
    default_reflection_engine: str = "default"

    # The default engine version to use for reflection
    default_reflection_engine_version: str = ""

    # Where to store the results from queries.
    results_backend: BaseCache = FileSystemCache("/tmp/djqs", default_timeout=0)

    paginating_timeout: timedelta = timedelta(minutes=5)

    # How long to wait when pinging databases to find out the fastest online database.
    do_ping_timeout: timedelta = timedelta(seconds=5)
