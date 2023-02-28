"""
Configuration for the metric repository.
"""

from datetime import timedelta
from pathlib import Path

from cachelib.base import BaseCache
from cachelib.file import FileSystemCache
from pydantic import BaseSettings


class Settings(BaseSettings):  # pylint: disable=too-few-public-methods
    """
    Configuration for the metric repository.
    """

    name: str = "DJ query server"
    description: str = "A DataJunction metrics repository"
    url: str = "http://localhost:8001/"

    # SQLAlchemy URI for the metadata database.
    index: str = "sqlite:///djqs.db?check_same_thread=False"

    # Directory where the repository lives. This should have 2 subdirectories, "nodes" and
    # "databases".
    repository: Path = Path(".")

    # Where to store the results from queries.
    results_backend: BaseCache = FileSystemCache("/tmp/djqs", default_timeout=0)

    paginating_timeout: timedelta = timedelta(minutes=5)

    # How long to wait when pinging databases to find out the fastest online database.
    do_ping_timeout: timedelta = timedelta(seconds=5)
