"""
Configuration for the query service
"""

import os
from datetime import timedelta
from typing import Optional

from cachelib.base import BaseCache
from cachelib.file import FileSystemCache


class Settings:  # pylint: disable=too-few-public-methods
    """
    Configuration for the query service
    """

    def __init__(self):
        self.name: str = os.getenv("NAME", "DJQS")
        self.description: str = os.getenv("DESCRIPTION", "A DataJunction Query Service")
        self.url: str = os.getenv("URL", "http://localhost:8001/")
        
        # SQLAlchemy URI for the metadata database.
        self.index: str = os.getenv("INDEX", "postgresql+psycopg://dj:dj@postgres_metadata:5432/djqs")
        
        # The default engine to use for reflection
        self.default_reflection_engine: str = os.getenv("DEFAULT_REFLECTION_ENGINE", "default")
        
        # The default engine version to use for reflection
        self.default_reflection_engine_version: str = os.getenv("DEFAULT_REFLECTION_ENGINE_VERSION", "")
        
        # Where to store the results from queries.
        self.results_backend: BaseCache = FileSystemCache(
            os.getenv("RESULTS_BACKEND_PATH", "/tmp/djqs"), 
            default_timeout=int(os.getenv("RESULTS_BACKEND_TIMEOUT", 0))
        )
        
        self.paginating_timeout: timedelta = timedelta(minutes=int(os.getenv("PAGINATING_TIMEOUT_MINUTES", 5)))
        
        # How long to wait when pinging databases to find out the fastest online database.
        self.do_ping_timeout: timedelta = timedelta(seconds=int(os.getenv("DO_PING_TIMEOUT_SECONDS", 5)))
        
        # Configuration file for catalogs and engines
        self.configuration_file: Optional[str] = os.getenv("CONFIGURATION_FILE", None)
        
        # Enable setting catalog and engine config via REST API calls
        self.enable_dynamic_config: bool = os.getenv("ENABLE_DYNAMIC_CONFIG", "true").lower() in ["true", "1", "yes"]
