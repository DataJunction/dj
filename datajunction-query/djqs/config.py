"""
Configuration for the query service
"""

from enum import Enum
import os
from datetime import timedelta
from typing import Optional, List, Dict
import yaml
import toml
import json
from cachelib.base import BaseCache
from cachelib.file import FileSystemCache


class EngineType(Enum):
    """
    Supported engine types
    """

    DUCKDB = "duckdb"
    SQLALCHEMY = "sqlalchemy"
    SNOWFLAKE = "snowflake"
    TRINO = "trino"

class EngineInfo:
    def __init__(self, name: str, version: str, type: str, uri: str, extra_params: Optional[Dict[str, str]] = None):
        self.name = name
        self.version = version
        self.type = EngineType(type)
        self.uri = uri
        self.extra_params = extra_params or {}

class CatalogInfo:
    def __init__(self, name: str, engines: List[str]):
        self.name = name
        self.engines = [EngineType(engine) for engine in engines]

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

        self.engines: List[EngineInfo] = []
        self.catalogs: List[CatalogInfo] = []

        self._load_configuration()

    def _load_configuration(self):
        config_file = self.configuration_file
        self.configuration_file = config_file

        if config_file.endswith('.yaml') or config_file.endswith('.yml'):
            with open(config_file, 'r') as file:
                config = yaml.safe_load(file)
        elif config_file.endswith('.toml'):
            with open(config_file, 'r') as file:
                config = toml.load(file)
        elif config_file.endswith('.json'):
            with open(config_file, 'r') as file:
                config = json.load(file)
        else:
            raise ValueError(f"Unsupported configuration file format: {config_file}")

        self.engines = [EngineInfo(**engine) for engine in config.get('engines', [])]
        self.catalogs = [CatalogInfo(**catalog) for catalog in config.get('catalogs', [])]

    def find_engine(self, engine_name: str, engine_version: str) -> Optional[EngineInfo]:
        for engine in self.engines:
            if engine.name == engine_name and engine.version == engine_version:
                return engine
        return None

    def find_catalog(self, catalog_name: str) -> Optional[CatalogInfo]:
        for catalog in self.catalogs:
            if catalog.name == catalog_name:
                return catalog
        return None