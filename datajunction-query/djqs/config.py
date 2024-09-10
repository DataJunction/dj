"""
Configuration for the query service
"""

import json
import logging
import os
from datetime import timedelta
from enum import Enum
from typing import Dict, List, Optional

import toml
import yaml
from cachelib.base import BaseCache
from cachelib.file import FileSystemCache

from djqs.exceptions import DJUnknownCatalog, DJUnknownEngine

_logger = logging.getLogger(__name__)


class EngineType(Enum):
    """
    Supported engine types
    """

    DUCKDB = "duckdb"
    SQLALCHEMY = "sqlalchemy"
    SNOWFLAKE = "snowflake"
    TRINO = "trino"


class EngineInfo:  # pylint: disable=too-few-public-methods
    """
    Information about a query engine
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        name: str,
        version: str,
        type: str,  # pylint: disable=redefined-builtin
        uri: str,
        extra_params: Optional[Dict[str, str]] = None,
    ):
        self.name = name
        self.version = str(version)
        self.type = EngineType(type)
        self.uri = uri
        self.extra_params = extra_params or {}


class CatalogInfo:  # pylint: disable=too-few-public-methods
    """
    Information about a catalog
    """

    def __init__(self, name: str, engines: List[str]):
        self.name = name
        self.engines = [engine for engine in engines]


class Settings:  # pylint: disable=too-many-instance-attributes
    """
    Configuration for the query service
    """

    def __init__(
        self,
        name: Optional[str] = "DJQS",
        description: Optional[str] = "A DataJunction Query Service",
        url: Optional[str] = "http://localhost:8001/",
        index: Optional[str] = "postgresql://dj:dj@postgres_metadata:5432/djqs",
        alembic_uri: Optional[
            str
        ] = "postgresql+psycopg://dj:dj@postgres_metadata:5432/djqs",
        default_engine: Optional[str] = "default",
        default_engine_version: Optional[str] = "",
        results_backend: Optional[BaseCache] = None,
        results_backend_path: Optional[str] = "/tmp/djqs",
        results_backend_timeout: Optional[BaseCache] = "0",
        paginating_timeout_minutes: Optional[str] = "5",
        do_ping_timeout_seconds: Optional[timedelta] = "5",
        configuration_file: Optional[str] = None,
        engines: Optional[List[EngineInfo]] = [],
        catalogs: Optional[List[CatalogInfo]] = [],
    ):
        self.name: str = os.getenv("NAME", name)
        self.description: str = os.getenv("DESCRIPTION", description)
        self.url: str = os.getenv("URL", url)

        # SQLAlchemy URI for the metadata database.
        self.index: str = os.getenv(
            "INDEX",
            index,
        )

        self.alembic_uri: str = os.getenv(
            "ALEMBIC_URI",
            alembic_uri,
        )
        # The default engine to use for reflection
        self.default_engine: str = os.getenv("DEFAULT_ENGINE", default_engine)

        # The default engine version to use for reflection
        self.default_engine_version: str = os.getenv(
            "DEFAULT_ENGINE_VERSION", default_engine_version,
        )

        # Where to store the results from queries.
        self.results_backend: BaseCache = results_backend or FileSystemCache(
            os.getenv("RESULTS_BACKEND_PATH", results_backend_path),
            default_timeout=int(
                os.getenv("RESULTS_BACKEND_TIMEOUT", results_backend_timeout),
            ),
        )

        self.paginating_timeout: timedelta = timedelta(
            minutes=int(
                os.getenv("PAGINATING_TIMEOUT_MINUTES", paginating_timeout_minutes),
            ),
        )

        # How long to wait when pinging databases to find out the fastest online database.
        self.do_ping_timeout: timedelta = timedelta(
            seconds=int(os.getenv("DO_PING_TIMEOUT_SECONDS", do_ping_timeout_seconds)),
        )

        # Configuration file for catalogs and engines
        self.configuration_file: Optional[str] = (
            os.getenv("CONFIGURATION_FILE") or configuration_file
        )

        self.engines: List[EngineInfo] = engines
        self.catalogs: List[CatalogInfo] = catalogs

        self._load_configuration()

    def _load_configuration(self):
        config_file = self.configuration_file

        if config_file:
            if config_file.endswith(".yaml") or config_file.endswith(".yml"):
                with open(config_file, "r", encoding="utf-8") as file:
                    config = yaml.safe_load(file)
            elif config_file.endswith(".toml"):
                with open(config_file, "r", encoding="utf-8") as file:
                    config = toml.load(file)
            elif config_file.endswith(".json"):
                with open(config_file, "r", encoding="utf-8") as file:
                    config = json.load(file)
            else:
                raise ValueError(
                    f"Unsupported configuration file format: {config_file}",
                )

            self.engines = [
                EngineInfo(**engine) for engine in config.get("engines", [])
            ]
            self.catalogs = [
                CatalogInfo(**catalog) for catalog in config.get("catalogs", [])
            ]
        else:
            _logger.warn("No settings configuration file has been set")

    def find_engine(
        self,
        engine_name: str,
        engine_version: str,
    ) -> EngineInfo:
        """
        Find an engine defined in the server configuration
        """
        found_engine = None
        for engine in self.engines:
            if engine.name == engine_name and engine.version == engine_version:
                found_engine = engine
        if not found_engine:
            raise DJUnknownEngine(
                (
                    f"Configuration error, cannot find engine {engine_name} "
                    f"with version {engine_version}"
                ),
            )
        return found_engine

    def find_catalog(self, catalog_name: str) -> CatalogInfo:
        """
        Find a catalog defined in the server configuration
        """
        found_catalog = None
        for catalog in self.catalogs:
            if catalog.name == catalog_name:
                found_catalog = catalog
        if not found_catalog:
            raise DJUnknownCatalog(
                f"Configuration error, cannot find catalog {catalog_name}",
            )
        return found_catalog
