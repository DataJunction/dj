"""
Configuration for the query service
"""

import json
from datetime import timedelta
from typing import Optional

import toml
import yaml
from cachelib.base import BaseCache
from cachelib.file import FileSystemCache
from pydantic import BaseSettings
from sqlmodel import Session, delete, select

from djqs.exceptions import DJException
from djqs.models.catalog import Catalog, CatalogEngines
from djqs.models.engine import Engine


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

    # Configuration file for catalogs and engines
    configuration_file: Optional[str] = None

    # Enable setting catalog and engine config via REST API calls
    enable_dynamic_config: bool = True


def load_djqs_config(settings: Settings, session: Session) -> None:  # pragma: no cover
    """
    Load the DJQS config file into the server metadata database
    """
    config_file = settings.configuration_file if settings.configuration_file else None
    if not config_file:
        return

    session.exec(delete(Catalog))
    session.exec(delete(Engine))
    session.exec(delete(CatalogEngines))
    session.commit()

    with open(config_file, mode="r", encoding="utf-8") as filestream:

        def unknown_filetype():
            raise DJException(message=f"Unknown config file type: {config_file}")

        data = (
            yaml.safe_load(filestream)
            if any([config_file.endswith(".yml"), config_file.endswith(".yaml")])
            else toml.load(filestream)
            if config_file.endswith(".toml")
            else json.load(filestream)
            if config_file.endswith(".json")
            else unknown_filetype()
        )

    for engine in data["engines"]:
        session.add(Engine.parse_obj(engine))
    session.commit()

    for catalog in data["catalogs"]:
        attached_engines = []
        catalog_engines = catalog.pop("engines")
        for name in catalog_engines:
            attached_engines.append(
                session.exec(select(Engine).where(Engine.name == name)).one(),
            )
        catalog_entry = Catalog.parse_obj(catalog)
        catalog_entry.engines = attached_engines
        session.add(catalog_entry)
    session.commit()
