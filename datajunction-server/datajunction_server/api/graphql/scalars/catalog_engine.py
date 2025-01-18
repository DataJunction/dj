"""Catalog/engine related scalars"""

import strawberry

from datajunction_server.models.catalog import CatalogInfo
from datajunction_server.models.engine import EngineInfo
from datajunction_server.models.node import Dialect as Dialect_

Dialect = strawberry.enum(Dialect_)


@strawberry.experimental.pydantic.type(model=EngineInfo, all_fields=True)
class Engine:  # pylint: disable=too-few-public-methods
    """
    Database engine
    """


@strawberry.experimental.pydantic.type(model=CatalogInfo, all_fields=True)
class Catalog:  # pylint: disable=too-few-public-methods
    """
    Class for a Catalog
    """
