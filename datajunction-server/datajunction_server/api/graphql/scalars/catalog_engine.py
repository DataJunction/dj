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


# @strawberry.type
# class Engine:  # pylint: disable=too-few-public-methods
#     """
#     Database engine
#     """

#     name: str
#     version: str
#     uri: Optional[str]
#     dialect: Optional[Dialect]  # type: ignore


# @strawberry.type
# class Catalog:  # pylint: disable=too-few-public-methods
#     """
#     Catalog
#     """

#     name: str
#     engines: Optional[List[Engine]]


@strawberry.experimental.pydantic.type(model=CatalogInfo, all_fields=True)
class Catalog:  # pylint: disable=too-few-public-methods
    """
    Class for a Catalog
    """
