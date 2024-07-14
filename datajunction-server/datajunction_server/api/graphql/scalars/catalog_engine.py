"""Catalog/engine related scalars"""

from typing import List, Optional

import strawberry

from datajunction_server.models.node import Dialect as Dialect_

Dialect = strawberry.enum(Dialect_)


@strawberry.type
class Engine:  # pylint: disable=too-few-public-methods
    """
    Database engine
    """

    name: str
    version: str
    uri: Optional[str]
    dialect: Optional[Dialect]  # type: ignore


@strawberry.type
class Catalog:  # pylint: disable=too-few-public-methods
    """
    Catalog
    """

    name: str
    engines: Optional[List[Engine]]
