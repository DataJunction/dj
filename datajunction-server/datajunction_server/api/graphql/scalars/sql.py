"""SQL-related scalars."""

from functools import cached_property

import strawberry
from strawberry.types import Info

from datajunction_server.api.graphql.resolvers.nodes import get_node_by_name
from datajunction_server.api.graphql.scalars.errors import DJError
from datajunction_server.api.graphql.scalars.node import Node
from datajunction_server.api.graphql.utils import extract_fields
from datajunction_server.database.queryrequest import QueryBuildType as QueryBuildType_
from datajunction_server.models.column import SemanticType as SemanticType_
from datajunction_server.models.engine import Dialect as Dialect_
from datajunction_server.models.sql import GeneratedSQL as GeneratedSQL_
from datajunction_server.utils import SEPARATOR

SemanticType = strawberry.enum(SemanticType_)
Dialect = strawberry.enum(Dialect_)
QueryBuildType = strawberry.enum(QueryBuildType_)


@strawberry.type
class SemanticEntity:
    """
    Column metadata for generated SQL
    """

    name: str

    @cached_property
    def _split_name(self) -> list[str]:
        """
        Private, cached property that splits the name into node and column parts.
        """
        return self.name.rsplit(SEPARATOR, 1)

    @strawberry.field(description="The node this semantic entity is sourced from")
    async def node(self) -> str:
        """
        Returns the node name that this semantic entity is sourced from
        """
        return self._split_name[0]

    @strawberry.field(
        description="The column on the node this semantic entity is sourced from",
    )
    async def column(self) -> str:
        """
        Returns the column on the node this semantic entity is sourced from
        """
        return self._split_name[1]


@strawberry.type
class ColumnMetadata:
    """
    Column metadata for generated SQL
    """

    name: str
    type: str
    semantic_entity: SemanticEntity | None
    semantic_type: SemanticType | None  # type: ignore


@strawberry.type
class GeneratedSQL:
    """
    Generated SQL for a given node
    """

    node: Node
    sql: str
    columns: list[ColumnMetadata]
    dialect: Dialect  # type: ignore
    upstream_tables: list[str]
    errors: list[DJError]

    @classmethod
    async def from_pydantic(cls, info: Info, obj: GeneratedSQL_):
        """
        Loads a strawberry GeneratedSQL from the original pydantic model.
        """
        fields = extract_fields(info)
        return GeneratedSQL(  # type: ignore
            node=await get_node_by_name(
                session=info.context["session"],
                fields=fields.get("node"),
                name=obj.node.name,
            ),
            sql=obj.sql,
            columns=[
                ColumnMetadata(  # type: ignore
                    name=col.name,
                    type=col.type,
                    semantic_entity=SemanticEntity(name=col.semantic_entity),  # type: ignore
                    semantic_type=SemanticType(col.semantic_type),
                )
                for col in obj.columns  # type: ignore
            ],
            dialect=obj.dialect,
            upstream_tables=obj.upstream_tables,
            errors=obj.errors,
        )
