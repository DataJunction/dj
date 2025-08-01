"""Query request schema."""

from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial
from http import HTTPStatus
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    JSON,
    BigInteger,
    DateTime,
    Enum,
    UniqueConstraint,
    and_,
    select,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, joinedload, mapped_column, selectinload

from datajunction_server.construction.utils import to_namespaced_name
from datajunction_server.database.base import Base
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.enum import StrEnum
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.sql.dag import (
    get_dimensions,
    get_shared_dimensions,
    get_upstream_nodes,
)
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.typing import UTCDatetime


class QueryBuildType(StrEnum):
    """
    Query building type.

    There are three types of query building patterns that DJ supports:
    * Metrics SQL
    * Measures SQL
    * Node SQL
    """

    METRICS = "metrics"
    MEASURES = "measures"
    NODE = "node"


class QueryRequest(Base):  # type: ignore
    """
    A query request represents a request for DJ to build a query.

    This table consists of the request key (i.e., all the inputs that uniquely define a
    query building request) and the request results (the built query and columns metadata)
    """

    __tablename__ = "queryrequest"
    __table_args__ = (
        UniqueConstraint(
            "query_type",
            "nodes",
            "parents",
            "dimensions",
            "filters",
            "engine_name",
            "engine_version",
            "limit",
            "orderby",
            name="query_request_unique",
            postgresql_nulls_not_distinct=True,
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger(),
        primary_key=True,
    )

    # ------------- #
    #  Request key  #
    # ------------- #

    # The type of query (metrics, measures, node)
    query_type: Mapped[str] = mapped_column(Enum(QueryBuildType))

    # A list of the nodes that SQL is being requested for, with node versions. This may
    # be a list of metrics or a single transform, metric, or dimension node, depending
    # on the query type.
    nodes: Mapped[List[str]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'[]'::jsonb"),
    )

    # A list of all the parents of the nodes above, with node versions
    parents: Mapped[List[str]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'[]'::jsonb"),
    )

    # A list of dimension attributes requested, with node versions
    dimensions: Mapped[List[str]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'[]'::jsonb"),
    )

    # A list of filters requested, with node versions
    filters: Mapped[List[str]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'[]'::jsonb"),
    )

    # Limit set for the query (note: some query types don't have limit)
    limit: Mapped[Optional[int]]

    # The ORDER BY clause requested, if any
    orderby: Mapped[List[str]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'[]'::jsonb"),
    )

    # The engine this query was built for (if any was set)
    engine_name: Mapped[Optional[str]]
    engine_version: Mapped[Optional[str]]

    # Additional input args
    other_args: Mapped[JSON] = mapped_column(
        JSONB,
        nullable=False,
        default=text("'{}'::jsonb"),
        server_default=text("'{}'::jsonb"),
    )

    # --------------------------------------------------- #
    #  Request results                                    #
    #  (values needed to rebuild a TranslatedSQL object)  #
    # --------------------------------------------------- #
    query: Mapped[str]
    columns: Mapped[JSON] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'[]'::jsonb"),
    )

    # ---------- #
    #  Metadata  #
    # ---------- #
    created_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        default=partial(datetime.now, timezone.utc),
    )
    updated_at: Mapped[UTCDatetime] = mapped_column(
        DateTime(timezone=True),
        default=partial(datetime.now, timezone.utc),
    )
    # External identifier for the query
    query_id: Mapped[Optional[str]]

    @classmethod
    async def get_query_request(
        cls,
        session: AsyncSession,
        query_type: QueryBuildType,
        nodes: List[str],
        dimensions: List[str],
        filters: List[str],
        engine_name: Optional[str],
        engine_version: Optional[str],
        limit: Optional[int],
        orderby: List[str],
        other_args: Optional[Dict[str, Any]] = None,
    ) -> Optional["QueryRequest"]:
        """
        Retrieves saved query for a node SQL request
        """
        versioned_request = await cls.to_versioned_query_request(
            session,
            nodes,
            dimensions,
            filters,
            orderby,
            query_type,
        )
        statement = select(cls).where(
            and_(
                cls.query_type == query_type,
                cls.nodes == (versioned_request["nodes"] or text("'[]'::jsonb")),
                cls.parents == (versioned_request["parents"] or text("'[]'::jsonb")),
                cls.dimensions
                == (versioned_request["dimensions"] or text("'[]'::jsonb")),
                cls.filters == (versioned_request["filters"] or text("'[]'::jsonb")),
                cls.engine_name == engine_name,
                cls.engine_version == engine_version,
                cls.limit == limit,
                cls.orderby == (versioned_request["orderby"] or text("'[]'::jsonb")),
                cls.other_args == (other_args or text("'{}'::jsonb")),
            ),
        )
        query_request = (await session.execute(statement)).scalar_one_or_none()
        if query_request:
            return query_request  # pragma: no cover
        return None

    @classmethod
    async def save_query_request(
        cls,
        session: AsyncSession,
        query_type: QueryBuildType,
        nodes: List[str],
        dimensions: List[str],
        filters: List[str],
        engine_name: Optional[str],
        engine_version: Optional[str],
        limit: Optional[int],
        orderby: List[str],
        query: str,
        columns: List[Dict[str, Any]],
        other_args: Optional[Dict[str, Any]] = None,
        save: bool = True,
    ) -> "QueryRequest":
        """
        Retrieves saved query for a node SQL request
        """
        query_request = await cls.get_query_request(
            session,
            query_type=query_type,
            nodes=nodes,
            dimensions=dimensions,
            filters=filters,
            engine_name=engine_name,
            engine_version=engine_version,
            limit=limit,
            orderby=orderby,
            other_args=other_args,
        )
        if query_request and save:  # pragma: no cover
            query_request.query = query
            query_request.columns = columns
            session.add(query_request)
            await session.commit()
        else:
            versioned_request = await cls.to_versioned_query_request(
                session,
                nodes,
                dimensions,
                filters,
                orderby,
                query_type,
            )
            query_request = QueryRequest(
                query_type=query_type,
                nodes=versioned_request["nodes"],
                parents=versioned_request["parents"],
                dimensions=versioned_request["dimensions"],
                filters=versioned_request["filters"],
                engine_name=engine_name,
                engine_version=engine_version,
                limit=limit,
                orderby=versioned_request["orderby"],
                query=query,
                columns=columns,
                other_args=other_args or text("'{}'::jsonb"),
            )
            if save:
                session.add(query_request)  # pragma: no cover
                await session.commit()  # pragma: no cover
        return query_request

    @classmethod
    async def to_versioned_query_request(
        cls,
        session: AsyncSession,
        nodes: List[str],
        dimensions: List[str],
        filters: List[str],
        orderby: List[str],
        query_type: QueryBuildType,
    ) -> Dict[str, List[str]]:
        """
        Prepare for searching in saved query requests by appending version numbers to all nodes
        being worked with.
        """
        nodes_objs = [
            await Node.get_by_name(
                session,
                node,
                options=[
                    joinedload(Node.current).options(
                        selectinload(NodeRevision.columns),
                        selectinload(NodeRevision.parents).options(
                            joinedload(Node.current),
                        ),
                    ),
                ],
                raise_if_not_exists=True,
            )
            for node in nodes
        ]

        if not nodes_objs and query_type in (
            QueryBuildType.MEASURES,
            QueryBuildType.METRICS,
        ):
            raise DJInvalidInputException(
                message="At least one metric is required",
                http_status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            )
        node_columns = []
        if len(nodes_objs) == 1:
            node_columns = [col.name for col in nodes_objs[0].current.columns]  # type: ignore
        available_dimensions = {
            dim.name
            for dim in (
                await get_dimensions(session, nodes_objs[0])  # type: ignore
                if len(nodes_objs) == 1
                else await get_shared_dimensions(session, nodes_objs)  # type: ignore
            )
        }.union(set(node_columns))
        invalid_dimensions = sorted(
            list(set(dimensions).difference(available_dimensions)),
        )
        if dimensions and invalid_dimensions:
            raise DJInvalidInputException(
                f"{', '.join(invalid_dimensions)} are not available "
                f"dimensions on {', '.join(nodes)}",
            )

        dimension_nodes = [
            await Node.get_by_name(session, ".".join(dim.split(".")[:-1]), options=[])
            for dim in dimensions
        ]
        filter_asts = {
            filter_: parse(f"SELECT 1 WHERE {filter_}")
            for filter_ in filters
            if filter_
        }
        for filter_ in filter_asts:
            for col in filter_asts[filter_].select.where.find_all(ast.Column):  # type: ignore
                if isinstance(col.parent, ast.Subscript):
                    if isinstance(col.parent.index, ast.Lambda):
                        col.role = str(col.parent.index)
                    else:
                        col.role = col.parent.index.identifier()  # type: ignore
                    col.parent.swap(col)
                dimension_node = await Node.get_by_name(
                    session,
                    ".".join(col.identifier().split(".")[:-1]),  # type: ignore
                    options=[],
                )
                if dimension_node:
                    col.alias_or_name.name = to_namespaced_name(
                        f"{col.alias_or_name.name}{'[' + col.role + ']' if col.role else ''}"
                        f"@{dimension_node.current_version}",  # type: ignore
                    )

        orders = []
        for order in orderby:
            order_rule = order.split(" ")
            metric = await Node.get_by_name(session, order_rule[0], options=[])
            if metric:
                order_rule[0] = f"{metric.name}@{metric.current_version}"
            else:
                node = await Node.get_by_name(
                    session,
                    ".".join(order_rule[0].split(".")[:-1]),
                    options=[],
                )
                order_rule[0] = f"{order_rule[0]}@{node.current_version}"  # type: ignore
            orders.append(" ".join(order_rule))

        parents = [
            upstream
            for node in nodes
            for upstream in await get_upstream_nodes(session, node)
        ]
        return {
            "nodes": [
                f"{node.name}@{node.current_version}"  # type: ignore
                for node in nodes_objs
            ],
            "parents": sorted(
                list(
                    {
                        f"{parent.name}@{parent.current_version}"  # type: ignore
                        for parent in parents
                    },
                ),
            ),
            "dimensions": [
                (
                    f"{dim}@{node.current_version}"
                    if node
                    else f"{dim}@{nodes_objs[0].current_version}"  # type: ignore
                )
                for node, dim in zip(dimension_nodes, dimensions)
            ],
            "filters": [
                str(filter_ast.select.where) for filter_ast in filter_asts.values()
            ],
            "orderby": orders,
        }


@dataclass(order=True)
class VersionedNodeKey:
    """
    A versioned key for a node, used to identify it in query requests.
    """

    name: str
    version: str | None = None

    def __str__(self) -> str:
        return f"{self.name}@{self.version}" if self.version else self.name

    def __eq__(self, value):
        if isinstance(value, VersionedNodeKey):
            return self.name == value.name and self.version == value.version
        elif isinstance(value, str):
            node_key = self.parse(value)
            return self.name == node_key.name and self.version == node_key.version
        return False

    def __hash__(self) -> int:
        return hash((self.name, self.version))

    @classmethod
    def from_node(cls, node: Node) -> "VersionedNodeKey":
        """
        Creates a versioned node key from a Node object.
        """
        return cls(name=node.name, version=node.current_version)

    @classmethod
    def parse(cls, node_key: str) -> "VersionedNodeKey":
        """
        Parses the versioned node key into a dictionary.
        """
        if "@" in node_key:
            node_key, version = node_key.split("@", 1)
            return VersionedNodeKey(name=node_key, version=version)
        return VersionedNodeKey(name=node_key, version=None)


@dataclass
class VersionedQueryKey:
    """
    A versioned query request encapsulates the logic to version nodes, dimensions,
    filters, and order by statements.
    """

    nodes: list[VersionedNodeKey]
    parents: list[VersionedNodeKey]
    dimensions: list[VersionedNodeKey]
    filters: list[str]
    orderby: list[str]

    @classmethod
    async def version_query_request(
        cls,
        session: AsyncSession,
        nodes: list[str],
        dimensions: list[str],
        filters: list[str],
        orderby: list[str],
    ) -> "VersionedQueryKey":
        """
        Versions a query request (e.g., nodes, dimensions, filters, and orderby).
        """
        versioned_nodes, versioned_parents = await cls.version_nodes(session, nodes)
        versioned_dims = await cls.version_dimensions(
            session,
            dimensions,
            current_node=versioned_nodes[0],
        )
        return VersionedQueryKey(
            nodes=versioned_nodes,
            parents=versioned_parents,
            dimensions=versioned_dims,
            filters=await cls.version_filters(session, filters),
            orderby=await cls.version_orderby(session, orderby),
        )

    @staticmethod
    async def version_nodes(
        session: AsyncSession,
        nodes: list[str],
    ) -> tuple[list[VersionedNodeKey], list[VersionedNodeKey]]:
        """
        Creates a versioned node key for each node in the list of nodes, and
        returns a list of versioned parents for the nodes.
        """
        nodes_objs = {
            node.name: node
            for node in await Node.get_by_names(
                session,
                nodes,
                options=[
                    joinedload(Node.current).options(
                        selectinload(NodeRevision.columns),
                        selectinload(NodeRevision.parents).options(
                            joinedload(Node.current),
                        ),
                    ),
                ],
            )
        }
        versioned_parents = sorted(
            {
                VersionedNodeKey.from_node(parent)
                for node in nodes_objs.values()
                for parent in node.current.parents
            },
        )
        versioned_nodes = [
            VersionedNodeKey.from_node(nodes_objs[node_name]) for node_name in nodes
        ]
        return versioned_nodes, versioned_parents

    @staticmethod
    async def version_dimensions(
        session: AsyncSession,
        dimensions: list[str],
        current_node: VersionedNodeKey | None = None,
    ) -> list[VersionedNodeKey]:
        """
        Versions the dimensions by creating a versioned node key for each dimension.
        """
        node_names = [".".join(dim.split(".")[:-1]) for dim in dimensions]
        dimension_nodes = {
            node.name: node
            for node in await Node.get_by_names(
                session,
                [".".join(dim.split(".")[:-1]) for dim in dimensions],
            )
        }
        return [
            VersionedNodeKey(
                dim,
                dimension_nodes[name].current_version
                if dimension_nodes[name]
                else current_node.version
                if current_node
                else None,
            )
            for dim, name in zip(dimensions, node_names)
        ]

    @staticmethod
    async def version_filters(session: AsyncSession, filters: list[str]) -> list[str]:
        """
        Versions the filters by parsing them and replacing dimension / metrics references
        with their versioned node keys.
        """
        results = []
        for filter_ in filters:
            if not filter_:
                continue  # pragma: no cover
            ast_tree = parse(f"SELECT 1 WHERE {filter_}")
            for col in ast_tree.select.where.find_all(ast.Column):  # type: ignore
                # Extract role if column is subscripted
                if isinstance(col.parent, ast.Subscript):
                    if isinstance(col.parent.index, ast.Lambda):
                        col.role = str(col.parent.index)  # pragma: no cover
                    else:
                        col.role = col.parent.index.identifier()  # type: ignore
                    col.parent.swap(col)

                # Try resolving as metric node first
                col_name = col.identifier()
                metric_node = await Node.get_by_name(
                    session,
                    col_name,
                    options=[],
                )
                if metric_node:
                    versioned_node_name = str(VersionedNodeKey.from_node(metric_node))
                    col.name = to_namespaced_name(versioned_node_name)
                else:
                    # Fallback to dimension node
                    dim_node_name = ".".join(col_name.split(".")[:-1])
                    dim_node = await Node.get_by_name(
                        session,
                        dim_node_name,
                        options=[],
                    )
                    if dim_node:
                        col.alias_or_name.name = to_namespaced_name(
                            f"{col.alias_or_name.name}"
                            f"{'[' + col.role + ']' if col.role else ''}"
                            f"@{dim_node.current_version}",
                        )
            results.append(str(ast_tree.select.where))
        return results

    @staticmethod
    async def version_orderby(session: AsyncSession, orderby: list[str]) -> list[str]:
        """
        This handles versioning two types of ORDER BY clauses:
        * dimension order bys: <dimension attribute> <ordering>
        * metric order bys: <metric node name> <ordering>
        """
        results = []
        for order in orderby:
            parts = order.split(" ")
            order_by_col = parts[0]
            order_by_metric_node = await Node.get_by_name(
                session,
                order_by_col,
                options=[],
            )
            if order_by_metric_node:
                # If it was a metric node in the order by clause, version the metric node
                parts[0] = str(VersionedNodeKey.from_node(order_by_metric_node))
            else:
                # Otherwise it is a dimension attribute
                versioned_dim = await VersionedQueryKey.version_dimensions(
                    session,
                    [order_by_col],
                )
                parts[0] = str(versioned_dim[0])
            results.append(" ".join(parts))
        return results


@dataclass
class QueryRequestKey:
    """
    A cacheable query request encapsulates the logic to build a cache key for a query request.
    """

    key: VersionedQueryKey
    query_type: QueryBuildType
    engine_name: str
    engine_version: str
    limit: int | None
    include_all_columns: bool
    preaggregate: bool
    use_materialized: bool
    query_parameters: dict
    other_args: dict
