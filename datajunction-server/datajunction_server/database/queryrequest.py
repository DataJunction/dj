"""Partition database schema."""
from typing import Any, Dict, List, Optional

from sqlalchemy import JSON, BigInteger, Enum, Index, and_, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from datajunction_server.construction.utils import to_namespaced_name
from datajunction_server.database import Node
from datajunction_server.database.base import Base
from datajunction_server.enum import StrEnum
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse


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


class QueryRequest(Base):  # type: ignore  # pylint: disable=too-few-public-methods
    """
    A query request represents a request for DJ to build a query.

    There are three types of query building patterns that DJ supports:
    * Metrics SQL: query by metrics, dimensions, filters, ...
    * Measures SQL: query by metrics, dimensions, filters, ...
    * Node SQL: query by node name, dimensions, filters, ...
    """

    __tablename__ = "queryrequest"

    id: Mapped[int] = mapped_column(
        BigInteger(),
        primary_key=True,
    )

    query_type: Mapped[str] = mapped_column(Enum(QueryBuildType))

    nodes: Mapped[List[str]] = mapped_column(JSONB, nullable=True)
    dimensions: Mapped[List[str]] = mapped_column(JSONB, nullable=True)
    filters: Mapped[List[str]] = mapped_column(JSONB, nullable=True)
    limit: Mapped[Optional[int]]
    orderby: Mapped[List[str]] = mapped_column(JSONB, nullable=True)
    engine_name: Mapped[Optional[str]]
    engine_version: Mapped[Optional[str]]
    other_args: Mapped[JSON] = mapped_column(JSONB, nullable=True)

    # Values needed to rebuild a TranslatedSQL object
    query: Mapped[str]
    columns: Mapped[JSON] = mapped_column(JSONB, nullable=True)

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
        )
        print("versioned_request", versioned_request)
        statement = select(cls).where(
            and_(
                cls.query_type == query_type,
                cls.nodes == versioned_request["nodes"],
                cls.dimensions == versioned_request["dimensions"],
                cls.filters == versioned_request["filters"],
                cls.engine_name == engine_name,
                cls.engine_version == engine_version,
                cls.limit == limit,
                cls.orderby == versioned_request["orderby"],
                cls.other_args == other_args,
            ),
        )
        query_request = (await session.execute(statement)).scalar_one_or_none()
        if query_request:
            return query_request
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
    ):
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
        )
        if query_request:
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
            )
            query_request = QueryRequest(
                query_type=query_type,
                nodes=versioned_request["nodes"],
                dimensions=versioned_request["dimensions"],
                filters=versioned_request["filters"],
                engine_name=engine_name,
                engine_version=engine_version,
                limit=limit,
                orderby=versioned_request["orderby"],
                query=query,
                columns=columns,
                other_args=other_args,
            )
            session.add(query_request)
            await session.commit()

    @classmethod
    async def to_versioned_query_request(
        cls,
        session: AsyncSession,
        nodes: List[str],
        dimensions: List[str],
        filters: List[str],
        orderby: List[str],
    ) -> Dict[str, List[str]]:
        """
        Prepare for searching in saved query requests by appending version numbers to all nodes
        being worked with, from the nodes we're retrieving the queries of to the
        """
        nodes = [await Node.get_by_name(session, node, options=[]) for node in nodes]
        dimension_nodes = [
            await Node.get_by_name(session, ".".join(dim.split(".")[:-1]), options=[])
            for dim in dimensions
        ]
        filter_asts = {
            filter_: parse(f"SELECT 1 WHERE {filter_}") for filter_ in filters
        }
        for filter_ in filter_asts:
            for col in filter_asts[filter_].select.where.find_all(ast.Column):
                dimension_node = await Node.get_by_name(
                    session,
                    col.alias_or_name.namespace.identifier(),
                    options=[],
                )
                col.alias_or_name.name = to_namespaced_name(
                    f"{col.alias_or_name.name}@{dimension_node.current_version}",
                )

        orders = []
        for idx, order in enumerate(orderby):
            order_rule = order.split(" ")
            metric = await Node.get_by_name(session, order_rule[0], options=[])
            if metric:
                order_rule[0] = f"{metric.name}@{metric.current_version}"
                orders.append(" ".join(order_rule))
            else:
                node = await Node.get_by_name(
                    session,
                    ".".join(order_rule[0].split(".")[:-1]),
                    options=[],
                )
                order_rule[0] = f"{order_rule[0]}@{node.current_version}"
                orders.append(" ".join(order_rule))
                print("ORDERSS", node.name, node.current_version, orders)
        return {
            "nodes": [f"{node.name}@{node.current_version}" for node in nodes],
            "dimensions": [
                f"{dim}@{node.current_version}"
                for node, dim in zip(dimension_nodes, dimensions)
            ],
            "filters": [
                str(filter_ast.select.where) for filter_ast in filter_asts.values()
            ],
            "orderby": orders,
        }


Index(
    "query_request_unique",
    QueryRequest.query_type,
    QueryRequest.nodes,
    QueryRequest.dimensions,
    QueryRequest.filters,
    QueryRequest.engine_name,
    QueryRequest.engine_version,
    QueryRequest.limit,
    QueryRequest.orderby,
    unique=True,
)
