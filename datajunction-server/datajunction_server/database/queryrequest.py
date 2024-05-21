"""Query request schema."""
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


class QueryRequest(Base):  # type: ignore  # pylint: disable=too-few-public-methods
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
        if query_request:  # pragma: no cover
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
            session.add(query_request)
            await session.commit()
        return query_request

    @classmethod
    async def to_versioned_query_request(  # pylint: disable=too-many-locals
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
        being worked with, from the nodes we're retrieving the queries of to the
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
