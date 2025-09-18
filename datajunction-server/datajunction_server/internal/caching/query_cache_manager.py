from copy import deepcopy
from dataclasses import asdict, dataclass
import json
import logging
from typing import Any, OrderedDict
from fastapi import Request
from datajunction_server.internal.caching.cache_manager import RefreshAheadCacheManager
from sqlalchemy.ext.asyncio import AsyncSession
from datajunction_server.internal.caching.interface import Cache
from datajunction_server.database.queryrequest import (
    QueryRequestKey,
    QueryBuildType,
    VersionedQueryKey,
)
from datajunction_server.internal.sql import build_sql_for_multiple_metrics
from datajunction_server.database.user import User
from datajunction_server.models import access
from datajunction_server.models.sql import GeneratedSQL
from datajunction_server.utils import session_context, get_settings
from datajunction_server.internal.sql import get_measures_query
from datajunction_server.internal.sql import build_node_sql
from datajunction_server.internal.engines import get_engine
from datajunction_server.models.metric import TranslatedSQL

logger = logging.getLogger(__name__)
settings = get_settings()


@dataclass
class QueryRequestParams:
    """
    Parameters for a query request. These are the inputs when requesting SQL building.
    """

    nodes: list[str]
    dimensions: list[str]
    filters: list[str]
    engine_name: str | None = None
    engine_version: str | None = None
    limit: int | None = None
    orderby: list[str] | None = None
    other_args: dict[str, Any] | None = None
    current_user: User | None = None
    validate_access: access.ValidateAccessFn | None = None
    include_all_columns: bool = False
    use_materialized: bool = False
    preaggregate: bool = False
    query_params: str | None = None
    ignore_errors: bool = True

    def __repr__(self):
        return (
            f"QueryRequestParams(nodes={self.nodes}, dimensions={self.dimensions},"
            f" filters={self.filters}, engine_name={self.engine_name}, engine_version={self.engine_version},"
            f" limit={self.limit}, orderby={self.orderby}, other_args={self.other_args},"
            f" include_all_columns={self.include_all_columns}, use_materialized={self.use_materialized},"
            f" preaggregate={self.preaggregate}, query_params={self.query_params})"
        )


class QueryCacheManager(RefreshAheadCacheManager):
    """
    A generic manager for handling caching operations.
    """

    _cache_key_prefix = "sql"
    default_timeout = settings.query_cache_timeout

    def __init__(self, cache: Cache, query_type: QueryBuildType):
        super().__init__(cache)
        self.query_type = query_type

    @property
    def cache_key_prefix(self) -> str:
        return f"{self._cache_key_prefix}:{self.query_type}"

    async def fallback(
        self,
        request: Request,
        params: QueryRequestParams,
    ) -> list[GeneratedSQL] | TranslatedSQL:
        """
        The fallback function to call if the cache is not hit. This should be overridden
        in subclasses.
        """
        params = deepcopy(params)
        async with session_context(request) as session:
            params.nodes = list(OrderedDict.fromkeys(params.nodes))
            query_parameters = (
                json.loads(params.query_params) if params.query_params else {}
            )
            access_control_store = (
                access.AccessControlStore(
                    validate_access=params.validate_access,
                    user=params.current_user,
                    base_verb=access.ResourceRequestVerb.READ,
                )
                if params.validate_access
                else None
            )
            match self.query_type:
                case QueryBuildType.MEASURES:
                    return await self._build_measures_query(
                        session,
                        params,
                        query_parameters,
                    )
                case QueryBuildType.NODE:
                    return await self._build_node_query(
                        session,
                        params,
                        query_parameters,
                        access_control_store,
                    )
                case QueryBuildType.METRICS:  # pragma: no cover
                    return await self._build_metrics_query(
                        session,
                        params,
                        query_parameters,
                        access_control_store,
                    )

    async def build_cache_key(
        self,
        request: Request,
        params: QueryRequestParams,
    ) -> str:
        """
        Returns a cache key for the query request.
        """
        async with session_context(request) as session:
            versioned_request = await VersionedQueryKey.version_query_request(
                session=session,
                nodes=sorted(params.nodes),
                dimensions=sorted(params.dimensions),
                filters=sorted(params.filters),
                orderby=params.orderby or [],
            )
            query_request = QueryRequestKey(
                key=versioned_request,
                query_type=self.query_type,
                engine_name=params.engine_name or "",
                engine_version=params.engine_version or "",
                limit=params.limit or None,
                include_all_columns=params.include_all_columns or False,
                preaggregate=params.preaggregate or False,
                use_materialized=params.use_materialized or False,
                query_parameters=json.loads(params.query_params or "{}"),
                other_args=params.other_args or {},
            )
            return await super().build_cache_key(request, asdict(query_request))

    async def _build_measures_query(
        self,
        session: AsyncSession,
        params: QueryRequestParams,
        query_parameters: dict[str, Any],
    ) -> list[GeneratedSQL]:
        return await get_measures_query(
            session=session,
            metrics=params.nodes,
            dimensions=params.dimensions or [],
            filters=params.filters or [],
            orderby=params.orderby or [],
            engine_name=params.engine_name,
            engine_version=params.engine_version,
            current_user=params.current_user,
            validate_access=params.validate_access,
            include_all_columns=params.include_all_columns,
            use_materialized=params.use_materialized,
            preagg_requested=params.preaggregate,
            query_parameters=query_parameters,
        )

    async def _build_node_query(
        self,
        session: AsyncSession,
        params: QueryRequestParams,
        query_parameters: dict[str, Any],
        access_control_store: access.AccessControlStore | None = None,
    ) -> TranslatedSQL:
        engine = (
            await get_engine(session, params.engine_name, params.engine_version)  # type: ignore
            if params.engine_name
            else None
        )
        built_sql = await build_node_sql(
            node_name=params.nodes[0],
            dimensions=[dim for dim in (params.dimensions or []) if dim and dim != ""],
            filters=params.filters or [],
            orderby=params.orderby or [],
            limit=params.limit,
            session=session,
            engine=engine,  # type: ignore
            ignore_errors=params.ignore_errors,
            use_materialized=params.use_materialized,
            query_parameters=query_parameters,
            access_control=access_control_store,
        )
        return TranslatedSQL.create(
            sql=built_sql.sql,
            columns=built_sql.columns,
            dialect=built_sql.dialect,
        )

    async def _build_metrics_query(
        self,
        session: AsyncSession,
        params: QueryRequestParams,
        query_parameters: dict[str, Any],
        access_control_store: access.AccessControlStore | None = None,
    ) -> TranslatedSQL:
        built_sql, _, _ = await build_sql_for_multiple_metrics(
            session=session,
            metrics=params.nodes,
            dimensions=[dim for dim in (params.dimensions or []) if dim and dim != ""],
            filters=params.filters or [],
            orderby=params.orderby,
            limit=params.limit,
            engine_name=params.engine_name,
            engine_version=params.engine_version,
            access_control=access_control_store,
            ignore_errors=params.ignore_errors,  # type: ignore
            query_parameters=query_parameters,
            use_materialized=params.use_materialized,  # type: ignore
        )
        return TranslatedSQL.create(
            sql=built_sql.sql,
            columns=built_sql.columns,
            dialect=built_sql.dialect,
        )
