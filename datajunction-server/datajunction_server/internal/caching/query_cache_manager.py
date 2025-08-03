from copy import deepcopy
from dataclasses import asdict, dataclass
import json
from typing import Any, OrderedDict
from fastapi import Request
from datajunction_server.internal.caching.cache_manager import RefreshAheadCacheManager

from datajunction_server.internal.caching.interface import Cache
from datajunction_server.database.queryrequest import (
    QueryRequestKey,
    QueryBuildType,
    VersionedQueryKey,
)
from datajunction_server.database.user import User
from datajunction_server.models import access
from datajunction_server.models.sql import GeneratedSQL
from datajunction_server.utils import session_context, get_settings
from datajunction_server.construction.build_v2 import get_measures_query

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
    ) -> list[GeneratedSQL]:
        """
        The fallback function to call if the cache is not hit. This should be overridden
        in subclasses.
        """
        params = deepcopy(params)
        async with session_context(request) as session:
            nodes = list(OrderedDict.fromkeys(params.nodes))
            measures_query = await get_measures_query(
                session=session,
                metrics=nodes,
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
                query_parameters=json.loads(params.query_params or "{}"),
            )
            return measures_query

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
