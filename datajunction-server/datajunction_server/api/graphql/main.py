"""DJ graphql"""

import gzip
import hashlib
import inspect
import json
import logging
import time
from dataclasses import dataclass
from functools import wraps

import strawberry
from graphql import (
    GraphQLSyntaxError,
    OperationType,
    get_operation_ast,
    parse,
    print_ast,
)
from fastapi import BackgroundTasks, Depends, Request, Response
from strawberry.extensions import SchemaExtension
from strawberry.fastapi import GraphQLRouter
from strawberry.http import GraphQLRequestData
from starlette.concurrency import run_in_threadpool
from strawberry.types import Info
from strawberry.types.unset import UNSET

from datajunction_server.api.graphql.dataloaders import (
    create_collection_nodes_loader,
    create_extracted_measures_loader,
    create_git_info_loader,
    create_node_by_name_loader,
)
from datajunction_server.api.graphql.queries.catalogs import list_catalogs
from datajunction_server.api.graphql.queries.collections import list_collections
from datajunction_server.api.graphql.queries.dag import (
    common_dimensions,
    downstream_nodes,
    upstream_nodes,
)
from datajunction_server.api.graphql.queries.engines import list_dialects, list_engines
from datajunction_server.api.graphql.queries.namespaces import list_namespaces
from datajunction_server.api.graphql.queries.nodes import (
    find_nodes,
    find_nodes_paginated,
    node_counts,
)
from datajunction_server.api.graphql.queries.sql import (
    materialization_plan,
    measures_sql,
)
from datajunction_server.api.graphql.queries.tables import nodes_for_table
from datajunction_server.api.graphql.queries.tags import (
    list_tag_types,
    list_tags,
    search_tags,
)
from datajunction_server.api.graphql.scalars import Connection
from datajunction_server.api.graphql.scalars.catalog_engine import (
    Catalog,
    DialectInfo,
    Engine,
)
from datajunction_server.api.graphql.scalars.collection import Collection
from datajunction_server.api.graphql.scalars.namespace import Namespace
from datajunction_server.api.graphql.scalars.node import (
    DimensionAttribute,
    Node,
    NodeCount,
)
from datajunction_server.api.graphql.scalars.sql import (
    GeneratedSQL,
    MaterializationPlan,
)
from datajunction_server.api.graphql.scalars.tag import Tag
from datajunction_server.instrumentation.provider import get_metrics_provider
from datajunction_server.internal.access.authentication.http import DJHTTPBearer
from datajunction_server.internal.caching.cachelib_cache import get_cache
from datajunction_server.internal.caching.data_version import read_data_version
from datajunction_server.utils import get_session, get_settings

from cross_web import HTTPException

logger = logging.getLogger(__name__)
_warned_unshared: set[type] = set()

# Resolvers that take longer than this are logged at WARNING with their path
# so slow fields are visible without enabling per-resolver timing for everyone.
SLOW_RESOLVER_THRESHOLD_MS = 500.0


def log_resolver(func):
    """
    Adds success-path logging and per-resolver latency timing to the GQL
    resolver. Error logging and ``dj.graphql.errors`` emission live in
    GraphQLErrorReporter — that single source of truth sees ALL errors
    that end up in the response, including programmatic GraphQL errors
    that don't bubble as Python exceptions.
    """

    @wraps(func)
    async def wrapper(*args, **kwargs):
        resolver_name = func.__name__

        info: Info = kwargs.get("info") if "info" in kwargs else None
        user = info.context.get("user", "anonymous") if info else "unknown"
        args_dict = {key: val for key, val in kwargs.items() if key != "info"}
        log_tags = {
            "query_name": resolver_name,
            "user": user,
            **args_dict,
        }
        log_args = " ".join(
            [f"{tag}={value}" for tag, value in log_tags.items() if value],
        )
        _start = time.monotonic()
        try:
            result = await func(*args, **kwargs)
            logger.info("[GQL] %s", log_args)
            return result
        finally:
            get_metrics_provider().timer(
                "dj.graphql.query_ms",
                (time.monotonic() - _start) * 1000,
                {"operation": resolver_name},
            )

    return wrapper


class GraphQLErrorReporter(SchemaExtension):
    """
    Strawberry extension that handles every error that ends up in the
    GraphQL response — including programmatic GraphQL errors returned as
    data (the spec way) that don't bubble as Python exceptions.

    For each error it:
      - emits ``dj.graphql.errors`` with ``operation``, ``operation_name``,
        and ``error_type`` tags
      - logs at ERROR level with the message, path, and the original
        exception's traceback (when one is attached)

    Also wraps every resolver with ``resolve`` so any single field that runs
    longer than ``SLOW_RESOLVER_THRESHOLD_MS`` is logged at WARNING.

    Tag semantics:
      - ``operation`` — top-level field path the error is attached to,
        or ``unknown`` if the error has no path (e.g. parse / validation).
      - ``operation_name`` — the client-supplied name on the GraphQL operation
        (``query Foo { ... }`` → ``Foo``); ``anonymous`` when omitted.
      - ``error_type`` — the underlying exception class name when the error
        was caused by a raised exception, or ``GraphQLError`` when it's a
        programmatic GraphQL error returned as data.
    """

    def on_operation(self):
        yield
        result = getattr(self.execution_context, "result", None)
        if not result or not getattr(result, "errors", None):
            return
        operation_name = (
            getattr(self.execution_context, "operation_name", None) or "anonymous"
        )
        for error in result.errors:
            path = getattr(error, "path", None)
            operation = str(path[0]) if path else "unknown"
            original = getattr(error, "original_error", None)
            error_type = type(original).__name__ if original else "GraphQLError"
            # Parse/validation errors fail before execution: no path, no
            # underlying Python exception. They are client mistakes (typo'd
            # field, malformed query), not server faults — log at WARNING and
            # tag them so Radar can filter them out of error budgets.
            is_client_error = original is None and path is None
            if is_client_error:
                logger.warning(
                    "[GQL] status=client_error operation_name=%s message=%s",
                    operation_name,
                    error.message,
                )
            else:
                logger.error(
                    "[GQL] status=error operation=%s operation_name=%s error_type=%s path=%s message=%s",
                    operation,
                    operation_name,
                    error_type,
                    path,
                    error.message,
                    exc_info=original,
                )
            get_metrics_provider().counter(
                "dj.graphql.errors",
                tags={
                    "operation": operation,
                    "operation_name": operation_name,
                    "error_type": error_type,
                    "severity": "client_error" if is_client_error else "server_error",
                },
            )

    def resolve(self, _next, root, info, *args, **kwargs):
        # Sync on purpose: an ``async def`` here makes every field resolution
        # awaitable, so graphql-core schedules a task per field per list item
        # even for plain attribute reads. Only an awaitable result needs a
        # coroutine to time it.
        start = time.monotonic()
        try:
            result = _next(root, info, *args, **kwargs)
        except BaseException:
            self._log_if_slow(start, info)
            raise
        if inspect.isawaitable(result):
            return self._time_awaitable(result, start, info)
        self._log_if_slow(start, info)
        return result

    async def _time_awaitable(self, result, start, info):
        try:
            return await result
        finally:
            self._log_if_slow(start, info)

    @staticmethod
    def _log_if_slow(start, info):
        elapsed_ms = (time.monotonic() - start) * 1000
        if elapsed_ms >= SLOW_RESOLVER_THRESHOLD_MS:
            logger.warning(
                "[GQL] slow_resolver path=%s elapsed_ms=%.1f",
                info.path.as_list(),
                elapsed_ms,
            )


async def get_context(
    request: Request,
    background_tasks: BackgroundTasks,
    cache=Depends(get_cache),
    _auth=Depends(DJHTTPBearer(auto_error=False)),
):
    """
    Provides the context for graphql requests
    """
    # In tests, get_session is overridden via dependency_overrides to return a
    # shared session. Attach it to request.state so DataLoaders (which use
    # session_context()) reuse the same transaction. In production we must
    # NOT attach a shared session — concurrent DataLoaders on one AsyncSession
    # raise "concurrent operations are not permitted" — so each DataLoader
    # opens its own session via session_context().
    override = request.app.dependency_overrides.get(get_session)
    if override is not None:
        request.state.test_session = override()

    return {
        "node_loader": create_node_by_name_loader(request),
        "collection_nodes_loader": create_collection_nodes_loader(request),
        "git_info_loader": create_git_info_loader(request),
        "extracted_measures_loader": create_extracted_measures_loader(request),
        "settings": get_settings(),
        "request": request,
        "background_tasks": background_tasks,
        "cache": cache,
    }


@strawberry.type
class Query:
    """
    Parent of all DJ graphql queries
    """

    # Catalog and engine queries
    list_catalogs: list[Catalog] = strawberry.field(
        resolver=log_resolver(list_catalogs),
        description="List available catalogs",
    )
    list_engines: list[Engine] = strawberry.field(
        resolver=log_resolver(list_engines),
        description="List all available engines",
    )
    list_dialects: list[DialectInfo] = strawberry.field(
        resolver=log_resolver(list_dialects),
        description="List all supported SQL dialects",
    )

    # Node search queries
    find_nodes: list[Node] = strawberry.field(
        resolver=log_resolver(find_nodes),
        description="Find nodes based on the search parameters.",
    )
    find_nodes_paginated: Connection[Node] = strawberry.field(
        resolver=log_resolver(find_nodes_paginated),
        description="Find nodes based on the search parameters with pagination",
    )
    node_counts: list[NodeCount] = strawberry.field(
        resolver=log_resolver(node_counts),
        description="Count nodes grouped by a field (e.g. type) in a namespace",
    )

    # DAG queries
    common_dimensions: list[DimensionAttribute] = strawberry.field(
        resolver=log_resolver(common_dimensions),
        description="Get common dimensions for one or more nodes",
    )
    downstream_nodes: list[Node] = strawberry.field(
        resolver=log_resolver(downstream_nodes),
        description="Find downstream nodes (optionally, of a given type) from a given node.",
    )
    upstream_nodes: list[Node] = strawberry.field(
        resolver=log_resolver(upstream_nodes),
        description="Find upstream nodes (optionally, of a given type) from a given node.",
    )
    nodes_for_table: list[Node] = strawberry.field(
        resolver=log_resolver(nodes_for_table),
        description="Find the nodes associated with a physical table: the source "
        "nodes on that table and, unless disabled, everything downstream of them.",
    )

    # Generate SQL queries
    measures_sql: list[GeneratedSQL] = strawberry.field(
        resolver=log_resolver(measures_sql),
        description="Get measures SQL for a list of metrics, dimensions, and filters.",
    )
    materialization_plan: MaterializationPlan = strawberry.field(
        resolver=log_resolver(materialization_plan),
        description="Get materialization plan for a list of metrics, dimensions, and filters.",
    )

    # Tags queries
    list_tags: list[Tag] = strawberry.field(
        resolver=log_resolver(list_tags),
        description="Find DJ node tags based on the search parameters.",
    )
    search_tags: list[Tag] = strawberry.field(
        resolver=log_resolver(search_tags),
        description=(
            "Trigram-ranked search across tag name, display name, and description. "
            "Used by the global search bar for tag autocomplete."
        ),
    )
    list_tag_types: list[str] = strawberry.field(
        resolver=log_resolver(list_tag_types),
        description="List all DJ node tag types",
    )

    # Collections queries
    list_collections: list[Collection] = strawberry.field(
        resolver=log_resolver(list_collections),
        description="List collections, optionally filtered by creator (for My Workspace).",
    )

    # Namespace queries
    list_namespaces: list[Namespace] = strawberry.field(
        resolver=log_resolver(list_namespaces),
        description="List all active namespaces with node counts and git configuration.",
    )


class DJSchema(strawberry.Schema):
    """
    Suppress Strawberry's default ``process_errors`` so the only thing
    logging GraphQL errors is ``GraphQLErrorReporter``. The default
    implementation logs every error (including client-side parse and
    validation failures) at ERROR via the ``strawberry.execution`` logger,
    which double-logs and floods Radar with WARN-level user mistakes
    re-promoted to ERROR.
    """

    def process_errors(self, errors, execution_context=None):
        return


schema = DJSchema(query=Query, extensions=[GraphQLErrorReporter])


@dataclass(frozen=True)
class CacheableRequest:
    """A request whose response may be cached."""

    operation: str
    key: str


def _cacheable_request(data: GraphQLRequestData) -> CacheableRequest | None:
    """
    The request, if it is a query whose operation is allowed to be cached.

    The operation is the one GraphQL would run: the one named by
    ``operationName``, or the only one in the document. The key is built from the
    printed document, so formatting does not matter, plus the variables.
    """
    allowed = get_settings().graphql_cacheable_operations
    if not allowed or data.query is None:
        # Nothing to cache, so don't parse every request.
        return None
    try:
        document = parse(data.query)
    except GraphQLSyntaxError:
        return None
    operation = get_operation_ast(document, data.operation_name)
    if (
        operation is None
        or operation.operation != OperationType.QUERY
        or operation.name is None
        or operation.name.value not in allowed
    ):
        return None
    canonical = json.dumps(
        [operation.name.value, print_ast(document), data.variables or {}],
        sort_keys=True,
        separators=(",", ":"),
    )
    return CacheableRequest(
        operation=operation.name.value,
        key=f"graphql:response:{hashlib.sha256(canonical.encode()).hexdigest()}",
    )


class CachingGraphQLRouter(GraphQLRouter):
    """
    GraphQL router that can serve the whole response of an allowed operation
    from the shared cache until something changes.

    Opt-in per operation name (``graphql_cacheable_operations``), for queries
    only: the cached response is the same for every caller, so only list
    operations whose result does not depend on who asks. A response is stored
    under a version of the data it reads (see ``_data_version``), read before
    the query runs, so any later change makes it unreachable;
    ``graphql_response_cache_ttl`` only bounds how long it is kept. Responses
    with errors are not cached. The request's cache (which honors
    ``Cache-Control: no-cache``) does the storing.
    """

    async def run(self, request, context=UNSET, root_value=UNSET):  # type: ignore[no-untyped-def]
        if request.method != "POST":
            return await super().run(request, context, root_value)
        try:
            data = await self.parse_http_body(self.request_adapter_class(request))
        except HTTPException:
            # Let the normal path answer an invalid request.
            return await super().run(request, context, root_value)
        # A batch, which this schema does not enable, is a list.
        cacheable = (
            _cacheable_request(data) if isinstance(data, GraphQLRequestData) else None
        )
        cache = context.get("cache") if isinstance(context, dict) else None
        if cacheable is None or cache is None:
            return await super().run(request, context, root_value)
        if not getattr(cache, "shared", True):
            # An edit replaces the version in one process only, so the others
            # would keep serving what they stored.
            if type(cache) not in _warned_unshared:
                _warned_unshared.add(type(cache))
                logger.warning(
                    "GraphQL response caching needs a cache shared by every server "
                    "process; %s is not, so it is skipped",
                    type(cache).__name__,
                )
            return await super().run(request, context, root_value)

        request.state.read_from_writer = True
        # Read before the query runs: the data is then at least this new. The
        # cache clients are blocking network calls, so keep them off the loop.
        version = await run_in_threadpool(read_data_version, cache, data.query)
        key = f"{cacheable.key}:{version}"
        if (
            version is not None
            and (stored := await run_in_threadpool(cache.get, key)) is not None
        ):
            get_metrics_provider().counter(
                "dj.graphql.response_cache",
                tags={"operation": cacheable.operation, "result": "hit"},
            )
            return Response(gzip.decompress(stored), media_type="application/json")

        response = await super().run(request, context, root_value)
        body = bytes(response.body)
        # Anywhere in the body: an error message can be arbitrarily long. A data
        # key named ``errors`` would only mean skipping the cache.
        if (
            version is not None
            and response.status_code == 200
            and b'"errors"' not in body
        ):
            await run_in_threadpool(
                cache.set,
                key,
                gzip.compress(body, compresslevel=1),
                timeout=get_settings().graphql_response_cache_ttl,
            )
        get_metrics_provider().counter(
            "dj.graphql.response_cache",
            tags={"operation": cacheable.operation, "result": "miss"},
        )
        return response


graphql_app = CachingGraphQLRouter(schema, context_getter=get_context)
