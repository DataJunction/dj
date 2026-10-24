"""
Whole-response caching of opted-in GraphQL operations.
"""

import pytest
from strawberry.fastapi import GraphQLRouter

from datajunction_server.api.main import app
from datajunction_server.config import Settings
from datajunction_server.internal.caching.interface import Cache
from datajunction_server.internal.caching.cachelib_cache import get_cache
from datajunction_server.utils import get_settings

QUERY = """
query NodeListing($types: [NodeType!]) {
  findNodes(nodeTypes: $types) { name }
}
"""


class DictCache(Cache):
    def __init__(self):
        super().__init__()
        self.store = {}
        self.sets = []

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value, timeout=300):
        self.sets.append((key, timeout))
        self.store[key] = value

    def delete(self, key):
        self.store.pop(key, None)


@pytest.fixture
def cache():
    cache = DictCache()
    app.dependency_overrides[get_cache] = lambda: cache
    yield cache
    app.dependency_overrides.pop(get_cache, None)


@pytest.fixture
def allow(monkeypatch):
    def _allow(*operations, ttl=60):
        settings = get_settings()
        patched = Settings(
            **{
                **settings.model_dump(),
                "graphql_cacheable_operations": list(operations),
                "graphql_response_cache_ttl": ttl,
            },
        )
        monkeypatch.setattr(
            "datajunction_server.api.graphql.main.get_settings",
            lambda: patched,
        )

    return _allow


@pytest.mark.asyncio
async def test_an_allowed_operation_is_served_from_the_cache(
    client_with_roads,
    cache,
    allow,
    mocker,
):
    allow("NodeListing", ttl=45)
    executed = mocker.spy(GraphQLRouter, "run")
    body = {"query": QUERY, "variables": {"types": ["CUBE"]}}

    first = await client_with_roads.post("/graphql", json=body)
    second = await client_with_roads.post("/graphql", json=body)

    assert first.status_code == second.status_code == 200
    assert second.content == first.content
    assert executed.call_count == 1
    assert [timeout for _, timeout in cache.sets] == [45]


@pytest.mark.asyncio
async def test_variables_and_query_text_are_part_of_the_key(
    client_with_roads,
    cache,
    allow,
    mocker,
):
    allow("NodeListing")
    executed = mocker.spy(GraphQLRouter, "run")

    await client_with_roads.post(
        "/graphql",
        json={"query": QUERY, "variables": {"types": ["CUBE"]}},
    )
    await client_with_roads.post(
        "/graphql",
        json={"query": QUERY, "variables": {"types": ["METRIC"]}},
    )
    await client_with_roads.post(
        "/graphql",
        json={
            "query": QUERY.replace("{ name }", "{ name type }"),
            "variables": {"types": ["METRIC"]},
        },
    )

    assert executed.call_count == 3
    assert len(cache.store) == 3


@pytest.mark.asyncio
async def test_operation_name_selects_the_operation(client_with_roads, cache, allow):
    allow("NodeListing")
    query = "query Other { findNodes { name } } " + QUERY.strip()

    other = await client_with_roads.post(
        "/graphql",
        json={"query": query, "operationName": "Other"},
    )
    assert other.status_code == 200
    assert cache.store == {}  # `operationName` picks `Other`, which is not allowed

    cache.store.clear()
    await client_with_roads.post(
        "/graphql",
        json={"query": query, "operationName": "NodeListing"},
    )
    assert len(cache.store) == 1


@pytest.mark.asyncio
async def test_other_operations_are_not_cached(client_with_roads, cache, allow):
    allow("SomethingElse")
    await client_with_roads.post("/graphql", json={"query": QUERY})
    await client_with_roads.post("/graphql", json={"query": "{ findNodes { name } }"})

    assert cache.store == {}


@pytest.mark.asyncio
async def test_nothing_is_cached_unless_operations_are_allowed(
    client_with_roads,
    cache,
    allow,
):
    allow()
    await client_with_roads.post("/graphql", json={"query": QUERY})

    assert cache.store == {}


@pytest.mark.asyncio
async def test_a_response_with_errors_is_not_cached(client_with_roads, cache, allow):
    allow("NodeListing")
    broken = "query NodeListing { findNodes { doesNotExist } }"

    response = await client_with_roads.post("/graphql", json={"query": broken})

    assert response.json()["errors"]
    assert cache.store == {}


@pytest.mark.asyncio
async def test_unparseable_and_batched_requests_bypass_the_cache(
    client_with_roads,
    cache,
    allow,
):
    allow("NodeListing")

    garbage = await client_with_roads.post(
        "/graphql",
        content=b"not json",
        headers={"Content-Type": "application/json"},
    )
    batch = await client_with_roads.post(
        "/graphql",
        json=[{"query": QUERY}, {"query": QUERY}],
    )
    no_query = await client_with_roads.post("/graphql", json={"operationName": "X"})

    assert garbage.status_code in (400, 422)
    assert batch.status_code in (200, 400)
    assert no_query.status_code in (200, 400)
    assert cache.store == {}


@pytest.mark.asyncio
async def test_get_requests_bypass_the_cache(client_with_roads, cache, allow):
    allow("NodeListing")

    await client_with_roads.get("/graphql", params={"query": QUERY})

    assert cache.store == {}


@pytest.mark.asyncio
async def test_a_change_makes_the_cached_response_unreachable(
    client_with_roads,
    cache,
    allow,
    mocker,
):
    allow("NodeListing")
    executed = mocker.spy(GraphQLRouter, "run")
    body = {"query": QUERY, "variables": {"types": ["SOURCE"]}}

    await client_with_roads.post("/graphql", json=body)
    await client_with_roads.post("/graphql", json=body)
    assert executed.call_count == 1

    created = await client_with_roads.post(
        "/nodes/source/",
        json={
            "name": "default.a_new_source",
            "mode": "published",
            "catalog": "default",
            "schema_": "roads",
            "table": "a_new_source",
            "columns": [{"name": "id", "type": "int"}],
        },
    )
    assert created.status_code in (200, 201), created.text
    after = await client_with_roads.post("/graphql", json=body)

    assert executed.call_count == 2
    assert "default.a_new_source" in after.text
    assert len(cache.store) == 2


@pytest.mark.asyncio
async def test_availability_updates_do_not_invalidate(
    client_with_roads,
    cache,
    allow,
    mocker,
):
    allow("NodeListing")
    executed = mocker.spy(GraphQLRouter, "run")
    body = {"query": QUERY, "variables": {"types": ["SOURCE"]}}

    await client_with_roads.post("/graphql", json=body)
    published = await client_with_roads.post(
        "/data/default.repair_orders/availability/",
        json={
            "catalog": "default",
            "schema_": "roads",
            "table": "repair_orders",
            "valid_through_ts": 9999999999,
        },
    )
    assert published.status_code == 200, published.text
    await client_with_roads.post("/graphql", json=body)

    assert executed.call_count == 1


@pytest.mark.asyncio
async def test_formatting_does_not_change_the_key(
    client_with_roads,
    cache,
    allow,
    mocker,
):
    allow("NodeListing")
    executed = mocker.spy(GraphQLRouter, "run")
    variables = {"types": ["SOURCE"]}

    await client_with_roads.post(
        "/graphql", json={"query": QUERY, "variables": variables}
    )
    compact = " ".join(QUERY.split())
    await client_with_roads.post(
        "/graphql", json={"query": compact, "variables": variables}
    )

    assert executed.call_count == 1


def _request(query, operation_name=None, variables=None):
    from strawberry.http import GraphQLRequestData

    return GraphQLRequestData(
        query=query,
        variables=variables,
        operation_name=operation_name,
        extensions=None,
    )


def test_only_queries_are_cached(allow):
    from datajunction_server.api.graphql.main import _cacheable_request

    allow("NodeListing", "Anonymous")

    assert _cacheable_request(_request("query NodeListing { findNodes { name } }"))
    # A mutation with an allowed name, an unnamed query, several operations with
    # no choice between them, and a name that is not in the document.
    assert not _cacheable_request(_request("mutation NodeListing { x }"))
    assert not _cacheable_request(_request("{ findNodes { name } }"))
    two = "query A { findNodes { name } } query NodeListing { findNodes { name } }"
    assert not _cacheable_request(_request(two))
    assert _cacheable_request(_request(two, "NodeListing"))
    assert not _cacheable_request(_request(two, "Nope"))
    # Invalid syntax, and a request with no query (a persisted query).
    assert not _cacheable_request(_request("query NodeListing {"))
    assert not _cacheable_request(_request(None))


@pytest.mark.asyncio
async def test_availability_only_matters_to_a_query_that_selects_it(
    client_with_roads,
    cache,
    allow,
    mocker,
):
    allow("NodeListing")
    executed = mocker.spy(GraphQLRouter, "run")
    with_availability = {
        "query": 'query NodeListing { findNodes(names: ["default.repair_orders"]) '
        "{ name current { availability { validThroughTs } } } }",
    }

    await client_with_roads.post("/graphql", json=with_availability)
    published = await client_with_roads.post(
        "/data/default.repair_orders/availability/",
        json={
            "catalog": "default",
            "schema_": "roads",
            "table": "repair_orders",
            "valid_through_ts": 9999999999,
        },
    )
    assert published.status_code == 200, published.text
    after = await client_with_roads.post("/graphql", json=with_availability)

    assert executed.call_count == 2
    assert "9999999999" in after.text


@pytest.mark.asyncio
async def test_collection_edits_invalidate_a_query_that_lists_them(
    client_with_roads,
    cache,
    allow,
    mocker,
):
    allow("NodeListing")
    executed = mocker.spy(GraphQLRouter, "run")
    listing = {
        "query": "query NodeListing { listCollections { name nodeCount } }",
    }

    first = await client_with_roads.post("/graphql", json=listing)
    await client_with_roads.post("/graphql", json=listing)
    assert executed.call_count == 1

    created = await client_with_roads.post(
        "/collections/",
        json={"name": "cache_test", "description": "a new collection"},
    )
    assert created.status_code in (200, 201), created.text
    second = await client_with_roads.post("/graphql", json=listing)
    assert executed.call_count == 2
    assert "cache_test" in second.text and "cache_test" not in first.text

    added = await client_with_roads.post(
        "/collections/cache_test/nodes/",
        json=["default.repair_orders"],
    )
    assert added.status_code == 204, added.text
    third = await client_with_roads.post("/graphql", json=listing)
    assert executed.call_count == 3
    assert '"nodeCount":1' in third.text.replace(" ", "")

    removed = await client_with_roads.post(
        "/collections/cache_test/remove/",
        json=["default.repair_orders"],
    )
    assert removed.status_code == 204, removed.text
    # Back to the state of the second request, whose response is still right.
    fourth = await client_with_roads.post("/graphql", json=listing)
    assert executed.call_count == 3
    assert '"nodeCount":0' in fourth.text.replace(" ", "")

    deleted = await client_with_roads.delete("/collections/cache_test")
    assert deleted.status_code == 204, deleted.text
    final = await client_with_roads.post("/graphql", json=listing)
    # Back to the state of the first request, whose response is still right.
    assert executed.call_count == 3
    assert "cache_test" not in final.text


@pytest.mark.asyncio
async def test_an_error_too_long_for_the_tail_of_the_body_is_not_cached(
    client_with_roads,
    cache,
    allow,
    mocker,
):
    import json

    from fastapi import Response

    allow("NodeListing")
    body = json.dumps(
        {"data": None, "errors": [{"message": "unknown cube " + "x" * 10_000}]},
    )
    mocker.patch.object(
        GraphQLRouter,
        "run",
        mocker.AsyncMock(return_value=Response(body, media_type="application/json")),
    )

    response = await client_with_roads.post("/graphql", json={"query": QUERY})

    assert response.status_code == 200
    assert cache.store == {}


async def _history_row(session, row_id, entity_type):
    from datajunction_server.database.history import History
    from datajunction_server.internal.history import ActivityType

    session.add(
        History(
            id=row_id,
            entity_type=entity_type,
            entity_name="default.somewhere",
            node="default.somewhere",
            activity_type=ActivityType.UPDATE,
            user="tester",
        ),
    )
    await session.commit()


@pytest.mark.asyncio
async def test_a_late_commit_with_a_lower_id_changes_the_version(session):
    from types import SimpleNamespace

    from datajunction_server.api.graphql.main import _data_version
    from datajunction_server.internal.history import EntityType

    request = SimpleNamespace(state=SimpleNamespace(test_session=session))
    nodes = "{ findNodes { name } }"

    # Ids are allocated before commit: 2_000_001 commits first, then 2_000_000.
    await _history_row(session, 2_000_001, EntityType.NODE)
    before = await _data_version(request, nodes)
    await _history_row(session, 2_000_000, EntityType.NODE)
    after = await _data_version(request, nodes)

    # The latest id is the same; only the count of recent rows moved.
    assert before.split(".")[0] == after.split(".")[0]
    assert before != after


@pytest.mark.asyncio
async def test_availability_events_only_move_the_version_of_a_query_that_selects_them(
    session,
):
    from types import SimpleNamespace

    from datajunction_server.api.graphql.main import _data_version
    from datajunction_server.internal.history import EntityType

    request = SimpleNamespace(state=SimpleNamespace(test_session=session))
    plain = "{ findNodes { name } }"
    selecting = "{ findNodes { current { availability { validThroughTs } } } }"

    plain_before = await _data_version(request, plain)
    selecting_before = await _data_version(request, selecting)
    await _history_row(session, 3_000_001, EntityType.AVAILABILITY)
    # A late availability commit with a lower id is noticed too.
    selecting_mid = await _data_version(request, selecting)
    await _history_row(session, 3_000_000, EntityType.AVAILABILITY)

    assert await _data_version(request, plain) == plain_before
    assert selecting_before != selecting_mid
    assert selecting_mid != await _data_version(request, selecting)
