"""Scoped Explore views and bidirectional selection against DJ's graph."""

from types import SimpleNamespace
from datetime import UTC, datetime
import re
from unittest.mock import AsyncMock

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.api import semantic_layer as api
from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.node import Node
from datajunction_server.errors import DJException
from datajunction_server.internal.access.authorization import get_access_checker
from datajunction_server.models.access import ResourceType
from datajunction_server.models.node import NodeStatus
from datajunction_server.typing import QueryState
from datajunction_server.utils import get_query_service_client
from tests.api.semantic_layer_test import _expect, _setup_cube


@pytest.mark.parametrize(
    "namespace,branch",
    [
        (None, None),
        ("sales", None),
        ("sales", "main"),
        ("sales", "feature/revenue"),
        ("sales", "feature~2Frevenue"),
        ("sales", "feature%2Frevenue"),
        ("sales", "feature/revenue?x=1#é%+"),
    ],
)
def test_explore_identity_round_trip(namespace, branch):
    summary = api._explore_summary(namespace, branch)
    assert re.fullmatch(r"@explore(?::[A-Za-z0-9_.~-]+){0,2}", summary.name)
    assert summary.uid == summary.name
    assert api._explore_configuration(summary.name, {"tenant": "test"}) == {
        "namespace": namespace,
        "branch": branch,
        "tenant": "test",
    }
    if namespace:
        label = f"{namespace} / {branch}" if branch else namespace
        assert summary.display_name == f"Explore ( {label} )"
    else:
        assert summary.display_name == "Explore"


@pytest.mark.parametrize(
    "name",
    [
        "@explore:",
        "@explore::main",
        "@explore:sales:",
        "@explore:sales:main:extra",
        "@explore:sales:feature~",
        "@explore:sales:feature~2",
        "@explore:sales:feature~GG",
        "@explore:sales:feature~FF",
        "@explore:sales:feature~2ffoo",
        "@explore:~73ales",
        "@explore:sales:feature/foo",
        "@explore:sales:feature%2Ffoo",
    ],
)
def test_explore_identity_rejects_invalid_scope(name):
    with pytest.raises(DJException) as error:
        api._explore_configuration(name, {})
    assert error.value.http_status_code == 404


def test_explore_names_are_readable_and_escape_ambiguous_components():
    assert api._explore_name(None, None) == "@explore"
    assert api._explore_name("sales", None) == "@explore:sales"
    assert api._explore_name("sales", "main") == "@explore:sales:main"
    assert api._explore_name("sales", "feature/foo") == "@explore:sales:feature~2Ffoo"
    assert (
        api._explore_name("sales", "feature~2Ffoo") == "@explore:sales:feature~7E2Ffoo"
    )
    assert (
        api._explore_name("sales", "feature%2Ffoo") == "@explore:sales:feature~252Ffoo"
    )
    assert api._explore_name("sales", "feature:foo") == "@explore:sales:feature~3Afoo"


@pytest.mark.parametrize(
    "configuration",
    [
        {"namespace": "other"},
        {"branch": "other"},
        {"namespace": None},
    ],
)
def test_explore_identity_rejects_conflicting_configuration(configuration):
    with pytest.raises(DJException) as error:
        api._explore_configuration(
            api._explore_summary("sales", "main").name,
            configuration,
        )
    assert error.value.http_status_code == 400


@pytest.mark.asyncio
async def test_malformed_explore_names_return_not_found(client: AsyncClient):
    for name in (
        "@explore:",
        "@explore:!",
        "@explore:abc",
        api._explore_summary("sales").name + "=",
    ):
        for suffix, payload in (
            ("", {}),
            ("/compatible-metrics", {}),
            ("/compatible-dimensions", {}),
            ("/sql", {"query": {"metrics": ["sales.revenue"]}}),
            ("/query", {"query": {"metrics": ["sales.revenue"]}}),
            ("/row-count", {"query": {"metrics": ["sales.revenue"]}}),
            ("/values", {"dimension": "sales.region.name"}),
        ):
            response = await client.post(
                f"/semantic/views/{name}{suffix}",
                json=payload,
            )
            assert response.status_code == 404, (name, suffix, response.text)


@pytest.mark.asyncio
async def test_explore_selection_and_execution(client: AsyncClient, monkeypatch):
    await _setup_cube(client)
    await _expect(await client.post("/namespaces/sem.child"), 200, 201)
    for name, metric_query in (
        ("sem.order_count", "SELECT COUNT(*) FROM sem.sales"),
        ("sem.region_count", "SELECT COUNT(*) FROM sem.region"),
        ("sem.child.other_count", "SELECT COUNT(*) FROM sem.sales"),
    ):
        await _expect(
            await client.post(
                "/nodes/metric/",
                json={
                    "name": name,
                    "query": metric_query,
                    "mode": "published",
                },
            ),
            200,
            201,
        )
    await _expect(
        await client.post(
            "/nodes/dimension/",
            json={
                "name": "sem.unrelated",
                "query": "SELECT id FROM sem.sales",
                "primary_key": ["id"],
                "mode": "published",
            },
        ),
        200,
        201,
    )
    await _expect(
        await client.post(
            "/nodes/sem.sales/link/",
            json={
                "dimension_node": "sem.region",
                "role": "billing",
                "join_type": "inner",
                "join_on": "sem.sales.region_id = sem.region.region_id",
            },
        ),
        200,
        201,
    )

    listing = await client.post("/semantic/views", json={"path": ["sem"]})
    view = listing.json()["views"][0]["name"]
    scope: dict[str, dict[str, str]] = {"additional_configuration": {}}
    response = await client.post(f"/semantic/views/{view}", json=scope)
    assert response.status_code == 200, response.text
    detail = response.json()
    assert detail["name"] == view
    assert detail["uid"] == view
    assert detail["display_name"] == "Explore ( sem )"
    assert {metric["id"] for metric in detail["metrics"]} == {
        "sem.total_amount",
        "sem.order_count",
        "sem.region_count",
    }
    dimensions = {dim["id"] for dim in detail["dimensions"]}
    assert {
        "sem.region.region_name",
        "sem.region.region_name[billing]",
        "sem.unrelated.id",
    } <= dimensions
    amount = next(
        metric for metric in detail["metrics"] if metric["id"] == "sem.total_amount"
    )
    assert amount["metadata"]["unit"] == {"kind": "currency", "code": "USD"}

    async def compatible(endpoint, **selection):
        result = await client.post(
            f"/semantic/views/{view}/compatible-{endpoint}",
            json={**scope, **selection},
        )
        assert result.status_code == 200, result.text
        return {column["id"] for column in result.json()}

    assert await compatible("metrics") == {metric["id"] for metric in detail["metrics"]}
    assert await compatible("dimensions") == dimensions
    assert await compatible("dimensions", selected_metrics=["sem.total_amount"]) == {
        "sem.region.region_id",
        "sem.region.region_name",
        "sem.region.region_id[billing]",
        "sem.region.region_name[billing]",
    }
    assert await compatible(
        "metrics",
        selected_dimensions=["sem.region.region_name[billing]"],
    ) == {
        "sem.total_amount",
        "sem.order_count",
    }
    assert await compatible(
        "dimensions",
        selected_metrics=["sem.total_amount", "sem.region_count"],
    ) == {
        "sem.region.region_id",
        "sem.region.region_name",
    }
    assert (
        await compatible(
            "metrics",
            selected_metrics=["sem.region_count"],
            selected_dimensions=["sem.region.region_name[billing]"],
        )
        == set()
    )
    assert (
        await compatible("metrics", selected_dimensions=["sem.unrelated.id"]) == set()
    )
    assert await compatible("dimensions", selected_dimensions=["sem.unrelated.id"]) == {
        "sem.unrelated.id",
    }

    for endpoint, selection in (
        ("compatible-metrics", {"selected_metrics": ["sem.child.other_count"]}),
        ("compatible-dimensions", {"selected_dimensions": ["missing.dimension"]}),
    ):
        result = await client.post(
            f"/semantic/views/{view}/{endpoint}",
            json={**scope, **selection},
        )
        assert result.status_code == 400, result.text

    query = {
        **scope,
        "query": {
            "metrics": ["sem.total_amount"],
            "dimensions": ["sem.region.region_name[billing]"],
        },
    }
    sql = await client.post(f"/semantic/views/{view}/sql", json=query)
    assert sql.status_code == 200, sql.text
    assert sql.json()["dialect"] == "trino"
    assert sql.json()["cube_name"] is None
    assert "SUM(" in sql.json()["sql"]
    for group_others in (False, True):
        grouped = await client.post(
            f"/semantic/views/{view}/sql",
            json={
                **scope,
                "query": {
                    "metrics": ["sem.total_amount"],
                    "dimensions": ["sem.region.region_name"],
                    "group_limit": {
                        "dimensions": ["sem.region.region_name"],
                        "top": 2,
                        "group_others": group_others,
                    },
                },
            },
        )
        assert grouped.status_code == 200, grouped.text
        assert "LIMIT 2" in grouped.json()["sql"]
    independent_ranking = await client.post(
        f"/semantic/views/{view}/sql",
        json={
            **scope,
            "query": {
                "metrics": ["sem.total_amount"],
                "dimensions": [
                    "sem.region.region_name",
                    "sem.region.region_name[billing]",
                ],
                "group_limit": {
                    "dimensions": ["sem.region.region_name"],
                    "metric": "sem.region_count",
                    "top": 2,
                },
            },
        },
    )
    assert independent_ranking.status_code == 200, independent_ranking.text
    assert (
        await client.post(
            f"/semantic/views/{view}/sql",
            json={
                **scope,
                "query": {
                    "metrics": ["sem.total_amount"],
                    "dimensions": ["sem.unrelated.id"],
                },
            },
        )
    ).status_code == 400
    assert (
        await client.post(
            f"/semantic/views/{view}/sql",
            json={
                **scope,
                "query": {
                    "metrics": ["sem.child.other_count"],
                },
            },
        )
    ).status_code == 400

    service = SimpleNamespace(
        submit_query=AsyncMock(
            return_value=SimpleNamespace(
                state=QueryState.FINISHED,
                errors=[],
                results=SimpleNamespace(root=[SimpleNamespace(rows=[[12.5]])]),
            ),
        ),
    )
    monkeypatch.setitem(
        client.app.dependency_overrides,
        get_query_service_client,
        lambda: service,
    )
    scalar_query = {**scope, "query": {"metrics": ["sem.total_amount"]}}
    for endpoint in ("query", "row-count"):
        service.submit_query.return_value.results.root[0].rows = [
            [12.5 if endpoint == "query" else 1],
        ]
        result = await client.post(
            f"/semantic/views/{view}/{endpoint}",
            json=scalar_query,
        )
        assert result.status_code == 200, result.text
        assert len(result.json()["results"]["rows"]) == 1
    service.submit_query.return_value.results.root[0].rows = [["North"]]
    result = await client.post(
        f"/semantic/views/{view}/values",
        json={**scope, "dimension": "sem.region.region_name"},
    )
    assert result.status_code == 200, result.text
    assert result.json()["results"]["rows"] == [{"sem.region.region_name": "North"}]
    assert "DISTINCT" in result.json()["requests"][0]["definition"]


@pytest.mark.asyncio
async def test_explore_names_are_unique_and_match_result_columns(client, monkeypatch):
    """Identical leaf names across nodes must remain distinct in a global view."""
    await _setup_cube(client)
    await _expect(
        await client.post(
            "/nodes/sem.sales/link/",
            json={
                "dimension_node": "sem.region",
                "role": "billing",
                "join_type": "inner",
                "join_on": "sem.sales.region_id = sem.region.region_id",
            },
        ),
        200,
        201,
    )
    await _expect(await client.post("/namespaces/sem.child"), 200, 201)
    for name in ("sem.revision_a", "sem.revision_b"):
        await _expect(
            await client.post(
                "/nodes/dimension/",
                json={
                    "name": name,
                    "query": "SELECT region_id AS node_revision_id FROM sem.regions",
                    "primary_key": ["node_revision_id"],
                    "mode": "published",
                },
            ),
            200,
            201,
        )
    await _expect(
        await client.post(
            "/nodes/metric/",
            json={
                "name": "sem.child.total_amount",
                "query": "SELECT SUM(amount) FROM sem.sales",
                "mode": "published",
            },
        ),
        200,
        201,
    )
    response = await client.post("/semantic/views/@explore", json={})
    assert response.status_code == 200, response.text
    detail = response.json()
    for kind in ("metrics", "dimensions"):
        names = [column["name"] for column in detail[kind]]
        assert len(names) == len(set(names))
        assert all(column["name"] == column["id"] for column in detail[kind])
    assert {
        "sem.revision_a.node_revision_id",
        "sem.revision_b.node_revision_id",
    } <= {column["name"] for column in detail["dimensions"]}

    service = SimpleNamespace(
        submit_query=AsyncMock(
            return_value=SimpleNamespace(
                state=QueryState.FINISHED,
                errors=[],
                results=SimpleNamespace(
                    root=[SimpleNamespace(rows=[["North", "South", 12.5, 12.5]])],
                ),
            ),
        ),
    )
    monkeypatch.setitem(
        client.app.dependency_overrides,
        get_query_service_client,
        lambda: service,
    )
    response = await client.post(
        "/semantic/views/@explore/query",
        json={
            "query": {
                "metrics": ["sem.total_amount", "sem.child.total_amount"],
                "dimensions": [
                    "sem.region.region_name",
                    "sem.region.region_name[billing]",
                ],
            },
        },
    )
    assert response.status_code == 200, response.text
    result = response.json()["results"]
    assert {column["name"] for column in result["schema"]} == {
        "sem.total_amount",
        "sem.child.total_amount",
        "sem.region.region_name",
        "sem.region.region_name[billing]",
    }
    assert result["rows"] == [
        {
            "sem.region.region_name": "North",
            "sem.region.region_name[billing]": "South",
            "sem.total_amount": 12.5,
            "sem.child.total_amount": 12.5,
        },
    ]


@pytest.mark.asyncio
async def test_explore_git_scopes(client: AsyncClient, session: AsyncSession):
    await _setup_cube(client)
    for namespace in ("feature", "sem.independent", "empty"):
        await _expect(await client.post(f"/namespaces/{namespace}"), 200, 201)
    for name in ("feature.count", "sem.independent.count"):
        await _expect(
            await client.post(
                "/nodes/metric/",
                json={
                    "name": name,
                    "query": "SELECT COUNT(*) FROM sem.sales",
                    "mode": "published",
                },
            ),
            200,
            201,
        )
    root = await NodeNamespace.get(session, "sem")
    feature = await NodeNamespace.get(session, "feature")
    independent = await NodeNamespace.get(session, "sem.independent")
    assert root and feature and independent
    root.github_repo_path = "team/sem"
    root.default_branch = "main"
    feature.parent_namespace = "sem"
    feature.git_branch = "feature/foo"
    independent.github_repo_path = "team/independent"
    independent.default_branch = "main"
    await session.commit()

    identities = set()
    for path, metric_ids in (
        ([], {"sem.total_amount", "feature.count", "sem.independent.count"}),
        (["sem"], {"sem.total_amount", "feature.count"}),
        (["sem", "main"], {"sem.total_amount"}),
        (["sem", "feature/foo"], {"feature.count"}),
        (["empty", None], set()),
    ):
        listing = await client.post("/semantic/views", json={"path": path})
        assert listing.status_code == 200, listing.text
        summary = listing.json()["views"][0]
        view = summary["name"]
        assert view == "@explore" if not path else view.startswith("@explore:")
        location = " / ".join(value for value in path if value is not None)
        assert summary["display_name"] == (
            f"Explore ( {location} )" if location else "Explore"
        )
        identities.add(summary["uid"])
        detail = await client.post(
            f"/semantic/views/{view}",
            json={"additional_configuration": {}},
        )
        assert detail.status_code == 200, detail.text
        assert detail.json()["uid"] == summary["uid"]
        assert detail.json()["name"] == view
        assert detail.json()["display_name"] == summary["display_name"]
        actual = {metric["id"] for metric in detail.json()["metrics"]}
        if path:
            assert actual == metric_ids
        else:
            assert metric_ids <= actual
        compatible = await client.post(
            f"/semantic/views/{view}/compatible-metrics",
            json={"additional_configuration": {}},
        )
        assert compatible.status_code == 200, compatible.text
        assert {metric["id"] for metric in compatible.json()} == actual
        if path == ["sem", "feature/foo"]:
            sql = await client.post(
                f"/semantic/views/{view}/sql",
                json={
                    "additional_configuration": {},
                    "query": {"metrics": ["feature.count"]},
                },
            )
            assert sql.status_code == 200, sql.text
            outside = await client.post(
                f"/semantic/views/{view}/sql",
                json={
                    "additional_configuration": {},
                    "query": {"metrics": ["sem.total_amount"]},
                },
            )
            assert outside.status_code == 400, outside.text
    assert len(identities) == 5
    for invalid_configuration, status in (
        ({"namespace": "sem", "branch": "missing"}, 404),
        ({"namespace": "feature"}, 404),
        ({"namespace": "missing"}, 404),
        ({"namespace": "empty", "branch": "main"}, 404),
    ):
        response = await client.post(
            f"/semantic/views/{api._explore_summary(**invalid_configuration).name}",
            json={"additional_configuration": {}},
        )
        assert response.status_code == status, response.text


@pytest.mark.asyncio
async def test_explore_authorizes_elements_and_scope(client: AsyncClient, monkeypatch):
    await _setup_cube(client)
    checker = AsyncMock()
    checker.approved_resource_keys.return_value = {
        (ResourceType.NAMESPACE, "sem"),
        (ResourceType.NODE, "sem.total_amount"),
    }
    monkeypatch.setitem(
        client.app.dependency_overrides,
        get_access_checker,
        lambda: checker,
    )
    listing = await client.post("/semantic/views", json={"path": ["sem"]})
    view = listing.json()["views"][0]["name"]
    scope: dict[str, dict[str, str]] = {"additional_configuration": {}}
    detail = await client.post(f"/semantic/views/{view}", json=scope)
    assert detail.status_code == 200, detail.text
    assert [metric["id"] for metric in detail.json()["metrics"]] == ["sem.total_amount"]
    assert detail.json()["dimensions"] == []
    denied_dim = await client.post(
        f"/semantic/views/{view}/sql",
        json={
            **scope,
            "query": {
                "metrics": ["sem.total_amount"],
                "dimensions": ["sem.region.region_name"],
            },
        },
    )
    assert denied_dim.status_code == 400
    checker.approved_resource_keys.return_value = {
        (ResourceType.NODE, "sem.total_amount"),
    }
    for suffix, payload in (
        ("", {}),
        ("/compatible-metrics", {}),
        ("/compatible-dimensions", {}),
        ("/sql", {"query": {"metrics": ["sem.total_amount"]}}),
        ("/query", {"query": {"metrics": ["sem.total_amount"]}}),
        ("/row-count", {"query": {"metrics": ["sem.total_amount"]}}),
        ("/values", {"dimension": "sem.region.region_name"}),
    ):
        result = await client.post(
            f"/semantic/views/{view}{suffix}",
            json={**scope, **payload},
        )
        assert result.status_code == 404, (suffix, result.text)
    global_detail = await client.post("/semantic/views/@explore", json={})
    assert global_detail.status_code == 200, global_detail.text
    assert global_detail.json()["metrics"] == []
    assert global_detail.json()["dimensions"] == []


@pytest.mark.asyncio
async def test_explore_excludes_invalid_and_deactivated_elements(
    client: AsyncClient,
    session: AsyncSession,
):
    await _setup_cube(client)
    metric = await Node.get_by_name(session, "sem.total_amount")
    dimension = await Node.get_by_name(session, "sem.region")
    assert metric and metric.current and dimension
    metric.current.status = NodeStatus.INVALID
    dimension.deactivated_at = datetime.now(UTC)
    await session.commit()
    listing = await client.post("/semantic/views", json={"path": ["sem"]})
    view = listing.json()["views"][0]["name"]
    scope: dict[str, dict[str, str]] = {"additional_configuration": {}}
    detail = await client.post(f"/semantic/views/{view}", json=scope)
    assert detail.status_code == 200, detail.text
    assert detail.json()["metrics"] == []
    assert detail.json()["dimensions"] == []
    rejected = await client.post(
        f"/semantic/views/{view}/compatible-metrics",
        json={
            **scope,
            "selected_metrics": ["sem.total_amount"],
        },
    )
    assert rejected.status_code == 400
