"""API round trip for metric filter requirements."""

import pytest


async def _revenue_query(client) -> str:
    response = await client.get("/nodes/v3.total_revenue/")
    assert response.status_code == 200
    return response.json()["query"]


@pytest.mark.asyncio
async def test_create_metric_with_filter_requirements(client_with_build_v3):
    client = client_with_build_v3
    response = await client.post(
        "/nodes/metric/",
        json={
            "name": "v3.rf_metric",
            "description": "metric with filter requirements",
            "query": await _revenue_query(client),
            "mode": "published",
            "required_filters": [
                {
                    "dimension": "v3.date.date_id[order]",
                    "default": "v3.date.date_id[order] = 20250101",
                },
            ],
            "recommended_filters": [{"dimension": ["v3.location.country[from]"]}],
        },
    )
    assert response.status_code == 201, response.text
    node = (await client.get("/nodes/v3.rf_metric/")).json()
    assert node["required_filters"] == [
        {
            "dimension": ["v3.date.date_id[order]"],
            "default": "v3.date.date_id[order] = 20250101",
        },
    ]
    assert node["recommended_filters"] == [
        {"dimension": ["v3.location.country[from]"], "default": None},
    ]
    metric = (await client.get("/metrics/v3.rf_metric/")).json()
    assert metric["required_filters"] == node["required_filters"]


@pytest.mark.asyncio
async def test_unset_fields_are_null(client_with_build_v3):
    node = (await client_with_build_v3.get("/nodes/v3.total_revenue/")).json()
    assert node["required_filters"] is None
    assert node["recommended_filters"] is None


@pytest.mark.asyncio
async def test_update_creates_major_revision_only_when_changed(client_with_build_v3):
    client = client_with_build_v3
    await client.post(
        "/nodes/metric/",
        json={
            "name": "v3.rf_update",
            "description": "d",
            "query": await _revenue_query(client),
            "mode": "published",
            "required_filters": [{"dimension": "v3.date.date_id[order]"}],
        },
    )
    first = (await client.get("/nodes/v3.rf_update/")).json()["version"]

    same = await client.patch(
        "/nodes/v3.rf_update/",
        json={"required_filters": [{"dimension": ["v3.date.date_id[order]"]}]},
    )
    assert same.json()["version"] == first

    changed = await client.patch(
        "/nodes/v3.rf_update/",
        json={"required_filters": [{"dimension": "v3.location.country[from]"}]},
    )
    assert changed.json()["version"] == "v2.0"
    assert changed.json()["required_filters"] == [
        {"dimension": ["v3.location.country[from]"], "default": None},
    ]

    carried = await client.patch("/nodes/v3.rf_update/", json={"description": "x"})
    assert carried.json()["required_filters"] == changed.json()["required_filters"]


@pytest.mark.asyncio
async def test_non_metric_rejects_filter_requirements(client_with_build_v3):
    response = await client_with_build_v3.post(
        "/nodes/transform/",
        json={
            "name": "v3.rf_transform",
            "description": "d",
            "query": "SELECT 1 AS x",
            "mode": "published",
            "required_filters": [{"dimension": "v3.date.date_id"}],
        },
    )
    assert response.status_code == 422
