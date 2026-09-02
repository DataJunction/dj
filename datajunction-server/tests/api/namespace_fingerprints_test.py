"""Tests for namespace semantic fingerprint snapshots."""

import asyncio
from unittest import mock

import pytest

from datajunction_server.database.node import Node
from datajunction_server.models.deployment import (
    ColumnSpec,
    CubeSpec,
    DeploymentSpec,
    MetricSpec,
    SourceSpec,
    TransformSpec,
)


@pytest.fixture(autouse=True, scope="module")
def patch_effective_writer_concurrency():
    from datajunction_server.internal.deployment.deployment import settings

    with mock.patch.object(
        settings.__class__,
        "effective_writer_concurrency",
        new_callable=mock.PropertyMock,
        return_value=1,
    ):
        yield


def lunch_kitchen(grill_table: str = "grill_orders") -> DeploymentSpec:
    return DeploymentSpec(
        namespace="lunch.kitchen",
        nodes=[
            SourceSpec(
                name="grill_orders",
                catalog="default",
                schema_="food",
                table=grill_table,
                columns=[ColumnSpec(name="id", type="int")],
                owners=["burger_chef"],
            ),
            TransformSpec(
                name="burger_orders",
                query="SELECT id FROM ${prefix}grill_orders",
                owners=["burger_chef"],
            ),
            MetricSpec(
                name="burger_count",
                query="SELECT COUNT(*) FROM ${prefix}burger_orders",
                owners=["burger_chef"],
            ),
            CubeSpec(
                name="picnic_basket",
                metrics=["${prefix}burger_count"],
                owners=["picnic_planner"],
            ),
            SourceSpec(
                name="milkshake_orders",
                catalog="default",
                schema_="food",
                table="milkshake_orders",
                columns=[ColumnSpec(name="id", type="int")],
                owners=["dessert_chef"],
            ),
            MetricSpec(
                name="milkshake_count",
                query="SELECT COUNT(*) FROM ${prefix}milkshake_orders",
                owners=["dessert_chef"],
            ),
        ],
    )


def taco_truck() -> DeploymentSpec:
    return DeploymentSpec(
        namespace="lunch.taco_truck",
        nodes=[
            TransformSpec(
                name="taco_orders",
                query="SELECT id FROM lunch.kitchen.grill_orders",
                owners=["taco_chef"],
            ),
            MetricSpec(
                name="taco_count",
                query="SELECT COUNT(*) FROM ${prefix}taco_orders",
                owners=["taco_chef"],
            ),
        ],
    )


async def deploy(client, *specs: DeploymentSpec) -> None:
    for spec in specs:
        response = await client.post(
            "/deployments",
            json=spec.model_dump(by_alias=True),
        )
        assert response.status_code == 200
        deployment_id = response.json()["uuid"]
        for _ in range(300):
            result = await client.get(f"/deployments/{deployment_id}")
            if result.json()["status"] in ("success", "failed"):
                assert result.json()["status"] == "success"
                break
            await asyncio.sleep(0.1)
        else:
            pytest.fail(f"Deployment {deployment_id} did not finish")


async def snapshot(client, namespace: str) -> dict:
    response = await client.get(
        f"/namespaces/{namespace}/semantic-fingerprints?version=1",
    )
    assert response.status_code == 200
    return response.json()


def fingerprint_map(*snapshots: dict) -> dict[str, dict | str]:
    return {
        node["name"]: node["semantic_fingerprint"]
        for namespace_snapshot in snapshots
        for node in namespace_snapshot["nodes"]
    }


@pytest.mark.asyncio
async def test_git_backed_lunch_fingerprints_match_deployment_impact(
    client_with_roads,
):
    initial_kitchen = lunch_kitchen()
    initial_taco_truck = taco_truck()
    await deploy(client_with_roads, initial_kitchen, initial_taco_truck)

    root_git = await client_with_roads.patch(
        "/namespaces/lunch/git",
        json={
            "github_repo_path": "food-festival/lunch-menu",
            "default_branch": "main",
        },
    )
    assert root_git.status_code == 200
    kitchen_git = await client_with_roads.patch(
        "/namespaces/lunch.kitchen/git",
        json={"git_branch": "kitchen", "parent_namespace": "lunch"},
    )
    assert kitchen_git.status_code == 200
    taco_truck_git = await client_with_roads.patch(
        "/namespaces/lunch.taco_truck/git",
        json={"git_branch": "taco-truck", "parent_namespace": "lunch"},
    )
    assert taco_truck_git.status_code == 200

    before_lunch = await snapshot(client_with_roads, "lunch")
    assert before_lunch["namespace"] == "lunch"
    assert before_lunch["version"] == 1
    assert [node["name"] for node in before_lunch["nodes"]] == sorted(
        node.rendered_name
        for spec in (initial_kitchen, initial_taco_truck)
        for node in spec.nodes
    )
    assert {node["name"]: node["owners"] for node in before_lunch["nodes"]} == {
        "lunch.kitchen.burger_count": ["burger_chef"],
        "lunch.kitchen.burger_orders": ["burger_chef"],
        "lunch.kitchen.grill_orders": ["burger_chef"],
        "lunch.kitchen.milkshake_count": ["dessert_chef"],
        "lunch.kitchen.milkshake_orders": ["dessert_chef"],
        "lunch.kitchen.picnic_basket": ["picnic_planner"],
        "lunch.taco_truck.taco_count": ["taco_chef"],
        "lunch.taco_truck.taco_orders": ["taco_chef"],
    }
    assert fingerprint_map(before_lunch) == {
        "lunch.kitchen.burger_count": {
            "version": 1,
            "digest": "51876c5b1c1dafdcc1830730b9e5f5c41267daf51f94a3cec564d84b5cc0fbf7",
        },
        "lunch.kitchen.burger_orders": {
            "version": 1,
            "digest": "0169918baf5a92f4545d6476214bdb2aa2d2975174c5e8e75c49785482f51ce1",
        },
        "lunch.kitchen.grill_orders": {
            "version": 1,
            "digest": "fa4e66f1924973b8cca8aeb59752e2fe90066bdfc45894d9a31cac5c7b0c4d2c",
        },
        "lunch.kitchen.milkshake_count": {
            "version": 1,
            "digest": "0438abad1a7bf147dc708a33ea20dac22b0d036647cea263d531f5a3a3e15a65",
        },
        "lunch.kitchen.milkshake_orders": {
            "version": 1,
            "digest": "c613ea9bcd613edabebd0b7e09840e641652aed1a119817e4a524da88ec80f57",
        },
        "lunch.kitchen.picnic_basket": {
            "version": 1,
            "digest": "2e506807af2296c51a63cca66e8ecb443385d482173161fe7b625cf6a7467aa8",
        },
        "lunch.taco_truck.taco_count": {
            "version": 1,
            "digest": "9d92b7b9637148f386fdceb31f84c862ebcacf47a3098d93c9ccf8c57467c0d2",
        },
        "lunch.taco_truck.taco_orders": {
            "version": 1,
            "digest": "0169918baf5a92f4545d6476214bdb2aa2d2975174c5e8e75c49785482f51ce1",
        },
    }

    proposed_kitchen = lunch_kitchen(grill_table="grill_orders_v2")
    impact_response = await client_with_roads.post(
        "/deployments/impact",
        json=proposed_kitchen.model_dump(by_alias=True),
    )
    assert impact_response.status_code == 200
    impact = impact_response.json()
    predicted = {
        result["name"]: result["semantic_fingerprint"]
        for result in impact["results"]
        if result["deploy_type"] == "node"
    }
    predicted.update(
        {
            downstream["name"]: downstream["semantic_fingerprint"]
            for downstream in impact["downstream_impacts"]
            if downstream["semantic_fingerprint"] is not None
        },
    )

    await deploy(client_with_roads, proposed_kitchen)
    after_lunch = await snapshot(client_with_roads, "lunch")
    before = fingerprint_map(before_lunch)
    after = fingerprint_map(after_lunch)
    changed = {name for name in before if before[name] != after[name]}

    assert changed == {
        "lunch.kitchen.burger_count",
        "lunch.kitchen.burger_orders",
        "lunch.kitchen.grill_orders",
        "lunch.kitchen.picnic_basket",
        "lunch.taco_truck.taco_count",
        "lunch.taco_truck.taco_orders",
    }
    assert all(predicted[name] == after[name] for name in changed)
    assert (
        before["lunch.kitchen.milkshake_count"]
        == after["lunch.kitchen.milkshake_count"]
    )
    assert (
        before["lunch.kitchen.milkshake_orders"]
        == after["lunch.kitchen.milkshake_orders"]
    )


@pytest.mark.asyncio
async def test_namespace_snapshot_uses_unknown_for_legacy_invalid_sql(
    client_with_roads,
    session,
):
    spec = DeploymentSpec(
        namespace="lunch.mystery",
        nodes=[
            SourceSpec(
                name="grill",
                catalog="default",
                schema_="food",
                table="grill",
                columns=[ColumnSpec(name="id", type="int")],
            ),
            TransformSpec(
                name="mystery_meat",
                query="SELECT id FROM ${prefix}grill",
            ),
            TransformSpec(
                name="customer",
                query="SELECT id FROM ${prefix}mystery_meat",
            ),
        ],
    )
    await deploy(client_with_roads, spec)

    mystery_meat = await Node.get_by_name(session, "lunch.mystery.mystery_meat")
    assert mystery_meat is not None
    mystery_meat.current.query = "SELECT ("
    await session.commit()

    data = await snapshot(client_with_roads, "lunch.mystery")
    fingerprints = fingerprint_map(data)
    assert fingerprints["lunch.mystery.mystery_meat"] == "unknown"
    assert fingerprints["lunch.mystery.customer"] == "unknown"


@pytest.mark.asyncio
async def test_namespace_snapshot_rejects_unsupported_version(client_with_roads):
    response = await client_with_roads.get(
        "/namespaces/default/semantic-fingerprints?version=2",
    )

    assert response.status_code == 422
    assert response.json()["message"] == "Unsupported semantic fingerprint version: 2"
