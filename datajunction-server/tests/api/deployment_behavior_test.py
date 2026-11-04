"""YAML edits must agree across deployment preview, persisted state, and SQL."""

import asyncio
from pathlib import Path
from unittest import mock

import duckdb
import pytest
import yaml

from datajunction_server.models.deployment import DeploymentSpec
from tests.conftest import transpile_to_duckdb


@pytest.fixture(autouse=True, scope="module")
def serial_deployment_writes():
    from datajunction_server.internal.deployment.deployment import settings

    with mock.patch.object(
        settings.__class__,
        "effective_writer_concurrency",
        new_callable=mock.PropertyMock,
        return_value=1,
    ):
        yield


@pytest.fixture
def project():
    path = Path(__file__).parents[1] / "fixtures" / "deployment_behavior.yaml"
    return yaml.safe_load(path.read_text())


@pytest.fixture
def warehouse():
    """Fixed inputs make stale SQL observable as wrong values, not just text."""
    with duckdb.connect(":memory:") as connection:
        connection.execute(
            """
            ATTACH ':memory:' AS "default";
            CREATE SCHEMA "default".yaml_behavior;
            CREATE TABLE "default".yaml_behavior.raw_orders AS
                SELECT * FROM (VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30))
                AS orders(id, region_id, amount);
            CREATE TABLE "default".yaml_behavior.updated_orders AS
                SELECT id, region_id, amount * 10 AS amount
                FROM "default".yaml_behavior.raw_orders;
            CREATE TABLE "default".yaml_behavior.regions AS
                SELECT * FROM (VALUES (1, 'North'), (2, 'South')) AS regions(id, label);
            CREATE TABLE "default".yaml_behavior.updated_regions AS
                SELECT * FROM (VALUES (1, 'East'), (2, 'West')) AS regions(id, label);
            """,
        )
        yield connection


async def submit(client, project, *, preview=False):
    # Parse the same authored YAML representation on every submission. This also
    # exercises namespace rendering instead of posting already-qualified nodes.
    spec = DeploymentSpec.model_validate(project)
    response = await client.post(
        "/deployments/impact" if preview else "/deployments",
        json=spec.model_dump(mode="json", by_alias=True),
        headers={"Prefer": "respond-async"},
    )
    assert response.status_code == 200, response.text
    data = response.json()
    async with asyncio.timeout(30):
        while data["status"] not in {"success", "failed"}:
            await asyncio.sleep(0.1)
            response = await client.get(f"/deployments/{data['uuid']}")
            assert response.status_code == 200, response.text
            data = response.json()
    assert data["status"] == "success", data
    results = {
        result["name"].removeprefix(f"{spec.namespace}."): result
        for result in data["results"]
        if result["deploy_type"] == "node"
    }
    assert set(results) == {node["name"] for node in project["nodes"]}
    assert all(
        result["status"] in {"success", "skipped"} for result in results.values()
    ), results
    return results


async def revisions(client, project):
    result = {}
    for node in project["nodes"]:
        response = await client.get(f"/nodes/{project['namespace']}.{node['name']}/")
        assert response.status_code == 200, response.text
        data = response.json()
        assert data["status"] == "valid", data
        result[node["name"]] = (data["node_revision_id"], data["version"])
    return result


async def cube_values(client, project, warehouse):
    cube = next(node for node in project["nodes"] if node["name"] == "revenue_cube")
    prefix = f"{project['namespace']}."
    response = await client.get(
        "/sql/metrics/v3/",
        params={
            "cube": f"{prefix}revenue_cube",
            "metrics": [
                metric.replace("${prefix}", prefix) for metric in cube["metrics"]
            ],
            "dimensions": [
                dimension.replace("${prefix}", prefix)
                for dimension in cube["dimensions"]
            ],
            "use_materialized": False,
        },
    )
    assert response.status_code == 200, response.text
    return set(
        warehouse.execute(transpile_to_duckdb(response.json()["sql"])).fetchall(),
    )


@pytest.mark.parametrize(
    "edited_node,field,value,tier,semantic_changes,expected_values",
    [
        pytest.param(
            "raw_orders",
            "table",
            "updated_orders",
            "major",
            {"raw_orders", "orders", "revenue", "order_count", "revenue_cube"},
            {("North", 300), ("South", 300)},
            id="transitive-source-table",
        ),
        pytest.param(
            "orders",
            "query",
            "SELECT id, region_id, amount FROM ${prefix}raw_orders WHERE amount >= 20",
            "major",
            {"orders", "revenue", "order_count", "revenue_cube"},
            {("North", 20), ("South", 30)},
            id="upstream-transform-query",
        ),
        pytest.param(
            "raw_orders",
            "columns",
            [
                {"name": "id", "type": "int"},
                {"name": "region_id", "type": "int"},
                {"name": "amount", "type": "int"},
                {"name": "note", "type": "string"},
            ],
            "major",
            {"raw_orders", "orders", "revenue", "order_count", "revenue_cube"},
            {("North", 30), ("South", 30)},
            id="upstream-source-schema",
        ),
        pytest.param(
            "region_source",
            "table",
            "updated_regions",
            "major",
            {
                "region_source",
                "regions",
                "orders",
                "revenue",
                "order_count",
                "revenue_cube",
            },
            {("East", 30), ("West", 30)},
            id="transitive-dimension-source",
        ),
        pytest.param(
            "regions",
            "query",
            "SELECT id, CONCAT(label, '!') AS label FROM ${prefix}region_source",
            "major",
            {"regions", "orders", "revenue", "order_count", "revenue_cube"},
            {("North!", 30), ("South!", 30)},
            id="upstream-dimension-query",
        ),
        pytest.param(
            "revenue",
            "query",
            "SELECT SUM(amount * 2) FROM ${prefix}orders",
            "major",
            {"revenue", "revenue_cube"},
            {("North", 60), ("South", 60)},
            id="direct-metric-query",
        ),
        pytest.param(
            "revenue_cube",
            "filters",
            ["${prefix}regions.label = 'North'"],
            "major",
            {"revenue_cube"},
            {("North", 30)},
            id="direct-cube-filter",
        ),
        pytest.param(
            "revenue_cube",
            "metrics",
            ["${prefix}revenue", "${prefix}order_count"],
            "major",
            {"revenue_cube"},
            {("North", 30, 2), ("South", 30, 1)},
            id="direct-cube-metric-list",
        ),
        pytest.param(
            "revenue_cube",
            "dimensions",
            [],
            "major",
            {"revenue_cube"},
            {(60,)},
            id="direct-cube-dimension-list",
        ),
        pytest.param(
            "revenue",
            "description",
            "Revenue in dollars",
            "minor",
            set(),
            {("North", 30), ("South", 30)},
            id="metadata-without-semantic-change",
        ),
        pytest.param(
            "unrelated",
            "table",
            "updated_unrelated",
            "major",
            {"unrelated"},
            {("North", 30), ("South", 30)},
            id="unrelated-branch",
        ),
        pytest.param(
            "revenue",
            "query",
            "SELECT SUM( amount )\nFROM ${prefix}orders",
            "none",
            set(),
            {("North", 30), ("South", 30)},
            id="whitespace-only-sql-is-noop",
        ),
    ],
)
async def test_yaml_changes_reach_preview_deployment_and_sql(
    client,
    project,
    warehouse,
    edited_node,
    field,
    value,
    tier,
    semantic_changes,
    expected_values,
):
    initial = await submit(client, project)
    before = await revisions(client, project)
    assert await cube_values(client, project, warehouse) == {
        ("North", 30),
        ("South", 30),
    }

    edited = next(node for node in project["nodes"] if node["name"] == edited_node)
    edited[field] = value
    # File order is not dependency order and must not create or conceal changes.
    project["nodes"].reverse()
    preview = await submit(client, project, preview=True)
    assert await revisions(client, project) == before

    deployed = await submit(client, project)
    after = await revisions(client, project)
    expected_updates = {edited_node} if tier != "none" else set()
    if expected_updates and edited_node != "unrelated":
        expected_updates.add("revenue_cube")
    for name, result in deployed.items():
        # Preview and execution must describe the same proposed graph, including
        # unchanged intermediates whose fingerprints inherit upstream edits.
        for key in (
            "operation",
            "change_tier",
            "semantic_fingerprint",
            "changed_fields",
        ):
            assert result[key] == preview[name][key], (name, key, result, preview[name])
        assert result["semantic_fingerprint"] not in (None, "unknown")
        assert (
            result["semantic_fingerprint"] != initial[name]["semantic_fingerprint"]
        ) == (name in semantic_changes), name
        if name in expected_updates:
            assert result["operation"] == "update", result
            assert result["change_tier"] == tier, result
            assert after[name][0] != before[name][0], name
            assert after[name][1] == ("v2.0" if tier == "major" else "v1.1"), name
        else:
            assert result["operation"] == "noop", result
            assert result["change_tier"] == "none", result
            assert after[name] == before[name], name
    if tier != "none":
        assert field in deployed[edited_node]["changed_fields"]
    persisted = await client.get(f"/nodes/{project['namespace']}.{edited_node}/")
    assert persisted.status_code == 200, persisted.text
    if field in {"table", "description"}:
        assert persisted.json()[field] == value
    elif field == "columns":
        assert {column["name"] for column in persisted.json()["columns"]} == {
            column["name"] for column in value
        }
    assert await cube_values(client, project, warehouse) == expected_values

    repeated = await submit(client, project)
    assert all(result["operation"] == "noop" for result in repeated.values()), repeated
    assert all(result["change_tier"] == "none" for result in repeated.values()), (
        repeated
    )
    assert {
        name: result["semantic_fingerprint"] for name, result in repeated.items()
    } == {name: result["semantic_fingerprint"] for name, result in deployed.items()}
    assert await revisions(client, project) == after
    assert await cube_values(client, project, warehouse) == expected_values
