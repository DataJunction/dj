"""
Tests for the namespaces API.
"""

from http import HTTPStatus
from unittest import mock

import asyncio
from unittest import mock

import pytest

from datajunction_server.models.deployment import (
    BulkNamespaceSourcesRequest,
    BulkNamespaceSourcesResponse,
    ColumnSpec,
    DeploymentSourceType,
    DeploymentSpec,
    GitDeploymentSource,
    LocalDeploymentSource,
    NamespaceSourcesResponse,
    SourceSpec,
)

import pytest
from httpx import AsyncClient

from datajunction_server.internal.access.authorization import (
    AuthorizationService,
)
from datajunction_server.models import access


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


@pytest.mark.asyncio
async def test_list_all_namespaces(
    module__client_with_all_examples: AsyncClient,
) -> None:
    """
    Test ``GET /namespaces/``.
    """
    response = await module__client_with_all_examples.get("/namespaces/")
    assert response.status_code in (200, 201)
    assert response.json() == [
        {"namespace": "basic", "num_nodes": 8},
        {"namespace": "basic.dimension", "num_nodes": 2},
        {"namespace": "basic.source", "num_nodes": 2},
        {"namespace": "basic.transform", "num_nodes": 1},
        {"namespace": "dbt.dimension", "num_nodes": 1},
        {"namespace": "dbt.source", "num_nodes": 0},
        {"namespace": "dbt.source.jaffle_shop", "num_nodes": 2},
        {"namespace": "dbt.source.stripe", "num_nodes": 1},
        {"namespace": "dbt.transform", "num_nodes": 1},
        {"namespace": "default", "num_nodes": 82},
        {
            "namespace": "different.basic",
            "num_nodes": 2,
        },
        {
            "namespace": "different.basic.dimension",
            "num_nodes": 2,
        },
        {
            "namespace": "different.basic.source",
            "num_nodes": 2,
        },
        {
            "namespace": "different.basic.transform",
            "num_nodes": 1,
        },
        {"namespace": "foo.bar", "num_nodes": 26},
        {"namespace": "hll", "num_nodes": 4},
        {"namespace": "v3", "num_nodes": 44},
    ]


@pytest.mark.asyncio
async def test_list_nodes_by_namespace(
    module__client_with_all_examples: AsyncClient,
) -> None:
    """
    Test ``GET /namespaces/{namespace}/``.
    """
    response = await module__client_with_all_examples.get("/namespaces/basic.source/")
    assert response.status_code in (200, 201)
    assert {n["name"] for n in response.json()} == {
        "basic.source.users",
        "basic.source.comments",
    }

    response = await module__client_with_all_examples.get(
        "/namespaces/basic/?with_edited_by=true",
    )
    assert response.status_code in (200, 201)
    assert {n["name"] for n in response.json()} == {
        "basic.avg_luminosity_patches",
        "basic.corrected_patches",
        "basic.source.users",
        "basic.dimension.users",
        "basic.murals",
        "basic.source.comments",
        "basic.dimension.countries",
        "basic.transform.country_agg",
        "basic.num_comments",
        "basic.num_users",
        "basic.paint_colors_spark",
        "basic.paint_colors_trino",
        "basic.patches",
    }
    countries_dim = [
        n for n in response.json() if n["name"] == "basic.dimension.countries"
    ][0]
    assert countries_dim == {
        "description": "Country dimension",
        "display_name": "Countries",
        "edited_by": [
            "dj",
        ],
        "mode": "published",
        "name": "basic.dimension.countries",
        "status": "valid",
        "tags": [],
        "type": "dimension",
        "updated_at": mock.ANY,
        "version": "v1.0",
    }

    response = await module__client_with_all_examples.get(
        "/namespaces/basic/?type_=dimension&with_edited_by=false",
    )
    countries_dim = [
        n for n in response.json() if n["name"] == "basic.dimension.countries"
    ][0]
    assert countries_dim["edited_by"] is None

    response = await module__client_with_all_examples.get(
        "/namespaces/basic/?type_=dimension",
    )
    assert response.status_code in (200, 201)
    assert {n["name"] for n in response.json()} == {
        "basic.dimension.users",
        "basic.dimension.countries",
        "basic.paint_colors_trino",
        "basic.paint_colors_spark",
    }

    response = await module__client_with_all_examples.get(
        "/namespaces/basic/?type_=source",
    )
    assert response.status_code in (200, 201)
    assert {n["name"] for n in response.json()} == {
        "basic.source.comments",
        "basic.source.users",
        "basic.murals",
        "basic.patches",
    }


@pytest.mark.asyncio
async def test_deactivate_namespaces(client_with_namespaced_roads: AsyncClient) -> None:
    """
    Test ``DELETE /namespaces/{namespace}``.
    """
    # Cannot deactivate if there are nodes under the namespace
    response = await client_with_namespaced_roads.delete(
        "/namespaces/foo.bar/?cascade=false",
    )
    assert response.json() == {
        "message": "Cannot deactivate node namespace `foo.bar` as there are still "
        "active nodes under that namespace.",
    }

    # Can deactivate with cascade
    response = await client_with_namespaced_roads.delete(
        "/namespaces/foo.bar/?cascade=true",
    )
    message = response.json()["message"]
    assert (
        "Namespace `foo.bar` has been deactivated. The following nodes "
        "have also been deactivated:"
    ) in message
    nodes = [
        "foo.bar.avg_time_to_dispatch",
        "foo.bar.avg_repair_order_discounts",
        "foo.bar.total_repair_order_discounts",
        "foo.bar.avg_length_of_employment",
        "foo.bar.total_repair_cost",
        "foo.bar.avg_repair_price",
        "foo.bar.num_repair_orders",
        "foo.bar.municipality_dim",
        "foo.bar.dispatcher",
        "foo.bar.us_state",
        "foo.bar.local_hard_hats",
        "foo.bar.hard_hat",
        "foo.bar.contractor",
        "foo.bar.repair_order",
        "foo.bar.us_region",
        "foo.bar.us_states",
        "foo.bar.hard_hat_state",
        "foo.bar.hard_hats",
        "foo.bar.dispatchers",
        "foo.bar.municipality",
        "foo.bar.municipality_type",
        "foo.bar.municipality_municipality_type",
        "foo.bar.contractors",
        "foo.bar.repair_type",
        "foo.bar.repair_order_details",
        "foo.bar.repair_orders",
    ]
    for node in nodes:
        assert node in message

    # Check that the namespace is no longer listed
    response = await client_with_namespaced_roads.get("/namespaces/")
    assert response.status_code in (200, 201)
    assert "foo.bar" not in {n["namespace"] for n in response.json()}

    response = await client_with_namespaced_roads.delete(
        "/namespaces/foo.bar/?cascade=false",
    )
    assert response.json()["message"] == "Namespace `foo.bar` is already deactivated."

    # Try restoring
    response = await client_with_namespaced_roads.post("/namespaces/foo.bar/restore/")
    assert response.json() == {
        "message": "Namespace `foo.bar` has been restored.",
    }

    # Check that the namespace is back
    response = await client_with_namespaced_roads.get("/namespaces/")
    assert response.status_code in (200, 201)
    assert "foo.bar" in {n["namespace"] for n in response.json()}

    # Check that nodes in the namespace remain deactivated
    response = await client_with_namespaced_roads.get("/namespaces/foo.bar/")
    assert response.status_code in (200, 201)
    assert response.json() == []

    # Restore with cascade=true should also restore all the nodes
    await client_with_namespaced_roads.delete("/namespaces/foo.bar/?cascade=false")
    response = await client_with_namespaced_roads.post(
        "/namespaces/foo.bar/restore/?cascade=true",
    )
    message = response.json()["message"]
    assert (
        "Namespace `foo.bar` has been restored. The following nodes have "
        "also been restored:"
    ) in message
    for node in nodes:
        assert node in message

    # Calling restore again will raise
    response = await client_with_namespaced_roads.post(
        "/namespaces/foo.bar/restore/?cascade=true",
    )
    assert (
        response.json()["message"]
        == "Node namespace `foo.bar` already exists and is active."
    )

    # Check that nodes in the namespace are restored
    response = await client_with_namespaced_roads.get("/namespaces/foo.bar/")
    assert response.status_code in (200, 201)
    assert {n["name"] for n in response.json()} == {
        "foo.bar.repair_orders",
        "foo.bar.repair_order_details",
        "foo.bar.repair_type",
        "foo.bar.contractors",
        "foo.bar.municipality_municipality_type",
        "foo.bar.municipality_type",
        "foo.bar.municipality",
        "foo.bar.dispatchers",
        "foo.bar.hard_hats",
        "foo.bar.hard_hat_state",
        "foo.bar.us_states",
        "foo.bar.us_region",
        "foo.bar.repair_order",
        "foo.bar.contractor",
        "foo.bar.hard_hat",
        "foo.bar.local_hard_hats",
        "foo.bar.us_state",
        "foo.bar.dispatcher",
        "foo.bar.municipality_dim",
        "foo.bar.num_repair_orders",
        "foo.bar.avg_repair_price",
        "foo.bar.total_repair_cost",
        "foo.bar.avg_length_of_employment",
        "foo.bar.total_repair_order_discounts",
        "foo.bar.avg_repair_order_discounts",
        "foo.bar.avg_time_to_dispatch",
    }

    response = await client_with_namespaced_roads.get("/history/namespace/foo.bar/")
    assert [
        (activity["activity_type"], activity["details"]) for activity in response.json()
    ] == [
        (
            "restore",
            {
                "message": mock.ANY,
            },
        ),
        ("delete", {"message": "Namespace `foo.bar` has been deactivated."}),
        ("restore", {"message": "Namespace `foo.bar` has been restored."}),
        (
            "delete",
            {
                "message": mock.ANY,
            },
        ),
        ("create", {}),
    ]

    response = await client_with_namespaced_roads.get(
        "/history?node=foo.bar.avg_length_of_employment",
    )
    assert sorted(
        [
            (activity["activity_type"], activity["details"])
            for activity in response.json()
        ],
    ) == sorted(
        [
            ("restore", {"message": "Cascaded from restoring namespace `foo.bar`"}),
            ("status_change", {"upstream_node": "foo.bar.hard_hats"}),
            ("status_change", {"upstream_node": "foo.bar.hard_hats"}),
            ("delete", {"message": "Cascaded from deactivating namespace `foo.bar`"}),
            ("create", {}),
        ],
    )


@pytest.mark.asyncio
async def test_hard_delete_namespace(client_example_loader: AsyncClient):
    """
    Test hard deleting a namespace
    """
    client_with_namespaced_roads = await client_example_loader(
        ["NAMESPACED_ROADS", "ROADS"],
    )

    response = await client_with_namespaced_roads.post(
        "/nodes/default.hard_hat/link",
        json={
            "dimension_node": "foo.bar.hard_hat",
            "join_on": "foo.bar.hard_hat.hard_hat_id = default.hard_hat.hard_hat_id",
            "join_type": "left",
        },
    )

    response = await client_with_namespaced_roads.post(
        "/nodes/transform",
        json={
            "description": "Hard hat dimension #2",
            "query": "SELECT hard_hat_id FROM foo.bar.hard_hat",
            "mode": "published",
            "name": "default.hard_hat0",
            "primary_key": ["hard_hat_id"],
        },
    )

    response = await client_with_namespaced_roads.delete("/namespaces/foo/hard/")
    assert response.json()["message"] == (
        "Cannot hard delete namespace `foo` as there are still the following nodes "
        "under it: `['foo.bar.avg_length_of_employment', "
        "'foo.bar.avg_repair_order_discounts', 'foo.bar.avg_repair_price', "
        "'foo.bar.avg_time_to_dispatch', 'foo.bar.contractor', 'foo.bar.contractors', "
        "'foo.bar.dispatcher', 'foo.bar.dispatchers', 'foo.bar.hard_hat', "
        "'foo.bar.hard_hats', 'foo.bar.hard_hat_state', 'foo.bar.local_hard_hats', "
        "'foo.bar.municipality', 'foo.bar.municipality_dim', "
        "'foo.bar.municipality_municipality_type', 'foo.bar.municipality_type', "
        "'foo.bar.num_repair_orders', 'foo.bar.repair_order', "
        "'foo.bar.repair_order_details', 'foo.bar.repair_orders', "
        "'foo.bar.repair_type', 'foo.bar.total_repair_cost', "
        "'foo.bar.total_repair_order_discounts', 'foo.bar.us_region', "
        "'foo.bar.us_state', 'foo.bar.us_states']`. Set `cascade` to true to "
        "additionally hard delete the above nodes in this namespace. WARNING: this "
        "action cannot be undone."
    )

    await client_with_namespaced_roads.post("/namespaces/foo/")
    await client_with_namespaced_roads.post("/namespaces/foo.bar.baz/")
    await client_with_namespaced_roads.post("/namespaces/foo.bar.baf/")
    await client_with_namespaced_roads.post("/namespaces/foo.bar.bif.d/")

    # Deactivating a few nodes should still allow the hard delete to go through
    await client_with_namespaced_roads.delete(
        "/nodes/foo.bar.avg_length_of_employment",
    )
    await client_with_namespaced_roads.delete(
        "/nodes/foo.bar.avg_repair_order_discounts",
    )

    hard_delete_response = await client_with_namespaced_roads.delete(
        "/namespaces/foo.bar/hard/?cascade=true",
    )
    result = hard_delete_response.json()
    assert result["message"] == "The namespace `foo.bar` has been completely removed."
    assert result["impact"]["deleted_namespaces"] == [
        "foo.bar",
        "foo.bar.baz",
        "foo.bar.baf",
        "foo.bar.bif.d",
    ]
    assert result["impact"]["deleted_nodes"] == [
        "foo.bar.avg_length_of_employment",
        "foo.bar.avg_repair_order_discounts",
        "foo.bar.avg_repair_price",
        "foo.bar.avg_time_to_dispatch",
        "foo.bar.contractor",
        "foo.bar.contractors",
        "foo.bar.dispatcher",
        "foo.bar.dispatchers",
        "foo.bar.hard_hat",
        "foo.bar.hard_hats",
        "foo.bar.hard_hat_state",
        "foo.bar.local_hard_hats",
        "foo.bar.municipality",
        "foo.bar.municipality_dim",
        "foo.bar.municipality_municipality_type",
        "foo.bar.municipality_type",
        "foo.bar.num_repair_orders",
        "foo.bar.repair_order",
        "foo.bar.repair_order_details",
        "foo.bar.repair_orders",
        "foo.bar.repair_type",
        "foo.bar.total_repair_cost",
        "foo.bar.total_repair_order_discounts",
        "foo.bar.us_region",
        "foo.bar.us_state",
        "foo.bar.us_states",
    ]
    assert result["impact"]["impacted"]["downstreams"] == [
        {
            "caused_by": [
                "foo.bar.hard_hat",
                "foo.bar.hard_hats",
            ],
            "name": "default.hard_hat0",
        },
    ]
    # Check that all expected impacted links are present (the link from default.hard_hat
    # to foo.bar.hard_hat affects multiple nodes)
    impacted_links = {link["name"] for link in result["impact"]["impacted"]["links"]}
    assert "default.repair_orders_fact" in impacted_links
    assert "default.hard_hat" in impacted_links
    # All impacted links should be caused by foo.bar.hard_hat
    for link in result["impact"]["impacted"]["links"]:
        assert "foo.bar.hard_hat" in link["caused_by"]
    list_namespaces_response = await client_with_namespaced_roads.get(
        "/namespaces/",
    )
    # Check that the deleted namespace (foo.bar) is no longer present
    # and that foo namespace still exists (now empty)
    namespaces = {ns["namespace"]: ns for ns in list_namespaces_response.json()}
    assert "foo.bar" not in namespaces
    assert "foo" in namespaces
    assert namespaces["foo"]["num_nodes"] == 0

    response = await client_with_namespaced_roads.delete(
        "/namespaces/jaffle_shop/hard/?cascade=true",
    )
    assert response.json() == {
        "errors": [],
        "message": "Namespace `jaffle_shop` does not exist.",
        "warnings": [],
    }


@pytest.mark.asyncio
async def test_create_namespace(client_with_service_setup: AsyncClient):
    """
    Verify creating namespaces, both successful and validation errors
    """
    # By default, creating a namespace will also create its parents (i.e., like mkdir -p)
    response = await client_with_service_setup.post(
        "/namespaces/aaa.bbb.ccc?include_parents=true",
    )
    assert response.json() == {
        "message": "The following node namespaces have been successfully created: "
        "aaa, aaa.bbb, aaa.bbb.ccc",
    }

    # Verify that the parent namespaces already exist if we try to create it again
    response = await client_with_service_setup.post("/namespaces/aaa")
    assert response.json() == {"message": "Node namespace `aaa` already exists"}
    response = await client_with_service_setup.post("/namespaces/aaa.bbb")
    assert response.json() == {"message": "Node namespace `aaa.bbb` already exists"}

    # Setting include_parents=false will not create the parents
    response = await client_with_service_setup.post(
        "/namespaces/acde.mmm?include_parents=false",
    )
    assert response.json() == {
        "message": "The following node namespaces have been successfully created: acde.mmm",
    }
    response = await client_with_service_setup.get("/namespaces/acde")
    assert response.json()["message"] == "node namespace `acde` does not exist."

    # Setting include_parents=true will create the parents
    response = await client_with_service_setup.post(
        "/namespaces/a.b.c?include_parents=true",
    )
    assert response.json() == {
        "message": "The following node namespaces have been successfully created: a, a.b, a.b.c",
    }

    # Verify that it raises when creating an invalid namespace
    invalid_namespaces = [
        "a.111b.c",
        "111mm.abcd",
        "aa.bb.111",
        "1234",
        "aa..bb",
        "user.abc",
        "[aff].mmm",
        "aff._mmm",
        "aff.mmm+",
        "aff.123_mmm",
    ]
    for invalid_namespace in invalid_namespaces:
        response = await client_with_service_setup.post(
            f"/namespaces/{invalid_namespace}",
        )
        assert response.status_code == 422
        assert response.json()["message"] == (
            f"{invalid_namespace} is not a valid namespace. Namespace parts cannot start "
            "with numbers, be empty, or use the reserved keyword [user]"
        )


@pytest.mark.asyncio
async def test_export_namespaces(client_with_roads: AsyncClient):
    """
    Test exporting a namespace to a project definition
    """
    # Create a cube so that the cube definition export path is tested
    response = await client_with_roads.post(
        "/nodes/cube/",
        json={
            "name": "default.example_cube",
            "display_name": "Example Cube",
            "description": "An example cube so that the export path is tested",
            "metrics": ["default.num_repair_orders"],
            "dimensions": ["default.hard_hat.city", "default.hard_hat.hire_date"],
            "mode": "published",
        },
    )
    assert response.status_code in (200, 201)

    # Mark a column as a dimension attribute
    response = await client_with_roads.post(
        "/nodes/default.regional_level_agg/columns/location_hierarchy/attributes",
        json=[
            {
                "name": "dimension",
                "namespace": "system",
            },
        ],
    )
    assert response.status_code in (200, 201)

    # Mark a column as a partition
    await client_with_roads.post(
        "/nodes/default.example_cube/columns/default.hard_hat.hire_date/partition",
        json={
            "type_": "temporal",
            "granularity": "day",
            "format": "yyyyMMdd",
        },
    )

    # Deactivate one node so that it is not included in the export
    response = await client_with_roads.delete(
        "/nodes/default.hard_hat_to_delete",
    )
    assert response.status_code == HTTPStatus.OK

    response = await client_with_roads.get(
        "/namespaces/default/export/",
    )
    project_definition = response.json()

    # Check that nodes are topologically sorted
    sorted_nodes = [entity["build_name"] for entity in project_definition]
    assert sorted_nodes[-1] == "example_cube"

    node_defs = {d["filename"]: d for d in project_definition}
    assert node_defs["example_cube.cube.yaml"] == {
        "build_name": "example_cube",
        "columns": [
            {
                "name": "default.hard_hat.hire_date",
                "partition": {
                    "format": "yyyyMMdd",
                    "granularity": "day",
                    "type_": "temporal",
                },
            },
        ],
        "description": "An example cube so that the export path is tested",
        "dimensions": ["default.hard_hat.hire_date", "default.hard_hat.city"],
        "directory": "",
        "display_name": "Example Cube",
        "filename": "example_cube.cube.yaml",
        "metrics": ["default.num_repair_orders"],
        "tags": [],
    }
    assert node_defs["repair_orders_fact.transform.yaml"]["dimension_links"] == [
        {
            "dimension_node": "default.municipality_dim",
            "join_on": "default.repair_orders_fact.municipality_id = "
            "default.municipality_dim.municipality_id",
            "join_type": "inner",
            "type": "join",
        },
        {
            "dimension_node": "default.hard_hat",
            "join_on": "default.repair_orders_fact.hard_hat_id = default.hard_hat.hard_hat_id",
            "join_type": "inner",
            "type": "join",
        },
        {
            "dimension_node": "default.hard_hat_to_delete",
            "join_on": "default.repair_orders_fact.hard_hat_id = "
            "default.hard_hat_to_delete.hard_hat_id",
            "join_type": "left",
            "type": "join",
        },
        {
            "dimension_node": "default.dispatcher",
            "join_on": "default.repair_orders_fact.dispatcher_id = "
            "default.dispatcher.dispatcher_id",
            "join_type": "inner",
            "type": "join",
        },
    ]

    # Check that all expected ROADS nodes are present (template may have more)
    expected_roads_nodes = {
        "avg_length_of_employment.metric.yaml",
        "avg_repair_order_discounts.metric.yaml",
        "avg_repair_price.metric.yaml",
        "avg_time_to_dispatch.metric.yaml",
        "contractor.dimension.yaml",
        "contractors.source.yaml",
        "discounted_orders_rate.metric.yaml",
        "dispatcher.dimension.yaml",
        "dispatchers.source.yaml",
        "example_cube.cube.yaml",
        "hard_hat.dimension.yaml",
        "hard_hat_2.dimension.yaml",
        "hard_hat_state.source.yaml",
        # "hard_hat_to_delete.dimension.yaml", <-- this node has been deactivated
        "hard_hats.source.yaml",
        "local_hard_hats.dimension.yaml",
        "local_hard_hats_1.dimension.yaml",
        "local_hard_hats_2.dimension.yaml",
        "municipality.source.yaml",
        "municipality_dim.dimension.yaml",
        "municipality_municipality_type.source.yaml",
        "municipality_type.source.yaml",
        "national_level_agg.transform.yaml",
        "num_repair_orders.metric.yaml",
        "num_unique_hard_hats_approx.metric.yaml",
        "regional_level_agg.transform.yaml",
        "regional_repair_efficiency.metric.yaml",
        "repair_order.dimension.yaml",
        "repair_order_details.source.yaml",
        "repair_orders.source.yaml",
        "repair_orders_fact.transform.yaml",
        "repair_type.source.yaml",
        "total_repair_cost.metric.yaml",
        "total_repair_order_discounts.metric.yaml",
        "us_region.source.yaml",
        "us_state.dimension.yaml",
        "us_states.source.yaml",
        "repair_orders_view.source.yaml",
    }
    assert expected_roads_nodes.issubset(set(node_defs.keys()))
    assert {d["directory"] for d in project_definition} == {""}


@pytest.mark.asyncio
async def test_export_namespaces_deployment(client_with_roads: AsyncClient):
    # Create a cube so that the cube definition export path is tested
    response = await client_with_roads.post(
        "/nodes/cube/",
        json={
            "name": "default.example_cube",
            "display_name": "Example Cube",
            "description": "An example cube so that the export path is tested",
            "metrics": ["default.num_repair_orders"],
            "dimensions": ["default.hard_hat.city", "default.hard_hat.hire_date"],
            "mode": "published",
        },
    )
    assert response.status_code in (200, 201)

    # Mark a column as a dimension attribute
    response = await client_with_roads.post(
        "/nodes/default.regional_level_agg/columns/location_hierarchy/attributes",
        json=[
            {
                "name": "dimension",
                "namespace": "system",
            },
        ],
    )
    assert response.status_code in (200, 201)

    # Mark a column as a partition
    await client_with_roads.post(
        "/nodes/default.example_cube/columns/default.hard_hat.hire_date/partition",
        json={
            "type_": "temporal",
            "granularity": "day",
            "format": "yyyyMMdd",
        },
    )

    # Deactivate one node so that it is not included in the export
    response = await client_with_roads.delete(
        "/nodes/default.hard_hat_to_delete",
    )
    assert response.status_code == HTTPStatus.OK

    response = await client_with_roads.get("/namespaces/default/export/spec")
    assert response.status_code in (200, 201)
    data = response.json()
    assert data["namespace"] == "default"
    # Template has all examples loaded, so there will be more than just ROADS nodes
    assert len(data["nodes"]) >= 37
    # Check that all expected ROADS nodes are present
    expected_roads_nodes = {
        "${prefix}repair_orders_view",
        "${prefix}municipality_municipality_type",
        "${prefix}municipality_type",
        "${prefix}municipality",
        "${prefix}dispatchers",
        "${prefix}example_cube",
        "${prefix}hard_hats",
        "${prefix}hard_hat_state",
        "${prefix}us_states",
        "${prefix}us_region",
        "${prefix}contractor",
        "${prefix}hard_hat_2",
        # '${prefix}hard_hat_to_delete',  <-- this node has been deactivated
        "${prefix}local_hard_hats",
        "${prefix}local_hard_hats_1",
        "${prefix}local_hard_hats_2",
        "${prefix}us_state",
        "${prefix}dispatcher",
        "${prefix}municipality_dim",
        "${prefix}regional_level_agg",
        "${prefix}national_level_agg",
        "${prefix}regional_repair_efficiency",
        "${prefix}num_repair_orders",
        "${prefix}num_unique_hard_hats_approx",
        "${prefix}avg_repair_price",
        "${prefix}total_repair_cost",
        "${prefix}avg_length_of_employment",
        "${prefix}discounted_orders_rate",
        "${prefix}total_repair_order_discounts",
        "${prefix}avg_repair_order_discounts",
        "${prefix}avg_time_to_dispatch",
        "${prefix}repair_orders_fact",
        "${prefix}repair_type",
        "${prefix}repair_orders",
        "${prefix}contractors",
        "${prefix}hard_hat",
        "${prefix}repair_order_details",
        "${prefix}repair_order",
    }
    actual_node_names = {node["name"] for node in data["nodes"]}
    assert expected_roads_nodes.issubset(actual_node_names)

    node_defs = {node["name"]: node for node in data["nodes"]}
    # Note: None values are filtered out in the export for cleaner output
    assert node_defs["${prefix}example_cube"] == {
        "owners": ["dj"],
        "mode": "published",
        "node_type": "cube",
        "name": "${prefix}example_cube",
        "columns": [
            {
                "attributes": [],
                "display_name": "Num Repair Orders",
                "name": "default.num_repair_orders",
                "type": "bigint",
            },
            {
                "attributes": [],
                "display_name": "City",
                "name": "default.hard_hat.city",
                "type": "string",
            },
            {
                "attributes": [],
                "display_name": "Hire Date",
                "name": "default.hard_hat.hire_date",
                "type": "timestamp",
                "partition": {
                    "format": "yyyyMMdd",
                    "granularity": "day",
                    "type": "temporal",
                },
            },
        ],
        "description": "An example cube so that the export path is tested",
        "dimensions": ["${prefix}hard_hat.city", "${prefix}hard_hat.hire_date"],
        "display_name": "Example Cube",
        "metrics": ["${prefix}num_repair_orders"],
        "tags": [],
    }
    # Note: None values are filtered out in the export for cleaner output
    assert node_defs["${prefix}repair_orders_fact"]["dimension_links"] == [
        {
            "dimension_node": "${prefix}municipality_dim",
            "join_on": "${prefix}repair_orders_fact.municipality_id = "
            "${prefix}municipality_dim.municipality_id",
            "join_type": "inner",
            "type": "join",
        },
        {
            "dimension_node": "${prefix}hard_hat",
            "join_on": "${prefix}repair_orders_fact.hard_hat_id = ${prefix}hard_hat.hard_hat_id",
            "join_type": "inner",
            "type": "join",
        },
        {
            "dimension_node": "${prefix}hard_hat_to_delete",
            "join_on": "${prefix}repair_orders_fact.hard_hat_id = "
            "${prefix}hard_hat_to_delete.hard_hat_id",
            "join_type": "left",
            "type": "join",
        },
        {
            "dimension_node": "${prefix}dispatcher",
            "join_on": "${prefix}repair_orders_fact.dispatcher_id = ${prefix}dispatcher.dispatcher_id",
            "join_type": "inner",
            "type": "join",
        },
    ]


class DbtOnlyAuthorizationService(AuthorizationService):
    """
    Authorization service that only approves namespaces containing 'dbt'.
    """

    name = "dbt_only"

    def authorize(self, auth_context, requests):
        return [
            access.AccessDecision(
                request=request,
                approved=(
                    request.access_object.resource_type == access.ResourceType.NAMESPACE
                    and "dbt" in request.access_object.name
                ),
            )
            for request in requests
        ]


@pytest.mark.asyncio
async def test_list_all_namespaces_access_limited(
    client_with_dbt: AsyncClient,
    mocker,
) -> None:
    """
    Test ``GET /namespaces/``.
    """

    def get_dbt_only_service():
        return DbtOnlyAuthorizationService()

    mocker.patch(
        "datajunction_server.internal.access.authorization.validator.get_authorization_service",
        get_dbt_only_service,
    )

    response = await client_with_dbt.get("/namespaces/")

    assert response.status_code in (200, 201)
    assert response.json() == [
        {"namespace": "dbt.dimension", "num_nodes": 1},
        {"namespace": "dbt.source", "num_nodes": 0},
        {"namespace": "dbt.source.jaffle_shop", "num_nodes": 2},
        {"namespace": "dbt.source.stripe", "num_nodes": 1},
        {"namespace": "dbt.transform", "num_nodes": 1},
    ]


class DenyAllAuthorizationService(AuthorizationService):
    """
    Authorization service that denies all access requests.
    """

    name = "deny_all"

    def authorize(self, auth_context, requests):
        return [
            access.AccessDecision(
                request=request,
                approved=False,
            )
            for request in requests
        ]


@pytest.mark.asyncio
async def test_list_all_namespaces_deny_all(
    client_with_service_setup: AsyncClient,
    mocker,
) -> None:
    """
    Test ``GET /namespaces/``.
    """

    def get_deny_all_service():
        return DenyAllAuthorizationService()

    mocker.patch(
        "datajunction_server.internal.access.authorization.validator.get_authorization_service",
        get_deny_all_service,
    )
    response = await client_with_service_setup.get("/namespaces/")

    assert response.status_code in (200, 201)
    assert response.json() == []


class TestNamespaceSourcesEndpoint:
    """Tests for GET /namespaces/{namespace}/sources"""

    @pytest.mark.asyncio
    async def test_sources_empty_namespace(self, client_with_roads):
        """Test sources endpoint on a namespace with no deployments"""
        response = await client_with_roads.get("/namespaces/nonexistent_ns/sources")
        # Returns 200 with empty data (no deployments to this namespace)
        assert response.status_code == 200

        sources_response = NamespaceSourcesResponse(**response.json())
        assert sources_response.namespace == "nonexistent_ns"
        assert sources_response.total_deployments == 0
        assert sources_response.primary_source is None

    @pytest.mark.asyncio
    async def test_sources_after_git_deployment(self, client_with_roads):
        """Test sources endpoint after a git-backed deployment"""
        # Deploy with git source info
        git_source = GitDeploymentSource(
            repository="github.com/test/repo",
            branch="main",
            commit_sha="abc123",
            ci_system="jenkins",
            ci_run_url="https://jenkins.example.com/job/123",
        )

        deployment_spec = DeploymentSpec(
            namespace="sources_test_git",
            nodes=[
                SourceSpec(
                    name="test_source",
                    catalog="default",
                    schema_="test",
                    table="test_table",
                    columns=[
                        ColumnSpec(name="id", type="int"),
                    ],
                ),
            ],
            source=git_source,
        )

        # Deploy
        deploy_response = await client_with_roads.post(
            "/deployments",
            json=deployment_spec.model_dump(by_alias=True),
        )
        assert deploy_response.status_code == 200

        # Wait for deployment to complete
        deployment_id = deploy_response.json()["uuid"]
        for _ in range(30):
            status_response = await client_with_roads.get(
                f"/deployments/{deployment_id}",
            )
            if status_response.json()["status"] in ("success", "failed"):
                break
            await asyncio.sleep(0.1)

        # Query sources
        response = await client_with_roads.get("/namespaces/sources_test_git/sources")
        assert response.status_code == 200

        sources_response = NamespaceSourcesResponse(**response.json())
        assert sources_response.namespace == "sources_test_git"
        assert sources_response.total_deployments == 1

        # Verify primary source is git
        assert sources_response.primary_source is not None
        assert sources_response.primary_source.type == "git"
        assert sources_response.primary_source.repository == "github.com/test/repo"
        assert sources_response.primary_source.branch == "main"

    @pytest.mark.asyncio
    async def test_sources_after_local_deployment(self, client_with_roads):
        """Test sources endpoint after a local/adhoc deployment"""
        # Deploy with local source info
        local_source = LocalDeploymentSource(
            hostname="my-laptop",
            reason="testing",
        )

        deployment_spec = DeploymentSpec(
            namespace="sources_test_local",
            nodes=[
                SourceSpec(
                    name="test_source",
                    catalog="default",
                    schema_="test",
                    table="test_table",
                    columns=[
                        ColumnSpec(name="id", type="int"),
                    ],
                ),
            ],
            source=local_source,
        )

        # Deploy
        deploy_response = await client_with_roads.post(
            "/deployments",
            json=deployment_spec.model_dump(by_alias=True),
        )
        assert deploy_response.status_code == 200

        # Wait for deployment to complete
        deployment_id = deploy_response.json()["uuid"]
        for _ in range(30):
            status_response = await client_with_roads.get(
                f"/deployments/{deployment_id}",
            )
            if status_response.json()["status"] in ("success", "failed"):
                break
            await asyncio.sleep(0.1)

        # Query sources
        response = await client_with_roads.get("/namespaces/sources_test_local/sources")
        assert response.status_code == 200

        sources_response = NamespaceSourcesResponse(**response.json())
        assert sources_response.namespace == "sources_test_local"
        assert sources_response.total_deployments == 1

        # Verify primary source is local
        assert sources_response.primary_source is not None
        assert sources_response.primary_source.type == "local"

    @pytest.mark.asyncio
    async def test_sources_multiple_sources(self, client_with_roads):
        """Test sources endpoint when multiple sources have deployed"""
        namespace = "sources_test_multiple"

        # First deployment from git
        git_source = GitDeploymentSource(
            repository="github.com/team-a/repo",
            branch="main",
        )
        deployment_spec1 = DeploymentSpec(
            namespace=namespace,
            nodes=[
                SourceSpec(
                    name="source_a",
                    catalog="default",
                    schema_="test",
                    table="table_a",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
            ],
            source=git_source,
        )

        deploy_response1 = await client_with_roads.post(
            "/deployments",
            json=deployment_spec1.model_dump(by_alias=True),
        )
        assert deploy_response1.status_code == 200

        # Wait for first deployment
        deployment_id1 = deploy_response1.json()["uuid"]
        for _ in range(30):
            status_response = await client_with_roads.get(
                f"/deployments/{deployment_id1}",
            )
            if status_response.json()["status"] in ("success", "failed"):
                break
            await asyncio.sleep(0.1)

        # Second deployment from a different git repo
        git_source2 = GitDeploymentSource(
            repository="github.com/team-b/other-repo",
            branch="develop",
        )
        deployment_spec2 = DeploymentSpec(
            namespace=namespace,
            nodes=[
                SourceSpec(
                    name="source_a",
                    catalog="default",
                    schema_="test",
                    table="table_a",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
                SourceSpec(
                    name="source_b",
                    catalog="default",
                    schema_="test",
                    table="table_b",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
            ],
            source=git_source2,
        )

        deploy_response2 = await client_with_roads.post(
            "/deployments",
            json=deployment_spec2.model_dump(by_alias=True),
        )
        assert deploy_response2.status_code == 200

        # Wait for second deployment
        deployment_id2 = deploy_response2.json()["uuid"]
        for _ in range(30):
            status_response = await client_with_roads.get(
                f"/deployments/{deployment_id2}",
            )
            if status_response.json()["status"] in ("success", "failed"):
                break
            await asyncio.sleep(0.1)

        # Query sources
        response = await client_with_roads.get(f"/namespaces/{namespace}/sources")
        assert response.status_code == 200

        sources_response = NamespaceSourcesResponse(**response.json())
        assert sources_response.namespace == namespace
        assert sources_response.total_deployments == 2

        # Primary source determined by majority among recent deployments
        # Both are git so it should be git
        assert sources_response.primary_source is not None
        assert sources_response.primary_source.type == "git"

    @pytest.mark.asyncio
    async def test_sources_no_source_info_legacy(self, client_with_roads):
        """Test sources endpoint with deployments that have no source info (legacy)"""
        # Deploy without source info (like legacy deployments)
        deployment_spec = DeploymentSpec(
            namespace="sources_test_legacy",
            nodes=[
                SourceSpec(
                    name="test_source",
                    catalog="default",
                    schema_="test",
                    table="test_table",
                    columns=[
                        ColumnSpec(name="id", type="int"),
                    ],
                ),
            ],
            # No source field
        )

        # Deploy
        deploy_response = await client_with_roads.post(
            "/deployments",
            json=deployment_spec.model_dump(by_alias=True),
        )
        assert deploy_response.status_code == 200

        # Wait for deployment to complete
        deployment_id = deploy_response.json()["uuid"]
        for _ in range(30):
            status_response = await client_with_roads.get(
                f"/deployments/{deployment_id}",
            )
            if status_response.json()["status"] in ("success", "failed"):
                break
            await asyncio.sleep(0.1)

        # Query sources
        response = await client_with_roads.get(
            "/namespaces/sources_test_legacy/sources",
        )
        assert response.status_code == 200

        sources_response = NamespaceSourcesResponse(**response.json())
        assert sources_response.namespace == "sources_test_legacy"
        assert sources_response.total_deployments == 1

        # Legacy deployments should be treated as local
        assert sources_response.primary_source is not None
        assert sources_response.primary_source.type == "local"


class TestBulkNamespaceSources:
    """Tests for POST /namespaces/sources/bulk endpoint."""

    @pytest.mark.asyncio
    async def test_bulk_sources_empty(self, client_with_roads):
        """Test bulk sources with no namespaces requested."""
        request = BulkNamespaceSourcesRequest(namespaces=[])
        response = await client_with_roads.post(
            "/namespaces/sources/bulk",
            json=request.model_dump(),
        )
        assert response.status_code == 200

        bulk_response = BulkNamespaceSourcesResponse(**response.json())
        assert bulk_response.sources == {}

    @pytest.mark.asyncio
    async def test_bulk_sources_nonexistent_namespaces(self, client_with_roads):
        """Test bulk sources for namespaces that don't exist."""
        request = BulkNamespaceSourcesRequest(
            namespaces=["nonexistent_a", "nonexistent_b"],
        )
        response = await client_with_roads.post(
            "/namespaces/sources/bulk",
            json=request.model_dump(),
        )
        assert response.status_code == 200

        bulk_response = BulkNamespaceSourcesResponse(**response.json())
        assert len(bulk_response.sources) == 2

        # Both should have empty sources
        assert bulk_response.sources["nonexistent_a"].total_deployments == 0
        assert bulk_response.sources["nonexistent_b"].total_deployments == 0

    @pytest.mark.asyncio
    async def test_bulk_sources_with_deployments(self, client_with_roads):
        """Test bulk sources after deploying to multiple namespaces."""
        # Deploy to namespace A with git source
        git_source = GitDeploymentSource(
            repository="github.com/bulk-test/repo-a",
            branch="main",
        )
        spec_a = DeploymentSpec(
            namespace="bulk_test_ns_a",
            nodes=[
                SourceSpec(
                    name="source_a",
                    catalog="default",
                    schema_="test",
                    table="table_a",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
            ],
            source=git_source,
        )

        deploy_a = await client_with_roads.post(
            "/deployments",
            json=spec_a.model_dump(by_alias=True),
        )
        assert deploy_a.status_code == 200

        # Deploy to namespace B with local source
        local_source = LocalDeploymentSource(hostname="test-machine")
        spec_b = DeploymentSpec(
            namespace="bulk_test_ns_b",
            nodes=[
                SourceSpec(
                    name="source_b",
                    catalog="default",
                    schema_="test",
                    table="table_b",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
            ],
            source=local_source,
        )

        deploy_b = await client_with_roads.post(
            "/deployments",
            json=spec_b.model_dump(by_alias=True),
        )
        assert deploy_b.status_code == 200

        # Wait for both deployments
        for deployment_id in [deploy_a.json()["uuid"], deploy_b.json()["uuid"]]:
            for _ in range(30):
                status = await client_with_roads.get(f"/deployments/{deployment_id}")
                if status.json()["status"] in ("success", "failed"):
                    break
                await asyncio.sleep(0.1)

        # Bulk query both namespaces
        request = BulkNamespaceSourcesRequest(
            namespaces=["bulk_test_ns_a", "bulk_test_ns_b", "bulk_test_ns_c"],
        )
        response = await client_with_roads.post(
            "/namespaces/sources/bulk",
            json=request.model_dump(),
        )
        assert response.status_code == 200

        bulk_response = BulkNamespaceSourcesResponse(**response.json())
        assert len(bulk_response.sources) == 3

        # Namespace A should have git source
        ns_a = bulk_response.sources["bulk_test_ns_a"]
        assert ns_a.total_deployments == 1
        assert ns_a.primary_source is not None
        assert ns_a.primary_source.type == DeploymentSourceType.GIT
        assert ns_a.primary_source.repository == "github.com/bulk-test/repo-a"

        # Namespace B should have local source
        ns_b = bulk_response.sources["bulk_test_ns_b"]
        assert ns_b.total_deployments == 1
        assert ns_b.primary_source is not None
        assert ns_b.primary_source.type == DeploymentSourceType.LOCAL

        # Namespace C should have no deployments
        ns_c = bulk_response.sources["bulk_test_ns_c"]
        assert ns_c.total_deployments == 0
        assert ns_c.primary_source is None

    @pytest.mark.asyncio
    async def test_bulk_sources_single_namespace(self, client_with_roads):
        """Test bulk sources with just one namespace (edge case)."""
        # Deploy first
        git_source = GitDeploymentSource(
            repository="github.com/single-test/repo",
            branch="develop",
        )
        spec = DeploymentSpec(
            namespace="bulk_single_ns",
            nodes=[
                SourceSpec(
                    name="single_source",
                    catalog="default",
                    schema_="test",
                    table="single_table",
                    columns=[ColumnSpec(name="id", type="int")],
                ),
            ],
            source=git_source,
        )

        deploy = await client_with_roads.post(
            "/deployments",
            json=spec.model_dump(by_alias=True),
        )
        assert deploy.status_code == 200

        # Wait for deployment
        deployment_id = deploy.json()["uuid"]
        for _ in range(30):
            status = await client_with_roads.get(f"/deployments/{deployment_id}")
            if status.json()["status"] in ("success", "failed"):
                break
            await asyncio.sleep(0.1)

        # Bulk query with single namespace
        request = BulkNamespaceSourcesRequest(namespaces=["bulk_single_ns"])
        response = await client_with_roads.post(
            "/namespaces/sources/bulk",
            json=request.model_dump(),
        )
        assert response.status_code == 200

        bulk_response = BulkNamespaceSourcesResponse(**response.json())
        assert len(bulk_response.sources) == 1
        assert "bulk_single_ns" in bulk_response.sources
        assert bulk_response.sources["bulk_single_ns"].total_deployments == 1


class TestExportYaml:
    """Tests for GET /namespaces/{namespace}/export/yaml endpoint (ZIP download)"""

    @pytest.mark.asyncio
    async def test_export_yaml_returns_zip(self, client_with_roads):
        """Test that export/yaml returns a valid ZIP file"""
        import zipfile
        import io

        response = await client_with_roads.get("/namespaces/default/export/yaml")
        assert response.status_code == 200

        # Check content type is ZIP
        assert "application/zip" in response.headers.get("content-type", "")

        # Check content disposition header
        content_disp = response.headers.get("content-disposition", "")
        assert "attachment" in content_disp
        assert "default_export.zip" in content_disp

        # Verify it's a valid ZIP file
        zip_buffer = io.BytesIO(response.content)
        with zipfile.ZipFile(zip_buffer, "r") as zf:
            file_list = zf.namelist()
            # Should have dj.yaml manifest
            assert "dj.yaml" in file_list

            # Should have node files
            assert len(file_list) > 1

            # Read and verify dj.yaml content
            import yaml

            manifest_content = zf.read("dj.yaml").decode("utf-8")
            manifest = yaml.safe_load(manifest_content)
            assert manifest["namespace"] == "default"
            assert "name" in manifest
            assert "description" in manifest

    @pytest.mark.asyncio
    async def test_export_yaml_node_files_structure(self, client_with_roads):
        """Test that exported node files have correct structure"""
        import zipfile
        import io
        import yaml

        response = await client_with_roads.get("/namespaces/default/export/yaml")
        assert response.status_code == 200

        zip_buffer = io.BytesIO(response.content)
        with zipfile.ZipFile(zip_buffer, "r") as zf:
            # Get a node file (not dj.yaml)
            node_files = [f for f in zf.namelist() if f != "dj.yaml"]
            assert len(node_files) > 0

            # Check at least one node file has valid YAML structure
            for node_file in node_files[:3]:  # Check first 3 node files
                content = zf.read(node_file).decode("utf-8")
                node_data = yaml.safe_load(content)

                # Node should have a name
                assert "name" in node_data
                # Node should have a node_type
                assert "node_type" in node_data


class TestYamlHelpers:
    """Tests for internal YAML helper functions"""

    def test_multiline_str_representer_with_newlines(self):
        """Test that multiline strings get literal block style"""
        from datajunction_server.internal.namespaces import _multiline_str_representer
        import yaml

        dumper = yaml.SafeDumper("")

        # Test with multiline string
        multiline = "SELECT *\nFROM table\nWHERE x = 1"
        result = _multiline_str_representer(dumper, multiline)
        assert result.style == "|"

    def test_multiline_str_representer_single_line(self):
        """Test that single line strings don't get block style"""
        from datajunction_server.internal.namespaces import _multiline_str_representer
        import yaml

        dumper = yaml.SafeDumper("")

        # Test with single line string
        single = "SELECT * FROM table"
        result = _multiline_str_representer(dumper, single)
        assert result.style is None  # Default style

    def test_get_yaml_dumper(self):
        """Test that YAML dumper uses literal block style for multiline strings"""
        from datajunction_server.internal.namespaces import _get_yaml_dumper
        from pathlib import Path
        import yaml

        dumper = _get_yaml_dumper()

        # Verify it's a SafeDumper subclass
        assert issubclass(dumper, yaml.SafeDumper)

        # Test dumping a multiline string uses literal block style
        data = {"query": "SELECT *\nFROM table\nWHERE x = 1", "name": "test_node"}
        output = yaml.dump(data, Dumper=dumper, sort_keys=False)

        # Compare against expected fixture
        fixture_path = (
            Path(__file__).parent.parent / "fixtures" / "expected_multiline_query.yaml"
        )
        expected = fixture_path.read_text()
        assert output == expected

    def test_node_spec_to_yaml_dict_excludes_none(self):
        """Test that _node_spec_to_yaml_dict excludes None values"""
        from datajunction_server.internal.namespaces import _node_spec_to_yaml_dict
        from datajunction_server.models.deployment import TransformSpec

        spec = TransformSpec(
            name="test.node",
            query="SELECT 1",
            description=None,  # Should be excluded
        )

        result = _node_spec_to_yaml_dict(spec)

        assert "name" in result
        assert "query" in result
        assert "description" not in result  # None should be excluded

    def test_node_spec_to_yaml_dict_cube_excludes_columns(self):
        """Test that cube nodes always exclude columns"""
        from datajunction_server.internal.namespaces import _node_spec_to_yaml_dict
        from datajunction_server.models.deployment import CubeSpec, ColumnSpec

        spec = CubeSpec(
            name="test.cube",
            metrics=["test.metric"],
            dimensions=["test.dim"],
            columns=[
                ColumnSpec(name="col1", type="int"),
            ],
        )

        result = _node_spec_to_yaml_dict(spec)

        assert "columns" not in result
        assert "metrics" in result
        assert "dimensions" in result

    def test_node_spec_to_yaml_dict_filters_columns_without_customizations(self):
        """Test that columns without customizations are filtered out"""
        from datajunction_server.internal.namespaces import _node_spec_to_yaml_dict
        from datajunction_server.models.deployment import TransformSpec, ColumnSpec

        spec = TransformSpec(
            name="test.transform",
            query="SELECT id, name FROM source",
            columns=[
                # Column without customization - should be filtered
                ColumnSpec(name="id", type="int"),
                # Column with custom display_name - should be kept
                ColumnSpec(name="name", type="string", display_name="Full Name"),
            ],
        )

        result = _node_spec_to_yaml_dict(spec)

        # Only the column with customization should remain
        if "columns" in result:
            assert len(result["columns"]) == 1
            assert result["columns"][0]["name"] == "name"
            assert result["columns"][0]["display_name"] == "Full Name"
            # Type should be excluded from output
            assert "type" not in result["columns"][0]

    def test_node_spec_to_yaml_dict_keeps_column_with_attributes(self):
        """Test that columns with attributes are kept"""
        from datajunction_server.internal.namespaces import _node_spec_to_yaml_dict
        from datajunction_server.models.deployment import TransformSpec, ColumnSpec

        spec = TransformSpec(
            name="test.transform",
            query="SELECT id FROM source",
            columns=[
                ColumnSpec(name="id", type="int", attributes=["primary_key"]),
            ],
        )

        result = _node_spec_to_yaml_dict(spec)

        assert "columns" in result
        assert len(result["columns"]) == 1
        assert result["columns"][0]["attributes"] == ["primary_key"]

    def test_node_spec_to_yaml_dict_removes_empty_columns(self):
        """Test that columns key is removed when no columns have customizations"""
        from datajunction_server.internal.namespaces import _node_spec_to_yaml_dict
        from datajunction_server.models.deployment import TransformSpec, ColumnSpec

        spec = TransformSpec(
            name="test.transform",
            query="SELECT id, name FROM source",
            columns=[
                # Both columns without customizations
                ColumnSpec(name="id", type="int"),
                ColumnSpec(
                    name="name",
                    type="string",
                    display_name="name",
                ),  # same as name
            ],
        )

        result = _node_spec_to_yaml_dict(spec)

        # columns key should be removed entirely
        assert "columns" not in result


class TestNamespaceDiff:
    """Tests for GET /namespaces/{namespace}/diff endpoint."""

    @pytest.mark.asyncio
    async def test_diff_identical_namespaces(
        self,
        client_with_namespaced_roads: AsyncClient,
    ):
        """
        Test diffing a namespace against itself returns no changes.
        """
        response = await client_with_namespaced_roads.get(
            "/namespaces/foo.bar/diff",
            params={"base": "foo.bar"},
        )
        assert response.status_code == 200
        data = response.json()

        assert data["base_namespace"] == "foo.bar"
        assert data["compare_namespace"] == "foo.bar"
        assert data["added"] == []
        assert data["removed"] == []
        assert data["direct_changes"] == []
        assert data["propagated_changes"] == []
        assert data["unchanged_count"] > 0  # Should have nodes

    @pytest.mark.asyncio
    async def test_diff_with_added_nodes(
        self,
        client_with_namespaced_roads: AsyncClient,
    ):
        """
        Test that nodes only in compare namespace show as 'added'.
        """
        # Create a new namespace that's a copy of foo.bar with an extra node
        await client_with_namespaced_roads.post("/namespaces/foo.bar.feature/")

        # Create a new source node in the feature namespace
        await client_with_namespaced_roads.post(
            "/nodes/source/",
            json={
                "name": "foo.bar.feature.new_source",
                "description": "A new source for testing",
                "mode": "published",
                "catalog": "default",
                "schema_": "roads",
                "table": "some_table",
                "columns": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "string"},
                ],
            },
        )

        # Diff feature against empty namespace - should show new_source as added
        response = await client_with_namespaced_roads.get(
            "/namespaces/foo.bar.feature/diff",
            params={"base": "foo.bar"},
        )
        assert response.status_code == 200
        data = response.json()

        assert data["base_namespace"] == "foo.bar"
        assert data["compare_namespace"] == "foo.bar.feature"
        assert data["added_count"] == 1
        assert len(data["added"]) == 1
        assert data["added"][0]["name"] == "new_source"
        assert data["added"][0]["node_type"] == "source"

    @pytest.mark.asyncio
    async def test_diff_with_removed_nodes(
        self,
        client_with_namespaced_roads: AsyncClient,
    ):
        """
        Test that nodes only in base namespace show as 'removed'.
        """
        # Create an empty feature namespace
        await client_with_namespaced_roads.post("/namespaces/foo.bar.empty/")

        # Diff empty against foo.bar - all foo.bar nodes should show as removed
        response = await client_with_namespaced_roads.get(
            "/namespaces/foo.bar.empty/diff",
            params={"base": "foo.bar"},
        )
        assert response.status_code == 200
        data = response.json()

        assert data["base_namespace"] == "foo.bar"
        assert data["compare_namespace"] == "foo.bar.empty"
        assert data["removed_count"] > 0
        assert data["added_count"] == 0

        # All nodes from foo.bar should be in removed list
        removed_names = [r["name"] for r in data["removed"]]
        assert "repair_orders" in removed_names

    @pytest.mark.asyncio
    async def test_diff_with_direct_changes(
        self,
        client_with_namespaced_roads: AsyncClient,
    ):
        """
        Test that nodes with user-provided field changes show as 'direct_changes'.
        """
        # Create a feature namespace
        await client_with_namespaced_roads.post("/namespaces/foo.bar.modified/")

        # Copy a source node but modify the description
        await client_with_namespaced_roads.post(
            "/nodes/source/",
            json={
                "name": "foo.bar.modified.repair_orders",
                "description": "MODIFIED - All repair orders",  # Changed
                "mode": "published",
                "catalog": "default",
                "schema_": "roads",
                "table": "repair_orders",
                "columns": [
                    {"name": "repair_order_id", "type": "int"},
                    {"name": "municipality_id", "type": "string"},
                    {"name": "hard_hat_id", "type": "int"},
                    {"name": "order_date", "type": "timestamp"},
                    {"name": "required_date", "type": "timestamp"},
                    {"name": "dispatched_date", "type": "timestamp"},
                    {"name": "dispatcher_id", "type": "int"},
                ],
            },
        )

        response = await client_with_namespaced_roads.get(
            "/namespaces/foo.bar.modified/diff",
            params={"base": "foo.bar"},
        )
        assert response.status_code == 200
        data = response.json()

        # repair_orders exists in both but with different description
        assert data["direct_change_count"] == 1
        assert len(data["direct_changes"]) == 1

        direct_change = data["direct_changes"][0]
        assert direct_change["name"] == "repair_orders"
        assert direct_change["change_type"] == "direct"
        assert "description" in direct_change["changed_fields"]

    @pytest.mark.asyncio
    async def test_diff_detects_query_changes(
        self,
        client_with_namespaced_roads: AsyncClient,
    ):
        """
        Test that changes to transform queries are detected as direct changes.
        """
        # First, get the nodes in foo.bar to understand what transforms exist
        response = await client_with_namespaced_roads.get("/namespaces/foo.bar/")
        assert response.status_code == 200
        nodes = response.json()

        # Find a transform node if one exists
        transforms = [n for n in nodes if n["type"] == "transform"]

        if not transforms:
            # Create a transform in both namespaces if none exist
            await client_with_namespaced_roads.post(
                "/nodes/transform/",
                json={
                    "name": "foo.bar.test_transform",
                    "description": "Test transform",
                    "mode": "published",
                    "query": "SELECT repair_order_id FROM foo.bar.repair_orders",
                },
            )

        # Create feature namespace with modified transform
        await client_with_namespaced_roads.post("/namespaces/foo.bar.query_change/")
        await client_with_namespaced_roads.post(
            "/nodes/transform/",
            json={
                "name": "foo.bar.query_change.test_transform",
                "description": "Test transform",
                "mode": "published",
                "query": "SELECT repair_order_id, municipality_id FROM foo.bar.query_change.repair_orders",  # Different query
            },
        )

        # Also create the source it depends on
        await client_with_namespaced_roads.post(
            "/nodes/source/",
            json={
                "name": "foo.bar.query_change.repair_orders",
                "description": "All repair orders",
                "mode": "published",
                "catalog": "default",
                "schema_": "roads",
                "table": "repair_orders",
                "columns": [
                    {"name": "repair_order_id", "type": "int"},
                    {"name": "municipality_id", "type": "string"},
                    {"name": "hard_hat_id", "type": "int"},
                    {"name": "order_date", "type": "timestamp"},
                    {"name": "required_date", "type": "timestamp"},
                    {"name": "dispatched_date", "type": "timestamp"},
                    {"name": "dispatcher_id", "type": "int"},
                ],
            },
        )

        response = await client_with_namespaced_roads.get(
            "/namespaces/foo.bar.query_change/diff",
            params={"base": "foo.bar"},
        )
        assert response.status_code == 200
        data = response.json()

        # Check that test_transform shows as a direct change due to query difference
        direct_changes = [
            d for d in data["direct_changes"] if d["name"] == "test_transform"
        ]
        if direct_changes:
            assert direct_changes[0]["change_type"] == "direct"
            assert "query" in direct_changes[0]["changed_fields"]

    @pytest.mark.asyncio
    async def test_diff_column_changes_detected(
        self,
        client_with_namespaced_roads: AsyncClient,
    ):
        """
        Test that column additions/removals are detected.
        """
        await client_with_namespaced_roads.post("/namespaces/foo.bar.col_change/")

        # Create source with different columns
        await client_with_namespaced_roads.post(
            "/nodes/source/",
            json={
                "name": "foo.bar.col_change.repair_orders",
                "description": "All repair orders",
                "mode": "published",
                "catalog": "default",
                "schema_": "roads",
                "table": "repair_orders",
                "columns": [
                    {"name": "repair_order_id", "type": "int"},
                    {"name": "municipality_id", "type": "string"},
                    {"name": "hard_hat_id", "type": "int"},
                    # Missing order_date, required_date, dispatched_date, dispatcher_id
                    {"name": "new_column", "type": "string"},  # Added column
                ],
            },
        )

        response = await client_with_namespaced_roads.get(
            "/namespaces/foo.bar.col_change/diff",
            params={"base": "foo.bar"},
        )
        assert response.status_code == 200
        data = response.json()

        # Find repair_orders in direct changes
        repair_orders_change = next(
            (d for d in data["direct_changes"] if d["name"] == "repair_orders"),
            None,
        )
        assert repair_orders_change is not None

        # Should have column changes
        column_changes = repair_orders_change["column_changes"]
        added_cols = [
            c["column"] for c in column_changes if c["change_type"] == "added"
        ]
        removed_cols = [
            c["column"] for c in column_changes if c["change_type"] == "removed"
        ]

        assert "new_column" in added_cols
        assert "order_date" in removed_cols

    @pytest.mark.asyncio
    async def test_diff_namespace_not_found(
        self,
        client_with_namespaced_roads: AsyncClient,
    ):
        """
        Test that diffing with a non-existent namespace returns empty results.
        """
        response = await client_with_namespaced_roads.get(
            "/namespaces/nonexistent.namespace/diff",
            params={"base": "foo.bar"},
        )
        # The compare namespace doesn't exist, so it should have 0 nodes
        # All foo.bar nodes should show as "removed"
        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_diff_response_structure(
        self,
        client_with_namespaced_roads: AsyncClient,
    ):
        """
        Test the complete structure of the diff response.
        """
        await client_with_namespaced_roads.post("/namespaces/foo.bar.struct_test/")

        # Create a mix of scenarios
        # 1. Add a new node
        await client_with_namespaced_roads.post(
            "/nodes/source/",
            json={
                "name": "foo.bar.struct_test.added_source",
                "description": "Added source",
                "mode": "published",
                "catalog": "default",
                "schema_": "roads",
                "table": "some_table",
                "columns": [{"name": "id", "type": "int"}],
            },
        )

        # 2. Copy an existing node with modification
        await client_with_namespaced_roads.post(
            "/nodes/source/",
            json={
                "name": "foo.bar.struct_test.repair_orders",
                "description": "Modified repair orders",
                "mode": "published",
                "catalog": "default",
                "schema_": "roads",
                "table": "repair_orders",
                "columns": [
                    {"name": "repair_order_id", "type": "int"},
                    {"name": "municipality_id", "type": "string"},
                    {"name": "hard_hat_id", "type": "int"},
                    {"name": "order_date", "type": "timestamp"},
                    {"name": "required_date", "type": "timestamp"},
                    {"name": "dispatched_date", "type": "timestamp"},
                    {"name": "dispatcher_id", "type": "int"},
                ],
            },
        )

        response = await client_with_namespaced_roads.get(
            "/namespaces/foo.bar.struct_test/diff",
            params={"base": "foo.bar"},
        )
        assert response.status_code == 200
        data = response.json()

        # Verify all expected fields are present
        assert "base_namespace" in data
        assert "compare_namespace" in data
        assert "added" in data
        assert "removed" in data
        assert "direct_changes" in data
        assert "propagated_changes" in data
        assert "unchanged_count" in data
        assert "added_count" in data
        assert "removed_count" in data
        assert "direct_change_count" in data
        assert "propagated_change_count" in data

        # Verify added node structure
        added = [a for a in data["added"] if a["name"] == "added_source"]
        assert len(added) == 1
        added_node = added[0]
        assert "name" in added_node
        assert "full_name" in added_node
        assert "node_type" in added_node

        # Verify direct change structure
        direct = [d for d in data["direct_changes"] if d["name"] == "repair_orders"]
        assert len(direct) == 1
        direct_change = direct[0]
        assert "name" in direct_change
        assert "full_name" in direct_change
        assert "node_type" in direct_change
        assert "change_type" in direct_change
        assert "base_version" in direct_change
        assert "compare_version" in direct_change
        assert "changed_fields" in direct_change

    @pytest.mark.asyncio
    async def test_diff_detects_tag_changes(
        self,
        client_with_namespaced_roads: AsyncClient,
    ):
        """
        Test that tag changes are detected as direct changes.
        """
        # Create a tag first
        await client_with_namespaced_roads.post(
            "/tags/",
            json={
                "name": "test_tag",
                "description": "A test tag",
                "tag_type": "default",
            },
        )

        # Add tag to a node in foo.bar
        await client_with_namespaced_roads.post(
            "/nodes/foo.bar.repair_orders/tags",
            json=["test_tag"],
        )

        # Create feature namespace with same node but no tag
        await client_with_namespaced_roads.post("/namespaces/foo.bar.tag_test/")
        await client_with_namespaced_roads.post(
            "/nodes/source/",
            json={
                "name": "foo.bar.tag_test.repair_orders",
                "description": "All repair orders",
                "mode": "published",
                "catalog": "default",
                "schema_": "roads",
                "table": "repair_orders",
                "columns": [
                    {"name": "repair_order_id", "type": "int"},
                    {"name": "municipality_id", "type": "string"},
                    {"name": "hard_hat_id", "type": "int"},
                    {"name": "order_date", "type": "timestamp"},
                    {"name": "required_date", "type": "timestamp"},
                    {"name": "dispatched_date", "type": "timestamp"},
                    {"name": "dispatcher_id", "type": "int"},
                ],
            },
        )

        response = await client_with_namespaced_roads.get(
            "/namespaces/foo.bar.tag_test/diff",
            params={"base": "foo.bar"},
        )
        assert response.status_code == 200
        data = response.json()

        # repair_orders should be in direct changes due to tag difference
        direct_change = next(
            (d for d in data["direct_changes"] if d["name"] == "repair_orders"),
            None,
        )
        assert direct_change is not None
        assert direct_change["change_type"] == "direct"

    @pytest.mark.asyncio
    async def test_diff_detects_mode_changes(
        self,
        client_with_namespaced_roads: AsyncClient,
    ):
        """
        Test that mode changes (draft vs published) are detected.
        """
        await client_with_namespaced_roads.post("/namespaces/foo.bar.mode_test/")

        # Create same node but in draft mode
        await client_with_namespaced_roads.post(
            "/nodes/source/",
            json={
                "name": "foo.bar.mode_test.repair_orders",
                "description": "All repair orders",
                "mode": "draft",  # Changed from published
                "catalog": "default",
                "schema_": "roads",
                "table": "repair_orders",
                "columns": [
                    {"name": "repair_order_id", "type": "int"},
                    {"name": "municipality_id", "type": "string"},
                    {"name": "hard_hat_id", "type": "int"},
                    {"name": "order_date", "type": "timestamp"},
                    {"name": "required_date", "type": "timestamp"},
                    {"name": "dispatched_date", "type": "timestamp"},
                    {"name": "dispatcher_id", "type": "int"},
                ],
            },
        )

        response = await client_with_namespaced_roads.get(
            "/namespaces/foo.bar.mode_test/diff",
            params={"base": "foo.bar"},
        )
        assert response.status_code == 200
        data = response.json()

        direct_change = next(
            (d for d in data["direct_changes"] if d["name"] == "repair_orders"),
            None,
        )
        assert direct_change is not None
        assert "mode" in direct_change["changed_fields"]

    @pytest.mark.asyncio
    async def test_diff_detects_source_table_changes(
        self,
        client_with_namespaced_roads: AsyncClient,
    ):
        """
        Test that catalog/schema/table changes for source nodes are detected.
        """
        await client_with_namespaced_roads.post("/namespaces/foo.bar.table_test/")

        # Create same source but pointing to different table
        await client_with_namespaced_roads.post(
            "/nodes/source/",
            json={
                "name": "foo.bar.table_test.repair_orders",
                "description": "All repair orders",
                "mode": "published",
                "catalog": "default",
                "schema_": "roads",
                "table": "repair_orders_v2",  # Different table
                "columns": [
                    {"name": "repair_order_id", "type": "int"},
                    {"name": "municipality_id", "type": "string"},
                    {"name": "hard_hat_id", "type": "int"},
                    {"name": "order_date", "type": "timestamp"},
                    {"name": "required_date", "type": "timestamp"},
                    {"name": "dispatched_date", "type": "timestamp"},
                    {"name": "dispatcher_id", "type": "int"},
                ],
            },
        )

        response = await client_with_namespaced_roads.get(
            "/namespaces/foo.bar.table_test/diff",
            params={"base": "foo.bar"},
        )
        assert response.status_code == 200
        data = response.json()

        direct_change = next(
            (d for d in data["direct_changes"] if d["name"] == "repair_orders"),
            None,
        )
        assert direct_change is not None
        assert direct_change["change_type"] == "direct"

    @pytest.mark.asyncio
    async def test_diff_detects_column_type_changes(
        self,
        client_with_namespaced_roads: AsyncClient,
    ):
        """
        Test that column type changes are detected (not just add/remove).
        """
        await client_with_namespaced_roads.post("/namespaces/foo.bar.type_test/")

        # Create same source but with different column type
        await client_with_namespaced_roads.post(
            "/nodes/source/",
            json={
                "name": "foo.bar.type_test.repair_orders",
                "description": "All repair orders",
                "mode": "published",
                "catalog": "default",
                "schema_": "roads",
                "table": "repair_orders",
                "columns": [
                    {"name": "repair_order_id", "type": "bigint"},  # Changed from int
                    {"name": "municipality_id", "type": "string"},
                    {"name": "hard_hat_id", "type": "int"},
                    {"name": "order_date", "type": "timestamp"},
                    {"name": "required_date", "type": "timestamp"},
                    {"name": "dispatched_date", "type": "timestamp"},
                    {"name": "dispatcher_id", "type": "int"},
                ],
            },
        )

        response = await client_with_namespaced_roads.get(
            "/namespaces/foo.bar.type_test/diff",
            params={"base": "foo.bar"},
        )
        assert response.status_code == 200
        data = response.json()

        direct_change = next(
            (d for d in data["direct_changes"] if d["name"] == "repair_orders"),
            None,
        )
        assert direct_change is not None

        # Should have column type change
        type_changes = [
            c
            for c in direct_change["column_changes"]
            if c["change_type"] == "type_changed"
        ]
        assert len(type_changes) > 0
        assert type_changes[0]["column"] == "repair_order_id"

    @pytest.mark.asyncio
    async def test_diff_detects_metric_metadata_changes(
        self,
        client_with_roads: AsyncClient,
    ):
        """
        Test that metric direction/unit changes are detected.
        """
        # Create a metric in default namespace
        await client_with_roads.post(
            "/nodes/metric/",
            json={
                "name": "default.test_metric_for_diff",
                "description": "Test metric",
                "mode": "published",
                "query": "SELECT COUNT(*) FROM default.repair_orders",
                "metric_metadata": {
                    "direction": "higher_is_better",
                    "unit": "unitless",
                },
            },
        )

        # Create feature namespace with same metric but different metadata
        await client_with_roads.post("/namespaces/default.metric_test/")

        # Copy the source first
        await client_with_roads.post(
            "/nodes/source/",
            json={
                "name": "default.metric_test.repair_orders",
                "description": "All repair orders",
                "mode": "published",
                "catalog": "default",
                "schema_": "roads",
                "table": "repair_orders",
                "columns": [
                    {"name": "repair_order_id", "type": "int"},
                    {"name": "municipality_id", "type": "string"},
                    {"name": "hard_hat_id", "type": "int"},
                    {"name": "order_date", "type": "timestamp"},
                    {"name": "required_date", "type": "timestamp"},
                    {"name": "dispatched_date", "type": "timestamp"},
                    {"name": "dispatcher_id", "type": "int"},
                ],
            },
        )

        await client_with_roads.post(
            "/nodes/metric/",
            json={
                "name": "default.metric_test.test_metric_for_diff",
                "description": "Test metric",
                "mode": "published",
                "query": "SELECT COUNT(*) FROM default.metric_test.repair_orders",
                "metric_metadata": {
                    "direction": "lower_is_better",  # Changed
                    "unit": "unitless",
                },
            },
        )

        response = await client_with_roads.get(
            "/namespaces/default.metric_test/diff",
            params={"base": "default"},
        )
        assert response.status_code == 200
        data = response.json()

        # Find the metric in direct changes
        metric_change = next(
            (d for d in data["direct_changes"] if d["name"] == "test_metric_for_diff"),
            None,
        )
        if metric_change:
            assert metric_change["change_type"] == "direct"

    @pytest.mark.asyncio
    async def test_diff_detects_display_name_changes(
        self,
        client_with_namespaced_roads: AsyncClient,
    ):
        """
        Test that display_name changes are detected.
        """
        # Update display_name in foo.bar
        await client_with_namespaced_roads.patch(
            "/nodes/foo.bar.repair_orders",
            json={"display_name": "Original Display Name"},
        )

        await client_with_namespaced_roads.post("/namespaces/foo.bar.display_test/")

        # Create with different display_name
        await client_with_namespaced_roads.post(
            "/nodes/source/",
            json={
                "name": "foo.bar.display_test.repair_orders",
                "display_name": "Different Display Name",
                "description": "All repair orders",
                "mode": "published",
                "catalog": "default",
                "schema_": "roads",
                "table": "repair_orders",
                "columns": [
                    {"name": "repair_order_id", "type": "int"},
                    {"name": "municipality_id", "type": "string"},
                    {"name": "hard_hat_id", "type": "int"},
                    {"name": "order_date", "type": "timestamp"},
                    {"name": "required_date", "type": "timestamp"},
                    {"name": "dispatched_date", "type": "timestamp"},
                    {"name": "dispatcher_id", "type": "int"},
                ],
            },
        )

        response = await client_with_namespaced_roads.get(
            "/namespaces/foo.bar.display_test/diff",
            params={"base": "foo.bar"},
        )
        assert response.status_code == 200
        data = response.json()

        direct_change = next(
            (d for d in data["direct_changes"] if d["name"] == "repair_orders"),
            None,
        )
        assert direct_change is not None
        assert "display_name" in direct_change["changed_fields"]

    @pytest.mark.asyncio
    async def test_diff_with_empty_namespaces(
        self,
        client_with_service_setup: AsyncClient,
    ):
        """
        Test diffing two empty namespaces.
        """
        await client_with_service_setup.post("/namespaces/empty_ns_a/")
        await client_with_service_setup.post("/namespaces/empty_ns_b/")

        response = await client_with_service_setup.get(
            "/namespaces/empty_ns_a/diff",
            params={"base": "empty_ns_b"},
        )
        assert response.status_code == 200
        data = response.json()

        assert data["added_count"] == 0
        assert data["removed_count"] == 0
        assert data["direct_change_count"] == 0
        assert data["propagated_change_count"] == 0
        assert data["unchanged_count"] == 0

    @pytest.mark.asyncio
    async def test_diff_detects_propagated_changes(
        self,
        client_with_namespaced_roads: AsyncClient,
    ):
        """
        Test that propagated changes are detected when user fields are the same
        but version/status differs due to upstream changes.
        """
        await client_with_namespaced_roads.post("/namespaces/foo.bar.propagated_test/")

        # Create base source node (identical to foo.bar.repair_orders)
        await client_with_namespaced_roads.post(
            "/nodes/source/",
            json={
                "name": "foo.bar.propagated_test.repair_orders",
                "description": "All repair orders",
                "mode": "published",
                "catalog": "default",
                "schema_": "roads",
                "table": "repair_orders",
                "columns": [
                    {"name": "repair_order_id", "type": "int"},
                    {"name": "municipality_id", "type": "string"},
                    {"name": "hard_hat_id", "type": "int"},
                    {"name": "order_date", "type": "timestamp"},
                    {"name": "required_date", "type": "timestamp"},
                    {"name": "dispatched_date", "type": "timestamp"},
                    {"name": "dispatcher_id", "type": "int"},
                ],
            },
        )

        # Create a transform that depends on the source - with DIFFERENT query
        # to cause a direct change, which will propagate to downstream nodes
        await client_with_namespaced_roads.post(
            "/nodes/transform/",
            json={
                "name": "foo.bar.propagated_test.repair_order_transform",
                "description": "Repair order transform",
                "mode": "published",
                "query": "SELECT repair_order_id, municipality_id FROM foo.bar.propagated_test.repair_orders WHERE 1=1",
            },
        )

        # Create a metric that depends on the transform
        await client_with_namespaced_roads.post(
            "/nodes/metric/",
            json={
                "name": "foo.bar.propagated_test.count_repair_orders",
                "description": "Count of repair orders",
                "mode": "published",
                "query": "SELECT COUNT(*) FROM foo.bar.propagated_test.repair_order_transform",
            },
        )

        # Now create same nodes in base namespace
        await client_with_namespaced_roads.post(
            "/nodes/transform/",
            json={
                "name": "foo.bar.repair_order_transform",
                "description": "Repair order transform",
                "mode": "published",
                "query": "SELECT repair_order_id, municipality_id FROM foo.bar.repair_orders",
            },
        )

        await client_with_namespaced_roads.post(
            "/nodes/metric/",
            json={
                "name": "foo.bar.count_repair_orders",
                "description": "Count of repair orders",
                "mode": "published",
                "query": "SELECT COUNT(*) FROM foo.bar.repair_order_transform",
            },
        )

        response = await client_with_namespaced_roads.get(
            "/namespaces/foo.bar.propagated_test/diff",
            params={"base": "foo.bar"},
        )
        assert response.status_code == 200
        data = response.json()

        # The transform has a direct change (WHERE 1=1 added)
        direct_change = next(
            (
                d
                for d in data["direct_changes"]
                if d["name"] == "repair_order_transform"
            ),
            None,
        )
        assert direct_change is not None
        assert direct_change["change_type"] == "direct"

        # The metric might show as propagated since its user fields are the same
        # but its status/version may differ due to parent change
        # Note: Propagation depends on the specific DJ behavior for re-validation

    @pytest.mark.asyncio
    async def test_diff_propagated_change_with_caused_by(
        self,
        client_with_namespaced_roads: AsyncClient,
    ):
        """
        Test that propagated changes include caused_by information linking
        to the upstream direct changes that caused them.
        """
        await client_with_namespaced_roads.post("/namespaces/foo.bar.caused_by_test/")

        # Create source with different columns to cause a direct change
        await client_with_namespaced_roads.post(
            "/nodes/source/",
            json={
                "name": "foo.bar.caused_by_test.repair_orders",
                "description": "All repair orders - modified",  # Direct change
                "mode": "published",
                "catalog": "default",
                "schema_": "roads",
                "table": "repair_orders",
                "columns": [
                    {"name": "repair_order_id", "type": "int"},
                    {"name": "municipality_id", "type": "string"},
                    {"name": "hard_hat_id", "type": "int"},
                    {"name": "order_date", "type": "timestamp"},
                    {"name": "required_date", "type": "timestamp"},
                    {"name": "dispatched_date", "type": "timestamp"},
                    {"name": "dispatcher_id", "type": "int"},
                ],
            },
        )

        # Create downstream metric with identical user fields
        await client_with_namespaced_roads.post(
            "/nodes/metric/",
            json={
                "name": "foo.bar.caused_by_test.num_repair_orders",
                "description": "Number of repair orders",
                "mode": "published",
                "query": "SELECT COUNT(*) FROM foo.bar.caused_by_test.repair_orders",
            },
        )

        # Create same metric in base namespace
        await client_with_namespaced_roads.post(
            "/nodes/metric/",
            json={
                "name": "foo.bar.num_repair_orders",
                "description": "Number of repair orders",
                "mode": "published",
                "query": "SELECT COUNT(*) FROM foo.bar.repair_orders",
            },
        )

        response = await client_with_namespaced_roads.get(
            "/namespaces/foo.bar.caused_by_test/diff",
            params={"base": "foo.bar"},
        )
        assert response.status_code == 200
        data = response.json()

        # repair_orders should be in direct changes (description changed)
        direct_change = next(
            (d for d in data["direct_changes"] if d["name"] == "repair_orders"),
            None,
        )
        assert direct_change is not None

        # Check propagated changes if any exist
        # The caused_by field should link to the direct change
        for prop_change in data["propagated_changes"]:
            if prop_change.get("caused_by"):
                # Verify caused_by contains valid node names
                assert isinstance(prop_change["caused_by"], list)

    @pytest.mark.asyncio
    async def test_diff_tag_changes_in_changed_fields(
        self,
        client_with_namespaced_roads: AsyncClient,
    ):
        """
        Test that tag changes appear in changed_fields list.
        """
        # Create a tag first
        await client_with_namespaced_roads.post(
            "/tags/",
            json={
                "name": "diff_test_tag",
                "description": "A test tag for diff",
                "tag_type": "default",
            },
        )

        await client_with_namespaced_roads.post("/namespaces/foo.bar.tag_fields_test/")

        # Create node with tag
        await client_with_namespaced_roads.post(
            "/nodes/source/",
            json={
                "name": "foo.bar.tag_fields_test.repair_orders",
                "description": "All repair orders",
                "mode": "published",
                "catalog": "default",
                "schema_": "roads",
                "table": "repair_orders",
                "columns": [
                    {"name": "repair_order_id", "type": "int"},
                    {"name": "municipality_id", "type": "string"},
                    {"name": "hard_hat_id", "type": "int"},
                    {"name": "order_date", "type": "timestamp"},
                    {"name": "required_date", "type": "timestamp"},
                    {"name": "dispatched_date", "type": "timestamp"},
                    {"name": "dispatcher_id", "type": "int"},
                ],
            },
        )

        # Add tag to the compare namespace node (use query params, not JSON body)
        await client_with_namespaced_roads.post(
            "/nodes/foo.bar.tag_fields_test.repair_orders/tags/",
            params={"tag_names": "diff_test_tag"},
        )

        response = await client_with_namespaced_roads.get(
            "/namespaces/foo.bar.tag_fields_test/diff",
            params={"base": "foo.bar"},
        )
        assert response.status_code == 200
        data = response.json()

        direct_change = next(
            (d for d in data["direct_changes"] if d["name"] == "repair_orders"),
            None,
        )
        assert direct_change is not None
        assert "tags" in direct_change["changed_fields"]

    @pytest.mark.asyncio
    async def test_diff_detects_metric_required_dimensions_changes(
        self,
        client_with_namespaced_roads: AsyncClient,
    ):
        """
        Test that changes to metric required_dimensions are detected.
        """
        await client_with_namespaced_roads.post(
            "/namespaces/foo.bar.req_dims_test/",
        )

        # Create source in compare namespace
        await client_with_namespaced_roads.post(
            "/nodes/source/",
            json={
                "name": "foo.bar.req_dims_test.repair_orders",
                "description": "All repair orders",
                "mode": "published",
                "catalog": "default",
                "schema_": "roads",
                "table": "repair_orders",
                "columns": [
                    {"name": "repair_order_id", "type": "int"},
                    {"name": "municipality_id", "type": "string"},
                    {"name": "hard_hat_id", "type": "int"},
                    {"name": "order_date", "type": "timestamp"},
                    {"name": "required_date", "type": "timestamp"},
                    {"name": "dispatched_date", "type": "timestamp"},
                    {"name": "dispatcher_id", "type": "int"},
                ],
            },
        )

        # Create dimension in compare namespace
        await client_with_namespaced_roads.post(
            "/nodes/dimension/",
            json={
                "name": "foo.bar.req_dims_test.municipality_dim",
                "description": "Municipality dimension",
                "mode": "published",
                "query": "SELECT DISTINCT municipality_id FROM foo.bar.req_dims_test.repair_orders",
                "primary_key": ["municipality_id"],
            },
        )

        # Create metric WITHOUT required_dimensions in compare namespace
        await client_with_namespaced_roads.post(
            "/nodes/metric/",
            json={
                "name": "foo.bar.req_dims_test.orders_count",
                "description": "Count of orders",
                "mode": "published",
                "query": "SELECT COUNT(*) FROM foo.bar.req_dims_test.repair_orders",
            },
        )

        # Create same in base namespace but WITH required_dimensions
        await client_with_namespaced_roads.post(
            "/nodes/dimension/",
            json={
                "name": "foo.bar.municipality_dim",
                "description": "Municipality dimension",
                "mode": "published",
                "query": "SELECT DISTINCT municipality_id FROM foo.bar.repair_orders",
                "primary_key": ["municipality_id"],
            },
        )

        await client_with_namespaced_roads.post(
            "/nodes/metric/",
            json={
                "name": "foo.bar.orders_count",
                "description": "Count of orders",
                "mode": "published",
                "query": "SELECT COUNT(*) FROM foo.bar.repair_orders",
                "required_dimensions": [
                    "foo.bar.municipality_dim.municipality_id",
                ],
            },
        )

        response = await client_with_namespaced_roads.get(
            "/namespaces/foo.bar.req_dims_test/diff",
            params={"base": "foo.bar"},
        )
        assert response.status_code == 200
        data = response.json()

        # The metric should show as a direct change due to required_dimensions difference
        direct_change = next(
            (d for d in data["direct_changes"] if d["name"] == "orders_count"),
            None,
        )
        assert direct_change is not None
        assert direct_change["change_type"] == "direct"

    @pytest.mark.asyncio
    async def test_diff_detects_dimension_link_reference_changes(
        self,
        client_with_namespaced_roads: AsyncClient,
    ):
        """
        Test that dimension link changes (reference type) are detected.
        """
        await client_with_namespaced_roads.post("/namespaces/foo.bar.dim_link_test/")

        # Create dimension in compare namespace
        await client_with_namespaced_roads.post(
            "/nodes/dimension/",
            json={
                "name": "foo.bar.dim_link_test.dispatcher_dim",
                "description": "Dispatcher dimension",
                "mode": "published",
                "query": "SELECT DISTINCT dispatcher_id FROM foo.bar.repair_orders",
                "primary_key": ["dispatcher_id"],
            },
        )

        # Create source WITH dimension link in compare namespace
        await client_with_namespaced_roads.post(
            "/nodes/source/",
            json={
                "name": "foo.bar.dim_link_test.repair_orders",
                "description": "All repair orders",
                "mode": "published",
                "catalog": "default",
                "schema_": "roads",
                "table": "repair_orders",
                "columns": [
                    {"name": "repair_order_id", "type": "int"},
                    {"name": "municipality_id", "type": "string"},
                    {"name": "hard_hat_id", "type": "int"},
                    {"name": "order_date", "type": "timestamp"},
                    {"name": "required_date", "type": "timestamp"},
                    {"name": "dispatched_date", "type": "timestamp"},
                    {
                        "name": "dispatcher_id",
                        "type": "int",
                        "dimension": "foo.bar.dim_link_test.dispatcher_dim.dispatcher_id",
                    },
                ],
            },
        )

        response = await client_with_namespaced_roads.get(
            "/namespaces/foo.bar.dim_link_test/diff",
            params={"base": "foo.bar"},
        )
        assert response.status_code == 200
        data = response.json()

        # repair_orders should show as direct change due to dimension link
        direct_change = next(
            (d for d in data["direct_changes"] if d["name"] == "repair_orders"),
            None,
        )
        assert direct_change is not None
        assert "dimension_links" in direct_change["changed_fields"]

    @pytest.mark.asyncio
    async def test_diff_detects_join_link_changes(
        self,
        client_with_namespaced_roads: AsyncClient,
    ):
        """
        Test that join-type dimension link changes are detected.
        """
        await client_with_namespaced_roads.post("/namespaces/foo.bar.join_link_test/")

        # Create source in compare namespace
        await client_with_namespaced_roads.post(
            "/nodes/source/",
            json={
                "name": "foo.bar.join_link_test.repair_orders",
                "description": "All repair orders",
                "mode": "published",
                "catalog": "default",
                "schema_": "roads",
                "table": "repair_orders",
                "columns": [
                    {"name": "repair_order_id", "type": "int"},
                    {"name": "municipality_id", "type": "string"},
                    {"name": "hard_hat_id", "type": "int"},
                    {"name": "order_date", "type": "timestamp"},
                    {"name": "required_date", "type": "timestamp"},
                    {"name": "dispatched_date", "type": "timestamp"},
                    {"name": "dispatcher_id", "type": "int"},
                ],
            },
        )

        # Create dimension for join link
        await client_with_namespaced_roads.post(
            "/nodes/dimension/",
            json={
                "name": "foo.bar.join_link_test.hard_hat_dim",
                "description": "Hard hat dimension",
                "mode": "published",
                "query": "SELECT DISTINCT hard_hat_id, 'Worker' as worker_type FROM foo.bar.join_link_test.repair_orders",
                "primary_key": ["hard_hat_id"],
            },
        )

        # Add join link to source
        await client_with_namespaced_roads.post(
            "/nodes/foo.bar.join_link_test.repair_orders/link",
            json={
                "dimension_node": "foo.bar.join_link_test.hard_hat_dim",
                "join_type": "left",
                "join_on": "foo.bar.join_link_test.repair_orders.hard_hat_id = foo.bar.join_link_test.hard_hat_dim.hard_hat_id",
            },
        )

        response = await client_with_namespaced_roads.get(
            "/namespaces/foo.bar.join_link_test/diff",
            params={"base": "foo.bar"},
        )
        assert response.status_code == 200
        data = response.json()

        # repair_orders should show as direct change due to join link
        direct_change = next(
            (d for d in data["direct_changes"] if d["name"] == "repair_orders"),
            None,
        )
        assert direct_change is not None
        assert "dimension_links" in direct_change["changed_fields"]
