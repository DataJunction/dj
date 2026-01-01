"""
Tests for the namespaces API.
"""

from http import HTTPStatus
from unittest import mock

import pytest
import pytest_asyncio
from httpx import AsyncClient

from datajunction_server.internal.access.authorization import (
    AuthorizationService,
)
from datajunction_server.models import access


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
        {"namespace": "v3", "num_nodes": 38},
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
    assert node_defs["${prefix}example_cube"] == {
        "custom_metadata": None,
        "filters": None,
        "owners": ["dj"],
        "mode": "published",
        "node_type": "cube",
        "name": "${prefix}example_cube",
        "columns": [
            {
                "attributes": [],
                "description": None,
                "display_name": "Num Repair Orders",
                "name": "default.num_repair_orders",
                "type": "bigint",
                "partition": None,
            },
            {
                "attributes": [],
                "description": None,
                "display_name": "City",
                "name": "default.hard_hat.city",
                "type": "string",
                "partition": None,
            },
            {
                "attributes": [],
                "description": None,
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
    assert node_defs["${prefix}repair_orders_fact"]["dimension_links"] == [
        {
            "dimension_node": "${prefix}municipality_dim",
            "join_on": "${prefix}repair_orders_fact.municipality_id = "
            "${prefix}municipality_dim.municipality_id",
            "join_type": "inner",
            "type": "join",
            "node_column": None,
            "role": None,
        },
        {
            "dimension_node": "${prefix}hard_hat",
            "join_on": "${prefix}repair_orders_fact.hard_hat_id = ${prefix}hard_hat.hard_hat_id",
            "join_type": "inner",
            "type": "join",
            "node_column": None,
            "role": None,
        },
        {
            "dimension_node": "${prefix}hard_hat_to_delete",
            "join_on": "${prefix}repair_orders_fact.hard_hat_id = "
            "${prefix}hard_hat_to_delete.hard_hat_id",
            "join_type": "left",
            "type": "join",
            "node_column": None,
            "role": None,
        },
        {
            "dimension_node": "${prefix}dispatcher",
            "join_on": "${prefix}repair_orders_fact.dispatcher_id = ${prefix}dispatcher.dispatcher_id",
            "join_type": "inner",
            "type": "join",
            "node_column": None,
            "role": None,
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


@pytest_asyncio.fixture
async def unique_namespace(client: AsyncClient):
    """
    Fixture that provides unique namespace names and auto-cleans up after test.
    """
    import uuid

    suffix = uuid.uuid4().hex[:8]
    created_namespaces = []
    created_roles = []

    def make_namespace(name: str) -> str:
        """Generate a unique namespace name and track it for cleanup."""
        full_name = f"{name}_{suffix}"
        created_namespaces.append(full_name)
        created_roles.append(f"{full_name}-owner")
        return full_name

    yield make_namespace

    # Cleanup after test
    for ns in reversed(created_namespaces):  # Delete children first
        await client.delete(f"/namespaces/{ns}/hard/?cascade=true")
    for role in created_roles:
        await client.delete(f"/roles/{role}")


@pytest.mark.asyncio
async def test_create_namespace_auto_creates_owner_role(
    client: AsyncClient,
    unique_namespace,
) -> None:
    """
    Test that creating a namespace automatically creates an owner role.

    When a namespace is created, the system should:
    1. Create a role named "{namespace}-owner"
    2. Add a MANAGE scope on the namespace
    3. Assign the role to the creating user
    """
    ns_name = unique_namespace("testrbacauto")
    child_ns = f"{ns_name}.autorole"

    # Create a new namespace with a unique name
    response = await client.post(f"/namespaces/{child_ns}")
    assert response.status_code in (200, 201)

    # Verify the owner role was created for the parent namespace
    response = await client.get(f"/roles/{ns_name}-owner")
    assert response.status_code == 200
    role_data = response.json()
    assert role_data["name"] == f"{ns_name}-owner"
    assert role_data["description"] == f"Owner role for namespace {ns_name}"

    # Check scopes - should have MANAGE on the namespace
    assert len(role_data["scopes"]) == 1
    scope = role_data["scopes"][0]
    assert scope["action"] == "manage"
    assert scope["scope_type"] == "namespace"
    assert scope["scope_value"] == ns_name

    # Verify the child namespace also got an owner role
    response = await client.get(f"/roles/{child_ns}-owner")
    assert response.status_code == 200
    child_role_data = response.json()
    assert child_role_data["name"] == f"{child_ns}-owner"

    # Check the role is assigned to the creator
    response = await client.get(f"/roles/{ns_name}-owner/assignments")
    assert response.status_code == 200
    assignments = response.json()
    assert len(assignments) >= 1
    # The creator should be assigned this role
    assert any(a["principal"]["username"] == "dj" for a in assignments)


@pytest.mark.asyncio
async def test_create_namespace_skips_existing_role(
    client: AsyncClient,
    unique_namespace,
) -> None:
    """
    Test that creating a namespace skips role creation if role already exists.
    """
    ns_name = unique_namespace("preexistingns")
    role_name = f"{ns_name}-owner"

    # First, manually create a role with the expected name
    response = await client.post(
        "/roles/",
        json={
            "name": role_name,
            "description": "Manually created role",
        },
    )
    assert response.status_code in (200, 201)

    # Now create a namespace with the same name
    response = await client.post(f"/namespaces/{ns_name}")
    assert response.status_code in (200, 201)

    # The role should still be the original one (not overwritten)
    response = await client.get(f"/roles/{role_name}")
    assert response.status_code == 200
    role_data = response.json()
    assert role_data["description"] == "Manually created role"  # Original description
