"""
Tests for the namespaces API.
"""

from http import HTTPStatus
from unittest import mock

import asyncio
from unittest import mock

import pytest

from datajunction_server.internal.namespaces import (
    _node_spec_to_yaml_dict,
    node_spec_to_yaml,
)
from datajunction_server.models.deployment import (
    BulkNamespaceSourcesRequest,
    BulkNamespaceSourcesResponse,
    ColumnSpec,
    CubeSpec,
    DeploymentSourceType,
    DeploymentSpec,
    DimensionJoinLinkSpec,
    GitDeploymentSource,
    LocalDeploymentSource,
    NamespaceSourcesResponse,
    SourceSpec,
    TransformSpec,
    PartitionSpec,
)
from datajunction_server.models.partition import PartitionType, Granularity

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
                print("content~", content)
                node_data = yaml.safe_load(content)

                # Node should have a name
                assert "name" in node_data
                # Node should have a node_type
                assert "node_type" in node_data


class TestYamlHelpers:
    """Tests for internal YAML helper functions"""

    def test_node_spec_to_yaml_dict_excludes_none(self):
        """Test that _node_spec_to_yaml_dict excludes None values"""

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

    def test_get_node_suffix_no_match(self):
        """Test _get_node_suffix returns None when name doesn't match prefix."""
        from datajunction_server.internal.namespaces import _get_node_suffix

        # Name doesn't start with namespace prefix
        result = _get_node_suffix("other.namespace.node", "demo.main")
        assert result is None

        # Name is shorter than prefix
        result = _get_node_suffix("demo", "demo.main")
        assert result is None

    def test_get_node_suffix_match(self):
        """Test _get_node_suffix extracts suffix correctly."""
        from datajunction_server.internal.namespaces import _get_node_suffix

        result = _get_node_suffix("demo.main.reports.revenue", "demo.main")
        assert result == "reports.revenue"

        result = _get_node_suffix("demo.main.my_node", "demo.main")
        assert result == "my_node"

    def test_inject_prefix_for_cube_ref_external(self):
        """Test _inject_prefix_for_cube_ref keeps external references as-is."""
        from datajunction_server.internal.namespaces import _inject_prefix_for_cube_ref

        # External reference (different namespace, no parent)
        result = _inject_prefix_for_cube_ref(
            ref_name="other.namespace.metric",
            namespace="demo.main",
            parent_namespace=None,
            namespace_suffixes={"my_metric"},
        )
        assert result == "other.namespace.metric"

    def test_inject_prefix_for_cube_ref_parent_namespace(self):
        """Test _inject_prefix_for_cube_ref handles parent namespace references."""
        from datajunction_server.internal.namespaces import _inject_prefix_for_cube_ref

        # Reference from parent namespace that exists in current namespace
        result = _inject_prefix_for_cube_ref(
            ref_name="demo.main.reports.revenue",
            namespace="demo.feature_x",
            parent_namespace="demo.main",
            namespace_suffixes={"reports.revenue", "my_metric"},
        )
        assert result == "${prefix}reports.revenue"

    def test_inject_prefix_for_cube_ref_parent_not_copied(self):
        """Test _inject_prefix_for_cube_ref when parent ref not in current namespace."""
        from datajunction_server.internal.namespaces import _inject_prefix_for_cube_ref

        # Reference from parent namespace that does NOT exist in current namespace
        result = _inject_prefix_for_cube_ref(
            ref_name="demo.main.other_metric",
            namespace="demo.feature_x",
            parent_namespace="demo.main",
            namespace_suffixes={"my_metric"},  # other_metric not in suffixes
        )
        # Should keep as-is since it's not copied to branch
        assert result == "demo.main.other_metric"

    def test_node_spec_to_yaml_dict_empty_join_on(self):
        """Test _node_spec_to_yaml_dict handles empty join_on in dimension_links."""
        spec = SourceSpec(
            name="test.source",
            catalog="default",
            schema="test",
            table="test_table",
            columns=[ColumnSpec(name="id", type="int")],
            dimension_links=[
                DimensionJoinLinkSpec(
                    dimension_node="test.dimension",
                    join_type="left",
                    join_on="",  # Empty join_on
                ),
            ],
        )

        result = _node_spec_to_yaml_dict(spec)

        # Should not crash; join_on should remain empty or be handled
        assert "dimension_links" in result

    def test_node_spec_to_yaml_handles_invalid_existing_yaml(self):
        """Test node_spec_to_yaml handles invalid existing YAML gracefully."""
        spec = TransformSpec(
            name="test.node",
            query="SELECT 1",
        )

        # Provide malformed YAML as existing_yaml
        invalid_yaml = "name: |-\n  invalid: syntax: here"

        # Should not raise, should fall back to generating new YAML
        result = node_spec_to_yaml(spec, existing_yaml=invalid_yaml)

        # Should return valid YAML (without preserving comments from invalid input)
        assert "name:" in result
        assert "query:" in result
        assert "test.node" in result

    def test_node_spec_to_yaml_preserves_column_comments(self):
        """Test that column-level comments are preserved when updating YAML."""

        spec = TransformSpec(
            name="test.orders",
            query="SELECT order_id, customer_id FROM orders",
            columns=[
                ColumnSpec(name="order_id", type="int"),
                ColumnSpec(name="customer_id", type="int"),
            ],
        )

        # Existing YAML with comments on columns
        # Using single-line flow style for list items so ruamel.yaml can parse comments correctly
        existing_yaml = """name: test.orders
query: SELECT order_id, customer_id FROM orders
columns:
  # Primary key for the order
  - {name: order_id, type: int}
  # Reference to customer table
  - {name: customer_id, type: int}
"""

        result = node_spec_to_yaml(spec, existing_yaml=existing_yaml)

        # Comments should be preserved
        assert "# Primary key for the order" in result
        assert "# Reference to customer table" in result
        assert "order_id" in result
        assert "customer_id" in result

    def test_node_spec_to_yaml_filters_columns_without_customizations(self):
        """Test that columns without meaningful customizations are filtered out."""
        spec = TransformSpec(
            name="test.basic",
            query="SELECT a, b, c FROM table",
            columns=[
                ColumnSpec(
                    name="a",
                    type="int",
                ),  # No customizations - should be filtered
                ColumnSpec(
                    name="b",
                    type="string",
                    description="Column B",
                ),  # Has description
                ColumnSpec(
                    name="c",
                    type="int",
                    attributes=["dimension"],
                ),  # Has attributes
            ],
        )

        result = node_spec_to_yaml(spec)

        # Column 'a' should be filtered out (no customizations)
        assert "name: a" not in result
        # Columns 'b' and 'c' should be included
        assert "name: b" in result
        assert "name: c" in result
        assert "description: Column B" in result

    def test_node_spec_to_yaml_includes_columns_with_custom_display_name(self):
        """Test that columns with custom display_name are included."""
        spec = TransformSpec(
            name="test.display",
            query="SELECT user_id FROM users",
            columns=[
                ColumnSpec(name="user_id", type="int", display_name="User ID"),
            ],
        )

        result = node_spec_to_yaml(spec)

        assert "name: user_id" in result
        assert "display_name: User ID" in result

    def test_node_spec_to_yaml_includes_columns_with_partition(self):
        """Test that columns with partition info are included."""
        spec = TransformSpec(
            name="test.partition",
            query="SELECT event_date FROM events",
            columns=[
                ColumnSpec(
                    name="event_date",
                    type="date",
                    partition=PartitionSpec(
                        type=PartitionType.TEMPORAL,
                        granularity=Granularity.DAY,
                        format="yyyyMMdd",
                    ),
                ),
            ],
        )

        result = node_spec_to_yaml(spec)

        assert "name: event_date" in result
        assert "partition:" in result

    def test_node_spec_to_yaml_merge_removes_old_keys(self):
        """Test that merging removes keys that no longer exist."""
        spec = TransformSpec(
            name="test.removed",
            query="SELECT id FROM table",
            columns=[
                ColumnSpec(name="id", type="int", description="ID field"),
            ],
        )

        # Existing YAML has an extra field that should be removed
        existing_yaml = """name: test.removed
query: SELECT id FROM table
columns:
  - {name: id, type: int, description: ID field, old_field: should_be_removed}
"""

        result = node_spec_to_yaml(spec, existing_yaml=existing_yaml)

        assert "old_field" not in result
        assert "name: id" in result
        assert "description: ID field" in result


@pytest.mark.asyncio
async def test_delete_namespace_git_config_success(
    module__client_with_all_examples: AsyncClient,
) -> None:
    """Test successfully deleting git configuration from a namespace."""
    root_namespace = "test.git.root"
    namespace = "test.git.delete"

    # Create root namespace with git config
    await module__client_with_all_examples.post(f"/namespaces/{root_namespace}/")
    await module__client_with_all_examples.patch(
        f"/namespaces/{root_namespace}/git",
        json={
            "github_repo_path": "owner/repo",
            "git_path": "nodes/",
        },
    )

    # Create branch namespace with git_only
    await module__client_with_all_examples.post(f"/namespaces/{namespace}/")
    git_config = {
        "parent_namespace": root_namespace,
        "git_branch": "main",
        "git_only": True,
    }
    patch_response = await module__client_with_all_examples.patch(
        f"/namespaces/{namespace}/git",
        json=git_config,
    )
    assert patch_response.status_code == 200

    # Verify git config exists (with inherited github_repo_path)
    get_response = await module__client_with_all_examples.get(
        f"/namespaces/{namespace}/git",
    )
    assert get_response.status_code == 200
    assert get_response.json()["github_repo_path"] == "owner/repo"
    assert get_response.json()["parent_namespace"] == root_namespace
    assert get_response.json()["git_branch"] == "main"

    # Delete git configuration
    delete_response = await module__client_with_all_examples.delete(
        f"/namespaces/{namespace}/git",
    )
    assert delete_response.status_code == 204

    # Verify git config is cleared
    verify_response = await module__client_with_all_examples.get(
        f"/namespaces/{namespace}/git",
    )
    assert verify_response.status_code == 200
    result = verify_response.json()
    assert result["github_repo_path"] is None
    assert result["git_branch"] is None
    assert result["git_path"] is None
    assert result["parent_namespace"] is None
    assert result["git_only"] is False
    assert result["default_branch"] is None

    # Delete git configuration (should succeed even though nothing is configured)
    delete_response = await module__client_with_all_examples.delete(
        f"/namespaces/{namespace}/git",
    )
    assert delete_response.status_code == 204


@pytest.mark.asyncio
async def test_delete_namespace_git_config_namespace_not_found(
    module__client_with_all_examples: AsyncClient,
) -> None:
    """Test deleting git config from non-existent namespace returns 404."""
    delete_response = await module__client_with_all_examples.delete(
        "/namespaces/nonexistent.namespace.test/git",
    )
    assert delete_response.status_code == 404
    assert delete_response.json()["message"] == (
        "node namespace `nonexistent.namespace.test` does not exist."
    )


@pytest.mark.asyncio
async def test_delete_namespace_git_config_preserves_namespace(
    module__client_with_all_examples: AsyncClient,
) -> None:
    """Test that deleting git config doesn't delete the namespace itself."""
    namespace = "test.git.delete.preserve"

    # Create namespace with git config
    await module__client_with_all_examples.post(f"/namespaces/{namespace}/")
    git_config = {
        "github_repo_path": "owner/repo",
        "git_branch": "feature",
        "git_path": "nodes/",
        "git_only": False,
    }
    await module__client_with_all_examples.patch(
        f"/namespaces/{namespace}/git",
        json=git_config,
    )

    # Delete git configuration
    delete_response = await module__client_with_all_examples.delete(
        f"/namespaces/{namespace}/git",
    )
    assert delete_response.status_code == 204

    # Verify namespace still exists by listing nodes in it
    list_response = await module__client_with_all_examples.get(
        f"/namespaces/{namespace}/",
    )
    assert list_response.status_code == 200
    # Should return empty list of nodes, not 404
    assert isinstance(list_response.json(), list)


# Tests for git configuration validation functions


@pytest.mark.asyncio
async def test_validate_sibling_relationship_valid_siblings(
    module__client_with_all_examples: AsyncClient,
) -> None:
    """Test that both sibling and direct-child relationships work."""
    # Create namespaces
    await module__client_with_all_examples.post("/namespaces/demo/")
    await module__client_with_all_examples.post("/namespaces/demo.main/")
    await module__client_with_all_examples.post("/namespaces/demo.feature/")

    # Configure git root namespace (demo) with repo/path
    root_git_config = {
        "github_repo_path": "corp/demo",
        "git_path": "definitions/",
    }
    await module__client_with_all_examples.patch(
        "/namespaces/demo/git",
        json=root_git_config,
    )

    # Configure main as direct child of demo (demo.main -> demo)
    main_git_config = {
        "git_branch": "main",
        "parent_namespace": "demo",
    }
    await module__client_with_all_examples.patch(
        "/namespaces/demo.main/git",
        json=main_git_config,
    )

    # Configure feature as direct child of demo (demo.feature -> demo)
    feature_git_config = {
        "git_branch": "feature",
        "parent_namespace": "demo",
    }
    response = await module__client_with_all_examples.patch(
        "/namespaces/demo.feature/git",
        json=feature_git_config,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["parent_namespace"] == "demo"
    assert data["git_branch"] == "feature"
    # Response shows resolved/effective values (including inherited)
    assert data["github_repo_path"] == "corp/demo"  # Inherited from demo
    assert data["git_path"] == "definitions/"  # Inherited from demo
    assert data["git_only"] is False


@pytest.mark.asyncio
async def test_validate_sibling_relationship_different_prefixes_blocked(
    module__client_with_all_examples: AsyncClient,
) -> None:
    """Test that namespaces with different prefixes cannot have parent-child relationships."""
    # Create namespaces with different prefixes
    await module__client_with_all_examples.post("/namespaces/team.main/")
    await module__client_with_all_examples.post("/namespaces/demo.feature/")

    # Configure parent with git
    parent_git_config = {
        "github_repo_path": "corp/repo",
        "git_branch": "main",
    }
    await module__client_with_all_examples.patch(
        "/namespaces/team.main/git",
        json=parent_git_config,
    )

    # Child with different prefix should be blocked
    # Note: child only sets branch and parent (not repo/path in new model)
    child_git_config = {
        "git_branch": "feature",
        "parent_namespace": "team.main",  # Different prefix: "demo" vs "team"
    }
    response = await module__client_with_all_examples.patch(
        "/namespaces/demo.feature/git",
        json=child_git_config,
    )
    assert response.status_code == 422
    assert response.json()["message"] == (
        "Namespace 'demo.feature' (prefix: 'demo') cannot have parent 'team.main'. "
        "Expected parent to either be 'demo' (direct parent) or have prefix 'demo' (sibling)."
    )


@pytest.mark.asyncio
async def test_validate_sibling_relationship_top_level_namespaces(
    module__client_with_all_examples: AsyncClient,
) -> None:
    """Test that top-level namespaces (no dots) can have parent-child relationships."""
    # Create top-level namespaces
    await module__client_with_all_examples.post("/namespaces/main/")
    await module__client_with_all_examples.post("/namespaces/feature/")

    # Configure parent as git root (only repo, no branch)
    parent_git_config = {
        "github_repo_path": "corp/repo",
    }
    await module__client_with_all_examples.patch(
        "/namespaces/main/git",
        json=parent_git_config,
    )

    # Top-level child should be able to set top-level parent (both have prefix "")
    # Note: child only sets branch and parent (not repo/path)
    child_git_config = {
        "git_branch": "feature",
        "parent_namespace": "main",
    }
    response = await module__client_with_all_examples.patch(
        "/namespaces/feature/git",
        json=child_git_config,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["parent_namespace"] == "main"
    # Verify inheritance works (resolved values at top level)
    assert data["github_repo_path"] == "corp/repo"


@pytest.mark.asyncio
async def test_validate_sibling_relationship_deep_hierarchy(
    module__client_with_all_examples: AsyncClient,
) -> None:
    """Test that deep namespace hierarchies work with sibling validation."""
    # Create deep hierarchy namespaces
    await module__client_with_all_examples.post(
        "/namespaces/company.division.team.main/",
    )
    await module__client_with_all_examples.post(
        "/namespaces/company.division.team.feature/",
    )

    # Configure parent with git
    parent_git_config = {
        "github_repo_path": "corp/deep-hierarchy-repo",
        "git_branch": "main",
    }
    await module__client_with_all_examples.patch(
        "/namespaces/company.division.team.main/git",
        json=parent_git_config,
    )

    # Deep hierarchy child should work (both have prefix "company.division.team")
    # Note: child only sets branch and parent (not repo/path)
    child_git_config = {
        "git_branch": "deep-feature",
        "parent_namespace": "company.division.team.main",
    }
    response = await module__client_with_all_examples.patch(
        "/namespaces/company.division.team.feature/git",
        json=child_git_config,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["parent_namespace"] == "company.division.team.main"
    # Verify inheritance works for deep hierarchies (resolved values at top level)
    assert data["github_repo_path"] == "corp/deep-hierarchy-repo"


@pytest.mark.asyncio
async def test_detect_parent_cycle_two_node_cycle(
    module__client_with_all_examples: AsyncClient,
) -> None:
    """Test that a two-node cycle is detected and blocked."""
    # Create two namespaces
    await module__client_with_all_examples.post("/namespaces/cycle.a/")
    await module__client_with_all_examples.post("/namespaces/cycle.b/")

    # Set up A -> B (no repo config needed to test cycle detection)
    await module__client_with_all_examples.patch(
        "/namespaces/cycle.a/git",
        json={
            "git_branch": "a",
            "parent_namespace": "cycle.b",
        },
    )

    # Try to set up B -> A (creates cycle)
    response = await module__client_with_all_examples.patch(
        "/namespaces/cycle.b/git",
        json={
            "git_branch": "b",
            "parent_namespace": "cycle.a",
        },
    )
    assert response.status_code == 422
    assert response.json()["message"] == (
        "Circular parent reference detected: cycle.b -> cycle.a -> cycle.b"
    )


@pytest.mark.asyncio
async def test_detect_parent_cycle_three_node_cycle(
    module__client_with_all_examples: AsyncClient,
) -> None:
    """Test that a three-node cycle is detected and blocked."""
    # Create three namespaces
    await module__client_with_all_examples.post("/namespaces/cycle3.a/")
    await module__client_with_all_examples.post("/namespaces/cycle3.b/")
    await module__client_with_all_examples.post("/namespaces/cycle3.c/")

    # Set up A -> B (no repo config needed to test cycle detection)
    await module__client_with_all_examples.patch(
        "/namespaces/cycle3.a/git",
        json={
            "git_branch": "three-cycle-a",
            "parent_namespace": "cycle3.b",
        },
    )

    # Set up B -> C
    await module__client_with_all_examples.patch(
        "/namespaces/cycle3.b/git",
        json={
            "git_branch": "three-cycle-b",
            "parent_namespace": "cycle3.c",
        },
    )

    # Try to set up C -> A (creates 3-node cycle)
    response = await module__client_with_all_examples.patch(
        "/namespaces/cycle3.c/git",
        json={
            "git_branch": "three-cycle-c",
            "parent_namespace": "cycle3.a",
        },
    )
    assert response.status_code == 422
    assert response.json()["message"] == (
        "Circular parent reference detected: cycle3.c -> cycle3.a -> cycle3.b -> cycle3.c"
    )


@pytest.mark.asyncio
async def test_validate_git_path_blocks_parent_directory_traversal(
    module__client_with_all_examples: AsyncClient,
) -> None:
    """Test that git_path containing .. is blocked."""
    await module__client_with_all_examples.post("/namespaces/security.test1/")

    git_config = {
        "github_repo_path": "corp/repo",
        "git_branch": "main",
        "git_path": "../other-repo/",  # Path traversal attempt
    }
    response = await module__client_with_all_examples.patch(
        "/namespaces/security.test1/git",
        json=git_config,
    )
    assert response.status_code == 422
    assert response.json()["message"] == (
        "git_path cannot contain '..' (path traversal)"
    )


@pytest.mark.asyncio
async def test_validate_git_path_blocks_absolute_paths(
    module__client_with_all_examples: AsyncClient,
) -> None:
    """Test that absolute paths in git_path are blocked."""
    await module__client_with_all_examples.post("/namespaces/security.test2/")

    git_config = {
        "github_repo_path": "corp/repo",
        "git_branch": "main",
        "git_path": "/etc/passwd",  # Absolute path
    }
    response = await module__client_with_all_examples.patch(
        "/namespaces/security.test2/git",
        json=git_config,
    )
    assert response.status_code == 422
    assert response.json()["message"] == (
        "git_path must be a relative path (cannot start with '/')"
    )


@pytest.mark.asyncio
async def test_validate_git_path_allows_valid_relative_paths(
    module__client_with_all_examples: AsyncClient,
) -> None:
    """Test that valid relative paths are allowed."""
    await module__client_with_all_examples.post("/namespaces/security.test3/")

    git_config = {
        "github_repo_path": "corp/repo",
        "git_branch": "main",
        "git_path": "definitions/metrics/",  # Valid relative path
    }
    response = await module__client_with_all_examples.patch(
        "/namespaces/security.test3/git",
        json=git_config,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["git_path"] == "definitions/metrics/"


@pytest.mark.asyncio
async def test_validate_git_only_blocked_without_git_config(
    module__client_with_all_examples: AsyncClient,
) -> None:
    """Test that git_only=true is blocked when not a branch namespace."""
    await module__client_with_all_examples.post("/namespaces/gitonly.test1/")

    # Try to enable git_only without parent_namespace and git_branch
    git_config = {
        "git_only": True,
        # Missing parent_namespace and git_branch (required for branch namespace)
    }
    response = await module__client_with_all_examples.patch(
        "/namespaces/gitonly.test1/git",
        json=git_config,
    )
    assert response.status_code == 422
    assert response.json()["message"] == (
        "Cannot enable git_only on a git root namespace. "
        "git_only is only applicable to branch namespaces that have "
        "parent_namespace and git_branch configured."
    )


@pytest.mark.asyncio
async def test_validate_git_only_allowed_with_git_config(
    module__client_with_all_examples: AsyncClient,
) -> None:
    """Test that git_only=true is allowed on branch namespaces."""
    # Create git root namespace
    root_namespace = "gitonly.root"
    await module__client_with_all_examples.post(f"/namespaces/{root_namespace}/")
    await module__client_with_all_examples.patch(
        f"/namespaces/{root_namespace}/git",
        json={
            "github_repo_path": "corp/repo-123",
        },
    )

    # Create branch namespace with git_only (sibling of root)
    branch_namespace = "gitonly.test2"
    await module__client_with_all_examples.post(f"/namespaces/{branch_namespace}/")
    git_config = {
        "parent_namespace": root_namespace,
        "git_branch": "main",
        "git_only": True,
    }
    response = await module__client_with_all_examples.patch(
        f"/namespaces/{branch_namespace}/git",
        json=git_config,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["git_only"] is True


@pytest.mark.asyncio
async def test_branch_namespace_cannot_set_git_path(
    module__client_with_all_examples: AsyncClient,
) -> None:
    """Test that branch namespaces cannot set git_path (must inherit from parent)."""
    # Create root namespace with git_path
    root_ns = "gitpath.root"
    await module__client_with_all_examples.post(f"/namespaces/{root_ns}/")
    await module__client_with_all_examples.patch(
        f"/namespaces/{root_ns}/git",
        json={
            "github_repo_path": "corp/repo",
            "git_path": "definitions/",
        },
    )

    # Create branch namespace
    branch_ns = "gitpath.branch"
    await module__client_with_all_examples.post(f"/namespaces/{branch_ns}/")

    # Try to set both parent_namespace AND git_path (should fail)
    response = await module__client_with_all_examples.patch(
        f"/namespaces/{branch_ns}/git",
        json={
            "parent_namespace": root_ns,
            "git_branch": "feature",
            "git_path": "custom/path/",  # This should trigger error
        },
    )
    assert response.status_code == 422
    assert "Cannot set git_path on a branch namespace" in response.json()["message"]
    assert "inherited from parent_namespace" in response.json()["message"]


@pytest.mark.asyncio
async def test_multi_level_git_branch_hierarchy(
    module__client_with_all_examples: AsyncClient,
) -> None:
    """Test that multi-level git branch hierarchy is allowed (flow -> main -> dev -> feature)."""
    # Create namespaces including git root
    await module__client_with_all_examples.post("/namespaces/flow/")
    await module__client_with_all_examples.post("/namespaces/flow.main/")
    await module__client_with_all_examples.post("/namespaces/flow.dev/")
    await module__client_with_all_examples.post("/namespaces/flow.feature/")

    # Set up git root (flow) with repo configuration
    await module__client_with_all_examples.patch(
        "/namespaces/flow/git",
        json={
            "github_repo_path": "corp/flow",
            "git_path": "definitions/",
        },
    )

    # Set up main -> flow (root)
    await module__client_with_all_examples.patch(
        "/namespaces/flow.main/git",
        json={
            "git_branch": "main",
            "parent_namespace": "flow",
        },
    )

    # Set up dev -> main
    await module__client_with_all_examples.patch(
        "/namespaces/flow.dev/git",
        json={
            "git_branch": "dev",
            "parent_namespace": "flow.main",
        },
    )

    # Set up feature -> dev
    response = await module__client_with_all_examples.patch(
        "/namespaces/flow.feature/git",
        json={
            "git_branch": "feature",
            "parent_namespace": "flow.dev",
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data["parent_namespace"] == "flow.dev"
    # Verify multi-level inheritance works (resolved values at top level)
    assert data["github_repo_path"] == "corp/flow"
    assert data["git_path"] == "definitions/"
    assert data["git_branch"] == "feature"

    # Also verify intermediate levels inherit correctly
    dev_response = await module__client_with_all_examples.get(
        "/namespaces/flow.dev/git",
    )
    assert dev_response.status_code == 200
    dev_data = dev_response.json()
    assert dev_data["github_repo_path"] == "corp/flow"  # Inherited from grandparent
    assert dev_data["git_path"] == "definitions/"  # Inherited from grandparent
    assert dev_data["git_branch"] == "dev"
    assert dev_data["parent_namespace"] == "flow.main"


@pytest.mark.asyncio
async def test_git_inheritance_with_no_git_path(
    module__client_with_all_examples: AsyncClient,
) -> None:
    """Test git config inheritance when root has no git_path (empty string is valid)."""
    # Create root namespace with github_repo_path but no git_path
    await module__client_with_all_examples.post("/namespaces/nopath/")
    await module__client_with_all_examples.patch(
        "/namespaces/nopath/git",
        json={
            "github_repo_path": "corp/nopath-repo",
            # Omit git_path - should default to None/root
        },
    )

    # Create branch namespace
    await module__client_with_all_examples.post("/namespaces/nopath.branch/")
    response = await module__client_with_all_examples.patch(
        "/namespaces/nopath.branch/git",
        json={
            "parent_namespace": "nopath",
            "git_branch": "feature",
        },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["github_repo_path"] == "corp/nopath-repo"
    assert data["git_path"] is None  # Inherited as None from parent
    assert data["git_branch"] == "feature"
