"""
Tests for the namespaces API.
"""
from fastapi.testclient import TestClient

from datajunction_server.internal.access.authorization import validate_access
from datajunction_server.models import access


def test_list_all_namespaces(client_with_examples: TestClient) -> None:
    """
    Test ``GET /namespaces/``.
    """
    response = client_with_examples.get("/namespaces/")
    assert response.ok
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
        {"namespace": "default", "num_nodes": 55},
        {"namespace": "foo.bar", "num_nodes": 26},
    ]


def test_list_all_namespaces_access_limited(client_with_examples: TestClient) -> None:
    """
    Test ``GET /namespaces/``.
    """

    def validate_access_override():
        def _validate_access(access_control: access.AccessControl):
            for request in access_control.requests:
                if (
                    request.access_object.resource_type == access.ResourceType.NAMESPACE
                    and "dbt" in request.access_object.name
                ):
                    request.approve()
                else:
                    request.deny()

        return _validate_access

    app = client_with_examples.app
    app.dependency_overrides[validate_access] = validate_access_override

    response = client_with_examples.get("/namespaces/")

    assert response.ok
    assert response.json() == [
        {"namespace": "dbt.dimension", "num_nodes": 1},
        {"namespace": "dbt.source", "num_nodes": 0},
        {"namespace": "dbt.source.jaffle_shop", "num_nodes": 2},
        {"namespace": "dbt.source.stripe", "num_nodes": 1},
        {"namespace": "dbt.transform", "num_nodes": 1},
    ]


def test_list_all_namespaces_access_bad_injection(
    client_with_examples: TestClient,
) -> None:
    """
    Test ``GET /namespaces/``.
    """

    def validate_access_override():
        def _validate_access(access_control: access.AccessControl):
            for i, request in enumerate(access_control.requests):
                if i != 0:
                    request.approve()

        return _validate_access

    app = client_with_examples.app
    app.dependency_overrides[validate_access] = validate_access_override

    response = client_with_examples.get("/namespaces/")

    assert response.status_code == 403
    assert response.json() == {
        "message": "Injected `validate_access` must approve or deny all requests.",
        "errors": [
            {
                "code": 501,
                "message": "Injected `validate_access` must approve or deny all requests.",
                "debug": None,
                "context": "",
            },
        ],
        "warnings": [],
    }


def test_list_all_namespaces_deny_all(client_with_examples: TestClient) -> None:
    """
    Test ``GET /namespaces/``.
    """

    def validate_access_override():
        def _validate_access(access_control: access.AccessControl):
            access_control.deny_all()

        return _validate_access

    app = client_with_examples.app
    app.dependency_overrides[validate_access] = validate_access_override

    response = client_with_examples.get("/namespaces/")

    assert response.ok
    assert response.json() == []


def test_list_nodes_by_namespace(client_with_basic: TestClient) -> None:
    """
    Test ``GET /namespaces/{namespace}/``.
    """
    response = client_with_basic.get("/namespaces/basic.source/")
    assert response.ok
    assert {n["name"] for n in response.json()} == {
        "basic.source.users",
        "basic.source.comments",
    }

    response = client_with_basic.get("/namespaces/basic/")
    assert response.ok
    assert {n["name"] for n in response.json()} == {
        "basic.source.users",
        "basic.dimension.users",
        "basic.source.comments",
        "basic.dimension.countries",
        "basic.transform.country_agg",
        "basic.num_comments",
        "basic.num_users",
    }

    response = client_with_basic.get("/namespaces/basic/?type_=dimension")
    assert response.ok
    assert {n["name"] for n in response.json()} == {
        "basic.dimension.users",
        "basic.dimension.countries",
    }

    response = client_with_basic.get("/namespaces/basic/?type_=source")
    assert response.ok
    assert {n["name"] for n in response.json()} == {
        "basic.source.comments",
        "basic.source.users",
    }


def test_deactivate_namespaces(client_with_namespaced_roads: TestClient) -> None:
    """
    Test ``DELETE /namespaces/{namespace}``.
    """
    # Cannot deactivate if there are nodes under the namespace
    response = client_with_namespaced_roads.delete("/namespaces/foo.bar/?cascade=false")
    assert response.json() == {
        "message": "Cannot deactivate node namespace `foo.bar` as there are still "
        "active nodes under that namespace.",
    }

    # Can deactivate with cascade
    response = client_with_namespaced_roads.delete("/namespaces/foo.bar/?cascade=true")
    assert response.json() == {
        "message": "Namespace `foo.bar` has been deactivated. The following nodes "
        "have also been deactivated: foo.bar.repair_orders,foo.bar.repair_order_details,"
        "foo.bar.repair_type,foo.bar.contractors,foo.bar.municipality_municipality_type,"
        "foo.bar.municipality_type,foo.bar.municipality,foo.bar.dispatchers,foo.bar.hard_hats,"
        "foo.bar.hard_hat_state,foo.bar.us_states,foo.bar.us_region,foo.bar.repair_order,"
        "foo.bar.contractor,foo.bar.hard_hat,foo.bar.local_hard_hats,foo.bar.us_state,"
        "foo.bar.dispatcher,foo.bar.municipality_dim,foo.bar.num_repair_orders,"
        "foo.bar.avg_repair_price,foo.bar.total_repair_cost,foo.bar.avg_length_of_employment,"
        "foo.bar.total_repair_order_discounts,foo.bar.avg_repair_order_discounts,"
        "foo.bar.avg_time_to_dispatch",
    }

    # Check that the namespace is no longer listed
    response = client_with_namespaced_roads.get("/namespaces/")
    assert response.ok
    assert "foo.bar" not in {n["namespace"] for n in response.json()}

    response = client_with_namespaced_roads.delete("/namespaces/foo.bar/?cascade=false")
    assert response.json()["message"] == "Namespace `foo.bar` is already deactivated."

    # Try restoring
    response = client_with_namespaced_roads.post("/namespaces/foo.bar/restore/")
    assert response.json() == {
        "message": "Namespace `foo.bar` has been restored.",
    }

    # Check that the namespace is back
    response = client_with_namespaced_roads.get("/namespaces/")
    assert response.ok
    assert "foo.bar" in {n["namespace"] for n in response.json()}

    # Check that nodes in the namespace remain deactivated
    response = client_with_namespaced_roads.get("/namespaces/foo.bar/")
    assert response.ok
    assert response.json() == []

    # Restore with cascade=true should also restore all the nodes
    client_with_namespaced_roads.delete("/namespaces/foo.bar/?cascade=false")
    response = client_with_namespaced_roads.post(
        "/namespaces/foo.bar/restore/?cascade=true",
    )
    assert response.json() == {
        "message": "Namespace `foo.bar` has been restored. The following nodes have "
        "also been restored: foo.bar.repair_orders,foo.bar.repair_order_details,foo."
        "bar.repair_type,foo.bar.contractors,foo.bar.municipality_municipality_type,"
        "foo.bar.municipality_type,foo.bar.municipality,foo.bar.dispatchers,foo.bar."
        "hard_hats,foo.bar.hard_hat_state,foo.bar.us_states,foo.bar.us_region,foo.ba"
        "r.repair_order,foo.bar.contractor,foo.bar.hard_hat,foo.bar.local_hard_hats,"
        "foo.bar.us_state,foo.bar.dispatcher,foo.bar.municipality_dim,foo.bar.num_re"
        "pair_orders,foo.bar.avg_repair_price,foo.bar.total_repair_cost,foo.bar.avg_"
        "length_of_employment,foo.bar.total_repair_order_discounts,foo.bar.avg_repai"
        "r_order_discounts,foo.bar.avg_time_to_dispatch",
    }
    # Calling restore again will raise
    response = client_with_namespaced_roads.post(
        "/namespaces/foo.bar/restore/?cascade=true",
    )
    assert (
        response.json()["message"]
        == "Node namespace `foo.bar` already exists and is active."
    )

    # Check that nodes in the namespace are restored
    response = client_with_namespaced_roads.get("/namespaces/foo.bar/")
    assert response.ok
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

    response = client_with_namespaced_roads.get("/history/namespace/foo.bar/")
    assert [
        (activity["activity_type"], activity["details"]) for activity in response.json()
    ] == [
        ("create", {}),
        (
            "delete",
            {
                "message": (
                    "Namespace `foo.bar` has been deactivated. The following nodes have "
                    "also been deactivated: foo.bar.repair_orders,foo.bar.repair_order_d"
                    "etails,foo.bar.repair_type,foo.bar.contractors,foo.bar.municipality"
                    "_municipality_type,foo.bar.municipality_type,foo.bar.municipality,f"
                    "oo.bar.dispatchers,foo.bar.hard_hats,foo.bar.hard_hat_state,foo.bar"
                    ".us_states,foo.bar.us_region,foo.bar.repair_order,foo.bar.contracto"
                    "r,foo.bar.hard_hat,foo.bar.local_hard_hats,foo.bar.us_state,foo.bar"
                    ".dispatcher,foo.bar.municipality_dim,foo.bar.num_repair_orders,foo."
                    "bar.avg_repair_price,foo.bar.total_repair_cost,foo.bar.avg_length_o"
                    "f_employment,foo.bar.total_repair_order_discounts,foo.bar.avg_repai"
                    "r_order_discounts,foo.bar.avg_time_to_dispatch"
                ),
            },
        ),
        ("restore", {"message": "Namespace `foo.bar` has been restored."}),
        ("delete", {"message": "Namespace `foo.bar` has been deactivated."}),
        (
            "restore",
            {
                "message": (
                    "Namespace `foo.bar` has been restored. The following nodes have also "
                    "been restored: foo.bar.repair_orders,foo.bar.repair_order_details,foo"
                    ".bar.repair_type,foo.bar.contractors,foo.bar.municipality_municipalit"
                    "y_type,foo.bar.municipality_type,foo.bar.municipality,foo.bar.dispatc"
                    "hers,foo.bar.hard_hats,foo.bar.hard_hat_state,foo.bar.us_states,foo.b"
                    "ar.us_region,foo.bar.repair_order,foo.bar.contractor,foo.bar.hard_hat"
                    ",foo.bar.local_hard_hats,foo.bar.us_state,foo.bar.dispatcher,foo.bar."
                    "municipality_dim,foo.bar.num_repair_orders,foo.bar.avg_repair_price,"
                    "foo.bar.total_repair_cost,foo.bar.avg_length_of_employment,foo.bar.t"
                    "otal_repair_order_discounts,foo.bar.avg_repair_order_discounts,foo.b"
                    "ar.avg_time_to_dispatch"
                ),
            },
        ),
    ]

    response = client_with_namespaced_roads.get(
        "/history?node=foo.bar.avg_length_of_employment",
    )
    assert [
        (activity["activity_type"], activity["details"]) for activity in response.json()
    ] == [
        ("create", {}),
        ("status_change", {"upstream_node": "foo.bar.hard_hats"}),
        ("delete", {"message": "Cascaded from deactivating namespace `foo.bar`"}),
        ("status_change", {"upstream_node": "foo.bar.hard_hats"}),
        ("restore", {"message": "Cascaded from restoring namespace `foo.bar`"}),
    ]


def test_hard_delete_namespace(client_with_examples: TestClient):
    """
    Test hard deleting a namespace
    """
    response = client_with_examples.delete("/namespaces/foo/hard/")
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

    client_with_examples.post("/namespaces/foo/")
    client_with_examples.post("/namespaces/foo.bar.baz/")
    client_with_examples.post("/namespaces/foo.bar.baf/")
    client_with_examples.post("/namespaces/foo.bar.bif.d/")

    # Deactivating a few nodes should still allow the hard delete to go through
    client_with_examples.delete("/nodes/foo.bar.avg_length_of_employment")
    client_with_examples.delete("/nodes/foo.bar.avg_repair_order_discounts")

    hard_delete_response = client_with_examples.delete(
        "/namespaces/foo.bar/hard/?cascade=true",
    )
    assert hard_delete_response.json() == {
        "impact": {
            "foo.bar": {"namespace": "foo.bar", "status": "deleted"},
            "foo.bar.avg_length_of_employment": [],
            "foo.bar.avg_repair_order_discounts": [],
            "foo.bar.avg_repair_price": [],
            "foo.bar.avg_time_to_dispatch": [],
            "foo.bar.baf": {"namespace": "foo.bar.baf", "status": "deleted"},
            "foo.bar.baz": {"namespace": "foo.bar.baz", "status": "deleted"},
            "foo.bar.bif.d": {"namespace": "foo.bar.bif.d", "status": "deleted"},
            "foo.bar.contractor": [
                {
                    "effect": "broken link",
                    "name": "foo.bar.repair_type",
                    "status": "valid",
                },
            ],
            "foo.bar.contractors": [],
            "foo.bar.dispatcher": [
                {
                    "effect": "broken link",
                    "name": "foo.bar.repair_orders",
                    "status": "valid",
                },
                {
                    "effect": "broken link",
                    "name": "foo.bar.repair_order_details",
                    "status": "valid",
                },
                {
                    "effect": "broken link",
                    "name": "foo.bar.num_repair_orders",
                    "status": "valid",
                },
                {
                    "effect": "broken link",
                    "name": "foo.bar.total_repair_cost",
                    "status": "valid",
                },
                {
                    "effect": "broken link",
                    "name": "foo.bar.total_repair_order_discounts",
                    "status": "valid",
                },
            ],
            "foo.bar.dispatchers": [],
            "foo.bar.hard_hat": [
                {
                    "effect": "broken link",
                    "name": "foo.bar.repair_orders",
                    "status": "valid",
                },
                {
                    "effect": "broken link",
                    "name": "foo.bar.repair_order_details",
                    "status": "valid",
                },
                {
                    "effect": "broken link",
                    "name": "foo.bar.num_repair_orders",
                    "status": "valid",
                },
                {
                    "effect": "broken link",
                    "name": "foo.bar.total_repair_cost",
                    "status": "valid",
                },
                {
                    "effect": "broken link",
                    "name": "foo.bar.total_repair_order_discounts",
                    "status": "valid",
                },
            ],
            "foo.bar.hard_hat_state": [
                {
                    "effect": "downstream node is now " "invalid",
                    "name": "foo.bar.local_hard_hats",
                    "status": "invalid",
                },
            ],
            "foo.bar.hard_hats": [
                {
                    "effect": "downstream node is now invalid",
                    "name": "foo.bar.local_hard_hats",
                    "status": "invalid",
                },
            ],
            "foo.bar.local_hard_hats": [],
            "foo.bar.municipality": [
                {
                    "effect": "downstream node is now " "invalid",
                    "name": "foo.bar.municipality_dim",
                    "status": "invalid",
                },
            ],
            "foo.bar.municipality_dim": [
                {
                    "effect": "broken link",
                    "name": "foo.bar.repair_orders",
                    "status": "valid",
                },
                {
                    "effect": "broken link",
                    "name": "foo.bar.repair_order_details",
                    "status": "valid",
                },
                {
                    "effect": "broken link",
                    "name": "foo.bar.num_repair_orders",
                    "status": "valid",
                },
                {
                    "effect": "broken link",
                    "name": "foo.bar.total_repair_cost",
                    "status": "valid",
                },
                {
                    "effect": "broken link",
                    "name": "foo.bar.total_repair_order_discounts",
                    "status": "valid",
                },
            ],
            "foo.bar.municipality_municipality_type": [],
            "foo.bar.municipality_type": [],
            "foo.bar.num_repair_orders": [],
            "foo.bar.repair_order": [
                {
                    "effect": "broken link",
                    "name": "foo.bar.repair_orders",
                    "status": "valid",
                },
                {
                    "effect": "broken link",
                    "name": "foo.bar.repair_order_details",
                    "status": "valid",
                },
                {
                    "effect": "broken link",
                    "name": "foo.bar.total_repair_cost",
                    "status": "valid",
                },
                {
                    "effect": "broken link",
                    "name": "foo.bar.total_repair_order_discounts",
                    "status": "valid",
                },
            ],
            "foo.bar.repair_order_details": [
                {
                    "effect": "downstream node is " "now invalid",
                    "name": "foo.bar.total_repair_cost",
                    "status": "invalid",
                },
                {
                    "effect": "downstream node is " "now invalid",
                    "name": "foo.bar.total_repair_order_discounts",
                    "status": "invalid",
                },
            ],
            "foo.bar.repair_orders": [],
            "foo.bar.repair_type": [],
            "foo.bar.total_repair_cost": [],
            "foo.bar.total_repair_order_discounts": [],
            "foo.bar.us_region": [
                {
                    "effect": "downstream node is now invalid",
                    "name": "foo.bar.us_state",
                    "status": "invalid",
                },
            ],
            "foo.bar.us_state": [],
            "foo.bar.us_states": [],
        },
        "message": "The namespace `foo.bar` has been completely removed.",
    }
    list_namespaces_response = client_with_examples.get("/namespaces/")
    assert list_namespaces_response.json() == [
        {"namespace": "basic", "num_nodes": 8},
        {"namespace": "basic.dimension", "num_nodes": 2},
        {"namespace": "basic.source", "num_nodes": 2},
        {"namespace": "basic.transform", "num_nodes": 1},
        {"namespace": "dbt.dimension", "num_nodes": 1},
        {"namespace": "dbt.source", "num_nodes": 0},
        {"namespace": "dbt.source.jaffle_shop", "num_nodes": 2},
        {"namespace": "dbt.source.stripe", "num_nodes": 1},
        {"namespace": "dbt.transform", "num_nodes": 1},
        {"namespace": "default", "num_nodes": 55},
        {"namespace": "foo", "num_nodes": 0},
    ]

    response = client_with_examples.delete("/namespaces/jaffle_shop/hard/?cascade=true")
    assert response.json() == {
        "errors": [],
        "message": "Namespace `jaffle_shop` does not exist.",
        "warnings": [],
    }


def test_create_namespace(client_with_service_setup: TestClient):
    """
    Verify creating namespaces, both successful and validation errors
    """
    # By default, creating a namespace will also create its parents (i.e., like mkdir -p)
    response = client_with_service_setup.post(
        "/namespaces/aaa.bbb.ccc?include_parents=true",
    )
    assert response.json() == {
        "message": "The following node namespaces have been successfully created: "
        "aaa, aaa.bbb, aaa.bbb.ccc",
    }

    # Verify that the parent namespaces already exist if we try to create it again
    response = client_with_service_setup.post("/namespaces/aaa")
    assert response.json() == {"message": "Node namespace `aaa` already exists"}
    response = client_with_service_setup.post("/namespaces/aaa.bbb")
    assert response.json() == {"message": "Node namespace `aaa.bbb` already exists"}

    # Setting include_parents=false will not create the parents
    response = client_with_service_setup.post(
        "/namespaces/acde.mmm?include_parents=false",
    )
    assert response.json() == {
        "message": "The following node namespaces have been successfully created: acde.mmm",
    }
    response = client_with_service_setup.get("/namespaces/acde")
    assert response.json()["message"] == "node namespace `acde` does not exist."

    # Setting include_parents=true will create the parents
    response = client_with_service_setup.post(
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
        response = client_with_service_setup.post(f"/namespaces/{invalid_namespace}")
        assert response.status_code == 422
        assert response.json()["message"] == (
            f"{invalid_namespace} is not a valid namespace. Namespace parts cannot start "
            "with numbers, be empty, or use the reserved keyword [user]"
        )


def test_export_namespaces(client_with_examples: TestClient):
    """
    Test exporting a namespace to a project definition
    """
    # Create a cube so that the cube definition export path is tested
    response = client_with_examples.post(
        "/nodes/cube/",
        json={
            "name": "default.example_cube",
            "display_name": "Example Cube",
            "description": "An example cube so that the export path is tested",
            "metrics": ["default.num_repair_orders"],
            "dimensions": ["default.hard_hat.city"],
            "mode": "published",
        },
    )
    assert response.ok
    response = client_with_examples.get(
        "/namespaces/default/export/",
    )
    project_definition = response.json()
    node_defs = {d["filename"]: d for d in project_definition}
    assert node_defs["example_cube.cube.yaml"] == {
        "description": "An example cube so that the export path is tested",
        "dimensions": ["default.hard_hat.city"],
        "directory": "",
        "display_name": "Example Cube",
        "filename": "example_cube.cube.yaml",
        "metrics": ["default.num_repair_orders"],
    }
    assert set(node_defs.keys()) == {
        "repair_orders.source.yaml",
        "repair_order_details.source.yaml",
        "repair_type.source.yaml",
        "contractors.source.yaml",
        "municipality_municipality_type.source.yaml",
        "municipality_type.source.yaml",
        "municipality.source.yaml",
        "dispatchers.source.yaml",
        "hard_hats.source.yaml",
        "hard_hat_state.source.yaml",
        "us_states.source.yaml",
        "us_region.source.yaml",
        "repair_order.dimension.yaml",
        "contractor.dimension.yaml",
        "hard_hat.dimension.yaml",
        "local_hard_hats.dimension.yaml",
        "us_state.dimension.yaml",
        "dispatcher.dimension.yaml",
        "municipality_dim.dimension.yaml",
        "regional_level_agg.transform.yaml",
        "national_level_agg.transform.yaml",
        "repair_orders_fact.transform.yaml",
        "regional_repair_efficiency.metric.yaml",
        "num_repair_orders.metric.yaml",
        "avg_repair_price.metric.yaml",
        "total_repair_cost.metric.yaml",
        "avg_length_of_employment.metric.yaml",
        "discounted_orders_rate.metric.yaml",
        "total_repair_order_discounts.metric.yaml",
        "avg_repair_order_discounts.metric.yaml",
        "avg_time_to_dispatch.metric.yaml",
        "account_type_table.source.yaml",
        "payment_type_table.source.yaml",
        "revenue.source.yaml",
        "payment_type.dimension.yaml",
        "account_type.dimension.yaml",
        "large_revenue_payments_only.transform.yaml",
        "large_revenue_payments_and_business_only.transform.yaml",
        "number_of_account_types.metric.yaml",
        "event_source.source.yaml",
        "long_events.transform.yaml",
        "country_dim.dimension.yaml",
        "device_ids_count.metric.yaml",
        "long_events_distinct_countries.metric.yaml",
        "sales.source.yaml",
        "items.dimension.yaml",
        "items_sold_count.metric.yaml",
        "total_profit.metric.yaml",
        "date.source.yaml",
        "countries.source.yaml",
        "users.source.yaml",
        "date_dim.dimension.yaml",
        "special_country_dim.dimension.yaml",
        "user_dim.dimension.yaml",
        "avg_user_age.metric.yaml",
        "example_cube.cube.yaml",
    }
    assert {d["directory"] for d in project_definition} == {""}
