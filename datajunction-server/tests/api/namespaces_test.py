"""
Tests for the namespaces API.
"""
from fastapi.testclient import TestClient


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
        {"namespace": "default", "num_nodes": 54},
        {"namespace": "foo.bar", "num_nodes": 26},
    ]


def test_list_nodes_by_namespace(client_with_examples: TestClient) -> None:
    """
    Test ``GET /namespaces/{namespace}/``.
    """
    response = client_with_examples.get("/namespaces/basic.source/")
    assert response.ok
    assert {n["name"] for n in response.json()} == {
        "basic.source.users",
        "basic.source.comments",
    }

    response = client_with_examples.get("/namespaces/basic/")
    assert response.ok
    assert {n["name"] for n in response.json()} == {
        "basic.source.users",
        "basic.dimension.users",
        "basic.source.comments",
        "basic.dimension.countries",
        "basic.transform.country_agg",
        "basic.num_comments",
        "basic.num_users",
        "basic.murals",
        "basic.patches",
        "basic.corrected_patches",
        "basic.paint_colors_trino",
        "basic.paint_colors_spark",
        "basic.avg_luminosity_patches",
    }

    response = client_with_examples.get("/namespaces/basic/?type_=dimension")
    assert response.ok
    assert {n["name"] for n in response.json()} == {
        "basic.dimension.users",
        "basic.dimension.countries",
        "basic.paint_colors_spark",
        "basic.paint_colors_trino",
    }

    response = client_with_examples.get("/namespaces/basic/?type_=source")
    assert response.ok
    assert {n["name"] for n in response.json()} == {
        "basic.source.comments",
        "basic.murals",
        "basic.source.users",
        "basic.patches",
    }


def test_deactivate_namespaces(client_with_examples: TestClient) -> None:
    """
    Test ``DELETE /namespaces/{namespace}``.
    """
    # Cannot deactivate if there are nodes under the namespace
    response = client_with_examples.delete("/namespaces/foo.bar/?cascade=false")
    assert response.json() == {
        "message": "Cannot deactivate node namespace `foo.bar` as there are still "
        "active nodes under that namespace.",
    }

    # Can deactivate with cascade
    response = client_with_examples.delete("/namespaces/foo.bar/?cascade=true")
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
    response = client_with_examples.get("/namespaces/")
    assert response.ok
    assert {n["namespace"] for n in response.json()} == {
        "default",
        "basic",
        "basic.source",
        "basic.transform",
        "basic.dimension",
        "dbt.source",
        "dbt.source.jaffle_shop",
        "dbt.transform",
        "dbt.dimension",
        "dbt.source.stripe",
    }

    response = client_with_examples.delete("/namespaces/foo.bar/?cascade=false")
    assert response.json()["message"] == "Namespace `foo.bar` is already deactivated."

    # Try restoring
    response = client_with_examples.post("/namespaces/foo.bar/restore/")
    assert response.json() == {
        "message": "Namespace `foo.bar` has been restored.",
    }

    # Check that the namespace is back
    response = client_with_examples.get("/namespaces/")
    assert response.ok
    assert {n["namespace"] for n in response.json()} == {
        "basic",
        "basic.dimension",
        "basic.source",
        "basic.transform",
        "dbt.dimension",
        "dbt.source",
        "dbt.source.jaffle_shop",
        "dbt.source.stripe",
        "dbt.transform",
        "default",
        "foo.bar",
    }

    # Check that nodes in the namespace remain deactivated
    response = client_with_examples.get("/namespaces/foo.bar/")
    assert response.ok
    assert response.json() == []

    # Restore with cascade=true should also restore all the nodes
    client_with_examples.delete("/namespaces/foo.bar/?cascade=false")
    response = client_with_examples.post("/namespaces/foo.bar/restore/?cascade=true")
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
    response = client_with_examples.post("/namespaces/foo.bar/restore/?cascade=true")
    assert (
        response.json()["message"]
        == "Node namespace `foo.bar` already exists and is active."
    )

    # Check that nodes in the namespace are restored
    response = client_with_examples.get("/namespaces/foo.bar/")
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

    response = client_with_examples.get("/history/namespace/foo.bar/")
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

    response = client_with_examples.get(
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
