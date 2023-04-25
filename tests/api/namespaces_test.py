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
        {"namespace": "default"},
        {"namespace": "foo.bar"},
        {"namespace": "basic"},
        {"namespace": "basic.source"},
        {"namespace": "basic.transform"},
        {"namespace": "basic.dimension"},
        {"namespace": "dbt.source"},
        {"namespace": "dbt.source.jaffle_shop"},
        {"namespace": "dbt.transform"},
        {"namespace": "dbt.dimension"},
        {"namespace": "dbt.source.stripe"},
    ]


def test_list_nodes_by_namespace(client_with_examples: TestClient) -> None:
    """
    Test ``GET /namespaces/{namespace}/``.
    """
    response = client_with_examples.get("/namespaces/basic.source/")
    assert response.ok
    assert set(response.json()) == {"basic.source.users", "basic.source.comments"}

    response = client_with_examples.get("/namespaces/basic/")
    assert response.ok
    assert set(response.json()) == {
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
    assert set(response.json()) == {
        "basic.dimension.users",
        "basic.dimension.countries",
        "basic.paint_colors_spark",
        "basic.paint_colors_trino",
    }

    response = client_with_examples.get("/namespaces/basic/?type_=source")
    assert response.ok
    assert set(response.json()) == {
        "basic.source.comments",
        "basic.murals",
        "basic.source.users",
        "basic.patches",
    }
