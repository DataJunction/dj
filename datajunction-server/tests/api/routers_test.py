"""
Tests for the custom API routers.
"""
from fastapi.testclient import TestClient


def test_api_router_trailing_slashes(
    client_with_examples: TestClient,
) -> None:
    """
    Test that the API router used by our endpoints will send both routes without
    trailing slashes and those with trailing slashes to right place.
    """
    response = client_with_examples.get("/attributes/")
    assert response.ok
    assert len(response.json()) > 0

    response = client_with_examples.get("/attributes")
    assert response.ok
    assert len(response.json()) > 0

    response = client_with_examples.get("/namespaces/")
    assert response.ok
    assert len(response.json()) > 0

    response = client_with_examples.get("/namespaces")
    assert response.ok
    assert len(response.json()) > 0

    response = client_with_examples.get("/namespaces/basic?type_=source")
    assert response.ok
    assert len(response.json()) > 0
