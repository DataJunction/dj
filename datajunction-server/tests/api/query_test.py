"""
Tests for the query API.
"""

from fastapi.testclient import TestClient


def test_query_endpoint(client_with_examples: TestClient):
    """
    Test ``GET /query/{sql}``.
    """
    response = client_with_examples.get(
        "/query/SELECT%20total_repair_cost%20FROM%20metrics",
    )
    assert response.ok
