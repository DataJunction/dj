"""
Tests for the query API.
"""


def test_query_endpoint(client, load_examples):
    """
    Test ``GET /query/{sql}``.
    """
    load_examples(client)
    response = client.get("/query/SELECT%20total_repair_cost%20FROM%20metrics")
    assert response.ok
