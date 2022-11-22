"""
Tests for ``djqs.urls``.
"""

from pytest import CaptureFixture

from djqs.cli.urls import run


def test_run(capsys: CaptureFixture) -> None:
    """
    Test ``run``.
    """
    run("http://localhost:8001")

    captured = capsys.readouterr()
    assert (
        captured.out
        == """http://localhost:8001/docs: Documentation.
http://localhost:8001/databases/: List the available databases.
http://localhost:8001/queries/: Run or schedule a query.
http://localhost:8001/queries/{query_id}/: Fetch information about a query.
http://localhost:8001/metrics/: List all available metrics.
http://localhost:8001/metrics/{node_id}/: Return a metric by ID.
http://localhost:8001/metrics/{node_id}/data/: Return data for a metric.
http://localhost:8001/metrics/{node_id}/sql/: Return SQL for a metric.
http://localhost:8001/nodes/: List the available nodes.
http://localhost:8001/graphql: GraphQL endpoint.
"""
    )
