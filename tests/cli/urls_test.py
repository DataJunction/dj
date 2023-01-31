"""
Tests for ``dj.urls``.
"""

from pytest import CaptureFixture

from dj.cli.urls import run


def test_run(capsys: CaptureFixture) -> None:
    """
    Test ``run``.
    """
    run("http://localhost:8000")

    captured = capsys.readouterr()
    assert (
        captured.out
        == """http://localhost:8000/docs: Documentation.
http://localhost:8000/databases/: List the available databases.
http://localhost:8000/queries/: Run or schedule a query.
http://localhost:8000/queries/{query_id}/: Fetch information about a query.
http://localhost:8000/metrics/: List all available metrics.
http://localhost:8000/metrics/{name}/: Return a metric by ID.
http://localhost:8000/metrics/{name}/data/: Return data for a metric.
http://localhost:8000/metrics/{name}/sql/: Return SQL for a metric.
http://localhost:8000/nodes/validate/: Validate a node.
http://localhost:8000/nodes/: List the available nodes.
http://localhost:8000/data/availability/{node_name}/: Add an availability state to a node
http://localhost:8000/graphql: GraphQL endpoint.
"""
    )
