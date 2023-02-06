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
    print(captured.out)
    assert (
        captured.out
        == """http://localhost:8000/docs: Documentation.
http://localhost:8000/catalogs/: List all available catalogs
http://localhost:8000/catalogs/{name}/: Return a catalog by name
http://localhost:8000/catalogs/{name}/engines/: Attach one or more engines to a catalog
http://localhost:8000/databases/: List the available databases.
http://localhost:8000/engines/: List all available engines
http://localhost:8000/engines/{name}/{version}/: Return an engine by name and version
http://localhost:8000/queries/: Run or schedule a query.
http://localhost:8000/queries/{query_id}/: Fetch information about a query.
http://localhost:8000/metrics/: List all available metrics.
http://localhost:8000/metrics/{name}/: Return a metric by name.
http://localhost:8000/metrics/{name}/data/: Return data for a metric.
http://localhost:8000/metrics/{name}/sql/: Return SQL for a metric.
http://localhost:8000/nodes/validate/: Validate a node.
http://localhost:8000/nodes/: List the available reference nodes.
http://localhost:8000/nodes/{name}/: List the specified reference node and include all revisions
http://localhost:8000/data/availability/{node_name}/: Add an availability state to a node
http://localhost:8000/graphql: GraphQL endpoint.
"""
    )
