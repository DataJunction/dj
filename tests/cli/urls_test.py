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
http://localhost:8000/metrics/: List all available metrics.
http://localhost:8000/metrics/{name}/: Return a metric by name.
http://localhost:8000/metrics/{name}/sql/: Return SQL for a metric.
http://localhost:8000/query/validate: Return SQL for a DJ Query.
http://localhost:8000/nodes/validate/: Validate a node.
http://localhost:8000/nodes/: List the available nodes.
http://localhost:8000/nodes/{name}/: Show the active version of the specified node.
http://localhost:8000/nodes/{name}/materialization/: Update materialization config of the specified node.
http://localhost:8000/nodes/{name}/revisions/: List all revisions for the node.
http://localhost:8000/nodes/{name}/columns/{column}/: Add information to a node column
http://localhost:8000/nodes/{name}/table/: Add a table to a node
http://localhost:8000/nodes/{name}/tag/: Add a tag to a node
http://localhost:8000/nodes/similarity/{node1_name}/{node2_name}: Compare two nodes by how similar their queries are
http://localhost:8000/nodes/{name}/downstream/: List all nodes that are downstream from the given node, filterable by type.
http://localhost:8000/data/availability/{node_name}/: Add an availability state to a node
http://localhost:8000/health/: Healthcheck for services.
http://localhost:8000/cubes/{name}/: Get information on a cube
http://localhost:8000/tags/: List all available tags.
http://localhost:8000/tags/{name}/: Return a tag by name.
http://localhost:8000/tags/{name}/nodes/: Find nodes tagged with the tag, filterable by node type.
http://localhost:8000/graphql: GraphQL endpoint.
"""
    )
