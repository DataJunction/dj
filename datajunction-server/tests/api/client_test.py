"""
Tests for client code generator.
"""

import os
from pathlib import Path

import pytest
from httpx import AsyncClient

TEST_DIR = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture
def load_expected_file():
    """
    Loads expected fixture file
    """

    def _load(filename: str):
        expected_path = TEST_DIR / Path("files/client_test")
        with open(expected_path / filename, encoding="utf-8") as fe:
            return fe.read().strip()

    return _load


def trim_trailing_whitespace(string: str) -> str:
    """Trim trailing whitespace on each line"""
    return "\n".join([chunk.rstrip() for chunk in string.split("\n")]).strip()


@pytest.mark.asyncio
async def test_generated_python_client_code_new_source(
    module__client_with_roads: AsyncClient,
    load_expected_file,
):
    """
    Test generating Python client code for creating a new source
    """
    expected = load_expected_file("register_table.txt")
    response = await module__client_with_roads.get(
        "/datajunction-clients/python/new_node/default.repair_order_details"
        "?include_client_setup=false",
    )
    assert trim_trailing_whitespace(response.json()) == expected
    response = await module__client_with_roads.get(
        "/datajunction-clients/python/new_node/default.repair_order_details"
        "?include_client_setup=false&replace_namespace=%7Bnamespace%7D",
    )
    assert trim_trailing_whitespace(response.json()) == expected


@pytest.mark.asyncio
async def test_generated_python_client_code_new_transform(
    module__client_with_roads: AsyncClient,
    load_expected_file,
):
    """
    Test generating Python client code for creating a new transform
    """
    response = await module__client_with_roads.get(
        "/datajunction-clients/python/new_node/default.regional_level_agg"
        "?include_client_setup=false",
    )
    assert (
        trim_trailing_whitespace(response.json()).strip()
        == load_expected_file("create_transform.regional_level_agg.txt").strip()
    )
    response = await module__client_with_roads.get(
        "/datajunction-clients/python/new_node/default.regional_level_agg"
        "?include_client_setup=false&replace_namespace=%7Bnamespace%7D",
    )
    assert (
        trim_trailing_whitespace(response.json()).strip()
        == load_expected_file(
            "create_transform.regional_level_agg.namespace.txt",
        ).strip()
    )


@pytest.mark.asyncio
async def test_generated_python_client_code_new_dimension(
    module__client_with_roads: AsyncClient,
    load_expected_file,
):
    """
    Test generating Python client code for creating a new dimension
    """
    response = await module__client_with_roads.get(
        "/datajunction-clients/python/new_node/default.repair_order"
        "?include_client_setup=false",
    )
    assert (
        trim_trailing_whitespace(response.json()).strip()
        == load_expected_file("create_dimension.repair_order.txt").strip()
    )

    response = await module__client_with_roads.get(
        "/datajunction-clients/python/new_node/default.repair_order"
        "?include_client_setup=false&replace_namespace=%7Bnamespace%7D",
    )
    assert (
        trim_trailing_whitespace(response.json()).strip()
        == load_expected_file("create_dimension.repair_order.namespace.txt").strip()
    )


@pytest.mark.asyncio
async def test_generated_python_client_code_new_metric(
    module__client_with_roads: AsyncClient,
    load_expected_file,
):
    """
    Test generating Python client code for creating a new metric
    """
    response = await module__client_with_roads.get(
        "/datajunction-clients/python/new_node/default.num_repair_orders"
        "?include_client_setup=false",
    )
    assert (
        trim_trailing_whitespace(response.json()).strip()
        == load_expected_file("create_metric.num_repair_orders.txt").strip()
    )

    response = await module__client_with_roads.get(
        "/datajunction-clients/python/new_node/default.num_repair_orders"
        "?include_client_setup=false&replace_namespace=%7Bnamespace%7D",
    )
    assert (
        trim_trailing_whitespace(response.json()).strip()
        == load_expected_file("create_metric.num_repair_orders.namespace.txt").strip()
    )


@pytest.mark.asyncio
async def test_generated_python_client_code_new_cube(
    module__client_with_roads: AsyncClient,
    load_expected_file,
):
    """
    Test generating Python client code for creating a new cube
    """
    await module__client_with_roads.post(
        "/nodes/cube/",
        json={
            "metrics": ["default.num_repair_orders", "default.total_repair_cost"],
            "dimensions": [
                "default.hard_hat.country",
                "default.hard_hat.city",
            ],
            "description": "Cube of various metrics related to repairs",
            "mode": "published",
            "name": "default.repairs_cube",
        },
    )
    response = await module__client_with_roads.get(
        "/datajunction-clients/python/new_node/default.repairs_cube"
        "?include_client_setup=false",
    )
    assert (
        trim_trailing_whitespace(response.json()).strip()
        == load_expected_file("create_cube.repairs_cube.txt").strip()
    )
    response = await module__client_with_roads.get(
        "/datajunction-clients/python/new_node/default.repairs_cube"
        "?include_client_setup=false&replace_namespace=%7Bnamespace%7D",
    )
    assert (
        trim_trailing_whitespace(response.json()).strip()
        == load_expected_file(
            "create_cube.repairs_cube.namespace.txt",
        ).strip()
    )


@pytest.mark.asyncio
async def test_generated_python_client_code_link_dimension(
    module__client_with_roads: AsyncClient,
    load_expected_file,
):
    """
    Test generating Python client code for creating a new dimension
    """
    response = await module__client_with_roads.get(
        "/datajunction-clients/python/dimension_links/default.repair_orders/"
        "?include_client_setup=false",
    )
    assert (
        response.json().strip()
        == load_expected_file("dimension_links.repair_orders.txt").strip()
    )
    # When replace_namespace is set, verify that the namespaces are replaced in the
    # dimension links' join SQL
    response = await module__client_with_roads.get(
        "/datajunction-clients/python/dimension_links/default.repair_orders/"
        "?include_client_setup=false&replace_namespace=%7Bnamespace%7D",
    )
    assert (
        response.json().strip()
        == load_expected_file(
            "dimension_links.repair_orders.namespace.txt",
        ).strip()
    )


@pytest.mark.asyncio
async def test_include_client_setup(
    module__client_with_roads: AsyncClient,
    load_expected_file,
):
    """
    Generate create new node python client code with client setup included.
    """
    response = await module__client_with_roads.get(
        "/datajunction-clients/python/new_node/default.num_repair_orders"
        "?include_client_setup=true",
    )
    assert (
        trim_trailing_whitespace(response.json()).strip()
        == load_expected_file("include_client_setup.txt").strip()
    )


@pytest.mark.asyncio
async def test_export_namespace_as_notebook(
    module__client_with_roads: AsyncClient,
    load_expected_file,
):
    """
    Verify exporting all nodes in a namespace as a notebook.
    """
    response = await module__client_with_roads.post(
        "/nodes/default.repair_order_details/columns/repair_type_id/attributes/",
        json=[
            {
                "namespace": "system",
                "name": "dimension",
            },
        ],
    )
    response = await module__client_with_roads.get(
        "/datajunction-clients/python/notebook?namespace=default",
    )
    assert (
        response.headers["content-disposition"] == 'attachment; filename="export.ipynb"'
    )
    notebook = response.json()
    assert len(notebook["cells"]) >= 50
    # Intro cell
    assert notebook["cells"][0]["cell_type"] == "markdown"
    assert (
        "## DJ Namespace Export\n\nExported `default`" in notebook["cells"][0]["source"]
    )

    # Client setup cell
    assert notebook["cells"][1]["cell_type"] == "code"
    assert "from datajunction import" in notebook["cells"][1]["source"]

    # Documenting what nodes are being upserted cell
    assert notebook["cells"][2]["cell_type"] == "markdown"
    assert "### Upserting Nodes:" in notebook["cells"][2]["source"]

    # Namespace mapping configuration cell
    assert notebook["cells"][3]["cell_type"] == "code"
    assert (
        notebook["cells"][3]["source"]
        == """# A mapping from current namespaces to new namespaces
# Note: Editing the mapping will result in the nodes under that namespace getting
# copied to the new namespace

NAMESPACE_MAPPING = {
    "default": "default",
}"""
    )

    # Registering table
    assert notebook["cells"][4]["cell_type"] == "code"
    assert load_expected_file("register_table.txt").strip() in [
        content["source"] for content in notebook["cells"]
    ]

    # Linking dimensions for table
    assert "Linking dimensions for source node `default.repair_order_details`:" in [
        content["source"] for content in notebook["cells"]
    ]
    assert load_expected_file(
        "notebook.link_dimension.txt",
    ).strip() in [content["source"] for content in notebook["cells"]]
    # Check column attributes
    assert load_expected_file(
        "notebook.set_attribute.txt",
    ).strip() in [content["source"] for content in notebook["cells"]]


@pytest.mark.asyncio
async def test_export_cube_as_notebook(
    module__client_with_roads: AsyncClient,
    load_expected_file,
):
    """
    Verify exporting all nodes relevant for a cube as a notebook.
    """
    await module__client_with_roads.post(
        "/nodes/cube/",
        json={
            "metrics": ["default.num_repair_orders", "default.total_repair_cost"],
            "dimensions": [
                "default.hard_hat.country",
                "default.hard_hat.city",
            ],
            "description": "Cube of various metrics related to repairs",
            "mode": "published",
            "name": "default.roads_cube",
        },
    )
    response = await module__client_with_roads.get(
        "/datajunction-clients/python/notebook?cube=default.roads_cube",
    )
    assert (
        response.headers["content-disposition"] == 'attachment; filename="export.ipynb"'
    )
    notebook = response.json()
    assert len(notebook["cells"]) == 10

    # Intro cell
    assert notebook["cells"][0]["cell_type"] == "markdown"
    assert (
        "## DJ Cube Export\n\nExported `default.roads_cube` v1.0"
        in notebook["cells"][0]["source"]
    )

    # Documenting which nodes are getting exported
    nodes_cell_source = notebook["cells"][2]["source"]
    assert "### Upserting Nodes:" in nodes_cell_source
    # These nodes should be in the export
    assert "default.repair_orders_fact" in nodes_cell_source
    assert "default.num_repair_orders" in nodes_cell_source
    assert "default.total_repair_cost" in nodes_cell_source
    assert "default.roads_cube" in nodes_cell_source

    # Export first transform
    assert trim_trailing_whitespace(
        notebook["cells"][4]["source"],
    ) == load_expected_file(
        "notebook.create_transform.txt",
    )

    # Include sources and dimensions
    response = await module__client_with_roads.get(
        "/datajunction-clients/python/notebook?cube=default.roads_cube"
        "&include_sources=true&include_dimensions=true",
    )
    notebook = response.json()
    assert len(notebook["cells"]) >= 20
    assert "### Upserting Nodes:" in notebook["cells"][2]["source"]
    assert load_expected_file(
        "notebook.create_cube.txt",
    ).strip() in [
        trim_trailing_whitespace(content["source"]) for content in notebook["cells"]
    ]


@pytest.mark.asyncio
async def test_export_notebook_failures(module__client_with_roads: AsyncClient):
    """
    Verify that trying to set both cube and namespace when exporting to a notebook will fail
    """
    response = await module__client_with_roads.get(
        "/datajunction-clients/python/notebook?namespace=default&cube=default.roads_cube",
    )
    assert (
        response.json()["message"]
        == "Can only specify export of either a namespace or a cube."
    )
