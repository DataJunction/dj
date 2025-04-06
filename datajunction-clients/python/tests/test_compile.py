"""
Test YAML project related things
"""

# pylint: disable=unused-argument
import os
from typing import Callable
from unittest.mock import MagicMock, call

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction import DJBuilder
from datajunction.compile import CompiledProject, Project, find_project_root
from datajunction.exceptions import DJClientException, DJDeploymentFailure
from datajunction.models import MetricDirection, MetricUnit, NodeMode


def test_compiled_project_deploy_namespaces():
    """
    Test deploying a namespace.
    """
    mock_table = MagicMock()
    cp = CompiledProject(
        name="foo",
        prefix="foo.fix",
        root_path="/foo",
        namespaces=["foo", "bar"],
    )
    # namespace creation random error
    builder_client = MagicMock(
        create_namespace=MagicMock(side_effect=DJClientException("foo error")),
    )
    cp._deploy_namespaces(  # pylint: disable=protected-access
        prefix="foo",
        table=mock_table,
        client=builder_client,
    )
    assert cp.errors[0]["error"] == "foo error"
    # namespace already exists, not an error
    cp = CompiledProject(
        name="foo",
        prefix="foo.fix",
        root_path="/foo",
        namespaces=["foo", "bar"],
    )
    builder_client = MagicMock(
        create_namespace=MagicMock(
            side_effect=DJClientException("foo bar already exists"),
        ),
    )
    cp._deploy_namespaces(  # pylint: disable=protected-access
        prefix="foo",
        table=mock_table,
        client=builder_client,
    )
    assert cp.errors == []
    assert mock_table.add_row.call_args == call(
        "foo.bar",
        "namespace",
        "[i][yellow]Namespace foo.bar already exists",
    )


def test_compiled_project__cleanup_namespace():
    """
    Test cleaning up a namespace.
    """
    cp = CompiledProject(
        name="foo",
        prefix="foo.fix",
        mode=NodeMode.DRAFT,
        root_path="/foo",
    )
    builder_client = MagicMock(
        delete_namespace=MagicMock(side_effect=DJClientException("foo error")),
    )
    cp._cleanup_namespace(  # pylint: disable=protected-access
        prefix="foo",
        client=builder_client,
    )
    assert cp.errors[0]["error"] == "foo error"


def test_find_project_root():
    """
    Test finding the project root
    """
    with pytest.raises(DJClientException) as exc_info:
        find_project_root(directory="foo")
    assert "Directory foo does not exist" in str(exc_info)


def test_compile_loading_a_project(change_to_project_dir: Callable):
    """
    Test loading a project
    """
    change_to_project_dir("project1")
    project = Project.load_current()
    assert project.name == "My DJ Project 1"
    assert project.prefix == "projects.project1"
    assert project.tags[0].name == "deprecated"
    assert project.build.priority == [
        "roads.date",
        "roads.date_dim",
        "roads.repair_orders",
        "roads.repair_order_transform",
        "roads.repair_order_details",
        "roads.contractors",
        "roads.hard_hats",
        "roads.hard_hat_state",
        "roads.us_states",
        "roads.us_region",
        "roads.dispatchers",
        "roads.municipality",
        "roads.municipality_municipality_type",
        "roads.municipality_type",
    ]
    assert project.mode == NodeMode.PUBLISHED
    assert project.root_path.endswith("project1")


def test_load_project_from_different_dir(change_to_project_dir: Callable):
    """
    Test loading a project
    """
    change_to_project_dir("./")
    project = Project.load("./project1")
    assert project.name == "My DJ Project 1"
    assert project.prefix == "projects.project1"
    assert project.tags[0].name == "deprecated"
    assert project.build.priority == [
        "roads.date",
        "roads.date_dim",
        "roads.repair_orders",
        "roads.repair_order_transform",
        "roads.repair_order_details",
        "roads.contractors",
        "roads.hard_hats",
        "roads.hard_hat_state",
        "roads.us_states",
        "roads.us_region",
        "roads.dispatchers",
        "roads.municipality",
        "roads.municipality_municipality_type",
        "roads.municipality_type",
    ]
    assert project.mode == NodeMode.PUBLISHED
    assert project.root_path.endswith("project1")


def test_compile_loading_a_project_from_a_nested_dir(change_to_project_dir: Callable):
    """
    Test loading a project while in a nested directory
    """
    change_to_project_dir("project1")
    os.chdir(os.path.join(os.getcwd(), "roads"))
    project = Project.load_current()
    assert project.name == "My DJ Project 1"
    assert project.prefix == "projects.project1"
    assert project.tags[0].name == "deprecated"
    assert project.build.priority == [
        "roads.date",
        "roads.date_dim",
        "roads.repair_orders",
        "roads.repair_order_transform",
        "roads.repair_order_details",
        "roads.contractors",
        "roads.hard_hats",
        "roads.hard_hat_state",
        "roads.us_states",
        "roads.us_region",
        "roads.dispatchers",
        "roads.municipality",
        "roads.municipality_municipality_type",
        "roads.municipality_type",
    ]
    assert project.mode == NodeMode.PUBLISHED
    assert project.root_path.endswith("project1")


def test_compile_loading_a_project_from_a_flat_dir(change_to_project_dir: Callable):
    """
    Test loading a project where everythign is flat (no sub-directories)
    """
    change_to_project_dir("project11")
    project = Project.load_current()
    assert project.name == "My DJ Project 11"
    assert project.prefix == "projects.project11"
    assert project.tags[0].name == "deprecated"
    assert project.build.priority == [
        "roads.date",
        "roads.date_dim",
        "roads.repair_orders",
        "roads.repair_order_transform",
        "roads.repair_order_details",
        "roads.contractors",
        "roads.hard_hats",
        "roads.hard_hat_state",
        "roads.us_states",
        "roads.us_region",
        "roads.dispatchers",
        "roads.municipality",
        "roads.municipality_municipality_type",
        "roads.municipality_type",
    ]
    assert project.mode == NodeMode.PUBLISHED
    assert project.root_path.endswith("project11")


def test_compile_raising_when_not_in_a_project_dir():
    """
    Test raising when using Project.load_current() while not in a project dir
    """
    with pytest.raises(DJClientException) as exc_info:
        Project.load_current()
    assert (
        "Cannot find project root, make sure you've defined a project in a dj.yaml file"
    ) in str(exc_info.value)


def test_compile_compiling_a_project(change_to_project_dir: Callable):
    """
    Test loading and compiling a project
    """
    change_to_project_dir("project1")
    project = Project.load_current()
    compiled_project = project.compile()
    assert compiled_project.name == "My DJ Project 1"
    assert compiled_project.prefix == "projects.project1"
    assert compiled_project.mode == NodeMode.PUBLISHED
    assert compiled_project.build.priority == [
        "roads.date",
        "roads.date_dim",
        "roads.repair_orders",
        "roads.repair_order_transform",
        "roads.repair_order_details",
        "roads.contractors",
        "roads.hard_hats",
        "roads.hard_hat_state",
        "roads.us_states",
        "roads.us_region",
        "roads.dispatchers",
        "roads.municipality",
        "roads.municipality_municipality_type",
        "roads.municipality_type",
    ]
    assert compiled_project.root_path.endswith("project1")
    assert not compiled_project.validated


def test_compile_validating_a_project(
    change_to_project_dir: Callable,
    builder_client: DJBuilder,
):
    """
    Test loading, compiling, and validating a project
    """
    change_to_project_dir("project1")
    project = Project.load_current()
    compiled_project = project.compile()
    compiled_project.validate(client=builder_client, with_cleanup=True)


def test_compile_deploying_a_project(
    change_to_project_dir: Callable,
    builder_client: DJBuilder,
    module__session_with_examples: TestClient,
):
    """
    Test loading, compiling, validating, and deploying a project
    """
    change_to_project_dir("project1")
    project = Project.load_current()
    compiled_project = project.compile()
    compiled_project.deploy(client=builder_client)  # Deploying will validate as well

    # Check complex join dimension links
    hard_hat = builder_client._session.get(  # pylint: disable=protected-access
        "/nodes/projects.project1.roads.hard_hat",
    ).json()
    assert [link["dimension"]["name"] for link in hard_hat["dimension_links"]] == [
        "projects.project1.roads.us_state",
        "projects.project1.roads.date_dim",
        "projects.project1.roads.date_dim",
    ]

    # Check reference dimension links
    local_hard_hats = builder_client._session.get(  # pylint: disable=protected-access
        "/nodes/projects.project1.roads.local_hard_hats",
    ).json()
    assert [
        link["dimension"]["name"] for link in local_hard_hats["dimension_links"]
    ] == ["projects.project1.roads.us_state"]
    assert [
        col["dimension"]["name"]
        for col in local_hard_hats["columns"]
        if col["name"] == "birth_date"
    ] == ["projects.project1.roads.date_dim"]

    # Check metric metadata and required dimensions
    avg_repair_price = builder_client.metric("projects.project1.roads.avg_repair_price")
    assert avg_repair_price.metric_metadata is not None
    assert avg_repair_price.metric_metadata.unit == MetricUnit.DOLLAR
    assert (
        avg_repair_price.metric_metadata.direction == MetricDirection.HIGHER_IS_BETTER
    )
    avg_length = builder_client.metric(
        "projects.project1.roads.avg_length_of_employment",
    )
    assert avg_length.required_dimensions == ["hard_hat_id"]
    assert avg_length.metric_metadata is not None
    assert avg_length.metric_metadata.unit == MetricUnit.SECOND
    assert avg_length.metric_metadata.direction == MetricDirection.HIGHER_IS_BETTER

    # Check column-level settings
    response = module__session_with_examples.get(
        "/nodes/projects.project1.roads.contractor",
    ).json()
    assert response["columns"][1]["display_name"] == "Contractor Company Name"
    response = module__session_with_examples.get(
        "/nodes/projects.project1.roads.regional_level_agg",
    ).json()
    assert response["columns"][2]["display_name"] == "Location (Hierarchy)"
    assert response["columns"][2]["description"] == "The hierarchy of the location"
    assert response["columns"][2]["attributes"] == [
        {"attribute_type": {"namespace": "system", "name": "dimension"}},
    ]

    # check custom metadata
    national_level_agg = builder_client.transform(
        "projects.project1.roads.national_level_agg",
    )
    assert national_level_agg.custom_metadata == {
        "level": "national",
        "sublevel": "state",
    }


def test_compile_redeploying_a_project(
    change_to_project_dir: Callable,
    builder_client: DJBuilder,
):
    """
    Test deploying and then redeploying a project
    """
    change_to_project_dir("project12")
    project = Project.load_current()
    compiled_project = project.compile()
    compiled_project.deploy(client=builder_client)
    compiled_project.deploy(client=builder_client)


def test_compile_raising_on_invalid_table_name(
    change_to_project_dir: Callable,
):
    """
    Test raising when a table name is missing a catalog
    """
    change_to_project_dir("project2")
    project = Project.load_current()
    with pytest.raises(DJClientException) as exc_info:
        project.compile()
    assert (
        "Invalid table name roads.us_states, table name "
        "must be fully qualified: <catalog>.<schema>.<table>"
    ) in str(exc_info.value)


def test_compile_raising_on_invalid_file_name(
    change_to_project_dir: Callable,
):
    """
    Test raising when a YAML file is missing a required node type identifier
    """
    change_to_project_dir("project3")
    project = Project.load_current()
    with pytest.raises(DJClientException) as exc_info:
        project.compile()
    assert (
        "Invalid node definition filename some_node, node definition filename "
        "must end with a node type i.e. my_node.source.yaml"
    ) in str(exc_info.value)

    change_to_project_dir("project5")
    project = Project.load_current()
    with pytest.raises(DJClientException) as exc_info:
        project.compile()
    assert (
        "Invalid node definition filename stem some_node.a.b.c, stem must only have a "
        "single dot separator and end with a node type i.e. my_node.source.yaml"
    ) in str(exc_info.value)


def test_compile_error_on_invalid_dimension_link(
    change_to_project_dir: Callable,
    builder_client: DJBuilder,
):
    """
    Test compiling and receiving an error on a dimension link
    """
    change_to_project_dir("project7")
    project = Project.load_current()
    compiled_project = project.compile()
    with pytest.raises(DJDeploymentFailure) as exc_info:
        compiled_project.deploy(client=builder_client)

    assert str("Node definition contains references to nodes that do not exist") in str(
        exc_info.value.errors[0],
    )


def test_compile_deeply_nested_namespace(
    change_to_project_dir: Callable,
    builder_client: DJBuilder,
):
    """
    Test compiling a node in a deeply nested namespace
    """
    change_to_project_dir("project4")
    project = Project.load_current()
    compiled_project = project.compile()
    compiled_project.deploy(client=builder_client)


def test_compile_error_on_individual_node(
    change_to_project_dir: Callable,
    builder_client: DJBuilder,
):
    """
    Test compiling and receiving an error on an individual node definition
    """
    change_to_project_dir("project6")
    project = Project.load_current()
    compiled_project = project.compile()
    with pytest.raises(DJDeploymentFailure) as exc_info:
        compiled_project.deploy(client=builder_client)

    assert str("Node definition contains references to nodes that do not exist") in str(
        exc_info.value.errors[0],
    )


def test_compile_raise_on_priority_with_node_missing_a_definition(
    change_to_project_dir: Callable,
):
    """
    Test raising an error when the priority list includes a node name
    that has no corresponding definition
    """
    change_to_project_dir("project8")
    project = Project.load_current()
    with pytest.raises(DJClientException) as exc_info:
        project.compile()

    assert str(
        "Build priority list includes node name "
        "node.that.does.not.exist which has no corresponding definition",
    ) in str(exc_info.value)


def test_compile_duplicate_tags(
    change_to_project_dir: Callable,
    builder_client: DJBuilder,
):
    """
    Test that deploying duplicate tags are gracefully handled
    """
    change_to_project_dir("project10")
    project = Project.load_current()
    compiled_project = project.compile()
    compiled_project.deploy(client=builder_client)
    compiled_project.deploy(client=builder_client)


@pytest.mark.asyncio
@pytest.mark.skip
async def test_deploy_remove_dimension_links(
    change_to_project_dir: Callable,
    builder_client: DJBuilder,
    module__session: AsyncSession,
    module__session_with_examples: TestClient,
):
    """
    Test deploying nodes with dimension links removed
    """
    change_to_project_dir("project9")
    project = Project.load_current()
    compiled_project = project.compile()
    compiled_project.deploy(client=builder_client)
    await module__session.commit()
    response = module__session_with_examples.get(
        "/nodes/projects.project7.roads.contractor",
    ).json()
    assert response["dimension_links"][0]["dimension"] == {
        "name": "projects.project7.roads.us_state",
    }

    change_to_project_dir("project12")
    project = Project.load_current()
    compiled_project = project.compile()
    compiled_project.deploy(client=builder_client)
    await module__session.commit()
    response = module__session_with_examples.get(
        "/nodes/projects.project7.roads.contractor",
    ).json()
    assert response["dimension_links"] == []


def test_compile_pull_a_namespaces(builder_client: DJBuilder, tmp_path):
    """
    Test pulling a namespace down into a local YAML project
    """
    # Link a dimension so that we can test that path
    node = builder_client.source("default.repair_order_details")
    node.link_dimension(column="repair_order_id", dimension="default.repair_order")

    os.chdir(tmp_path)
    Project.pull(client=builder_client, namespace="default", target_path=tmp_path)
    project = Project.load_current()
    assert project.name == "Project default (Autogenerated)"
    assert project.prefix == "default"
    compiled_project = project.compile()

    # The cube should be last in the topological ordering
    assert compiled_project.build.priority[-1] == "cube_two"

    assert {node.name for node in compiled_project.definitions} == {
        "municipality_municipality_type",
        "contractors",
        "hard_hats",
        "municipality",
        "repair_order_details",
        "municipality_type",
        "repair_type",
        "us_states",
        "dispatchers",
        "hard_hat_state",
        "repair_orders",
        "repair_orders_foo",
        "us_region",
        "repair_orders_thin",
        "repair_order",
        "municipality_dim",
        "dispatcher",
        "contractor",
        "local_hard_hats",
        "us_state",
        "hard_hat",
        "total_repair_order_discounts",
        "avg_repair_order_discounts",
        "avg_time_to_dispatch",
        "avg_length_of_employment",
        "total_repair_cost",
        "num_repair_orders",
        "avg_repair_price",
        "cube_two",
    }


def test_compile_pull_raise_error_if_dir_not_empty(builder_client: DJBuilder, tmp_path):
    """
    Test raising an error when pulling a namespace down into a non-empty directory
    """
    os.chdir(tmp_path)
    open(  # pylint: disable=consider-using-with
        "random_file.txt",
        "w",
        encoding="utf-8",
    ).close()
    with pytest.raises(DJClientException) as exc_info:
        Project.pull(client=builder_client, namespace="default", target_path=tmp_path)
    assert "The target path must be empty" in str(exc_info.value)
