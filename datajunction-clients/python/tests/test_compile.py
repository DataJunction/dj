"""
Test YAML project related things
"""
# pylint: disable=unused-argument
import os
from typing import Callable

import pytest

from datajunction import DJBuilder
from datajunction.compile import Project
from datajunction.exceptions import DJClientException, DJDeploymentFailure
from datajunction.models import NodeMode


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
        "Cannot find project root, make sure you've defined "
        "a project in a dj.yaml file"
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
    compiled_project.validate(client=builder_client)


def test_compile_deploying_a_project(
    change_to_project_dir: Callable,
    builder_client: DJBuilder,
):
    """
    Test loading, compiling, validating, and deploying a project
    """
    change_to_project_dir("project1")
    project = Project.load_current()
    compiled_project = project.compile()
    compiled_project.deploy(client=builder_client)  # Deploying will validate as well


def test_compile_redeploying_a_project(
    change_to_project_dir: Callable,
    builder_client: DJBuilder,
):
    """
    Test deploying and then redeploying a project
    """
    change_to_project_dir("project1")
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
        "Definition file stem must end with .source, .transform, "
        ".dimension, .metric, or .cube"
    ) in str(exc_info.value)

    change_to_project_dir("project5")
    project = Project.load_current()
    with pytest.raises(DJClientException) as exc_info:
        project.compile()
    assert (
        "Invalid node definition filename stem some_node.a.b.c, stem must only have a "
        "single dot separator and end with a node type i.e. my_node.source.yaml"
    ) in str(exc_info.value)


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


def test_compile_json_schema_up_to_date(change_to_package_root_dir):
    """
    Load dj.project.schema.json and make sure it's current

    If it needs to be updated, that can be done via the Project pydantic model

    ```py
    from datajunction import Project
    with open("dj.project.schema.json", "w", encoding="utf-8") as schema_file:
       schema_file.write(Project.schema_json(indent=2))
    ```
    """
    with open("dj.project.schema.json", "r", encoding="utf-8") as schema_file:
        assert schema_file.read() == Project.schema_json(indent=2)
