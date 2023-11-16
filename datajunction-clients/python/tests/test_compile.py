"""
Test YAML project related things
"""
# pylint: disable=unused-argument
import os

import pytest

from datajunction import DJBuilder
from datajunction.compile import Project
from datajunction.exceptions import DJClientException
from datajunction.models import NodeMode


def test_loading_a_project(change_to_example_project_dir: None):
    """
    Test loading a project
    """
    project = Project.load_current()
    assert project.name == "My DJ Project"
    assert project.prefix == "users.me"
    assert project.priority == [
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
    assert project.root_path.endswith("examples")


def test_loading_a_project_from_a_nested_dir(change_to_example_project_dir: None):
    """
    Test loading a project while in a nested directory
    """
    os.chdir(os.path.join(os.getcwd(), "roads"))
    project = Project.load_current()
    assert project.name == "My DJ Project"
    assert project.prefix == "users.me"
    assert project.priority == [
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
    assert project.root_path.endswith("examples")


def test_raising_when_not_in_a_project_dir():
    """
    Test raising when using Project.load_current() while not in a project dir
    """
    with pytest.raises(DJClientException) as exc_info:
        Project.load_current()
    assert (
        "Cannot find project root, make sure you've defined "
        "a project in a dj.yaml file"
    ) in str(exc_info.value)


def test_compiling_a_project(change_to_example_project_dir: None):
    """
    Test loading and compiling a project
    """
    project = Project.load_current()
    compiled_project = project.compile()
    assert compiled_project.name == "My DJ Project"
    assert compiled_project.prefix == "users.me"
    assert compiled_project.mode == NodeMode.PUBLISHED
    assert compiled_project.priority == [
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
    assert compiled_project.root_path.endswith("examples")
    assert not compiled_project.validated


def test_validating_a_project(
    change_to_example_project_dir: None,
    builder_client: DJBuilder,
):
    """
    Test loading, compiling, and validating a project
    """
    project = Project.load_current()
    compiled_project = project.compile()
    compiled_project.validate(client=builder_client)


def test_deploying_a_project(
    change_to_example_project_dir: None,
    builder_client: DJBuilder,
):
    """
    Test loading, compiling, validating, and deploying a project
    """
    project = Project.load_current()
    compiled_project = project.compile()
    compiled_project.deploy(client=builder_client)  # Deploying will validate as well


def test_redeploying_a_project(
    change_to_example_project_dir: None,
    builder_client: DJBuilder,
):
    """
    Test deploying and then redeploying a project
    """
    project = Project.load_current()
    compiled_project = project.compile()
    compiled_project.deploy(client=builder_client)
    compiled_project.deploy(client=builder_client)
