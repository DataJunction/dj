"""
Test YAML project related things
"""
# pylint: disable=unused-argument
import os
from typing import Callable

import pytest

from datajunction import DJBuilder
from datajunction.compile import Project
from datajunction.exceptions import DJClientException
from datajunction.models import NodeMode


def test_compile_loading_a_project(change_to_project_dir: Callable):
    """
    Test loading a project
    """
    change_to_project_dir("project1")
    project = Project.load_current()
    assert project.name == "My DJ Project 1"
    assert project.prefix == "projects.project1"
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
    assert project.root_path.endswith("project1")


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
