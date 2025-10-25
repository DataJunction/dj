"""Tests DJ CLI"""

import os
import sys
from io import StringIO
from typing import Callable
from unittest import mock
from unittest.mock import patch

import pytest

from datajunction import DJBuilder
from datajunction.cli import main


def test_pull(
    tmp_path,
    builder_client: DJBuilder,  # pylint: disable=redefined-outer-name
):
    """
    Test `dj pull <namespace> <dir>`
    """
    test_args = ["dj", "pull", "default", tmp_path.absolute().as_posix()]
    env_vars = {
        "DJ_USER": "datajunction",
        "DJ_PWD": "datajunction",
    }
    with patch.dict(os.environ, env_vars, clear=False):
        with patch.object(sys, "argv", test_args):
            main(builder_client=builder_client)
    assert len(os.listdir(tmp_path)) == 30


def test_push_full(
    tmp_path,
    builder_client: DJBuilder,  # pylint: disable=redefined-outer-name
    change_to_project_dir: Callable,
):
    """
    Test `dj push <dir>`
    """
    env_vars = {
        "DJ_USER": "datajunction",
        "DJ_PWD": "datajunction",
    }
    change_to_project_dir("./")
    test_args = ["dj", "push", "./deploy0"]
    with patch.dict(os.environ, env_vars, clear=False):
        with patch.object(sys, "argv", test_args):
            main(builder_client=builder_client)
    results = builder_client.list_nodes(namespace="deps.deploy0")
    assert len(results) == 6

    test_args = ["dj", "push", "./deploy0", "--namespace", "deps.deploy0.main"]
    with patch.dict(os.environ, env_vars, clear=False):
        with patch.object(sys, "argv", test_args):
            main(builder_client=builder_client)

    results = builder_client.list_nodes(namespace="deps.deploy0.main")
    assert len(results) == 6
    results = builder_client.list_nodes(namespace="deps.deploy0")
    assert len(results) == 12


def test_seed():
    """
    Test `dj seed`
    """
    builder_client = mock.MagicMock()

    test_args = ["dj", "seed"]
    with patch.object(sys, "argv", test_args):
        main(builder_client=builder_client)

    func_names = [mock_call[0] for mock_call in builder_client.mock_calls]
    assert "basic_login" in func_names
    assert "register_table" in func_names
    assert "create_dimension" in func_names
    assert "create_metric" in func_names
    assert "dimension().link_complex_dimension" in func_names


def test_help(builder_client: DJBuilder):  # pylint: disable=redefined-outer-name
    """
    Test the '--help' output.
    """
    test_args = ["dj", "--help"]
    with patch.object(sys, "argv", test_args):
        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            with pytest.raises(SystemExit) as excinfo:
                main(builder_client=builder_client)
            assert excinfo.value.code == 0  # Ensure exit code is 0 (success)
    output = mock_stdout.getvalue()
    assert "usage: dj" in output
    assert "deploy" in output
    assert "pull" in output
    assert "delete-node" in output
    assert "delete-namespace" in output


def test_invalid_command(
    builder_client: DJBuilder,  # pylint: disable=redefined-outer-name
):
    """
    Test behavior for an invalid command.
    """
    test_args = ["dj", "invalid_command"]
    with patch.object(sys, "argv", test_args):
        with pytest.raises(SystemExit):
            main(builder_client=builder_client)


def test_delete_node(
    builder_client: DJBuilder,  # pylint: disable=redefined-outer-name
):
    """
    Test `dj delete-node <node_name>`
    """
    env_vars = {
        "DJ_USER": "datajunction",
        "DJ_PWD": "datajunction",
    }

    # Mock the delete_node method
    with patch.object(builder_client, "delete_node") as mock_delete:
        test_args = ["dj", "delete-node", "default.repair_orders"]
        with patch.dict(os.environ, env_vars, clear=False):
            with patch.object(sys, "argv", test_args):
                main(builder_client=builder_client)

        # Verify the node was called with soft delete
        mock_delete.assert_called_once_with("default.repair_orders", hard=False)


def test_delete_node_hard(
    builder_client: DJBuilder,  # pylint: disable=redefined-outer-name
):
    """
    Test `dj delete-node <node_name> --hard`
    """
    env_vars = {
        "DJ_USER": "datajunction",
        "DJ_PWD": "datajunction",
    }

    # Mock the delete_node method
    with patch.object(builder_client, "delete_node") as mock_delete:
        test_args = ["dj", "delete-node", "default.repair_orders", "--hard"]
        with patch.dict(os.environ, env_vars, clear=False):
            with patch.object(sys, "argv", test_args):
                main(builder_client=builder_client)

        # Verify the node was called with hard delete
        mock_delete.assert_called_once_with("default.repair_orders", hard=True)


def test_delete_namespace(
    builder_client: DJBuilder,  # pylint: disable=redefined-outer-name
):
    """
    Test `dj delete-namespace <namespace>`
    """
    env_vars = {
        "DJ_USER": "datajunction",
        "DJ_PWD": "datajunction",
    }

    # Mock the delete_namespace method
    with patch.object(builder_client, "delete_namespace") as mock_delete:
        test_args = ["dj", "delete-namespace", "test_namespace"]
        with patch.dict(os.environ, env_vars, clear=False):
            with patch.object(sys, "argv", test_args):
                main(builder_client=builder_client)

        # Verify the namespace was called with correct parameters
        mock_delete.assert_called_once_with(
            "test_namespace",
            cascade=False,
            hard=False,
        )


def test_delete_namespace_cascade(
    builder_client: DJBuilder,  # pylint: disable=redefined-outer-name
):
    """
    Test `dj delete-namespace <namespace> --cascade`
    """
    env_vars = {
        "DJ_USER": "datajunction",
        "DJ_PWD": "datajunction",
    }

    # Mock the delete_namespace method
    with patch.object(builder_client, "delete_namespace") as mock_delete:
        test_args = ["dj", "delete-namespace", "test_namespace", "--cascade"]
        with patch.dict(os.environ, env_vars, clear=False):
            with patch.object(sys, "argv", test_args):
                main(builder_client=builder_client)

        # Verify the namespace was called with cascade
        mock_delete.assert_called_once_with(
            "test_namespace",
            cascade=True,
            hard=False,
        )


def test_delete_namespace_hard(
    builder_client: DJBuilder,  # pylint: disable=redefined-outer-name
):
    """
    Test `dj delete-namespace <namespace> --hard`
    """
    env_vars = {
        "DJ_USER": "datajunction",
        "DJ_PWD": "datajunction",
    }

    # Mock the delete_namespace method
    with patch.object(builder_client, "delete_namespace") as mock_delete:
        test_args = ["dj", "delete-namespace", "test_namespace", "--hard"]
        with patch.dict(os.environ, env_vars, clear=False):
            with patch.object(sys, "argv", test_args):
                main(builder_client=builder_client)

        # Verify the namespace was called with hard delete
        mock_delete.assert_called_once_with(
            "test_namespace",
            cascade=False,
            hard=True,
        )


def test_delete_namespace_cascade_hard(
    builder_client: DJBuilder,  # pylint: disable=redefined-outer-name
):
    """
    Test `dj delete-namespace <namespace> --cascade --hard`
    """
    env_vars = {
        "DJ_USER": "datajunction",
        "DJ_PWD": "datajunction",
    }

    # Mock the delete_namespace method
    with patch.object(builder_client, "delete_namespace") as mock_delete:
        test_args = ["dj", "delete-namespace", "test_namespace", "--cascade", "--hard"]
        with patch.dict(os.environ, env_vars, clear=False):
            with patch.object(sys, "argv", test_args):
                main(builder_client=builder_client)

        # Verify the namespace was called with both cascade and hard delete
        mock_delete.assert_called_once_with(
            "test_namespace",
            cascade=True,
            hard=True,
        )
