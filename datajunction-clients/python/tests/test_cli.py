"""Tests DJ CLI"""

import json
import os
import sys
from io import StringIO
from typing import Callable
from unittest import mock
from unittest.mock import patch

import pytest

from datajunction import DJBuilder
from datajunction.cli import main


# Test helper functions
def run_cli_command(builder_client, args, env_vars=None):
    """
    Helper function to run a CLI command and capture output.

    Args:
        builder_client: The DJBuilder client
        args: List of command arguments (e.g., ["dj", "list", "metrics"])
        env_vars: Optional dict of environment variables

    Returns:
        The captured stdout output as a string
    """
    if env_vars is None:
        env_vars = {
            "DJ_USER": "datajunction",
            "DJ_PWD": "datajunction",
        }

    with patch.dict(os.environ, env_vars, clear=False):
        with patch.object(sys, "argv", args):
            with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                main(builder_client=builder_client)
    return mock_stdout.getvalue()


def assert_in_output(output, *expected_strings):
    """Assert that all expected strings are in the output."""
    for expected in expected_strings:
        assert expected in output, f"Expected '{expected}' not found in output"


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


@pytest.mark.parametrize(
    "object_type,expected_in_output",
    [
        # dj list metrics
        ("metrics", ["Metrics:", "Total:"]),
        # dj list dimensions
        ("dimensions", ["Dimensions:", "Total:"]),
        # dj list namespaces
        ("namespaces", ["Namespaces:", "default"]),
        # dj list nodes
        ("nodes", ["Nodes:", "Total:"]),
        # dj list cubes
        ("cubes", ["Cubes:", "Total:"]),
        # dj list sources
        ("sources", ["Sources:", "Total:"]),
        # dj list transforms
        ("transforms", ["Transforms:", "Total:"]),
    ],
)
def test_list_objects(
    builder_client: DJBuilder,
    object_type: str,
    expected_in_output: list,
):
    """
    Test `dj list <type>` for various object types.
    """
    output = run_cli_command(builder_client, ["dj", "list", object_type])
    assert_in_output(output, *expected_in_output)


@pytest.mark.parametrize(
    "object_type,expected_in_output",
    [
        # dj list metrics
        ("metrics", ["ERROR: node namespace `na` does not exist"]),
        # dj list dimensions
        ("dimensions", ["ERROR: node namespace `na` does not exist"]),
        # # dj list namespaces
        ("namespaces", ["No namespaces found in `na`"]),
        # dj list nodes
        ("nodes", ["ERROR: node namespace `na` does not exist"]),
        # dj list cubes
        ("cubes", ["ERROR: node namespace `na` does not exist"]),
        # dj list sources
        ("sources", ["ERROR: node namespace `na` does not exist"]),
        # dj list transforms
        ("transforms", ["ERROR: node namespace `na` does not exist"]),
    ],
)
def test_list_objects_none(
    builder_client: DJBuilder,
    object_type: str,
    expected_in_output: list,
):
    """
    Test `dj list <type>` for various object types.
    """
    output = run_cli_command(
        builder_client,
        ["dj", "list", object_type, "--namespace", "na"],
    )
    assert_in_output(output, *expected_in_output)


def test_list_json(builder_client: DJBuilder):  # pylint: disable=redefined-outer-name
    """
    Test `dj list metrics --format json`
    """
    output = run_cli_command(
        builder_client,
        ["dj", "list", "metrics", "--format", "json"],
    )
    data = json.loads(output)
    assert isinstance(data, list)


@pytest.mark.parametrize(
    "command,node_name,expected_in_output",
    [
        ("sql", "default.num_repair_orders", ["SELECT"]),
        (
            "lineage",
            "default.num_repair_orders",
            ["Lineage for:", "Upstream dependencies", "Downstream dependencies"],
        ),
        ("dimensions", "default.num_repair_orders", ["Available dimensions for:"]),
    ],
)
def test_node_commands(
    builder_client: DJBuilder,
    command: str,
    node_name: str,
    expected_in_output: list,
):  # pylint: disable=redefined-outer-name
    """
    Test various node-specific commands (sql, lineage, dimensions).
    """
    output = run_cli_command(builder_client, ["dj", command, node_name])
    assert_in_output(output, *expected_in_output)


@pytest.mark.parametrize(
    "command,node_name,format_type",
    [
        # dj lineage default.num_repair_orders --format json
        ("lineage", "default.num_repair_orders", "json"),
        # dj dimensions default.num_repair_orders --format json
        ("dimensions", "default.num_repair_orders", "json"),
        # dj describe default.num_repair_orders --format json
        ("describe", "default.num_repair_orders", "json"),
    ],
)
def test_json_output_commands(
    builder_client: DJBuilder,
    command: str,
    node_name: str,
    format_type: str,
):  # pylint: disable=redefined-outer-name
    """
    Test JSON output format for various commands.
    """
    output = run_cli_command(
        builder_client,
        ["dj", command, node_name, "--format", format_type],
    )
    data = json.loads(output)
    assert isinstance(data, (dict, list))


@pytest.mark.parametrize(
    "node_name,expected_in_output",
    [
        (
            "default.repair_orders",
            [
                "Node: default.repair_orders",
                "Type:",
                "Description:",
                "Status:",
                "Mode:",
                "Display Name:",
                "Columns:",
            ],
        ),
        (
            "default.repair_orders_thin",
            [
                "Node: default.repair_orders_thin",
                "Type:",
                "Description:",
                "Status:",
                "Mode:",
                "Display Name:",
                "Columns:",
            ],
        ),
        (
            "default.hard_hat",
            [
                "Node: default.hard_hat",
                "Type:",
                "Description:",
                "Status:",
                "Mode:",
                "Display Name:",
                "Columns:",
                "Primary Key:",
            ],
        ),
        (
            "default.num_repair_orders",
            [
                "Node: default.num_repair_orders",
                "Type:",
                "Description:",
                "Status:",
                "Mode:",
                "Display Name:",
                "Version:",
            ],
        ),
        (
            "default.cube_two",
            [
                "Node: default.cube_two",
                "Type:",
                "Description:",
                "Metrics:",
                "Dimensions:",
            ],
        ),
        ("default.none", ["ERROR: No node with name default.none exists"]),
    ],
)
def test_describe(builder_client: DJBuilder, node_name: str, expected_in_output: list):  # pylint: disable=redefined-outer-name
    """
    Test `dj describe <node-name>` for various node types.
    """
    output = run_cli_command(builder_client, ["dj", "describe", node_name])
    assert_in_output(output, *expected_in_output)


def test_list_metrics(builder_client: DJBuilder):  # pylint: disable=redefined-outer-name
    """
    Test `dj list metrics`
    """
    env_vars = {
        "DJ_USER": "datajunction",
        "DJ_PWD": "datajunction",
    }
    test_args = ["dj", "list", "metrics"]
    with patch.dict(os.environ, env_vars, clear=False):
        with patch.object(sys, "argv", test_args):
            with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                main(builder_client=builder_client)
    output = mock_stdout.getvalue()
    assert "Metrics:" in output
    assert "Total:" in output


def test_list_namespaces(builder_client: DJBuilder):  # pylint: disable=redefined-outer-name
    """
    Test `dj list namespaces`
    """
    env_vars = {
        "DJ_USER": "datajunction",
        "DJ_PWD": "datajunction",
    }
    test_args = ["dj", "list", "namespaces"]
    with patch.dict(os.environ, env_vars, clear=False):
        with patch.object(sys, "argv", test_args):
            with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                main(builder_client=builder_client)
    output = mock_stdout.getvalue()
    assert "Namespaces:" in output
    assert "default" in output


def test_sql(builder_client: DJBuilder):  # pylint: disable=redefined-outer-name
    """
    Test `dj sql <node-name>`
    """
    env_vars = {
        "DJ_USER": "datajunction",
        "DJ_PWD": "datajunction",
    }
    test_args = ["dj", "sql", "default.num_repair_orders"]
    with patch.dict(os.environ, env_vars, clear=False):
        with patch.object(sys, "argv", test_args):
            with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                main(builder_client=builder_client)
    output = mock_stdout.getvalue()
    assert "SELECT" in output.upper()


def test_sql_with_dimensions(builder_client: DJBuilder):  # pylint: disable=redefined-outer-name
    """
    Test `dj sql <node-name> --dimensions dim1,dim2`
    """
    env_vars = {
        "DJ_USER": "datajunction",
        "DJ_PWD": "datajunction",
    }
    test_args = [
        "dj",
        "sql",
        "default.num_repair_orders",
        "--dimensions",
        "default.hard_hat.city",
    ]
    with patch.dict(os.environ, env_vars, clear=False):
        with patch.object(sys, "argv", test_args):
            with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                main(builder_client=builder_client)
    output = mock_stdout.getvalue()
    assert "SELECT" in output.upper()


def test_lineage(builder_client: DJBuilder):  # pylint: disable=redefined-outer-name
    """
    Test `dj lineage <node-name>`
    """
    output = run_cli_command(
        builder_client,
        ["dj", "lineage", "default.num_repair_orders"],
    )
    assert "Lineage for:" in output
    assert "Upstream dependencies" in output
    assert "Downstream dependencies" in output

    output = run_cli_command(
        builder_client,
        ["dj", "lineage", "default.num_repair_orders", "--direction", "upstream"],
    )
    assert "Lineage for:" in output
    assert "Upstream dependencies" in output

    output = run_cli_command(
        builder_client,
        ["dj", "lineage", "default.num_repair_orders", "--direction", "downstream"],
    )
    assert "Lineage for:" in output
    assert "Downstream dependencies" in output

    # cube nodes will have no downstreams
    output = run_cli_command(
        builder_client,
        ["dj", "lineage", "default.cube_two", "--direction", "downstream"],
    )
    assert "(none)" in output

    # source nodes will have no upstreams
    output = run_cli_command(
        builder_client,
        ["dj", "lineage", "default.repair_orders", "--direction", "upstream"],
    )
    assert "(none)" in output


def test_lineage_upstream(builder_client: DJBuilder):  # pylint: disable=redefined-outer-name
    """
    Test `dj lineage <node-name> --direction upstream`
    """
    env_vars = {
        "DJ_USER": "datajunction",
        "DJ_PWD": "datajunction",
    }
    test_args = [
        "dj",
        "lineage",
        "default.num_repair_orders",
        "--direction",
        "upstream",
    ]
    with patch.dict(os.environ, env_vars, clear=False):
        with patch.object(sys, "argv", test_args):
            with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                main(builder_client=builder_client)
    output = mock_stdout.getvalue()
    assert "Lineage for:" in output
    assert "Upstream dependencies" in output


def test_lineage_json(builder_client: DJBuilder):  # pylint: disable=redefined-outer-name
    """
    Test `dj lineage <node-name> --format json`
    """
    env_vars = {
        "DJ_USER": "datajunction",
        "DJ_PWD": "datajunction",
    }
    test_args = ["dj", "lineage", "default.num_repair_orders", "--format", "json"]
    with patch.dict(os.environ, env_vars, clear=False):
        with patch.object(sys, "argv", test_args):
            with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                main(builder_client=builder_client)
    output = mock_stdout.getvalue()
    import json

    data = json.loads(output)
    assert "node" in data
    assert "upstream" in data
    assert "downstream" in data


def test_dimensions(builder_client: DJBuilder):  # pylint: disable=redefined-outer-name
    """
    Test `dj dimensions <node-name>`
    """
    output = run_cli_command(
        builder_client,
        ["dj", "dimensions", "default.num_repair_orders"],
    )
    assert "Available dimensions for:" in output
    output = run_cli_command(builder_client, ["dj", "dimensions", "default.hard_hats"])
    assert "No dimensions available" in output


def test_dimensions_json(builder_client: DJBuilder):  # pylint: disable=redefined-outer-name
    """
    Test `dj dimensions <node-name> --format json`
    """
    output = run_cli_command(
        builder_client,
        ["dj", "dimensions", "default.num_repair_orders", "--format", "json"],
    )
    data = json.loads(output)
    assert isinstance(data, list)


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
