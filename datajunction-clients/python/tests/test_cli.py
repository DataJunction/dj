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


class TestPushDeploymentSourceFlags:
    """Tests for the deployment source CLI flags on push command."""

    def test_push_help_shows_source_flags(self, builder_client: DJBuilder):
        """Test that --help shows the new deployment source flags."""
        test_args = ["dj", "push", "--help"]
        with patch.object(sys, "argv", test_args):
            with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                with pytest.raises(SystemExit) as excinfo:
                    main(builder_client=builder_client)
                assert excinfo.value.code == 0
        output = mock_stdout.getvalue()
        assert "--repo" in output
        assert "--branch" in output
        assert "--commit" in output
        assert "--ci-system" in output
        assert "--ci-run-url" in output

    def test_push_with_repo_flag_sets_env_var(
        self,
        builder_client: DJBuilder,
        change_to_project_dir,
    ):
        """Test that --repo flag sets DJ_DEPLOY_REPO env var."""
        change_to_project_dir("./")

        env_vars = {
            "DJ_USER": "datajunction",
            "DJ_PWD": "datajunction",
        }
        test_args = [
            "dj",
            "push",
            "./deploy0",
            "--repo",
            "github.com/test/repo",
        ]

        with patch.dict(os.environ, env_vars, clear=False):
            with patch.object(sys, "argv", test_args):
                main(builder_client=builder_client)
            # Verify env var was set (check inside the patch.dict context)
            assert os.environ.get("DJ_DEPLOY_REPO") == "github.com/test/repo"

    def test_push_with_all_source_flags_sets_env_vars(
        self,
        builder_client: DJBuilder,
        change_to_project_dir,
    ):
        """Test that all source flags set corresponding env vars."""
        change_to_project_dir("./")

        env_vars = {
            "DJ_USER": "datajunction",
            "DJ_PWD": "datajunction",
        }
        test_args = [
            "dj",
            "push",
            "./deploy0",
            "--namespace",
            "source_flags_test",
            "--repo",
            "github.com/org/repo",
            "--branch",
            "main",
            "--commit",
            "abc123def",
            "--ci-system",
            "jenkins",
            "--ci-run-url",
            "https://jenkins.example.com/job/123",
        ]

        with patch.dict(os.environ, env_vars, clear=False):
            with patch.object(sys, "argv", test_args):
                main(builder_client=builder_client)
            # Verify all env vars were set (check inside the patch.dict context)
            assert os.environ.get("DJ_DEPLOY_REPO") == "github.com/org/repo"
            assert os.environ.get("DJ_DEPLOY_BRANCH") == "main"
            assert os.environ.get("DJ_DEPLOY_COMMIT") == "abc123def"
            assert os.environ.get("DJ_DEPLOY_CI_SYSTEM") == "jenkins"
            assert (
                os.environ.get("DJ_DEPLOY_CI_RUN_URL")
                == "https://jenkins.example.com/job/123"
            )

    def test_push_flags_override_existing_env_vars(
        self,
        builder_client: DJBuilder,
        change_to_project_dir,
    ):
        """Test that CLI flags override existing env vars."""
        change_to_project_dir("./")

        env_vars = {
            "DJ_USER": "datajunction",
            "DJ_PWD": "datajunction",
            "DJ_DEPLOY_REPO": "old-repo",
            "DJ_DEPLOY_BRANCH": "old-branch",
        }
        test_args = [
            "dj",
            "push",
            "./deploy0",
            "--namespace",
            "override_test",
            "--repo",
            "new-repo",
            "--branch",
            "new-branch",
        ]

        with patch.dict(os.environ, env_vars, clear=False):
            with patch.object(sys, "argv", test_args):
                main(builder_client=builder_client)
            # CLI flags should override (check inside the patch.dict context)
            assert os.environ.get("DJ_DEPLOY_REPO") == "new-repo"
            assert os.environ.get("DJ_DEPLOY_BRANCH") == "new-branch"

    def test_push_without_flags_uses_existing_env_vars(
        self,
        builder_client: DJBuilder,
        change_to_project_dir,
    ):
        """Test that push without flags respects existing env vars."""
        change_to_project_dir("./")

        env_vars = {
            "DJ_USER": "datajunction",
            "DJ_PWD": "datajunction",
            "DJ_DEPLOY_REPO": "existing-repo",
            "DJ_DEPLOY_BRANCH": "existing-branch",
        }
        test_args = ["dj", "push", "./deploy0", "--namespace", "existing_env_test"]

        with patch.dict(os.environ, env_vars, clear=False):
            with patch.object(sys, "argv", test_args):
                main(builder_client=builder_client)
            # Existing env vars should remain unchanged (check inside the patch.dict context)
            assert os.environ.get("DJ_DEPLOY_REPO") == "existing-repo"
            assert os.environ.get("DJ_DEPLOY_BRANCH") == "existing-branch"


class TestImpactAnalysis:
    """Tests for deployment impact analysis (dryrun)."""

    def test_deploy_dryrun_shows_impact(
        self,
        builder_client: DJBuilder,
        change_to_project_dir,
        capsys,
    ):
        """Test that deploy --dryrun shows impact analysis."""
        change_to_project_dir("./")

        # Use a unique namespace for this test
        test_args = ["dj", "deploy", "./deploy0", "--dryrun"]

        with patch.dict(
            os.environ,
            {"DJ_USER": "datajunction", "DJ_PWD": "datajunction"},
            clear=False,
        ):
            with patch.object(sys, "argv", test_args):
                main(builder_client=builder_client)

        # Check output contains impact analysis elements
        captured = capsys.readouterr()
        assert "Impact Analysis" in captured.out
        assert "Direct Changes" in captured.out

    def test_deploy_dryrun_json_format(
        self,
        builder_client: DJBuilder,
        change_to_project_dir,
        capsys,
    ):
        """Test that deploy --dryrun --format json outputs JSON."""
        change_to_project_dir("./")

        test_args = ["dj", "deploy", "./deploy0", "--dryrun", "--format", "json"]

        with patch.dict(
            os.environ,
            {"DJ_USER": "datajunction", "DJ_PWD": "datajunction"},
            clear=False,
        ):
            with patch.object(sys, "argv", test_args):
                main(builder_client=builder_client)

        captured = capsys.readouterr()
        # Should be valid JSON
        import json as json_module

        impact_data = json_module.loads(captured.out)
        assert "namespace" in impact_data
        assert "changes" in impact_data
        assert "create_count" in impact_data

    def test_display_impact_analysis_with_changes(self, capsys):
        """Test display_impact_analysis function with various changes."""
        from datajunction.cli import display_impact_analysis
        from rich.console import Console

        console = Console(force_terminal=True, no_color=True)

        impact = {
            "namespace": "test.namespace",
            "changes": [
                {
                    "name": "test.namespace.new_metric",
                    "operation": "create",
                    "node_type": "metric",
                    "changed_fields": [],
                },
                {
                    "name": "test.namespace.updated_transform",
                    "operation": "update",
                    "node_type": "transform",
                    "changed_fields": ["query", "description"],
                },
                {
                    "name": "test.namespace.unchanged_source",
                    "operation": "noop",
                    "node_type": "source",
                    "changed_fields": [],
                },
            ],
            "create_count": 1,
            "update_count": 1,
            "delete_count": 0,
            "skip_count": 1,
            "downstream_impacts": [
                {
                    "name": "other.namespace.downstream_node",
                    "node_type": "metric",
                    "current_status": "valid",
                    "predicted_status": "valid",
                    "impact_type": "may_affect",
                    "impact_reason": "Depends on test.namespace.updated_transform",
                    "depth": 1,
                    "caused_by": ["test.namespace.updated_transform"],
                },
            ],
            "will_invalidate_count": 0,
            "may_affect_count": 1,
            "warnings": [],
        }

        display_impact_analysis(impact, console=console)

        captured = capsys.readouterr()
        assert "test.namespace" in captured.out
        assert "Create" in captured.out
        assert "Update" in captured.out
        assert "Skip" in captured.out
        assert "May Affect" in captured.out
        assert "Ready to deploy" in captured.out

    def test_display_impact_analysis_with_warnings(self, capsys):
        """Test display_impact_analysis shows warnings."""
        from datajunction.cli import display_impact_analysis
        from rich.console import Console

        console = Console(force_terminal=True, no_color=True)

        impact = {
            "namespace": "test.namespace",
            "changes": [
                {
                    "name": "test.namespace.source_with_column_change",
                    "operation": "update",
                    "node_type": "source",
                    "changed_fields": ["columns"],
                    "column_changes": [
                        {
                            "column": "old_col",
                            "change_type": "removed",
                        },
                    ],
                },
            ],
            "create_count": 0,
            "update_count": 1,
            "delete_count": 0,
            "skip_count": 0,
            "downstream_impacts": [
                {
                    "name": "other.downstream",
                    "node_type": "transform",
                    "current_status": "valid",
                    "predicted_status": "invalid",
                    "impact_type": "will_invalidate",
                    "impact_reason": "Uses removed column 'old_col'",
                    "depth": 1,
                    "caused_by": ["test.namespace.source_with_column_change"],
                },
            ],
            "will_invalidate_count": 1,
            "may_affect_count": 0,
            "warnings": [
                "Breaking change: Column 'old_col' removed from test.namespace.source_with_column_change",
            ],
        }

        display_impact_analysis(impact, console=console)

        captured = capsys.readouterr()
        assert "Column Changes" in captured.out
        assert "Removed" in captured.out
        assert "Will Invalidate" in captured.out
        assert "Breaking change" in captured.out
        assert "Review the warnings" in captured.out

    def test_display_impact_analysis_no_changes(self, capsys):
        """Test display_impact_analysis with empty deployment."""
        from datajunction.cli import display_impact_analysis
        from rich.console import Console

        console = Console(force_terminal=True, no_color=True)

        impact = {
            "namespace": "empty.namespace",
            "changes": [],
            "create_count": 0,
            "update_count": 0,
            "delete_count": 0,
            "skip_count": 0,
            "downstream_impacts": [],
            "will_invalidate_count": 0,
            "may_affect_count": 0,
            "warnings": [],
        }

        display_impact_analysis(impact, console=console)

        captured = capsys.readouterr()
        assert "empty.namespace" in captured.out
        assert "No downstream impact" in captured.out
        assert "No warnings" in captured.out
        assert "Ready to deploy" in captured.out

    def test_display_impact_analysis_with_delete_count(self, capsys):
        """Test display_impact_analysis shows delete count in summary (covers line 92)."""
        from datajunction.cli import display_impact_analysis
        from rich.console import Console
        import re

        console = Console(force_terminal=True, no_color=True)

        impact = {
            "namespace": "test.namespace",
            "changes": [
                {
                    "name": "test.namespace.deleted_node",
                    "operation": "delete",
                    "node_type": "transform",
                    "changed_fields": [],
                },
            ],
            "create_count": 0,
            "update_count": 0,
            "delete_count": 1,
            "skip_count": 0,
            "downstream_impacts": [],
            "will_invalidate_count": 0,
            "may_affect_count": 0,
            "warnings": [],
        }

        display_impact_analysis(impact, console=console)

        captured = capsys.readouterr()
        # Strip ANSI codes for assertion
        ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
        clean_output = ansi_escape.sub("", captured.out)
        assert "1 delete" in clean_output

    def test_display_impact_analysis_with_type_changed_column(self, capsys):
        """Test display_impact_analysis shows type_changed column details (covers line 138)."""
        from datajunction.cli import display_impact_analysis
        from rich.console import Console
        import re

        console = Console(force_terminal=True, no_color=True)

        impact = {
            "namespace": "test.namespace",
            "changes": [
                {
                    "name": "test.namespace.source_node",
                    "operation": "update",
                    "node_type": "source",
                    "changed_fields": ["columns"],
                    "column_changes": [
                        {
                            "column": "user_id",
                            "change_type": "type_changed",
                            "old_type": "INT",
                            "new_type": "BIGINT",
                        },
                    ],
                },
            ],
            "create_count": 0,
            "update_count": 1,
            "delete_count": 0,
            "skip_count": 0,
            "downstream_impacts": [],
            "will_invalidate_count": 0,
            "may_affect_count": 0,
            "warnings": [],
        }

        display_impact_analysis(impact, console=console)

        captured = capsys.readouterr()
        # Strip ANSI codes for assertion
        ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
        clean_output = ansi_escape.sub("", captured.out)
        assert "Column Changes" in clean_output
        # "Type Changed" may be split across lines in the table, check for both parts
        assert "Type" in clean_output
        assert "Changed" in clean_output
        assert "user_id" in clean_output
        assert "INT" in clean_output
        assert "BIGINT" in clean_output

    def test_display_impact_analysis_with_added_column(self, capsys):
        """Test display_impact_analysis shows added column details (covers line 142)."""
        from datajunction.cli import display_impact_analysis
        from rich.console import Console
        import re

        console = Console(force_terminal=True, no_color=True)

        impact = {
            "namespace": "test.namespace",
            "changes": [
                {
                    "name": "test.namespace.source_node",
                    "operation": "update",
                    "node_type": "source",
                    "changed_fields": ["columns"],
                    "column_changes": [
                        {
                            "column": "new_column",
                            "change_type": "added",
                        },
                    ],
                },
            ],
            "create_count": 0,
            "update_count": 1,
            "delete_count": 0,
            "skip_count": 0,
            "downstream_impacts": [],
            "will_invalidate_count": 0,
            "may_affect_count": 0,
            "warnings": [],
        }

        display_impact_analysis(impact, console=console)

        captured = capsys.readouterr()
        # Strip ANSI codes for assertion
        ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
        clean_output = ansi_escape.sub("", captured.out)
        assert "Column Changes" in clean_output
        assert "Added" in clean_output
        assert "new_column" in clean_output

    def test_display_impact_analysis_with_downstream_but_no_impact_summary(
        self,
        capsys,
    ):
        """Test when downstream_impacts exists but no will_invalidate or may_affect (covers line 197->207)."""
        from datajunction.cli import display_impact_analysis
        from rich.console import Console
        import re

        console = Console(force_terminal=True, no_color=True)

        # This tests the case where downstream_impacts is non-empty but
        # will_invalidate_count and may_affect_count are both 0
        impact = {
            "namespace": "test.namespace",
            "changes": [
                {
                    "name": "test.namespace.node",
                    "operation": "update",
                    "node_type": "transform",
                    "changed_fields": ["description"],
                },
            ],
            "create_count": 0,
            "update_count": 1,
            "delete_count": 0,
            "skip_count": 0,
            "downstream_impacts": [
                {
                    "name": "other.downstream",
                    "node_type": "metric",
                    "current_status": "valid",
                    "predicted_status": "valid",
                    "impact_type": "no_impact",
                    "impact_reason": "No functional change",
                    "depth": 1,
                    "caused_by": ["test.namespace.node"],
                },
            ],
            "will_invalidate_count": 0,
            "may_affect_count": 0,
            "warnings": [],
        }

        display_impact_analysis(impact, console=console)

        captured = capsys.readouterr()
        # Strip ANSI codes for assertion
        ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
        clean_output = ansi_escape.sub("", captured.out)
        # Should NOT show "No downstream impact" since there are downstream_impacts
        # But should NOT show any summary items since counts are 0
        assert "Downstream Impact" in clean_output
        # The downstream summary line should not appear when both counts are 0
        assert "will invalidate" not in clean_output.lower()
        assert "may be affected" not in clean_output.lower()

    def test_dryrun_with_exception(self, capsys):
        """Test dryrun handles DJClientException properly (covers lines 276-286)."""
        from unittest.mock import MagicMock
        from datajunction.cli import DJCLI
        from datajunction.exceptions import DJClientException

        mock_client = MagicMock()
        cli = DJCLI(builder_client=mock_client)

        # Mock deployment_service.get_impact to raise exception
        cli.deployment_service.get_impact = MagicMock(
            side_effect=DJClientException({"message": "Test error message"}),
        )

        cli.dryrun("/fake/directory", format="text")

        captured = capsys.readouterr()
        assert "ERROR" in captured.out
        assert "Test error message" in captured.out

    def test_dryrun_with_exception_json_format(self, capsys):
        """Test dryrun handles DJClientException with JSON format (covers lines 276-286)."""
        from unittest.mock import MagicMock
        from datajunction.cli import DJCLI
        from datajunction.exceptions import DJClientException

        mock_client = MagicMock()
        cli = DJCLI(builder_client=mock_client)

        # Mock deployment_service.get_impact to raise exception
        cli.deployment_service.get_impact = MagicMock(
            side_effect=DJClientException({"message": "JSON error message"}),
        )

        cli.dryrun("/fake/directory", format="json")

        captured = capsys.readouterr()
        import json as json_module

        result = json_module.loads(captured.out)
        assert "error" in result
        assert result["error"] == "JSON error message"

    def test_dryrun_with_exception_string_error(self, capsys):
        """Test dryrun handles DJClientException with string error."""
        from unittest.mock import MagicMock
        from datajunction.cli import DJCLI
        from datajunction.exceptions import DJClientException

        mock_client = MagicMock()
        cli = DJCLI(builder_client=mock_client)

        # Mock deployment_service.get_impact to raise exception with string
        cli.deployment_service.get_impact = MagicMock(
            side_effect=DJClientException("Simple string error"),
        )

        cli.dryrun("/fake/directory", format="text")

        captured = capsys.readouterr()
        assert "ERROR" in captured.out
        assert "Simple string error" in captured.out

    def test_deploy_without_dryrun(
        self,
        builder_client,
        change_to_project_dir,
    ):
        """Test deploy command without --dryrun flag (covers line 814)."""
        change_to_project_dir("./")

        # deploy command without --dryrun should call push
        test_args = ["dj", "deploy", "./deploy0"]

        with patch.dict(
            os.environ,
            {"DJ_USER": "datajunction", "DJ_PWD": "datajunction"},
            clear=False,
        ):
            with patch.object(sys, "argv", test_args):
                main(builder_client=builder_client)

        # Verify nodes were deployed (push was called)
        results = builder_client.list_nodes(namespace="deps.deploy0")
        # deploy0 has 6 nodes, they should be deployed
        assert len(results) >= 6


class TestDJCLIClientCreation:
    """Tests for DJCLI client creation from environment variables."""

    def test_djcli_creates_client_from_dj_url_env_var(self, monkeypatch):
        """Test DJCLI creates client from DJ_URL env var when builder_client is None (covers lines 245-246)."""
        from datajunction.cli import DJCLI

        # Set DJ_URL environment variable
        monkeypatch.setenv("DJ_URL", "http://test-server:9000")

        # Create DJCLI without passing builder_client
        cli = DJCLI(builder_client=None)

        # Verify the client was created with the correct URL
        assert cli.builder_client is not None
        assert cli.builder_client.uri == "http://test-server:9000"

    def test_djcli_uses_default_url_when_env_not_set(self, monkeypatch):
        """Test DJCLI uses localhost:8000 when DJ_URL is not set."""
        from datajunction.cli import DJCLI

        # Ensure DJ_URL is not set
        monkeypatch.delenv("DJ_URL", raising=False)

        # Create DJCLI without passing builder_client
        cli = DJCLI(builder_client=None)

        # Verify the client was created with the default URL
        assert cli.builder_client is not None
        assert cli.builder_client.uri == "http://localhost:8000"


class TestPlanCommand:
    """Tests for the plan command."""

    @pytest.fixture
    def sample_plan(self):
        """Sample plan response for testing."""
        return {
            "dialect": "spark",
            "requested_dimensions": ["default.hard_hat.city", "default.hard_hat.state"],
            "grain_groups": [
                {
                    "parent_name": "default.repair_order_details",
                    "grain": ["default.hard_hat.city", "default.hard_hat.state"],
                    "aggregability": "full",
                    "metrics": [
                        "default.num_repair_orders",
                        "default.total_repair_cost",
                    ],
                    "components": [
                        {
                            "name": "default.num_repair_orders_count",
                            "expression": "COUNT(DISTINCT repair_order_id)",
                            "aggregation": "COUNT",
                            "merge": "SUM",
                        },
                        {
                            "name": "default.total_repair_cost_sum",
                            "expression": "SUM(cost)",
                            "aggregation": "SUM",
                            "merge": "SUM",
                        },
                    ],
                    "sql": "SELECT city, state, COUNT(DISTINCT repair_order_id) AS num_repair_orders_count\nFROM repair_orders\nGROUP BY city, state",
                },
            ],
            "metric_formulas": [
                {
                    "name": "default.num_repair_orders",
                    "combiner": "SUM(num_repair_orders_count)",
                    "components": ["default.num_repair_orders_count"],
                    "is_derived": False,
                },
                {
                    "name": "default.total_repair_cost",
                    "combiner": "SUM(total_repair_cost_sum)",
                    "components": ["default.total_repair_cost_sum"],
                    "is_derived": False,
                },
            ],
        }

    def test_plan_text_output(self, builder_client, sample_plan, capsys):
        """Test `dj plan --metrics <metric>` with text output."""
        with patch.object(builder_client, "plan", return_value=sample_plan):
            test_args = [
                "dj",
                "plan",
                "--metrics",
                "default.num_repair_orders",
                "default.total_repair_cost",
                "--dimensions",
                "default.hard_hat.city",
                "default.hard_hat.state",
            ]
            with patch.dict(
                os.environ,
                {"DJ_USER": "datajunction", "DJ_PWD": "datajunction"},
                clear=False,
            ):
                with patch.object(sys, "argv", test_args):
                    main(builder_client=builder_client)

        captured = capsys.readouterr()
        assert "Query Execution Plan" in captured.out
        assert "Grain Groups" in captured.out
        assert "Metric Formulas" in captured.out
        assert "default.num_repair_orders" in captured.out

    def test_plan_json_output(self, builder_client, sample_plan, capsys):
        """Test `dj plan --metrics <metric> --format json` with JSON output."""
        with patch.object(builder_client, "plan", return_value=sample_plan):
            test_args = [
                "dj",
                "plan",
                "--metrics",
                "default.num_repair_orders",
                "--format",
                "json",
            ]
            with patch.dict(
                os.environ,
                {"DJ_USER": "datajunction", "DJ_PWD": "datajunction"},
                clear=False,
            ):
                with patch.object(sys, "argv", test_args):
                    main(builder_client=builder_client)

        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert "dialect" in data
        assert "grain_groups" in data
        assert "metric_formulas" in data
        assert data["dialect"] == "spark"

    def test_plan_with_filters(self, builder_client, sample_plan, capsys):
        """Test `dj plan` with filters argument."""
        with patch.object(
            builder_client,
            "plan",
            return_value=sample_plan,
        ) as mock_plan:
            test_args = [
                "dj",
                "plan",
                "--metrics",
                "default.num_repair_orders",
                "--filters",
                "default.hard_hat.city = 'NYC'",
            ]
            with patch.dict(
                os.environ,
                {"DJ_USER": "datajunction", "DJ_PWD": "datajunction"},
                clear=False,
            ):
                with patch.object(sys, "argv", test_args):
                    main(builder_client=builder_client)

        # Verify plan was called with filters
        mock_plan.assert_called_once_with(
            metrics=["default.num_repair_orders"],
            dimensions=[],
            filters=["default.hard_hat.city = 'NYC'"],
            dialect=None,
        )

    def test_plan_with_dialect(self, builder_client, sample_plan, capsys):
        """Test `dj plan` with dialect argument."""
        with patch.object(
            builder_client,
            "plan",
            return_value=sample_plan,
        ) as mock_plan:
            test_args = [
                "dj",
                "plan",
                "--metrics",
                "default.num_repair_orders",
                "--dialect",
                "trino",
            ]
            with patch.dict(
                os.environ,
                {"DJ_USER": "datajunction", "DJ_PWD": "datajunction"},
                clear=False,
            ):
                with patch.object(sys, "argv", test_args):
                    main(builder_client=builder_client)

        # Verify plan was called with dialect
        mock_plan.assert_called_once_with(
            metrics=["default.num_repair_orders"],
            dimensions=[],
            filters=[],
            dialect="trino",
        )

    def test_plan_help(self, builder_client):
        """Test `dj plan --help` shows expected options."""
        test_args = ["dj", "plan", "--help"]
        with patch.object(sys, "argv", test_args):
            with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                with pytest.raises(SystemExit) as excinfo:
                    main(builder_client=builder_client)
                assert excinfo.value.code == 0
        output = mock_stdout.getvalue()
        assert "--metrics" in output
        assert "--dimensions" in output
        assert "--filters" in output
        assert "--dialect" in output
        assert "--format" in output

    def test_print_plan_text_with_empty_grain_groups(self, capsys):
        """Test _print_plan_text with empty grain groups."""
        from datajunction.cli import DJCLI

        mock_client = mock.MagicMock()
        cli = DJCLI(builder_client=mock_client)

        plan = {
            "dialect": "spark",
            "requested_dimensions": [],
            "grain_groups": [],
            "metric_formulas": [],
        }

        cli._print_plan_text(plan)

        captured = capsys.readouterr()
        assert "Query Execution Plan" in captured.out
        assert "Grain Groups (0)" in captured.out
        assert "Metric Formulas (0)" in captured.out

    def test_print_plan_text_with_no_sql(self, capsys):
        """Test _print_plan_text when grain group has no SQL."""
        from datajunction.cli import DJCLI

        mock_client = mock.MagicMock()
        cli = DJCLI(builder_client=mock_client)

        plan = {
            "dialect": "spark",
            "requested_dimensions": ["dim1"],
            "grain_groups": [
                {
                    "parent_name": "test.parent",
                    "grain": ["dim1"],
                    "aggregability": "full",
                    "metrics": ["metric1"],
                    "components": [],
                    "sql": "",  # Empty SQL
                },
            ],
            "metric_formulas": [
                {
                    "name": "metric1",
                    "combiner": "SUM(comp1)",
                    "components": ["comp1"],
                    "is_derived": False,
                },
            ],
        }

        cli._print_plan_text(plan)

        captured = capsys.readouterr()
        assert "Group 1: test.parent" in captured.out
        # SQL panel should not appear for empty SQL

    def test_print_plan_text_with_derived_metric(self, capsys):
        """Test _print_plan_text shows derived metric indicator."""
        from datajunction.cli import DJCLI

        mock_client = mock.MagicMock()
        cli = DJCLI(builder_client=mock_client)

        plan = {
            "dialect": "spark",
            "requested_dimensions": [],
            "grain_groups": [],
            "metric_formulas": [
                {
                    "name": "derived_metric",
                    "combiner": "metric1 / metric2",
                    "components": ["metric1", "metric2"],
                    "is_derived": True,
                },
            ],
        }

        cli._print_plan_text(plan)

        captured = capsys.readouterr()
        assert "derived_metric" in captured.out
        assert "✓" in captured.out  # Derived indicator

    def test_print_plan_text_with_component_no_merge(self, capsys):
        """Test _print_plan_text when component has no merge function."""
        from datajunction.cli import DJCLI

        mock_client = mock.MagicMock()
        cli = DJCLI(builder_client=mock_client)

        plan = {
            "dialect": "spark",
            "requested_dimensions": [],
            "grain_groups": [
                {
                    "parent_name": "test.parent",
                    "grain": [],
                    "aggregability": "full",
                    "metrics": ["metric1"],
                    "components": [
                        {
                            "name": "comp1",
                            "expression": "COUNT(*)",
                            "aggregation": "COUNT",
                            "merge": None,
                        },
                    ],
                    "sql": "SELECT COUNT(*) FROM t",
                },
            ],
            "metric_formulas": [],
        }

        cli._print_plan_text(plan)

        captured = capsys.readouterr()
        assert "comp1" in captured.out
        assert "COUNT(*)" in captured.out
