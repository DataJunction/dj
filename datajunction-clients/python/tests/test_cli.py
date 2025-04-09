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


def test_deploy(
    change_to_project_dir: Callable,
):
    """
    Test `dj deploy <dir>`
    """
    builder_client = mock.MagicMock()

    # Test deploy with dryrun
    change_to_project_dir("./")
    test_args = ["dj", "deploy", "./project9", "--dryrun"]
    with patch.object(sys, "argv", test_args):
        main(builder_client=builder_client)

    func_names = [mock_call[0] for mock_call in builder_client.mock_calls]
    assert "basic_login" in func_names
    assert "create_namespace" in func_names
    assert "create_source" in func_names
    assert "create_dimension" in func_names
    assert "delete_namespace" in func_names

    # Test deploy without dryrun
    change_to_project_dir("./")
    test_args = ["dj", "deploy", "./project9"]
    with patch.object(sys, "argv", test_args):
        main(builder_client=builder_client)

    func_names = [mock_call[0] for mock_call in builder_client.mock_calls]
    assert "basic_login" in func_names
    assert "create_namespace" in func_names
    assert "create_source" in func_names
    assert "create_dimension" in func_names
    assert "delete_namespace" in func_names


def test_pull(
    tmp_path,
    builder_client: DJBuilder,  # pylint: disable=redefined-outer-name
):
    """
    Test `dj pull <namespace> <dir>`
    """
    test_args = ["dj", "pull", "default", tmp_path.absolute().as_posix()]
    with patch.object(sys, "argv", test_args):
        main(builder_client=builder_client)
    assert len(os.listdir(tmp_path)) == 30


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
