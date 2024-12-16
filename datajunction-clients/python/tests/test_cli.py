"""Tests DJ CLI"""
import os
import sys
from io import StringIO
from typing import Callable
from unittest.mock import patch

import pytest

from datajunction import DJBuilder
from datajunction.cli import main


def test_deploy(
    change_to_project_dir: Callable,
    builder_client: DJBuilder,
):
    """
    Test `dj deploy <dir>`
    """
    # Test deploy with dryrun
    change_to_project_dir("./")
    test_args = ["dj", "deploy", "./project9", "--dryrun"]
    with patch.object(sys, "argv", test_args):
        main(builder_client=builder_client)

    # Test deploy without dryrun
    change_to_project_dir("./")
    test_args = ["dj", "deploy", "./project9"]
    with patch.object(sys, "argv", test_args):
        main(builder_client=builder_client)

    assert len(builder_client.list_nodes(namespace="projects.project7")) == 6


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
