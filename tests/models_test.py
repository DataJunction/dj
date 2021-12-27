"""
Tests for ``datajuntion.models``.
"""

from pathlib import Path

import pytest

from datajunction.models import get_name_from_path


def test_get_name_from_path() -> None:
    """
    Test ``get_name_from_path``.
    """
    with pytest.raises(Exception) as excinfo:
        get_name_from_path(Path("/path/to/repository"), Path("/path/to/repository"))
    assert str(excinfo.value) == "Invalid path: /path/to/repository"

    with pytest.raises(Exception) as excinfo:
        get_name_from_path(
            Path("/path/to/repository"), Path("/path/to/repository/nodes")
        )
    assert str(excinfo.value) == "Invalid path: /path/to/repository/nodes"

    with pytest.raises(Exception) as excinfo:
        get_name_from_path(
            Path("/path/to/repository"), Path("/path/to/repository/invalid/test.yaml")
        )
    assert str(excinfo.value) == "Invalid path: /path/to/repository/invalid/test.yaml"

    assert (
        get_name_from_path(
            Path("/path/to/repository"), Path("/path/to/repository/nodes/test.yaml")
        )
        == "test"
    )

    assert (
        get_name_from_path(
            Path("/path/to/repository"),
            Path("/path/to/repository/nodes/core/test.yaml"),
        )
        == "core.test"
    )

    assert (
        get_name_from_path(
            Path("/path/to/repository"),
            Path("/path/to/repository/nodes/dev.nodes/test.yaml"),
        )
        == "dev%2Enodes.test"
    )

    assert (
        get_name_from_path(
            Path("/path/to/repository"),
            Path("/path/to/repository/nodes/5%_nodes/test.yaml"),
        )
        == "5%25_nodes.test"
    )
