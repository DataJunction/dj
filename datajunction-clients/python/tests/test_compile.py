"""
Tests for the deprecated client-side compilation module.

Client-side YAML compilation has moved server-side (see DeploymentService).
The ``Project`` symbol is kept as a deprecation shim that errors when used.
"""

import pytest

from datajunction import Project
from datajunction.exceptions import DJClientException


def test_project_instantiation_raises():
    """Instantiating the deprecated Project raises a clear error."""
    with pytest.raises(DJClientException, match="handled server-side"):
        Project()


def test_project_load_raises():
    """Project.load is deprecated and raises."""
    with pytest.raises(DJClientException, match="handled server-side"):
        Project.load("nodes")


def test_project_load_current_raises():
    """Project.load_current is deprecated and raises."""
    with pytest.raises(DJClientException, match="handled server-side"):
        Project.load_current()
