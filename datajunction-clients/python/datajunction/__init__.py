"""
A DataJunction client for connecting to a DataJunction server
"""

from importlib.metadata import PackageNotFoundError, version  # pragma: no cover

from datajunction.admin import DJAdmin
from datajunction.builder import DJBuilder
from datajunction.client import DJClient
from datajunction.compile import Project
from datajunction.models import (
    AvailabilityState,
    ColumnAttribute,
    Engine,
    Materialization,
    MaterializationJobType,
    MaterializationStrategy,
    MetricDirection,
    MetricMetadata,
    MetricUnit,
    NodeMode,
)
from datajunction.nodes import (
    Cube,
    Dimension,
    Metric,
    Namespace,
    Node,
    Source,
    Transform,
)
from datajunction.tags import Tag

try:
    # Change here if project is renamed and does not equal the package name
    DIST_NAME = __name__
    __version__ = version(DIST_NAME)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"
finally:
    del version, PackageNotFoundError


__all__ = [
    "DJClient",
    "DJBuilder",
    "DJAdmin",
    "AvailabilityState",
    "ColumnAttribute",
    "Source",
    "Dimension",
    "Transform",
    "Materialization",
    "MaterializationJobType",
    "MaterializationStrategy",
    "Metric",
    "MetricDirection",
    "MetricMetadata",
    "MetricUnit",
    "Cube",
    "Node",
    "NodeMode",
    "Namespace",
    "Engine",
    "Project",
    "Tag",
]
