"""
DAG related functions.
"""

from typing import List

from dj.models.node import Node
from dj.utils import get_settings

settings = get_settings()


def get_dimensions(node: Node) -> List[str]:
    """
    Return the available dimensions in a given node.
    """
    dimensions = []
    for parent in node.current.parents:
        for column in parent.current.columns:
            dimensions.append(f"{parent.name}.{column.name}")

            if column.dimension:
                for dimension_column in column.dimension.current.columns:
                    dimensions.append(
                        f"{column.dimension.name}.{dimension_column.name}",
                    )

    return sorted(dimensions)
