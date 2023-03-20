"""
DAG related functions.
"""
import collections
from typing import List

from dj.models.node import Node, NodeRevision
from dj.utils import get_settings

settings = get_settings()


def get_dimensions(node: Node) -> List[str]:
    """
    Return the available dimensions in a given node.
    """
    dimensions = []
    to_process = collections.deque([node.current.parents])
    while to_process:
        current_batch = to_process.popleft()
        for parent in current_batch:
            for column in parent.current.columns:
                dimensions.append(f"{parent.name}.{column.name}")

                if column.dimension:
                    to_process.append(column.dimension.current.parents)
                    for dimension_column in column.dimension.current.columns:
                        dimensions.append(
                            f"{column.dimension.name}.{dimension_column.name}",
                        )

    return sorted(dimensions)
