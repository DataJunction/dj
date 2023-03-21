"""
DAG related functions.
"""
import collections
from typing import List

from dj.models.node import Node
from dj.utils import get_settings

settings = get_settings()


def get_dimensions(node: Node) -> List[str]:
    """
    Return all available dimensions for a given node.
    """
    dimensions = []
    to_process = collections.deque([node, *node.current.parents])
    processed = set()

    while to_process:
        current_node = to_process.popleft()
        for column in current_node.current.columns:
            if any(
                attr.attribute_type.name == "dimension" for attr in column.attributes
            ):
                dimensions.append(f"{current_node.name}.{column.name}")
            if column.dimension and column.dimension not in processed:
                processed.add(column.dimension)
                to_process.extend(column.dimension.current.parents)
                for dimension_column in column.dimension.current.columns:
                    dimensions.append(
                        f"{column.dimension.name}.{dimension_column.name}",
                    )
    return sorted(dimensions)
