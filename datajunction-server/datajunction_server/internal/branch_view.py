"""
Default branch nodes shown as if they had been copied into a branch.
"""

from typing import TypeVar

from datajunction_server.internal.namespaces import (
    inject_prefixes,
    inject_prefixes_outside_strings,
)
from datajunction_server.models.deployment import render_prefixes
from datajunction_server.models.filter_requirements import FilterRequirement
from datajunction_server.models.node import NodeOutput, NodeRevisionOutput
from datajunction_server.models.node_type import NodeType
from datajunction_server.utils import SEPARATOR

Output = TypeVar("Output", NodeOutput, NodeRevisionOutput)


def branch_view(output: Output, source: str, target: str) -> Output:
    """
    Rewrites a node's names from the `source` namespace to the `target` one.

    Applies the rules a branch copy applies, so the result reads like the copy.
    The given output is not changed.
    """

    def swap(text: str) -> str:
        return render_prefixes(inject_prefixes(text, source), target)

    def swap_outside_strings(text: str) -> str:
        return render_prefixes(inject_prefixes_outside_strings(text, source), target)

    def swap_requirements(
        requirements: list[FilterRequirement] | None,
    ) -> list[FilterRequirement] | None:
        if requirements is None:
            return None
        return [
            FilterRequirement(
                dimension=tuple(swap(dim) for dim in entry.dimension),
                default=swap_outside_strings(entry.default) if entry.default else None,
            )
            for entry in requirements
        ]

    view = output.model_copy(deep=True)
    view.name = swap(view.name)
    if isinstance(view, NodeOutput) and (
        view.namespace == source or view.namespace.startswith(source + SEPARATOR)
    ):
        view.namespace = target + view.namespace[len(source) :]
    for parent in view.parents:
        parent.name = swap(parent.name)
    for column in view.columns:
        if column.dimension:
            column.dimension.name = swap(column.dimension.name)
        if view.type == NodeType.CUBE:
            column.name = swap(column.name)

    if view.type in (NodeType.TRANSFORM, NodeType.DIMENSION, NodeType.METRIC):
        if view.query:
            view.query = swap(view.query)
    if view.type in (NodeType.SOURCE, NodeType.TRANSFORM, NodeType.DIMENSION):
        for link in view.dimension_links or []:
            link.dimension.name = swap(link.dimension.name)
            link.join_sql = swap(link.join_sql)
            link.foreign_keys = {
                swap(column): swap(target_column) if target_column else None
                for column, target_column in link.foreign_keys.items()
            }
    if view.type == NodeType.METRIC:
        if view.fixed_grain:
            view.fixed_grain = [swap(dim) for dim in view.fixed_grain]
        view.required_filters = swap_requirements(view.required_filters)
        view.recommended_filters = swap_requirements(view.recommended_filters)
        if view.reaggregate:
            for rule in view.reaggregate.rules:
                rule.dimension = swap(rule.dimension)
    if isinstance(view, NodeOutput) and view.cube_filters:
        view.cube_filters = [swap(cube_filter) for cube_filter in view.cube_filters]
    return view
