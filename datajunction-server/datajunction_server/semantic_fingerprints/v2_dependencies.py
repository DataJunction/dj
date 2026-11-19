"""Semantic contract helpers for the v2 fingerprint graph."""

from __future__ import annotations

from datajunction_server.models.deployment import (
    ColumnSpec,
    CubeSpec,
    DimensionJoinLinkSpec,
    DimensionReferenceLinkSpec,
    LinkableNodeSpec,
    MetricSpec,
    NodeSpec,
    SourceSpec,
)
from datajunction_server.models.filter_requirements import FilterRequirement
from datajunction_server.semantic_fingerprints.normalization import (
    canonical_json,
    normalize_value,
)
from datajunction_server.semantic_fingerprints.v2 import _parsed_type


def _column_contract(column: ColumnSpec) -> dict[str, object]:
    parsed_type = _parsed_type(column.type)
    return {
        "name": column.name,
        "type": str(parsed_type) if parsed_type else column.type,
        "partition": normalize_value(column.partition),
    }


def _columns_contract(columns: tuple[ColumnSpec, ...]) -> list[dict[str, object]]:
    return [
        _column_contract(column)
        for column in sorted(columns, key=lambda column: column.name)
    ]


def _output_column_contract(
    name: str,
    column: ColumnSpec | None,
) -> dict[str, object]:
    return (
        _column_contract(column)
        if column
        else {
            "name": name,
            "type": None,
            "partition": None,
        }
    )


def _effective_filter_requirements(
    spec: MetricSpec,
) -> list[FilterRequirement]:
    requirements = [
        *(spec.rendered_required_filters or []),
        *(spec.rendered_recommended_filters or []),
    ]
    unique = {}
    for requirement in requirements:
        if not requirement.default:
            continue
        dimensions = tuple(sorted(set(requirement.dimension)))
        key = canonical_json([dimensions, requirement.default])
        unique[key] = FilterRequirement(
            dimension=dimensions,
            default=requirement.default,
        )
    return [unique[key] for key in sorted(unique)]


def _dimension_link_contract(link: object) -> dict[str, object]:
    if isinstance(link, DimensionJoinLinkSpec):
        return {
            "type": link.type.value,
            "role": link.role,
            "dimension_node": link.rendered_dimension_node,
            "join_type": link.join_type.value,
            "join_on": link.rendered_join_on,
            "node_column": link.node_column,
            "default_value": (
                {
                    "type": type(link.default_value).__name__,
                    "value": link.default_value,
                }
                if link.default_value is not None
                else None
            ),
        }
    if isinstance(link, DimensionReferenceLinkSpec):
        return {
            "type": link.type.value,
            "role": link.role,
            "dimension_node": link.rendered_dimension_node,
            "dimension_attribute": link.dimension_attribute,
            "node_column": link.node_column,
        }
    raise TypeError(f"Unsupported dimension link: {type(link).__name__}")


def _node_contract(
    spec: NodeSpec,
    columns: tuple[ColumnSpec, ...] | None = None,
    *,
    include_dimension_links: bool = True,
) -> dict[str, object]:

    contract: dict[str, object] = {"node_type": spec.node_type.value}
    if columns is not None:
        contract["columns"] = _columns_contract(columns)
    if isinstance(spec, SourceSpec):
        contract.update(
            catalog=spec.catalog,
            schema=spec.schema_,
            table=spec.table,
            primary_key=sorted(spec.primary_key or []),
        )
    elif isinstance(spec, LinkableNodeSpec):
        contract["primary_key"] = sorted(spec.primary_key or [])
    if include_dimension_links and isinstance(spec, LinkableNodeSpec):
        contract["dimension_links"] = sorted(
            (_dimension_link_contract(link) for link in spec.dimension_links),
            key=canonical_json,
        )
    if isinstance(spec, MetricSpec):
        contract.update(
            required_dimensions=sorted(spec.rendered_required_dimensions),
            fixed_grain=(
                sorted(spec.rendered_fixed_grain)
                if spec.rendered_fixed_grain is not None
                else None
            ),
            reaggregate=normalize_value(spec.rendered_reaggregate),
            filter_defaults=normalize_value(_effective_filter_requirements(spec)),
        )
    if isinstance(spec, CubeSpec):
        contract.update(
            metrics=sorted(spec.rendered_metrics),
            dimensions=sorted(spec.rendered_dimensions),
            filters=sorted(spec.rendered_filters),
        )
    return contract
