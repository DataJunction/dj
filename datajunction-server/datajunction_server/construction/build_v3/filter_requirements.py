"""Enforce the filter requirements declared on metrics."""

from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, load_only, noload

from datajunction_server.construction.build_v3.filters import parse_filter
from datajunction_server.construction.build_v3.loaders import find_upstream_node_names
from datajunction_server.construction.build_v3.utils import (
    extract_filter_dimension_refs,
)
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.errors import (
    DJError,
    DJInvalidInputException,
    DJWarning,
    ErrorCode,
)
from datajunction_server.models.filter_requirements import parse_filter_requirements
from datajunction_server.models.node_type import NodeType


@dataclass(frozen=True)
class RequirementEntry:
    """
    One requirement, with the metrics that declared it.
    """

    attributes: tuple[str, ...]
    default: str | None
    required: bool
    metrics: frozenset[str]


def ref_covers_attribute(attribute: str, ref: str) -> bool:
    """
    Whether a filter ref covers an entry attribute. No role matches any role.
    """
    if "[" in attribute:
        return attribute == ref
    return ref.split("[", 1)[0] == attribute


def _merge(entries: list[RequirementEntry]) -> list[RequirementEntry]:
    merged: dict[tuple[frozenset[str], str | None], RequirementEntry] = {}
    for item in entries:
        default = str(parse_filter(item.default)) if item.default else None
        key = (frozenset(item.attributes), default)
        current = merged.get(key)
        if current is None:
            merged[key] = RequirementEntry(
                item.attributes,
                item.default,
                item.required,
                item.metrics,
            )
        else:
            merged[key] = RequirementEntry(
                current.attributes,
                current.default,
                current.required or item.required,
                current.metrics | item.metrics,
            )
    return list(merged.values())


def _names(entry: RequirementEntry) -> str:
    return ", ".join(sorted(entry.metrics))


def apply_requirements(
    entries: list[RequirementEntry],
    filters: list[str],
) -> tuple[list[str], list[DJWarning]]:
    """
    Check the entries against the caller's filters.

    Returns the filters with any needed defaults added, plus warnings.
    Raises when a required filter is missing or two defaults conflict.
    """
    if not entries:
        return list(filters), []

    refs = extract_filter_dimension_refs(filters)
    errors: list[DJError] = []
    warnings: list[DJWarning] = []
    to_apply: list[RequirementEntry] = []

    for item in _merge(entries):
        if any(
            ref_covers_attribute(attr, ref) for attr in item.attributes for ref in refs
        ):
            continue
        attributes = ", ".join(item.attributes)
        if item.default:
            to_apply.append(item)
        elif item.required:
            errors.append(
                DJError(
                    code=ErrorCode.REQUIRED_FILTER_MISSING,
                    message=(
                        f"Missing required filter: one of {attributes} "
                        f"(required by metric {_names(item)})"
                    ),
                ),
            )
        else:
            warnings.append(
                DJWarning(
                    code=ErrorCode.RECOMMENDED_FILTER_MISSING,
                    message=(
                        f"Missing recommended filter: one of {attributes} "
                        f"(recommended by metric {_names(item)})"
                    ),
                ),
            )

    defaults: dict[str, RequirementEntry] = {}
    for item in to_apply:
        assert item.default is not None
        text = str(parse_filter(item.default))
        seen = defaults.get(text)
        defaults[text] = (
            item
            if seen is None
            else RequirementEntry(
                seen.attributes,
                seen.default,
                seen.required or item.required,
                seen.metrics | item.metrics,
            )
        )

    by_attribute: dict[str, str] = {}
    for text, item in defaults.items():
        assert item.default is not None
        for ref in extract_filter_dimension_refs([item.default]):
            other = by_attribute.get(ref)
            if other is not None and other != text:
                errors.append(
                    DJError(
                        code=ErrorCode.CONFLICTING_DEFAULT_FILTERS,
                        message=(
                            f"Conflicting default filters on {ref}: "
                            f"{other} (metric {_names(defaults[other])}) and "
                            f"{text} (metric {_names(item)})"
                        ),
                    ),
                )
            by_attribute.setdefault(ref, text)

    if errors:
        raise DJInvalidInputException(errors=errors)

    effective = list(filters)
    for text, item in defaults.items():
        effective.append(text)
        warnings.append(
            DJWarning(
                code=ErrorCode.DEFAULT_FILTER_APPLIED,
                message=(f"Applied default filter {text} (from metric {_names(item)})"),
            ),
        )
    return effective, warnings


async def load_requirement_entries(
    session: AsyncSession,
    metrics: list[str],
) -> list[RequirementEntry]:
    """
    Entries declared on the requested metrics and on every metric upstream.
    """
    if not metrics:
        return []
    names, _ = await find_upstream_node_names(session, metrics)
    nodes = await Node.get_by_names(
        session,
        sorted(names),
        options=[
            load_only(Node.name, Node.type, Node.current_version),
            noload(Node.created_by),
            noload(Node.tags),
            joinedload(Node.current).options(
                noload(NodeRevision.created_by),
                load_only(
                    NodeRevision.name,
                    NodeRevision.required_filters,
                    NodeRevision.recommended_filters,
                ),
            ),
        ],
    )
    entries: list[RequirementEntry] = []
    for node in nodes:
        if node.type != NodeType.METRIC or node.current is None:
            continue
        for required, declared in (
            (True, node.current.required_filters),
            (False, node.current.recommended_filters),
        ):
            for item in parse_filter_requirements(declared) or []:
                entries.append(
                    RequirementEntry(
                        attributes=item.dimension,
                        default=item.default,
                        required=required,
                        metrics=frozenset({node.name}),
                    ),
                )
    return entries


async def apply_metric_filter_requirements(
    session: AsyncSession,
    metrics: list[str],
    filters: list[str] | None,
) -> tuple[list[str], list[DJWarning]]:
    """
    Check the filters against the metrics' requirements.

    Returns the filters with defaults added, and warnings. Raises a 422 when
    a required filter is missing.
    """
    entries = await load_requirement_entries(session, metrics)
    return apply_requirements(entries, list(filters or []))


REQUIREMENT_WARNING_CODES = frozenset(
    {ErrorCode.DEFAULT_FILTER_APPLIED, ErrorCode.RECOMMENDED_FILTER_MISSING},
)


async def apply_node_filter_requirements(
    session: AsyncSession,
    node_name: str,
    filters: list[str],
) -> tuple[list[str], list[DJWarning]]:
    """
    Check the requirements of a metric or cube node.

    Returns the caller's filters with defaults added, and warnings. A cube's
    stored filters count toward its entries but are not returned. Other node
    types return the filters unchanged.
    """
    node_type = (
        await session.execute(
            select(Node.type)
            .where(Node.name == node_name)
            .where(Node.deactivated_at.is_(None)),
        )
    ).scalar_one_or_none()
    if node_type == NodeType.METRIC:
        return await apply_metric_filter_requirements(session, [node_name], filters)
    if node_type == NodeType.CUBE:
        cube = await Node.get_cube_by_name(session, node_name)
        revision = cube.current  # type: ignore[union-attr]
        stored = list(revision.cube_filters or [])
        effective, warnings = await apply_metric_filter_requirements(
            session,
            list(revision.cube_node_metrics),
            stored + list(filters),
        )
        return effective[len(stored) :], warnings
    return list(filters), []
