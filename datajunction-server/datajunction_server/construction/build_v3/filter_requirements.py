"""Enforce the filter requirements declared on metrics."""

from dataclasses import dataclass, field

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.construction.build_v3.filters import parse_filter
from datajunction_server.construction.build_v3.loaders import find_upstream_node_names
from datajunction_server.construction.build_v3.utils import (
    extract_filter_dimension_refs,
)
from datajunction_server.construction.utils import missing_to_parent
from datajunction_server.database.node import Node, NodeRelationship, NodeRevision
from datajunction_server.errors import (
    DJError,
    DJInvalidInputException,
    DJWarning,
    ErrorCode,
)
from datajunction_server.models.filter_requirements import (
    EffectiveFilterRequirement,
    parse_filter_requirements,
)
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


async def load_declared_entries(
    session: AsyncSession,
    names: set[str],
    inherit: bool = False,
) -> dict[str, list[RequirementEntry]]:
    """
    Entries each metric among the named nodes declares itself.
    """
    rows = await _declared_rows(session, sorted(names))
    parents = await missing_to_parent(session, names, rows) if inherit else {}
    if parents:
        parent_rows = await _declared_rows(session, sorted(set(parents.values())))
        for name, parent in parents.items():
            if parent in parent_rows:
                rows[name] = parent_rows[parent]
    return {
        name: _revision_entries(name, required, recommended)
        for name, (required, recommended) in sorted(rows.items())
    }


async def _declared_rows(
    session: AsyncSession,
    names: list[str],
) -> dict[str, tuple[list[dict] | None, list[dict] | None]]:
    """
    The (required, recommended) filters of each active metric named.
    """
    rows = await session.execute(
        select(
            Node.name,
            NodeRevision.required_filters,
            NodeRevision.recommended_filters,
        )
        .join(
            NodeRevision,
            and_(
                NodeRevision.node_id == Node.id,
                NodeRevision.version == Node.current_version,
            ),
        )
        .where(Node.name.in_(names))
        .where(Node.type == NodeType.METRIC)
        .where(Node.deactivated_at.is_(None))
        .order_by(Node.name),
    )
    return {name: (required, recommended) for name, required, recommended in rows}


def _revision_entries(
    name: str,
    required_filters: list[dict] | None,
    recommended_filters: list[dict] | None,
) -> list[RequirementEntry]:
    return [
        RequirementEntry(
            attributes=item.dimension,
            default=item.default,
            required=required,
            metrics=frozenset({name}),
        )
        for required, entries in (
            (True, required_filters),
            (False, recommended_filters),
        )
        for item in parse_filter_requirements(entries) or []
    ]


async def load_requirement_entries(
    session: AsyncSession,
    metrics: list[str],
    inherit: bool = False,
) -> list[RequirementEntry]:
    """
    Entries declared on the requested metrics and on every metric upstream.
    """
    if not metrics:
        return []
    names, _ = await find_upstream_node_names(session, metrics, inherit)
    declared = await load_declared_entries(session, names, inherit)
    return [entry for entries in declared.values() for entry in entries]


def split_effective_entries(
    entries: list[RequirementEntry],
) -> tuple[list[EffectiveFilterRequirement], list[EffectiveFilterRequirement]]:
    """
    Merge the entries into (required, recommended) lists without checking
    them, so conflicting defaults are all kept.
    """
    required: list[EffectiveFilterRequirement] = []
    recommended: list[EffectiveFilterRequirement] = []
    for item in _merge(entries):
        (required if item.required else recommended).append(
            EffectiveFilterRequirement(
                dimension=item.attributes,
                default=item.default,
                declared_by=tuple(sorted(item.metrics)),
            ),
        )
    return required, recommended


async def load_revision_filters(
    session: AsyncSession,
    revision_ids: list[int],
) -> dict[
    int,
    tuple[list[EffectiveFilterRequirement], list[EffectiveFilterRequirement]],
]:
    """
    The (required, recommended) filters each metric revision has, declared
    or inherited from the metrics upstream of its parents.
    """
    if not revision_ids:
        return {}
    revisions = (
        await session.execute(
            select(
                NodeRevision.id,
                NodeRevision.name,
                NodeRevision.required_filters,
                NodeRevision.recommended_filters,
            ).where(NodeRevision.id.in_(revision_ids)),
        )
    ).all()
    parents: dict[int, list[str]] = {row.id: [] for row in revisions}
    for child_id, parent_name in await session.execute(
        select(NodeRelationship.child_id, Node.name)
        .join(Node, Node.id == NodeRelationship.parent_id)
        .where(NodeRelationship.child_id.in_(revision_ids))
        .order_by(NodeRelationship.child_id, Node.name),
    ):
        parents[child_id].append(parent_name)
    parent_names = sorted({name for names in parents.values() for name in names})
    parent_map: dict[str, list[str]] = {}
    declared: dict[str, list[RequirementEntry]] = {}
    if parent_names:
        names, parent_map = await find_upstream_node_names(session, parent_names)
        declared = await load_declared_entries(session, names)
    result: dict[int, tuple[list, list]] = {}
    for rev in revisions:
        seen: set[str] = set()
        pending = list(parents[rev.id])
        while pending:
            name = pending.pop()
            if name not in seen:
                seen.add(name)
                pending.extend(parent_map.get(name, []))
        entries = _revision_entries(
            rev.name,
            rev.required_filters,
            rev.recommended_filters,
        )
        entries += [e for name in sorted(seen) for e in declared.get(name, [])]
        result[rev.id] = split_effective_entries(entries)
    return result


async def apply_metric_filter_requirements(
    session: AsyncSession,
    metrics: list[str],
    filters: list[str] | None,
    inherit: bool = False,
) -> tuple[list[str], list[DJWarning]]:
    """
    Check the filters against the metrics' requirements.

    Returns the filters with defaults added, and warnings. Raises a 422 when
    a required filter is missing.
    """
    entries = await load_requirement_entries(session, metrics, inherit)
    return apply_requirements(entries, list(filters or []))


@dataclass(frozen=True)
class CheckedFilters:
    """
    Filters that went through the metrics' filter requirements, with the
    warnings that check gave.

    build_metrics_sql checks plain filters itself. Pass this to hand it filters
    that were already checked, for example to read them before the build.
    """

    filters: list[str]
    warnings: list[DJWarning] = field(default_factory=list)


async def check_filter_requirements(
    session: AsyncSession,
    metrics: list[str],
    filters: list[str] | None,
    inherit: bool = False,
) -> CheckedFilters:
    """
    apply_metric_filter_requirements, returned as CheckedFilters.
    """
    effective, warnings = await apply_metric_filter_requirements(
        session,
        metrics,
        filters,
        inherit,
    )
    return CheckedFilters(effective, warnings)


REQUIREMENT_WARNING_CODES = frozenset(
    {ErrorCode.DEFAULT_FILTER_APPLIED, ErrorCode.RECOMMENDED_FILTER_MISSING},
)


async def apply_node_filter_requirements(
    session: AsyncSession,
    node_name: str,
    filters: list[str],
    inherit: bool = False,
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
    if node_type is None and inherit:
        parent = (await missing_to_parent(session, [node_name], [])).get(node_name)
        if parent:
            node_type = (
                await session.execute(
                    select(Node.type)
                    .where(Node.name == parent)
                    .where(Node.deactivated_at.is_(None)),
                )
            ).scalar_one_or_none()
    if node_type == NodeType.METRIC:
        return await apply_metric_filter_requirements(
            session,
            [node_name],
            filters,
            inherit,
        )
    if node_type == NodeType.CUBE:
        cube = await Node.get_cube_by_name(
            session,
            node_name,
            inherit=inherit,
        )
        revision = cube.current  # type: ignore[union-attr]
        stored = list(revision.cube_filters or [])
        effective, warnings = await apply_metric_filter_requirements(
            session,
            list(revision.cube_node_metrics),
            stored + list(filters),
            inherit,
        )
        return effective[len(stored) :], warnings
    return list(filters), []
