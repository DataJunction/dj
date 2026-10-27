"""Group non-ranked dimension tuples before final metric aggregation."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import cast

from datajunction_server.construction.build_v3.decomposition import (
    _reaggregate_dimension_requested,
)
from datajunction_server.construction.build_v3.ranked_groups import (
    ranked_tuple_join,
)
from datajunction_server.construction.build_v3.types import (
    ColumnMetadata,
    GeneratedMeasuresSQL,
    GeneratedSQL,
    GrainGroupSQL,
)
from datajunction_server.construction.build_v3.utils import (
    as_subquery,
    extract_filter_dimension_refs,
    make_column_ref,
)
from datajunction_server.errors import (
    DJInvalidInputException,
    DJInvalidMetricQueryException,
)
from datajunction_server.models.decompose import Aggregability
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.types import StringType

_RANKED_ALIAS = "__dj_ranked_groups"
# A real ranked value named "Others" is indistinguishable from this bucket in
# the current API. A request-level label override needs a spec change.
_OTHERS_LABEL = "Others"


@dataclass(frozen=True)
class GroupOthers:
    """The ranked tuples and dimensions to bucket in a metrics build."""

    dimensions: tuple[str, ...]
    ranked: GeneratedSQL


def _projection_name(expr: ast.Aliasable | ast.Expression) -> str | None:
    alias = getattr(expr, "alias", None)
    if alias is not None:
        return alias.name
    if isinstance(expr, ast.Column):
        return expr.name.name
    return None


def _unaliased(expr: ast.Aliasable | ast.Expression) -> ast.Expression:
    raw = deepcopy(
        expr.child if isinstance(expr, ast.Alias) else cast(ast.Expression, expr),
    )
    if isinstance(raw, ast.Aliasable):
        raw.alias = None
        raw.as_ = None
    return cast(ast.Expression, raw)


def _ranked_query(
    ranked: GeneratedSQL,
    dimensions: tuple[str, ...],
    occupied_names: set[str],
) -> tuple[ast.Query, dict[str, str], str]:
    """Expose only ranked keys under names absent from the source query."""
    ranked_columns = {col.semantic_name: col.name for col in ranked.columns}
    if any(dimension not in ranked_columns for dimension in dimensions):
        raise DJInvalidInputException("GROUP_OTHERS cannot resolve ranking dimensions.")

    occupied = {name.lower() for name in occupied_names}

    def unused_name(base: str) -> str:
        candidate = base
        suffix = 1
        while candidate.lower() in occupied:
            candidate = f"{base}_{suffix}"
            suffix += 1
        occupied.add(candidate.lower())
        return candidate

    key_names = {
        dimension: unused_name(f"__dj_ranked_key_{index}")
        for index, dimension in enumerate(dimensions)
    }
    member_name = unused_name("__dj_group_member")
    source_alias = "__dj_ranked_source"
    source = as_subquery(ranked.query, source_alias)
    projection: list[ast.Aliasable | ast.Expression] = [
        ast.Alias(
            child=make_column_ref(ranked_columns[dimension], source_alias),
            alias=ast.Name(key_names[dimension]),
        ).set_as(True)
        for dimension in dimensions
    ]
    projection.append(
        ast.Alias(child=ast.Number(1), alias=ast.Name(member_name)).set_as(True),
    )
    query = ast.Query(
        select=ast.Select(
            projection=projection,
            from_=ast.From(relations=[ast.Relation(primary=source)]),
        ),
    )
    query.parenthesized = True
    query.alias = ast.Name(_RANKED_ALIAS)
    query.as_ = True
    return query, key_names, member_name


def _bucket_expression(original: ast.Expression, member_name: str) -> ast.Case:
    return ast.Case(
        conditions=[
            ast.IsNull(expr=make_column_ref(member_name, _RANKED_ALIAS), negated=True),
        ],
        results=[ast.Cast(data_type=StringType(), expression=deepcopy(original))],
        else_result=ast.String(f"'{_OTHERS_LABEL}'"),
    )


def _preserve_raw_group_input(
    group: GrainGroupSQL,
    select: ast.Select,
    original: ast.Expression,
    dimension: ColumnMetadata,
    index: int,
    occupied_names: set[str],
) -> str:
    """Keep an unbucketed metric input alongside the displayed dimension."""
    raw_alias = f"__dj_raw_group_key_{index}"
    suffix = 1
    while raw_alias.lower() in occupied_names:
        raw_alias = f"__dj_raw_group_key_{index}_{suffix}"
        suffix += 1
    occupied_names.add(raw_alias.lower())
    select.projection.append(
        ast.Alias(child=deepcopy(original), alias=ast.Name(raw_alias)).set_as(True),
    )
    group.columns.append(
        ColumnMetadata(
            name=raw_alias,
            semantic_name=f"{dimension.semantic_name}:raw",
            type=dimension.type,
            semantic_type="metric_input",
        ),
    )
    group.grain.append(raw_alias)
    if select.group_by:
        select.group_by.append(deepcopy(original))
    return raw_alias


def _non_decomposable_input_names(
    group: GrainGroupSQL,
    measures: GeneratedMeasuresSQL,
) -> set[str]:
    """Find raw columns read by non-decomposable metrics in this grain group."""
    names: set[str] = set()
    for metric_name in group.metrics:
        decomposed = measures.decomposed_metrics.get(metric_name)
        if decomposed is None or decomposed.components:
            continue
        names.update(
            col.name.name
            for col in decomposed.combiner_ast.find_all(ast.Column)
            if col.name and not col._table
        )
    return names


def _bucket_grain_group(
    group: GrainGroupSQL,
    ranked: GeneratedSQL,
    dimensions: tuple[str, ...],
    raw_metric_input_names: set[str],
) -> None:
    select = group.query.select
    if (
        not isinstance(select, ast.Select)
        or not select.from_
        or not select.from_.relations
    ):
        raise DJInvalidInputException("GROUP_OTHERS requires a grouped metrics query.")

    dimension_columns = {
        col.semantic_name: col
        for col in group.columns
        if col.semantic_type == "dimension"
    }
    selected: list[tuple[int, ast.Expression, str]] = []

    for dimension in dimensions:
        if dimension not in dimension_columns:
            raise DJInvalidInputException(
                f"GROUP_OTHERS cannot resolve dimension `{dimension}` in a metric grain.",
            )
        alias = dimension_columns[dimension].name
        selected_projection = next(
            (
                (index, projection)
                for index, projection in enumerate(select.projection)
                if _projection_name(projection) == alias
            ),
            None,
        )
        if selected_projection is None:
            raise DJInvalidInputException(
                f"GROUP_OTHERS cannot find the output for `{dimension}`.",
            )
        index, projection = selected_projection
        original = _unaliased(projection)
        selected.append((index, original, dimension))

    occupied_names = {
        col.name.name for col in select.find_all(ast.Column) if not col._table
    }
    ranked_query, ranked_keys, member_name = _ranked_query(
        ranked,
        dimensions,
        occupied_names,
    )
    pairs = [
        (
            deepcopy(original),
            make_column_ref(ranked_keys[dimension], _RANKED_ALIAS),
        )
        for _, original, dimension in selected
    ]

    select.from_.relations[0].extensions.append(
        ranked_tuple_join(ranked_query, pairs, "LEFT OUTER"),
    )

    # A selected dimension can also be a raw input to COUNT DISTINCT or a
    # non-decomposable metric. Preserve that input before displaying its bucket.
    raw_aliases_in_use = {name.lower() for name in occupied_names}
    raw_aliases_in_use.update(col.name.lower() for col in group.columns)
    raw_aliases_in_use.update(name.lower() for name in ranked_keys.values())
    raw_aliases_in_use.add(member_name.lower())

    for index, original, dimension in selected:
        alias = dimension_columns[dimension].name
        raw_components = [
            component
            for component in group.components
            if component.rule.type == Aggregability.LIMITED
            and not component.aggregation
            and group.component_aliases.get(component.name) == alias
        ]
        if raw_components or alias in raw_metric_input_names:
            raw_alias = _preserve_raw_group_input(
                group,
                select,
                original,
                dimension_columns[dimension],
                index,
                raw_aliases_in_use,
            )
            for component in raw_components:
                group.component_aliases[component.name] = raw_alias
            if alias in raw_metric_input_names:
                group.raw_metric_input_aliases[alias] = raw_alias

        bucketed = ast.Alias(
            child=_bucket_expression(original, member_name),
            alias=ast.Name(alias),
        )
        bucketed.set_as(True)
        select.projection[index] = bucketed
        # A non-decomposable raw input can share the displayed dimension's
        # metadata name. Keep every record for that output alias in sync.
        for column in group.columns:
            if column.semantic_type == "dimension" and column.name == alias:
                column.type = "string"

        if select.group_by:
            position = group.dimension_group_by_positions.get(dimension)
            if position is None or position >= len(select.group_by):
                raise DJInvalidInputException(
                    f"GROUP_OTHERS cannot group dimension `{dimension}` at its source grain.",
                )
            select.group_by[position] = _bucket_expression(
                original,
                member_name,
            )


def apply_group_others(
    measures: GeneratedMeasuresSQL,
    group_others: GroupOthers,
) -> list[str]:
    """Bucket grain dimensions and return filters for the final output stage."""
    for metric_name, decomposed in measures.decomposed_metrics.items():
        for component in decomposed.components:
            reaggregate = component.rule.reaggregate
            if reaggregate and _reaggregate_dimension_requested(
                reaggregate.dimension,
                list(group_others.dimensions),
            ):
                raise DJInvalidMetricQueryException(
                    "GROUP_OTHERS cannot bucket protected dimension "
                    f"`{reaggregate.dimension}` for semi-additive metric "
                    f"`{metric_name}`.",
                )
    for group in measures.grain_groups:
        # Auxiliary window groups are rebuilt from the base groups by metrics.py.
        # Their coarser grain may not contain the requested bucket dimension.
        if group.is_window_grain_group:
            continue
        _bucket_grain_group(
            group,
            group_others.ranked,
            group_others.dimensions,
            _non_decomposable_input_names(group, measures),
        )

    # The measures layer has already applied WHERE predicates to its source rows.
    # Reapplying a predicate on a bucketed dimension after aggregation would
    # discard the Others row (for example, country != 'US').
    bucketed_dimensions = set(group_others.dimensions)
    return [
        flt
        for flt in measures.ctx.dimension_filters
        if not bucketed_dimensions.intersection(extract_filter_dimension_refs([flt]))
    ]


def bucket_dimension_rows(
    main: GeneratedSQL,
    ranked: GeneratedSQL,
    dimensions: tuple[str, ...],
) -> GeneratedSQL:
    """Bucket a dimension-only result using an explicit ranking metric."""
    main_alias = "__dj_dimension_rows"
    main_query = as_subquery(main.query, main_alias)
    ranked_query, ranked_keys, member_name = _ranked_query(ranked, dimensions, set())
    pairs = [
        (
            make_column_ref(col.name, main_alias),
            make_column_ref(ranked_keys[col.semantic_name], _RANKED_ALIAS),
        )
        for col in main.columns
        if col.semantic_name in dimensions
    ]
    if len(pairs) != len(dimensions):
        raise DJInvalidInputException("GROUP_OTHERS cannot resolve ranking dimensions.")

    projection: list[ast.Aliasable | ast.Expression] = []
    columns = deepcopy(main.columns)
    for col in columns:
        original = make_column_ref(col.name, main_alias)
        expression = (
            _bucket_expression(original, member_name)
            if col.semantic_name in dimensions
            else original
        )
        projection.append(
            ast.Alias(child=expression, alias=ast.Name(col.name)).set_as(True),
        )
        if col.semantic_name in dimensions:
            col.type = "string"

    query = ast.Query(
        select=ast.Select(
            quantifier="DISTINCT",
            projection=projection,
            from_=ast.From(
                relations=[
                    ast.Relation(
                        primary=main_query,
                        extensions=[
                            ranked_tuple_join(ranked_query, pairs, "LEFT OUTER"),
                        ],
                    ),
                ],
            ),
        ),
    )
    return GeneratedSQL(
        query=query,
        columns=columns,
        dialect=main.dialect,
        cube_name=main.cube_name,
        scan_estimate=main.scan_estimate,
        warnings=main.warnings + ranked.warnings,
    )
