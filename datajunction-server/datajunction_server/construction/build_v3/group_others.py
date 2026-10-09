"""Group non-ranked dimension tuples before final metric aggregation."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import cast

from datajunction_server.construction.build_v3.filters import (
    get_filter_column_references,
)
from datajunction_server.construction.build_v3.types import (
    GeneratedMeasuresSQL,
    GeneratedSQL,
    GrainGroupSQL,
)
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.types import StringType

_RANKED_ALIAS = "__dj_ranked_groups"
_MEMBER_COLUMN = "__dj_group_member"
_OTHERS_LABEL = "Others"


@dataclass(frozen=True)
class GroupOthers:
    """The ranked tuples and dimensions to bucket in a metrics build."""

    dimensions: tuple[str, ...]
    ranked: GeneratedSQL


def _column(name: str, alias: str) -> ast.Column:
    return ast.Column(name=ast.Name(name), _table=ast.Table(ast.Name(alias)))


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


def _ranked_query(ranked: GeneratedSQL) -> ast.Query:
    query = deepcopy(ranked.query)
    marker = ast.Alias(child=ast.Number(1), alias=ast.Name(_MEMBER_COLUMN))
    marker.set_as(True)
    query.select.projection.append(marker)
    query.parenthesized = True
    query.alias = ast.Name(_RANKED_ALIAS)
    query.as_ = True
    return query


def _null_safe_match(left: ast.Expression, right: ast.Expression) -> ast.Expression:
    match = ast.BinaryOp(
        left=ast.BinaryOp(left=left, right=right, op=ast.BinaryOpKind.Eq),
        right=ast.BinaryOp(
            left=ast.IsNull(expr=deepcopy(left)),
            right=ast.IsNull(expr=deepcopy(right)),
            op=ast.BinaryOpKind.And,
        ),
        op=ast.BinaryOpKind.Or,
    )
    match.parenthesized = True
    return match


def _bucket_expression(original: ast.Expression) -> ast.Case:
    return ast.Case(
        conditions=[
            ast.IsNull(expr=_column(_MEMBER_COLUMN, _RANKED_ALIAS), negated=True),
        ],
        results=[ast.Cast(data_type=StringType(), expression=deepcopy(original))],
        else_result=ast.String(f"'{_OTHERS_LABEL}'"),
    )


def _bucket_grain_group(
    group: GrainGroupSQL,
    ranked: GeneratedSQL,
    dimensions: tuple[str, ...],
) -> None:
    select = group.query.select
    if (
        not isinstance(select, ast.Select)
        or not select.from_
        or not select.from_.relations
    ):
        raise DJInvalidInputException("GROUP_OTHERS requires a grouped metrics query.")

    ranked_columns = {col.semantic_name: col.name for col in ranked.columns}
    dimension_columns = {
        col.semantic_name: col
        for col in group.columns
        if col.semantic_type == "dimension"
    }
    selected: list[tuple[int, ast.Expression, str]] = []
    matches: list[ast.Expression] = []

    for dimension in dimensions:
        if dimension not in dimension_columns or dimension not in ranked_columns:
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
        matches.append(
            _null_safe_match(
                deepcopy(original),
                _column(ranked_columns[dimension], _RANKED_ALIAS),
            ),
        )

    select.from_.relations[0].extensions.append(
        ast.Join(
            join_type="LEFT OUTER",
            right=_ranked_query(ranked),
            criteria=ast.JoinCriteria(on=ast.BinaryOp.And(*matches)),
        ),
    )

    for index, original, dimension in selected:
        alias = dimension_columns[dimension].name
        bucketed = ast.Alias(
            child=_bucket_expression(original),
            alias=ast.Name(alias),
        )
        bucketed.set_as(True)
        select.projection[index] = bucketed
        dimension_columns[dimension].type = "string"

        if select.group_by:
            matching_positions = [
                position
                for position, expression in enumerate(select.group_by)
                if str(expression) == str(original)
            ]
            if len(matching_positions) != 1:
                raise DJInvalidInputException(
                    f"GROUP_OTHERS cannot group dimension `{dimension}` at its source grain.",
                )
            select.group_by[matching_positions[0]] = _bucket_expression(original)


def apply_group_others(
    measures: GeneratedMeasuresSQL,
    group_others: GroupOthers,
) -> None:
    """Bucket grain dimensions before component merge and derived metrics."""
    for group in measures.grain_groups:
        _bucket_grain_group(group, group_others.ranked, group_others.dimensions)

    # The measures layer has already applied WHERE predicates to its source rows.
    # Reapplying a predicate on a bucketed dimension after aggregation would
    # discard the Others row (for example, country != 'US').
    bucketed_dimensions = set(group_others.dimensions)
    measures.ctx.dimension_filters = [
        flt
        for flt in measures.ctx.dimension_filters
        if not bucketed_dimensions.intersection(get_filter_column_references(flt))
    ]


def bucket_dimension_rows(
    main: GeneratedSQL,
    ranked: GeneratedSQL,
    dimensions: tuple[str, ...],
) -> GeneratedSQL:
    """Bucket a dimension-only result using an explicit ranking metric."""
    main_alias = "__dj_dimension_rows"
    main_query = deepcopy(main.query)
    main_query.parenthesized = True
    main_query.alias = ast.Name(main_alias)
    main_query.as_ = True
    ranked_columns = {col.semantic_name: col.name for col in ranked.columns}
    matches = [
        _null_safe_match(
            _column(col.name, main_alias),
            _column(ranked_columns[col.semantic_name], _RANKED_ALIAS),
        )
        for col in main.columns
        if col.semantic_name in dimensions
    ]
    if len(matches) != len(dimensions):
        raise DJInvalidInputException("GROUP_OTHERS cannot resolve ranking dimensions.")

    projection: list[ast.Aliasable | ast.Expression] = []
    columns = deepcopy(main.columns)
    for col in columns:
        original = _column(col.name, main_alias)
        expression = (
            _bucket_expression(original)
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
                            ast.Join(
                                join_type="LEFT OUTER",
                                right=_ranked_query(ranked),
                                criteria=ast.JoinCriteria(
                                    on=ast.BinaryOp.And(*matches),
                                ),
                            ),
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
