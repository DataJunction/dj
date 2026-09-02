"""
The invariant every generated query must hold: whatever a CTE reads from
another CTE, that other CTE projects.

``prune_cte_projections`` trims each CTE to what its readers ask for, so this is
the oracle for whether a trim went too far. It needs nothing but the SQL text.
"""

from collections.abc import Iterator
from typing import cast

from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse


def _projected_names(cte: ast.Query) -> set[str]:
    """Names a CTE exposes, taking the first branch of any set operation."""
    names = set()
    for expression in cte.select.projection:
        names.add(cast(ast.Aliasable, expression).alias_or_name.name)
    return names


def _branches(select: ast.SelectExpression) -> list[ast.SelectExpression]:
    """Each arm of a set operation, or the select itself when there is none."""
    branches = []
    branch: ast.SelectExpression | None = select
    while branch is not None:
        branches.append(branch)
        branch = branch.set_op.right if branch.set_op else None
    return branches


def _branch_columns(
    branch: ast.SelectExpression,
) -> Iterator[tuple[ast.Column, bool]]:
    """
    Columns of one arm, not those of the arms unioned after it.

    Each column is flagged with whether it sits in a clause that may name the
    select's own output aliases rather than a column of its source.
    """
    keyed = [
        *((part, False) for part in branch.projection),
        *((part, False) for part in branch.group_by),
        *([(branch.from_, False)] if branch.from_ else []),
        *([(branch.where, True)] if branch.where else []),
        *([(branch.having, True)] if branch.having else []),
    ]
    for part, may_use_output_alias in keyed:
        for column in part.find_all(ast.Column):
            yield column, may_use_output_alias


def _output_aliases(branch: ast.SelectExpression) -> set[str]:
    """
    Names this arm introduces with an explicit ``AS``.

    A filter DJ pushes into a CTE can land on such a name — the alias is the
    only handle the column has once the underlying expression is not projected
    under its own name. Charging it to the source would be wrong: the source
    never had a column by that name.
    """
    names = set()
    for expression in branch.projection:
        alias = getattr(expression, "alias", None)
        if alias is not None:
            names.add(alias.name)
    return names


def unprojected_references(sql: str) -> set[str]:
    """
    Find every column a CTE reads from another CTE that is not projected there.

    Each arm of a set operation is read on its own, so a bare column in an arm
    drawing on a single CTE is charged to that CTE. Returns
    ``<producing cte>.<column>`` for each reference the producer does not
    project, so an empty set means the query is internally consistent.
    """
    query = parse(sql)
    ctes = {cte.alias_or_name.name: cte for cte in query.ctes}

    missing = set()
    for scope in [*query.ctes, query]:
        for branch in _branches(scope.select):
            tables = list(branch.from_.find_all(ast.Table)) if branch.from_ else []
            sources = {
                (table.alias.name if table.alias else table.name.name): ctes[
                    table.name.name
                ]
                for table in tables
                if table.name.name in ctes
            }
            lone_source = (
                next(iter(sources.values())) if len(tables) == 1 and sources else None
            )
            aliases = _output_aliases(branch)
            for column, may_use_output_alias in _branch_columns(branch):
                qualifier, _, name = column.identifier().rpartition(".")
                producer = sources.get(qualifier) if qualifier else lone_source
                if producer is None:
                    continue
                if not qualifier and may_use_output_alias and name in aliases:
                    continue
                if name not in _projected_names(producer):
                    missing.add(f"{producer.alias_or_name.name}.{name}")
    return missing
