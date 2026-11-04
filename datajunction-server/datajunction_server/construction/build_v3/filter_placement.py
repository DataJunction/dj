"""Place request predicates at their consumers, without narrowing shared inputs."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import cast

from datajunction_server.construction.build_v3.cte import (
    _filter_bindings_for_select,
    _is_star,
    _joins_naturally,
    _non_preserved_sides,
    _rewrite_filter_for_cte,
)
from datajunction_server.construction.build_v3.filters import (
    combine_filters,
    parse_filter,
    resolve_filter_references,
)
from datajunction_server.construction.build_v3.types import (
    BuildContext,
    PushdownFilters,
)
from datajunction_server.construction.build_v3.utils import (
    extract_filter_dimension_refs,
)
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.sql.parsing import ast


def _definition(table: ast.Table, queries: dict[str, ast.Query]) -> ast.Query | None:
    """Resolve a table lexically before looking for an authored node."""
    name = table.name.identifier(quotes=False)
    current = table.parent
    while current is not None:
        if isinstance(current, ast.Query):
            for cte in current.ctes:
                if cte.alias_or_name.identifier(quotes=False) == name:
                    return cte
        current = current.parent
    return queries.get(name)


@dataclass
class _Step:
    query: ast.Query
    relation: ast.Expression
    nullable: bool


@dataclass
class _Binding:
    column: str
    path: tuple[_Step, ...]


def _find_bindings(
    query: ast.Query,
    candidates: list[tuple[str, str]],
    queries: dict[str, ast.Query],
    path: tuple[_Step, ...] = (),
) -> list[_Binding]:
    """Find explicit links through FROM/JOIN edges, never expression subqueries.

    Stop at the nearest linked node: its output is the semantic boundary, even
    if one of its inputs also declares the same link. Repeated occurrences stay
    distinct so callers can reject an ambiguous owner rather than filter both.
    """
    if any(query is step.query for step in path) or query.select.set_op is not None:
        return []
    result: list[_Binding] = []

    def visit(relation: ast.Expression, nullable: bool) -> None:
        steps = (*path, _Step(query, relation, nullable))
        if isinstance(relation, ast.Query):
            result.extend(_find_bindings(relation, candidates, queries, steps))
        elif isinstance(relation, ast.Table):
            name = relation.name.identifier(quotes=False)
            definition = _definition(relation, queries)
            columns = [
                column
                for owner, column in candidates
                if owner == name and definition is queries.get(name)
            ]
            if columns:
                result.extend(_Binding(column, steps) for column in columns)
            elif definition is not None:
                result.extend(
                    _find_bindings(
                        definition,
                        candidates,
                        queries,
                        steps,
                    ),
                )

    if query.select.from_ is not None:
        for relation in query.select.from_.relations:
            nullable = _non_preserved_sides(relation)
            visit(relation.primary, relation.primary in nullable)
            for join in relation.extensions:
                visit(join.right, join.right in nullable)
    return result


def _can_extend_projection(query: ast.Query) -> bool:
    """Adding a column must preserve the authored rows and their grain."""
    select = query.select
    return not (
        query.column_list
        or select.group_by
        or select.having is not None
        or (select.quantifier or "").upper() == "DISTINCT"
        or select.limit is not None
        or select.lateral_views
        or any(expr.is_aggregation() for expr in select.projection)  # type: ignore[union-attr]
        or any(func.over is not None for func in select.find_all(ast.Function))
        or _joins_naturally(select)
    )


def _reference(
    relation: ast.Expression,
    column: ast.Name,
    aliases: dict[int, ast.Table],
) -> ast.Name:
    qualifier = getattr(relation, "alias", None)
    if isinstance(relation, ast.Table):
        aliases[id(relation)] = relation
        qualifier = qualifier or relation.name
    assert qualifier is not None
    return ast.Name(
        column.name,
        quote_style=column.quote_style,
        namespace=ast.Name(qualifier.name, quote_style=qualifier.quote_style),
    )


def _projected_name(select: ast.Select, reference: ast.Name) -> ast.Name | None:
    """Reuse only an unchanged column, never a cast or computed expression."""
    for expr in select.projection:
        inner = getattr(expr, "child", expr)
        if isinstance(inner, ast.Column) and inner.name.identifier(
            quotes=False,
        ) == reference.identifier(quotes=False):
            return cast(ast.Aliasable, expr).alias_or_name
    return None


def place_filters(
    ctx: BuildContext,
    queries: dict[str, ast.Query],
    request: PushdownFilters,
) -> set[str]:
    """Resolve every predicate before applying any placements for this consumer.

    Local predicates can constrain the fact definition only when it has no
    other readers. Hidden-key predicates constrain the SELECT that reads their
    explicit link, exposing keys through plain projections when a predicate
    must move above a nullable join or span multiple SELECT scopes.
    """
    hidden = {
        ref: candidates
        for (owner, ref), candidates in ctx.upstream_filter_bindings.items()
        if owner == request.parent_node_name
    }
    root = queries.get(request.parent_node_name)
    if root is None:
        if any(
            hidden.keys() & set(extract_filter_dimension_refs([f]))
            for f in request.filters
        ):
            raise DJInvalidInputException(
                f"Cannot place an upstream filter for materialized fact "
                f"{request.parent_node_name}. Expose the filtered key on the fact.",
            )
        return set()
    readers = Counter(
        id(definition)
        for query in queries.values()
        for table in query.find_all(ast.Table)
        if (definition := _definition(table, queries)) is not None
    )
    placements: dict[int, tuple[ast.Select, list[ast.Expression]]] = {}
    aliases: dict[int, ast.Table] = {}
    projections: dict[int, tuple[ast.Select, dict[str, ast.Alias]]] = {}
    names = {
        name.name.casefold()
        for query in queries.values()
        for name in query.find_all(ast.Name)
    }
    consumed: set[str] = set()
    root_select = cast(ast.Select, root.select)
    node = ctx.nodes[request.parent_node_name]
    output_cols = {col.name for col in node.current.columns}

    for filter_str in request.filters:
        refs = extract_filter_dimension_refs([filter_str], include_bare=True)
        if not hidden.keys() & set(refs):
            if readers[id(root)]:
                continue
            rewritten = _rewrite_filter_for_cte(
                filter_str,
                request.column_aliases,
                output_cols,
                root,
                request.outer_only_refs,
            )
            if rewritten is not None:
                placements.setdefault(id(root_select), (root_select, []))[1].append(
                    rewritten,
                )
                consumed.add(filter_str)
            continue

        def invalid(reason: str) -> None:
            raise DJInvalidInputException(
                f"Cannot place filter `{filter_str}` for {request.parent_node_name}: "
                f"{reason}. Expose the filtered key on the fact or provide an "
                "unambiguous dimension link.",
                http_status_code=422,
            )

        local_refs = [ref for ref in refs if ref not in hidden]
        bindings = _filter_bindings_for_select(
            local_refs,
            request.column_aliases,
            output_cols,
            root_select,
            request.outer_only_refs,
        )
        if bindings is None:
            invalid("local references cannot bind to the fact output")
        assert bindings is not None
        owners: dict[str, _Binding] = {}
        for ref in refs:
            if ref not in hidden:
                continue
            matches = _find_bindings(root, hidden[ref], queries)
            if len(matches) != 1:
                invalid(f"{ref} has {len(matches)} eligible relational owners")
            binding = matches[0]
            if readers[id(root)] or any(
                readers[id(step.query)] > 1 for step in binding.path[1:]
            ):
                invalid(f"{ref} requires narrowing a shared query definition")
            owners[ref] = binding

        # The deepest common SELECT, lifted above any NULL-filling boundary.
        paths = [binding.path for binding in owners.values()]
        depth = 0
        if not local_refs:
            for index, steps in enumerate(zip(*paths)):
                if any(step.query is not steps[0].query for step in steps):
                    break
                depth = index
        for path in paths:
            depth = min(
                [depth, *(i for i, step in enumerate(path[:-1]) if step.nullable)],
            )
        target = cast(ast.Select, paths[0][depth].query.select)

        for ref, binding in owners.items():
            path = binding.path
            reference = _reference(path[-1].relation, ast.Name(binding.column), aliases)
            for index in range(len(path) - 1, depth, -1):
                step, parent = path[index], path[index - 1]
                select = cast(ast.Select, step.query.select)
                if (
                    not _can_extend_projection(step.query)
                    or getattr(parent.relation, "column_list", [])
                    or _joins_naturally(parent.query.select)
                ):
                    invalid("propagation requires a plain projection boundary")
                output = _projected_name(select, reference)
                if output is None:
                    if any(
                        any(scope.filter(_is_star))
                        for scope in (select, parent.query.select)
                    ):
                        invalid(
                            "propagation requires explicit projections without wildcard readers",
                        )
                    additions = projections.setdefault(id(select), (select, {}))[1]
                    key = reference.identifier(quotes=False)
                    if key not in additions:
                        suffix = 1
                        internal_name = f"__dj_filter_{suffix}"
                        while internal_name.casefold() in names:
                            suffix += 1
                            internal_name = f"__dj_filter_{suffix}"
                        names.add(internal_name.casefold())
                        additions[key] = ast.Alias(
                            child=ast.Column(name=reference),
                            alias=ast.Name(internal_name),
                        )
                    output = additions[key].alias_or_name
                reference = _reference(parent.relation, output, aliases)
            bindings[ref] = reference
        rewritten = resolve_filter_references(parse_filter(filter_str), bindings)
        placements.setdefault(id(target), (target, []))[1].append(rewritten)
        consumed.add(filter_str)

    # Apply only after every predicate has a complete, enforceable placement.
    for table in aliases.values():
        if table.alias is None:
            table.alias = ast.Name(table.name.name, quote_style=table.name.quote_style)
    for select, additions in projections.values():
        select.projection = [*select.projection, *additions.values()]
    for select, predicates in placements.values():
        select.where = combine_filters(
            [select.where, *predicates] if select.where is not None else predicates,
        )
    return consumed
