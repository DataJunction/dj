"""Place request predicates at their consumers, without narrowing shared inputs."""

from __future__ import annotations

from collections import Counter
from typing import cast

from datajunction_server.construction.build_v3.cte import (
    _get_relation_side_id,
    _rewrite_filter_for_cte,
    _wrap_relation_side_with_filter,
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


def _find_bindings(
    query: ast.Query,
    candidates: list[tuple[str, str]],
    queries: dict[str, ast.Query],
    path: tuple[ast.Query, ...] = (),
) -> list[tuple[ast.Table, str, tuple[ast.Query, ...]]]:
    """Find explicit links through FROM/JOIN edges, never expression subqueries.

    Stop at the nearest linked node: its output is the semantic boundary, even
    if one of its inputs also declares the same link. Repeated occurrences stay
    distinct so callers can reject an ambiguous owner rather than filter both.
    """
    if any(query is scope for scope in path) or query.select.set_op is not None:
        return []
    path = (*path, query)
    result: list[tuple[ast.Table, str, tuple[ast.Query, ...]]] = []

    def visit(relation: ast.Expression) -> None:
        if isinstance(relation, ast.Query):
            result.extend(_find_bindings(relation, candidates, queries, path))
        elif isinstance(relation, ast.Table):
            name = relation.name.identifier(quotes=False)
            definition = _definition(relation, queries)
            columns = [
                column
                for owner, column in candidates
                if owner == name and definition is queries.get(name)
            ]
            if columns:
                result.extend((relation, column, path) for column in columns)
            elif definition is not None:
                result.extend(_find_bindings(definition, candidates, queries, path))

    if query.select.from_ is not None:
        for relation in query.select.from_.relations:
            visit(relation.primary)
            for join in relation.extensions:
                visit(join.right)
    return result


def place_filters(
    ctx: BuildContext,
    queries: dict[str, ast.Query],
    request: PushdownFilters,
) -> set[str]:
    """Resolve every predicate before applying any placements for this consumer.

    Local predicates can constrain the fact definition only when it has no
    other readers. Hidden-key predicates constrain a specific linked relation;
    reaching it through a shared definition is refused. Neither path broadcasts
    predicates into other CTEs that happen to expose the same column.
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
    # Target identity groups predicates on one relation into one wrapper.
    relations: dict[int, tuple[ast.Table, list[ast.Expression]]] = {}
    local: list[ast.Expression] = []
    consumed: set[str] = set()

    for filter_str in request.filters:
        refs = extract_filter_dimension_refs([filter_str], include_bare=True)
        if not hidden.keys() & set(refs):
            if readers[id(root)]:
                continue
            node = ctx.nodes[request.parent_node_name]
            rewritten = _rewrite_filter_for_cte(
                filter_str,
                request.column_aliases,
                {col.name for col in node.current.columns},
                root,
                request.outer_only_refs,
            )
            if rewritten is not None:
                local.append(rewritten)
                consumed.add(filter_str)
            continue

        def invalid(reason: str) -> None:
            raise DJInvalidInputException(
                f"Cannot place filter `{filter_str}` for {request.parent_node_name}: "
                f"{reason}. Expose the filtered key on the fact or provide an "
                "unambiguous dimension link.",
                http_status_code=422,
            )

        bindings: dict[str, ast.Name] = {}
        target: ast.Table | None = None
        for ref in refs:
            matches = _find_bindings(root, hidden.get(ref, []), queries)
            if len(matches) != 1:
                invalid(f"{ref} has {len(matches)} eligible relational owners")
            table, column, path = matches[0]
            if readers[id(root)] or any(readers[id(scope)] > 1 for scope in path[1:]):
                invalid(f"{ref} requires narrowing a shared query definition")
            if target is not None and table is not target:
                invalid("the complete predicate does not bind to one consumer")
            target = table
            qualifier = _get_relation_side_id(cast(ast.Expression, table))
            assert qualifier is not None
            bindings[ref] = ast.Name(column, namespace=ast.Name(qualifier))
        assert target is not None
        rewritten = resolve_filter_references(parse_filter(filter_str), bindings)
        relations.setdefault(id(target), (target, []))[1].append(rewritten)
        consumed.add(filter_str)

    for target, predicates in relations.values():
        # An explicit alias survives the later physical-table rewrite, so the
        # wrapper predicate never depends on a source's logical short name.
        if target.alias is None:
            target.alias = ast.Name(target.name.name)
        predicate = combine_filters(predicates)
        assert predicate is not None
        target.swap(
            _wrap_relation_side_with_filter(cast(ast.Expression, target), predicate),
        )
    # Local predicates constrain the fact's output. Moving them into the
    # nullable input of an authored outer join changes which rows qualify.
    select = cast(ast.Select, root.select)
    for predicate in local:
        select.where = (
            ast.BinaryOp.And(select.where, predicate) if select.where else predicate
        )
    return consumed
