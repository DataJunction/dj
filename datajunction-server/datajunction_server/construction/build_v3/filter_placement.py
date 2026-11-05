"""Place request predicates at their consumers, without narrowing shared inputs."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import cast

from datajunction_server.construction.build_v3.cte import (
    _filter_bindings_for_select,
    _get_relation_side_id,
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
    get_short_name,
)
from datajunction_server.database.node import Node
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
class _Binding:
    table: ast.Table
    column: str
    path: tuple[ast.Query, ...]
    crosses_nullable_boundary: bool


def _find_bindings(
    query: ast.Query,
    candidates: list[tuple[str, str]],
    queries: dict[str, ast.Query],
    path: tuple[ast.Query, ...] = (),
    crosses_nullable_boundary: bool = False,
) -> list[_Binding]:
    """Find explicit links through FROM/JOIN edges, never expression subqueries.

    Stop at the nearest linked node: its output is the semantic boundary, even
    if one of its inputs also declares the same link. Repeated occurrences stay
    distinct so callers can reject an ambiguous owner rather than filter both.
    """
    if any(query is scope for scope in path) or query.select.set_op is not None:
        return []
    path = (*path, query)
    result: list[_Binding] = []

    def visit(relation: ast.Expression, nullable: bool) -> None:
        if isinstance(relation, ast.Query):
            result.extend(
                _find_bindings(
                    relation,
                    candidates,
                    queries,
                    path,
                    crosses_nullable_boundary or nullable,
                ),
            )
        elif isinstance(relation, ast.Table):
            name = relation.name.identifier(quotes=False)
            definition = _definition(relation, queries)
            columns = [
                column
                for owner, column in candidates
                if owner == name and definition is queries.get(name)
            ]
            if columns:
                result.extend(
                    _Binding(relation, column, path, crosses_nullable_boundary)
                    for column in columns
                )
            elif definition is not None:
                result.extend(
                    _find_bindings(
                        definition,
                        candidates,
                        queries,
                        path,
                        crosses_nullable_boundary or nullable,
                    ),
                )

    if query.select.from_ is not None:
        for relation in query.select.from_.relations:
            nullable = _non_preserved_sides(relation)
            visit(relation.primary, relation.primary in nullable)
            for join in relation.extensions:
                visit(join.right, join.right in nullable)
    return result


def place_filters(
    ctx: BuildContext,
    queries: dict[str, ast.Query],
    request: PushdownFilters,
) -> set[str]:
    """Resolve every predicate before applying any placements for this consumer.

    Local predicates can constrain the fact definition only when it has no
    other readers. Hidden-key predicates constrain the SELECT that reads their
    explicit link. Shared definitions and nullable boundaries hiding the key
    cannot enforce the predicate for this consumer and are refused.
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
        target = root_select if local_refs else None
        for ref in refs:
            if ref not in hidden:
                continue
            matches = _find_bindings(root, hidden[ref], queries)
            if len(matches) != 1:
                invalid(f"{ref} has {len(matches)} eligible relational owners")
            binding = matches[0]
            table, column, path = binding.table, binding.column, binding.path
            if readers[id(root)] or any(readers[id(scope)] > 1 for scope in path[1:]):
                invalid(f"{ref} requires narrowing a shared query definition")
            if binding.crosses_nullable_boundary:
                invalid(f"{ref} crosses a nullable query boundary that hides the key")
            scope = cast(ast.Select, path[-1].select)
            if target is not None and scope is not target:
                invalid("the complete predicate does not bind to one SELECT scope")
            target = scope
            qualifier = _get_relation_side_id(cast(ast.Expression, table))
            assert qualifier is not None
            bindings[ref] = ast.Name(column, namespace=ast.Name(qualifier))
            aliases[id(table)] = table
        assert target is not None
        rewritten = resolve_filter_references(parse_filter(filter_str), bindings)
        placements.setdefault(id(target), (target, []))[1].append(rewritten)
        consumed.add(filter_str)

    # Apply only after every predicate has a complete, enforceable placement.
    for table in aliases.values():
        if table.alias is None:
            table.alias = ast.Name(table.name.name)
    for select, predicates in placements.values():
        select.where = combine_filters(
            [select.where, *predicates] if select.where is not None else predicates,
        )
    return consumed


def _role_bindings(node: Node) -> dict[str, str]:
    """Role-qualified dimension ref -> this node's own key column."""
    if not node.current:  # pragma: no cover
        return {}
    return {
        f"{dim_col}[{link.role}]": get_short_name(fk)
        for link in node.current.dimension_links
        if link.role
        for dim_col, fk in (link.foreign_keys_reversed or {}).items()
        if fk
    }


def place_dimension_filters(
    ctx: BuildContext,
    queries: dict[str, ast.Query],
    request: PushdownFilters,
) -> None:
    """Apply a filter on a roled attribute to the dimension that owns the role.

    A joined dimension with its own link under a role (e.g. ``[measure]``)
    can bind a filter on that role to its own key column. This applies when
    the fact has the same role link, the dimension is joined once, and no
    other query reads it.
    """
    fact_refs = _role_bindings(ctx.nodes[request.parent_node_name]).keys()
    for name, joins in request.dimension_joins.items():
        node = ctx.nodes[name]
        query = queries.get(name)
        if joins != 1 or query is None or not node.current:
            continue
        if any(
            _definition(table, queries) is query
            for other in queries.values()
            for table in other.find_all(ast.Table)
        ):
            continue
        bindings = _role_bindings(node)
        output_cols = {col.name for col in node.current.columns}
        select = cast(ast.Select, query.select)
        for filter_str in request.filters:
            refs = set(extract_filter_dimension_refs([filter_str], include_bare=True))
            if not refs or not refs <= bindings.keys() & fact_refs:
                continue
            rewritten = _rewrite_filter_for_cte(
                filter_str,
                bindings,
                output_cols,
                query,
            )
            if rewritten is not None:
                select.where = combine_filters(
                    [select.where, rewritten]
                    if select.where is not None
                    else [rewritten],
                )
