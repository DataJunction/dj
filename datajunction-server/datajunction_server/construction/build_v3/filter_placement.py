"""Place request predicates at their consumers, without narrowing shared inputs."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import cast

from datajunction_server.construction.build_v3.cte import (
    _filter_bindings_for_select,
    _get_relation_side_id,
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
    scope: ast.Select
    # Found below another linked node, not directly from the fact.
    nested: bool = False


def _find_bindings(
    query: ast.Query,
    candidates: list[tuple[str, str]],
    queries: dict[str, ast.Query],
    nested: bool = False,
    visited: set[int] | None = None,
) -> list[_Binding]:
    """Find every scan of a linked node through FROM/JOIN edges, in any arm of a
    set operation and in any nested query, but never in an expression subquery.

    A linked node's own inputs are searched too, so a deeper node that also
    links the dimension gets the filter on its own scan. Each definition is
    searched once, however many readers it has; each reader still gets its own
    binding.
    """
    visited = set() if visited is None else visited
    result: list[_Binding] = []

    def visit(relation: ast.Expression, scope: ast.Select) -> None:
        if isinstance(relation, ast.Query):
            result.extend(
                _find_bindings(relation, candidates, queries, nested, visited),
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
                    _Binding(relation, column, scope, nested) for column in columns
                )
            # A linked node can read nodes that link the dimension too.
            if definition is not None and id(definition) not in visited:
                visited.add(id(definition))
                result.extend(
                    _find_bindings(
                        definition,
                        candidates,
                        queries,
                        nested or bool(columns),
                        visited,
                    ),
                )

    arm: ast.SelectExpression | None = query.select
    while isinstance(arm, ast.Select):
        if arm.from_ is not None:
            for relation in arm.from_.relations:
                visit(relation.primary, arm)
                for join in relation.extensions:
                    visit(join.right, arm)
        arm = arm.set_op.right if arm.set_op else None
    return result


def place_filters(
    ctx: BuildContext,
    queries: dict[str, ast.Query],
    request: PushdownFilters,
) -> set[str]:
    """Resolve every predicate before applying any placements for this consumer.

    Local predicates can constrain the fact definition only when it has no
    other readers. Hidden-key predicates constrain the SELECT that reads their
    explicit link, on every scan of a linked node.
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
        local_bindings = _filter_bindings_for_select(
            local_refs,
            request.column_aliases,
            output_cols,
            root_select,
            request.outer_only_refs,
        )
        if local_bindings is None:
            invalid("local references cannot bind to the fact output")
        assert local_bindings is not None
        owners = {
            ref: _find_bindings(root, hidden[ref], queries)
            for ref in refs
            if ref in hidden
        }
        for ref, found in owners.items():
            if not found:
                invalid(f"{ref} has 0 eligible relational owners")
        # A lone hidden key goes on every owner; several keys need one each.
        if len(owners) == 1 and not local_refs:
            [(ref, found)] = owners.items()
            units = [{ref: binding} for binding in found]
        else:
            # A compound predicate needs one placement per key: the nearest.
            nearest = {
                ref: [binding for binding in found if not binding.nested]
                for ref, found in owners.items()
            }
            for ref, found in nearest.items():
                if len(found) > 1:
                    invalid(f"{ref} has {len(found)} eligible relational owners")
            units = [{ref: found[0] for ref, found in nearest.items()}]
        for unit in units:
            bindings = dict(local_bindings)
            target = root_select if local_refs else None
            for ref, binding in unit.items():
                table, column = binding.table, binding.column
                scope = binding.scope
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


def _projects_cast(select: ast.Select, column: str) -> bool:
    """Whether the select exposes ``column`` as a CAST of something."""
    return any(
        isinstance(getattr(expr, "child", expr), ast.Cast)
        and getattr(getattr(expr, "alias", None), "name", None) == column
        for expr in select.projection
    )


def _redefines_rows(query: ast.Query) -> bool:
    """Whether filtering inside would change which rows or values it defines."""
    return query.select.limit is not None or any(query.find_all(ast.Over))


def place_dimension_filters(
    ctx: BuildContext,
    queries: dict[str, ast.Query],
    request: PushdownFilters,
) -> None:
    """Take each joined dimension and add a WHERE clause when it can bind the
    request's filter to its own link key.

    A dimension with its own link under a role (e.g. ``[measure]``) binds a
    filter on that role to its own key column. It is skipped when it is joined
    more than once or read by another query. A dimension read from a
    materialized table has its predicate recorded, for the join to wrap the
    table with.
    """
    for name, joins in request.dimension_joins.items():
        node = ctx.nodes[name]
        if joins != 1 or not node.current:
            continue
        query = queries.get(name)
        if query is not None and any(
            _definition(table, queries) is query
            for other in queries.values()
            for table in other.find_all(ast.Table)
        ):
            continue
        bindings = _role_bindings(node)
        output_cols = {col.name for col in node.current.columns}
        for filter_str in request.filters:
            refs = set(extract_filter_dimension_refs([filter_str], include_bare=True))
            if not refs or not refs <= bindings.keys():
                continue
            if query is None:
                predicate = cast(
                    ast.Expression,
                    resolve_filter_references(
                        parse_filter(filter_str),
                        {ref: ast.Name(bindings[ref]) for ref in refs},
                    ),
                )
                existing = request.materialized_predicates.get(name)
                request.materialized_predicates[name] = cast(
                    ast.Expression,
                    combine_filters(
                        [existing, predicate] if existing is not None else [predicate],
                    ),
                )
                continue
            select = cast(ast.Select, query.select)
            if _redefines_rows(query) or any(
                _projects_cast(select, bindings[ref]) for ref in refs
            ):
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


def _conjuncts(expr: ast.Expression) -> list[str]:
    """The AND-ed terms of an expression, as SQL text."""
    if isinstance(expr, ast.BinaryOp) and expr.op == ast.BinaryOpKind.And:
        return _conjuncts(expr.left) + _conjuncts(expr.right)
    return [str(expr)]


def place_upstream_link_filters(
    ctx: BuildContext,
    queries: dict[str, ast.Query],
    request: PushdownFilters,
) -> None:
    """Also filter nodes upstream of the fact that link the same dimension.

    A fact that links the dimension itself gets the filter on its own column.
    A node it reads that links the dimension too gets the filter where the
    fact reads it, so that node's scan is narrowed as well.
    """
    from datajunction_server.construction.build_v3.dimensions import (
        parse_dimension_ref,
        upstream_link_candidates,
    )

    root = queries.get(request.parent_node_name)
    if root is None:
        return
    hidden = {
        ref
        for (owner, ref) in ctx.upstream_filter_bindings
        if owner == request.parent_node_name
    }
    for filter_str in request.filters:
        refs = extract_filter_dimension_refs([filter_str], include_bare=True)
        if len(refs) != 1 or refs[0] in hidden:
            continue
        candidates = upstream_link_candidates(
            ctx,
            request.parent_node_name,
            parse_dimension_ref(refs[0]),
        )
        for binding in _find_bindings(root, candidates, queries):
            qualifier = _get_relation_side_id(cast(ast.Expression, binding.table))
            assert qualifier is not None
            if binding.table.alias is None:
                binding.table.alias = ast.Name(binding.table.name.name)
            predicate = resolve_filter_references(
                parse_filter(filter_str),
                {refs[0]: ast.Name(binding.column, namespace=ast.Name(qualifier))},
            )
            if binding.scope.where is not None and str(predicate) in _conjuncts(
                binding.scope.where,
            ):
                continue
            binding.scope.where = combine_filters(
                [binding.scope.where, predicate]
                if binding.scope.where is not None
                else [predicate],
            )
