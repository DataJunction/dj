"""Preserve lexical CTE bindings when independently built queries are combined."""

from collections.abc import Iterator
from copy import deepcopy

from datajunction_server.sql.parsing import ast


def _binding_key(name: ast.Name) -> str:
    return name.name if name.quote_style else name.name.casefold()


class CTENameRegistry:
    """Allocate names without capturing existing relations or nested CTEs."""

    # These are emitted by the final metrics/window/filter/lookback stages.
    WRAPPER_NAMES = frozenset({"base_metrics", "__metric_filtered", "__windowed"})

    def __init__(
        self,
        queries: list[ast.Query],
        *,
        reserved: list[str] | None = None,
    ):
        # Fixed names are emitted by the caller; lifted CTEs must not keep them.
        self.fixed = set(self.WRAPPER_NAMES) | {
            name.casefold() for name in reserved or []
        }
        self.used: set[str] = set(self.fixed)
        self.physical_names: set[str] = set()
        for query in queries:
            for node in query.flatten():
                if (
                    isinstance(node, ast.TableExpression)
                    or isinstance(node, ast.Alias)
                    and isinstance(node.child, ast.TableExpression)
                ) and node.alias:
                    self.used.add(node.alias.name.casefold())
                if isinstance(node, ast.Table):
                    self.used.add(node.name.name.casefold())
            # Lifting a CTE must not capture a physical, unqualified relation
            # from another group's independent WITH scope.
            self.physical_names.update(
                table.name.name.casefold()
                for table, target in _bound_relations(query, {})
                if target is None and table.name.namespace is None
            )

    def allocate(self, name: str, *, with_wrapper: bool = False) -> str:
        """Reserve a name and, if needed, its pre-aggregation wrapper together."""
        candidate = name
        index = 0
        while candidate.casefold() in self.used or (
            with_wrapper and f"{candidate}_agg".casefold() in self.used
        ):
            index += 1
            candidate = f"{name}__{index}"
        self.used.add(candidate.casefold())
        if with_wrapper:
            self.used.add(f"{candidate}_agg".casefold())
        return candidate

    @classmethod
    def for_combined_ctes(cls, ctes: list[ast.Query]) -> "CTENameRegistry":
        """Reserve only names visible to a new top-level CTE in combined SQL.

        Lifted variants are already distinct. Relation aliases and qualified
        physical tables cannot be captured by a CTE name, so they do not force
        fixed wrapper names such as base_metrics (which later stages look up).
        """
        names = cls([])
        names.used = {
            table.name.name.casefold()
            for cte in ctes
            for table, target in _bound_relations(cte, {})
            if target is None and table.name.namespace is None
        } | {cte.alias_or_name.name.casefold() for cte in ctes}
        return names


def _bound_relations(
    node: ast.Node,
    bindings: dict[str, str],
) -> Iterator[tuple[ast.Table, str | None]]:
    """Resolve relation references through non-recursive lexical WITH scopes."""
    if isinstance(node, ast.Query):
        local = dict(bindings)
        for cte in node.ctes:
            # A nested WITH sees its preceding siblings and shadows outer names.
            yield from _bound_relations(cte, local)
            assert cte.alias is not None
            local[_binding_key(cte.alias)] = cte.alias.name
        yield from _bound_relations(node.select, local)
    elif isinstance(node, ast.Column):
        # A Column may own a Table used only as a qualifier, or point to a
        # compiled FROM relation. Neither is a new relation reference. Keeping
        # the original name as a relation alias handles both representations.
        return
    elif isinstance(node, ast.Table):
        target = (
            bindings.get(_binding_key(node.name))
            if node.name.namespace is None
            else None
        )
        yield node, target
    else:
        for child in node.children:
            yield from _bound_relations(child, bindings)


def _rebind_ctes(query: ast.Query, bindings: dict[str, str]) -> None:
    """Rewrite CTE relations, retaining the qualifiers visible to their columns."""
    for table, target in _bound_relations(query, bindings):
        if target is not None and target != table.name.name:
            has_outer_alias = isinstance(table.parent, ast.Alias) and table.parent.alias
            if table.alias is None and not has_outer_alias:
                table.alias = deepcopy(table.name)
            table.name = ast.Name(target, quote_style=table.name.quote_style)


def collect_shared_ctes(
    queries: list[ast.Query],
    names: CTENameRegistry,
) -> tuple[list[ast.Query], list[ast.Query]]:
    """Share equal bodies after binding dependencies, returning copied main queries.

    Compiler CTEs are ordered before their consumers. Resolving each definition
    in that order makes a conflicting dependency distinguish its readers too.
    Rendered AST equality is deliberately conservative: no semantic equivalence
    of different SQL expressions is assumed.
    """
    shared: list[ast.Query] = []
    mains: list[ast.Query] = []
    identities: dict[tuple[str, str], str] = {}
    original_owners: set[str] = set()
    for query in queries:
        main = deepcopy(query)
        # Compiled columns can point directly at a CTE Query rather than the
        # FROM Table. Freeze their original qualifiers before renaming those
        # Query objects; renamed FROM relations retain these names as aliases.
        qualifiers = {
            id(cte): ast.Table(name=deepcopy(cte.alias))
            for cte in main.ctes
            if cte.alias is not None
        }
        for column in main.find_all(ast.Column):
            if qualifier := qualifiers.get(id(column.table)):
                column.add_table(qualifier)
        bindings: dict[str, str] = {}
        for cte in main.ctes:
            assert cte.alias is not None
            original = cte.alias.name
            key = _binding_key(cte.alias)
            _rebind_ctes(cte, bindings)
            identity = (key, str(cte))
            target = identities.get(identity)
            if target is None:
                # Original names were reserved before allocating any new ones.
                # Their first definition can keep them, except fixed wrappers.
                target = (
                    original
                    if original.casefold() not in original_owners
                    and original.casefold() not in names.fixed
                    and original.casefold() not in names.physical_names
                    else names.allocate(original)
                )
                original_owners.add(original.casefold())
                identities[identity] = target
                cte.alias = ast.Name(target, quote_style=cte.alias.quote_style)
                shared.append(cte)
            bindings[key] = target
        main.ctes = []
        main.invalidate_table_expr_cache()
        _rebind_ctes(main, bindings)
        mains.append(main)
    return shared, mains
