"""Preserve lexical CTE bindings when independently built queries are combined."""

from collections.abc import Iterator
from copy import deepcopy

from sqlglot import Dialect as SQLGlotDialect
from sqlglot import exp

from datajunction_server.models.dialect import Dialect
from datajunction_server.sql.parsing import ast


class CTENameRegistry:
    """Allocate names without capturing existing relations or nested CTEs."""

    # These are emitted by the final metrics/window/filter/lookback stages.
    WRAPPER_NAMES = frozenset({"base_metrics", "__metric_filtered", "__windowed"})

    def __init__(
        self,
        queries: list[ast.Query],
        *,
        dialect: Dialect = Dialect.SPARK,
        reserved: list[str] | None = None,
    ):
        # Use the same identifier rules as SQL rendering. Unknown plugin dialects
        # retain the generic rule: fold unquoted names, preserve quoted names.
        self.dialect = SQLGlotDialect.classes.get(str(dialect), SQLGlotDialect)()
        # Fixed names are emitted by the caller; lifted CTEs must not keep them.
        # Reserve quoted spellings too: later stages also locate these aliases
        # by their literal names (for example, lookback's base_metrics wrapper).
        self.fixed = {
            self.key(ast.Name(name, quote_style=quote_style))
            for name in (*self.WRAPPER_NAMES, *(reserved or []))
            for quote_style in ("", '"')
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
                    self.used.add(self.key(node.alias))
                if isinstance(node, ast.Table):
                    self.used.add(self.key(node.name))
            # Lifting a CTE must not capture a physical, unqualified relation
            # from another group's independent WITH scope.
            self.physical_names.update(
                self.key(table.name)
                for table, target in _bound_relations(query, {}, self)
                if target is None and table.name.namespace is None
            )

    def key(self, name: ast.Name) -> str:
        """Resolve identifiers without changing their spelling or emitted quotes."""
        return self.dialect.normalize_identifier(
            exp.Identifier(this=name.name, quoted=bool(name.quote_style)),
        ).name

    def allocate(
        self,
        name: str,
        *,
        quote_style: str = "",
        with_wrapper: bool = False,
    ) -> str:
        """Reserve a name and, if needed, its pre-aggregation wrapper together."""
        candidate = name
        index = 0
        while self.key(ast.Name(candidate, quote_style=quote_style)) in self.used or (
            with_wrapper and self.key(ast.Name(f"{candidate}_agg")) in self.used
        ):
            index += 1
            candidate = f"{name}__{index}"
        self.used.add(self.key(ast.Name(candidate, quote_style=quote_style)))
        if with_wrapper:
            self.used.add(self.key(ast.Name(f"{candidate}_agg")))
        return candidate

    @classmethod
    def for_combined_ctes(
        cls,
        ctes: list[ast.Query],
        dialect: Dialect = Dialect.SPARK,
    ) -> "CTENameRegistry":
        """Reserve only names visible to a new top-level CTE in combined SQL.

        Lifted variants are already distinct. Relation aliases and qualified
        physical tables cannot be captured by a CTE name, so they do not force
        fixed wrapper names such as base_metrics (which later stages look up).
        """
        names = cls([], dialect=dialect)
        names.used = {
            names.key(table.name)
            for cte in ctes
            for table, target in _bound_relations(cte, {}, names)
            if target is None and table.name.namespace is None
        } | {names.key(cte.alias_or_name) for cte in ctes}
        return names


def _bound_relations(
    node: ast.Node,
    bindings: dict[str, ast.Name],
    names: CTENameRegistry,
) -> Iterator[tuple[ast.Table, ast.Name | None]]:
    """Resolve relation references through non-recursive lexical WITH scopes."""
    if isinstance(node, ast.Query):
        local = dict(bindings)
        for cte in node.ctes:
            # A nested WITH sees its preceding siblings and shadows outer names.
            yield from _bound_relations(cte, local, names)
            assert cte.alias is not None
            local[names.key(cte.alias)] = cte.alias
        yield from _bound_relations(node.select, local, names)
    elif isinstance(node, ast.Column):
        # A Column may own a Table used only as a qualifier, or point to a
        # compiled FROM relation. Neither is a new relation reference. Keeping
        # the original name as a relation alias handles both representations.
        return
    elif isinstance(node, ast.Table):
        target = (
            bindings.get(names.key(node.name)) if node.name.namespace is None else None
        )
        yield node, target
    else:
        for child in node.children:
            yield from _bound_relations(child, bindings, names)


def _rebind_ctes(
    query: ast.Query,
    bindings: dict[str, ast.Name],
    names: CTENameRegistry,
) -> None:
    """Rewrite CTE relations, retaining the qualifiers visible to their columns."""
    for table, target in _bound_relations(query, bindings, names):
        if target is not None and (
            target.name != table.name.name
            or target.quote_style != table.name.quote_style
        ):
            has_outer_alias = isinstance(table.parent, ast.Alias) and table.parent.alias
            if table.alias is None and not has_outer_alias:
                table.alias = deepcopy(table.name)
            # A quoted reader may resolve to an unquoted producer (or vice
            # versa). Keep the producer's quoting so case-sensitive dialects
            # resolve the new reference to that same producer.
            table.name = ast.Name(target.name, quote_style=target.quote_style)


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
    identities: dict[tuple[str, str], ast.Name] = {}
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
        bindings: dict[str, ast.Name] = {}
        for cte in main.ctes:
            assert cte.alias is not None
            original = cte.alias.name
            key = names.key(cte.alias)
            _rebind_ctes(cte, bindings, names)
            # Alias spelling/quoting is already represented by the dialect key;
            # compare bodies without making equivalent aliases distinct again.
            alias = cte.alias
            cte.alias = None
            identity = (key, str(cte))
            cte.alias = alias
            target = identities.get(identity)
            if target is None:
                # Original names were reserved before allocating any new ones.
                # Their first definition can keep them, except fixed wrappers.
                target_name = (
                    original
                    if key not in original_owners
                    and key not in names.fixed
                    and key not in names.physical_names
                    else names.allocate(original, quote_style=cte.alias.quote_style)
                )
                target = ast.Name(target_name, quote_style=cte.alias.quote_style)
                original_owners.add(key)
                identities[identity] = target
                cte.alias = ast.Name(target.name, quote_style=target.quote_style)
                shared.append(cte)
            bindings[key] = target
        main.ctes = []
        main.invalidate_table_expr_cache()
        _rebind_ctes(main, bindings, names)
        mains.append(main)
    return shared, mains
