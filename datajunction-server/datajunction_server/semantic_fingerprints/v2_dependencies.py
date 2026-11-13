"""Narrow output and row dependencies for semantic fingerprint version 2."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from datajunction_server.models.deployment import (
    ColumnSpec,
    CubeSpec,
    DimensionJoinLinkSpec,
    DimensionReferenceLinkSpec,
    DimensionSpec,
    LinkableNodeSpec,
    MetricSpec,
    NodeSpec,
    SourceSpec,
)
from datajunction_server.models.filter_requirements import FilterRequirement
from datajunction_server.naming import amenable_name
from datajunction_server.semantic_fingerprints.normalization import (
    canonical_json,
    normalize_value,
)
from datajunction_server.semantic_fingerprints.v2 import (
    ComponentDefinition,
    ComponentKey,
    SemanticGraphSnapshot,
    _parsed_type,
)
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import (
    SqlBaseParser,
    SqlParsingError,
    cached_definition_tree,
    parse,
)
from datajunction_server.sql.parsing.backends.exceptions import DJParseException
from datajunction_server.sql.parsing.structural import serialize_ast

_AGGREGATE_FUNCTIONS = frozenset(
    {
        "ANY_VALUE",
        "APPROX_COUNT_DISTINCT",
        "AVG",
        "COUNT",
        "COUNT_IF",
        "EVERY",
        "MAX",
        "MIN",
        "SOME",
        "STDDEV",
        "STDDEV_POP",
        "STDDEV_SAMP",
        "SUM",
        "VARIANCE",
        "VAR_POP",
        "VAR_SAMP",
    },
)
_SCALAR_FUNCTIONS = frozenset(
    {
        "ABS",
        "ACOS",
        "ARRAY_CONTAINS",
        "ASIN",
        "ATAN",
        "CEIL",
        "CEILING",
        "COALESCE",
        "CONCAT",
        "CONCAT_WS",
        "COS",
        "DATE_ADD",
        "DATE_FORMAT",
        "DATE_SUB",
        "DATEDIFF",
        "DAY",
        "EXP",
        "FLOOR",
        "GREATEST",
        "IF",
        "IFNULL",
        "JSON_EXTRACT",
        "LEAST",
        "LENGTH",
        "LN",
        "LOG",
        "LOWER",
        "LTRIM",
        "MOD",
        "MONTH",
        "NULLIF",
        "POWER",
        "REGEXP_EXTRACT",
        "REPLACE",
        "ROUND",
        "RTRIM",
        "SIN",
        "SQRT",
        "SUBSTR",
        "SUBSTRING",
        "TAN",
        "TO_DATE",
        "TRIM",
        "UPPER",
        "YEAR",
    },
)
_SUPPORTED_FUNCTIONS = _AGGREGATE_FUNCTIONS | _SCALAR_FUNCTIONS
_LOSSY_CLAUSES = ("TABLESAMPLE", "TRY_CAST", "FILTER", "NULLS", "TRIM")
_LOSSY_TOKENS = frozenset(
    getattr(SqlBaseParser, clause) for clause in _LOSSY_CLAUSES if clause != "TRIM"
)
_DimensionLink = DimensionJoinLinkSpec | DimensionReferenceLinkSpec


class _Unsupported(ValueError):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class _BoundSource:
    provider: str
    schema: Mapping[str, ColumnSpec]
    aliases: frozenset[str]
    is_source: bool
    numeric_literals: Mapping[int, str]


def _column_contract(column: ColumnSpec) -> dict[str, object]:
    parsed_type = _parsed_type(column.type)
    return {
        "name": column.name,
        "type": str(parsed_type) if parsed_type else column.type,
        "partition": normalize_value(column.partition),
    }


def _columns_contract(columns: tuple[ColumnSpec, ...]) -> list[dict[str, object]]:
    return [
        _column_contract(column)
        for column in sorted(columns, key=lambda column: column.name)
    ]


def _output_column_contract(
    name: str,
    column: ColumnSpec | None,
) -> dict[str, object]:
    return (
        _column_contract(column)
        if column
        else {
            "name": name,
            "type": None,
            "partition": None,
        }
    )


def _effective_recommended_filters(
    requirements: list[FilterRequirement] | None,
) -> list[FilterRequirement]:
    return [requirement for requirement in requirements or [] if requirement.default]


def _parse_query(spec: NodeSpec) -> ast.Query:
    text = getattr(spec, "rendered_query", None)
    if not text:
        raise _Unsupported("query_missing")
    try:
        query = spec.query_ast
    except (DJParseException, TypeError, ValueError) as exc:
        raise _Unsupported("query_parse_failure") from exc
    if not isinstance(query, ast.Query):
        raise _Unsupported("unsupported_query_shape")
    upper = text.upper()
    if any(clause in upper for clause in _LOSSY_CLAUSES):
        tree = cached_definition_tree(text, "singleStatement")
        if any(
            token.type in _LOSSY_TOKENS for token in tree.parser.getTokenStream().tokens
        ):
            raise _Unsupported("unsupported_query_modifier")
        if "TRIM" in upper:
            pending = [tree]
            while pending:
                node = pending.pop()
                if isinstance(node, SqlBaseParser.TrimContext) and node.trimStr:
                    raise _Unsupported("unsupported_query_modifier")
                pending.extend(getattr(node, "children", None) or ())
    return query


def _parse_opaque_query(spec: NodeSpec) -> ast.Query:
    """Parse SQL for parent discovery while preserving it as authored tokens."""
    text = getattr(spec, "rendered_query", None)
    if not text:
        raise _Unsupported("query_missing")
    try:
        tree = cached_definition_tree(text, "singleStatement")
    except SqlParsingError as exc:
        raise _Unsupported("query_parse_failure") from exc
    tokens = tree.parser.getTokenStream().tokens
    if not any(token.channel == 0 and token.type != -1 for token in tokens):
        raise _Unsupported("query_parse_failure")
    try:
        query = spec.query_ast
    except (DJParseException, TypeError, ValueError) as exc:
        raise _Unsupported("query_parse_failure") from exc
    if not isinstance(query, ast.Query):
        raise _Unsupported("unsupported_query_shape")
    return query


def _opaque_sql_nodes(spec: NodeSpec) -> list[object] | None:
    text = getattr(spec, "rendered_query", None)
    if not text:
        return None
    try:
        tree = cached_definition_tree(text, "singleStatement")
    except SqlParsingError:
        return None
    pending = [tree]
    nodes = []
    while pending:
        node = pending.pop()
        nodes.append(node)
        pending.extend(getattr(node, "children", None) or ())
    return nodes


def _query_contexts(node: object) -> list[SqlBaseParser.QueryContext]:
    contexts = []
    parent = node
    while parent is not None:
        if isinstance(parent, SqlBaseParser.QueryContext):
            contexts.append(parent)
        parent = getattr(parent, "parentCtx", None)
    return contexts


def _visible_cte_names(node: object) -> set[str]:
    declared_names: set[str] = set()
    for query in _query_contexts(node):
        ctes = query.ctes()
        if ctes is not None:
            declared_names.update(
                named_query.name.getText().strip('`"').casefold()
                for named_query in ctes.namedQuery()
                if named_query.name is not None
            )
    return {
        name
        for name in declared_names
        if _cte_reference_status(node, name) == "visible"
    }


def _is_ancestor_context(ancestor: object, node: object) -> bool:
    parent = node
    while parent is not None:
        if parent is ancestor:
            return True
        parent = getattr(parent, "parentCtx", None)
    return False


def _cte_reference_status(node: object, name: str) -> str:
    """Return visible, ambiguous, or absent for a one-part CTE reference."""
    for query in _query_contexts(node):
        ctes = query.ctes()
        if ctes is None:
            continue
        named_queries = ctes.namedQuery()
        matching_indexes = [
            index
            for index, named_query in enumerate(named_queries)
            if named_query.name
            and named_query.name.getText().strip('`"').casefold() == name.casefold()
        ]
        if not matching_indexes:
            continue
        current_index = next(
            (
                index
                for index, named_query in enumerate(named_queries)
                if _is_ancestor_context(named_query, node)
            ),
            None,
        )
        if current_index is None or min(matching_indexes) < current_index:
            return "visible"
        return "ambiguous"
    return "absent"


def _opaque_relation_references(
    spec: NodeSpec,
    nodes: Iterable[object] | None = None,
) -> tuple[set[str], bool]:
    """Read relations from the complete SQL tree, excluding scoped CTE refs."""
    nodes = _opaque_sql_nodes(spec) if nodes is None else list(nodes)
    if nodes is None:
        return set(), True

    relations: set[str] = set()
    for node in nodes:
        if not isinstance(node, SqlBaseParser.TableNameContext):
            continue
        identifier = node.multipartIdentifier()
        if identifier is None:
            return relations, True
        parts = [part.getText().strip('`"') for part in identifier.parts]
        if not parts or any(not part for part in parts):
            return relations, True
        reference = ".".join(parts)
        if len(parts) == 1:
            cte_status = _cte_reference_status(node, parts[0])
            if cte_status == "visible":
                continue
            if cte_status == "ambiguous":
                return relations, True
        relations.add(reference)
    return relations, False


def _opaque_sql_references(
    spec: NodeSpec,
    specs: Mapping[str, NodeSpec],
    schemas: Mapping[str, tuple[ColumnSpec, ...]] | None = None,
) -> tuple[set[str], set[str], bool]:
    """Read captured relation and dimension references from the full SQL tree."""
    nodes = _opaque_sql_nodes(spec)
    if nodes is None:
        return set(), set(), True
    relations, unresolved = _opaque_relation_references(spec, nodes)

    def columns_for(name: str) -> tuple[ColumnSpec, ...] | None:
        if schemas is not None and name in schemas:
            return schemas[name]
        node_spec = specs.get(name)
        if node_spec is None:
            return None
        return _declared_columns(node_spec)

    table_bindings: dict[
        int,
        list[tuple[tuple[str, ...], str, tuple[ColumnSpec, ...] | None]],
    ] = {}
    derived_output_names: dict[str, set[str] | None] = {}

    def explicit_output_names(
        query: SqlBaseParser.QueryContext,
    ) -> set[str] | None:
        names: set[str] = set()
        wildcard = False
        for node in nodes:
            if (
                not isinstance(node, SqlBaseParser.SelectClauseContext)
                or next(iter(_query_contexts(node)), None) is not query
            ):
                continue
            sequence = node.namedExpressionSeq()
            if sequence is None:
                continue
            for expression in sequence.namedExpression():
                if expression.name is not None:
                    names.add(expression.name.getText().strip('`"').casefold())
                elif expression.expression().getText().endswith(".*") or (
                    expression.expression().getText() == "*"
                ):
                    wildcard = True
        return None if wildcard else names

    def add_local_relation_binding(
        query: SqlBaseParser.QueryContext,
        alias: str | None,
        provider_key: str,
        columns: tuple[ColumnSpec, ...] | None,
    ) -> None:
        if not alias:
            return
        table_bindings.setdefault(id(query), []).append(
            ((alias.casefold(),), provider_key, columns),
        )

    for node in nodes:
        if not isinstance(node, SqlBaseParser.TableNameContext):
            continue
        query = next(iter(_query_contexts(node)), None)
        if query is None:
            continue
        identifier = node.multipartIdentifier()
        if identifier is None:
            continue
        parts = [part.getText().strip('`"') for part in identifier.parts]
        if not parts:
            continue
        reference = ".".join(parts)
        cte_reference = len(parts) == 1 and parts[0].casefold() in _visible_cte_names(
            node
        )
        provider = None if cte_reference else _resolve_node_name(reference, specs)
        provider_key = (
            f"cte:{id(query)}:{reference.casefold()}"
            if cte_reference
            else provider or f"external:{reference.casefold()}"
        )
        columns = columns_for(provider) if provider else None
        aliases = {tuple(part.casefold() for part in parts)}
        aliases.add((parts[-1].casefold(),))
        if provider is not None:
            aliases.add(tuple(part.casefold() for part in provider.split(".")))
        table_alias = node.tableAlias()
        if table_alias is not None and table_alias.strictIdentifier() is not None:
            alias = table_alias.strictIdentifier().getText().strip('`"').casefold()
            aliases.add((alias,))
        for alias in aliases:
            table_bindings.setdefault(id(query), []).append(
                (alias, provider_key, columns),
            )

    for node in nodes:
        query = next(iter(_query_contexts(node)), None)
        if query is None:
            continue
        if isinstance(node, SqlBaseParser.LateralViewContext):
            add_local_relation_binding(
                query,
                node.tblName.getText().strip('`"') if node.tblName else None,
                f"lateral:{id(node)}",
                None,
            )
            continue
        if isinstance(
            node,
            (
                SqlBaseParser.AliasedQueryContext,
                SqlBaseParser.AliasedRelationContext,
            ),
        ):
            alias_context = node.tableAlias()
            provider_key = f"derived:{id(node)}"
            if isinstance(node, SqlBaseParser.AliasedQueryContext):
                derived_output_names[provider_key] = explicit_output_names(node.query())
        elif isinstance(node, SqlBaseParser.InlineTableDefault2Context):
            alias_context = node.inlineTable().tableAlias()
            provider_key = f"derived:{id(node)}"
        elif isinstance(node, SqlBaseParser.TableValuedFunctionContext):
            alias_context = node.functionTable().tableAlias()
            provider_key = f"derived:{id(node)}"
        else:
            continue
        alias = (
            alias_context.strictIdentifier().getText().strip('`"')
            if alias_context is not None
            and alias_context.strictIdentifier() is not None
            else None
        )
        add_local_relation_binding(query, alias, provider_key, None)

    def input_binding(path: list[str], node: object) -> tuple[bool, bool]:
        lowered_path = tuple(part.casefold() for part in path)
        for query in _query_contexts(node):
            matches = [
                binding
                for binding in table_bindings.get(id(query), ())
                if len(binding[0]) < len(lowered_path)
                and lowered_path[: len(binding[0])] == binding[0]
            ]
            if not matches:
                continue
            longest = max(len(alias) for alias, _, _ in matches)
            matches = [binding for binding in matches if len(binding[0]) == longest]
            providers = {provider for _, provider, _ in matches}
            if len(providers) != 1:
                return True, False
            columns = matches[0][2]
            if columns is None:
                return True, True
            output_name = lowered_path[longest]
            return True, output_name in {column.name.casefold() for column in columns}
        return False, False

    def struct_binding(path: list[str], node: object) -> bool | None:
        root = path[0].casefold()
        for query in _query_contexts(node):
            providers = {
                provider
                for _, provider, columns in table_bindings.get(id(query), ())
                if columns is not None
                and root in {column.name.casefold() for column in columns}
            }
            for _, provider, _ in table_bindings.get(id(query), ()):
                if provider not in derived_output_names:
                    continue
                outputs = derived_output_names[provider]
                if outputs is None or root in outputs:
                    providers.add(provider)
            if providers:
                return len(providers) == 1
        return None

    def lambda_binding(path: list[str], node: object) -> bool:
        parent = node
        while parent is not None:
            if isinstance(parent, SqlBaseParser.LambdaContext):
                names = {
                    identifier.getText().strip('`"').casefold()
                    for identifier in parent.identifier()
                }
                if path[0].casefold() in names:
                    return True
            parent = getattr(parent, "parentCtx", None)
        return False

    dimension_references: set[str] = set()
    dimension_names: dict[str, set[str]] = {}
    for name, node_spec in specs.items():
        if not isinstance(node_spec, DimensionSpec):
            continue
        canonical_name = name.casefold()
        dimension_names.setdefault(canonical_name, set()).add(name)
        dimension_names.setdefault(name.rsplit(".", 1)[-1].casefold(), set()).add(name)

    def column_path(node: object) -> list[str] | None:
        if isinstance(node, SqlBaseParser.ColumnReferenceContext):
            return [node.identifier().getText().strip('`"')]
        if isinstance(node, SqlBaseParser.DereferenceContext):
            base = node.primaryExpression()
            prefix = column_path(base)
            if prefix is None:
                return None
            return [*prefix, node.identifier().getText().strip('`"')]
        return None

    for node in nodes:
        if not isinstance(node, SqlBaseParser.DereferenceContext):
            continue
        parent = getattr(node, "parentCtx", None)
        if (
            isinstance(parent, SqlBaseParser.DereferenceContext)
            and parent.primaryExpression() is node
        ):
            continue
        path = column_path(node)
        if not path or len(path) < 2:
            continue
        if lambda_binding(path, node):
            continue
        matched_input, input_is_bound = input_binding(path, node)
        if matched_input:
            unresolved = unresolved or not input_is_bound
            continue
        input_struct = struct_binding(path, node)
        if input_struct is not None:
            unresolved = unresolved or not input_struct
            continue
        if path[0].casefold() in _visible_cte_names(node):
            continue

        reference = ".".join(path)
        lowered_reference = reference.casefold()
        matches = [
            (dimension, alias)
            for alias, dimensions in dimension_names.items()
            if lowered_reference.startswith(alias + ".")
            for dimension in dimensions
        ]
        matched_dimensions = {dimension for dimension, _ in matches}
        if len(matched_dimensions) == 1:
            dimension = next(iter(matched_dimensions))
            alias = max(
                (alias for matched, alias in matches if matched == dimension),
                key=lambda candidate: (len(candidate), candidate.casefold()),
            )
            attribute_path = path[len(alias.split(".")) :]
            dimension_columns = columns_for(dimension)
            if (
                not attribute_path
                or dimension_columns is None
                or attribute_path[0].casefold()
                not in {column.name.casefold() for column in dimension_columns}
            ):
                unresolved = True
                continue
            dimension_references.add(f"{dimension}.{'.'.join(attribute_path)}")
        elif len(matched_dimensions) > 1:
            unresolved = True
        elif _resolve_node_name(reference, specs) is None:
            unresolved = True
    return relations, dimension_references, unresolved


def _numeric_literals(query: ast.Query, text: str) -> dict[int, str]:
    numbers = list(query.find_all(ast.Number))
    if not numbers:
        return {}
    literals = []
    pending = [cached_definition_tree(text, "singleStatement")]
    while pending:
        node = pending.pop()
        if isinstance(node, SqlBaseParser.NumericLiteralContext):
            literals.append(node.number().getText())
        else:
            pending.extend(reversed(getattr(node, "children", None) or ()))
    if len(numbers) != len(literals):
        raise _Unsupported("unsupported_numeric_literal")
    return {id(number): literal for number, literal in zip(numbers, literals)}


def _source_columns(
    name: str,
    spec: SourceSpec,
    snapshot: SemanticGraphSnapshot,
) -> tuple[ColumnSpec, ...]:
    if name in snapshot._resolved_columns:
        return snapshot._resolved_columns[name]
    return tuple(spec.columns or ())


def _require_unique_columns(
    columns: tuple[ColumnSpec, ...],
    reason: str,
) -> tuple[ColumnSpec, ...]:
    if len({column.name.casefold() for column in columns}) != len(columns):
        raise _Unsupported(reason)
    return columns


def _declared_columns(spec: NodeSpec) -> tuple[ColumnSpec, ...]:
    return tuple(getattr(spec, "columns", None) or ())


def _projection_name(
    expression: ast.Node,
    index: int,
    declared: tuple[ColumnSpec, ...],
) -> str:
    alias = getattr(expression, "alias", None)
    if alias is not None:
        return alias.name
    if isinstance(expression, ast.Column):
        return expression.name.name
    return declared[index].name if index < len(declared) else f"col{index}"


def build_node_component_keys(
    output_names: Mapping[str, tuple[str, ...]],
) -> dict[str, dict[str, Any]]:
    """Create the stable component addresses used by the graph evaluator."""
    return {
        node: {
            "outputs": {name: ComponentKey(node, "output", name) for name in names},
            "rows": ComponentKey(node, "rows"),
            "full": ComponentKey(node, "full"),
        }
        for node, names in output_names.items()
    }


def _node_contract(
    spec: NodeSpec,
    columns: tuple[ColumnSpec, ...] | None = None,
    *,
    include_dimension_links: bool = True,
) -> dict[str, object]:
    def link_contract(link: object) -> dict[str, object]:
        if isinstance(link, DimensionJoinLinkSpec):
            return {
                "type": link.type.value,
                "role": link.role,
                "dimension_node": link.rendered_dimension_node,
                "join_type": link.join_type.value,
                "join_cardinality": link.join_cardinality.value,
                "join_on": link.rendered_join_on,
                "node_column": link.node_column,
                "default_value": (
                    {
                        "type": type(link.default_value).__name__,
                        "value": link.default_value,
                    }
                    if link.default_value is not None
                    else None
                ),
            }
        if isinstance(link, DimensionReferenceLinkSpec):
            return {
                "type": link.type.value,
                "role": link.role,
                "dimension_node": link.rendered_dimension_node,
                "dimension_attribute": link.dimension_attribute,
                "node_column": link.node_column,
            }
        raise TypeError(f"Unsupported dimension link: {type(link).__name__}")

    contract: dict[str, object] = {"node_type": spec.node_type.value}
    if columns is not None:
        contract["columns"] = _columns_contract(columns)
    if isinstance(spec, SourceSpec):
        contract.update(
            catalog=spec.catalog,
            schema=spec.schema_,
            table=spec.table,
            primary_key=sorted(spec.primary_key or []),
        )
    elif isinstance(spec, LinkableNodeSpec):
        contract["primary_key"] = sorted(spec.primary_key or [])
    if include_dimension_links and isinstance(spec, LinkableNodeSpec):
        contract["dimension_links"] = sorted(
            (link_contract(link) for link in spec.dimension_links),
            key=canonical_json,
        )
    if isinstance(spec, MetricSpec):
        contract.update(
            required_dimensions=sorted(spec.rendered_required_dimensions),
            fixed_grain=(
                sorted(spec.rendered_fixed_grain)
                if spec.rendered_fixed_grain is not None
                else None
            ),
            reaggregate=normalize_value(spec.rendered_reaggregate),
            required_filters=normalize_value(spec.rendered_required_filters),
            recommended_filters=normalize_value(
                _effective_recommended_filters(spec.rendered_recommended_filters),
            ),
        )
    if isinstance(spec, CubeSpec):
        contract.update(
            metrics=list(spec.rendered_metrics),
            dimensions=list(spec.rendered_dimensions),
            filters=sorted(spec.rendered_filters),
        )
    return contract


def _resolve_node_name(reference: str, specs: Mapping[str, NodeSpec]) -> str | None:
    if reference in specs:
        return reference
    if "." in reference:
        return None
    matches = [name for name in specs if name.rsplit(".", 1)[-1] == reference]
    return matches[0] if len(matches) == 1 else None


def _add_parent_reference(
    reference: str,
    specs: Mapping[str, NodeSpec],
    parents: set[str],
) -> bool:
    """Resolve a node or node-column reference, returning whether it is missing."""
    path = reference.split("[", 1)[0].strip()
    resolved = _resolve_node_name(path, specs)
    if resolved is not None:
        parents.add(resolved)
        return False
    parts = path.split(".")
    for end in range(len(parts) - 1, 1, -1):
        resolved = _resolve_node_name(".".join(parts[:end]), specs)
        if resolved is not None:
            parents.add(resolved)
            return False
    return True


def _add_filter_parents(
    filters: list[str],
    specs: Mapping[str, NodeSpec],
    parents: set[str],
    *,
    require_qualified_columns: bool,
) -> bool:
    """Collect dimension references in filters or report missing evidence."""
    unresolved = False
    for filter_expression in filters:
        try:
            tree = parse(f"SELECT 1 WHERE ({filter_expression})")
        except (DJParseException, TypeError, ValueError):
            unresolved = True
            continue
        for column in tree.find_all(ast.Column):
            if not column.namespace:
                unresolved = unresolved or require_qualified_columns
                continue
            namespace = ".".join(part.name for part in column.namespace)
            unresolved = _add_parent_reference(namespace, specs, parents) or unresolved
    return unresolved


def _bound_struct_references(
    query: ast.Query,
    specs: Mapping[str, NodeSpec],
    schemas: Mapping[str, tuple[ColumnSpec, ...]] | None,
) -> set[str]:
    """Find qualified struct paths whose first field belongs to an input schema."""
    cte_names = {cte.alias_or_name.identifier().casefold() for cte in query.ctes}
    relations: list[tuple[set[tuple[str, ...]], set[str]]] = []
    for table in query.find_all(ast.Table):
        reference = table.identifier(quotes=False)
        if table.name.identifier().casefold() in cte_names:
            continue
        provider = _resolve_node_name(reference, specs)
        if provider is None:
            continue
        columns = (schemas or {}).get(provider)
        if columns is None:
            provider_spec = specs[provider]
            columns = (
                tuple(provider_spec.columns or ())
                if isinstance(provider_spec, SourceSpec)
                else _declared_columns(provider_spec)
            )
        column_names = {column.name.casefold() for column in columns}
        aliases = {
            tuple(alias.casefold().split("."))
            for alias in (reference, provider, table.name.name)
        }
        if table.alias is not None:
            aliases.add((table.alias.name.casefold(),))
        relations.append((aliases, column_names))

    bound: set[str] = set()
    for column in query.find_all(ast.Column):
        if not column.namespace:
            continue
        parts = tuple(part.name.casefold() for part in column.namespace)
        candidate = ".".join(part.name for part in column.namespace)
        if _resolve_node_name(candidate, specs) is not None:
            continue
        for aliases, column_names in relations:
            if any(
                len(alias) < len(parts)
                and parts[: len(alias)] == alias
                and parts[len(alias)] in column_names
                for alias in aliases
            ):
                bound.add(candidate)
                break
        else:
            if len(relations) == 1 and parts[0] in relations[0][1]:
                bound.add(candidate)
            elif sum(parts[0] in names for _, names in relations) == 1:
                bound.add(candidate)
    return bound


def _fallback_parent_names(
    spec: NodeSpec,
    specs: Mapping[str, NodeSpec],
    query: ast.Query | None,
    schemas: Mapping[str, tuple[ColumnSpec, ...]] | None = None,
    opaque_relations: Iterable[str] = (),
    opaque_dimension_references: Iterable[str] = (),
    opaque_references_unresolved: bool = False,
) -> tuple[set[str], bool]:
    """Find conservative parents and flag references that are not captured."""
    parents: set[str] = set()
    unresolved = opaque_references_unresolved
    for reference in opaque_relations:
        unresolved = _add_parent_reference(reference, specs, parents) or unresolved

    if isinstance(spec, CubeSpec):
        for reference in [*spec.rendered_metrics, *spec.rendered_dimensions]:
            unresolved = _add_parent_reference(reference, specs, parents) or unresolved
        unresolved = (
            _add_filter_parents(
                spec.rendered_filters,
                specs,
                parents,
                require_qualified_columns=True,
            )
            or unresolved
        )
        return parents, unresolved

    if query is not None:
        try:
            from datajunction_server.internal.deployment.utils import (
                extract_dimension_attribute_nodes,
                extract_upstream_candidates,
            )

            candidates = extract_upstream_candidates(
                query,
                is_metric=isinstance(spec, MetricSpec),
            )
            bound_structs = _bound_struct_references(query, specs, schemas)
            if query.select.from_ is not None:
                candidates.update(extract_dimension_attribute_nodes(query))
            candidates.update(opaque_dimension_references)
            for candidate in candidates:
                if candidate in bound_structs:
                    continue
                unresolved = (
                    _add_parent_reference(candidate, specs, parents) or unresolved
                )
        except (DJParseException, TypeError, ValueError):
            unresolved = True

    if isinstance(spec, MetricSpec):
        requirements = [
            *(spec.rendered_required_filters or []),
            *_effective_recommended_filters(spec.rendered_recommended_filters),
        ]
        references = [
            *spec.rendered_required_dimensions,
            *(spec.rendered_fixed_grain or []),
            *(reference for item in requirements for reference in item.dimension),
            *(
                rule.dimension
                for rule in (
                    spec.rendered_reaggregate.rules if spec.rendered_reaggregate else []
                )
            ),
        ]
        for reference in references:
            unresolved = _add_parent_reference(reference, specs, parents) or unresolved
            if (
                "." not in reference
                and query is not None
                and query.select.from_ is None
            ):
                unresolved = True
        defaults = [item.default for item in requirements if item.default]
        unresolved = (
            _add_filter_parents(
                defaults,
                specs,
                parents,
                require_qualified_columns=False,
            )
            or unresolved
        )
    return parents, unresolved


def _opaque_dimension_output_dependencies(
    references: Iterable[str],
    specs: Mapping[str, NodeSpec],
) -> list[tuple[str, ComponentKey]]:
    dependencies = set()
    for reference in references:
        matching_dimensions = [
            name
            for name, node_spec in specs.items()
            if isinstance(node_spec, DimensionSpec)
            and reference.casefold().startswith(name.casefold() + ".")
        ]
        if not matching_dimensions:
            continue
        dimension = max(matching_dimensions, key=lambda name: (len(name), name))
        attribute_path = reference[len(dimension) + 1 :].split(".")
        if attribute_path:
            attribute = attribute_path[0]
            dependencies.add(
                (
                    f"opaque_dimension_output:{dimension}:{attribute}",
                    ComponentKey(dimension, "output", attribute),
                ),
            )
    return sorted(dependencies)


def _link_target_dependencies(
    spec: NodeSpec,
    specs: Mapping[str, NodeSpec],
) -> list[tuple[str, ComponentKey]]:
    if not isinstance(spec, LinkableNodeSpec):
        return []
    dependencies = []
    for link in spec.dimension_links:
        reference = link.rendered_dimension_node
        target = _resolve_node_name(reference, specs) or reference
        dependencies.append(
            (f"dimension_link:{target}", ComponentKey(target, "calculation")),
        )
    return sorted(dependencies)


def _implicit_dimension_link_context(
    query: ast.Query | None,
    specs: Mapping[str, NodeSpec],
    references: Iterable[str] = (),
    relation_references: Iterable[str] = (),
    relation_cache: dict[str, tuple[frozenset[str], bool]] | None = None,
    ancestry_cache: dict[str, frozenset[str] | None] | None = None,
) -> tuple[list[dict[str, object]], list[tuple[str, ComponentKey]], bool]:
    if query is None:
        return [], [], False
    from datajunction_server.internal.deployment.utils import (
        extract_dimension_attribute_nodes,
    )

    table_nodes = {
        node
        for table in query.find_all(ast.Table)
        if (node := _resolve_node_name(table.identifier(quotes=False), specs))
    }
    targets = {
        node
        for reference in extract_dimension_attribute_nodes(query)
        if (node := _resolve_node_name(reference, specs))
        and isinstance(specs.get(node), DimensionSpec)
        and node not in table_nodes
    }
    for reference in references:
        attribute = reference.split("[", 1)[0]
        parts = attribute.split(".")
        if len(parts) < 2:
            continue
        node = _resolve_node_name(".".join(parts[:-1]), specs)
        if (
            node
            and isinstance(specs.get(node), DimensionSpec)
            and node not in table_nodes
        ):
            targets.add(node)
    if not targets:
        return [], [], False

    relation_cache = relation_cache if relation_cache is not None else {}
    ancestry_cache = ancestry_cache if ancestry_cache is not None else {}
    active: set[str] = set()

    def ancestors(node: str) -> frozenset[str] | None:
        if node in ancestry_cache:
            return ancestry_cache[node]
        if node in active:
            return None
        spec = specs.get(node)
        if isinstance(spec, SourceSpec):
            ancestry_cache[node] = frozenset({node})
            return ancestry_cache[node]
        if spec is None:
            ancestry_cache[node] = None
            return None
        if node not in relation_cache:
            captured_parents, unresolved = _opaque_relation_references(spec)
            relation_cache[node] = (frozenset(captured_parents), unresolved)
        parents, unresolved = relation_cache[node]
        if unresolved:
            ancestry_cache[node] = None
            return None
        active.add(node)
        result = {node}
        for reference in sorted(parents):
            parent = _resolve_node_name(reference, specs)
            upstream = ancestors(parent) if parent else None
            if upstream is None:
                active.remove(node)
                ancestry_cache[node] = None
                return None
            result.update(upstream)
        active.remove(node)
        ancestry_cache[node] = frozenset(result)
        return ancestry_cache[node]

    route_sources: set[str] = set()
    complete_ancestry = True
    for reference in sorted(set(relation_references)):
        node = _resolve_node_name(reference, specs)
        upstream = ancestors(node) if node else None
        if upstream is None:
            complete_ancestry = False
        else:
            route_sources.update(upstream)
    if not complete_ancestry or not route_sources:
        return [], [], True

    outgoing: dict[str, list[tuple[str, str, _DimensionLink]]] = {}
    incoming: dict[str, list[str]] = {}
    for source, source_spec in specs.items():
        if not isinstance(source_spec, LinkableNodeSpec):
            continue
        for link in source_spec.dimension_links:
            target = _resolve_node_name(link.rendered_dimension_node, specs)
            if target is None:
                continue
            edge = (source, target, link)
            outgoing.setdefault(source, []).append(edge)
            incoming.setdefault(target, []).append(source)

    reachable = set(route_sources)
    queue = list(sorted(route_sources))
    while queue:
        source = queue.pop(0)
        for _, target, _ in outgoing.get(source, ()):
            if target not in reachable:
                reachable.add(target)
                queue.append(target)

    relevant: dict[tuple[str, str, str], tuple[str, str, _DimensionLink]] = {}
    dependencies: set[tuple[str, ComponentKey]] = set()
    contracts: list[dict[str, object]] = []
    missing_targets = targets - reachable
    for target in sorted(missing_targets):
        contracts.append(
            {"source_nodes": sorted(route_sources), "unresolved_target": target},
        )
        dependencies.add(
            (f"implicit_dimension:{target}", ComponentKey(target, "calculation")),
        )
    for target in sorted(targets & reachable):
        can_reach = {target}
        queue = [target]
        while queue:
            node = queue.pop(0)
            for source in incoming.get(node, ()):
                if source not in can_reach:
                    can_reach.add(source)
                    queue.append(source)
        for source, next_node, link in (
            edge
            for parent in reachable
            for edge in outgoing.get(parent, ())
            if edge[1] in can_reach
        ):
            link_key = canonical_json(normalize_value(link._comparison_key()))
            relevant[(source, next_node, link_key)] = (source, next_node, link)
            dependencies.add(
                (f"implicit_link_source:{source}", ComponentKey(source, "calculation")),
            )
            dependencies.add(
                (
                    f"implicit_link_target:{next_node}",
                    ComponentKey(next_node, "calculation"),
                ),
            )

    contracts.extend(
        {
            "source": source,
            "target": target,
            "link": normalize_value(link._comparison_key()),
        }
        for source, target, link in (relevant[key] for key in sorted(relevant))
    )
    return contracts, sorted(dependencies), bool(missing_targets)


def _metric_context_references(spec: NodeSpec) -> tuple[str, ...]:
    if not isinstance(spec, MetricSpec):
        return ()
    references = {
        *spec.rendered_required_dimensions,
        *(spec.rendered_fixed_grain or []),
        *(
            reference
            for requirement in [
                *(spec.rendered_required_filters or []),
                *_effective_recommended_filters(spec.rendered_recommended_filters),
            ]
            for reference in requirement.dimension
        ),
        *(
            rule.dimension
            for rule in (
                spec.rendered_reaggregate.rules if spec.rendered_reaggregate else []
            )
        ),
    }
    requirements = [
        *(spec.rendered_required_filters or []),
        *_effective_recommended_filters(spec.rendered_recommended_filters),
    ]
    for requirement in requirements:
        if not requirement.default:
            continue
        try:
            query = parse(f"SELECT 1 WHERE ({requirement.default})")
        except (DJParseException, TypeError, ValueError):
            continue
        references.update(
            ".".join(
                [*(part.name for part in column.namespace or ()), column.name.name],
            )
            for column in query.find_all(ast.Column)
            if column.namespace
        )
    return tuple(sorted(references))


def _calculation_definition(
    name: str,
    spec: NodeSpec,
    output_names: tuple[str, ...],
    rows: ComponentKey,
    columns: tuple[ColumnSpec, ...],
) -> ComponentDefinition:
    dependencies = [
        ("rows", rows),
        *(
            (f"output:{index}:{output}", ComponentKey(name, "output", output))
            for index, output in enumerate(output_names)
        ),
    ]
    contract_columns = columns if isinstance(spec, SourceSpec) else None
    return _definition(
        ComponentKey(name, "calculation"),
        {
            "node": _node_contract(
                spec,
                contract_columns,
                include_dimension_links=False,
            ),
            "output_layout": [
                _output_column_contract(
                    output,
                    next((column for column in columns if column.name == output), None),
                )
                for output in output_names
            ],
        },
        dependencies,
    )


def _opaque_query_tokens(text: str) -> list[str]:
    tree = cached_definition_tree(text, "singleStatement")
    return [
        token.text
        for token in tree.parser.getTokenStream().tokens
        if token.channel == 0 and token.type != -1
    ]


def _query_schema(
    name: str,
    spec: NodeSpec,
    snapshot: SemanticGraphSnapshot,
    specs: Mapping[str, NodeSpec],
    cache: dict[str, tuple[ColumnSpec, ...]],
) -> tuple[ColumnSpec, ...]:
    if name in cache:
        return cache[name]
    if isinstance(spec, SourceSpec):
        columns = _require_unique_columns(
            _source_columns(name, spec, snapshot),
            "duplicate_source_column_schema",
        )
    else:
        query = _parse_query(spec)
        declared = _declared_columns(spec)
        columns = tuple(
            ColumnSpec(
                name=output,
                type=next(
                    (column.type for column in declared if column.name == output),
                    None,
                ),
            )
            for index, expression in enumerate(query.select.projection)
            for output in [
                amenable_name(name)
                if isinstance(spec, MetricSpec)
                else _projection_name(expression, index, declared)
            ]
        )
    _require_unique_columns(columns, "duplicate_provider_output")
    cache[name] = columns
    return columns


def _bind_source(
    query: ast.Query,
    specs: Mapping[str, NodeSpec],
    snapshot: SemanticGraphSnapshot,
    schemas: dict[str, tuple[ColumnSpec, ...]],
    numeric_literals: Mapping[int, str],
) -> _BoundSource:
    if query.ctes or not isinstance(query.select, ast.Select):
        raise _Unsupported("unsupported_query_shape")
    from_clause = query.select.from_
    if from_clause is None or len(from_clause.relations) != 1:
        raise _Unsupported("unsupported_from_expression")
    relation = from_clause.relations[0]
    table = relation.primary
    if not isinstance(table, ast.Table):
        raise _Unsupported("unsupported_from_expression")
    if relation.extensions:
        raise _Unsupported("unsupported_join")
    if table.column_list or table.path or table.dimension_link:
        raise _Unsupported("unsupported_table_modifier")
    reference = table.identifier(quotes=False)
    provider = _resolve_node_name(reference, specs)
    if provider is None:
        raise _Unsupported("unresolved_source")
    schema = _query_schema(provider, specs[provider], snapshot, specs, schemas)
    aliases = {table.name.name, reference, provider}
    if table.alias is not None:
        aliases.add(table.alias.name)
    return _BoundSource(
        provider,
        {column.name: column for column in schema},
        frozenset(aliases),
        isinstance(specs[provider], SourceSpec),
        numeric_literals,
    )


def _bind_expression(
    expression: ast.Node,
    source: _BoundSource,
) -> tuple[object, list[tuple[str, ComponentKey]], bool]:
    dependencies: dict[str, ComponentKey] = {}
    aggregate = False
    numeric_literals = []
    for node in expression.flatten():
        if isinstance(
            node,
            (ast.Query, ast.Select, ast.Over, ast.Subscript, ast.Lambda),
        ):
            raise _Unsupported("unsupported_expression_shape")
        if isinstance(node, ast.Function):
            function_name = node.name.name.upper()
            if node.name.namespace or function_name not in _SUPPORTED_FUNCTIONS:
                raise _Unsupported("unsupported_function")
            aggregate = aggregate or function_name in _AGGREGATE_FUNCTIONS
        if isinstance(node, ast.Number):
            numeric_literals.append(source.numeric_literals[id(node)])
        if isinstance(node, ast.Column):
            qualifier = ".".join(part.name for part in node.namespace)
            if node.role or node.dimension_ref:
                raise _Unsupported("unsupported_expression_shape")
            if qualifier and qualifier not in source.aliases:
                raise _Unsupported("unresolved_or_ambiguous_column")
            name = node.name.name
            if name not in source.schema:
                raise _Unsupported("resolved_column_schema_missing")
            if source.is_source and _parsed_type(source.schema[name].type) is None:
                raise _Unsupported("source_column_type_unavailable")
            dependencies[name] = ComponentKey(source.provider, "output", name)

    def canonicalize(value: Any) -> Any:
        if isinstance(value, list):
            return [canonicalize(item) for item in value]
        if not isinstance(value, dict) or "fields" not in value:
            return value
        fields = value["fields"]
        for field in (
            "alias",
            "as_",
            "parenthesized",
            "semantic_entity",
            "semantic_type",
            "args_compiled",
        ):
            fields.pop(field, None)
        if value["type"] in {"Column", "Wildcard"}:
            fields["name"]["fields"]["namespace"] = None
        if value["type"] == "Name" and not fields.get("quote_style"):
            fields["name"] = fields["name"].lower()
        value["fields"] = {
            name: canonicalize(field_value)
            for name, field_value in fields.items()
            if field_value is not None and field_value != "" and field_value != []
        }
        return value

    return (
        {
            "ast": canonicalize(serialize_ast(expression, version=1)),
            "numeric_literals": numeric_literals,
        },
        [(f"column:{name}", key) for name, key in sorted(dependencies.items())],
        aggregate,
    )


def _definition(
    key: ComponentKey,
    payload: object,
    dependencies: Iterable[tuple[str, ComponentKey]] = (),
    *,
    precision: str = "precise",
    reasons: tuple[str, ...] = (),
    unavailable: bool = False,
) -> ComponentDefinition:
    return ComponentDefinition(
        key=key,
        payload=payload,
        dependencies=tuple(dependencies),
        precision=precision,  # type: ignore[arg-type]
        reasons=reasons,
        unavailable=unavailable,
    )


def _source_definitions(
    name: str,
    spec: SourceSpec,
    snapshot: SemanticGraphSnapshot,
    names: tuple[str, ...],
) -> list[ComponentDefinition]:
    columns = _source_columns(name, spec, snapshot)
    schema = {column.name: column for column in columns}
    definitions = [
        _definition(
            ComponentKey(name, "rows"),
            {
                "source": [spec.catalog, spec.schema_, spec.table],
                "grain": sorted(spec.primary_key or []),
            },
        ),
    ]
    for output in names:
        column = schema.get(output)
        parsed_type = _parsed_type(column.type) if column else None
        definitions.append(
            _definition(
                ComponentKey(name, "output", output),
                _output_column_contract(output, column),
                precision="precise" if parsed_type else "unknown",
                reasons=() if parsed_type else ("source_column_type_unavailable",),
                unavailable=parsed_type is None,
            ),
        )
    definitions.append(
        _definition(
            ComponentKey(name, "full"),
            {
                "node": _node_contract(spec, columns),
                "output_layout": [
                    _output_column_contract(output, schema.get(output))
                    for output in names
                ],
            },
            [
                ("rows", ComponentKey(name, "rows")),
                *(
                    (f"output:{index}:{output}", ComponentKey(name, "output", output))
                    for index, output in enumerate(names)
                ),
            ],
        ),
    )
    definitions.append(
        _calculation_definition(
            name,
            spec,
            names,
            ComponentKey(name, "rows"),
            columns,
        ),
    )
    return definitions


def _opaque_definitions(
    name: str,
    spec: NodeSpec,
    output_names: tuple[str, ...],
    specs: Mapping[str, NodeSpec],
    reason: str,
    snapshot: SemanticGraphSnapshot,
    schema_cache: Mapping[str, tuple[ColumnSpec, ...]] | None = None,
    relation_cache: dict[str, tuple[frozenset[str], bool]] | None = None,
    ancestry_cache: dict[str, frozenset[str] | None] | None = None,
) -> list[ComponentDefinition]:
    query = None
    opaque_relations: set[str] = set()
    opaque_dimension_references: set[str] = set()
    opaque_references_unresolved = False
    unresolved = isinstance(spec, CubeSpec) or reason in {
        "resolved_column_schema_missing",
        "source_column_type_unavailable",
        "duplicate_source_column_schema",
        "duplicate_provider_output",
    }
    if not isinstance(spec, CubeSpec):
        try:
            query = _parse_opaque_query(spec)
            (
                opaque_relations,
                opaque_dimension_references,
                opaque_references_unresolved,
            ) = _opaque_sql_references(spec, specs, schema_cache)
            if relation_cache is not None:
                relation_cache[name] = (
                    frozenset(opaque_relations),
                    opaque_references_unresolved,
                )
        except (_Unsupported, DJParseException, SqlParsingError, TypeError, ValueError):
            unresolved = True
        if query is None or not isinstance(query, ast.Query):
            unresolved = True
    implicit_links, implicit_link_dependencies, unresolved_implicit_links = (
        _implicit_dimension_link_context(
            query,
            specs,
            (*_metric_context_references(spec), *opaque_dimension_references),
            opaque_relations,
            relation_cache,
            ancestry_cache,
        )
    )
    parents, missing_parent = _fallback_parent_names(
        spec,
        specs,
        query,
        schema_cache,
        opaque_relations,
        opaque_dimension_references,
        opaque_references_unresolved,
    )
    unresolved = unresolved or missing_parent or unresolved_implicit_links
    local_query: object = getattr(spec, "rendered_query", None)
    if query is not None:
        local_query = _opaque_query_tokens(spec.rendered_query or "")
    opaque = ComponentKey(name, "opaque")
    columns = (
        _source_columns(name, spec, snapshot)
        if isinstance(spec, SourceSpec)
        else tuple(spec.matched_rendered_columns)
        if isinstance(spec, CubeSpec)
        else _declared_columns(spec)
    )
    reasons = tuple(
        sorted(
            {reason} | ({"unresolved_parent_dependency"} if unresolved else set()),
        ),
    )
    precision = "unknown" if unresolved else "conservative"
    definitions = [
        _definition(
            opaque,
            {
                "node": _node_contract(
                    spec,
                    columns,
                    include_dimension_links=False,
                ),
                "query": local_query,
                "implicit_dimension_links": implicit_links,
            },
            [
                (f"opaque_parent:{parent}", ComponentKey(parent, "calculation"))
                for parent in sorted(parents)
            ]
            + _opaque_dimension_output_dependencies(opaque_dimension_references, specs)
            + implicit_link_dependencies,
            precision=precision,
            reasons=reasons,
            unavailable=unresolved,
        ),
        _definition(
            ComponentKey(name, "rows"),
            {"opaque_component": opaque.token},
            [("opaque", opaque)],
            precision=precision,
            reasons=reasons,
            unavailable=unresolved,
        ),
    ]
    definitions.extend(
        _definition(
            ComponentKey(name, "output", output),
            {"opaque_component": opaque.token, "output": output},
            [("opaque", opaque)],
            precision=precision,
            reasons=reasons,
            unavailable=unresolved,
        )
        for output in output_names
    )
    definitions.append(
        _definition(
            ComponentKey(name, "full"),
            {
                "node": _node_contract(spec, columns),
                "opaque_component": opaque.token,
                "output_layout": list(output_names),
            },
            [
                ("opaque", opaque),
                *_link_target_dependencies(spec, specs),
            ],
            precision=precision,
            reasons=reasons,
            unavailable=unresolved,
        ),
    )
    definitions.append(
        _calculation_definition(
            name,
            spec,
            output_names,
            ComponentKey(name, "rows"),
            columns,
        ),
    )
    return definitions


def _query_definitions(
    name: str,
    spec: NodeSpec,
    snapshot: SemanticGraphSnapshot,
    specs: Mapping[str, NodeSpec],
    output_names: tuple[str, ...],
    schemas: dict[str, tuple[ColumnSpec, ...]],
) -> list[ComponentDefinition]:
    if isinstance(spec, MetricSpec) and (
        spec.rendered_required_dimensions
        or spec.rendered_fixed_grain
        or spec.rendered_required_filters
        or _effective_recommended_filters(spec.rendered_recommended_filters)
        or spec.rendered_reaggregate
    ):
        raise _Unsupported("metric_dimension_context_requires_trace")
    query = _parse_query(spec)
    select = query.select
    if (
        not isinstance(select, ast.Select)
        or any(
            getattr(select, field)
            for field in (
                "quantifier",
                "group_by",
                "having",
                "lateral_views",
                "set_op",
                "limit",
                "offset",
                "hints",
            )
        )
        or (
            select.organization is not None
            and (select.organization.order or select.organization.sort)
        )
    ):
        raise _Unsupported("unsupported_query_shape")
    source = _bind_source(
        query,
        specs,
        snapshot,
        schemas,
        _numeric_literals(query, spec.rendered_query or ""),
    )
    if not output_names or len(output_names) != len(select.projection):
        raise _Unsupported("duplicate_output_name")
    if any(isinstance(expression, ast.Wildcard) for expression in select.projection):
        raise _Unsupported("star_projection")
    if len(set(output_names)) != len(output_names):
        raise _Unsupported("duplicate_output_name")

    outputs = {output: ComponentKey(name, "output", output) for output in output_names}
    where = select.where
    row_dependencies: dict[tuple[str, str, str, str], tuple[str, ComponentKey]] = {
        ("source_rows", source.provider, "rows", ""): (
            "source_rows",
            ComponentKey(source.provider, "rows"),
        ),
    }
    where_shape = None
    if where is not None:
        where_shape, dependencies, _ = _bind_expression(where, source)
        for label, key in dependencies:
            row_dependencies[(label, key.node, key.kind, key.field)] = (label, key)
    input_rows = ComponentKey(name, "rows", "input")
    aggregate_query = False
    output_definitions = []
    for output, projection in zip(output_names, select.projection):
        expression = (
            projection.child if isinstance(projection, ast.Alias) else projection
        )
        expression_shape, dependencies, aggregate = _bind_expression(expression, source)
        aggregate_query = aggregate_query or aggregate
        output_dependencies = list(dependencies)
        if aggregate:
            output_dependencies.append(("input_rows", input_rows))
        declared = next(
            (column for column in _declared_columns(spec) if column.name == output),
            None,
        )
        output_definitions.append(
            _definition(
                outputs[output],
                {
                    "expression": expression_shape,
                    "column": _output_column_contract(output, declared),
                },
                output_dependencies,
            ),
        )

    row_payload = {
        "source": source.provider,
        "where": where_shape,
        "grain": "global_aggregate" if aggregate_query else "source_rows",
    }
    definitions = [
        _definition(input_rows, {"where": where_shape}, row_dependencies.values()),
        *output_definitions,
        _definition(
            ComponentKey(name, "rows"),
            row_payload,
            [("input_rows", input_rows)],
        ),
        _definition(
            ComponentKey(name, "full"),
            {
                "node": _node_contract(spec),
                "output_layout": [
                    _output_column_contract(
                        output,
                        next(
                            (
                                column
                                for column in _declared_columns(spec)
                                if column.name == output
                            ),
                            None,
                        ),
                    )
                    for output in output_names
                ],
            },
            [
                ("rows", ComponentKey(name, "rows")),
                *(
                    (f"output:{index}:{output}", outputs[output])
                    for index, output in enumerate(output_names)
                ),
            ],
        ),
        _calculation_definition(
            name,
            spec,
            output_names,
            ComponentKey(name, "rows"),
            schemas[name],
        ),
    ]
    return definitions


def build_component_definitions(
    snapshot: SemanticGraphSnapshot,
) -> tuple[
    dict[str, ComponentDefinition],
    dict[str, tuple[str, ...]],
    dict[str, tuple[str, ...]],
]:
    """Extract v2 component recipes from captured specifications and schemas."""
    specs = snapshot._specs
    schemas: dict[str, tuple[ColumnSpec, ...]] = {}
    output_names: dict[str, tuple[str, ...]] = {}
    reasons_by_node: dict[str, tuple[str, ...]] = {}
    for name, spec in specs.items():
        try:
            if isinstance(spec, SourceSpec):
                schema = _require_unique_columns(
                    _source_columns(name, spec, snapshot),
                    "duplicate_source_column_schema",
                )
                names = tuple(column.name for column in schema)
            else:
                schema = _query_schema(name, spec, snapshot, specs, schemas)
                names = tuple(column.name for column in schema)
            schemas[name] = schema
            output_names[name] = names
        except (_Unsupported, DJParseException, TypeError, ValueError) as exc:
            columns = _declared_columns(spec)
            schemas[name] = columns
            output_names[name] = tuple(column.name for column in columns)
            reason = (
                exc.reason if isinstance(exc, _Unsupported) else "query_parse_failure"
            )
            reasons_by_node[name] = (reason,)

    definitions: dict[str, ComponentDefinition] = {}
    relation_cache: dict[str, tuple[frozenset[str], bool]] = {}
    ancestry_cache: dict[str, frozenset[str] | None] = {}
    for name, spec in specs.items():
        names = output_names[name]
        if name in reasons_by_node:
            node_definitions = _opaque_definitions(
                name,
                spec,
                names,
                specs,
                reasons_by_node[name][0],
                snapshot,
                schemas,
                relation_cache,
                ancestry_cache,
            )
        elif isinstance(spec, SourceSpec):
            node_definitions = _source_definitions(name, spec, snapshot, names)
        elif isinstance(spec, CubeSpec):
            node_definitions = _opaque_definitions(
                name,
                spec,
                names,
                specs,
                "cube_dependency_context_unsupported",
                snapshot,
                schemas,
                relation_cache,
                ancestry_cache,
            )
        else:
            try:
                node_definitions = _query_definitions(
                    name,
                    spec,
                    snapshot,
                    specs,
                    names,
                    schemas,
                )
            except _Unsupported as exc:
                reasons_by_node[name] = (exc.reason,)
                node_definitions = _opaque_definitions(
                    name,
                    spec,
                    names,
                    specs,
                    exc.reason,
                    snapshot,
                    schemas,
                    relation_cache,
                    ancestry_cache,
                )
            except (DJParseException, TypeError, ValueError) as exc:
                reason = (
                    "query_parse_failure"
                    if isinstance(exc, DJParseException)
                    else "unsupported_query_shape"
                )
                reasons_by_node[name] = (reason,)
                node_definitions = _opaque_definitions(
                    name,
                    spec,
                    names,
                    specs,
                    reason,
                    snapshot,
                    schemas,
                    relation_cache,
                    ancestry_cache,
                )
        for definition in node_definitions:
            definitions[definition.key.token] = definition
    return definitions, output_names, reasons_by_node
