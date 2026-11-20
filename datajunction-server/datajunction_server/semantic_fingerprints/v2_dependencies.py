"""Narrow output and row dependencies for semantic fingerprint version 2."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from functools import lru_cache
from typing import Any, Literal

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
from datajunction_server.sql.parsing.types import ColumnType, StructType

_AGGREGATE_FUNCTIONS = frozenset(
    {
        "ANY_VALUE",
        "APPROX_COUNT_DISTINCT",
        "APPROX_PERCENTILE",
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
        "NF_DATEADD",
        "NF_DATEDIFF",
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
_MAX_CTE_EXPANDED_NODES = 4096
_MAX_CTE_COUNT = 32
_DimensionLink = DimensionJoinLinkSpec | DimensionReferenceLinkSpec
_JoinColumnCache = dict[
    tuple[str, str, str, str],
    tuple[frozenset[tuple[str, str]], bool],
]


@lru_cache(maxsize=512)
def _join_column_type(value: str) -> ColumnType | None:
    try:
        return ColumnType.validate(value)
    except Exception:
        return None


class _Unsupported(ValueError):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class _BoundOutput:
    expression: object
    dependencies: tuple[tuple[str, ComponentKey], ...]
    expansion_cost: int


@dataclass(frozen=True)
class _BoundInput:
    provider: str
    schema: Mapping[str, ColumnSpec]
    aliases: frozenset[str]
    is_source: bool
    output_bindings: Mapping[str, _BoundOutput] | None = None
    row_dependencies: tuple[tuple[str, ComponentKey], ...] = ()
    row_semantics: object | None = None


@dataclass(frozen=True)
class _BoundSource:
    provider: str
    schema: Mapping[str, ColumnSpec]
    aliases: frozenset[str]
    is_source: bool
    numeric_literals: Mapping[int, str]
    inputs: tuple[_BoundInput, ...] = ()
    joins: tuple[dict[str, object], ...] = ()
    join_dependencies: tuple[tuple[str, ComponentKey], ...] = ()
    context_columns: Mapping[str, ComponentKey] | None = None
    expansion_tracker: list[int] | None = None

    @property
    def providers(self) -> tuple[_BoundInput, ...]:
        return self.inputs or (
            _BoundInput(
                self.provider,
                self.schema,
                self.aliases,
                self.is_source,
            ),
        )

    @property
    def is_joined(self) -> bool:
        return len(self.inputs) > 1


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


def _effective_filter_requirements(
    spec: MetricSpec,
) -> list[FilterRequirement]:
    requirements = [
        *(spec.rendered_required_filters or []),
        *(spec.rendered_recommended_filters or []),
    ]
    unique = {}
    for requirement in requirements:
        if not requirement.default:
            continue
        dimensions = tuple(sorted(set(requirement.dimension)))
        key = canonical_json([dimensions, requirement.default])
        unique[key] = FilterRequirement(
            dimension=dimensions,
            default=requirement.default,
        )
    return [unique[key] for key in sorted(unique)]


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
            node,
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
    def ordered_numbers(node: object) -> Iterable[ast.Number]:
        if isinstance(node, ast.Query):
            for cte in node.ctes or ():
                yield from ordered_numbers(cte)
            yield from ordered_numbers(node.select)
            return
        if isinstance(node, ast.Select):
            for value in (
                node.projection,
                node.from_,
                node.where,
                node.group_by,
                node.having,
                node.lateral_views,
                node.set_op,
                node.limit,
                node.offset,
                node.organization,
                node.hints,
            ):
                yield from ordered_numbers(value)
            return
        if isinstance(node, ast.Number):
            yield node
            return
        if isinstance(node, ast.Node):
            for _, value in node.fields(
                flat=False,
                nodes_only=False,
                obfuscated=False,
                nones=False,
                named=True,
            ):
                yield from ordered_numbers(value)
            return
        if isinstance(node, (list, tuple)):
            for value in node:
                yield from ordered_numbers(value)

    if (
        not query.ctes
        and isinstance(query.select, ast.Select)
        and not query.select.group_by
        and query.select.having is None
    ):
        numbers = list(query.find_all(ast.Number))
    else:
        numbers = list(ordered_numbers(query))
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


def _dimension_link_contract(link: object) -> dict[str, object]:
    if isinstance(link, DimensionJoinLinkSpec):
        return {
            "type": link.type.value,
            "role": link.role,
            "dimension_node": link.rendered_dimension_node,
            "join_type": link.join_type.value,
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


def _node_contract(
    spec: NodeSpec,
    columns: tuple[ColumnSpec, ...] | None = None,
    *,
    include_dimension_links: bool = True,
) -> dict[str, object]:

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
            (_dimension_link_contract(link) for link in spec.dimension_links),
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
            filter_defaults=normalize_value(_effective_filter_requirements(spec)),
        )
    if isinstance(spec, CubeSpec):
        contract.update(
            metrics=sorted(spec.rendered_metrics),
            dimensions=sorted(spec.rendered_dimensions),
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
        requirements = _effective_filter_requirements(spec)
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


def _join_columns(
    source: str,
    target: str,
    link: _DimensionLink,
    cache: _JoinColumnCache,
) -> tuple[set[tuple[str, str]], bool]:
    if isinstance(link, DimensionReferenceLinkSpec):
        return {(source, link.node_column), (target, link.dimension_attribute)}, True
    cache_key = (source, target, link.rendered_join_on or "", link.join_type.value)
    if cache_key in cache:
        columns, parsed = cache[cache_key]
        return set(columns), parsed
    if not link.rendered_join_on:
        return set(), link.join_type.value == "cross"
    text = f"SELECT 1 WHERE ({link.rendered_join_on})"
    try:
        tree = cached_definition_tree(text, "singleStatement")
        # The link contract retains TRY_CAST; this pass only collects columns.
        if any(
            token.type in _LOSSY_TOKENS and token.type != SqlBaseParser.TRY_CAST
            for token in tree.parser.getTokenStream().tokens
        ):
            raise _Unsupported("unsupported_query_modifier")
        query = parse(text)
    except (DJParseException, SqlParsingError, TypeError, ValueError):
        cache[cache_key] = (frozenset(), False)
        return set(), False
    if any(query.find_all(ast.Table)):
        cache[cache_key] = (frozenset(), False)
        return set(), False
    columns = set()
    for column in query.find_all(ast.Column):
        namespace = ".".join(part.name for part in column.namespace or ())
        if namespace not in {source, target}:
            cache[cache_key] = (frozenset(), False)
            return set(), False
        columns.add((namespace, column.name.name))
    result = (frozenset(columns), bool(columns))
    cache[cache_key] = result
    return set(result[0]), result[1]


def _implicit_dimension_link_context(
    query: ast.Query | None,
    specs: Mapping[str, NodeSpec],
    references: Iterable[str] = (),
    relation_references: Iterable[str] = (),
    relation_cache: dict[str, tuple[frozenset[str], bool]] | None = None,
    ancestry_cache: dict[str, frozenset[str] | None] | None = None,
    *,
    narrow_dependencies: bool = False,
    join_column_cache: _JoinColumnCache | None = None,
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
    join_column_cache = join_column_cache if join_column_cache is not None else {}
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
            link_key = canonical_json(normalize_value(_dimension_link_contract(link)))
            relevant[(source, next_node, link_key)] = (source, next_node, link)
            columns, parsed = (
                _join_columns(source, next_node, link, join_column_cache)
                if narrow_dependencies
                else (set(), False)
            )
            if parsed:
                dependencies.update(
                    (f"implicit_link_rows:{node}", ComponentKey(node, "rows"))
                    for node in (source, next_node)
                )
                dependencies.update(
                    (
                        f"implicit_join_key:{node}.{column}",
                        ComponentKey(node, "output", column),
                    )
                    for node, column in columns
                )
            else:
                dependencies.update(
                    (
                        (
                            f"implicit_link_source:{source}",
                            ComponentKey(source, "calculation"),
                        ),
                        (
                            f"implicit_link_target:{next_node}",
                            ComponentKey(next_node, "calculation"),
                        ),
                    ),
                )

    contracts.extend(
        {
            "source": source,
            "target": target,
            "link": normalize_value(_dimension_link_contract(link)),
            **(
                {
                    "grains": {
                        node: sorted(getattr(specs[node], "primary_key", None) or [])
                        for node in (source, target)
                    }
                }
                if narrow_dependencies
                else {}
            ),
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
            for requirement in _effective_filter_requirements(spec)
            for reference in requirement.dimension
        ),
        *(
            rule.dimension
            for rule in (
                spec.rendered_reaggregate.rules if spec.rendered_reaggregate else []
            )
        ),
    }
    requirements = _effective_filter_requirements(spec)
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


def _node_result_definition(
    name: str,
    spec: NodeSpec,
    output_names: tuple[str, ...],
    columns: tuple[ColumnSpec, ...],
    *,
    kind: Literal["full", "calculation"] = "full",
) -> ComponentDefinition:
    dependencies = [
        ("rows", ComponentKey(name, "rows")),
        *(
            (f"output:{index}:{output}", ComponentKey(name, "output", output))
            for index, output in enumerate(output_names)
        ),
    ]
    schema: dict[str, ColumnSpec] = {}
    for column in columns:
        schema.setdefault(column.name, column)
    contract_columns = columns if isinstance(spec, SourceSpec) else None
    return _definition(
        ComponentKey(name, kind),
        {
            "node": _node_contract(
                spec,
                contract_columns,
                include_dimension_links=kind == "full",
            ),
            "output_layout": [
                _output_column_contract(output, schema.get(output))
                for output in output_names
            ],
        },
        dependencies,
    )


def _bind_row_semantics(
    select: ast.Select,
    source: _BoundSource,
) -> tuple[dict[tuple[str, str, str, str], tuple[str, ComponentKey]], dict]:
    """Bind clauses that determine which grouped or ungrouped rows exist."""
    dependencies: dict[
        tuple[str, str, str, str],
        tuple[str, ComponentKey],
    ] = {}

    def add(label: str, key: ComponentKey) -> None:
        dependencies[(label, key.node, key.kind, key.field)] = (label, key)

    for index, provider in enumerate(source.providers):
        if provider.row_semantics is not None:
            for label, key in provider.row_dependencies:
                add(f"input:{index}:{label}", key)
        else:
            label = (
                "source_rows"
                if not source.is_joined
                else f"source_rows:{provider.provider}"
            )
            add(label, ComponentKey(provider.provider, "rows"))
    for label, key in source.join_dependencies:
        add(label, key)

    where_shape = None
    if select.where is not None:
        where_shape, where_dependencies, where_aggregate = _bind_expression(
            select.where,
            source,
        )
        if where_aggregate:
            raise _Unsupported("unsupported_where_aggregate")
        for label, key in where_dependencies:
            add(f"where:{label}", key)

    group_by_shapes = []
    for expression in select.group_by or ():
        if isinstance(expression, (ast.Number, ast.Wildcard)):
            raise _Unsupported("unsupported_group_by")
        shape, group_dependencies, aggregate = _bind_expression(expression, source)
        if aggregate:
            raise _Unsupported("unsupported_group_by")
        group_by_shapes.append(shape)
        for label, key in group_dependencies:
            add(f"group:{label}", key)
    group_by_shapes = sorted(
        {canonical_json(shape): shape for shape in group_by_shapes}.values(),
        key=canonical_json,
    )

    having_shape = None
    if select.having is not None:
        having_shape, having_dependencies, _ = _bind_expression(
            select.having,
            source,
        )
        for label, key in having_dependencies:
            add(f"having:{label}", key)

    inherited_rows = [
        provider.row_semantics
        for provider in source.providers
        if provider.row_semantics is not None
    ]
    row_semantics: dict[str, object]
    if source.is_joined:
        row_semantics = {
            "sources": [provider.provider for provider in source.providers],
            "joins": list(source.joins),
        }
    else:
        row_semantics = {
            "source": ("cte_input" if inherited_rows else source.provider),
        }
    if inherited_rows:
        row_semantics["inputs"] = inherited_rows
    row_semantics.update(
        {
            "where": where_shape,
            "group_by": group_by_shapes,
            "having": having_shape,
            "grain": (
                "grouped_rows"
                if group_by_shapes
                else "global_aggregate"
                if select.having is not None
                else "source_rows"
            ),
        },
    )
    return dependencies, row_semantics


def _compile_pass_through_ctes(
    query: ast.Query,
    specs: Mapping[str, NodeSpec],
    snapshot: SemanticGraphSnapshot,
    schemas: dict[str, tuple[ColumnSpec, ...]],
    numeric_literals: Mapping[int, str],
    expansion_tracker: list[int],
) -> dict[str, _BoundInput]:
    """Bind simple, ordered CTEs to their source outputs and row contracts."""
    ctes = list(query.ctes or ())
    if not ctes:
        return {}
    if len(ctes) > _MAX_CTE_COUNT:
        raise _Unsupported("cte_count_limit")
    declarations: list[tuple[ast.Query, str]] = []
    for cte in ctes:
        alias = cte.alias
        if alias is None or alias.quote_style:
            raise _Unsupported("unsupported_cte_declaration")
        declarations.append((cte, alias.name.casefold()))
    names = [name for _, name in declarations]
    if len(set(names)) != len(names):
        raise _Unsupported("unsupported_cte_declaration")

    bindings: dict[str, _BoundInput] = {}
    all_names = set(names)
    for cte, cte_name in declarations:
        if (
            getattr(query, "recursive", False)
            or cte.column_list
            or cte.ctes
            or not isinstance(cte.select, ast.Select)
        ):
            raise _Unsupported("unsupported_cte_shape")
        select = cte.select
        if (
            select.quantifier
            or select.group_by
            or select.having
            or select.lateral_views
            or select.set_op
            or select.limit
            or select.offset
            or select.hints
            or (
                select.organization
                and (select.organization.order or select.organization.sort)
            )
            or any(
                isinstance(expression, ast.Wildcard) for expression in select.projection
            )
        ):
            raise _Unsupported("unsupported_cte_shape")
        if select.from_ is None or len(select.from_.relations) != 1:
            raise _Unsupported("unsupported_cte_source")
        if any(
            column.name.quote_style
            or any(part.quote_style for part in column.namespace or ())
            for column in select.find_all(ast.Column)
        ):
            raise _Unsupported("unsupported_cte_identifier")
        relation = select.from_.relations[0]
        if not isinstance(relation.primary, ast.Table) or relation.extensions:
            raise _Unsupported("unsupported_cte_source")
        reference = relation.primary.identifier(quotes=False)
        if (
            "." not in reference
            and reference.casefold() in all_names
            and reference.casefold() not in bindings
        ):
            raise _Unsupported("unsupported_cte_reference")
        if "." not in reference and reference.casefold() in all_names:
            if (
                relation.primary.name.quote_style
                or any(part.quote_style for part in relation.primary.namespace or ())
                or (
                    relation.primary.alias is not None
                    and relation.primary.alias.quote_style
                )
            ):
                raise _Unsupported("unsupported_cte_identifier")
        source = _bind_source(
            cte,
            specs,
            snapshot,
            schemas,
            numeric_literals,
            bindings,
            expansion_tracker if bindings else None,
        )
        output_names_list: list[str] = []
        for index, projection in enumerate(select.projection):
            expression = (
                projection.child if isinstance(projection, ast.Alias) else projection
            )
            if isinstance(projection, (ast.Alias, ast.Column)):
                output_alias = projection.alias
            else:
                output_alias = None
            if output_alias is not None:
                if output_alias.quote_style:
                    raise _Unsupported("unsupported_cte_identifier")
                output_names_list.append(output_alias.name)
            elif isinstance(expression, ast.Column):
                output_names_list.append(expression.name.name)
            else:
                raise _Unsupported("unsupported_cte_output_name")
        output_names = tuple(output_names_list)
        if (
            not output_names
            or len(output_names) != len(select.projection)
            or len({output.casefold() for output in output_names}) != len(output_names)
        ):
            raise _Unsupported("unsupported_cte_outputs")

        row_dependencies, row_semantics = _bind_row_semantics(select, source)
        output_bindings = {}
        output_schema = {}
        for output_name, projection in zip(output_names, select.projection):
            expression = (
                projection.child if isinstance(projection, ast.Alias) else projection
            )
            expansion_cost = _cte_expression_expansion_cost(expression, source)
            expression_shape, output_dependencies, aggregate = _bind_expression(
                expression,
                source,
            )
            if aggregate:
                raise _Unsupported("unsupported_cte_aggregate")
            output_bindings[output_name.casefold()] = _BoundOutput(
                expression=expression_shape,
                dependencies=tuple(output_dependencies),
                expansion_cost=expansion_cost,
            )
            output_type = None
            if isinstance(expression, ast.Column):
                input_name = expression.name.name
                input_column = next(
                    (
                        column
                        for column in source.schema.values()
                        if column.name.casefold() == input_name.casefold()
                    ),
                    None,
                )
                output_type = input_column.type if input_column else None
            output_schema[output_name] = ColumnSpec(name=output_name, type=output_type)

        prefixed_row_dependencies = tuple(
            (f"cte:{label}", key) for label, key in row_dependencies.values()
        )
        bindings[cte_name] = _BoundInput(
            provider="__inline_cte__",
            schema=output_schema,
            aliases=frozenset({cte_name}),
            is_source=False,
            output_bindings=output_bindings,
            row_dependencies=prefixed_row_dependencies,
            row_semantics=row_semantics,
        )
    return bindings


def _cte_expression_expansion_cost(
    expression: ast.Node,
    source: _BoundSource,
    tracker: list[int] | None = None,
) -> int:
    cost = 0
    input_relation = source.providers[0]
    outputs = input_relation.output_bindings
    aliases = {alias.casefold() for alias in source.aliases}
    for node in expression.flatten():
        cost += 1
        if not isinstance(node, ast.Column) or outputs is None:
            continue
        qualifier = ".".join(part.name for part in node.namespace or ())
        if qualifier and qualifier.casefold() not in aliases:
            continue
        output = outputs.get(node.name.name.casefold())
        if output is not None:
            cost += output.expansion_cost - 1
        if cost > _MAX_CTE_EXPANDED_NODES:
            raise _Unsupported("cte_expansion_limit")
    if tracker is not None:
        tracker[0] += cost
        if tracker[0] > _MAX_CTE_EXPANDED_NODES:
            raise _Unsupported("cte_expansion_limit")
    return cost


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
                (
                    amenable_name(name)
                    if isinstance(spec, MetricSpec)
                    else _projection_name(expression, index, declared)
                ),
            ]
        )
    _require_unique_columns(columns, "duplicate_provider_output")
    cache[name] = columns
    return columns


def _bound_join_column(column: ast.Column, source: _BoundSource) -> _BoundInput:
    if (
        not column.namespace
        or column.role
        or column.dimension_ref
        or column.name.quote_style
        or any(part.quote_style for part in column.namespace)
    ):
        raise _Unsupported("unsupported_join_reference")
    alias = column.namespace[0].name.casefold()
    matches = [relation for relation in source.providers if alias in relation.aliases]
    if len(matches) != 1:
        raise _Unsupported("unresolved_or_ambiguous_column")
    return matches[0]


def _bind_join_source(
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
    if not isinstance(relation.primary, ast.Table) or not relation.extensions:
        raise _Unsupported("unsupported_join")
    tables = [relation.primary]
    joins: list[tuple[str, ast.Expression | None]] = []
    for join in relation.extensions:
        join_type = join.join_type.strip().casefold() or "inner"
        if (
            join_type not in {"inner", "left", "left outer", "cross"}
            or join.lateral
            or join.natural
            or not isinstance(join.right, ast.Table)
            or join.right.alias is None
            or (
                join_type != "cross"
                and (
                    join.criteria is None
                    or join.criteria.on is None
                    or join.criteria.using
                )
            )
            or (join_type == "cross" and join.criteria is not None)
        ):
            raise _Unsupported("unsupported_join")
        tables.append(join.right)
        joins.append((join_type, join.criteria.on if join.criteria else None))

    bindings: list[_BoundInput] = []
    for table in tables:
        if (
            table.alias is None
            or table.alias.quote_style
            or table.column_list
            or table.path
            or table.dimension_link
            or table.name.quote_style
            or any(part.quote_style for part in table.namespace or ())
        ):
            raise _Unsupported("unsupported_table_modifier")
        reference = table.identifier(quotes=False)
        provider = _resolve_node_name(reference, specs)
        if provider is None:
            raise _Unsupported("unresolved_source")
        schema = _require_unique_columns(
            _query_schema(provider, specs[provider], snapshot, specs, schemas),
            "duplicate_provider_output",
        )
        binding = _BoundInput(
            provider,
            {column.name: column for column in schema},
            frozenset({table.alias.name.casefold()}),
            isinstance(specs[provider], SourceSpec),
        )
        if any(
            binding.provider == existing.provider or binding.aliases & existing.aliases
            for existing in bindings
        ):
            raise _Unsupported("unsupported_self_join")
        bindings.append(binding)

    join_shapes, dependencies = [], []
    for index, (join_type, predicate) in enumerate(joins):
        source = _BoundSource(
            bindings[0].provider,
            bindings[0].schema,
            frozenset().union(*(item.aliases for item in bindings[: index + 2])),
            bindings[0].is_source,
            numeric_literals,
            tuple(bindings[: index + 2]),
        )
        on_shape = None
        if predicate is not None:
            on_shape, on_dependencies, aggregate = _bind_expression(
                predicate,
                source,
            )
            if aggregate:
                raise _Unsupported("unsupported_join_predicate")
            dependencies.extend(on_dependencies)
        join_shapes.append(
            {"type": join_type.removesuffix(" outer"), "on": on_shape},
        )
    return _BoundSource(
        bindings[0].provider,
        bindings[0].schema,
        frozenset().union(*(item.aliases for item in bindings)),
        bindings[0].is_source,
        numeric_literals,
        tuple(bindings),
        tuple(join_shapes),
        tuple(dependencies),
    )


def _bind_source(
    query: ast.Query,
    specs: Mapping[str, NodeSpec],
    snapshot: SemanticGraphSnapshot,
    schemas: dict[str, tuple[ColumnSpec, ...]],
    numeric_literals: Mapping[int, str],
    cte_bindings: Mapping[str, _BoundInput] | None = None,
    expansion_tracker: list[int] | None = None,
) -> _BoundSource:
    if not isinstance(query.select, ast.Select):
        raise _Unsupported("unsupported_query_shape")
    from_clause = query.select.from_
    if from_clause is None or len(from_clause.relations) != 1:
        raise _Unsupported("unsupported_from_expression")
    relation = from_clause.relations[0]
    if relation.extensions:
        if query.ctes or cte_bindings:
            raise _Unsupported("unsupported_cte_join")
        return _bind_join_source(query, specs, snapshot, schemas, numeric_literals)
    table = relation.primary
    if not isinstance(table, ast.Table):
        raise _Unsupported("unsupported_from_expression")
    if table.column_list or table.path or table.dimension_link:
        raise _Unsupported("unsupported_table_modifier")
    reference = table.identifier(quotes=False)
    cte = (
        (cte_bindings or {}).get(reference.casefold()) if "." not in reference else None
    )
    if cte is not None:
        if (
            table.name.quote_style
            or any(part.quote_style for part in table.namespace or ())
            or (table.alias is not None and table.alias.quote_style)
        ):
            raise _Unsupported("unsupported_cte_identifier")
        aliases = {table.name.name, reference, cte.provider}
        if table.alias is not None:
            aliases.add(table.alias.name)
        binding = replace(cte, aliases=frozenset(alias.casefold() for alias in aliases))
        return _BoundSource(
            binding.provider,
            binding.schema,
            binding.aliases,
            False,
            numeric_literals,
            (binding,),
            expansion_tracker=expansion_tracker,
        )
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
        expansion_tracker=expansion_tracker,
    )


def _bind_expression(
    expression: ast.Node,
    source: _BoundSource,
    *,
    allow_unknown_functions: bool = False,
) -> tuple[object, list[tuple[str, ComponentKey]], bool]:
    if source.expansion_tracker is not None and any(
        provider.output_bindings is not None for provider in source.providers
    ):
        _cte_expression_expansion_cost(
            expression,
            source,
            source.expansion_tracker,
        )
    dependencies: dict[str, ComponentKey] = {}
    aggregate = False
    numeric_literals = []
    bound_columns: dict[str, tuple[str, str]] = {}
    inline_columns: dict[str, object] = {}
    joined = source.is_joined
    source_aliases = {alias.casefold() for alias in source.aliases}
    for node in expression.flatten():
        if isinstance(
            node,
            (
                ast.Query,
                ast.Select,
                ast.Over,
                ast.Lambda,
                ast.FunctionTableExpression,
            ),
        ):
            raise _Unsupported("unsupported_expression_shape")
        if isinstance(node, ast.Function):
            function_name = node.name.name.upper()
            if node.name.namespace or (
                function_name not in _SUPPORTED_FUNCTIONS
                and not allow_unknown_functions
            ):
                raise _Unsupported("unsupported_function")
            aggregate = aggregate or function_name in _AGGREGATE_FUNCTIONS
        if isinstance(node, ast.Number):
            numeric_literals.append(source.numeric_literals[id(node)])
        if isinstance(node, ast.Column):
            context_key = (source.context_columns or {}).get(
                node.identifier(quotes=False).casefold(),
            )
            if context_key is not None and not node.role and not node.dimension_ref:
                dependencies[f"dimension:{context_key.node}.{context_key.field}"] = (
                    context_key
                )
                bound_columns[node.identifier(quotes=False).casefold()] = (
                    context_key.node,
                    context_key.field,
                )
                continue
            if joined:
                provider, path, has_type = _resolve_join_column(node, source)
                if provider.output_bindings is not None:
                    if len(path) != 1:
                        raise _Unsupported("unsupported_cte_column_path")
                    if node.name.quote_style or any(
                        part.quote_style for part in node.namespace or ()
                    ):
                        raise _Unsupported("unsupported_cte_identifier")
                    output = provider.output_bindings.get(path[0].casefold())
                    if output is None:
                        raise _Unsupported("resolved_column_schema_missing")
                    reference = node.identifier(quotes=False).casefold()
                    inline_columns[reference] = output.expression
                    dependencies.update(
                        {f"cte:{label}": key for label, key in output.dependencies},
                    )
                    continue
                if provider.is_source and not has_type:
                    raise _Unsupported("source_column_type_unavailable")
                column_key = ComponentKey(
                    provider.provider,
                    "output",
                    path[0],
                )
                identity = (provider.provider, ".".join(path))
                dependencies[f"{identity[0]}:{identity[1]}"] = column_key
                bound_columns[node.identifier(quotes=False).casefold()] = identity
                continue
            qualifier = ".".join(part.name for part in node.namespace)
            if node.role or node.dimension_ref:
                raise _Unsupported("unsupported_expression_shape")
            if (
                qualifier
                and qualifier.casefold() not in source_aliases
                and len(node.namespace) == 1
                and not node.namespace[0].quote_style
                and not node.name.quote_style
            ):
                roots = [
                    column
                    for column in source.schema.values()
                    if column.name.casefold() == qualifier.casefold()
                ]
                if len(roots) != 1:
                    raise _Unsupported("unresolved_or_ambiguous_column")
                root = roots[0]
                root_type = _join_column_type(root.type) if root.type else None
                if not isinstance(root_type, StructType):
                    raise _Unsupported("resolved_column_schema_missing")
                fields = [
                    field
                    for field in root_type.fields
                    if field.name.name.casefold() == node.name.name.casefold()
                ]
                if len(fields) != 1:
                    raise _Unsupported("resolved_column_schema_missing")
                field = fields[0]
                identity = (source.provider, f"{root.name}.{field.name.name}")
                dependencies[f"struct:{identity[1]}"] = ComponentKey(
                    source.provider,
                    "output",
                    root.name,
                )
                bound_columns[node.identifier(quotes=False).casefold()] = identity
                continue
            if qualifier and qualifier not in source.aliases:
                raise _Unsupported("unresolved_or_ambiguous_column")
            name = node.name.name
            matched_name = (
                name
                if name in source.schema
                else next(
                    (
                        column_name
                        for column_name in source.schema
                        if column_name.casefold() == name.casefold()
                    ),
                    None,
                )
            )
            if matched_name is None:
                raise _Unsupported("resolved_column_schema_missing")
            input_relation = source.providers[0]
            if input_relation.output_bindings is not None:
                if node.name.quote_style or any(
                    part.quote_style for part in node.namespace or ()
                ):
                    raise _Unsupported("unsupported_cte_identifier")
                output = input_relation.output_bindings.get(matched_name.casefold())
                if output is None:
                    raise _Unsupported("resolved_column_schema_missing")
                reference = node.identifier(quotes=False).casefold()
                inline_columns[reference] = output.expression
                dependencies.update(
                    {f"cte:{label}": key for label, key in output.dependencies},
                )
                continue
            if (
                input_relation.is_source
                and _parsed_type(source.schema[matched_name].type) is None
            ):
                raise _Unsupported("source_column_type_unavailable")
            dependencies[matched_name] = ComponentKey(
                input_relation.provider,
                "output",
                matched_name,
            )

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
        if value["type"] == "Column":
            parts = _serialized_column_parts(value)
            reference = ".".join(parts).casefold()
            inline = inline_columns.get(reference)
            if inline is not None:
                return {"inline_expression": inline}
            identity = bound_columns.get(reference)
            if joined and identity is None:
                raise _Unsupported("unresolved_or_ambiguous_column")
            if identity is not None:
                fields["bound_relation"] = identity[0]
                fields["bound_field"] = identity[1]
            fields["name"]["fields"]["namespace"] = None
        elif value["type"] == "Wildcard":
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


def _serialized_column_parts(value: dict[str, Any]) -> list[str]:
    name = value["fields"]["name"]["fields"]
    namespace = name.get("namespace")
    parts = []
    while namespace is not None:
        parts.append(namespace["fields"]["name"])
        namespace = namespace["fields"].get("namespace")
    parts.reverse()
    parts.append(name["name"])
    return parts


def _resolve_join_column(
    column: ast.Column,
    source: _BoundSource,
) -> tuple[_BoundInput, list[str], bool]:
    if column.name.quote_style:
        raise _Unsupported("unsupported_join_reference")
    parts = [part.name for part in column.namespace or ()] + [column.name.name]
    if not column.namespace and not column.role and not column.dimension_ref:
        matches = [
            relation
            for relation in source.providers
            if any(name.casefold() == parts[0].casefold() for name in relation.schema)
        ]
        if len(matches) != 1:
            raise _Unsupported("unresolved_or_ambiguous_column")
        provider, path = matches[0], parts
    else:
        provider = _bound_join_column(column, source)
        path = parts[1:]
    if not path:
        raise _Unsupported("unresolved_or_ambiguous_column")
    matching_columns = [
        value
        for name, value in provider.schema.items()
        if name.casefold() == path[0].casefold()
    ]
    if len(matching_columns) != 1:
        raise _Unsupported("resolved_column_schema_missing")
    column_spec = matching_columns[0]
    nested_fields = path[1:]
    path = [column_spec.name]
    current_type = None
    if nested_fields:
        current_type = _join_column_type(column_spec.type) if column_spec.type else None
        if current_type is None:
            raise _Unsupported("resolved_column_schema_missing")
    for field_name in nested_fields:
        if not isinstance(current_type, StructType):
            raise _Unsupported("unresolved_or_ambiguous_column")
        matches = [
            field
            for field in current_type.fields
            if field.name.name.casefold() == field_name.casefold()
        ]
        if len(matches) != 1:
            raise _Unsupported("resolved_column_schema_missing")
        field = matches[0]
        path.append(field.name.name)
        current_type = field.type
    return provider, path, _parsed_type(column_spec.type) is not None


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
    definitions.append(_node_result_definition(name, spec, names, columns))
    definitions.append(
        _node_result_definition(
            name,
            spec,
            names,
            columns,
            kind="calculation",
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
        else (
            tuple(spec.matched_rendered_columns)
            if isinstance(spec, CubeSpec)
            else _declared_columns(spec)
        )
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
            [("opaque", opaque)],
            precision=precision,
            reasons=reasons,
            unavailable=unresolved,
        ),
    )
    definitions.append(
        _node_result_definition(
            name,
            spec,
            output_names,
            columns,
            kind="calculation",
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
    relation_cache: dict[str, tuple[frozenset[str], bool]],
    ancestry_cache: dict[str, frozenset[str] | None],
    join_column_cache: _JoinColumnCache,
) -> list[ComponentDefinition]:
    if isinstance(spec, MetricSpec) and (
        spec.rendered_required_dimensions
        or spec.rendered_fixed_grain
        or _effective_filter_requirements(spec)
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
    numeric_literals = _numeric_literals(query, spec.rendered_query or "")
    expansion_tracker = [0]
    cte_bindings = _compile_pass_through_ctes(
        query,
        specs,
        snapshot,
        schemas,
        numeric_literals,
        expansion_tracker,
    )
    source = _bind_source(
        query,
        specs,
        snapshot,
        schemas,
        numeric_literals,
        cte_bindings,
        expansion_tracker if cte_bindings else None,
    )
    implicit_links, implicit_dependencies = [], []
    if isinstance(spec, MetricSpec):
        context_columns: dict[str, ComponentKey] = {}
        for column in query.find_all(ast.Column):
            namespace = ".".join(part.name for part in column.namespace or ())
            if (
                namespace not in source.aliases
                and (
                    not column.namespace
                    or column.namespace[0].name not in source.aliases
                )
                and isinstance(specs.get(namespace), DimensionSpec)
            ):
                if column.name.quote_style or any(
                    part.quote_style for part in column.namespace or ()
                ):
                    raise _Unsupported("unsupported_implicit_dimension_identifier")
                reference = column.identifier(quotes=False).casefold()
                component = ComponentKey(
                    namespace,
                    "output",
                    column.name.name,
                )
                existing = context_columns.get(reference)
                if existing is not None and existing != component:
                    raise _Unsupported("ambiguous_implicit_dimension_reference")
                context_columns[reference] = component
        if context_columns:
            source = replace(source, context_columns=context_columns)
            implicit_links, implicit_dependencies, unresolved = (
                _implicit_dimension_link_context(
                    query,
                    specs,
                    relation_references=tuple(
                        item.provider for item in source.providers
                    ),
                    relation_cache=relation_cache,
                    ancestry_cache=ancestry_cache,
                    narrow_dependencies=True,
                    join_column_cache=join_column_cache,
                )
            )
            if unresolved:
                raise _Unsupported("unresolved_dimension_context")
    if not output_names or len(output_names) != len(select.projection):
        raise _Unsupported("duplicate_output_name")
    if any(isinstance(expression, ast.Wildcard) for expression in select.projection):
        raise _Unsupported("star_projection")
    if len(set(output_names)) != len(output_names):
        raise _Unsupported("duplicate_output_name")

    outputs = {output: ComponentKey(name, "output", output) for output in output_names}
    row_dependencies, row_payload = _bind_row_semantics(select, source)
    for label, key in implicit_dependencies:
        row_dependencies[(label, key.node, key.kind, key.field)] = (label, key)
    input_rows = ComponentKey(name, "rows", "input")
    aggregate_query = bool(select.group_by) or select.having is not None
    output_definitions = []
    conservative_projections = []
    opaque = ComponentKey(name, "opaque")
    opaque_dependencies = {
        f"opaque_parent:{provider.provider}": ComponentKey(
            provider.provider, "calculation"
        )
        for provider in source.providers
    }
    for output, projection in zip(output_names, select.projection):
        expression = (
            projection.child if isinstance(projection, ast.Alias) else projection
        )
        conservative_projection = False
        try:
            expression_shape, dependencies, aggregate = _bind_expression(
                expression, source
            )
        except _Unsupported as exc:
            if exc.reason != "unsupported_function":
                raise
            expression_shape, dependencies, aggregate = _bind_expression(
                expression,
                source,
                allow_unknown_functions=True,
            )
            if aggregate:
                raise _Unsupported("unsupported_function") from exc
            conservative_projection = True
            conservative_projections.append(
                {"output": output, "expression": expression_shape}
            )
            opaque_dependencies.update(dependencies)
            dependencies = [*dependencies, ("opaque_projection", opaque)]
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
                precision="conservative" if conservative_projection else "precise",
                reasons=("unsupported_function",) if conservative_projection else (),
            ),
        )

    if source.is_joined and aggregate_query:
        raise _Unsupported("unsupported_join_aggregate")
    if conservative_projections and aggregate_query:
        raise _Unsupported("unsupported_function")
    row_payload["grain"] = (
        "grouped_rows"
        if select.group_by
        else "global_aggregate"
        if aggregate_query
        else "joined_rows"
        if source.is_joined
        else row_payload["grain"]
    )
    opaque_definitions = []
    if conservative_projections:
        row_dependencies[("opaque_projection", name, "opaque", "")] = (
            "opaque_projection",
            opaque,
        )
        opaque_definitions.append(
            _definition(
                opaque,
                {"projections": conservative_projections},
                opaque_dependencies.items(),
                precision="conservative",
                reasons=("unsupported_function",),
            ),
        )

    if implicit_links:
        row_payload["implicit_dimension_links"] = implicit_links
    definitions = [
        *opaque_definitions,
        _definition(
            input_rows,
            row_payload,
            row_dependencies.values(),
            precision="conservative" if implicit_links else "precise",
            reasons=(
                ("candidate_dimension_paths_over_approximated",)
                if implicit_links
                else ()
            ),
        ),
        *output_definitions,
        _definition(
            ComponentKey(name, "rows"),
            row_payload,
            [("input_rows", input_rows)],
        ),
        _node_result_definition(
            name,
            spec,
            output_names,
            _declared_columns(spec),
        ),
        _node_result_definition(
            name,
            spec,
            output_names,
            schemas[name],
            kind="calculation",
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
    join_column_cache: _JoinColumnCache = {}
    for name, spec in specs.items():
        names = output_names[name]
        reason = reasons_by_node[name][0] if name in reasons_by_node else None
        if reason is None:
            if isinstance(spec, SourceSpec):
                node_definitions = _source_definitions(name, spec, snapshot, names)
            elif isinstance(spec, CubeSpec):
                reason = "cube_dependency_context_unsupported"
            else:
                try:
                    node_definitions = _query_definitions(
                        name,
                        spec,
                        snapshot,
                        specs,
                        names,
                        schemas,
                        relation_cache,
                        ancestry_cache,
                        join_column_cache,
                    )
                except (DJParseException, TypeError, ValueError) as exc:
                    reason = (
                        exc.reason
                        if isinstance(exc, _Unsupported)
                        else "query_parse_failure"
                        if isinstance(exc, DJParseException)
                        else "unsupported_query_shape"
                    )
                    reasons_by_node[name] = (reason,)
        if reason is not None:
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
