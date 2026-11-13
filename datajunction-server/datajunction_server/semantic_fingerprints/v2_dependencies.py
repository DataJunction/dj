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
    cached_definition_tree,
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
            dimension_links=sorted(
                (link_contract(link) for link in spec.dimension_links),
                key=canonical_json,
            ),
        )
    elif isinstance(spec, LinkableNodeSpec):
        contract.update(
            primary_key=sorted(spec.primary_key or []),
            dimension_links=sorted(
                (link_contract(link) for link in spec.dimension_links),
                key=canonical_json,
            ),
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
    matches = [name for name in specs if name.rsplit(".", 1)[-1] == reference]
    return matches[0] if len(matches) == 1 else None


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
    return definitions


def _opaque_definitions(
    name: str,
    output_names: tuple[str, ...],
    reason: str,
) -> list[ComponentDefinition]:
    opaque = ComponentKey(name, "opaque")
    reasons = (reason,)
    definitions = [
        _definition(
            opaque,
            {},
            precision="unknown",
            reasons=reasons,
            unavailable=True,
        ),
        _definition(
            ComponentKey(name, "rows"),
            {"opaque_component": opaque.token},
            [("opaque", opaque)],
            precision="unknown",
            reasons=reasons,
            unavailable=True,
        ),
    ]
    definitions.extend(
        _definition(
            ComponentKey(name, "output", output),
            {"opaque_component": opaque.token, "output": output},
            [("opaque", opaque)],
            precision="unknown",
            reasons=reasons,
            unavailable=True,
        )
        for output in output_names
    )
    definitions.append(
        _definition(
            ComponentKey(name, "full"),
            {"opaque_component": opaque.token, "output_layout": list(output_names)},
            [("opaque", opaque)],
            precision="unknown",
            reasons=reasons,
            unavailable=True,
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
    for name, spec in specs.items():
        names = output_names[name]
        if name in reasons_by_node:
            node_definitions = _opaque_definitions(
                name,
                names,
                reasons_by_node[name][0],
            )
        elif isinstance(spec, SourceSpec):
            node_definitions = _source_definitions(name, spec, snapshot, names)
        elif isinstance(spec, CubeSpec):
            node_definitions = _opaque_definitions(
                name,
                names,
                "cube_dependency_context_unsupported",
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
                    names,
                    exc.reason,
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
                    names,
                    reason,
                )
        for definition in node_definitions:
            definitions[definition.key.token] = definition
    return definitions, output_names, reasons_by_node
