"""Core component hashing stays deterministic and preserves evidence quality."""

from datajunction_server.models.deployment import ColumnSpec, SourceSpec
from datajunction_server.semantic_fingerprints.v2 import (
    ComponentDefinition,
    ComponentKey,
    SemanticGraphSnapshot,
    _evaluate_definitions,
    compare_components,
)


def _results(*definitions):
    return _evaluate_definitions({item.key.token: item for item in definitions})


def _source_definition():
    key = ComponentKey("ns.source", "output", "amount")
    return ComponentDefinition(key, {"value": 3})


def test_component_cycle_hash_is_order_independent_and_labelled():
    source = _source_definition()
    first = ComponentKey("ns.cycle", "output", "first")
    second = ComponentKey("ns.cycle", "output", "second")
    definitions = [
        source,
        ComponentDefinition(first, {}, (("left", second), ("input", source.key))),
        ComponentDefinition(second, {}, (("previous", first),)),
    ]
    forward = _results(*definitions)
    reverse = _results(
        *(
            ComponentDefinition(
                item.key,
                item.payload,
                tuple(reversed(item.dependencies)),
                item.precision,
                item.reasons,
                item.unavailable,
            )
            for item in reversed(definitions)
        ),
    )
    assert forward == reverse
    assert forward[first.token].digest is not None
    assert "cyclic_component" in forward[first.token].reasons


def test_dependency_labels_are_part_of_component_hashes():
    source = _source_definition()
    output = ComponentKey("ns.metric", "output", "value")
    amount = ComponentDefinition(
        output,
        {"expression": "amount"},
        (("amount", source.key),),
    )
    renamed_binding = ComponentDefinition(
        output,
        {"expression": "amount"},
        (("other_input", source.key),),
    )
    before = _results(source, amount)[output.token]
    after = _results(source, renamed_binding)[output.token]
    assert compare_components(before, after) == "changed"


def test_missing_dependency_propagates_unknown_and_compares_indeterminate():
    missing = ComponentKey("ns.missing", "output", "value")
    output = ComponentKey("ns.metric", "full")
    definition = ComponentDefinition(output, {}, (("required", missing),))
    before = _results(definition)[output.token]
    after = _results(definition)[output.token]
    assert before.digest is None
    assert before.precision == "unknown"
    assert "unresolved_component_dependency" in before.reasons
    assert compare_components(before, after) == "indeterminate"


def test_conservative_precision_propagates_through_dependencies():
    source = _source_definition()
    conservative = ComponentDefinition(
        source.key,
        {"value": 3},
        precision="conservative",
        reasons=("opaque_sql",),
    )
    output = ComponentKey("ns.metric", "full")
    consumer = ComponentDefinition(output, {}, (("source", source.key),))
    result = _results(conservative, consumer)[output.token]
    assert result.digest is not None
    assert result.precision == "conservative"
    assert "opaque_sql" in result.reasons


def test_snapshot_copies_inputs_and_values_returned_to_callers():
    source = SourceSpec(
        name="ns.source",
        catalog="warehouse",
        schema_="sales",
        table="orders",
        columns=[ColumnSpec(name="amount", type="double")],
    )
    captured = [ColumnSpec(name="external_id", type="bigint")]
    snapshot = SemanticGraphSnapshot(
        {"ns.source": source},
        resolved_columns={"ns.external": captured},
    )
    source.table = "changed"
    captured[0].type = "string"
    assert snapshot.specs["ns.source"].table == "orders"
    assert snapshot.resolved_columns["ns.external"][0].type == "bigint"

    returned = snapshot.specs
    returned["ns.source"].table = "caller mutation"
    columns = snapshot.resolved_columns
    columns["ns.external"][0].type = "caller mutation"
    assert snapshot.specs["ns.source"].table == "orders"
    assert snapshot.resolved_columns["ns.external"][0].type == "bigint"
