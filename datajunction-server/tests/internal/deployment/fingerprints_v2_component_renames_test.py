"""Logical addresses stay separate from component data and input bindings."""

import pytest

from datajunction_server.semantic_fingerprints.v2 import (
    ComponentDefinition,
    ComponentKey,
    UnorderedPayload,
    _evaluate_definitions,
)


def _evaluate(*definitions):
    return _evaluate_definitions({item.key.token: item for item in definitions})


def _graph(source_name, consumer_name, *, table="orders", literal="ns.source"):
    source = ComponentDefinition(
        ComponentKey(source_name, "output", "amount"),
        {"table": table},
    )
    consumer = ComponentDefinition(
        ComponentKey(consumer_name, "full"),
        {"input": source.key, "literal": literal, "nested": (None, [True, 1])},
        (("input", source.key),),
    )
    return _evaluate(source, consumer)[consumer.key.token]


def test_logical_node_and_provider_renames_preserve_hashes():
    before = _graph("ns.source", "ns.metric")
    assert before.digest is not None
    assert before.digest == _graph("ns.renamed_source", "ns.renamed_metric").digest
    assert (
        before.digest != _graph("ns.source", "ns.metric", table="other_orders").digest
    )
    assert (
        before.digest
        != _graph("ns.source", "ns.metric", literal="ns.renamed_source").digest
    )


def test_input_bindings_preserve_operand_order_and_ignore_lookup_sort_order():
    def evaluate(left_name, right_name, *, swap=False):
        left = ComponentDefinition(
            ComponentKey(left_name, "output", "rate"),
            {"value": 2},
        )
        right = ComponentDefinition(
            ComponentKey(right_name, "output", "rate"),
            {"value": 5},
        )
        providers = (right.key, left.key) if swap else (left.key, right.key)
        consumer = ComponentDefinition(
            ComponentKey("ns.metric", "full"),
            {
                "subtract": list(providers),
                "inputs": UnorderedPayload(providers),
                "contracts": UnorderedPayload(
                    tuple({"provider": item} for item in sorted(providers)),
                ),
            },
            (("left", providers[0]), ("right", providers[1])),
        )
        return _evaluate(left, right, consumer)[consumer.key.token].digest

    before = evaluate("ns.a", "ns.z")
    assert before is not None
    assert before == evaluate("ns.z", "ns.a")
    assert before != evaluate("ns.a", "ns.z", swap=True)


@pytest.mark.parametrize("kind", ["demand_context", "demand_rows", "demand_value"])
def test_demand_cache_addresses_are_excluded_but_demand_content_is_hashed(kind):
    def evaluate(name, address, field):
        definition = ComponentDefinition(
            ComponentKey(name, kind, address),
            {"dimension": field},
            semantic_field="",
        )
        return _evaluate(definition)[definition.key.token].digest

    before = evaluate("ns.metric", "old_cache_key", "account_id")
    assert before == evaluate("ns.renamed", "new_cache_key", "account_id")
    assert before != evaluate("ns.metric", "old_cache_key", "manager_id")


def test_output_field_names_remain_part_of_the_hash():
    first = ComponentDefinition(ComponentKey("ns.source", "output", "amount"), {})
    second = ComponentDefinition(ComponentKey("ns.source", "output", "revenue"), {})
    results = _evaluate(first, second)
    assert results[first.key.token].digest != results[second.key.token].digest


def test_unregistered_payload_reference_is_unknown_and_propagates():
    provider = ComponentDefinition(ComponentKey("ns.source", "output", "amount"), {})
    consumer = ComponentDefinition(
        ComponentKey("ns.consumer", "full"),
        {"input": provider.key},
    )
    downstream = ComponentDefinition(
        ComponentKey("ns.downstream", "full"),
        {},
        (("input", consumer.key),),
    )
    results = _evaluate(provider, consumer, downstream)
    assert results[consumer.key.token].digest is None
    assert "unresolved_payload_dependency" in results[consumer.key.token].reasons
    assert results[downstream.key.token].digest is None
