"""Tests for the CEL binding contract and the activation builder."""

from enum import Enum

import pytest

from datajunction_server.internal.checks.context import (
    VARIABLES,
    activation,
    build_env,
    custom_metadata,
    node_snapshot,
)
from tests.internal.checks.conftest import DECLARED_PROPERTIES, make_link, make_node


class _Colour(Enum):
    RED = "red"


def test_only_the_declared_bindings_exist():
    assert set(VARIABLES) == {"node", "dependencies", "previous", "change"}


def test_undeclared_binding_is_a_compile_error():
    # compile() type-checks, so this is a hard failure rather than a runtime one.
    with pytest.raises(Exception, match="warehouse"):
        build_env().compile("warehouse.owner != null")


def test_declared_properties_are_null_prefilled():
    prefilled = custom_metadata(None, DECLARED_PROPERTIES)
    assert prefilled == {"sample": {"colour": None, "size": None, "shape": None}}


def test_authored_values_survive_the_prefill():
    prefilled = custom_metadata({"sample": {"colour": "blue"}}, DECLARED_PROPERTIES)
    assert prefilled["sample"] == {"colour": "blue", "size": None, "shape": None}


def test_json_null_metadata_block_is_normalized():
    # A revision may store JSON null for the whole map or for one block; both
    # have to become a dict, because selecting a field from null is an error.
    for raw in (None, {}, {"sample": None}, {"sample": "not-a-block"}):
        assert custom_metadata(raw, DECLARED_PROPERTIES)["sample"]["colour"] is None


def test_unschematized_keys_pass_through_untouched():
    raw = {"freeform": {"nested": {"leaf": 1}}}
    assert custom_metadata(raw, DECLARED_PROPERTIES)["freeform"] == raw["freeform"]


def test_node_snapshot_flattens_relationships():
    snapshot = node_snapshot(
        make_node(
            node_type=_Colour.RED,
            custom_metadata={"sample": {"colour": "blue"}},
            description="A widget.",
            owners=[("one", "one@example.com")],
            tags=[("citrus", "flavour")],
            columns=[("id", "The id."), ("value", None)],
            primary_key=["id"],
            dimension_links=[make_link(join_type="inner", default_value="unknown")],
        ),
        DECLARED_PROPERTIES,
    )
    assert snapshot["type"] == "red"
    assert snapshot["description"] == "A widget."
    assert snapshot["owners"] == [{"username": "one", "email": "one@example.com"}]
    assert snapshot["tags"] == [{"name": "citrus", "tag_type": "flavour"}]
    assert snapshot["columns"][1] == {"name": "value", "description": None}
    assert snapshot["primary_key"] == ["id"]
    link = snapshot["dimension_links"][0]
    assert link["join_type"] == "inner"
    assert link["default_value"] == "unknown"
    assert link["dimension"]["custom_metadata"]["sample"]["colour"] is None


def test_absent_description_and_join_type_get_defaults():
    snapshot = node_snapshot(
        make_node(dimension_links=[make_link()]),
        DECLARED_PROPERTIES,
    )
    assert snapshot["description"] == ""
    assert snapshot["dimension_links"][0]["join_type"] == "left"
    assert snapshot["dimension_links"][0]["role"] is None


def test_activation_without_history_or_dependencies():
    bound = activation(make_node(), DECLARED_PROPERTIES, is_new=True)
    assert bound["dependencies"] == []
    assert bound["previous"]["exists"] is False
    assert bound["previous"]["custom_metadata"]["sample"]["colour"] is None
    assert bound["change"] == {"is_new": True, "is_removal": False}


def test_activation_carries_dependencies_and_previous_state():
    previous = make_node().current
    previous.custom_metadata = {"sample": {"colour": "blue"}}
    bound = activation(
        make_node(),
        DECLARED_PROPERTIES,
        dependencies=[make_node(name="default.parent", node_type="source")],
        previous=previous,
        is_removal=True,
    )
    assert bound["dependencies"] == [
        {
            "name": "default.parent",
            "type": "source",
            "custom_metadata": {
                "sample": {"colour": None, "size": None, "shape": None},
            },
        },
    ]
    assert bound["previous"]["exists"] is True
    assert bound["previous"]["custom_metadata"]["sample"]["colour"] == "blue"
    assert bound["change"]["is_removal"] is True


def test_activation_is_evaluable_as_bound():
    env = build_env()
    bound = activation(
        make_node(owners=[("one", "one@example.com")]),
        DECLARED_PROPERTIES,
    )
    value = env.compile("size(node.owners) >= 1").eval(data=bound)
    assert str(value.type()) == "BOOL"
    assert value.value() is True
