"""Tests for the CEL binding contract and metadata prefill."""

import pytest

from datajunction_server.internal.checks.context import (
    VARIABLES,
    build_env,
    custom_metadata,
)
from tests.internal.checks.conftest import DECLARED_PROPERTIES


def test_only_the_declared_bindings_exist():
    assert set(VARIABLES) == {"node", "dependencies", "previous", "change"}


def test_undeclared_binding_is_a_compile_error():
    # compile() type-checks, so this is a hard failure rather than a runtime one.
    with pytest.raises(Exception, match="warehouse"):
        build_env().compile("warehouse.owner != null")


def test_declared_properties_are_null_prefilled():
    prefilled = custom_metadata(None, DECLARED_PROPERTIES)
    assert prefilled == {"sample": {"color": None, "size": None, "shape": None}}


def test_authored_values_survive_the_prefill():
    prefilled = custom_metadata({"sample": {"color": "blue"}}, DECLARED_PROPERTIES)
    assert prefilled["sample"] == {"color": "blue", "size": None, "shape": None}


def test_json_null_metadata_block_is_normalized():
    # A revision may store JSON null for the whole map or for one block; both
    # have to become a dict, because selecting a field from null is an error.
    for raw in (None, {}, {"sample": None}, {"sample": "not-a-block"}):
        assert custom_metadata(raw, DECLARED_PROPERTIES)["sample"]["color"] is None


def test_unschematized_keys_pass_through_untouched():
    raw = {"freeform": {"nested": {"leaf": 1}}}
    assert custom_metadata(raw, DECLARED_PROPERTIES)["freeform"] == raw["freeform"]
