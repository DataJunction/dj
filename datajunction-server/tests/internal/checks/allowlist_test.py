"""Tests for reading the overloads out of a compiled expression."""

import pytest

from datajunction_server.internal.checks import allowlist
from datajunction_server.internal.checks.context import build_env


def _overloads(source):
    return allowlist.called_overloads(build_env().compile(source).serialize())


def test_stdlib_subsetting_is_still_ignored_by_the_library():
    """
    CEL's env config can name which standard-library functions an environment
    allows, which would do this module's job. cel-expr-python accepts that
    block, silently discards it, and still compiles `matches()` -- so the
    permitted set has to be enforced here instead. If this test ever fails, the
    library honors the block and this module can go.
    """
    from cel_expr_python import cel

    config = cel.NewEnvConfigFromYaml(
        "name: subset-probe\n"
        "stdlib:\n"
        "  include:\n"
        "    - name: _&&_\n"
        "variables:\n"
        "  - name: node\n"
        "    type_name: map\n"
        "    params:\n"
        "      - type_name: string\n"
        "      - type_name: dyn\n",
    )
    assert "stdlib" not in config.to_yaml()
    assert cel.NewEnv(config=config).compile("node.name.matches('^x')") is not None


def test_comparison_overloads_are_reported():
    # A dyn receiver leaves size() with every candidate overload unresolved,
    # which is why the allowlist has to name all four of them.
    assert _overloads("node.name == 'x' && size(node.owners) >= 1") == {
        "equals",
        "logical_and",
        "greater_equals_int64",
        "size_list",
        "size_map",
        "size_string",
        "size_bytes",
    }


@pytest.mark.parametrize(
    ("source", "overload"),
    [
        ("node.description.matches('^[A-Z]')", "matches_string"),
        ("node.name.startsWith('default.')", "starts_with_string"),
    ],
)
def test_off_surface_calls_are_visible_in_the_scan(source, overload):
    assert overload in _overloads(source)


def test_macro_expansion_machinery_is_reported():
    # has/all/exists/filter never appear as references, but the comprehensions
    # they expand into do, which is why those overloads must be allowlisted.
    expanded = _overloads(
        "node.tags.all(t, t.tag_type != '')"
        " && size(node.columns.filter(c, c.description != null)) >= 1",
    )
    assert {"not_strictly_false", "add_list", "conditional"} <= expanded


def test_field_walker_skips_every_non_length_delimited_wire_type():
    # Built by hand: CEL does not emit fixed32, fixed64 or group fields in a
    # CheckedExpr, so the walker's skip paths need a synthetic message.
    buf = bytes(
        [
            0x08,
            0x01,  # field 1, varint
            0x15,
            0x00,
            0x00,
            0x00,
            0x00,  # field 2, fixed32
            0x19,
            *([0x00] * 8),  # field 3, fixed64
            0x22,
            0x02,
            0x68,
            0x69,  # field 4, length-delimited "hi"
        ],
    )
    assert list(allowlist._fields(buf)) == [(4, b"hi")]


def test_field_walker_rejects_an_unsupported_wire_type():
    with pytest.raises(ValueError, match="unsupported protobuf wire type 3"):
        list(allowlist._fields(bytes([0x0B])))


def test_multi_byte_varints_decode():
    # Two-byte length prefix, exercising the varint continuation branch.
    payload = b"x" * 200
    buf = bytes([0x0A, 0xC8, 0x01]) + payload
    assert list(allowlist._fields(buf)) == [(1, payload)]
