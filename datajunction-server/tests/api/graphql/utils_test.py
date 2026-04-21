from types import SimpleNamespace

import pytest
from strawberry.types.nodes import FragmentSpread, InlineFragment, SelectedField

from datajunction_server.api.graphql.utils import (
    convert_camel_case,
    dedupe_append,
    extract_fields,
)


def _field(name: str, *subselections) -> SelectedField:
    return SelectedField(
        name=name,
        directives={},
        arguments={},
        alias=None,
        selections=list(subselections),
    )


def _info(*top_level_selections) -> SimpleNamespace:
    """Build a minimal stand-in for a Strawberry ``Info``."""
    return SimpleNamespace(
        selected_fields=[_field("findNodes", *top_level_selections)],
    )


@pytest.mark.parametrize(
    "input_str, expected",
    [
        ("camelCase", "camel_case"),
        ("CamelCase", "camel_case"),
        ("HttpRequest", "http_request"),
        ("getUrlFromHtml", "get_url_from_html"),
        ("already_snake_case", "already_snake_case"),
        ("single", "single"),
        ("", ""),
    ],
)
def test_convert_camel_case(input_str, expected):
    assert convert_camel_case(input_str) == expected


@pytest.mark.parametrize(
    "base, extras, expected",
    [
        (["a", "b"], ["c", "d"], ["a", "b", "c", "d"]),  # no duplicates
        (["a", "b"], ["b", "c"], ["a", "b", "c"]),  # some duplicates
        (["a", "b"], ["a", "b"], ["a", "b"]),  # all duplicates
        ([], ["x", "y"], ["x", "y"]),  # empty base
        (["x", "y"], [], ["x", "y"]),  # empty extras
        ([], [], []),  # both empty
    ],
)
def test_dedupe_append(base, extras, expected):
    assert dedupe_append(base, extras) == expected


class TestExtractFields:
    """
    Fragment spreads and inline fragments must be flattened into the parent
    selection — otherwise the conditional eager-loading in ``find_nodes_by``
    misses fields that were only requested via a fragment and ``noload``s the
    relationships those fields need.
    """

    def test_inline_fields(self):
        """Baseline: plain inline fields produce a simple nested dict."""
        info = _info(
            _field("name"),
            _field("type"),
            _field("current", _field("mode")),
        )
        assert extract_fields(info) == {
            "name": None,
            "type": None,
            "current": {"mode": None},
        }

    def test_fragment_spread_matches_inline(self):
        """``...NodeInfo`` must produce the same output as listing its fields inline."""
        fields = [
            _field("name"),
            _field("type"),
            _field("current", _field("mode")),
        ]
        fragment = FragmentSpread(
            name="NodeInfo",
            type_condition="Node",
            directives={},
            selections=fields,
        )

        inline = extract_fields(_info(*fields))
        via_fragment = extract_fields(_info(fragment))
        assert via_fragment == inline
        # Sanity: the fragment's name must not leak in as a field.
        assert "node_info" not in via_fragment

    def test_inline_fragment_flattens(self):
        """``... on Type { ... }`` is also flattened into the parent."""
        inline_frag = InlineFragment(
            type_condition="Node",
            directives={},
            selections=[_field("name"), _field("type")],
        )
        assert extract_fields(
            _info(inline_frag, _field("current", _field("mode"))),
        ) == {
            "name": None,
            "type": None,
            "current": {"mode": None},
        }

    def test_fields_mixed_with_fragment(self):
        """Fields alongside a fragment spread merge cleanly."""
        fragment = FragmentSpread(
            name="NodeInfo",
            type_condition="Node",
            directives={},
            selections=[_field("name"), _field("type")],
        )
        assert extract_fields(_info(fragment, _field("current", _field("mode")))) == {
            "name": None,
            "type": None,
            "current": {"mode": None},
        }

    def test_nested_fragment_in_subselection(self):
        """A fragment used inside a sub-selection is flattened at that level."""
        inner_fragment = FragmentSpread(
            name="RevisionInfo",
            type_condition="NodeRevision",
            directives={},
            selections=[_field("mode"), _field("status")],
        )
        assert extract_fields(_info(_field("current", inner_fragment))) == {
            "current": {"mode": None, "status": None},
        }

    def test_camel_case_fields_converted(self):
        """Field names are snake_cased (existing behavior, preserved)."""
        assert extract_fields(_info(_field("displayName"), _field("createdBy"))) == {
            "display_name": None,
            "created_by": None,
        }

    def test_duplicate_scalar_field_deduplicates(self):
        """``{ name name }`` resolves to a single ``name`` entry, not a crash."""
        assert extract_fields(_info(_field("name"), _field("name"))) == {"name": None}

    def test_duplicate_object_field_merges_subselections(self):
        """
        ``{ current { mode } current { status } }`` must merge subselections.
        Without the merge, the second occurrence overwrites the first and the
        eager-loader misses ``mode``.
        """
        assert extract_fields(
            _info(
                _field("current", _field("mode")),
                _field("current", _field("status")),
            ),
        ) == {
            "current": {"mode": None, "status": None},
        }

    def test_overlapping_fragments_merge(self):
        """
        Two fragments whose selections on the same field overlap must merge —
        same failure mode as the duplicate-object-field case above, reached
        through fragment spreads instead of direct repetition.
        """
        frag_a = FragmentSpread(
            name="A",
            type_condition="Node",
            directives={},
            selections=[_field("current", _field("mode"))],
        )
        frag_b = FragmentSpread(
            name="B",
            type_condition="Node",
            directives={},
            selections=[_field("current", _field("status"))],
        )
        assert extract_fields(_info(frag_a, frag_b)) == {
            "current": {"mode": None, "status": None},
        }

    def test_merge_preserves_nested_siblings(self):
        """Deep merge should combine grandchildren, not just top-level keys."""
        assert extract_fields(
            _info(
                _field("current", _field("columns", _field("name"))),
                _field("current", _field("columns", _field("type"))),
            ),
        ) == {
            "current": {"columns": {"name": None, "type": None}},
        }
