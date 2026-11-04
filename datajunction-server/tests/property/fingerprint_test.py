"""
Property: a semantic fingerprint changes when a node's meaning changes, and only then.

Editing metadata that does not affect what a node computes (descriptions,
display names, tags, owners, custom metadata, query formatting) must leave the
fingerprint alone, so those edits never look like a data change. Editing the
definition must move it.

`local_node_fingerprint` is a pure function of a spec, so none of this needs a
database.
"""

import re

from hypothesis import given, settings
from hypothesis import strategies as st

from datajunction_server.models.deployment import (
    ColumnSpec,
    CubeSpec,
    DimensionSpec,
    MetricSpec,
    NodeSpec,
    SourceSpec,
    TransformSpec,
)
from datajunction_server.semantic_fingerprints.engine import local_node_fingerprint
from tests.property.budget import examples
from tests.property.comparison import PropertyMismatch
from tests.property.known_issues import (
    COLUMN_METADATA_CHANGES_FINGERPRINT,
    FUNCTION_NAME_CASE_CHANGES_FINGERPRINT,
)

NAMES = st.sampled_from(["a", "b", "c", "d"])
TEXT = st.text(
    alphabet=st.characters(min_codepoint=32, max_codepoint=126),
    max_size=30,
)

QUERIES = [
    "SELECT a, b FROM ns.src",
    "SELECT a, SUM(b) AS total FROM ns.src GROUP BY a",
    "SELECT a FROM ns.src WHERE b > 0",
    "SELECT a, b FROM ns.src WHERE a IS NOT NULL",
]
METRIC_QUERIES = [
    "SELECT SUM(b) FROM ns.src",
    "SELECT COUNT(a) FROM ns.src",
    "SELECT MAX(b) FROM ns.src",
]

columns = st.lists(NAMES, min_size=1, max_size=3, unique=True).map(
    lambda names: [ColumnSpec(name=name, type="int") for name in names],
)


@st.composite
def specs(draw) -> NodeSpec:
    kind = draw(st.sampled_from(["source", "transform", "dimension", "metric", "cube"]))
    if kind == "source":
        return SourceSpec(
            namespace="ns",
            name="node",
            catalog="catalog",
            schema_="schema",
            table="table",
            columns=draw(columns),
        )
    if kind == "transform":
        return TransformSpec(
            namespace="ns",
            name="node",
            query=draw(st.sampled_from(QUERIES)),
        )
    if kind == "dimension":
        return DimensionSpec(
            namespace="ns",
            name="node",
            query=draw(st.sampled_from(QUERIES)),
            primary_key=["a"],
        )
    if kind == "metric":
        return MetricSpec(
            namespace="ns",
            name="node",
            query=draw(st.sampled_from(METRIC_QUERIES)),
        )
    return CubeSpec(
        namespace="ns",
        name="node",
        metrics=draw(
            st.lists(
                st.sampled_from(["ns.m1", "ns.m2", "ns.m3"]),
                min_size=1,
                max_size=3,
                unique=True,
            ),
        ),
        dimensions=draw(
            st.lists(
                st.sampled_from(["ns.d1.x", "ns.d2.y"]),
                min_size=1,
                max_size=2,
                unique=True,
            ),
        ),
    )


metadata_edits = st.fixed_dictionaries(
    {
        "description": st.none() | TEXT,
        "display_name": st.none() | TEXT,
        "tags": st.lists(NAMES, max_size=3, unique=True),
        "owners": st.lists(NAMES, max_size=3, unique=True),
        "custom_metadata": st.none() | st.dictionaries(NAMES, TEXT, max_size=3),
    },
)


def fingerprint(spec: NodeSpec) -> str:
    return local_node_fingerprint(spec).digest


@settings(max_examples=examples(200))
@given(spec=specs(), edit=metadata_edits)
def test_node_metadata_does_not_change_the_fingerprint(spec, edit):
    assert fingerprint(spec.model_copy(update=edit)) == fingerprint(spec)


KEYWORDS = re.compile(r"\b(SELECT|FROM|WHERE|GROUP BY|AS|IS NOT NULL)\b")


def reformat(query: str, spacing: list[str], lower_keywords: bool) -> str:
    """The same query with different whitespace and keyword case."""
    parts = iter(spacing)
    reformatted = re.sub(r" +", lambda _: next(parts, " "), query)
    if lower_keywords:
        reformatted = KEYWORDS.sub(lambda match: match.group(0).lower(), reformatted)
    return reformatted


@settings(max_examples=examples(200))
@given(
    query=st.sampled_from(QUERIES),
    spacing=st.lists(st.sampled_from([" ", "  ", "\n", "\t", " \n "]), min_size=40),
    lower_keywords=st.booleans(),
)
def test_query_formatting_does_not_change_the_fingerprint(
    query,
    spacing,
    lower_keywords,
):
    original = TransformSpec(namespace="ns", name="node", query=query)
    reformatted = TransformSpec(
        namespace="ns",
        name="node",
        query=reformat(query, spacing, lower_keywords),
    )
    assert fingerprint(reformatted) == fingerprint(original)


@settings(max_examples=examples(100))
@given(first=st.sampled_from(QUERIES), second=st.sampled_from(QUERIES))
def test_a_different_query_changes_the_fingerprint(first, second):
    one = TransformSpec(namespace="ns", name="node", query=first)
    two = TransformSpec(namespace="ns", name="node", query=second)
    assert (fingerprint(one) == fingerprint(two)) == (first == second)


@settings(max_examples=examples(100))
@given(
    metrics=st.lists(
        st.sampled_from(["ns.m1", "ns.m2", "ns.m3"]),
        min_size=1,
        max_size=3,
        unique=True,
    ),
    dimensions=st.lists(
        st.sampled_from(["ns.d1.x", "ns.d2.y"]),
        min_size=1,
        max_size=2,
        unique=True,
    ),
    extra_dimension=st.sampled_from(["ns.d3.z", "ns.d4.w"]),
)
def test_a_cube_with_another_dimension_has_a_different_fingerprint(
    metrics,
    dimensions,
    extra_dimension,
):
    base = CubeSpec(namespace="ns", name="node", metrics=metrics, dimensions=dimensions)
    wider = CubeSpec(
        namespace="ns",
        name="node",
        metrics=metrics,
        dimensions=dimensions + [extra_dimension],
    )
    assert fingerprint(base) != fingerprint(wider)


def source_with_column(**column_fields) -> SourceSpec:
    return SourceSpec(
        namespace="ns",
        name="node",
        catalog="catalog",
        schema_="schema",
        table="table",
        columns=[ColumnSpec(name="a", type="int", **column_fields)],
    )


def check_same_fingerprint(one: NodeSpec, two: NodeSpec) -> None:
    if fingerprint(one) != fingerprint(two):
        raise PropertyMismatch("the fingerprint changed")


@COLUMN_METADATA_CHANGES_FINGERPRINT.xfail()
def test_known_issue_column_description_changes_the_fingerprint():
    check_same_fingerprint(
        source_with_column(),
        source_with_column(description="the account id"),
    )


@COLUMN_METADATA_CHANGES_FINGERPRINT.xfail()
def test_known_issue_column_display_name_changes_the_fingerprint():
    check_same_fingerprint(
        source_with_column(),
        source_with_column(display_name="Account"),
    )


@FUNCTION_NAME_CASE_CHANGES_FINGERPRINT.xfail()
def test_known_issue_function_name_case_changes_the_fingerprint():
    check_same_fingerprint(
        TransformSpec(namespace="ns", name="node", query="SELECT SUM(b) FROM ns.src"),
        TransformSpec(namespace="ns", name="node", query="SELECT sum(b) FROM ns.src"),
    )
