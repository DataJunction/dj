"""Generated metric DAGs have the same extraction with a cold or warm cache."""

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from datajunction_server.sql.parsing import ast
from tests.construction.build_v3.metric_extraction_test import (
    extract,
    extraction_context,
)
from tests.property.budget import examples
from tests.property.interaction_scenarios import formula_dags


@pytest.mark.asyncio
@settings(max_examples=examples(30))
@given(formulas=formula_dags(), reverse=st.booleans())
async def test_cached_extraction_is_order_independent_and_owns_results(
    formulas,
    reverse,
):
    names = ["test.a", "test.b"] + [f"test.d{i}" for i in range(len(formulas))]
    definitions = {
        "test.a": "SELECT SUM(x) FROM test.fact",
        "test.b": "SELECT AVG(y) FROM test.fact",
    }
    parents = {name: ["test.fact"] for name in names[:2]}
    for name, formula in zip(names[2:], formulas):
        a, b = names[formula.left], names[formula.right]
        definitions[name] = f"SELECT {a} {formula.op} {b}"
        parents[name] = list(dict.fromkeys([a, b]))
    ctx = extraction_context(definitions, parents, names)
    cache = {}
    for name in names[::-1] if reverse else names:
        cold_components, cold_query = await extract(ctx, name, None)
        for _ in range(2):
            components, query = await extract(ctx, name, cache)
            assert components == cold_components
            assert str(query) == str(cold_query)
            # Corrupt only the caller-owned result, then read the cache again.
            next(query.find_all(ast.Column)).name = ast.Name("corrupted")
            components[0].expression = "corrupted"
    assert len(cache) == len(names)
