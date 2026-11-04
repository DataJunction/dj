"""Metric extraction is shared within a build without sharing mutable results."""

from collections import Counter
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from datajunction_server.construction.build_v3.decomposition import (
    decompose_and_group_metrics,
)
from datajunction_server.construction.build_v3.types import BuildContext
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.dialect import Dialect
from datajunction_server.models.node_type import NodeType
from datajunction_server.sql.decompose import MetricComponentExtractor
from datajunction_server.sql.parsing import ast


def extraction_context(definitions, parents, requested):
    nodes = {"test.fact": SimpleNamespace(name="test.fact", type=NodeType.SOURCE)}
    for revision_id, (name, query) in enumerate(definitions.items(), start=1):
        nodes[name] = SimpleNamespace(
            name=name,
            type=NodeType.METRIC,
            current=SimpleNamespace(
                id=revision_id,
                query=query,
                reaggregate=None,
                fixed_grain=None,
            ),
        )
    return BuildContext(
        session=AsyncMock(),
        metrics=requested,
        dimensions=[],
        nodes=nodes,
        parent_map=parents,
    )


async def extract(ctx, name, cache, dialect=Dialect.SPARK, **options):
    node = ctx.nodes[name]
    return await MetricComponentExtractor(node.current.id, dialect=dialect).extract(
        ctx.session,
        nodes_cache=ctx.nodes,
        parent_map=ctx.parent_map,
        metric_node=node,
        extraction_cache=cache,
        **options,
    )


@pytest.mark.parametrize("depth", [5, 10, 20])
@pytest.mark.parametrize("reverse", [False, True])
async def test_chain_extracts_each_revision_once_per_build(mocker, depth, reverse):
    definitions = {
        f"test.m{i}": (
            "SELECT SUM(amount) FROM test.fact"
            if i == 0
            else f"SELECT test.m{i - 1} + 1"
        )
        for i in range(depth)
    }
    parents = {
        name: ["test.fact" if i == 0 else f"test.m{i - 1}"]
        for i, name in enumerate(definitions)
    }
    ctx = extraction_context(definitions, parents, [f"test.m{depth - 1}"])
    if reverse:
        ctx.nodes = dict(reversed(list(ctx.nodes.items())))
    reads = mocker.spy(MetricComponentExtractor, "_build_metric_data_from_cache")
    base_extractions = mocker.spy(MetricComponentExtractor, "_extract_base")

    for build in range(1, 3):
        groups, decomposed = await decompose_and_group_metrics(ctx)
        assert set(decomposed) == set(definitions)
        assert len(groups) == 1
        assert [m.metric_node.name for m in groups[0].decomposed_metrics] == ["test.m0"]
        assert Counter(call.args[1].name for call in reads.call_args_list) == {
            name: build for name in definitions
        }
        assert base_extractions.call_count == build


async def test_shared_dependency_is_extracted_once(mocker):
    definitions = {
        "test.base": "SELECT SUM(amount) FROM test.fact",
        "test.shared": "SELECT test.base + 1",
        "test.branch_a": "SELECT test.shared * 2",
        "test.branch_b": "SELECT test.shared * 3",
        "test.total": "SELECT test.branch_a + test.branch_b",
    }
    parents = {
        "test.base": ["test.fact"],
        "test.shared": ["test.base"],
        "test.branch_a": ["test.shared"],
        "test.branch_b": ["test.shared"],
        "test.total": ["test.branch_a", "test.branch_b"],
    }
    ctx = extraction_context(definitions, parents, ["test.total", "test.branch_b"])
    reads = mocker.spy(MetricComponentExtractor, "_build_metric_data_from_cache")
    base_extractions = mocker.spy(MetricComponentExtractor, "_extract_base")

    _, decomposed = await decompose_and_group_metrics(ctx)

    assert Counter(call.args[1].name for call in reads.call_args_list) == {
        name: 1 for name in definitions
    }
    assert base_extractions.call_count == 1
    assert len(decomposed["test.total"].components) == 1


async def test_cache_owns_its_components_and_function_subtrees(mocker):
    ctx = extraction_context(
        {"test.base": "SELECT SUM(amount) FROM test.fact"},
        {"test.base": ["test.fact"]},
        ["test.base"],
    )
    reads = mocker.spy(MetricComponentExtractor, "_build_metric_data_from_cache")
    cache = {}
    original_sql = None
    for _ in range(3):
        components, query = await extract(ctx, "test.base", cache)
        if original_sql is None:
            original_sql = str(query)
        assert str(query) == original_sql
        assert components[0].expression == "amount"
        assert components[0].rule.fixed_grain is None
        # Neither the initial result nor subsequent cache hits may mutate the
        # snapshot used by a different root or recursive dependency.
        next(query.find_all(ast.Column)).name = ast.Name("changed")
        components[0].expression = "changed"
        components[0].rule.fixed_grain = ["changed"]
    assert reads.call_count == 1


async def test_cache_separates_revisions(mocker):
    ctx = extraction_context(
        {"test.base": "SELECT SUM(amount) FROM test.fact"},
        {"test.base": ["test.fact"]},
        ["test.base"],
    )
    reads = mocker.spy(MetricComponentExtractor, "_build_metric_data_from_cache")
    cache = {}
    old_components, _ = await extract(ctx, "test.base", cache)
    ctx.nodes["test.base"].current.id += 1
    ctx.nodes["test.base"].current.query = "SELECT COUNT(amount) FROM test.fact"
    for _ in range(2):
        components, _ = await extract(ctx, "test.base", cache)
        assert components[0].aggregation == "COUNT"
    assert old_components[0].aggregation == "SUM"
    assert reads.call_count == 2


async def test_cache_separates_dialects(mocker):
    ctx = extraction_context(
        {"test.base": "SELECT SUM(amount) FROM test.fact"},
        {"test.base": ["test.fact"]},
        ["test.base"],
    )
    reads = mocker.spy(MetricComponentExtractor, "_build_metric_data_from_cache")
    cache = {}
    for dialect in [Dialect.SPARK, Dialect.DRUID, Dialect.SPARK, Dialect.DRUID]:
        await extract(ctx, "test.base", cache, dialect=dialect)
    assert [call.args[0]._dialect for call in reads.call_args_list] == [
        Dialect.SPARK,
        Dialect.DRUID,
    ]


async def test_permissive_cache_cannot_bypass_fixed_grain_validation():
    ctx = extraction_context(
        {"test.base": "SELECT SUM(amount) OVER () FROM test.fact"},
        {"test.base": ["test.fact"]},
        ["test.base"],
    )
    ctx.nodes["test.base"].current.fixed_grain = []
    cache = {}
    await extract(ctx, "test.base", cache, validate_fixed_grain_windows=False)
    with pytest.raises(
        DJInvalidInputException,
        match="own aggregate is already windowed",
    ):
        await extract(ctx, "test.base", cache)


async def test_failed_cycle_is_not_cached():
    ctx = extraction_context(
        {"test.a": "SELECT test.b + 1", "test.b": "SELECT test.a + 1"},
        {"test.a": ["test.b"], "test.b": ["test.a"]},
        ["test.a"],
    )
    cache = {}
    for _ in range(2):
        with pytest.raises(ValueError, match="Circular metric reference detected"):
            await extract(ctx, "test.a", cache)
        assert not cache


async def test_active_path_is_checked_before_a_cache_hit():
    ctx = extraction_context(
        {"test.base": "SELECT SUM(amount) FROM test.fact"},
        {"test.base": ["test.fact"]},
        ["test.base"],
    )
    cache = {}
    await extract(ctx, "test.base", cache)
    with pytest.raises(ValueError, match="Circular metric reference detected"):
        await extract(ctx, "test.base", cache, _visited={"test.base"})
