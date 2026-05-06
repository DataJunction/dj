"""
Tests for ``datajunction_server.mcp.tools``.

Covers every tool function in the MCP surface — input handling, formatting,
the threshold-refusal guard, and integration with the underlying internal
services (mocked at the seam). Real-data integration tests live in
``test_list_namespaces.py``; this file uses stubs so it can exercise every
branch deterministically.
"""

from typing import Any
from unittest.mock import MagicMock

import pytest
import pytest_asyncio

from datajunction_server.construction.build_v3.types import GeneratedSQL
from datajunction_server.mcp import context, tools
from datajunction_server.models.dialect import Dialect
from datajunction_server.models.node_type import NodeType as NodeTypeEnum
from datajunction_server.models.sql import ScanEstimate
from datajunction_server.sql.parsing.backends.antlr4 import parse


# ---------------------------------------------------------------------------
# tools._format_bytes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value,expected",
    [
        (None, "unknown"),
        (0, "0.0 B"),
        (512, "512.0 B"),
        (2048, "2.0 KB"),
        (5 * 1024**2, "5.0 MB"),
        (3 * 1024**3, "3.0 GB"),
        (2 * 1024**4, "2.0 TB"),
    ],
)
def test_format_bytes(value: Any, expected: str) -> None:
    assert tools._format_bytes(value) == expected


# ---------------------------------------------------------------------------
# tools.visualize_metrics + get_metric_data happy paths
# ---------------------------------------------------------------------------


def _stub_dataset(rows: list[list[Any]], col_names: list[str]) -> Any:
    """Build a stub QueryWithResults-like object that ``_execute_metrics_query``
    returns. Mirrors the shape ``get_metric_data`` and ``visualize_metrics`` read.
    """

    class _Col:
        def __init__(self, name: str) -> None:
            self.name = name

    class _ResultBlock:
        def __init__(self) -> None:
            self.columns = [_Col(n) for n in col_names]
            self.rows = rows

    class _Results:
        root = [_ResultBlock()]

    result = MagicMock()
    result.results = _Results()
    result.state = "FINISHED"
    result.id = "q-123"
    result.errors = None
    return result


@pytest_asyncio.fixture
async def bound_session_for_tools(monkeypatch):
    """Bind a contextvar session value (any object — tools we test here
    only call internals we stub out)."""
    token = context._session_var.set(MagicMock())  # type: ignore[arg-type]
    try:
        yield
    finally:
        context._session_var.reset(token)


@pytest.mark.asyncio
async def test_get_metric_data_happy_path(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """Stub ``_execute_metrics_query`` to return a small dataset; verify
    the rendered text contains every row in the expected layout."""
    rows = [["2024-01-01", 10], ["2024-01-02", 20]]
    fake_result = _stub_dataset(rows, ["date", "amount"])

    fake_generated = MagicMock()
    fake_generated.scan_estimate = ScanEstimate(
        total_bytes=42_000_000,
        sources=[],
        has_materialization=False,
    )

    async def fake_execute(*args, **kwargs):
        return fake_result, fake_generated

    monkeypatch.setattr(tools, "_execute_metrics_query", fake_execute)

    out = await tools.get_metric_data(metrics=["m"], dimensions=["d"])
    expected = (
        "Query Results:\n" + "=" * 60 + "\n"
        "\n"
        "Source: ad-hoc (scan ~40.1 MB)\n"
        "\n"
        "Query State: FINISHED\n"
        "Query ID: q-123\n"
        "Row Count: 2\n"
        "\n"
        "Data:\n" + "-" * 60 + "\n"
        "Row 1:\n"
        "  date: 2024-01-01\n"
        "  amount: 10\n"
        "\n"
        "Row 2:\n"
        "  date: 2024-01-02\n"
        "  amount: 20\n"
    )
    assert out == expected


@pytest.mark.asyncio
async def test_get_metric_data_materialized_source_label(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """When the cube hit is materialized, the source label says so."""
    fake_result = _stub_dataset([["x", 1]], ["dim", "amount"])

    fake_generated = MagicMock()
    fake_generated.scan_estimate = ScanEstimate(
        total_bytes=None,
        sources=[],
        has_materialization=True,
    )

    async def fake_execute(*args, **kwargs):
        return fake_result, fake_generated

    monkeypatch.setattr(tools, "_execute_metrics_query", fake_execute)

    out = await tools.get_metric_data(metrics=["m"])
    assert "Source: materialized cube" in out
    assert "Source: ad-hoc" not in out


@pytest.mark.asyncio
async def test_visualize_metrics_renders_chart(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """Happy path — stubbed dataset → plotext renders → the tool returns
    a single TextContent block with the chart and a metadata footer."""
    rows = [
        ["20240101", 10],
        ["20240102", 20],
        ["20240103", 15],
    ]
    fake_result = _stub_dataset(rows, ["date", "amount"])

    fake_generated = MagicMock()
    fake_generated.scan_estimate = MagicMock(
        total_bytes=1024,
        has_materialization=True,
    )

    async def fake_execute(*args, **kwargs):
        return fake_result, fake_generated

    monkeypatch.setattr(tools, "_execute_metrics_query", fake_execute)

    blocks = await tools.visualize_metrics(
        metrics=["my.amount"],
        dimensions=["d.date"],
        chart_type="line",
    )

    # Single block, text type, ends with the metadata footer we hard-code.
    assert len(blocks) == 1
    assert blocks[0].type == "text"
    text = blocks[0].text
    assert text.endswith("📊 amount | 3 points | line")
    # Chart body is non-empty (plotext output).
    assert text.count("\n") > 5


@pytest.mark.asyncio
async def test_visualize_metrics_categorical_x_axis_switches_to_bar(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """Non-numeric, non-date x values trigger automatic switch to bar chart."""
    rows = [["alpha", 5], ["beta", 7], ["gamma", 3]]
    fake_result = _stub_dataset(rows, ["region", "amount"])

    fake_generated = MagicMock()
    fake_generated.scan_estimate = MagicMock(
        total_bytes=1024,
        has_materialization=True,
    )

    async def fake_execute(*args, **kwargs):
        return fake_result, fake_generated

    monkeypatch.setattr(tools, "_execute_metrics_query", fake_execute)

    blocks = await tools.visualize_metrics(
        metrics=["my.amount"],
        dimensions=["d.region"],
        chart_type="line",  # asked for line, should auto-switch
    )
    text = blocks[0].text
    assert text.endswith("📊 amount | 3 points | bar")


@pytest.mark.asyncio
async def test_visualize_metrics_no_results_returns_friendly_message(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """When the query returns no result rows, surface a helpful message
    instead of crashing on empty data."""

    class _EmptyResults:
        root: list = []

    fake_result = MagicMock()
    fake_result.results = _EmptyResults()
    fake_generated = MagicMock()

    async def fake_execute(*args, **kwargs):
        return fake_result, fake_generated

    monkeypatch.setattr(tools, "_execute_metrics_query", fake_execute)

    blocks = await tools.visualize_metrics(metrics=["m"], dimensions=["d"])
    assert len(blocks) == 1
    assert (
        blocks[0].text
        == "No results returned from query. Cannot generate visualization."
    )


@pytest.mark.asyncio
async def test_visualize_metrics_empty_rows_returns_friendly_message(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """A result block with zero rows is functionally the same as no results."""
    fake_result = _stub_dataset([], ["dim", "amount"])
    fake_generated = MagicMock()

    async def fake_execute(*args, **kwargs):
        return fake_result, fake_generated

    monkeypatch.setattr(tools, "_execute_metrics_query", fake_execute)

    blocks = await tools.visualize_metrics(metrics=["m"], dimensions=["d"])
    assert (
        blocks[0].text
        == "No data rows returned from query. Cannot generate visualization."
    )


@pytest.mark.asyncio
async def test_visualize_metrics_threshold_refusal(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """``visualize_metrics`` surfaces the scan-cost ValueError as text."""

    async def fake_execute(*args, **kwargs):
        raise ValueError("would scan too much")

    monkeypatch.setattr(tools, "_execute_metrics_query", fake_execute)

    blocks = await tools.visualize_metrics(metrics=["m"], dimensions=["d"])
    assert len(blocks) == 1
    assert blocks[0].text == ("⚠️  would scan too much\n\nMetrics: m\nDimensions: d")


@pytest.mark.asyncio
async def test_visualize_metrics_dateint_int_dimension(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """Numeric dateints (>19000000) are parsed as dates and labelled MM/DD when many."""
    rows = [[20240100 + i, i] for i in range(15)]  # 15 points → triggers date xticks
    fake_result = _stub_dataset(rows, ["dateint", "amount"])
    fake_generated = MagicMock()

    async def fake_execute(*args, **kwargs):
        return fake_result, fake_generated

    monkeypatch.setattr(tools, "_execute_metrics_query", fake_execute)

    blocks = await tools.visualize_metrics(
        metrics=["m.amount"],
        dimensions=["d.dateint"],
    )
    assert blocks[0].text.endswith("📊 amount | 15 points | line")


@pytest.mark.asyncio
async def test_visualize_metrics_numeric_x_axis(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """Plain numeric x values (not dates) take the float-cast branch."""
    rows = [[float(i), i * 2] for i in range(5)]
    fake_result = _stub_dataset(rows, ["x", "amount"])
    fake_generated = MagicMock()

    async def fake_execute(*args, **kwargs):
        return fake_result, fake_generated

    monkeypatch.setattr(tools, "_execute_metrics_query", fake_execute)

    blocks = await tools.visualize_metrics(
        metrics=["m.amount"],
        dimensions=["d.x"],
        chart_type="scatter",
    )
    assert blocks[0].text.endswith("📊 amount | 5 points | scatter")


@pytest.mark.asyncio
async def test_visualize_metrics_no_dimensions_picks_first_non_metric(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """Without ``dimensions``, the tool picks the first non-metric column as x."""
    rows = [["a", 1], ["b", 2]]
    fake_result = _stub_dataset(rows, ["region", "amount"])
    fake_generated = MagicMock()

    async def fake_execute(*args, **kwargs):
        return fake_result, fake_generated

    monkeypatch.setattr(tools, "_execute_metrics_query", fake_execute)

    blocks = await tools.visualize_metrics(metrics=["m.amount"])
    assert blocks[0].text.endswith("📊 amount | 2 points | bar")


@pytest.mark.asyncio
async def test_visualize_metrics_multiple_metrics_with_y_min(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """Multiple metrics → legend; y_min set → fixed lower bound."""
    rows = [["20240101", 1, 10], ["20240102", 2, 20], ["20240103", 3, 30]]
    fake_result = _stub_dataset(rows, ["d", "a", "b"])
    fake_generated = MagicMock()

    async def fake_execute(*args, **kwargs):
        return fake_result, fake_generated

    monkeypatch.setattr(tools, "_execute_metrics_query", fake_execute)

    blocks = await tools.visualize_metrics(
        metrics=["x.a", "x.b"],
        dimensions=["d.d"],
        y_min=0,
        title="Two metrics",
    )
    assert blocks[0].text.endswith("📊 a, b | 3 points | line")


@pytest.mark.asyncio
async def test_visualize_metrics_skips_metric_with_no_valid_data(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """A metric whose every row is None/non-numeric is skipped silently."""
    rows = [["a", "junk"], ["b", "junk"], ["c", "junk"]]
    fake_result = _stub_dataset(rows, ["x", "amount"])
    fake_generated = MagicMock()

    async def fake_execute(*args, **kwargs):
        return fake_result, fake_generated

    monkeypatch.setattr(tools, "_execute_metrics_query", fake_execute)

    blocks = await tools.visualize_metrics(
        metrics=["m.amount"],
        dimensions=["d.x"],
    )
    # Chart still produced (with categorical x labels) but no metric series.
    assert blocks[0].text.endswith("📊 amount | 3 points | bar")


# ---------------------------------------------------------------------------
# tools.list_namespaces — empty path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_namespaces_no_rows(bound_session_for_tools, monkeypatch) -> None:
    """When the namespace query returns no rows, emit a fixed message."""
    fake_session = MagicMock()
    result = MagicMock()
    result.all = MagicMock(return_value=[])

    async def fake_execute(stmt):
        return result

    fake_session.execute = fake_execute
    monkeypatch.setattr(tools, "get_mcp_session", lambda: fake_session)

    out = await tools.list_namespaces()
    assert out == "No namespaces found."


# ---------------------------------------------------------------------------
# tools._node_to_dict — git=False and git=None branches
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_node_to_dict_without_git(monkeypatch) -> None:
    """``include_git=False`` short-circuits the namespace lookup entirely."""
    node = MagicMock()
    node.name = "n.foo"
    node.type = MagicMock()
    node.type.value = "metric"
    node.current = None
    node.tags = []
    node.owners = []

    out = await tools._node_to_dict(node, include_git=False)
    assert out == {
        "name": "n.foo",
        "type": "metric",
        "current": {},
        "tags": [],
        "owners": [],
    }


@pytest.mark.asyncio
async def test_node_to_dict_with_git_lookup_returning_none(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """``include_git=True`` with no git config registered returns the node
    dict unchanged (no ``gitInfo`` key)."""

    async def fake_get_git(session, namespace):
        return None

    monkeypatch.setattr(tools, "get_git_info_for_namespace", fake_get_git)

    node = MagicMock()
    node.name = "n.foo"
    node.type = MagicMock()
    node.type.value = "metric"
    node.namespace = "n"
    node.current = None
    node.tags = []
    node.owners = []

    out = await tools._node_to_dict(node, include_git=True)
    assert "gitInfo" not in out


# ---------------------------------------------------------------------------
# tools.search_nodes — namespace.main resolution
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_search_nodes_resolves_to_main_branch(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """When ``namespace`` doesn't end in a branch suffix and ``<ns>.main``
    exists, the search re-targets to ``<ns>.main``."""
    fake_session = MagicMock()

    async def fake_scalar(stmt):
        return "finance.main"

    fake_session.scalar = fake_scalar

    captured_namespace = {}

    async def fake_find_by(session, **kwargs):
        captured_namespace["value"] = kwargs.get("namespace")
        return []

    monkeypatch.setattr(tools, "get_mcp_session", lambda: fake_session)
    monkeypatch.setattr(tools.Node, "find_by", fake_find_by)

    out = await tools.search_nodes(namespace="finance")
    assert captured_namespace["value"] == "finance.main"
    assert out == "No nodes found."


# ---------------------------------------------------------------------------
# tools.get_node_details — branches not hit by the integration test
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_node_details_node_not_found(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    async def fake_get_by_name(session, name, options=None):
        return None

    monkeypatch.setattr(tools.Node, "get_by_name", fake_get_by_name)
    out = await tools.get_node_details("does.not.exist")
    assert out == "Node 'does.not.exist' not found."


@pytest.mark.asyncio
async def test_get_node_details_with_metric_metadata_unit(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """Cover the metric-metadata branch where ``unit`` is set on the
    underlying NodeRevision; format_node_details renders the Unit line."""
    direction = MagicMock()
    direction.value = "higher_is_better"

    unit_value = MagicMock()
    unit_value.name = "usd"
    unit_value.label = "US Dollars"

    metric_meta = MagicMock()
    metric_meta.direction = direction
    metric_meta.unit = MagicMock(value=unit_value)

    current = MagicMock()
    current.display_name = "Revenue"
    current.description = "Daily revenue"
    current.status = MagicMock(value="valid")
    current.mode = MagicMock(value="published")
    current.query = "SELECT 1"
    current.columns = []
    current.parents = []
    current.metric_metadata = metric_meta

    node = MagicMock()
    node.name = "n.revenue"
    node.namespace = "n"
    node.type = NodeTypeEnum.METRIC  # filled in below
    node.current = current
    node.tags = []
    node.owners = []

    async def fake_get_by_name(session, name, options=None):
        return node

    async def fake_get_dims(session, node, with_attributes=True):
        return []

    async def fake_get_git(session, namespace):
        return None

    monkeypatch.setattr(tools.Node, "get_by_name", fake_get_by_name)
    monkeypatch.setattr(tools, "get_dimensions", fake_get_dims)
    monkeypatch.setattr(tools, "get_git_info_for_namespace", fake_get_git)

    out = await tools.get_node_details("n.revenue")
    assert "Direction: higher_is_better" in out
    assert "Unit: US Dollars" in out


@pytest.mark.asyncio
async def test_get_node_details_cube_skips_dimension_lookup(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """Cube nodes don't have queryable dimensions in the same sense — the
    tool skips the get_dimensions call entirely."""
    current = MagicMock()
    current.display_name = None
    current.description = None
    current.status = MagicMock(value="valid")
    current.mode = MagicMock(value="published")
    current.query = ""
    current.columns = []
    current.parents = []
    current.metric_metadata = None

    node = MagicMock()
    node.name = "n.cube"
    node.namespace = "n"
    node.type = NodeTypeEnum.CUBE
    node.current = current
    node.tags = []
    node.owners = []

    get_dims_called = []

    async def fake_get_by_name(session, name, options=None):
        return node

    async def fake_get_dims(session, node, with_attributes=True):
        get_dims_called.append(True)
        return []

    async def fake_get_git(session, namespace):
        return None

    monkeypatch.setattr(tools.Node, "get_by_name", fake_get_by_name)
    monkeypatch.setattr(tools, "get_dimensions", fake_get_dims)
    monkeypatch.setattr(tools, "get_git_info_for_namespace", fake_get_git)

    await tools.get_node_details("n.cube")
    assert get_dims_called == []  # cube branch must not call get_dimensions


# ---------------------------------------------------------------------------
# tools.get_common — branches
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_common_rejects_both_args(bound_session_for_tools) -> None:
    out = await tools.get_common(metrics=["m"], dimensions=["d"])
    assert out == "Error: provide either 'metrics' or 'dimensions', not both."


@pytest.mark.asyncio
async def test_get_common_metric_lookup_finds_nothing(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """Every metric name fails to resolve → friendly message, not a crash."""

    async def fake_get_by_name(session, name, options=None):
        return None

    monkeypatch.setattr(tools.Node, "get_by_name", fake_get_by_name)
    out = await tools.get_common(metrics=["a", "b"])
    assert out == "No metrics found among: a, b"


@pytest.mark.asyncio
async def test_get_common_by_dimensions_branch(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """The dimensions→metrics branch returns each compatible metric on its own line."""
    dim_node = MagicMock()
    dim_node.name = "d.country"

    metric_node = MagicMock()
    metric_node.name = "x.revenue"

    async def fake_get_by_name(session, name, options=None):
        return dim_node

    async def fake_get_nodes_with_common(session, dim_nodes, node_types=None):
        return [metric_node]

    monkeypatch.setattr(tools.Node, "get_by_name", fake_get_by_name)
    monkeypatch.setattr(
        tools,
        "get_nodes_with_common_dimensions",
        fake_get_nodes_with_common,
    )

    out = await tools.get_common(dimensions=["d.country"])
    assert out == (
        "Metrics compatible with dimensions:\n" + "=" * 60 + "\n"
        "\n"
        "Dimensions: d.country\n"
        "\n"
        "Found 1 compatible metric(s):\n"
        "\n"
        "  • x.revenue"
    )


@pytest.mark.asyncio
async def test_get_common_by_dimensions_no_compatible_metrics(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    dim_node = MagicMock()

    async def fake_get_by_name(session, name, options=None):
        return dim_node

    async def fake_get_nodes_with_common(session, dim_nodes, node_types=None):
        return []

    monkeypatch.setattr(tools.Node, "get_by_name", fake_get_by_name)
    monkeypatch.setattr(
        tools,
        "get_nodes_with_common_dimensions",
        fake_get_nodes_with_common,
    )

    out = await tools.get_common(dimensions=["d.country"])
    assert out.endswith("No metrics found that share all specified dimensions.")


@pytest.mark.asyncio
async def test_get_common_by_dimensions_when_dim_lookup_finds_nothing(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """Dim names that don't resolve → friendly message."""

    async def fake_get_by_name(session, name, options=None):
        return None

    monkeypatch.setattr(tools.Node, "get_by_name", fake_get_by_name)
    out = await tools.get_common(dimensions=["a", "b"])
    assert out == "No dimension nodes found among: a, b"


# ---------------------------------------------------------------------------
# tools._execute_metrics_query — submit + no-query-service branches
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_execute_metrics_query_no_query_service_configured(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """Without a query service client, ``_execute_metrics_query`` raises
    a ValueError that the public tool surfaces as an error message."""

    async def fake_resolve(*args, **kwargs):
        ctx = MagicMock()
        ctx.dialect = Dialect.SPARK
        # QueryCreate is a Pydantic model — engine_name/version must be
        # real strings, not MagicMock attribute auto-vivifications.
        engine_stub = MagicMock()
        engine_stub.name = "spark"
        engine_stub.version = "3.5"
        ctx.engine = engine_stub
        ctx.catalog_name = "default"
        return ctx

    async def fake_build(*args, **kwargs):
        return GeneratedSQL(
            query=parse("SELECT 1"),
            columns=[],
            dialect=Dialect.SPARK,
            scan_estimate=ScanEstimate(
                total_bytes=100,
                sources=[],
                has_materialization=True,
            ),
        )

    monkeypatch.setattr(
        "datajunction_server.construction.build_v3.cube_matcher.resolve_dialect_and_engine_for_metrics",
        fake_resolve,
    )
    monkeypatch.setattr(tools, "build_metrics_sql", fake_build)
    monkeypatch.setattr(tools, "get_query_service_client", lambda settings: None)

    out = await tools.get_metric_data(metrics=["m"])
    assert "Query service client is not configured" in out


# ---------------------------------------------------------------------------
# tools.get_metric_data — extra branches
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_metric_data_truncates_after_ten_rows(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """More than 10 rows → first 10 + "... and N more rows" tail."""
    rows = [[i, i * 10] for i in range(15)]
    fake_result = _stub_dataset(rows, ["x", "amount"])
    fake_generated = MagicMock()
    fake_generated.scan_estimate = None  # exercises the "no estimate" branch

    async def fake_execute(*args, **kwargs):
        return fake_result, fake_generated

    monkeypatch.setattr(tools, "_execute_metrics_query", fake_execute)

    out = await tools.get_metric_data(metrics=["m"])
    assert "Row 1:" in out
    assert "Row 10:" in out
    assert "Row 11:" not in out
    assert "... and 5 more rows" in out


@pytest.mark.asyncio
async def test_get_metric_data_surfaces_query_errors(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """Any errors on the query result are appended at the end of the output."""
    fake_result = _stub_dataset([["a", 1]], ["dim", "amount"])
    fake_result.errors = ["boom", "another"]
    fake_generated = MagicMock()
    fake_generated.scan_estimate = None

    async def fake_execute(*args, **kwargs):
        return fake_result, fake_generated

    monkeypatch.setattr(tools, "_execute_metrics_query", fake_execute)

    out = await tools.get_metric_data(metrics=["m"])
    assert out.endswith("Errors:\n  • boom\n  • another")


@pytest.mark.asyncio
async def test_get_metric_data_no_rows(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """Empty results block → ``No results returned`` line."""

    class _Empty:
        root: list = []

    fake_result = MagicMock()
    fake_result.results = _Empty()
    fake_result.state = "FINISHED"
    fake_result.id = None
    fake_result.errors = None

    fake_generated = MagicMock()
    fake_generated.scan_estimate = None

    async def fake_execute(*args, **kwargs):
        return fake_result, fake_generated

    monkeypatch.setattr(tools, "_execute_metrics_query", fake_execute)

    out = await tools.get_metric_data(metrics=["m"])
    assert "No results returned" in out


# ---------------------------------------------------------------------------
# tools.get_query_plan — branches not exercised by the integration test
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_query_plan_with_no_components_and_no_dimensions(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """A grain group with no components, no requested dimensions, and a
    derived metric exercises the `derived` tag and the empty-components
    branch in one shot.
    """
    component = MagicMock()
    component.name = "comp1"
    component.expression = "SUM(x)"
    component.aggregation = "sum"
    component.merge = "sum"

    decomposed = MagicMock()
    decomposed.is_derived_for_parents = MagicMock(return_value=True)
    decomposed.metric_node.current.query = "SELECT 1"
    decomposed.components = []  # empty → no Components: line

    grain = MagicMock()
    grain.metrics = ["m1"]
    grain.grain = []  # empty → "none"
    grain.aggregability = MagicMock(value="FULL")
    grain.parent_name = "n.fact"
    grain.components = []
    grain.sql = "SELECT 1"

    result = MagicMock()
    result.dialect = MagicMock(value="spark")
    result.requested_dimensions = []  # empty → "none"
    result.grain_groups = [grain]
    result.decomposed_metrics = {"m1": decomposed}
    result.ctx.parent_map = {}
    result.ctx.nodes = {}

    async def fake_build_measures(**kwargs):
        return result

    monkeypatch.setattr(tools, "build_measures_sql", fake_build_measures)

    out = await tools.get_query_plan(metrics=["m1"])
    assert "[derived]" in out
    assert "Dimensions: none" in out
    assert "Grain:           none" in out
    # Empty components list in decomposed → no "Components: " line in formula block
    assert out.count("Components:") == 0


# ---------------------------------------------------------------------------
# tools.get_node_lineage — direction-specific paths
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_node_lineage_upstream_only(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    upstream_node = MagicMock()
    upstream_node.name = "n.parent"
    upstream_node.type = MagicMock(value="source")
    upstream_node.current = MagicMock(status=MagicMock(value="valid"))

    async def fake_upstream(session, node_name):
        return [upstream_node]

    async def fake_downstream(session, node_name, depth=-1):
        # Should never be called when direction='upstream'.
        raise AssertionError("downstream walked when only upstream was requested")

    monkeypatch.setattr(tools, "get_upstream_nodes", fake_upstream)
    monkeypatch.setattr(tools, "get_downstream_nodes", fake_downstream)

    out = await tools.get_node_lineage("n.foo", direction="upstream")
    assert "Upstream Dependencies (1 nodes):" in out
    assert "Downstream Dependencies" not in out


@pytest.mark.asyncio
async def test_get_node_lineage_downstream_only_empty(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    async def fake_upstream(session, node_name):
        raise AssertionError("upstream walked when only downstream was requested")

    async def fake_downstream(session, node_name, depth=-1):
        return []

    monkeypatch.setattr(tools, "get_upstream_nodes", fake_upstream)
    monkeypatch.setattr(tools, "get_downstream_nodes", fake_downstream)

    out = await tools.get_node_lineage("n.foo", direction="downstream", max_depth=2)
    assert "Downstream Dependencies (0 nodes):" in out
    assert "(none)" in out


@pytest.mark.asyncio
async def test_get_node_lineage_node_without_current(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """A node with no current revision should still render — status falls
    back to N/A rather than crashing."""
    node = MagicMock()
    node.name = "n.broken"
    node.type = MagicMock(value="metric")
    node.current = None  # no current revision

    async def fake_upstream(session, node_name):
        return [node]

    async def fake_downstream(session, node_name, depth=-1):
        return []

    monkeypatch.setattr(tools, "get_upstream_nodes", fake_upstream)
    monkeypatch.setattr(tools, "get_downstream_nodes", fake_downstream)

    out = await tools.get_node_lineage("n.foo", direction="upstream")
    assert "n.broken (metric) - N/A" in out


# ---------------------------------------------------------------------------
# tools.get_node_dimensions — branches
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_node_dimensions_node_not_found(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    async def fake_get_by_name(session, name, options=None):
        return None

    monkeypatch.setattr(tools.Node, "get_by_name", fake_get_by_name)
    out = await tools.get_node_dimensions("does.not.exist")
    assert out == "Node 'does.not.exist' not found."


@pytest.mark.asyncio
async def test_get_node_dimensions_no_dims(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    node = MagicMock()
    node.name = "n.foo"

    async def fake_get_by_name(session, name, options=None):
        return node

    async def fake_get_dims(session, node, with_attributes=True):
        return []

    monkeypatch.setattr(tools.Node, "get_by_name", fake_get_by_name)
    monkeypatch.setattr(tools, "get_dimensions", fake_get_dims)

    out = await tools.get_node_dimensions("n.foo")
    assert "(no dimensions available)" in out


@pytest.mark.asyncio
async def test_get_node_dimensions_with_type_and_path(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """When dimensions carry a type and a path, both render in the output."""
    node = MagicMock()

    dim = MagicMock()
    dim.name = "d.region"
    dim.type = "string"
    dim.path = ["fact", "customer", "region"]

    async def fake_get_by_name(session, name, options=None):
        return node

    async def fake_get_dims(session, node, with_attributes=True):
        return [dim]

    monkeypatch.setattr(tools.Node, "get_by_name", fake_get_by_name)
    monkeypatch.setattr(tools, "get_dimensions", fake_get_dims)

    out = await tools.get_node_dimensions("n.foo")
    assert "  • d.region (string) - via: fact → customer → region" in out


# ---------------------------------------------------------------------------
# Final coverage — branches that need direct stubbing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_node_to_dict_with_git_info_populated(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """When ``get_git_info_for_namespace`` returns a real dict, the
    ``gitInfo`` block on the output dict is populated. Covers line 79."""

    async def fake_git(session, namespace):
        return {
            "github_repo_path": "org/repo",
            "git_path": None,
            "git_branch": "main",
            "default_branch": "main",
        }

    monkeypatch.setattr(tools, "get_git_info_for_namespace", fake_git)

    node = MagicMock()
    node.name = "n.foo"
    node.namespace = "n"
    node.type = MagicMock()
    node.type.value = "metric"
    node.current = None
    node.tags = []
    node.owners = []

    out = await tools._node_to_dict(node, include_git=True)
    assert out["gitInfo"] == {
        "repo": "org/repo",
        "branch": "main",
        "defaultBranch": "main",
    }


@pytest.mark.asyncio
async def test_search_nodes_namespace_main_branch_missing_keeps_original(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """When ``<ns>.main`` doesn't exist, the search keeps the original
    namespace. Covers the branch where ``exists`` is None."""
    fake_session = MagicMock()

    async def fake_scalar(stmt):
        return None  # no .main namespace registered

    fake_session.scalar = fake_scalar

    captured = {}

    async def fake_find_by(session, **kwargs):
        captured["namespace"] = kwargs.get("namespace")
        return []

    monkeypatch.setattr(tools, "get_mcp_session", lambda: fake_session)
    monkeypatch.setattr(tools.Node, "find_by", fake_find_by)

    await tools.search_nodes(namespace="finance")
    assert captured["namespace"] == "finance"


@pytest.mark.asyncio
async def test_get_node_details_node_with_no_current_revision(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """A node with ``node.current = None`` still renders — the augment
    block is skipped. Covers the 202->237 branch (skipping the if)."""
    node = MagicMock()
    node.name = "n.no_current"
    node.namespace = "n"
    node.type = NodeTypeEnum.SOURCE
    node.current = None
    node.tags = []
    node.owners = []

    async def fake_get_by_name(session, name, options=None):
        return node

    async def fake_get_dims(session, node, with_attributes=True):
        return []

    async def fake_git(session, namespace):
        return None

    monkeypatch.setattr(tools.Node, "get_by_name", fake_get_by_name)
    monkeypatch.setattr(tools, "get_dimensions", fake_get_dims)
    monkeypatch.setattr(tools, "get_git_info_for_namespace", fake_git)

    out = await tools.get_node_details("n.no_current")
    assert out.startswith("Node: n.no_current\nType: source\n")


@pytest.mark.asyncio
async def test_get_node_dimensions_with_type_only(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """A dimension with ``type`` set but no ``path`` covers the 690->692
    branch (type → True, path → False)."""
    node = MagicMock()

    dim = MagicMock(spec=["name", "type", "path"])
    dim.name = "d.country"
    dim.type = "string"
    dim.path = None  # path branch off

    async def fake_get_by_name(session, name, options=None):
        return node

    async def fake_get_dims(session, node, with_attributes=True):
        return [dim]

    monkeypatch.setattr(tools.Node, "get_by_name", fake_get_by_name)
    monkeypatch.setattr(tools, "get_dimensions", fake_get_dims)

    out = await tools.get_node_dimensions("n.foo")
    assert "  • d.country (string)" in out
    assert "via:" not in out


@pytest.mark.asyncio
async def test_get_node_dimensions_with_path_only(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """A dimension with ``path`` set but no ``type`` covers the 693->695
    branch (type → False, path → True)."""
    node = MagicMock()

    dim = MagicMock(spec=["name", "type", "path"])
    dim.name = "d.country"
    dim.type = None
    dim.path = ["fact", "customer", "country"]

    async def fake_get_by_name(session, name, options=None):
        return node

    async def fake_get_dims(session, node, with_attributes=True):
        return [dim]

    monkeypatch.setattr(tools.Node, "get_by_name", fake_get_by_name)
    monkeypatch.setattr(tools, "get_dimensions", fake_get_dims)

    out = await tools.get_node_dimensions("n.foo")
    assert "  • d.country - via: fact → customer → country" in out


@pytest.mark.asyncio
async def test_execute_metrics_query_full_round_trip(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """Cover lines 451-461: a stubbed query service receives the
    ``QueryCreate`` payload and returns rows; the columns get injected
    into the result block."""

    submitted: list = []

    class _FakeQueryServiceClient:
        def submit_query(self, query_create):
            submitted.append(query_create)
            return _stub_dataset([["a", 1]], ["dim", "amount"])

    async def fake_resolve(*args, **kwargs):
        ctx = MagicMock()
        ctx.dialect = Dialect.SPARK
        # QueryCreate is a Pydantic model — engine_name/version must be
        # real strings, not MagicMock attribute auto-vivifications.
        engine_stub = MagicMock()
        engine_stub.name = "spark"
        engine_stub.version = "3.5"
        ctx.engine = engine_stub
        ctx.catalog_name = "default"
        return ctx

    async def fake_build(*args, **kwargs):
        return GeneratedSQL(
            query=parse("SELECT 1"),
            columns=[],
            dialect=Dialect.SPARK,
            scan_estimate=ScanEstimate(
                total_bytes=100,
                sources=[],
                has_materialization=True,
            ),
        )

    monkeypatch.setattr(
        "datajunction_server.construction.build_v3.cube_matcher.resolve_dialect_and_engine_for_metrics",
        fake_resolve,
    )
    monkeypatch.setattr(tools, "build_metrics_sql", fake_build)
    monkeypatch.setattr(
        tools,
        "get_query_service_client",
        lambda settings: _FakeQueryServiceClient(),
    )

    out = await tools.get_metric_data(metrics=["m"])
    assert "Source: materialized cube" in out
    assert "Row 1:" in out
    assert len(submitted) == 1


@pytest.mark.asyncio
async def test_visualize_metrics_dates_with_unparseable_mixed(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """A mix of parseable and unparseable date strings exercises the
    fallback-to-index path inside the date-extraction loop. Covers
    lines 810->813 and 820."""
    rows = [
        ["20240101", 1],
        ["not-a-date", 2],  # falls back to index
        ["20240103", 3],
    ]
    fake_result = _stub_dataset(rows, ["d", "amount"])
    fake_generated = MagicMock()

    async def fake_execute(*args, **kwargs):
        return fake_result, fake_generated

    monkeypatch.setattr(tools, "_execute_metrics_query", fake_execute)

    blocks = await tools.visualize_metrics(
        metrics=["m.amount"],
        dimensions=["d.d"],
    )
    assert blocks[0].text.endswith("📊 amount | 3 points | line")


@pytest.mark.asyncio
async def test_execute_metrics_query_empty_results_skips_column_inject(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """``submit_query`` returns a result with no root → column-injection
    branch (459) doesn't fire. Covers 459->461."""

    class _EmptyResults:
        root: list = []

    class _FakeQueryServiceClient:
        def submit_query(self, query_create):
            r = MagicMock()
            r.results = _EmptyResults()
            r.state = "FINISHED"
            r.id = "q-empty"
            r.errors = None
            return r

    async def fake_resolve(*args, **kwargs):
        ctx = MagicMock()
        ctx.dialect = Dialect.SPARK
        engine_stub = MagicMock()
        engine_stub.name = "spark"
        engine_stub.version = "3.5"
        ctx.engine = engine_stub
        ctx.catalog_name = "default"
        return ctx

    async def fake_build(*args, **kwargs):
        return GeneratedSQL(
            query=parse("SELECT 1"),
            columns=[],
            dialect=Dialect.SPARK,
            scan_estimate=ScanEstimate(
                total_bytes=10,
                sources=[],
                has_materialization=True,
            ),
        )

    monkeypatch.setattr(
        "datajunction_server.construction.build_v3.cube_matcher.resolve_dialect_and_engine_for_metrics",
        fake_resolve,
    )
    monkeypatch.setattr(tools, "build_metrics_sql", fake_build)
    monkeypatch.setattr(
        tools,
        "get_query_service_client",
        lambda settings: _FakeQueryServiceClient(),
    )

    out = await tools.get_metric_data(metrics=["m"])
    assert "No results returned" in out


@pytest.mark.asyncio
async def test_visualize_metrics_categorical_keeps_bar_chart_type(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """Categorical x-axis + chart_type='bar' (not line) → no auto-switch.
    Covers 810->813 branch (the if condition is False)."""
    rows = [["alpha", 5], ["beta", 7]]
    fake_result = _stub_dataset(rows, ["region", "amount"])
    fake_generated = MagicMock()

    async def fake_execute(*args, **kwargs):
        return fake_result, fake_generated

    monkeypatch.setattr(tools, "_execute_metrics_query", fake_execute)

    blocks = await tools.visualize_metrics(
        metrics=["m.amount"],
        dimensions=["d.region"],
        chart_type="bar",
    )
    # No auto-switch needed since we asked for bar.
    assert blocks[0].text.endswith("📊 amount | 2 points | bar")


@pytest.mark.asyncio
async def test_visualize_metrics_handles_none_metric_values(
    bound_session_for_tools,
    monkeypatch,
) -> None:
    """A metric value of None preserves the gap (line 820: y_numeric.append(None))."""
    rows: list[list[Any]] = [
        ["20240101", 10],
        ["20240102", None],  # gap
        ["20240103", 15],
    ]
    fake_result = _stub_dataset(rows, ["d", "amount"])
    fake_generated = MagicMock()

    async def fake_execute(*args, **kwargs):
        return fake_result, fake_generated

    monkeypatch.setattr(tools, "_execute_metrics_query", fake_execute)

    blocks = await tools.visualize_metrics(
        metrics=["m.amount"],
        dimensions=["d.d"],
    )
    assert blocks[0].text.endswith("📊 amount | 3 points | line")
