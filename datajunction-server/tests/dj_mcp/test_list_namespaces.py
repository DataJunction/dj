"""
Smoke tests for the server-side MCP transport and the ported
``list_namespaces`` tool. Validates that:

1. The tool function produces the expected formatted output for known
   namespace data (unit-level test against the test DB).
2. The HTTP MCP endpoint responds to ``tools/list`` and ``tools/call``
   requests with the same wire format the stdio transport produces.
"""

import httpx
import pytest
import pytest_asyncio
from httpx import ASGITransport
from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamablehttp_client

from datajunction_server.mcp import tools
from datajunction_server.mcp.context import _session_var


@pytest.mark.asyncio
async def test_list_namespaces_tool_against_test_db(
    module__client_with_all_examples,
) -> None:
    """Call ``tools.list_namespaces`` directly with the test DB session.

    This exercises the same query the HTTP path runs but skips the MCP
    transport — fast unit-level coverage of the tool logic.
    """
    # Borrow a session from the test app's session manager. The client
    # fixture has already loaded examples, so namespaces should exist.
    from datajunction_server.utils import get_session_manager

    session_factory = get_session_manager().get_writer_session_factory()
    async with session_factory() as session:
        token = _session_var.set(session)
        try:
            output = await tools.list_namespaces()
        finally:
            _session_var.reset(token)

    # Spot-check rather than asserting the full string — the namespace set
    # is shared across many tests and could grow.
    assert output.startswith("Available Namespaces:")
    assert "• default" in output
    assert "• basic" in output


@pytest_asyncio.fixture
async def mcp_http_factory(module__client_with_all_examples):
    """An ``httpx_client_factory`` that backs MCP requests with the test app.

    ``streamablehttp_client`` calls this to create an ``AsyncClient`` for
    its HTTP requests. Pointing the transport at the in-process FastAPI
    app means we exercise the full mount + ASGI handler without binding
    a real port.
    """
    test_client = module__client_with_all_examples
    fastapi_app = test_client.app

    def factory(headers=None, timeout=None, auth=None):
        return httpx.AsyncClient(
            transport=ASGITransport(app=fastapi_app),
            base_url="http://test",
            headers=headers,
            timeout=timeout if timeout is not None else httpx.Timeout(30.0),
            auth=auth,
            follow_redirects=True,
        )

    return factory


@pytest_asyncio.fixture
async def bound_session(module__client_with_all_examples):
    """A session bound to the MCP context for direct tool calls.

    Yields the session so individual tests can use it for additional
    DB access if needed; the contextvar is bound for the duration.
    """
    from datajunction_server.utils import get_session_manager

    session_factory = get_session_manager().get_writer_session_factory()
    async with session_factory() as session:
        token = _session_var.set(session)
        try:
            yield session
        finally:
            _session_var.reset(token)


@pytest.mark.asyncio
async def test_get_node_details_tool(bound_session) -> None:
    output = await tools.get_node_details("default.num_repair_orders")
    assert "Node: default.num_repair_orders" in output
    assert "Type: metric" in output


@pytest.mark.asyncio
async def test_get_node_details_missing(bound_session) -> None:
    output = await tools.get_node_details("does.not.exist")
    assert output == "Node 'does.not.exist' not found."


@pytest.mark.asyncio
async def test_get_node_dimensions_tool(bound_session) -> None:
    output = await tools.get_node_dimensions("default.repair_orders_fact")
    assert "Dimensions for: default.repair_orders_fact" in output
    assert "Available Dimensions:" in output


@pytest.mark.asyncio
async def test_get_node_lineage_tool(bound_session) -> None:
    output = await tools.get_node_lineage("default.num_repair_orders")
    assert output.startswith("Lineage for: default.num_repair_orders")
    assert "Upstream Dependencies" in output
    assert "Downstream Dependencies" in output


@pytest.mark.asyncio
async def test_get_common_metrics_to_dimensions(bound_session) -> None:
    output = await tools.get_common(metrics=["default.num_repair_orders"])
    assert "Dimension Compatibility Analysis" in output


@pytest.mark.asyncio
async def test_get_common_requires_one_arg(bound_session) -> None:
    output = await tools.get_common()
    assert output == "Error: provide either 'metrics' or 'dimensions'."


@pytest.mark.asyncio
async def test_build_metric_sql_tool(bound_session) -> None:
    output = await tools.build_metric_sql(
        metrics=["default.num_repair_orders"],
        dimensions=["default.hard_hat.state"],
        limit=5,
    )
    assert output.startswith("Generated SQL Query:")
    assert "SQL:" in output
    assert "Output Columns:" in output


@pytest.mark.asyncio
async def test_get_query_plan_tool(bound_session) -> None:
    output = await tools.get_query_plan(
        metrics=["default.num_repair_orders"],
        dimensions=["default.hard_hat.state"],
    )
    assert output.startswith("Query Execution Plan")
    assert "Metric Formulas" in output
    assert "Grain Groups" in output


@pytest.mark.asyncio
async def test_get_metric_data_runs_when_under_scan_threshold(
    bound_session,
    monkeypatch,
) -> None:
    """``get_metric_data`` no longer refuses non-materialized queries — it
    runs them against the catalog's default engine (Trino in real deploys)
    as long as the scan estimate is under the threshold. With the query
    service stubbed out we just verify that we got past the materialization
    check and tried to submit.
    """
    from datajunction_server.mcp import tools as mcp_tools

    def fake_get_query_service_client(settings=None):
        return None

    monkeypatch.setattr(
        mcp_tools,
        "get_query_service_client",
        fake_get_query_service_client,
    )

    output = await tools.get_metric_data(
        metrics=["default.num_repair_orders"],
        dimensions=["default.hard_hat.state"],
    )
    assert "Query service client is not configured" in output


@pytest.mark.asyncio
async def test_get_metric_data_refuses_when_scan_exceeds_threshold(
    bound_session,
    monkeypatch,
) -> None:
    """When the scan estimate is over ``_AD_HOC_SCAN_LIMIT_BYTES`` we
    refuse before ever calling the query service. Stub ``build_metrics_sql``
    to return a deliberately huge ad-hoc estimate.
    """
    from datajunction_server.mcp import tools as mcp_tools
    from datajunction_server.models.dialect import Dialect
    from datajunction_server.models.sql import ScanEstimate
    from datajunction_server.sql.parsing.backends.antlr4 import parse

    big_estimate = ScanEstimate(
        total_bytes=mcp_tools._AD_HOC_SCAN_LIMIT_BYTES * 2,
        sources=[],
        has_materialization=False,
    )

    async def fake_build_metrics_sql(*args, **kwargs):
        from datajunction_server.construction.build_v3.types import GeneratedSQL

        return GeneratedSQL(
            query=parse("SELECT 1"),
            columns=[],
            dialect=Dialect.SPARK,
            scan_estimate=big_estimate,
        )

    monkeypatch.setattr(mcp_tools, "build_metrics_sql", fake_build_metrics_sql)

    output = await tools.get_metric_data(
        metrics=["default.num_repair_orders"],
        dimensions=["default.hard_hat.state"],
    )
    assert "over the safety threshold" in output
    assert "2.0 TB" in output  # 2× the 1 TB limit


@pytest.mark.asyncio
async def test_visualize_metrics_refuses_when_scan_exceeds_threshold(
    bound_session,
    monkeypatch,
) -> None:
    """``visualize_metrics`` shares the scan-cost guard with
    ``get_metric_data`` — a query that would scan too much data refuses
    rather than rendering a chart on incomplete results.
    """
    from datajunction_server.mcp import tools as mcp_tools
    from datajunction_server.models.dialect import Dialect
    from datajunction_server.models.sql import ScanEstimate
    from datajunction_server.sql.parsing.backends.antlr4 import parse

    big_estimate = ScanEstimate(
        total_bytes=mcp_tools._AD_HOC_SCAN_LIMIT_BYTES * 2,
        sources=[],
        has_materialization=False,
    )

    async def fake_build_metrics_sql(*args, **kwargs):
        from datajunction_server.construction.build_v3.types import GeneratedSQL

        return GeneratedSQL(
            query=parse("SELECT 1"),
            columns=[],
            dialect=Dialect.SPARK,
            scan_estimate=big_estimate,
        )

    monkeypatch.setattr(mcp_tools, "build_metrics_sql", fake_build_metrics_sql)

    result = await tools.visualize_metrics(
        metrics=["default.num_repair_orders"],
        dimensions=["default.hard_hat.state"],
    )
    assert len(result) == 1
    assert "over the safety threshold" in result[0].text


@pytest.mark.asyncio
async def test_search_nodes_tool_against_test_db(
    module__client_with_all_examples,
) -> None:
    """``search_nodes`` returns a formatted list pulled from Node.find_by."""
    from datajunction_server.utils import get_session_manager

    session_factory = get_session_manager().get_writer_session_factory()
    async with session_factory() as session:
        token = _session_var.set(session)
        try:
            output = await tools.search_nodes(
                node_type="metric",
                limit=5,
            )
        finally:
            _session_var.reset(token)

    # The metric examples are scattered across multiple namespaces — at
    # least the section header should appear and we should see at least
    # one metric in the output.
    assert output.startswith("Found")
    assert "(metric)" in output


@pytest.mark.asyncio
async def test_mcp_http_lists_namespaces(
    module__client_with_all_examples,
    mcp_http_factory,
) -> None:
    """Drive the /mcp endpoint with the official MCP client SDK.

    Confirms (a) the route is mounted, (b) the streamable-HTTP wire
    format works, (c) ``list_namespaces`` round-trips through the same
    code path the Slack agent and Claude Desktop will exercise.
    """
    async with streamablehttp_client(
        url="http://test/mcp",
        httpx_client_factory=mcp_http_factory,
    ) as (read_stream, write_stream, _get_session_id):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()

            # tools/list should return the registered tool surface.
            tool_list = await session.list_tools()
            tool_names = {t.name for t in tool_list.tools}
            assert "list_namespaces" in tool_names
            assert "search_nodes" in tool_names  # full surface registered

            # tools/call list_namespaces should produce the same string
            # the stdio transport would.
            result = await session.call_tool("list_namespaces", arguments={})
            assert not result.isError, f"tool errored: {result.content}"
            assert len(result.content) == 1
            text = result.content[0].text
            assert text.startswith("Available Namespaces:")
            assert "• default" in text
