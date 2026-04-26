"""Tests for GraphQL main module."""

import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from graphql import GraphQLError

from datajunction_server.api.graphql.main import (
    GraphQLErrorReporter,
    get_context,
)
from datajunction_server.instrumentation.provider import (
    MetricsProvider,
    get_metrics_provider,
    set_metrics_provider,
)


class _SpyProvider(MetricsProvider):
    """Records metrics calls so tests can assert against them."""

    def __init__(self) -> None:
        self.counters: list[tuple] = []

    def counter(self, name, value=1, tags=None):
        self.counters.append((name, value, tags))

    def gauge(self, name, value, tags=None):  # pragma: no cover
        pass

    def timer(self, name, value_ms, tags=None):  # pragma: no cover
        pass


@pytest.fixture
def spy_metrics():
    """Swap in a spy provider for the duration of the test."""
    original = get_metrics_provider()
    spy = _SpyProvider()
    set_metrics_provider(spy)
    yield spy
    set_metrics_provider(original)


def _run_on_operation(errors, *, operation_name=None):
    """Drive GraphQLErrorReporter.on_operation to completion with the given errors."""
    extension = GraphQLErrorReporter(execution_context=MagicMock())
    extension.execution_context = MagicMock()
    extension.execution_context.operation_name = operation_name
    if errors is None:
        extension.execution_context.result = None
    else:
        extension.execution_context.result.errors = errors
    gen = extension.on_operation()
    next(gen)  # advance past the pre-execution yield
    with pytest.raises(StopIteration):
        next(gen)  # post-execution body runs, then generator exits


def test_error_reporter_no_result(spy_metrics):
    """When the operation has no result (e.g. early failure), do nothing."""
    with patch("datajunction_server.api.graphql.main.logger") as mock_logger:
        _run_on_operation(None)
    assert spy_metrics.counters == []
    mock_logger.error.assert_not_called()


def test_error_reporter_no_errors(spy_metrics):
    """When result has no errors, do nothing."""
    with patch("datajunction_server.api.graphql.main.logger") as mock_logger:
        _run_on_operation([])
    assert spy_metrics.counters == []
    mock_logger.error.assert_not_called()


def test_error_reporter_python_exception(spy_metrics):
    """A resolver-raised Python exception emits the underlying class name and traceback."""
    try:
        raise ValueError("boom")
    except ValueError as exc:
        original = exc

    error = GraphQLError("boom", path=["findNodes", 0, "name"], original_error=original)

    with patch("datajunction_server.api.graphql.main.logger") as mock_logger:
        _run_on_operation([error], operation_name="LoadDashboard")

    assert spy_metrics.counters == [
        (
            "dj.graphql.errors",
            1,
            {
                "operation": "findNodes",
                "operation_name": "LoadDashboard",
                "error_type": "ValueError",
            },
        ),
    ]
    mock_logger.error.assert_called_once()
    _, kwargs = mock_logger.error.call_args
    assert kwargs["exc_info"] is original


def test_error_reporter_programmatic_error(spy_metrics):
    """A spec-side error with no path / no original / no operation name tags as anonymous."""
    error = GraphQLError("validation failed")

    with patch("datajunction_server.api.graphql.main.logger") as mock_logger:
        _run_on_operation([error])

    assert spy_metrics.counters == [
        (
            "dj.graphql.errors",
            1,
            {
                "operation": "unknown",
                "operation_name": "anonymous",
                "error_type": "GraphQLError",
            },
        ),
    ]
    mock_logger.error.assert_called_once()
    _, kwargs = mock_logger.error.call_args
    assert kwargs["exc_info"] is None


@pytest.mark.asyncio
async def test_resolve_logs_slow_resolvers():
    """Resolvers exceeding the threshold are logged at WARNING with their path."""
    from datajunction_server.api.graphql.main import GraphQLErrorReporter

    extension = GraphQLErrorReporter(execution_context=MagicMock())

    async def slow_next(_root, _info):
        return "ok"

    info = MagicMock()
    info.path.as_list.return_value = ["findNodes", 0, "name"]

    with (
        patch("datajunction_server.api.graphql.main.logger") as mock_logger,
        patch(
            "datajunction_server.api.graphql.main.time.monotonic",
            side_effect=[0.0, 1.0],  # 1000ms elapsed
        ),
    ):
        result = await extension.resolve(slow_next, None, info)

    assert result == "ok"
    mock_logger.warning.assert_called_once()
    args, _ = mock_logger.warning.call_args
    assert args[1] == ["findNodes", 0, "name"]
    assert args[2] == 1000.0


@pytest.mark.asyncio
async def test_resolve_silent_for_fast_resolvers():
    """Fast resolvers do not produce slow-resolver warnings."""
    from datajunction_server.api.graphql.main import GraphQLErrorReporter

    extension = GraphQLErrorReporter(execution_context=MagicMock())

    def sync_next(_root, _info):
        return 42

    info = MagicMock()

    with (
        patch("datajunction_server.api.graphql.main.logger") as mock_logger,
        patch(
            "datajunction_server.api.graphql.main.time.monotonic",
            side_effect=[0.0, 0.001],  # 1ms — well under the threshold
        ),
    ):
        result = await extension.resolve(sync_next, None, info)

    assert result == 42
    mock_logger.warning.assert_not_called()


@pytest.mark.asyncio
@patch("datajunction_server.api.graphql.main.create_node_by_name_loader")
@patch("datajunction_server.api.graphql.main.get_settings")
async def test_get_context_production(mock_get_settings, mock_create_loader):
    """Test get_context in production: no test_session attached."""
    mock_request = MagicMock()
    mock_request.state = MagicMock(spec=[])
    mock_request.app.dependency_overrides = {}

    mock_background_tasks = MagicMock()
    mock_cache = MagicMock()
    mock_settings = MagicMock()
    mock_loader = MagicMock()

    mock_get_settings.return_value = mock_settings
    mock_create_loader.return_value = mock_loader

    context = await get_context(
        request=mock_request,
        background_tasks=mock_background_tasks,
        cache=mock_cache,
    )

    # No shared "session" in context — DataLoaders open their own via
    # session_context() to avoid concurrent-session crashes.
    assert "session" not in context
    assert not hasattr(mock_request.state, "test_session")
    assert context["node_loader"] == mock_loader
    assert context["settings"] == mock_settings
    assert context["request"] == mock_request
    assert context["background_tasks"] == mock_background_tasks
    assert context["cache"] == mock_cache


@pytest.mark.asyncio
@patch("datajunction_server.api.graphql.main.create_node_by_name_loader")
@patch("datajunction_server.api.graphql.main.get_settings")
async def test_get_context_test_override(mock_get_settings, mock_create_loader):
    """Test get_context in tests: shared session is attached from override."""
    from datajunction_server.utils import get_session

    mock_request = MagicMock()
    mock_request.state = MagicMock(spec=[])
    mock_test_session = AsyncMock()
    mock_request.app.dependency_overrides = {get_session: lambda: mock_test_session}

    mock_background_tasks = MagicMock()
    mock_cache = MagicMock()
    mock_get_settings.return_value = MagicMock()
    mock_create_loader.return_value = MagicMock()

    await get_context(
        request=mock_request,
        background_tasks=mock_background_tasks,
        cache=mock_cache,
    )

    assert mock_request.state.test_session is mock_test_session
