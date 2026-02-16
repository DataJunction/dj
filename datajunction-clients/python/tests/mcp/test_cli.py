"""
Tests for MCP CLI entry point
"""

from unittest.mock import MagicMock, patch

import pytest

from datajunction.mcp.cli import main, run


@pytest.mark.asyncio
async def test_main_starts_server():
    """Test that main() initializes and starts the MCP server"""
    mock_read_stream = MagicMock()
    mock_write_stream = MagicMock()

    with (
        patch("datajunction.mcp.cli.stdio_server") as mock_stdio,
        patch("datajunction.mcp.cli.app") as mock_app,
        patch("datajunction.mcp.cli.get_mcp_settings") as mock_settings,
    ):
        # Setup mock settings
        mock_settings.return_value = MagicMock(
            dj_api_url="http://localhost:8000",
        )

        # Setup mock stdio_server context manager
        mock_stdio.return_value.__aenter__.return_value = (
            mock_read_stream,
            mock_write_stream,
        )

        # Setup mock app.run
        mock_app.run.return_value = None

        # Run main
        await main()

        # Verify server was started with correct parameters
        mock_app.run.assert_called_once_with(
            mock_read_stream,
            mock_write_stream,
            mock_app.create_initialization_options(),
        )


def test_run_calls_asyncio_run():
    """Test that run() wrapper properly invokes asyncio.run"""
    with (
        patch("datajunction.mcp.cli.asyncio.run") as mock_asyncio_run,
        patch("datajunction.mcp.cli.main") as mock_main,
    ):
        mock_asyncio_run.return_value = None

        run()

        mock_asyncio_run.assert_called_once_with(mock_main())


def test_run_handles_keyboard_interrupt():
    """Test that run() handles KeyboardInterrupt gracefully"""
    with (
        patch("datajunction.mcp.cli.asyncio.run") as mock_asyncio_run,
        patch("datajunction.mcp.cli.logger") as mock_logger,
    ):
        mock_asyncio_run.side_effect = KeyboardInterrupt()

        # Should not raise exception
        run()

        # Verify it logged the message
        mock_logger.info.assert_called_with("MCP Server stopped by user")


def test_run_handles_exception():
    """Test that run() handles exceptions and exits with code 1"""
    with (
        patch("datajunction.mcp.cli.asyncio.run") as mock_asyncio_run,
        patch("datajunction.mcp.cli.logger") as mock_logger,
        patch("sys.exit") as mock_exit,
    ):
        mock_asyncio_run.side_effect = Exception("Test error")

        run()

        # Verify error was logged
        mock_logger.error.assert_called_once()
        assert "Test error" in str(mock_logger.error.call_args)

        # Verify sys.exit was called with code 1
        mock_exit.assert_called_once_with(1)


def test_cli_entry_point_exists():
    """Test that the dj-mcp entry point is properly configured"""
    # This test verifies that the module can be imported and run() exists
    from datajunction.mcp.cli import run

    assert callable(run)
    assert run.__module__ == "datajunction.mcp.cli"
