"""
Tests for FastAPI app wiring in `datajunction_server.api.main`.
"""

import logging
from http import HTTPStatus
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from httpx import ASGITransport, AsyncClient
from starlette.requests import ClientDisconnect

from datajunction_server.errors import DJException


def _install_unhandled_handler(app: FastAPI) -> None:
    """Re-register the same generic Exception handler `configure_app`
    installs in production. Kept inline so the test isolates the handler's
    behaviour from the rest of the app bootstrap."""

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(
        request: Request,
        exc: Exception,
    ) -> JSONResponse:
        logging.getLogger("datajunction_server.api.main").exception(
            "Unhandled %s in %s %s",
            type(exc).__name__,
            request.method,
            request.url.path,
        )
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal Server Error"},
        )


def _install_dj_exception_handler(app: FastAPI) -> None:
    """Minimal DJException handler matching production behaviour."""

    @app.exception_handler(DJException)
    async def dj_exception_handler(
        request: Request,
        exc: DJException,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=exc.http_status_code,
            content=exc.to_dict(),
        )


@pytest.mark.asyncio
async def test_unhandled_exception_returns_500_with_default_body(caplog):
    """A non-DJException escaping a route is caught by the global handler,
    returning the same 500 body FastAPI's default handler would emit. The
    response body must NOT leak the exception's string representation."""
    app = FastAPI()
    _install_unhandled_handler(app)

    @app.get("/boom")
    async def _boom():
        raise RuntimeError("secret internal detail that must not leak")

    with caplog.at_level(logging.ERROR, logger="datajunction_server.api.main"):
        async with AsyncClient(
            transport=ASGITransport(app=app, raise_app_exceptions=False),
            base_url="http://t",
        ) as client:
            response = await client.get("/boom")

    assert response.status_code == 500
    assert response.json() == {"detail": "Internal Server Error"}
    # Crucially, no internal detail leaked into the response body.
    assert "secret internal detail" not in response.text


@pytest.mark.asyncio
async def test_unhandled_exception_log_message_names_class_and_route(caplog):
    """The log message must name the real exception class and the offending
    route so Radar's `formattedMessage` field is self-documenting instead
    of the generic "Exception in ASGI application"."""
    app = FastAPI()
    _install_unhandled_handler(app)

    @app.post("/widgets/{name}/restore")
    async def _restore(name: str):
        raise KeyError(0)

    with caplog.at_level(logging.ERROR, logger="datajunction_server.api.main"):
        async with AsyncClient(
            transport=ASGITransport(app=app, raise_app_exceptions=False),
            base_url="http://t",
        ) as client:
            await client.post("/widgets/foo/restore")

    messages = [
        rec.getMessage()
        for rec in caplog.records
        if rec.name == "datajunction_server.api.main"
    ]
    assert any(
        "Unhandled KeyError in POST /widgets/foo/restore" in m for m in messages
    ), f"expected typed log line, got: {messages}"


@pytest.mark.asyncio
async def test_dj_exception_handler_still_wins_over_generic(caplog):
    """The DJException handler is registered separately; the generic
    Exception handler must not swallow DJException subclasses. (FastAPI
    dispatches to the most-specific handler first.)"""
    app = FastAPI()
    _install_unhandled_handler(app)
    _install_dj_exception_handler(app)

    from datajunction_server.errors import DJInvalidInputException

    @app.get("/typed-error")
    async def _typed():
        raise DJInvalidInputException("bad input")

    async with AsyncClient(
        transport=ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://t",
    ) as client:
        response = await client.get("/typed-error")

    # DJException handler runs (HTTP 422 for DJInvalidInputException) —
    # NOT the generic 500 path.
    assert response.status_code == DJInvalidInputException.http_status_code
    body = response.json()
    # DJException's to_dict produces a structured "message" field.
    assert body.get("message") == "bad input"


@pytest.mark.asyncio
async def test_client_disconnect_handler_logs_warning() -> None:
    """``ClientDisconnect`` is logged at WARNING and returns 204 instead of
    bubbling up as a 500 server error.
    """
    from datajunction_server.api.main import app

    handler = app.exception_handlers[ClientDisconnect]

    request = MagicMock()
    request.method = "POST"
    request.url.path = "/graphql"

    with patch("datajunction_server.api.main._logger") as mock_logger:
        response = await handler(request, ClientDisconnect())

    assert response.status_code == HTTPStatus.NO_CONTENT
    mock_logger.warning.assert_called_once()
    mock_logger.error.assert_not_called()
    mock_logger.exception.assert_not_called()
