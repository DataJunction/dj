"""
Tests for ``datajunction_server.utils``.
"""

import logging
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pytest_mock import MockerFixture
from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, AsyncEngine
from starlette.background import BackgroundTasks
from testcontainers.postgres import PostgresContainer
from yarl import URL

from datajunction_server.config import DatabaseConfig, Settings
from datajunction_server.database.user import OAuthProvider, User
from datajunction_server.errors import DJDatabaseException, DJException
from datajunction_server.utils import (
    DatabaseSessionManager,
    Version,
    execute_with_retry,
    get_and_update_current_user,
    get_issue_url,
    get_query_service_client,
    get_session,
    get_settings,
    setup_logging,
)


def test_setup_logging() -> None:
    """
    Test ``setup_logging``.
    """
    setup_logging("debug")
    assert logging.root.level == logging.DEBUG

    with pytest.raises(ValueError) as excinfo:
        setup_logging("invalid")
    assert str(excinfo.value) == "Invalid log level: invalid"


@pytest.mark.asyncio
async def test_get_session(mocker: MockerFixture) -> None:
    """
    Test ``get_session``.
    """
    with patch(
        "fastapi.BackgroundTasks",
        mocker.MagicMock(autospec=BackgroundTasks),
    ) as background_tasks:
        background_tasks.side_effect = lambda x, y: None
        session = await anext(get_session(request=mocker.MagicMock()))
        assert isinstance(session, AsyncSession)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method,expected_session_attr",
    [
        ("GET", "reader_session"),
        ("POST", "writer_session"),
    ],
)
async def test_get_session_uses_correct_session(method, expected_session_attr):
    """
    Ensure get_session uses reader_session for GET and writer_session for others.
    """
    mock_reader = AsyncMock(spec=AsyncSession)
    mock_writer = AsyncMock(spec=AsyncSession)

    mock_reader_callable = MagicMock(return_value=mock_reader)
    mock_writer_callable = MagicMock(return_value=mock_writer)

    mock_session_manager = SimpleNamespace(
        reader_session=mock_reader_callable,
        writer_session=mock_writer_callable,
    )

    with patch(
        "datajunction_server.utils.get_session_manager",
        return_value=mock_session_manager,
    ):
        request = MagicMock()
        request.method = method

        session = await anext(get_session(request))
        if expected_session_attr == "reader_session":
            mock_reader_callable.assert_called_once()
            mock_writer_callable.assert_not_called()
            assert session is mock_reader
        else:
            mock_writer_callable.assert_called_once()
            mock_reader_callable.assert_not_called()
            assert session is mock_writer


def test_get_settings(mocker: MockerFixture) -> None:
    """
    Test ``get_settings``.
    """
    mocker.patch("datajunction_server.utils.load_dotenv")
    Settings = mocker.patch(
        "datajunction_server.utils.Settings",
    )

    # should be already cached, since it's called by the Celery app
    get_settings()
    Settings.assert_not_called()


def test_get_issue_url() -> None:
    """
    Test ``get_issue_url``.
    """
    assert get_issue_url() == URL(
        "https://github.com/DataJunction/dj/issues/new",
    )
    assert get_issue_url(
        baseurl=URL("https://example.org/"),
        title="Title with spaces",
        body="This is the body",
        labels=["help", "troubleshoot"],
    ) == URL(
        "https://example.org/?title=Title+with+spaces&"
        "body=This+is+the+body&labels=help,troubleshoot",
    )


def test_database_session_manager(
    mocker: MockerFixture,
    settings: Settings,
    postgres_container: PostgresContainer,
) -> None:
    """
    Test ``get_engine``.
    """
    connection_url = postgres_container.get_connection_url()
    settings.writer_db = DatabaseConfig(uri=connection_url)
    mocker.patch("datajunction_server.utils.get_settings", return_value=settings)

    session_manager = DatabaseSessionManager()
    session_manager.init_db()

    writer_engine = cast(AsyncEngine, session_manager.writer_engine)
    writer_engine.pool.size() == settings.writer_db.pool_size
    writer_engine.pool.timeout() == settings.writer_db.pool_timeout
    writer_engine.pool.overflow() == settings.writer_db.max_overflow

    reader_engine = cast(AsyncEngine, session_manager.reader_engine)
    reader_engine.pool.size() == settings.reader_db.pool_size
    reader_engine.pool.timeout() == settings.reader_db.pool_timeout
    reader_engine.pool.overflow() == settings.reader_db.max_overflow


def test_get_query_service_client(mocker: MockerFixture, settings: Settings) -> None:
    """
    Test ``get_query_service_client``.
    """
    settings.query_service = "http://query_service:8001"
    mocker.patch("datajunction_server.utils.get_settings", return_value=settings)
    query_service_client = get_query_service_client()
    assert query_service_client.uri == "http://query_service:8001"  # type: ignore


def test_version_parse() -> None:
    """
    Test version parsing
    """
    ver = Version.parse("v1.0")
    assert ver.major == 1
    assert ver.minor == 0
    assert str(ver.next_major_version()) == "v2.0"
    assert str(ver.next_minor_version()) == "v1.1"
    assert str(ver.next_minor_version().next_minor_version()) == "v1.2"

    ver = Version.parse("v21.12")
    assert ver.major == 21
    assert ver.minor == 12
    assert str(ver.next_major_version()) == "v22.0"
    assert str(ver.next_minor_version()) == "v21.13"
    assert str(ver.next_minor_version().next_minor_version()) == "v21.14"
    assert str(ver.next_major_version().next_minor_version()) == "v22.1"

    with pytest.raises(DJException) as excinfo:
        Version.parse("0")
    assert str(excinfo.value) == "Unparseable version 0!"


@pytest.mark.asyncio
async def test_get_and_update_current_user(session: AsyncSession):
    """
    Test upserting the current user
    """
    example_user = User(
        id=1,
        username="userfoo",
        password="passwordfoo",
        name="djuser",
        email="userfoo@datajunction.io",
        oauth_provider=OAuthProvider.BASIC,
    )

    # Confirm that the current user is returned after upserting
    current_user = await get_and_update_current_user(
        session=session,
        current_user=example_user,
    )
    assert current_user.id == example_user.id
    assert current_user.username == example_user.username

    # Confirm that the user was upserted
    result = await session.execute(select(User).where(User.username == "userfoo"))
    found_user = result.unique().scalar_one_or_none()
    assert found_user.id == 1
    assert found_user.username == "userfoo"
    assert (
        found_user.password is None
    )  # If the user is added via upsert, auth is externally managed
    assert found_user.name == "djuser"
    assert found_user.email == "userfoo@datajunction.io"
    assert found_user.oauth_provider == "basic"


@pytest.mark.asyncio
async def test_execute_with_retry_success_after_flaky_connection():
    """
    Test that execute_with_retry succeeds after a flaky connection.
    """
    session = AsyncMock(spec=AsyncSession)
    statement = MagicMock()

    # Simulate flaky DB: first 2 calls raise OperationalError, 3rd returns success
    mock_result = MagicMock()
    mock_result.unique.return_value.scalars.return_value.all.return_value = [
        "node1",
        "node2",
    ]
    session.execute.side_effect = [
        OperationalError("flaky", None, None),
        OperationalError("still flaky", None, None),
        mock_result,
    ]

    result = await execute_with_retry(session, statement, retries=5, base_delay=0.01)
    values = result.unique().scalars().all()
    assert values == ["node1", "node2"]
    assert session.execute.call_count == 3


@pytest.mark.asyncio
async def test_execute_with_retry_exhausts_retries():
    """
    Test that execute_with_retry exhausts retries and fails.
    """
    session = AsyncMock(spec=AsyncSession)
    statement = MagicMock()

    # Always fail
    session.execute.side_effect = OperationalError("permanent fail", None, None)

    with pytest.raises(DJDatabaseException):
        await execute_with_retry(session, statement, retries=3, base_delay=0.01)

    assert session.execute.call_count == 4  # initial try + 3 retries
