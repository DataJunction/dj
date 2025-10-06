"""
Tests for ``datajunction_server.utils``.
"""

import logging
from unittest.mock import AsyncMock, MagicMock, patch
import json
import pytest
from starlette.requests import Request
from starlette.datastructures import Headers
from starlette.types import Scope

import pytest
from pytest_mock import MockerFixture
from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.background import BackgroundTasks
from testcontainers.postgres import PostgresContainer
from yarl import URL

from datajunction_server.config import DatabaseConfig, Settings
from datajunction_server.database.user import OAuthProvider, User
from datajunction_server.errors import (
    DJDatabaseException,
    DJException,
    DJUninitializedResourceException,
)
from datajunction_server.utils import (
    DatabaseSessionManager,
    Version,
    execute_with_retry,
    get_and_update_current_user,
    get_issue_url,
    get_query_service_client,
    get_legacy_query_service_client,
    get_session,
    get_session_manager,
    get_settings,
    setup_logging,
    is_graphql_query,
    _create_configured_query_client,
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
    get_session_manager.cache_clear()
    mock_session_manager = get_session_manager()
    request = MagicMock()
    request.method = method
    assert mock_session_manager.reader_engine is not None
    assert mock_session_manager.writer_engine is not None
    assert mock_session_manager.reader_sessionmaker is not None
    assert mock_session_manager.writer_sessionmaker is not None

    agen = get_session(request)
    session = await anext(agen)
    if expected_session_attr == "reader_session":
        assert (
            str(session.bind.url)
            == "postgresql+psycopg://readonly_user:***@postgres_metadata:5432/dj"
        )
    else:
        assert (
            str(session.bind.url)
            == "postgresql+psycopg://dj:***@postgres_metadata:5432/dj"
        )
    await agen.aclose()


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
    Test DatabaseSessionManager.
    """
    connection_url = postgres_container.get_connection_url()
    settings.writer_db = DatabaseConfig(uri=connection_url)
    mocker.patch("datajunction_server.utils.get_settings", return_value=settings)

    session_manager = DatabaseSessionManager()
    with pytest.raises(DJUninitializedResourceException):
        session_manager.reader_engine
    with pytest.raises(DJUninitializedResourceException):
        session_manager.writer_engine
    with pytest.raises(DJUninitializedResourceException):
        session_manager.reader_sessionmaker
    with pytest.raises(DJUninitializedResourceException):
        session_manager.writer_sessionmaker

    session_manager.init_db()

    writer_engine = session_manager.writer_engine
    writer_engine.pool.size() == settings.writer_db.pool_size  # type: ignore
    writer_engine.pool.timeout() == settings.writer_db.pool_timeout  # type: ignore
    writer_engine.pool.overflow() == settings.writer_db.max_overflow  # type: ignore

    reader_engine = session_manager.reader_engine
    reader_engine.pool.size() == settings.reader_db.pool_size  # type: ignore
    reader_engine.pool.timeout() == settings.reader_db.pool_timeout  # type: ignore
    reader_engine.pool.overflow() == settings.reader_db.max_overflow  # type: ignore

    assert session_manager.reader_engine != session_manager.writer_engine
    assert isinstance(session_manager.reader_sessionmaker, async_sessionmaker)
    assert isinstance(session_manager.writer_sessionmaker, async_sessionmaker)
    assert session_manager.sessionmaker == session_manager.writer_sessionmaker


def test_get_query_service_client(mocker: MockerFixture, settings: Settings) -> None:
    """
    Test ``get_query_service_client``.
    """
    settings.query_service = "http://query_service:8001"
    query_service_client = get_query_service_client(settings=settings)
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
    assert current_user.id == 1
    assert current_user.username == example_user.username

    # Confirm that the user was upserted
    result = await session.execute(select(User).where(User.username == "userfoo"))
    found_user = result.unique().scalar_one_or_none()
    assert found_user.id == current_user.id
    assert found_user.username == "userfoo"  # type: ignore
    assert (
        found_user.password is None  # type: ignore
    )  # If the user is added via upsert, auth is externally managed
    assert found_user.name == "djuser"  # type: ignore
    assert found_user.email == "userfoo@datajunction.io"  # type: ignore
    assert found_user.oauth_provider == "basic"  # type: ignore


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
        OperationalError("flaky", None, None),  # type: ignore
        OperationalError("still flaky", None, None),  # type: ignore
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
    session.execute.side_effect = OperationalError("permanent fail", None, None)  # type: ignore

    with pytest.raises(DJDatabaseException):
        await execute_with_retry(session, statement, retries=3, base_delay=0.01)

    assert session.execute.call_count == 4  # initial try + 3 retries


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path, body, expected",
    [
        # Not /graphql
        ("/not-graphql", json.dumps({"query": "query { users }"}), False),
        # /graphql with query
        ("/graphql", json.dumps({"query": "query { users }"}), True),
        # /graphql with mutation
        (
            "/graphql",
            json.dumps({"query": 'mutation { addUser(name: "Hi") { id } }'}),
            False,
        ),
        # /graphql with invalid JSON
        ("/graphql", "not json", False),
        # /graphql with no query key
        ("/graphql", json.dumps({"foo": "bar"}), False),
        # /graphql with empty body
        ("/graphql", "", False),
    ],
)
async def test_is_graphql_query(path, body, expected):
    """
    Test the `is_graphql_query` utility function.
    This function checks if the request is a GraphQL query based on the path and body.
    """
    # Build a fake ASGI scope
    scope: Scope = {
        "type": "http",
        "method": "POST",
        "path": path,
        "headers": Headers({"content-type": "application/json"}).raw,
    }

    # Create a receive function that yields the body
    async def receive() -> dict:
        return {
            "type": "http.request",
            "body": body.encode(),
            "more_body": False,
        }

    request = Request(scope, receive)
    result = await is_graphql_query(request)
    assert result is expected


def test_get_query_service_client_with_configured_client(
    mocker: MockerFixture,
    settings: Settings,
) -> None:
    """
    Test get_query_service_client with configured client (non-HTTP).
    """
    from datajunction_server.config import QueryClientConfig

    # Configure Snowflake client
    settings.query_client = QueryClientConfig(
        type="snowflake",
        connection={"account": "test_account", "user": "test_user"},
    )
    settings.query_service = None

    # Mock the SnowflakeClient import to avoid dependency issues
    mock_snowflake_client = mocker.MagicMock()
    mocker.patch(
        "datajunction_server.query_clients.SnowflakeClient",
        mock_snowflake_client,
    )

    client = get_query_service_client(settings=settings)
    assert client is not None
    mock_snowflake_client.assert_called_once_with(
        account="test_account",
        user="test_user",
    )


def test_get_query_service_client_returns_none(
    mocker: MockerFixture,
    settings: Settings,
) -> None:
    """
    Test get_query_service_client returns None when no configuration is provided.
    """
    settings.query_service = None
    from datajunction_server.config import QueryClientConfig

    settings.query_client = QueryClientConfig(type="http", connection={})

    client = get_query_service_client(settings=settings)
    assert client is None


def test_create_configured_query_client_http_success(mocker: MockerFixture) -> None:
    """
    Test _create_configured_query_client creates HTTP client successfully.
    """
    from datajunction_server.config import QueryClientConfig
    from datajunction_server.query_clients import HttpQueryServiceClient

    config = QueryClientConfig(type="http", connection={"uri": "http://test:8001"})

    client = _create_configured_query_client(config)
    assert isinstance(client, HttpQueryServiceClient)
    assert client.uri == "http://test:8001"


def test_create_configured_query_client_http_missing_uri(mocker: MockerFixture) -> None:
    """
    Test _create_configured_query_client raises error for HTTP client without URI.
    """
    from datajunction_server.config import QueryClientConfig

    config = QueryClientConfig(type="http", connection={})

    with pytest.raises(ValueError) as exc_info:
        _create_configured_query_client(config)
    assert "HTTP client requires 'uri' in connection parameters" in str(exc_info.value)


def test_create_configured_query_client_snowflake_missing_params(
    mocker: MockerFixture,
) -> None:
    """
    Test _create_configured_query_client raises error for Snowflake client without required params.
    """
    from datajunction_server.config import QueryClientConfig

    # Missing 'user' parameter
    config = QueryClientConfig(type="snowflake", connection={"account": "test_account"})

    with pytest.raises(ValueError) as exc_info:
        _create_configured_query_client(config)
    assert "Snowflake client requires 'user' in connection parameters" in str(
        exc_info.value,
    )

    # Missing 'account' parameter
    config = QueryClientConfig(type="snowflake", connection={"user": "test_user"})

    with pytest.raises(ValueError) as exc_info:
        _create_configured_query_client(config)
    assert "Snowflake client requires 'account' in connection parameters" in str(
        exc_info.value,
    )


def test_create_configured_query_client_snowflake_import_error(
    mocker: MockerFixture,
) -> None:
    """
    Test _create_configured_query_client handles ImportError for Snowflake client.
    """
    from datajunction_server.config import QueryClientConfig

    config = QueryClientConfig(
        type="snowflake",
        connection={"account": "test_account", "user": "test_user"},
    )

    # Mock the import to fail
    mocker.patch(
        "datajunction_server.query_clients.SnowflakeClient",
        side_effect=ImportError("No module named 'snowflake'"),
    )

    with pytest.raises(ValueError) as exc_info:
        _create_configured_query_client(config)
    assert "Snowflake client dependencies not installed" in str(exc_info.value)
    assert "pip install 'datajunction-server[snowflake]'" in str(exc_info.value)


def test_create_configured_query_client_unsupported_type(mocker: MockerFixture) -> None:
    """
    Test _create_configured_query_client raises error for unsupported client type.
    """
    from datajunction_server.config import QueryClientConfig

    config = QueryClientConfig(type="unsupported", connection={})

    with pytest.raises(ValueError) as exc_info:
        _create_configured_query_client(config)
    assert "Unsupported query client type: unsupported" in str(exc_info.value)


def test_get_legacy_query_service_client(
    mocker: MockerFixture,
    settings: Settings,
) -> None:
    """
    Test get_legacy_query_service_client returns QueryServiceClient.
    """
    settings.query_service = "http://query_service:8001"

    mock_query_service_client_cls = mocker.MagicMock()
    mock_query_service_client_instance = mocker.MagicMock()
    mock_query_service_client_cls.return_value = mock_query_service_client_instance
    mocker.patch(
        "datajunction_server.service_clients.QueryServiceClient",
        mock_query_service_client_cls,
    )

    client = get_legacy_query_service_client(settings=settings)
    mock_query_service_client_cls.assert_called_once_with("http://query_service:8001")
    assert client == mock_query_service_client_instance


def test_http_query_service_client_wrapper(mocker: MockerFixture) -> None:
    """
    Test HttpQueryServiceClient properly wraps QueryServiceClient.
    """
    from datajunction_server.query_clients import HttpQueryServiceClient
    from datajunction_server.models.query import QueryCreate

    # Mock the underlying QueryServiceClient
    mock_client = mocker.MagicMock()
    mocker.patch(
        "datajunction_server.query_clients.http.QueryServiceClient",
        return_value=mock_client,
    )

    # Create HTTP client
    client = HttpQueryServiceClient("http://test:8001", retries=3)
    assert client.uri == "http://test:8001"

    # Test get_columns_for_table
    mock_client.get_columns_for_table.return_value = []
    result = client.get_columns_for_table("cat", "sch", "tbl")
    assert result == []
    mock_client.get_columns_for_table.assert_called_once()

    # Test submit_query
    mock_result = mocker.MagicMock()
    mock_client.submit_query.return_value = mock_result
    query = QueryCreate(
        submitted_query="SELECT 1",
        catalog_name="test",
        engine_name="test",
        engine_version="v1",
    )
    result = client.submit_query(query)
    assert result == mock_result
    mock_client.submit_query.assert_called_once()


def test_snowflake_client_initialization_with_mock(mocker: MockerFixture) -> None:
    """
    Test SnowflakeClient initialization when snowflake package is available.
    """
    # Mock snowflake being available
    mocker.patch(
        "datajunction_server.query_clients.snowflake.SNOWFLAKE_AVAILABLE",
        True,
    )

    # Mock the snowflake connector
    mock_snowflake = mocker.MagicMock()
    mocker.patch(
        "datajunction_server.query_clients.snowflake.snowflake",
        mock_snowflake,
    )

    from datajunction_server.query_clients.snowflake import SnowflakeClient

    # Create client with basic params
    client = SnowflakeClient(
        account="test_account",
        user="test_user",
        password="test_pass",
        warehouse="TEST_WH",
        database="TEST_DB",
    )

    assert client.connection_params["account"] == "test_account"
    assert client.connection_params["user"] == "test_user"
    assert client.connection_params["password"] == "test_pass"
    assert client.connection_params["warehouse"] == "TEST_WH"
    assert client.connection_params["database"] == "TEST_DB"
