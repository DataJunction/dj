"""
Tests for ``datajunction_server.utils``.
"""

import logging
from unittest.mock import patch

import pytest
from pytest_mock import MockerFixture
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.background import BackgroundTasks
from testcontainers.postgres import PostgresContainer
from yarl import URL

from datajunction_server.config import Settings
from datajunction_server.database.user import OAuthProvider, User
from datajunction_server.errors import DJException
from datajunction_server.utils import (
    Version,
    get_and_update_current_user,
    get_engine,
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
        session = await anext(get_session())
        assert isinstance(session, AsyncSession)


def test_get_settings(mocker: MockerFixture) -> None:
    """
    Test ``get_settings``.
    """
    mocker.patch("datajunction_server.utils.load_dotenv")
    Settings = mocker.patch(  # pylint: disable=invalid-name, redefined-outer-name
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


def test_get_engine(
    mocker: MockerFixture,
    settings: Settings,
    postgres_container: PostgresContainer,
) -> None:
    """
    Test ``get_engine``.
    """
    connection_url = postgres_container.get_connection_url()
    settings.index = connection_url
    mocker.patch("datajunction_server.utils.get_settings", return_value=settings)
    engine = get_engine()
    assert engine.pool.size() == settings.db_pool_size
    assert engine.pool.timeout() == settings.db_pool_timeout
    assert engine.pool.overflow() == -settings.db_max_overflow


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
