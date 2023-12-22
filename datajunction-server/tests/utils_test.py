"""
Tests for ``datajunction_server.utils``.
"""

import logging

import pytest
from pytest_mock import MockerFixture
from sqlalchemy.engine.url import make_url
from yarl import URL

from datajunction_server.config import Settings
from datajunction_server.errors import DJException
from datajunction_server.utils import (
    Version,
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


def test_get_session(mocker: MockerFixture) -> None:
    """
    Test ``get_session``.
    """
    engine = mocker.patch("datajunction_server.utils.get_engine")
    sessionmaker = mocker.patch(  # pylint: disable=invalid-name
        "datajunction_server.utils.sessionmaker",
    )

    session = next(get_session())
    assert (
        session
        == sessionmaker(autocommit=False, autoflush=False, bind=engine).return_value
    )


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


def test_get_engine(mocker: MockerFixture, settings: Settings) -> None:
    """
    Test ``get_engine``.
    """
    mocker.patch("datajunction_server.utils.get_settings", return_value=settings)
    engine = get_engine()
    assert engine.url == make_url("sqlite://")


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
