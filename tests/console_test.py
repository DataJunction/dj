"""
Tests for ``datajunction.console``.
"""

import asyncio
from pathlib import Path
import tempfile

import pytest
from pytest_mock import MockerFixture

from datajunction import console
from datajunction.config import Settings
from datajunction.errors import DJException


@pytest.mark.asyncio
async def test_main_compile(mocker: MockerFixture) -> None:
    """
    Test ``main`` with the "compile" action.
    """
    compile_ = mocker.patch("datajunction.console.compile_")
    compile_.run = mocker.AsyncMock()

    mocker.patch(
        "datajunction.console.docopt",
        return_value={
            "--loglevel": "debug",
            "--force": False,
            "--reload": False,
            "compile": True,
            "REPOSITORY": None,
        },
    )
    mocker.patch(
        "datajunction.console.get_settings",
        return_value=Settings(
            index="sqlite:///dj.db",
            repository=Path("/path/to/repository"),
        ),
    )

    await console.main()
    compile_.run.assert_called_with(Path("/path/to/repository"), False, False)


@pytest.mark.asyncio
async def test_main_compile_passing_repository(mocker: MockerFixture) -> None:
    """
    Test ``main`` with the "compile" action.
    """
    compile_ = mocker.patch("datajunction.console.compile_")
    compile_.run = mocker.AsyncMock()

    mocker.patch(
        "datajunction.console.docopt",
        return_value={
            "--loglevel": "debug",
            "--force": False,
            "--reload": False,
            "compile": True,
            "REPOSITORY": "/path/to/another/repository",
        },
    )

    await console.main()
    compile_.run.assert_called_with(Path("/path/to/another/repository"), False, False)


@pytest.mark.asyncio
async def test_main_canceled(mocker: MockerFixture) -> None:
    """
    Test canceling the ``main`` coroutine.
    """
    compile_ = mocker.patch("datajunction.console.compile_")
    compile_.run = mocker.AsyncMock(side_effect=asyncio.CancelledError("Canceled"))
    _logger = mocker.patch("datajunction.console._logger")

    mocker.patch(
        "datajunction.console.docopt",
        return_value={
            "--loglevel": "debug",
            "--force": False,
            "--reload": False,
            "compile": True,
            "REPOSITORY": "/path/to/another/repository",
        },
    )

    await console.main()

    _logger.info.assert_called_with("Canceled")


@pytest.mark.asyncio
async def test_main_error(mocker: MockerFixture) -> None:
    """
    Test canceling the ``main`` coroutine.
    """
    exc = DJException("An error occurred")
    compile_ = mocker.patch("datajunction.console.compile_")
    compile_.run = mocker.AsyncMock(side_effect=exc)
    _logger = mocker.patch("datajunction.console._logger")

    mocker.patch(
        "datajunction.console.docopt",
        return_value={
            "--loglevel": "debug",
            "--force": False,
            "--reload": False,
            "compile": True,
            "REPOSITORY": "/path/to/another/repository",
        },
    )

    await console.main()

    _logger.error.assert_called_with(exc)


@pytest.mark.asyncio
async def test_main_no_action(mocker: MockerFixture) -> None:
    """
    Test ``main`` without any actions -- should not happen.
    """

    mocker.patch(
        "datajunction.console.docopt",
        return_value={
            "--loglevel": "debug",
            "compile": False,
            "REPOSITORY": "/path/to/another/repository",
        },
    )

    await console.main()


def test_run(mocker: MockerFixture) -> None:
    """
    Test ``run``.
    """
    main = mocker.AsyncMock()
    mocker.patch("datajunction.console.main", main)

    console.run()

    main.assert_called()


def test_interrupt(mocker: MockerFixture) -> None:
    """
    Test that ``CTRL-C`` stops the CLI.
    """
    main = mocker.AsyncMock(side_effect=KeyboardInterrupt())
    mocker.patch("datajunction.console.main", main)
    _logger = mocker.patch("datajunction.console._logger")

    console.run()

    _logger.info.assert_called_with("Stopping DJ")

@pytest.mark.asyncio
async def test_main_add_database(mocker: MockerFixture) -> None:
    """
    Test ``main`` with the "add-database" action.
    """
    add_database_ = mocker.patch("datajunction.console.add_database_")
    add_database_.run = mocker.AsyncMock()

    mocker.patch(
        "datajunction.console.docopt",
        return_value={
            "--loglevel": "debug",
            "--force": False,
            "--reload": False,
            "--description": "This is a description",
            "--read-only": True,
            "--uri": "testdb://test",
            "--cost": 11.0,
            "add-database": True,
            "DATABASE": "testdb",
            "REPOSITORY": None,
        },
    )
    mocker.patch(
        "datajunction.console.get_settings",
        return_value=Settings(
            index="sqlite:///dj.db",
            repository=Path("/path/to/repository"),
        ),
    )

    await console.main()
    add_database_.run.assert_called_with(
        Path("/path/to/repository"),
        database="testdb",
        uri="testdb://test",
        description="This is a description",
        read_only=True,
        cost=11.0
    )

@pytest.mark.asyncio
async def test_main_add_database_passing_repository(mocker: MockerFixture) -> None:
    """
    Test ``main`` with the "add-database" action.
    """
    with tempfile.TemporaryDirectory() as tmpdirname:
        add_database_ = mocker.patch("datajunction.console.add_database_")
        add_database_.run = mocker.AsyncMock()

        mocker.patch(
            "datajunction.console.docopt",
            return_value={
                "--loglevel": "debug",
                "--force": False,
                "--reload": False,
                "--description": "This is a description",
                "--read-only": True,
                "--uri": "testdb://test",
                "--cost": 11.0,
                "add-database": True,
                "DATABASE": "testdb",
                "REPOSITORY": "/path/to/another/repository",
            },
        )

        await console.main()
        add_database_.run.assert_called_with(
            Path("/path/to/another/repository"),
            database="testdb",
            uri="testdb://test",
            description="This is a description",
            read_only=True,
            cost=11.0
        )

@pytest.mark.asyncio
async def test_main_add_database_raise_already_exists(mocker: MockerFixture) -> None:
    """
    Test ``main`` with the "add-database" action raising when the database already exists
    """
    with tempfile.TemporaryDirectory() as tmpdirname:
        mocker.patch(
            "datajunction.console.docopt",
            return_value={
                "--loglevel": "debug",
                "--force": False,
                "--reload": False,
                "--description": "This is a description",
                "--read-only": True,
                "--uri": "testdb://test",
                "--cost": 11.0,
                "add-database": True,
                "DATABASE": "testdb",
                "REPOSITORY": None,
            },
        )
        mocker.patch(
            "datajunction.console.get_settings",
            return_value=Settings(
                index="sqlite:///dj.db",
                repository=Path(tmpdirname),
            ),
        )

        await console.main()
        await console.main()  # Run an add-database command a second time, logs an already exists exception