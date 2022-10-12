"""
Tests for ``dj.console``.
"""
# pylint: disable=invalid-name

import asyncio
from pathlib import Path

import pytest
from pyfakefs.fake_filesystem import FakeFilesystem
from pytest_mock import MockerFixture

from dj import console
from dj.config import Settings
from dj.errors import DJError, DJException, ErrorCode


@pytest.mark.asyncio
async def test_main_compile(mocker: MockerFixture) -> None:
    """
    Test ``main`` with the "compile" action.
    """
    compile_ = mocker.patch("dj.console.compile_")
    compile_.run = mocker.AsyncMock()

    mocker.patch(
        "dj.console.docopt",
        return_value={
            "--loglevel": "debug",
            "--force": False,
            "--reload": False,
            "compile": True,
            "REPOSITORY": None,
        },
    )
    mocker.patch(
        "dj.console.get_settings",
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
    compile_ = mocker.patch("dj.console.compile_")
    compile_.run = mocker.AsyncMock()

    mocker.patch(
        "dj.console.docopt",
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
    compile_ = mocker.patch("dj.console.compile_")
    compile_.run = mocker.AsyncMock(side_effect=asyncio.CancelledError("Canceled"))
    _logger = mocker.patch("dj.console._logger")

    mocker.patch(
        "dj.console.docopt",
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
    compile_ = mocker.patch("dj.console.compile_")
    compile_.run = mocker.AsyncMock(side_effect=exc)
    _logger = mocker.patch("dj.console._logger")

    mocker.patch(
        "dj.console.docopt",
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
        "dj.console.docopt",
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
    mocker.patch("dj.console.main", main)

    console.run()

    main.assert_called()


def test_interrupt(mocker: MockerFixture) -> None:
    """
    Test that ``CTRL-C`` stops the CLI.
    """
    main = mocker.AsyncMock(side_effect=KeyboardInterrupt())
    mocker.patch("dj.console.main", main)
    _logger = mocker.patch("dj.console._logger")

    console.run()

    _logger.info.assert_called_with("Stopping DJ")


@pytest.mark.asyncio
async def test_main_add_database(mocker: MockerFixture) -> None:
    """
    Test ``main`` with the "add-database" action.
    """
    add_database = mocker.patch("dj.console.add_database")
    add_database.run = mocker.AsyncMock()

    mocker.patch(
        "dj.console.docopt",
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
        "dj.console.get_settings",
        return_value=Settings(
            index="sqlite:///dj.db",
            repository=Path("/path/to/repository"),
        ),
    )

    await console.main()
    add_database.run.assert_called_with(
        Path("/path/to/repository"),
        database="testdb",
        uri="testdb://test",
        description="This is a description",
        read_only=True,
        cost=11.0,
    )


@pytest.mark.asyncio
async def test_main_add_database_passing_repository(mocker: MockerFixture) -> None:
    """
    Test ``main`` with the "add-database" action.
    """
    add_database = mocker.patch("dj.console.add_database")
    add_database.run = mocker.AsyncMock()

    mocker.patch(
        "dj.console.docopt",
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
    add_database.run.assert_called_with(
        Path("/path/to/another/repository"),
        database="testdb",
        uri="testdb://test",
        description="This is a description",
        read_only=True,
        cost=11.0,
    )


@pytest.mark.asyncio
async def test_main_add_database_raise_already_exists(
    mocker: MockerFixture,
    fs: FakeFilesystem,
) -> None:
    """
    Test ``main`` with the "add-database" action raising when the database already exists
    """
    exc = DJException(
        message="Database configuration already exists",
        errors=[
            DJError(
                message="/foo/databases/testdb.yaml already exists",
                code=ErrorCode.ALREADY_EXISTS,
            ),
        ],
    )
    _logger = mocker.patch("dj.console._logger")
    test_repo = "/foo"
    fs.create_dir(test_repo)
    mocker.patch(
        "dj.console.docopt",
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
        "dj.console.get_settings",
        return_value=Settings(
            index="sqlite:///dj.db",
            repository=Path(test_repo),
        ),
    )

    await console.main()
    await console.main()  # Run a second time to log an already exists exception
    _logger.error.assert_called_with(exc)


@pytest.mark.asyncio
async def test_main_urls(mocker: MockerFixture) -> None:
    """
    Test ``main`` with the "urls" action.
    """
    urls = mocker.patch("dj.console.urls")
    urls.run = mocker.AsyncMock()

    mocker.patch(
        "dj.console.docopt",
        return_value={
            "--loglevel": "debug",
            "urls": True,
        },
    )
    mocker.patch(
        "dj.console.get_settings",
        return_value=Settings(url="http://localhost:8000/"),
    )

    await console.main()
    urls.run.assert_called_with("http://localhost:8000/")
