"""
Tests for ``datajunction.console``.
"""

import asyncio
from pathlib import Path

import pytest
from pytest_mock import MockerFixture

from datajunction import console


@pytest.mark.asyncio
async def test_main_compile(mocker: MockerFixture) -> None:
    """
    Test ``main`` with the "compile" action.
    """
    compile = mocker.patch("datajunction.console.compile")
    compile.run = mocker.AsyncMock()

    mocker.patch(
        "datajunction.console.docopt",
        return_value={
            "--loglevel": "debug",
            "compile": True,
            "REPOSITORY": None,
        },
    )
    mocker.patch(
        "datajunction.console.find_directory", return_value=Path("/path/to/repository")
    )

    await console.main()
    compile.run.assert_called_with(Path("/path/to/repository"))


@pytest.mark.asyncio
async def test_main_compile_passing_repository(mocker: MockerFixture) -> None:
    """
    Test ``main`` with the "compile" action.
    """
    compile = mocker.patch("datajunction.console.compile")
    compile.run = mocker.AsyncMock()

    mocker.patch(
        "datajunction.console.docopt",
        return_value={
            "--loglevel": "debug",
            "compile": True,
            "REPOSITORY": "/path/to/another/repository",
        },
    )

    await console.main()
    compile.run.assert_called_with(Path("/path/to/another/repository"))


@pytest.mark.asyncio
async def test_main_canceled(mocker: MockerFixture) -> None:
    """
    Test canceling the ``main`` coroutine.
    """
    compile = mocker.patch("datajunction.console.compile")
    compile.run = mocker.AsyncMock(side_effect=asyncio.CancelledError("Canceled"))
    _logger = mocker.patch("datajunction.console._logger")

    mocker.patch(
        "datajunction.console.docopt",
        return_value={
            "--loglevel": "debug",
            "compile": True,
            "REPOSITORY": "/path/to/another/repository",
        },
    )

    await console.main()

    _logger.info.assert_called_with("Canceled")


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
