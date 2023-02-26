"""
DataJunction (DJ) is a metrics layer.

Usage:
    dj compile [REPOSITORY] [-f] [--loglevel=INFO] [--reload]
    dj add-database DATABASE [REPOSITORY] --uri=URI \
[-f --loglevel=INFO --description=<str> --read-only=<bool> --cost=COST]
    dj urls

Actions:
    compile                 Compile repository
    add-database            Add a database to an existing repository
    urls                    Show URLs for documentation and APIs

Arguments:
    REPOSITORY              Path to a DJ repository
    DATABASE                Name of a database
    URI                     URI to a database
    COST                    The relative cost as compared to other databases

Options:
    -f, --force             Force indexing.    [default: false]
    --loglevel=LEVEL        Level for logging. [default: INFO]
    --reload                Watch for changes. [default: false]

Released under the MIT license.
(c) 2018 Beto Dealmeida <roberto@dealmeida.net>
"""

import asyncio
import logging
from pathlib import Path

from docopt import docopt

from dj import __version__
from dj.cli import add_database
from dj.cli import compile as compile_
from dj.cli import urls
from dj.errors import DJException
from dj.utils import get_settings, setup_logging

_logger = logging.getLogger(__name__)


async def main() -> None:
    """
    Dispatch command.
    """
    arguments = docopt(__doc__, version=__version__)

    setup_logging(arguments["--loglevel"])

    repository = arguments.get("REPOSITORY")
    if repository is None:
        settings = get_settings()
        repository_path = settings.repository
    else:
        repository_path = Path(repository)

    try:
        if arguments.get("compile"):
            try:
                await compile_.run(
                    repository_path,
                    arguments["--force"],
                    arguments["--reload"],
                )
            except DJException as exc:
                _logger.error(exc)
        elif arguments.get("add-database"):
            try:
                await add_database.run(
                    repository_path,
                    database=arguments["DATABASE"],
                    uri=arguments["--uri"],
                    description=arguments["--description"],
                    read_only=arguments["--read-only"],
                    cost=arguments["--cost"],
                )
            except DJException as exc:
                _logger.error(exc)
        elif arguments.get("urls"):
            return urls.run(settings.url)

    except asyncio.CancelledError:
        _logger.info("Canceled")


def run() -> None:
    """
    Run the DJ CLI.
    """
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        _logger.info("Stopping DJ")


if __name__ == "__main__":
    run()
