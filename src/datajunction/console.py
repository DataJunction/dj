"""
DataJunction (DJ) is a metric repository.

Usage:
    dj compile [REPOSITORY] [-f] [--loglevel=INFO] [--reload]

Actions:
    compile                 Compile repository

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

from datajunction import __version__
from datajunction.cli import compile as compile_
from datajunction.errors import DJException
from datajunction.utils import get_settings, setup_logging

_logger = logging.getLogger(__name__)


async def main() -> None:
    """
    Dispatch command.
    """
    arguments = docopt(__doc__, version=__version__)

    setup_logging(arguments["--loglevel"])

    if arguments["REPOSITORY"] is None:
        settings = get_settings()
        repository = settings.repository
    else:
        repository = Path(arguments["REPOSITORY"])

    try:
        if arguments["compile"]:
            try:
                await compile_.run(
                    repository,
                    arguments["--force"],
                    arguments["--reload"],
                )
            except DJException as exc:
                _logger.error(exc)
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
