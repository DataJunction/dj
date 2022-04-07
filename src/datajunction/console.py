"""
DataJunction (DJ) is a metric repository.

Usage:
    dj compile [REPOSITORY] [--loglevel=INFO] [--reload]

Actions:
    compile                 Compile repository

Options:
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
            await compile_.run(repository, arguments["--reload"])
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
