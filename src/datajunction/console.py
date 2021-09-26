"""
DataJunction (DJ) is a metric repository.

Usage:
    dj c [REPOSITORY] [--loglevel=INFO]
    dj t METRIC [-db DATABASE] [--loglevel=INFO]
    dj g METRIC [-db DATABASE] [-f FORMAT] [--loglevel=INFO]

Actions:
    c                   Compile repository
    t                   Translate a metric into SQL
    g                   Get data for a given metric

Options:
    -db DATABASE        Specify a database
    -f FORMAT           Data format (JSON, CSV)
    --loglevel=LEVEL    Level for logging. [default: INFO]

Released under the MIT license.
(c) 2018 Beto Dealmeida <roberto@dealmeida.net>
"""

import asyncio
import logging
import os
from pathlib import Path

from docopt import docopt

from datajunction import __version__
from datajunction.cli import compile
from datajunction.utils import find_directory, setup_logging

_logger = logging.getLogger(__name__)


async def main() -> None:
    """
    Dispatch command.
    """
    arguments = docopt(__doc__, version=__version__)

    setup_logging(arguments["--loglevel"])

    if arguments["REPOSITORY"] is None:
        repository = find_directory(Path(os.getcwd()))
    else:
        repository = Path(arguments["REPOSITORY"])

    try:
        if arguments["c"]:
            await compile.run(repository)
        elif arguments["t"]:
            raise NotImplementedError()
        elif arguments["g"]:
            raise NotImplementedError()
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
