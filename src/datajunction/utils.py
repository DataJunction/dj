"""
Utility functions.
"""

import logging
from pathlib import Path

import yaml
from rich.logging import RichHandler

from datajunction.constants import CONFIG_FILENAME
from datajunction.models import Config


def setup_logging(loglevel: str) -> None:
    """
    Setup basic logging.
    """
    level = getattr(logging, loglevel.upper(), None)
    if not isinstance(level, int):
        raise ValueError(f"Invalid log level: {loglevel}")

    logformat = "[%(asctime)s] %(levelname)s: %(name)s: %(message)s"
    logging.basicConfig(
        level=level,
        format=logformat,
        datefmt="[%X]",
        handlers=[RichHandler()],
        force=True,
    )


def find_directory(cwd: Path) -> Path:
    """
    Find root of the metrics repository, starting from `cwd`.

    The function will traverse up trying to find a configuration file.
    """
    while not (cwd / CONFIG_FILENAME).exists():
        if cwd == cwd.parent:
            raise SystemExit("No configuration found!")
        cwd = cwd.parent

    return cwd


def load_config(root: Path) -> Config:
    """
    Return the configuration for a metrics repository.
    """
    path = root / CONFIG_FILENAME
    if not path.exists():
        raise SystemExit("No configuration found!")

    with open(path, encoding="utf-8") as input_:
        config_data = yaml.safe_load(input_)

    config = Config(**config_data)

    return config
