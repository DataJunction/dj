from logging import config
from os import path

config.fileConfig(
    path.join(path.dirname(path.abspath(__file__)), "logging.conf"),
    disable_existing_loggers=False,
)
