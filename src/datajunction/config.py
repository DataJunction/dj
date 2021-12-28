"""
Configuration for the metric repository.
"""

from pydantic import BaseSettings


class Settings(BaseSettings):  # pylint: disable=too-few-public-methods
    """
    Configuration for the metric repository.
    """

    # SQLAlchemy URI for the metadata database.
    index: str = "sqlite:///dj.db"

    class Config:  # pylint: disable=too-few-public-methods, missing-class-docstring
        env_file = ".env"
