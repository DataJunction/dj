"""
Configuration for the metric repository.
"""

from pathlib import Path

from pydantic import BaseSettings


class Settings(BaseSettings):  # pylint: disable=too-few-public-methods
    """
    Configuration for the metric repository.
    """

    # SQLAlchemy URI for the metadata database.
    index: str = "sqlite:///dj.db"

    # Directory where the repository lives. This should have 2 subdirectories, "nodes" and
    # "databases".
    repository: Path = Path(".")
