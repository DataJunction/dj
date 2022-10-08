"""
Add a database to an existing repository
"""

import logging
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from dj.errors import DJError, DJException, ErrorCode
from dj.models.database import Database

_logger = logging.getLogger(__name__)


def create_databases_dir(repository: Path) -> Path:
    """
    Creates a databases directory in the repository if one doesn't already exist
    """
    databases_dir = repository / Path("databases")
    databases_dir.mkdir(exist_ok=True)
    return databases_dir


async def run(  # pylint: disable=too-many-arguments
    repository: Path,
    database: str,
    uri: str,
    description: Optional[str] = None,
    read_only: Optional[bool] = None,
    cost: Optional[float] = None,
) -> None:
    """
    Add a database.
    """
    databases_dir = create_databases_dir(repository)
    db_config_path = (databases_dir / database).with_suffix(".yaml")
    if db_config_path.exists():
        raise DJException(
            message="Database configuration already exists",
            errors=[
                DJError(
                    message=f"{db_config_path} already exists",
                    code=ErrorCode.ALREADY_EXISTS,
                ),
            ],
        )

    db_kwargs: Dict[str, Any] = {
        "database": database,
        "URI": uri,
    }
    if description:
        db_kwargs["description"] = description
    if read_only:
        db_kwargs["read_only"] = read_only
    if cost:
        db_kwargs["cost"] = cost

    new_database = Database(**db_kwargs)
    with db_config_path.open("w", encoding="utf-8") as output:
        yaml.safe_dump(new_database.to_yaml(), output, sort_keys=False)
