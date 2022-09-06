"""
Add a database to an existing repository
"""

import logging
from pathlib import Path
from typing import Optional

import yaml

from datajunction.errors import DJError, DJException, ErrorCode
from datajunction.models.database import Database

_logger = logging.getLogger(__name__)

def create_databases_dir(repository: Path) -> None:
    """
    Creates a databases directory in the repository if one doesn't already exist
    """
    database_dir_path = repository / Path("databases")
    database_dir_path.mkdir(exist_ok=True)

async def run(repository: Path, database: str, uri: str, description: Optional[str] = None, read_only: Optional[bool] = None, cost: Optional[float] = None) -> None:
    """
    Add a database.
    """
    databases_dir = create_databases_dir(repository)
    db_config_path = (databases_dir / database).with_suffix(".yaml")
    if db_config_path.exists():
        raise DJException(message=f"Database configuration already exists", errors=[DJError(message=f"{db_config_path} already exists", code=ErrorCode.ALREADY_EXISTS)])

    db_kwargs = {
      "database": database,
      "URI": uri,
    }
    if description:
      db_kwargs["description"] = description
    if read_only:
      db_kwargs["read_only"] = read_only
    if cost:
      db_kwargs["cost"] = cost

    db = Database(**db_kwargs)
    with db_config_path.open("w", encoding="utf-8") as output:
        yaml.safe_dump(db.to_yaml(), output, sort_keys=False)
